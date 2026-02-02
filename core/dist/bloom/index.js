/**
 * Split-Block Bloom Filter Implementation
 *
 * Implements Apache Parquet's split-block bloom filter (SBBF) specification
 * for efficient file skipping in Apache Iceberg tables.
 *
 * Key features:
 * - Split-block design with 256-bit blocks (8 x 32-bit words)
 * - XXH64-based hashing with salt-based bit positioning
 * - Configurable false positive rate
 * - Serialization/deserialization for storage
 * - Integration with Iceberg manifest entries
 *
 * @see https://parquet.apache.org/docs/file-format/bloomfilter/
 * @see https://iceberg.apache.org/spec/
 */
// ============================================================================
// Constants
// ============================================================================
/** Size of each block in bits (256 bits = 32 bytes) */
const BLOCK_SIZE_BITS = 256;
/** Size of each block in bytes */
const BLOCK_SIZE_BYTES = BLOCK_SIZE_BITS / 8;
/** Number of 32-bit words per block */
const WORDS_PER_BLOCK = 8;
/** Salt constants for bit positioning within blocks */
const SALT = new Uint32Array([
    0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
    0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31,
]);
/** Default false positive probability */
const DEFAULT_FPP = 0.01;
/** Minimum number of blocks */
const MIN_BLOCKS = 1;
/** Maximum filter size in bytes (1MB default, per Iceberg spec) */
const DEFAULT_MAX_BYTES = 1048576;
/** Magic bytes for bloom filter file format */
const BLOOM_FILTER_MAGIC = new Uint8Array([0x42, 0x4c, 0x4f, 0x4f, 0x4d]); // "BLOOM"
/** Format version */
const FORMAT_VERSION = 1;
// ============================================================================
// XXH64 Hash Implementation
// ============================================================================
/**
 * XXH64 hash constants
 */
const XXH64_PRIME1 = 0x9e3779b185ebca87n;
const XXH64_PRIME2 = 0xc2b2ae3d27d4eb4fn;
const XXH64_PRIME3 = 0x165667b19e3779f9n;
const XXH64_PRIME4 = 0x85ebca77c2b2ae63n;
const XXH64_PRIME5 = 0x27d4eb2f165667c5n;
/**
 * Rotate left operation for BigInt
 */
function rotl64(value, bits) {
    return ((value << BigInt(bits)) | (value >> BigInt(64 - bits))) & 0xffffffffffffffffn;
}
/**
 * Mix function for finalization
 */
function xxh64Avalanche(hash) {
    hash ^= hash >> 33n;
    hash = (hash * XXH64_PRIME2) & 0xffffffffffffffffn;
    hash ^= hash >> 29n;
    hash = (hash * XXH64_PRIME3) & 0xffffffffffffffffn;
    hash ^= hash >> 32n;
    return hash;
}
/**
 * XXH64 hash function implementation
 *
 * @param data - Input data to hash
 * @param seed - Optional seed value (default: 0)
 * @returns 64-bit hash as BigInt
 */
export function xxh64(data, seed = 0n) {
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const len = data.length;
    let hash;
    if (len >= 32) {
        let v1 = seed + XXH64_PRIME1 + XXH64_PRIME2;
        let v2 = seed + XXH64_PRIME2;
        let v3 = seed;
        let v4 = seed - XXH64_PRIME1;
        let offset = 0;
        const limit = len - 32;
        while (offset <= limit) {
            v1 = rotl64((v1 + BigInt(view.getBigUint64(offset, true)) * XXH64_PRIME2) & 0xffffffffffffffffn, 31) * XXH64_PRIME1;
            v2 = rotl64((v2 + BigInt(view.getBigUint64(offset + 8, true)) * XXH64_PRIME2) & 0xffffffffffffffffn, 31) * XXH64_PRIME1;
            v3 = rotl64((v3 + BigInt(view.getBigUint64(offset + 16, true)) * XXH64_PRIME2) & 0xffffffffffffffffn, 31) * XXH64_PRIME1;
            v4 = rotl64((v4 + BigInt(view.getBigUint64(offset + 24, true)) * XXH64_PRIME2) & 0xffffffffffffffffn, 31) * XXH64_PRIME1;
            offset += 32;
        }
        hash = rotl64(v1, 1) + rotl64(v2, 7) + rotl64(v3, 12) + rotl64(v4, 18);
        v1 = rotl64(v1 * XXH64_PRIME2 & 0xffffffffffffffffn, 31) * XXH64_PRIME1;
        hash = ((hash ^ v1) * XXH64_PRIME1 + XXH64_PRIME4) & 0xffffffffffffffffn;
        v2 = rotl64(v2 * XXH64_PRIME2 & 0xffffffffffffffffn, 31) * XXH64_PRIME1;
        hash = ((hash ^ v2) * XXH64_PRIME1 + XXH64_PRIME4) & 0xffffffffffffffffn;
        v3 = rotl64(v3 * XXH64_PRIME2 & 0xffffffffffffffffn, 31) * XXH64_PRIME1;
        hash = ((hash ^ v3) * XXH64_PRIME1 + XXH64_PRIME4) & 0xffffffffffffffffn;
        v4 = rotl64(v4 * XXH64_PRIME2 & 0xffffffffffffffffn, 31) * XXH64_PRIME1;
        hash = ((hash ^ v4) * XXH64_PRIME1 + XXH64_PRIME4) & 0xffffffffffffffffn;
    }
    else {
        hash = seed + XXH64_PRIME5;
    }
    hash = (hash + BigInt(len)) & 0xffffffffffffffffn;
    let offset = len >= 32 ? len - (len % 32) : 0;
    // Process remaining 8-byte chunks
    while (offset + 8 <= len) {
        const k1 = rotl64(BigInt(view.getBigUint64(offset, true)) * XXH64_PRIME2 & 0xffffffffffffffffn, 31) * XXH64_PRIME1;
        hash = (rotl64(hash ^ k1, 27) * XXH64_PRIME1 + XXH64_PRIME4) & 0xffffffffffffffffn;
        offset += 8;
    }
    // Process remaining 4-byte chunk
    if (offset + 4 <= len) {
        hash = (rotl64(hash ^ (BigInt(view.getUint32(offset, true)) * XXH64_PRIME1) & 0xffffffffffffffffn, 23) * XXH64_PRIME2 + XXH64_PRIME3) & 0xffffffffffffffffn;
        offset += 4;
    }
    // Process remaining bytes
    while (offset < len) {
        hash = (rotl64(hash ^ (BigInt(data[offset]) * XXH64_PRIME5) & 0xffffffffffffffffn, 11) * XXH64_PRIME1) & 0xffffffffffffffffn;
        offset++;
    }
    return xxh64Avalanche(hash);
}
/**
 * Hash a string value using XXH64
 */
export function xxh64String(value, seed = 0n) {
    const encoder = new TextEncoder();
    return xxh64(encoder.encode(value), seed);
}
/**
 * Hash a number value using XXH64
 */
export function xxh64Number(value, seed = 0n) {
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);
    view.setFloat64(0, value, true);
    return xxh64(new Uint8Array(buffer), seed);
}
/**
 * Hash a BigInt value using XXH64
 */
export function xxh64BigInt(value, seed = 0n) {
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);
    view.setBigInt64(0, value, true);
    return xxh64(new Uint8Array(buffer), seed);
}
// ============================================================================
// Split-Block Bloom Filter
// ============================================================================
/**
 * Calculate optimal number of blocks for given parameters
 *
 * Based on the formula: m = -n * ln(p) / (ln(2)^2)
 * where m is bits, n is items, p is false positive rate
 *
 * @param expectedItems - Expected number of distinct values
 * @param fpp - Target false positive probability
 * @param maxBytes - Maximum filter size in bytes
 * @returns Number of blocks
 */
export function calculateOptimalBlocks(expectedItems, fpp = DEFAULT_FPP, maxBytes = DEFAULT_MAX_BYTES) {
    if (expectedItems <= 0) {
        return MIN_BLOCKS;
    }
    // Optimal bits: m = -n * ln(p) / (ln(2)^2)
    const optimalBits = Math.ceil((-expectedItems * Math.log(fpp)) / (Math.log(2) ** 2));
    // Convert to blocks (256 bits per block)
    let numBlocks = Math.ceil(optimalBits / BLOCK_SIZE_BITS);
    // Ensure minimum
    numBlocks = Math.max(numBlocks, MIN_BLOCKS);
    // Ensure we don't exceed max bytes
    const maxBlocks = Math.floor(maxBytes / BLOCK_SIZE_BYTES);
    numBlocks = Math.min(numBlocks, maxBlocks);
    return numBlocks;
}
/**
 * Estimate false positive rate for given parameters
 *
 * For split-block bloom filter, the FPR is approximately:
 * (1 - e^(-k*n/m))^k where k is number of hash functions (8 for SBBF),
 * n is number of items, and m is number of bits.
 *
 * @param numBlocks - Number of blocks
 * @param itemCount - Number of items inserted
 * @returns Estimated false positive rate
 */
export function estimateFalsePositiveRate(numBlocks, itemCount) {
    if (itemCount === 0 || numBlocks === 0) {
        return 0;
    }
    const numBits = numBlocks * BLOCK_SIZE_BITS;
    const k = WORDS_PER_BLOCK; // 8 hash functions in SBBF
    // FPR = (1 - e^(-k*n/m))^k
    const exponent = (-k * itemCount) / numBits;
    return Math.pow(1 - Math.exp(exponent), k);
}
/**
 * Split-Block Bloom Filter implementation per Parquet specification.
 *
 * Each block is 256 bits (8 x 32-bit words). When inserting a value:
 * 1. Hash the value using XXH64 to get a 64-bit hash
 * 2. Use lower 32 bits to select a block
 * 3. Use salt constants to determine which bit to set in each word
 *
 * This implementation follows the Parquet SBBF specification used by Apache Iceberg
 * for efficient file-level filtering.
 */
export class BloomFilter {
    blocks;
    numBlocks;
    itemCount = 0;
    fpp;
    /**
     * Create a new BloomFilter
     *
     * @param options - Filter configuration options
     */
    constructor(options = {}) {
        const { expectedItems = 10000, falsePositiveRate = DEFAULT_FPP, maxBytes = DEFAULT_MAX_BYTES, numBlocks: providedBlocks, } = options;
        this.fpp = falsePositiveRate;
        // Use provided blocks or calculate optimal
        this.numBlocks = providedBlocks ?? calculateOptimalBlocks(expectedItems, falsePositiveRate, maxBytes);
        // Allocate blocks (8 words per block = 32 bytes per block)
        this.blocks = new Uint32Array(this.numBlocks * WORDS_PER_BLOCK);
    }
    /**
     * Get the number of blocks in the filter
     */
    get blockCount() {
        return this.numBlocks;
    }
    /**
     * Get the number of items added to the filter
     */
    get count() {
        return this.itemCount;
    }
    /**
     * Get the target false positive rate
     */
    get falsePositiveRate() {
        return this.fpp;
    }
    /**
     * Get estimated actual false positive rate based on fill ratio
     */
    get estimatedFalsePositiveRate() {
        return estimateFalsePositiveRate(this.numBlocks, this.itemCount);
    }
    /**
     * Get size of the filter in bytes
     */
    get sizeInBytes() {
        return this.numBlocks * BLOCK_SIZE_BYTES;
    }
    /**
     * Compute block index and word masks for a hash value
     */
    computeBlockMask(hash) {
        // Use lower 32 bits to select block
        const lower32 = Number(hash & 0xffffffffn);
        const blockIndex = lower32 % this.numBlocks;
        // Use upper 32 bits for word mask computation
        const upper32 = Number((hash >> 32n) & 0xffffffffn);
        // Compute masks for each of the 8 words using salt constants
        const masks = new Uint32Array(WORDS_PER_BLOCK);
        for (let i = 0; i < WORDS_PER_BLOCK; i++) {
            // Multiply by salt, keep lower 32 bits, shift right by 27
            // This gives a value 0-31, indicating which bit to set in that word
            const product = Math.imul(upper32, SALT[i]) >>> 0;
            const bitPosition = product >>> 27; // 0-31
            masks[i] = 1 << bitPosition;
        }
        return { blockIndex, masks };
    }
    /**
     * Add a value to the bloom filter
     *
     * @param value - Value to add (will be hashed)
     */
    add(value) {
        const hash = this.hashValue(value);
        const { blockIndex, masks } = this.computeBlockMask(hash);
        // Set bits in each word of the block
        const blockOffset = blockIndex * WORDS_PER_BLOCK;
        for (let i = 0; i < WORDS_PER_BLOCK; i++) {
            this.blocks[blockOffset + i] |= masks[i];
        }
        this.itemCount++;
    }
    /**
     * Add multiple values to the bloom filter
     *
     * @param values - Values to add
     */
    addAll(values) {
        for (const value of values) {
            this.add(value);
        }
    }
    /**
     * Check if a value might be in the set
     *
     * @param value - Value to check
     * @returns true if the value might be present, false if definitely not present
     */
    mightContain(value) {
        const hash = this.hashValue(value);
        const { blockIndex, masks } = this.computeBlockMask(hash);
        // Check if all bits are set in the block
        const blockOffset = blockIndex * WORDS_PER_BLOCK;
        for (let i = 0; i < WORDS_PER_BLOCK; i++) {
            if ((this.blocks[blockOffset + i] & masks[i]) === 0) {
                return false;
            }
        }
        return true;
    }
    /**
     * Hash a value to a 64-bit integer
     */
    hashValue(value) {
        if (typeof value === 'string') {
            return xxh64String(value);
        }
        else if (typeof value === 'number') {
            return xxh64Number(value);
        }
        else if (typeof value === 'bigint') {
            return xxh64BigInt(value);
        }
        else {
            return xxh64(value);
        }
    }
    /**
     * Clear the bloom filter
     */
    clear() {
        this.blocks.fill(0);
        this.itemCount = 0;
    }
    /**
     * Merge another bloom filter into this one (union)
     * Both filters must have the same number of blocks.
     *
     * @param other - Other bloom filter to merge
     */
    merge(other) {
        if (other.numBlocks !== this.numBlocks) {
            throw new Error(`Cannot merge bloom filters with different block counts: ${this.numBlocks} vs ${other.numBlocks}`);
        }
        for (let i = 0; i < this.blocks.length; i++) {
            this.blocks[i] |= other.blocks[i];
        }
        this.itemCount += other.itemCount;
    }
    /**
     * Serialize the bloom filter to bytes
     *
     * Format:
     * - 5 bytes: Magic "BLOOM"
     * - 1 byte: Version
     * - 4 bytes: Number of blocks (little-endian)
     * - 4 bytes: Item count (little-endian)
     * - 8 bytes: False positive rate (float64 little-endian)
     * - N bytes: Block data
     *
     * @returns Serialized filter data
     */
    serialize() {
        const headerSize = 5 + 1 + 4 + 4 + 8; // magic + version + numBlocks + itemCount + fpp
        const dataSize = this.numBlocks * BLOCK_SIZE_BYTES;
        const buffer = new Uint8Array(headerSize + dataSize);
        const view = new DataView(buffer.buffer);
        let offset = 0;
        // Magic bytes
        buffer.set(BLOOM_FILTER_MAGIC, offset);
        offset += 5;
        // Version
        buffer[offset++] = FORMAT_VERSION;
        // Number of blocks
        view.setUint32(offset, this.numBlocks, true);
        offset += 4;
        // Item count
        view.setUint32(offset, this.itemCount, true);
        offset += 4;
        // False positive rate
        view.setFloat64(offset, this.fpp, true);
        offset += 8;
        // Block data (convert Uint32Array to bytes)
        const blockBytes = new Uint8Array(this.blocks.buffer);
        buffer.set(blockBytes, offset);
        return buffer;
    }
    /**
     * Get the raw block data for low-level access
     *
     * @returns Raw block data as Uint8Array
     */
    getRawData() {
        return new Uint8Array(this.blocks.buffer);
    }
    /**
     * Create a bloom filter from serialized bytes
     *
     * @param data - Serialized filter data
     * @returns Deserialized bloom filter
     */
    static deserialize(data) {
        const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
        let offset = 0;
        // Verify magic bytes
        for (let i = 0; i < BLOOM_FILTER_MAGIC.length; i++) {
            if (data[offset + i] !== BLOOM_FILTER_MAGIC[i]) {
                throw new Error('Invalid bloom filter magic bytes');
            }
        }
        offset += 5;
        // Version
        const version = data[offset++];
        if (version !== FORMAT_VERSION) {
            throw new Error(`Unsupported bloom filter version: ${version}`);
        }
        // Number of blocks
        const numBlocks = view.getUint32(offset, true);
        offset += 4;
        // Item count
        const itemCount = view.getUint32(offset, true);
        offset += 4;
        // False positive rate
        const fpp = view.getFloat64(offset, true);
        offset += 8;
        // Create filter with known parameters
        const filter = new BloomFilter({
            numBlocks,
            falsePositiveRate: fpp,
        });
        // Load block data
        const blockData = data.slice(offset);
        const blocks = new Uint32Array(blockData.buffer, blockData.byteOffset, numBlocks * WORDS_PER_BLOCK);
        filter.blocks.set(blocks);
        filter.itemCount = itemCount;
        return filter;
    }
    /**
     * Create a bloom filter from raw block data
     *
     * @param blockData - Raw block data
     * @param itemCount - Number of items that were added
     * @param fpp - Target false positive rate
     * @returns BloomFilter instance
     */
    static fromRawData(blockData, itemCount = 0, fpp = DEFAULT_FPP) {
        const numBlocks = Math.floor(blockData.length / BLOCK_SIZE_BYTES);
        const filter = new BloomFilter({ numBlocks, falsePositiveRate: fpp });
        const blocks = new Uint32Array(blockData.buffer, blockData.byteOffset, numBlocks * WORDS_PER_BLOCK);
        filter.blocks.set(blocks);
        filter.itemCount = itemCount;
        return filter;
    }
}
/**
 * Writer for creating bloom filter files for Iceberg data files.
 *
 * Creates a bloom filter file containing filters for one or more columns.
 * The file format is:
 * - Header with metadata
 * - Array of per-column filter data
 */
export class BloomFilterWriter {
    _basePath;
    expectedItems;
    fpp;
    maxBytes;
    filters = new Map();
    constructor(options) {
        this._basePath = options.basePath;
        this.expectedItems = options.expectedItemsPerColumn ?? 10000;
        this.fpp = options.falsePositiveRate ?? DEFAULT_FPP;
        this.maxBytes = options.maxBytesPerFilter ?? DEFAULT_MAX_BYTES;
    }
    /**
     * Get the base path for bloom filter files
     */
    get basePath() {
        return this._basePath;
    }
    /**
     * Get or create a bloom filter for a column
     *
     * @param fieldId - Column field ID
     * @param columnName - Column name
     * @returns BloomFilter for the column
     */
    getOrCreateFilter(fieldId, columnName) {
        let entry = this.filters.get(fieldId);
        if (!entry) {
            entry = {
                fieldId,
                columnName,
                filter: new BloomFilter({
                    expectedItems: this.expectedItems,
                    falsePositiveRate: this.fpp,
                    maxBytes: this.maxBytes,
                }),
            };
            this.filters.set(fieldId, entry);
        }
        return entry.filter;
    }
    /**
     * Add a value to the filter for a column
     *
     * @param fieldId - Column field ID
     * @param columnName - Column name
     * @param value - Value to add
     */
    addValue(fieldId, columnName, value) {
        const filter = this.getOrCreateFilter(fieldId, columnName);
        filter.add(value);
    }
    /**
     * Add multiple values to the filter for a column
     *
     * @param fieldId - Column field ID
     * @param columnName - Column name
     * @param values - Values to add
     */
    addValues(fieldId, columnName, values) {
        const filter = this.getOrCreateFilter(fieldId, columnName);
        filter.addAll(values);
    }
    /**
     * Get the number of columns with filters
     */
    get columnCount() {
        return this.filters.size;
    }
    /**
     * Get the field IDs with filters
     */
    get fieldIds() {
        return Array.from(this.filters.keys());
    }
    /**
     * Finalize and serialize all filters
     *
     * Format:
     * - 5 bytes: Magic "BLOOM"
     * - 1 byte: Version
     * - 4 bytes: Number of filters
     * - For each filter:
     *   - 4 bytes: Field ID
     *   - 4 bytes: Column name length
     *   - N bytes: Column name (UTF-8)
     *   - 4 bytes: Filter data length
     *   - M bytes: Filter data (serialized BloomFilter)
     *
     * @returns Serialized bloom filter file data and metadata
     */
    finalize() {
        const encoder = new TextEncoder();
        const filterEntries = Array.from(this.filters.values());
        const metadata = [];
        // Calculate total size
        let totalSize = 5 + 1 + 4; // magic + version + count
        const serializedFilters = [];
        for (const entry of filterEntries) {
            const nameBytes = encoder.encode(entry.columnName);
            const filterData = entry.filter.serialize();
            serializedFilters.push({ entry, nameBytes, filterData });
            totalSize += 4 + 4 + nameBytes.length + 4 + filterData.length;
            metadata.push({
                fieldId: entry.fieldId,
                columnName: entry.columnName,
                numBlocks: entry.filter.blockCount,
                itemCount: entry.filter.count,
                falsePositiveRate: entry.filter.falsePositiveRate,
                algorithm: 'SPLIT_BLOCK',
                hashFunction: 'XXHASH64',
                createdAt: Date.now(),
            });
        }
        // Write data
        const buffer = new Uint8Array(totalSize);
        const view = new DataView(buffer.buffer);
        let offset = 0;
        // Magic
        buffer.set(BLOOM_FILTER_MAGIC, offset);
        offset += 5;
        // Version
        buffer[offset++] = FORMAT_VERSION;
        // Filter count
        view.setUint32(offset, filterEntries.length, true);
        offset += 4;
        // Each filter
        for (const { entry, nameBytes, filterData } of serializedFilters) {
            // Field ID
            view.setUint32(offset, entry.fieldId, true);
            offset += 4;
            // Column name length + data
            view.setUint32(offset, nameBytes.length, true);
            offset += 4;
            buffer.set(nameBytes, offset);
            offset += nameBytes.length;
            // Filter data length + data
            view.setUint32(offset, filterData.length, true);
            offset += 4;
            buffer.set(filterData, offset);
            offset += filterData.length;
        }
        return { data: buffer, metadata };
    }
    /**
     * Clear all filters
     */
    clear() {
        this.filters.clear();
    }
}
/**
 * Parse a bloom filter file
 *
 * @param data - Serialized bloom filter file data
 * @returns Array of parsed filter entries
 */
export function parseBloomFilterFile(data) {
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const decoder = new TextDecoder();
    let offset = 0;
    // Verify magic
    for (let i = 0; i < BLOOM_FILTER_MAGIC.length; i++) {
        if (data[offset + i] !== BLOOM_FILTER_MAGIC[i]) {
            throw new Error('Invalid bloom filter file magic bytes');
        }
    }
    offset += 5;
    // Version
    const version = data[offset++];
    if (version !== FORMAT_VERSION) {
        throw new Error(`Unsupported bloom filter file version: ${version}`);
    }
    // Filter count
    const filterCount = view.getUint32(offset, true);
    offset += 4;
    const entries = [];
    for (let i = 0; i < filterCount; i++) {
        // Field ID
        const fieldId = view.getUint32(offset, true);
        offset += 4;
        // Column name
        const nameLength = view.getUint32(offset, true);
        offset += 4;
        const columnName = decoder.decode(data.slice(offset, offset + nameLength));
        offset += nameLength;
        // Filter data
        const filterLength = view.getUint32(offset, true);
        offset += 4;
        const filterData = data.slice(offset, offset + filterLength);
        offset += filterLength;
        const filter = BloomFilter.deserialize(filterData);
        entries.push({ fieldId, columnName, filter });
    }
    return entries;
}
/**
 * Create a bloom filter lookup map from parsed entries
 *
 * @param entries - Parsed bloom filter entries
 * @returns Map from field ID to BloomFilter
 */
export function createBloomFilterMap(entries) {
    const map = new Map();
    for (const entry of entries) {
        map.set(entry.fieldId, entry.filter);
    }
    return map;
}
// ============================================================================
// Utility Functions
// ============================================================================
/**
 * Check if a value might exist in a data file based on bloom filter
 *
 * @param filter - Bloom filter for the column
 * @param value - Value to check
 * @returns true if value might exist (should not skip file), false if definitely doesn't exist (can skip)
 */
export function shouldReadFile(filter, value) {
    // If no filter, assume file might contain value
    if (!filter) {
        return true;
    }
    return filter.mightContain(value);
}
/**
 * Check if any of the values might exist in a data file
 *
 * @param filter - Bloom filter for the column
 * @param values - Values to check (for IN clause)
 * @returns true if any value might exist, false if none definitely exist
 */
export function shouldReadFileForAny(filter, values) {
    if (!filter) {
        return true;
    }
    for (const value of values) {
        if (filter.mightContain(value)) {
            return true;
        }
    }
    return false;
}
/**
 * Generate a bloom filter file path for a data file
 *
 * @param dataFilePath - Path to the data file
 * @returns Path for the bloom filter file
 */
export function getBloomFilterPath(dataFilePath) {
    // Replace file extension with .bloom
    const lastDot = dataFilePath.lastIndexOf('.');
    if (lastDot === -1) {
        return `${dataFilePath}.bloom`;
    }
    return `${dataFilePath.substring(0, lastDot)}.bloom`;
}
//# sourceMappingURL=index.js.map