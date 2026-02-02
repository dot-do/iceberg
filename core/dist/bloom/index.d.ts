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
/**
 * Bloom filter configuration options
 */
export interface BloomFilterOptions {
    /** Expected number of distinct values */
    expectedItems?: number;
    /** Target false positive probability (0 < fpp < 1) */
    falsePositiveRate?: number;
    /** Maximum filter size in bytes */
    maxBytes?: number;
    /** Number of blocks (overrides calculated value if provided) */
    numBlocks?: number;
}
/**
 * Bloom filter metadata stored alongside the filter data
 */
export interface BloomFilterMetadata {
    /** Column field ID this filter applies to */
    fieldId: number;
    /** Column name */
    columnName: string;
    /** Number of blocks in the filter */
    numBlocks: number;
    /** Number of items added to the filter */
    itemCount: number;
    /** Target false positive rate */
    falsePositiveRate: number;
    /** Algorithm identifier: 'SPLIT_BLOCK' */
    algorithm: 'SPLIT_BLOCK';
    /** Hash function: 'XXHASH64' */
    hashFunction: 'XXHASH64';
    /** Creation timestamp */
    createdAt: number;
}
/**
 * Serialized bloom filter structure
 */
export interface SerializedBloomFilter {
    metadata: BloomFilterMetadata;
    data: Uint8Array;
}
/**
 * Bloom filter file reference for data files
 */
export interface BloomFilterFileRef {
    /** Path to the bloom filter file */
    path: string;
    /** Size in bytes */
    sizeInBytes: number;
    /** Field IDs covered by this bloom filter file */
    fieldIds: number[];
}
/**
 * XXH64 hash function implementation
 *
 * @param data - Input data to hash
 * @param seed - Optional seed value (default: 0)
 * @returns 64-bit hash as BigInt
 */
export declare function xxh64(data: Uint8Array, seed?: bigint): bigint;
/**
 * Hash a string value using XXH64
 */
export declare function xxh64String(value: string, seed?: bigint): bigint;
/**
 * Hash a number value using XXH64
 */
export declare function xxh64Number(value: number, seed?: bigint): bigint;
/**
 * Hash a BigInt value using XXH64
 */
export declare function xxh64BigInt(value: bigint, seed?: bigint): bigint;
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
export declare function calculateOptimalBlocks(expectedItems: number, fpp?: number, maxBytes?: number): number;
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
export declare function estimateFalsePositiveRate(numBlocks: number, itemCount: number): number;
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
export declare class BloomFilter {
    private blocks;
    private readonly numBlocks;
    private itemCount;
    private readonly fpp;
    /**
     * Create a new BloomFilter
     *
     * @param options - Filter configuration options
     */
    constructor(options?: BloomFilterOptions);
    /**
     * Get the number of blocks in the filter
     */
    get blockCount(): number;
    /**
     * Get the number of items added to the filter
     */
    get count(): number;
    /**
     * Get the target false positive rate
     */
    get falsePositiveRate(): number;
    /**
     * Get estimated actual false positive rate based on fill ratio
     */
    get estimatedFalsePositiveRate(): number;
    /**
     * Get size of the filter in bytes
     */
    get sizeInBytes(): number;
    /**
     * Compute block index and word masks for a hash value
     */
    private computeBlockMask;
    /**
     * Add a value to the bloom filter
     *
     * @param value - Value to add (will be hashed)
     */
    add(value: string | number | bigint | Uint8Array): void;
    /**
     * Add multiple values to the bloom filter
     *
     * @param values - Values to add
     */
    addAll(values: Iterable<string | number | bigint | Uint8Array>): void;
    /**
     * Check if a value might be in the set
     *
     * @param value - Value to check
     * @returns true if the value might be present, false if definitely not present
     */
    mightContain(value: string | number | bigint | Uint8Array): boolean;
    /**
     * Hash a value to a 64-bit integer
     */
    private hashValue;
    /**
     * Clear the bloom filter
     */
    clear(): void;
    /**
     * Merge another bloom filter into this one (union)
     * Both filters must have the same number of blocks.
     *
     * @param other - Other bloom filter to merge
     */
    merge(other: BloomFilter): void;
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
    serialize(): Uint8Array;
    /**
     * Get the raw block data for low-level access
     *
     * @returns Raw block data as Uint8Array
     */
    getRawData(): Uint8Array;
    /**
     * Create a bloom filter from serialized bytes
     *
     * @param data - Serialized filter data
     * @returns Deserialized bloom filter
     */
    static deserialize(data: Uint8Array): BloomFilter;
    /**
     * Create a bloom filter from raw block data
     *
     * @param blockData - Raw block data
     * @param itemCount - Number of items that were added
     * @param fpp - Target false positive rate
     * @returns BloomFilter instance
     */
    static fromRawData(blockData: Uint8Array, itemCount?: number, fpp?: number): BloomFilter;
}
/**
 * Options for BloomFilterWriter
 */
export interface BloomFilterWriterOptions {
    /** Base path for bloom filter files */
    basePath: string;
    /** Expected items per column (default: 10000) */
    expectedItemsPerColumn?: number;
    /** Target false positive rate (default: 0.01) */
    falsePositiveRate?: number;
    /** Maximum bytes per filter (default: 1MB) */
    maxBytesPerFilter?: number;
}
/**
 * Writer for creating bloom filter files for Iceberg data files.
 *
 * Creates a bloom filter file containing filters for one or more columns.
 * The file format is:
 * - Header with metadata
 * - Array of per-column filter data
 */
export declare class BloomFilterWriter {
    private readonly _basePath;
    private readonly expectedItems;
    private readonly fpp;
    private readonly maxBytes;
    private filters;
    constructor(options: BloomFilterWriterOptions);
    /**
     * Get the base path for bloom filter files
     */
    get basePath(): string;
    /**
     * Get or create a bloom filter for a column
     *
     * @param fieldId - Column field ID
     * @param columnName - Column name
     * @returns BloomFilter for the column
     */
    getOrCreateFilter(fieldId: number, columnName: string): BloomFilter;
    /**
     * Add a value to the filter for a column
     *
     * @param fieldId - Column field ID
     * @param columnName - Column name
     * @param value - Value to add
     */
    addValue(fieldId: number, columnName: string, value: string | number | bigint | Uint8Array): void;
    /**
     * Add multiple values to the filter for a column
     *
     * @param fieldId - Column field ID
     * @param columnName - Column name
     * @param values - Values to add
     */
    addValues(fieldId: number, columnName: string, values: Iterable<string | number | bigint | Uint8Array>): void;
    /**
     * Get the number of columns with filters
     */
    get columnCount(): number;
    /**
     * Get the field IDs with filters
     */
    get fieldIds(): number[];
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
    finalize(): {
        data: Uint8Array;
        metadata: BloomFilterMetadata[];
    };
    /**
     * Clear all filters
     */
    clear(): void;
}
/**
 * Parsed bloom filter file entry
 */
export interface ParsedBloomFilterEntry {
    fieldId: number;
    columnName: string;
    filter: BloomFilter;
}
/**
 * Parse a bloom filter file
 *
 * @param data - Serialized bloom filter file data
 * @returns Array of parsed filter entries
 */
export declare function parseBloomFilterFile(data: Uint8Array): ParsedBloomFilterEntry[];
/**
 * Create a bloom filter lookup map from parsed entries
 *
 * @param entries - Parsed bloom filter entries
 * @returns Map from field ID to BloomFilter
 */
export declare function createBloomFilterMap(entries: ParsedBloomFilterEntry[]): Map<number, BloomFilter>;
/**
 * Check if a value might exist in a data file based on bloom filter
 *
 * @param filter - Bloom filter for the column
 * @param value - Value to check
 * @returns true if value might exist (should not skip file), false if definitely doesn't exist (can skip)
 */
export declare function shouldReadFile(filter: BloomFilter | undefined, value: string | number | bigint | Uint8Array): boolean;
/**
 * Check if any of the values might exist in a data file
 *
 * @param filter - Bloom filter for the column
 * @param values - Values to check (for IN clause)
 * @returns true if any value might exist, false if none definitely exist
 */
export declare function shouldReadFileForAny(filter: BloomFilter | undefined, values: Iterable<string | number | bigint | Uint8Array>): boolean;
/**
 * Generate a bloom filter file path for a data file
 *
 * @param dataFilePath - Path to the data file
 * @returns Path for the bloom filter file
 */
export declare function getBloomFilterPath(dataFilePath: string): string;
//# sourceMappingURL=index.d.ts.map