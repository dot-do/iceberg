/**
 * Deletion Vector Implementation
 *
 * Implements reading/writing deletion vectors using a Roaring bitmap-like structure
 * per the Iceberg deletion-vector-v1 spec.
 *
 * Deletion vectors track which row positions in a data file have been deleted.
 * Positions are 64-bit integers, where:
 * - High 32 bits form the "key" (container index)
 * - Low 32 bits form the "sub-position" within that container
 *
 * This implementation uses a Map<bigint, Set<number>> structure where:
 * - The map key is the high 32 bits of the position
 * - The set contains the low 32 bits (sub-positions) for that key
 *
 * @see https://iceberg.apache.org/spec/#deletion-vectors
 * @see https://roaringbitmap.org/
 */
/**
 * Blob type identifier for deletion vectors in Puffin files.
 */
export const DELETION_VECTOR_V1_BLOB_TYPE = 'deletion-vector-v1';
/**
 * Magic bytes for the deletion vector binary format.
 * Using 'DV01' as a simple marker.
 */
const MAGIC_BYTES = new Uint8Array([0x44, 0x56, 0x30, 0x31]); // 'DV01'
/**
 * Mask for extracting low 32 bits.
 */
const LOW_32_MASK = BigInt(0xffffffff);
/**
 * Number of bits to shift for high 32 bits.
 */
const HIGH_32_SHIFT = 32n;
/**
 * DeletionVector class for tracking deleted row positions.
 *
 * Uses a Roaring bitmap-like structure internally where positions are
 * organized by their high 32 bits (key) and low 32 bits (sub-position).
 *
 * @example
 * ```ts
 * const dv = new DeletionVector();
 * dv.add(42n);
 * dv.add(100n);
 * dv.add(BigInt(2 ** 33) + 5n); // Large 64-bit position
 *
 * console.log(dv.has(42n)); // true
 * console.log(dv.cardinality()); // 3
 * ```
 */
export class DeletionVector {
    /**
     * Internal storage: Map<key (high 32 bits), Set<sub-position (low 32 bits)>>
     */
    containers;
    constructor() {
        this.containers = new Map();
    }
    /**
     * Add a position to the deletion vector.
     *
     * @param position - The 64-bit row position to mark as deleted (must be non-negative)
     * @throws Error if position is negative
     */
    add(position) {
        if (position < 0n) {
            throw new Error('Position must be non-negative');
        }
        const key = position >> HIGH_32_SHIFT;
        const subPos = Number(position & LOW_32_MASK);
        let container = this.containers.get(key);
        if (!container) {
            container = new Set();
            this.containers.set(key, container);
        }
        container.add(subPos);
    }
    /**
     * Add multiple positions to the deletion vector.
     *
     * @param positions - Array of positions to add
     */
    addAll(positions) {
        for (const pos of positions) {
            this.add(pos);
        }
    }
    /**
     * Remove a position from the deletion vector.
     *
     * @param position - The position to remove
     */
    remove(position) {
        if (position < 0n) {
            return;
        }
        const key = position >> HIGH_32_SHIFT;
        const subPos = Number(position & LOW_32_MASK);
        const container = this.containers.get(key);
        if (container) {
            container.delete(subPos);
            if (container.size === 0) {
                this.containers.delete(key);
            }
        }
    }
    /**
     * Check if a position is marked as deleted.
     *
     * @param position - The position to check
     * @returns true if the position is deleted
     */
    has(position) {
        if (position < 0n) {
            return false;
        }
        const key = position >> HIGH_32_SHIFT;
        const subPos = Number(position & LOW_32_MASK);
        const container = this.containers.get(key);
        return container?.has(subPos) ?? false;
    }
    /**
     * Get the number of deleted positions.
     *
     * @returns The cardinality (count of deleted positions)
     */
    cardinality() {
        let count = 0;
        for (const container of this.containers.values()) {
            count += container.size;
        }
        return count;
    }
    /**
     * Check if the deletion vector is empty.
     *
     * @returns true if no positions are deleted
     */
    isEmpty() {
        return this.containers.size === 0;
    }
    /**
     * Clear all deleted positions.
     */
    clear() {
        this.containers.clear();
    }
    /**
     * Merge another deletion vector into this one.
     *
     * @param other - The deletion vector to merge from
     */
    merge(other) {
        for (const [key, otherContainer] of other.containers) {
            let container = this.containers.get(key);
            if (!container) {
                container = new Set();
                this.containers.set(key, container);
            }
            for (const subPos of otherContainer) {
                container.add(subPos);
            }
        }
    }
    /**
     * Create a copy of this deletion vector.
     *
     * @returns A new DeletionVector with the same positions
     */
    clone() {
        const copy = new DeletionVector();
        for (const [key, container] of this.containers) {
            copy.containers.set(key, new Set(container));
        }
        return copy;
    }
    /**
     * Iterate over all deleted positions.
     *
     * @yields Each deleted position
     */
    *positions() {
        for (const [key, container] of this.containers) {
            for (const subPos of container) {
                yield (key << HIGH_32_SHIFT) | BigInt(subPos);
            }
        }
    }
    /**
     * Get all deleted positions in sorted order.
     *
     * @returns Array of positions sorted in ascending order
     */
    sortedPositions() {
        const result = [];
        for (const pos of this.positions()) {
            result.push(pos);
        }
        return result.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    }
    /**
     * Get internal container data for serialization.
     * @internal
     */
    getContainers() {
        return this.containers;
    }
    /**
     * Set internal container data from deserialization.
     * @internal
     */
    setContainers(containers) {
        this.containers = containers;
    }
}
/**
 * Serialize a deletion vector to binary format.
 *
 * Binary format (deletion-vector-v1):
 * - 4 bytes: magic ('DV01')
 * - 4 bytes: version (1)
 * - 4 bytes: number of containers (N)
 * - For each container:
 *   - 8 bytes: key (high 32 bits as 64-bit integer)
 *   - 4 bytes: count of sub-positions (M)
 *   - M * 4 bytes: sub-positions (sorted 32-bit integers)
 *
 * @param dv - The deletion vector to serialize
 * @returns Binary representation of the deletion vector
 */
export function serializeDeletionVector(dv) {
    const containers = dv.getContainers();
    // Calculate total size
    let size = 4 + 4 + 4; // magic + version + container count
    for (const container of containers.values()) {
        size += 8 + 4 + container.size * 4; // key + count + positions
    }
    const buffer = new ArrayBuffer(size);
    const view = new DataView(buffer);
    const uint8 = new Uint8Array(buffer);
    let offset = 0;
    // Write magic bytes
    uint8.set(MAGIC_BYTES, offset);
    offset += 4;
    // Write version (1)
    view.setUint32(offset, 1, true); // little-endian
    offset += 4;
    // Write number of containers
    view.setUint32(offset, containers.size, true);
    offset += 4;
    // Sort keys for deterministic output
    const sortedKeys = Array.from(containers.keys()).sort((a, b) => a < b ? -1 : a > b ? 1 : 0);
    // Write each container
    for (const key of sortedKeys) {
        const container = containers.get(key);
        // Write key (8 bytes, little-endian)
        view.setBigUint64(offset, key, true);
        offset += 8;
        // Write count
        view.setUint32(offset, container.size, true);
        offset += 4;
        // Write sorted sub-positions
        const sortedSubPos = Array.from(container).sort((a, b) => a - b);
        for (const subPos of sortedSubPos) {
            view.setUint32(offset, subPos, true);
            offset += 4;
        }
    }
    return uint8;
}
/**
 * Deserialize a deletion vector from binary format.
 *
 * @param data - Binary data to deserialize
 * @returns The deserialized deletion vector
 * @throws Error if the data is invalid or corrupted
 */
export function deserializeDeletionVector(data) {
    if (data.length < 12) {
        throw new Error('Invalid deletion vector: data too short');
    }
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    let offset = 0;
    // Verify magic bytes
    for (let i = 0; i < 4; i++) {
        if (data[offset + i] !== MAGIC_BYTES[i]) {
            throw new Error('Invalid deletion vector: bad magic bytes');
        }
    }
    offset += 4;
    // Read version
    const version = view.getUint32(offset, true);
    if (version !== 1) {
        throw new Error(`Invalid deletion vector: unsupported version ${version}`);
    }
    offset += 4;
    // Read number of containers
    const containerCount = view.getUint32(offset, true);
    offset += 4;
    const dv = new DeletionVector();
    const containers = new Map();
    // Read each container
    for (let i = 0; i < containerCount; i++) {
        if (offset + 12 > data.length) {
            throw new Error('Invalid deletion vector: truncated container header');
        }
        // Read key
        const key = view.getBigUint64(offset, true);
        offset += 8;
        // Read count
        const count = view.getUint32(offset, true);
        offset += 4;
        if (offset + count * 4 > data.length) {
            throw new Error('Invalid deletion vector: truncated container data');
        }
        // Read sub-positions
        const container = new Set();
        for (let j = 0; j < count; j++) {
            const subPos = view.getUint32(offset, true);
            container.add(subPos);
            offset += 4;
        }
        containers.set(key, container);
    }
    dv.setContainers(containers);
    return dv;
}
/**
 * Create a Puffin blob for a deletion vector.
 *
 * @param dv - The deletion vector to create a blob for
 * @param referencedDataFile - Path to the data file this DV references
 * @returns A PuffinBlob structure ready to be written to a Puffin file
 */
export function createDeletionVectorBlob(dv, referencedDataFile) {
    const data = serializeDeletionVector(dv);
    return {
        type: DELETION_VECTOR_V1_BLOB_TYPE,
        data,
        size: data.byteLength,
        referencedDataFile,
        properties: {
            'referenced-data-file': referencedDataFile,
        },
    };
}
/**
 * Merge multiple deletion vectors into a new one.
 *
 * @param dvs - Array of deletion vectors to merge
 * @returns A new deletion vector containing all positions from the input vectors
 */
export function mergeDeletionVectors(dvs) {
    const result = new DeletionVector();
    for (const dv of dvs) {
        result.merge(dv);
    }
    return result;
}
//# sourceMappingURL=deletion-vector.js.map