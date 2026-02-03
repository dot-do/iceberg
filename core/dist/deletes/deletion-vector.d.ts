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
export declare const DELETION_VECTOR_V1_BLOB_TYPE = "deletion-vector-v1";
/**
 * Puffin blob structure for deletion vectors.
 */
export interface PuffinBlob {
    /** Blob type identifier */
    type: string;
    /** Serialized blob data */
    data: Uint8Array;
    /** Size of the blob data in bytes */
    size: number;
    /** Path to the data file this deletion vector references */
    referencedDataFile: string;
    /** Additional properties */
    properties: Record<string, string>;
}
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
export declare class DeletionVector {
    /**
     * Internal storage: Map<key (high 32 bits), Set<sub-position (low 32 bits)>>
     */
    private containers;
    constructor();
    /**
     * Add a position to the deletion vector.
     *
     * @param position - The 64-bit row position to mark as deleted (must be non-negative)
     * @throws Error if position is negative
     */
    add(position: bigint): void;
    /**
     * Add multiple positions to the deletion vector.
     *
     * @param positions - Array of positions to add
     */
    addAll(positions: bigint[]): void;
    /**
     * Remove a position from the deletion vector.
     *
     * @param position - The position to remove
     */
    remove(position: bigint): void;
    /**
     * Check if a position is marked as deleted.
     *
     * @param position - The position to check
     * @returns true if the position is deleted
     */
    has(position: bigint): boolean;
    /**
     * Get the number of deleted positions.
     *
     * @returns The cardinality (count of deleted positions)
     */
    cardinality(): number;
    /**
     * Check if the deletion vector is empty.
     *
     * @returns true if no positions are deleted
     */
    isEmpty(): boolean;
    /**
     * Clear all deleted positions.
     */
    clear(): void;
    /**
     * Merge another deletion vector into this one.
     *
     * @param other - The deletion vector to merge from
     */
    merge(other: DeletionVector): void;
    /**
     * Create a copy of this deletion vector.
     *
     * @returns A new DeletionVector with the same positions
     */
    clone(): DeletionVector;
    /**
     * Iterate over all deleted positions.
     *
     * @yields Each deleted position
     */
    positions(): Generator<bigint>;
    /**
     * Get all deleted positions in sorted order.
     *
     * @returns Array of positions sorted in ascending order
     */
    sortedPositions(): bigint[];
    /**
     * Get internal container data for serialization.
     * @internal
     */
    getContainers(): Map<bigint, Set<number>>;
    /**
     * Set internal container data from deserialization.
     * @internal
     */
    setContainers(containers: Map<bigint, Set<number>>): void;
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
export declare function serializeDeletionVector(dv: DeletionVector): Uint8Array;
/**
 * Deserialize a deletion vector from binary format.
 *
 * @param data - Binary data to deserialize
 * @returns The deserialized deletion vector
 * @throws Error if the data is invalid or corrupted
 */
export declare function deserializeDeletionVector(data: Uint8Array): DeletionVector;
/**
 * Create a Puffin blob for a deletion vector.
 *
 * @param dv - The deletion vector to create a blob for
 * @param referencedDataFile - Path to the data file this DV references
 * @returns A PuffinBlob structure ready to be written to a Puffin file
 */
export declare function createDeletionVectorBlob(dv: DeletionVector, referencedDataFile: string): PuffinBlob;
/**
 * Merge multiple deletion vectors into a new one.
 *
 * @param dvs - Array of deletion vectors to merge
 * @returns A new deletion vector containing all positions from the input vectors
 */
export declare function mergeDeletionVectors(dvs: DeletionVector[]): DeletionVector;
//# sourceMappingURL=deletion-vector.d.ts.map