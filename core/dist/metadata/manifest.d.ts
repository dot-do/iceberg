/**
 * Iceberg Manifest File Handling
 *
 * Generates manifest files and manifest lists for tracking data files.
 * Supports both JSON (for testing) and Avro (production) formats.
 *
 * @see https://iceberg.apache.org/spec/
 */
import type { DataFile, ManifestEntry, ManifestFile, ManifestEntryStatus, IcebergSchema, PartitionSpec, PartitionFieldSummary } from './types.js';
import type { ComputedFileStats } from './column-stats.js';
/**
 * Generates Iceberg manifest files from data file metadata.
 *
 * Manifests track individual data files along with their partition values,
 * file statistics, and status (added/existing/deleted).
 */
export declare class ManifestGenerator {
    private entries;
    private readonly sequenceNumber;
    private readonly snapshotId;
    constructor(options: {
        sequenceNumber: number;
        snapshotId: number;
    });
    /**
     * Add a data file to the manifest.
     */
    addDataFile(file: Omit<DataFile, 'content'>, status?: ManifestEntryStatus): void;
    /**
     * Add a data file with pre-computed column statistics.
     * This method applies encoded stats directly to the data file entry.
     */
    addDataFileWithStats(file: Omit<DataFile, 'content'>, stats: ComputedFileStats, status?: ManifestEntryStatus): void;
    /**
     * Add a position delete file to the manifest.
     */
    addPositionDeleteFile(file: Omit<DataFile, 'content'>, status?: ManifestEntryStatus): void;
    /**
     * Add an equality delete file to the manifest.
     */
    addEqualityDeleteFile(file: Omit<DataFile, 'content'>, equalityFieldIds: number[], status?: ManifestEntryStatus): void;
    /**
     * Generate the manifest content as JSON (simplified format).
     * Note: In production, this would be Avro-encoded.
     */
    generate(): {
        entries: ManifestEntry[];
        summary: {
            addedFiles: number;
            existingFiles: number;
            deletedFiles: number;
            addedRows: number;
            existingRows: number;
            deletedRows: number;
        };
    };
    /**
     * Get all entries.
     */
    getEntries(): ManifestEntry[];
    /**
     * Serialize the manifest to JSON (for testing/debugging).
     * Production would use Avro encoding.
     */
    toJSON(): string;
    /**
     * Get the number of entries in the manifest.
     */
    get entryCount(): number;
}
/**
 * Generates manifest list files that index multiple manifests.
 *
 * The manifest list contains references to all manifest files in a snapshot,
 * along with aggregated statistics for efficient manifest pruning.
 */
export declare class ManifestListGenerator {
    private manifests;
    private readonly snapshotId;
    private readonly sequenceNumber;
    constructor(options: {
        snapshotId: number;
        sequenceNumber: number;
    });
    /**
     * Add a manifest file reference to the list.
     */
    addManifest(manifest: Omit<ManifestFile, 'added-snapshot-id' | 'sequence-number' | 'min-sequence-number'>): void;
    /**
     * Add a manifest file with computed statistics.
     */
    addManifestWithStats(path: string, length: number, partitionSpecId: number, summary: {
        addedFiles: number;
        existingFiles: number;
        deletedFiles: number;
        addedRows: number;
        existingRows: number;
        deletedRows: number;
    }, isDeleteManifest?: boolean, partitionSummaries?: PartitionFieldSummary[]): void;
    /**
     * Generate the manifest list content.
     */
    generate(): ManifestFile[];
    /**
     * Get all manifests.
     */
    getManifests(): ManifestFile[];
    /**
     * Serialize to JSON (for testing/debugging).
     */
    toJSON(): string;
    /**
     * Get total counts across all manifests.
     */
    getTotals(): {
        totalFiles: number;
        totalRows: number;
        addedFiles: number;
        deletedFiles: number;
    };
    /**
     * Get the number of manifests in the list.
     */
    get manifestCount(): number;
}
export interface ManifestWriterOptions {
    /** Table schema */
    schema: IcebergSchema;
    /** Partition specification */
    partitionSpec: PartitionSpec;
    /** Schema ID */
    schemaId?: number;
    /** Format version (default: 2) */
    formatVersion?: number;
    /** Content type: 0 for data, 1 for deletes */
    content?: number;
}
export interface ManifestListWriterOptions {
    /** Snapshot ID */
    snapshotId: number;
    /** Parent snapshot ID (optional) */
    parentSnapshotId?: number;
    /** Sequence number */
    sequenceNumber: number;
    /** Format version */
    formatVersion?: number;
}
/**
 * Create data file statistics from raw values.
 */
export declare function createDataFileStats(schema: IcebergSchema, stats: {
    columnSizes?: Record<number, number>;
    valueCounts?: Record<number, number>;
    nullValueCounts?: Record<number, number>;
    nanValueCounts?: Record<number, number>;
    lowerBounds?: Record<number, unknown>;
    upperBounds?: Record<number, unknown>;
}, encodeStatValue: (value: unknown, type: string) => Uint8Array): {
    'column-sizes'?: Record<number, number>;
    'value-counts'?: Record<number, number>;
    'null-value-counts'?: Record<number, number>;
    'nan-value-counts'?: Record<number, number>;
    'lower-bounds'?: Record<number, Uint8Array>;
    'upper-bounds'?: Record<number, Uint8Array>;
};
//# sourceMappingURL=manifest.d.ts.map