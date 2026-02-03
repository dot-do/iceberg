/**
 * Deletion Vector Manifest Tracking
 *
 * Functions for tracking and managing deletion vectors in manifests.
 * Deletion vectors are a v3 feature that provide a more efficient way to
 * track deleted rows compared to traditional position delete files.
 *
 * @see https://iceberg.apache.org/spec/#deletion-vectors
 */
import type { DataFile, ManifestEntry } from './types.js';
/**
 * Options for creating a deletion vector entry.
 */
export interface CreateDeletionVectorOptions {
    /** Path to the Puffin file containing the deletion vector */
    filePath: string;
    /** File format (typically 'parquet' for the Puffin container) */
    fileFormat: 'parquet' | 'avro' | 'orc';
    /** Partition values */
    partition: Record<string, unknown>;
    /** Number of deleted row positions in the DV */
    recordCount: number;
    /** Size of the Puffin file in bytes */
    fileSizeInBytes: number;
    /** Byte offset of the DV blob within the Puffin file */
    contentOffset: number;
    /** Size of the DV blob in bytes */
    contentSizeInBytes: number;
    /** Path to the data file this DV references */
    referencedDataFile: string;
}
/**
 * Result of validating v3 deletion vector rules.
 */
export interface V3DeletionVectorValidationResult {
    /** Whether the validation passed */
    valid: boolean;
    /** List of validation error messages */
    errors: string[];
    /** List of warning messages */
    warnings: string[];
}
/**
 * Create a deletion vector DataFile entry.
 *
 * Deletion vectors are stored as position delete files (content=1) with
 * additional fields: content-offset, content-size-in-bytes, and referenced-data-file.
 *
 * @param options - Options for creating the deletion vector entry
 * @returns A DataFile representing the deletion vector
 */
export declare function createDeletionVectorEntry(options: CreateDeletionVectorOptions): DataFile;
/**
 * Find all deletion vectors that apply to a specific data file.
 *
 * Scans manifest entries to find DVs that reference the given data file path.
 * Only considers entries with content=1 (position deletes) that have all
 * required deletion vector fields.
 *
 * @param manifestEntries - Array of manifest entries to search
 * @param dataFilePath - Path to the data file to find DVs for
 * @returns Array of DataFile entries representing deletion vectors for the file
 */
export declare function findDeletionVectorsForFile(manifestEntries: ManifestEntry[], dataFilePath: string): DataFile[];
/**
 * Check if position delete files should be ignored for a data file because
 * a deletion vector exists for it.
 *
 * When a deletion vector exists for a data file, legacy position delete files
 * should be ignored in favor of the DV.
 *
 * @param manifestEntries - Array of manifest entries to search
 * @param dataFilePath - Path to the data file to check
 * @returns true if position deletes should be ignored (DV exists)
 */
export declare function shouldIgnorePositionDeletes(manifestEntries: ManifestEntry[], dataFilePath: string): boolean;
/**
 * Count the number of deletion vectors per data file for a specific snapshot.
 *
 * In Iceberg, at most one deletion vector is allowed per data file per snapshot.
 * This function counts DVs grouped by referenced data file for entries that
 * were added in the specified snapshot.
 *
 * @param manifestEntries - Array of manifest entries to analyze
 * @param snapshotId - Snapshot ID to filter by (counts ADDED entries for this snapshot)
 * @returns Map of data file path to DV count
 */
export declare function countDeletionVectorsPerDataFile(manifestEntries: ManifestEntry[], snapshotId: number): Map<string, number>;
/**
 * Validate deletion vector rules for v3 writers.
 *
 * V3 writer rules:
 * 1. v3 writers cannot add new position delete files (must use DVs)
 * 2. Existing position delete files should be merged into DVs
 * 3. At most one DV per data file per snapshot
 *
 * @param formatVersion - Table format version (2 or 3)
 * @param existingEntries - Existing manifest entries
 * @param newDeleteFiles - New delete files being added
 * @returns Validation result with errors and warnings
 */
export declare function validateV3DeletionVectorRules(formatVersion: number, existingEntries: ManifestEntry[], newDeleteFiles: DataFile[]): V3DeletionVectorValidationResult;
//# sourceMappingURL=deletion-vectors.d.ts.map