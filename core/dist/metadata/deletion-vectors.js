/**
 * Deletion Vector Manifest Tracking
 *
 * Functions for tracking and managing deletion vectors in manifests.
 * Deletion vectors are a v3 feature that provide a more efficient way to
 * track deleted rows compared to traditional position delete files.
 *
 * @see https://iceberg.apache.org/spec/#deletion-vectors
 */
import { isDeletionVector } from './types.js';
import { CONTENT_POSITION_DELETES } from './constants.js';
// ============================================================================
// Deletion Vector Entry Creation
// ============================================================================
/**
 * Create a deletion vector DataFile entry.
 *
 * Deletion vectors are stored as position delete files (content=1) with
 * additional fields: content-offset, content-size-in-bytes, and referenced-data-file.
 *
 * @param options - Options for creating the deletion vector entry
 * @returns A DataFile representing the deletion vector
 */
export function createDeletionVectorEntry(options) {
    return {
        content: CONTENT_POSITION_DELETES,
        'file-path': options.filePath,
        'file-format': options.fileFormat,
        partition: options.partition,
        'record-count': options.recordCount,
        'file-size-in-bytes': options.fileSizeInBytes,
        'content-offset': options.contentOffset,
        'content-size-in-bytes': options.contentSizeInBytes,
        'referenced-data-file': options.referencedDataFile,
    };
}
// ============================================================================
// Scan Planning - Finding DVs for Data Files
// ============================================================================
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
export function findDeletionVectorsForFile(manifestEntries, dataFilePath) {
    const dvs = [];
    for (const entry of manifestEntries) {
        const dataFile = entry['data-file'];
        // Only consider position delete files (content=1)
        if (dataFile.content !== CONTENT_POSITION_DELETES) {
            continue;
        }
        // Check if this is a deletion vector (has all DV fields)
        if (!isDeletionVector(dataFile)) {
            continue;
        }
        // Check if this DV references the target data file (exact match)
        if (dataFile['referenced-data-file'] === dataFilePath) {
            dvs.push(dataFile);
        }
    }
    return dvs;
}
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
export function shouldIgnorePositionDeletes(manifestEntries, dataFilePath) {
    const dvs = findDeletionVectorsForFile(manifestEntries, dataFilePath);
    return dvs.length > 0;
}
// ============================================================================
// V3 Writer Rules Validation
// ============================================================================
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
export function countDeletionVectorsPerDataFile(manifestEntries, snapshotId) {
    const counts = new Map();
    for (const entry of manifestEntries) {
        const dataFile = entry['data-file'];
        // Only count DVs added in the specified snapshot
        if (entry['snapshot-id'] !== snapshotId) {
            continue;
        }
        // Only count ADDED entries (status=1)
        if (entry.status !== 1) {
            continue;
        }
        // Only consider deletion vectors
        if (!isDeletionVector(dataFile)) {
            continue;
        }
        const referencedFile = dataFile['referenced-data-file'];
        const currentCount = counts.get(referencedFile) || 0;
        counts.set(referencedFile, currentCount + 1);
    }
    return counts;
}
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
export function validateV3DeletionVectorRules(formatVersion, existingEntries, newDeleteFiles) {
    const errors = [];
    const warnings = [];
    // Rule: v2 tables don't have DV restrictions
    if (formatVersion < 3) {
        return { valid: true, errors: [], warnings: [] };
    }
    // Rule 1: v3 writers cannot add new position delete files without DV fields
    for (const file of newDeleteFiles) {
        if (file.content === CONTENT_POSITION_DELETES && !isDeletionVector(file)) {
            errors.push('v3 writers cannot add new position delete files without deletion vector fields');
        }
    }
    // Rule 2: Check for existing position delete files that should be merged
    for (const entry of existingEntries) {
        const dataFile = entry['data-file'];
        if (dataFile.content === CONTENT_POSITION_DELETES &&
            !isDeletionVector(dataFile) &&
            entry.status === 0 // EXISTING
        ) {
            if (!warnings.includes('existing position delete files should be merged into deletion vectors')) {
                warnings.push('existing position delete files should be merged into deletion vectors');
            }
        }
    }
    // Rule 3: At most one DV per data file per snapshot
    // Get all unique snapshot IDs from entries with ADDED status
    const snapshotIds = new Set();
    for (const entry of existingEntries) {
        if (entry.status === 1) {
            snapshotIds.add(entry['snapshot-id']);
        }
    }
    for (const snapshotId of snapshotIds) {
        const dvCounts = countDeletionVectorsPerDataFile(existingEntries, snapshotId);
        for (const [_dataFilePath, count] of dvCounts) {
            if (count > 1) {
                errors.push('at most one deletion vector per data file per snapshot is allowed');
                break; // Only add this error once
            }
        }
    }
    return {
        valid: errors.length === 0,
        errors,
        warnings,
    };
}
//# sourceMappingURL=deletion-vectors.js.map