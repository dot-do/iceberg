/**
 * Iceberg Manifest File Handling
 *
 * Generates manifest files and manifest lists for tracking data files.
 * Supports both JSON (for testing) and Avro (production) formats.
 *
 * @see https://iceberg.apache.org/spec/
 */
// ============================================================================
// Manifest Generator
// ============================================================================
/**
 * Generates Iceberg manifest files from data file metadata.
 *
 * Manifests track individual data files along with their partition values,
 * file statistics, and status (added/existing/deleted).
 */
export class ManifestGenerator {
    entries = [];
    sequenceNumber;
    snapshotId;
    constructor(options) {
        this.sequenceNumber = options.sequenceNumber;
        this.snapshotId = options.snapshotId;
    }
    /**
     * Add a data file to the manifest.
     */
    addDataFile(file, status = 1 // Default to ADDED
    ) {
        // Validate status is 0, 1, or 2
        if (status !== 0 && status !== 1 && status !== 2) {
            throw new Error('Manifest entry status must be 0 (EXISTING), 1 (ADDED), or 2 (DELETED)');
        }
        this.entries.push({
            status,
            'snapshot-id': this.snapshotId,
            'sequence-number': this.sequenceNumber,
            'file-sequence-number': this.sequenceNumber,
            'data-file': {
                content: 0, // data file
                ...file,
            },
        });
    }
    /**
     * Add a data file with pre-computed column statistics.
     * This method applies encoded stats directly to the data file entry.
     */
    addDataFileWithStats(file, stats, status = 1) {
        // Build the data file with statistics applied using spread operator
        const dataFile = {
            content: 0,
            ...file,
            ...(Object.keys(stats.valueCounts).length > 0 && { 'value-counts': stats.valueCounts }),
            ...(Object.keys(stats.nullValueCounts).length > 0 && { 'null-value-counts': stats.nullValueCounts }),
            ...(Object.keys(stats.nanValueCounts).length > 0 && { 'nan-value-counts': stats.nanValueCounts }),
            ...(Object.keys(stats.columnSizes).length > 0 && { 'column-sizes': stats.columnSizes }),
            ...(Object.keys(stats.lowerBounds).length > 0 && { 'lower-bounds': stats.lowerBounds }),
            ...(Object.keys(stats.upperBounds).length > 0 && { 'upper-bounds': stats.upperBounds }),
        };
        this.entries.push({
            status,
            'snapshot-id': this.snapshotId,
            'sequence-number': this.sequenceNumber,
            'file-sequence-number': this.sequenceNumber,
            'data-file': dataFile,
        });
    }
    /**
     * Add a position delete file to the manifest.
     */
    addPositionDeleteFile(file, status = 1) {
        this.entries.push({
            status,
            'snapshot-id': this.snapshotId,
            'sequence-number': this.sequenceNumber,
            'file-sequence-number': this.sequenceNumber,
            'data-file': {
                content: 1, // position deletes
                ...file,
            },
        });
    }
    /**
     * Add an equality delete file to the manifest.
     */
    addEqualityDeleteFile(file, equalityFieldIds, status = 1) {
        this.entries.push({
            status,
            'snapshot-id': this.snapshotId,
            'sequence-number': this.sequenceNumber,
            'file-sequence-number': this.sequenceNumber,
            'data-file': {
                content: 2, // equality deletes
                ...file,
                'equality-ids': equalityFieldIds,
            },
        });
    }
    /**
     * Generate the manifest content as JSON (simplified format).
     * Note: In production, this would be Avro-encoded.
     */
    generate() {
        let addedFiles = 0;
        let existingFiles = 0;
        let deletedFiles = 0;
        let addedRows = 0;
        let existingRows = 0;
        let deletedRows = 0;
        for (const entry of this.entries) {
            const records = entry['data-file']['record-count'];
            switch (entry.status) {
                case 0: // EXISTING
                    existingFiles++;
                    existingRows += records;
                    break;
                case 1: // ADDED
                    addedFiles++;
                    addedRows += records;
                    break;
                case 2: // DELETED
                    deletedFiles++;
                    deletedRows += records;
                    break;
            }
        }
        return {
            entries: this.entries,
            summary: {
                addedFiles,
                existingFiles,
                deletedFiles,
                addedRows,
                existingRows,
                deletedRows,
            },
        };
    }
    /**
     * Get all entries.
     */
    getEntries() {
        return this.entries;
    }
    /**
     * Serialize the manifest to JSON (for testing/debugging).
     * Production would use Avro encoding.
     */
    toJSON() {
        return JSON.stringify(this.generate(), null, 2);
    }
    /**
     * Get the number of entries in the manifest.
     */
    get entryCount() {
        return this.entries.length;
    }
}
// ============================================================================
// Manifest List Generator
// ============================================================================
/**
 * Generates manifest list files that index multiple manifests.
 *
 * The manifest list contains references to all manifest files in a snapshot,
 * along with aggregated statistics for efficient manifest pruning.
 */
export class ManifestListGenerator {
    manifests = [];
    snapshotId;
    sequenceNumber;
    constructor(options) {
        this.snapshotId = options.snapshotId;
        this.sequenceNumber = options.sequenceNumber;
    }
    /**
     * Add a manifest file reference to the list.
     */
    addManifest(manifest) {
        this.manifests.push({
            'added-snapshot-id': this.snapshotId,
            'sequence-number': this.sequenceNumber,
            'min-sequence-number': this.sequenceNumber,
            ...manifest,
        });
    }
    /**
     * Add a manifest file with computed statistics.
     */
    addManifestWithStats(path, length, partitionSpecId, summary, isDeleteManifest = false, partitionSummaries) {
        const manifest = {
            'manifest-path': path,
            'manifest-length': length,
            'partition-spec-id': partitionSpecId,
            content: isDeleteManifest ? 1 : 0,
            'sequence-number': this.sequenceNumber,
            'min-sequence-number': this.sequenceNumber,
            'added-snapshot-id': this.snapshotId,
            'added-files-count': summary.addedFiles,
            'existing-files-count': summary.existingFiles,
            'deleted-files-count': summary.deletedFiles,
            'added-rows-count': summary.addedRows,
            'existing-rows-count': summary.existingRows,
            'deleted-rows-count': summary.deletedRows,
            ...(partitionSummaries && partitionSummaries.length > 0 && { partitions: partitionSummaries }),
        };
        this.manifests.push(manifest);
    }
    /**
     * Generate the manifest list content.
     */
    generate() {
        return this.manifests;
    }
    /**
     * Get all manifests.
     */
    getManifests() {
        return this.manifests;
    }
    /**
     * Serialize to JSON (for testing/debugging).
     */
    toJSON() {
        return JSON.stringify(this.manifests, null, 2);
    }
    /**
     * Get total counts across all manifests.
     */
    getTotals() {
        let totalFiles = 0;
        let totalRows = 0;
        let addedFiles = 0;
        let deletedFiles = 0;
        for (const manifest of this.manifests) {
            totalFiles += manifest['added-files-count'] + manifest['existing-files-count'];
            totalRows += manifest['added-rows-count'] + manifest['existing-rows-count'];
            addedFiles += manifest['added-files-count'];
            deletedFiles += manifest['deleted-files-count'];
        }
        return { totalFiles, totalRows, addedFiles, deletedFiles };
    }
    /**
     * Get the number of manifests in the list.
     */
    get manifestCount() {
        return this.manifests.length;
    }
}
// ============================================================================
// Data File Statistics Helper
// ============================================================================
/**
 * Create data file statistics from raw values.
 */
export function createDataFileStats(schema, stats, encodeStatValue) {
    const result = {};
    if (stats.columnSizes) {
        result['column-sizes'] = stats.columnSizes;
    }
    if (stats.valueCounts) {
        result['value-counts'] = stats.valueCounts;
    }
    if (stats.nullValueCounts) {
        result['null-value-counts'] = stats.nullValueCounts;
    }
    if (stats.nanValueCounts) {
        result['nan-value-counts'] = stats.nanValueCounts;
    }
    if (stats.lowerBounds) {
        const encoded = {};
        for (const [fieldIdStr, value] of Object.entries(stats.lowerBounds)) {
            const fieldId = Number(fieldIdStr);
            const field = schema.fields.find((f) => f.id === fieldId);
            const type = field ? (typeof field.type === 'string' ? field.type : 'binary') : 'binary';
            encoded[fieldId] = encodeStatValue(value, type);
        }
        result['lower-bounds'] = encoded;
    }
    if (stats.upperBounds) {
        const encoded = {};
        for (const [fieldIdStr, value] of Object.entries(stats.upperBounds)) {
            const fieldId = Number(fieldIdStr);
            const field = schema.fields.find((f) => f.id === fieldId);
            const type = field ? (typeof field.type === 'string' ? field.type : 'binary') : 'binary';
            encoded[fieldId] = encodeStatValue(value, type);
        }
        result['upper-bounds'] = encoded;
    }
    return result;
}
//# sourceMappingURL=manifest.js.map