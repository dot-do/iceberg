/**
 * Iceberg Manifest File Handling
 *
 * Generates manifest files and manifest lists for tracking data files.
 * Supports both JSON (for testing) and Avro (production) formats.
 *
 * @see https://iceberg.apache.org/spec/
 */

import type {
  DataFile,
  ManifestEntry,
  ManifestFile,
  ManifestEntryStatus,
  IcebergSchema,
  PartitionSpec,
  PartitionFieldSummary,
} from './types.js';
import type { ComputedFileStats } from './column-stats.js';

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
  private entries: ManifestEntry[] = [];
  private readonly sequenceNumber: number;
  private readonly snapshotId: number;

  constructor(options: { sequenceNumber: number; snapshotId: number }) {
    this.sequenceNumber = options.sequenceNumber;
    this.snapshotId = options.snapshotId;
  }

  /**
   * Add a data file to the manifest.
   */
  addDataFile(
    file: Omit<DataFile, 'content'>,
    status: ManifestEntryStatus = 1 // Default to ADDED
  ): void {
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
  addDataFileWithStats(
    file: Omit<DataFile, 'content'>,
    stats: ComputedFileStats,
    status: ManifestEntryStatus = 1
  ): void {
    // Build the data file with statistics applied using spread operator
    const dataFile: DataFile = {
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
  addPositionDeleteFile(
    file: Omit<DataFile, 'content'>,
    status: ManifestEntryStatus = 1
  ): void {
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
  addEqualityDeleteFile(
    file: Omit<DataFile, 'content'>,
    equalityFieldIds: number[],
    status: ManifestEntryStatus = 1
  ): void {
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
  } {
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
  getEntries(): ManifestEntry[] {
    return this.entries;
  }

  /**
   * Serialize the manifest to JSON (for testing/debugging).
   * Production would use Avro encoding.
   */
  toJSON(): string {
    return JSON.stringify(this.generate(), null, 2);
  }

  /**
   * Get the number of entries in the manifest.
   */
  get entryCount(): number {
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
  private manifests: ManifestFile[] = [];
  private readonly snapshotId: number;
  private readonly sequenceNumber: number;

  constructor(options: { snapshotId: number; sequenceNumber: number }) {
    this.snapshotId = options.snapshotId;
    this.sequenceNumber = options.sequenceNumber;
  }

  /**
   * Add a manifest file reference to the list.
   */
  addManifest(manifest: Omit<ManifestFile, 'added-snapshot-id' | 'sequence-number' | 'min-sequence-number'>): void {
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
  addManifestWithStats(
    path: string,
    length: number,
    partitionSpecId: number,
    summary: {
      addedFiles: number;
      existingFiles: number;
      deletedFiles: number;
      addedRows: number;
      existingRows: number;
      deletedRows: number;
    },
    isDeleteManifest: boolean = false,
    partitionSummaries?: PartitionFieldSummary[]
  ): void {
    const manifest: ManifestFile = {
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
  generate(): ManifestFile[] {
    return this.manifests;
  }

  /**
   * Get all manifests.
   */
  getManifests(): ManifestFile[] {
    return this.manifests;
  }

  /**
   * Serialize to JSON (for testing/debugging).
   */
  toJSON(): string {
    return JSON.stringify(this.manifests, null, 2);
  }

  /**
   * Get total counts across all manifests.
   */
  getTotals(): {
    totalFiles: number;
    totalRows: number;
    addedFiles: number;
    deletedFiles: number;
  } {
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
  get manifestCount(): number {
    return this.manifests.length;
  }
}

// ============================================================================
// Manifest Writer Options
// ============================================================================

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

// ============================================================================
// Data File Statistics Helper
// ============================================================================

/**
 * Create data file statistics from raw values.
 */
export function createDataFileStats(
  schema: IcebergSchema,
  stats: {
    columnSizes?: Record<number, number>;
    valueCounts?: Record<number, number>;
    nullValueCounts?: Record<number, number>;
    nanValueCounts?: Record<number, number>;
    lowerBounds?: Record<number, unknown>;
    upperBounds?: Record<number, unknown>;
  },
  encodeStatValue: (value: unknown, type: string) => Uint8Array
): {
  'column-sizes'?: Record<number, number>;
  'value-counts'?: Record<number, number>;
  'null-value-counts'?: Record<number, number>;
  'nan-value-counts'?: Record<number, number>;
  'lower-bounds'?: Record<number, Uint8Array>;
  'upper-bounds'?: Record<number, Uint8Array>;
} {
  const result: {
    'column-sizes'?: Record<number, number>;
    'value-counts'?: Record<number, number>;
    'null-value-counts'?: Record<number, number>;
    'nan-value-counts'?: Record<number, number>;
    'lower-bounds'?: Record<number, Uint8Array>;
    'upper-bounds'?: Record<number, Uint8Array>;
  } = {};

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
    const encoded: Record<number, Uint8Array> = {};
    for (const [fieldIdStr, value] of Object.entries(stats.lowerBounds)) {
      const fieldId = Number(fieldIdStr);
      const field = schema.fields.find((f) => f.id === fieldId);
      const type = field ? (typeof field.type === 'string' ? field.type : 'binary') : 'binary';
      encoded[fieldId] = encodeStatValue(value, type);
    }
    result['lower-bounds'] = encoded;
  }

  if (stats.upperBounds) {
    const encoded: Record<number, Uint8Array> = {};
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
