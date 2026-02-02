/**
 * Iceberg Row-Level Delete Support
 *
 * This module implements row-level deletes per the Apache Iceberg v2 specification.
 * Iceberg supports two types of delete files:
 *
 * 1. Position Deletes (content = 1): Delete rows by file path and row position
 *    - Fixed schema: file_path (string), pos (long)
 *    - More efficient for targeted deletes with known positions
 *    - Rows are identified by the data file path and 0-indexed row position
 *
 * 2. Equality Deletes (content = 2): Delete rows by matching column values
 *    - Schema defined by equality field IDs
 *    - More flexible, can delete rows across multiple files
 *    - Uses equality predicates on specified columns
 *
 * Delete files are tracked in manifests with content type 1 (deletes).
 * Sequence numbers determine delete ordering - deletes apply to data files
 * with sequence numbers less than or equal to the delete's sequence number.
 *
 * @see https://iceberg.apache.org/spec/#row-level-deletes
 */

import type {
  DataFile,
  ManifestEntry,
  ManifestEntryStatus,
  IcebergSchema,
  IcebergStructField,
  FileFormat,
} from './types.js';
import { generateUUID } from './snapshot.js';
import {
  CONTENT_DATA,
  CONTENT_POSITION_DELETES,
  CONTENT_EQUALITY_DELETES,
  MANIFEST_CONTENT_DATA,
  MANIFEST_CONTENT_DELETES,
  POSITION_DELETE_FILE_PATH_FIELD_ID,
  POSITION_DELETE_POS_FIELD_ID,
  POSITION_DELETE_SCHEMA_ID,
  EQUALITY_DELETE_SCHEMA_ID,
} from './constants.js';

// Re-export content type constants for backward compatibility
export {
  CONTENT_DATA,
  CONTENT_POSITION_DELETES,
  CONTENT_EQUALITY_DELETES,
  MANIFEST_CONTENT_DATA,
  MANIFEST_CONTENT_DELETES,
};

// ============================================================================
// Types
// ============================================================================

/** Position delete entry - identifies a row by file path and position */
export interface PositionDelete {
  /** Path to the data file containing the row to delete */
  filePath: string;
  /** Zero-based position of the row within the data file */
  pos: number;
}

/** Equality delete entry - identifies rows by column values */
export interface EqualityDelete {
  /** Column values that identify rows to delete (field name -> value) */
  values: Record<string, unknown>;
}

/**
 * Delete file extending DataFile with delete-specific fields.
 * The content field distinguishes between position (1) and equality (2) deletes.
 */
export interface DeleteFile extends DataFile {
  /** Content type: 1 for position deletes, 2 for equality deletes */
  content: typeof CONTENT_POSITION_DELETES | typeof CONTENT_EQUALITY_DELETES;
  /** Equality field IDs (required for equality deletes) */
  'equality-ids'?: number[];
}

/** Position delete file metadata */
export interface PositionDeleteFile extends DeleteFile {
  content: typeof CONTENT_POSITION_DELETES;
  /** Lower bound of referenced file paths */
  'referenced-data-file'?: string;
}

/** Equality delete file metadata */
export interface EqualityDeleteFile extends DeleteFile {
  content: typeof CONTENT_EQUALITY_DELETES;
  /** Field IDs that define equality conditions */
  'equality-ids': number[];
}

/** Configuration for position delete builder */
export interface PositionDeleteBuilderOptions {
  /** Sequence number for this delete file */
  sequenceNumber: number;
  /** Snapshot ID that will contain this delete */
  snapshotId: number;
  /** File format for the delete file */
  fileFormat?: FileFormat;
  /** Output path prefix */
  outputPrefix?: string;
  /** Partition values (if partitioned) */
  partition?: Record<string, unknown>;
}

/** Configuration for equality delete builder */
export interface EqualityDeleteBuilderOptions {
  /** Table schema */
  schema: IcebergSchema;
  /** Field IDs that define equality conditions */
  equalityFieldIds: number[];
  /** Sequence number for this delete file */
  sequenceNumber: number;
  /** Snapshot ID that will contain this delete */
  snapshotId: number;
  /** File format for the delete file */
  fileFormat?: FileFormat;
  /** Output path prefix */
  outputPrefix?: string;
  /** Partition values (if partitioned) */
  partition?: Record<string, unknown>;
}

/** Result of building a delete file */
export interface DeleteFileResult<T extends DeleteFile> {
  /** The generated delete file metadata */
  deleteFile: T;
  /** The serialized file data (to be written to storage) */
  data: Uint8Array;
  /** Statistics about the delete file */
  statistics: {
    /** Number of delete entries */
    recordCount: number;
    /** File size in bytes */
    fileSizeBytes: number;
  };
}

/** Position delete statistics */
export interface PositionDeleteStatistics {
  /** Number of position delete entries */
  recordCount: number;
  /** File size in bytes */
  fileSizeBytes: number;
  /** Number of unique data files referenced */
  uniqueFilesCount: number;
  /** Lower bound file path */
  lowerBoundFilePath?: string;
  /** Upper bound file path */
  upperBoundFilePath?: string;
}

/** Equality delete statistics */
export interface EqualityDeleteStatistics {
  /** Number of equality delete entries */
  recordCount: number;
  /** File size in bytes */
  fileSizeBytes: number;
  /** Lower bounds per field ID */
  lowerBounds: Record<number, unknown>;
  /** Upper bounds per field ID */
  upperBounds: Record<number, unknown>;
  /** Null count per field ID */
  nullCounts: Record<number, number>;
}

/** Delete application result */
export interface DeleteApplicationResult {
  /** Number of rows that passed (not deleted) */
  passedRows: number;
  /** Number of rows deleted by position deletes */
  positionDeletedRows: number;
  /** Number of rows deleted by equality deletes */
  equalityDeletedRows: number;
}

// ============================================================================
// Position Delete Schema (Fixed per Iceberg spec)
// ============================================================================

/**
 * Iceberg position delete schema.
 * Position delete files have a fixed schema with two required columns.
 */
export const POSITION_DELETE_SCHEMA: IcebergSchema = {
  'schema-id': POSITION_DELETE_SCHEMA_ID,
  type: 'struct',
  fields: [
    {
      id: POSITION_DELETE_FILE_PATH_FIELD_ID,
      name: 'file_path',
      required: true,
      type: 'string',
      doc: 'Path of the data file containing the row to delete',
    },
    {
      id: POSITION_DELETE_POS_FIELD_ID,
      name: 'pos',
      required: true,
      type: 'long',
      doc: 'Ordinal position of the row to delete (0-indexed)',
    },
  ],
};

// ============================================================================
// PositionDeleteBuilder
// ============================================================================

/**
 * Builder for creating position delete files.
 *
 * Position delete files identify rows to delete by their file path and
 * 0-indexed position within that file. This is the most efficient delete
 * method when row positions are known.
 *
 * @example
 * ```typescript
 * const builder = new PositionDeleteBuilder({
 *   sequenceNumber: 5,
 *   snapshotId: 1234567890,
 * });
 *
 * // Add deletes for specific rows
 * builder.addDelete('data/file1.parquet', 42);
 * builder.addDelete('data/file1.parquet', 100);
 * builder.addDelete('data/file2.parquet', 5);
 *
 * // Build the delete file
 * const result = builder.build();
 * // Write result.data to storage at result.deleteFile['file-path']
 * ```
 */
export class PositionDeleteBuilder {
  private readonly options: Required<PositionDeleteBuilderOptions>;
  private readonly entries: PositionDelete[] = [];
  private built = false;

  constructor(options: PositionDeleteBuilderOptions) {
    this.options = {
      fileFormat: 'parquet',
      outputPrefix: 'data/delete/',
      partition: {},
      ...options,
    };
  }

  /**
   * Add a position delete entry.
   *
   * @param filePath - Path to the data file containing the row to delete
   * @param pos - Zero-based position of the row within the file
   * @throws Error if pos is negative or if builder has already been built
   */
  addDelete(filePath: string, pos: number): this {
    this.ensureNotBuilt();

    if (pos < 0) {
      throw new Error(`Position must be non-negative, got: ${pos}`);
    }

    if (!filePath || typeof filePath !== 'string') {
      throw new Error('File path must be a non-empty string');
    }

    this.entries.push({ filePath, pos });
    return this;
  }

  /**
   * Add multiple position deletes for the same file.
   *
   * @param filePath - Path to the data file
   * @param positions - Array of row positions to delete
   */
  addDeletesForFile(filePath: string, positions: number[]): this {
    for (const pos of positions) {
      this.addDelete(filePath, pos);
    }
    return this;
  }

  /**
   * Add multiple position delete entries.
   *
   * @param entries - Array of position delete entries
   */
  addDeletes(entries: PositionDelete[]): this {
    for (const entry of entries) {
      this.addDelete(entry.filePath, entry.pos);
    }
    return this;
  }

  /**
   * Get the current number of entries.
   */
  getEntryCount(): number {
    return this.entries.length;
  }

  /**
   * Check if the builder has any entries.
   */
  hasEntries(): boolean {
    return this.entries.length > 0;
  }

  /**
   * Build the position delete file.
   *
   * @returns The delete file metadata, serialized data, and statistics
   */
  build(): DeleteFileResult<PositionDeleteFile> & { statistics: PositionDeleteStatistics } {
    this.ensureNotBuilt();
    this.built = true;

    // Sort entries by file path and position for efficient read-side application
    const sortedEntries = [...this.entries].sort((a, b) => {
      const pathCompare = a.filePath.localeCompare(b.filePath);
      if (pathCompare !== 0) return pathCompare;
      return a.pos - b.pos;
    });

    // Serialize to JSON format (production would use Parquet/Avro)
    const data = this.serializeEntries(sortedEntries);

    // Compute statistics
    const uniqueFiles = new Set(sortedEntries.map((e) => e.filePath));
    const sortedFilePaths = Array.from(uniqueFiles).sort();

    // Generate unique file path
    const uniqueId = generateUUID();
    const filePath = `${this.options.outputPrefix}position-delete-${this.options.snapshotId}-${uniqueId}.${this.options.fileFormat}`;

    // Build delete file metadata
    const deleteFile: PositionDeleteFile = {
      content: CONTENT_POSITION_DELETES,
      'file-path': filePath,
      'file-format': this.options.fileFormat,
      partition: this.options.partition,
      'record-count': sortedEntries.length,
      'file-size-in-bytes': data.byteLength,
    };

    // Add referenced data file bounds for optimization
    if (sortedFilePaths.length > 0) {
      deleteFile['referenced-data-file'] = sortedFilePaths[0];
    }

    const statistics: PositionDeleteStatistics = {
      recordCount: sortedEntries.length,
      fileSizeBytes: data.byteLength,
      uniqueFilesCount: uniqueFiles.size,
      lowerBoundFilePath: sortedFilePaths[0],
      upperBoundFilePath: sortedFilePaths[sortedFilePaths.length - 1],
    };

    return {
      deleteFile,
      data,
      statistics,
    };
  }

  /**
   * Serialize position delete entries.
   * In production, this would write Parquet format.
   */
  private serializeEntries(entries: PositionDelete[]): Uint8Array {
    // JSON serialization for now - production would use Parquet
    const json = JSON.stringify({
      schema: 'position_delete',
      entries: entries.map((e) => ({
        file_path: e.filePath,
        pos: e.pos,
      })),
    });
    return new TextEncoder().encode(json);
  }

  private ensureNotBuilt(): void {
    if (this.built) {
      throw new Error('PositionDeleteBuilder has already been built');
    }
  }
}

// ============================================================================
// EqualityDeleteBuilder
// ============================================================================

/**
 * Builder for creating equality delete files.
 *
 * Equality delete files identify rows to delete by matching column values.
 * Any row that matches all the specified column values will be deleted.
 * This is more flexible than position deletes but requires scanning.
 *
 * @example
 * ```typescript
 * const builder = new EqualityDeleteBuilder({
 *   schema: tableSchema,
 *   equalityFieldIds: [1, 2], // Delete by 'id' and 'name' columns
 *   sequenceNumber: 5,
 *   snapshotId: 1234567890,
 * });
 *
 * // Add deletes for specific values
 * builder.addDelete({ id: '123', name: 'Alice' });
 * builder.addDelete({ id: '456', name: 'Bob' });
 *
 * // Build the delete file
 * const result = builder.build();
 * ```
 */
export class EqualityDeleteBuilder {
  private readonly options: Required<EqualityDeleteBuilderOptions>;
  private readonly entries: EqualityDelete[] = [];
  private readonly equalityFields: Array<{ id: number; name: string; type: string }>;
  private built = false;

  constructor(options: EqualityDeleteBuilderOptions) {
    this.options = {
      fileFormat: 'parquet',
      outputPrefix: 'data/delete/',
      partition: {},
      ...options,
    };

    // Resolve field IDs to field metadata
    this.equalityFields = this.resolveEqualityFields(
      options.schema,
      options.equalityFieldIds
    );
  }

  /**
   * Resolve field IDs to field names and types.
   */
  private resolveEqualityFields(
    schema: IcebergSchema,
    fieldIds: number[]
  ): Array<{ id: number; name: string; type: string }> {
    const fields: Array<{ id: number; name: string; type: string }> = [];
    const fieldMap = new Map<number, IcebergStructField>();

    // Build field map from schema
    for (const field of schema.fields) {
      fieldMap.set(field.id, field);
    }

    // Resolve each field ID
    for (const fieldId of fieldIds) {
      const field = fieldMap.get(fieldId);
      if (!field) {
        throw new Error(`Field ID ${fieldId} not found in schema`);
      }
      fields.push({
        id: field.id,
        name: field.name,
        type: typeof field.type === 'string' ? field.type : 'struct',
      });
    }

    return fields;
  }

  /**
   * Add an equality delete entry.
   *
   * @param values - Column values that identify rows to delete
   * @throws Error if required equality fields are missing
   */
  addDelete(values: Record<string, unknown>): this {
    this.ensureNotBuilt();

    // Validate that all equality fields are present
    for (const field of this.equalityFields) {
      if (!(field.name in values)) {
        throw new Error(`Missing required equality field: ${field.name}`);
      }
    }

    // Store only the equality field values
    const filteredValues: Record<string, unknown> = {};
    for (const field of this.equalityFields) {
      filteredValues[field.name] = values[field.name];
    }

    this.entries.push({ values: filteredValues });
    return this;
  }

  /**
   * Add multiple equality delete entries.
   *
   * @param entries - Array of value records to delete
   */
  addDeletes(entries: Array<Record<string, unknown>>): this {
    for (const entry of entries) {
      this.addDelete(entry);
    }
    return this;
  }

  /**
   * Get the current number of entries.
   */
  getEntryCount(): number {
    return this.entries.length;
  }

  /**
   * Get the equality field IDs.
   */
  getEqualityFieldIds(): number[] {
    return [...this.options.equalityFieldIds];
  }

  /**
   * Get the equality field names.
   */
  getEqualityFieldNames(): string[] {
    return this.equalityFields.map((f) => f.name);
  }

  /**
   * Check if the builder has any entries.
   */
  hasEntries(): boolean {
    return this.entries.length > 0;
  }

  /**
   * Build the equality delete file.
   *
   * @returns The delete file metadata, serialized data, and statistics
   */
  build(): DeleteFileResult<EqualityDeleteFile> & { statistics: EqualityDeleteStatistics } {
    this.ensureNotBuilt();
    this.built = true;

    // Serialize entries
    const data = this.serializeEntries(this.entries);

    // Compute statistics (bounds and null counts)
    const statistics = this.computeStatistics(data.byteLength);

    // Generate unique file path
    const uniqueId = generateUUID();
    const filePath = `${this.options.outputPrefix}equality-delete-${this.options.snapshotId}-${uniqueId}.${this.options.fileFormat}`;

    // Build delete file metadata
    // Build delete file with bounds included if computed
    const deleteFile: EqualityDeleteFile = {
      content: CONTENT_EQUALITY_DELETES,
      'file-path': filePath,
      'file-format': this.options.fileFormat,
      partition: this.options.partition,
      'record-count': this.entries.length,
      'file-size-in-bytes': data.byteLength,
      'equality-ids': this.options.equalityFieldIds,
      ...(Object.keys(statistics.lowerBounds).length > 0 && {
        'lower-bounds': this.encodeBounds(statistics.lowerBounds),
        'upper-bounds': this.encodeBounds(statistics.upperBounds),
      }),
    };

    return {
      deleteFile,
      data,
      statistics,
    };
  }

  /**
   * Compute statistics for equality delete entries.
   */
  private computeStatistics(fileSizeBytes: number): EqualityDeleteStatistics {
    const lowerBounds: Record<number, unknown> = {};
    const upperBounds: Record<number, unknown> = {};
    const nullCounts: Record<number, number> = {};

    for (const field of this.equalityFields) {
      nullCounts[field.id] = 0;
      const values: unknown[] = [];

      for (const entry of this.entries) {
        const value = entry.values[field.name];
        if (value === null || value === undefined) {
          nullCounts[field.id]++;
        } else {
          values.push(value);
        }
      }

      if (values.length > 0) {
        if (typeof values[0] === 'number') {
          const numValues = values as number[];
          lowerBounds[field.id] = Math.min(...numValues);
          upperBounds[field.id] = Math.max(...numValues);
        } else if (typeof values[0] === 'string') {
          const strValues = (values as string[]).sort();
          lowerBounds[field.id] = strValues[0];
          upperBounds[field.id] = strValues[strValues.length - 1];
        } else if (typeof values[0] === 'bigint') {
          const bigValues = values as bigint[];
          lowerBounds[field.id] = bigValues.reduce((a, b) => (a < b ? a : b));
          upperBounds[field.id] = bigValues.reduce((a, b) => (a > b ? a : b));
        }
      }
    }

    return {
      recordCount: this.entries.length,
      fileSizeBytes,
      lowerBounds,
      upperBounds,
      nullCounts,
    };
  }

  /**
   * Encode bounds to binary format for DataFile.
   */
  private encodeBounds(bounds: Record<number, unknown>): Record<number, Uint8Array | string> {
    const encoded: Record<number, string> = {};
    for (const [fieldId, value] of Object.entries(bounds)) {
      if (value !== undefined && value !== null) {
        encoded[Number(fieldId)] = String(value);
      }
    }
    return encoded;
  }

  /**
   * Serialize equality delete entries.
   * In production, this would write Parquet format.
   */
  private serializeEntries(entries: EqualityDelete[]): Uint8Array {
    // JSON serialization for now - production would use Parquet
    const json = JSON.stringify({
      schema: 'equality_delete',
      equalityFieldIds: this.options.equalityFieldIds,
      fieldNames: this.equalityFields.map((f) => f.name),
      entries: entries.map((e) => e.values),
    });
    return new TextEncoder().encode(json);
  }

  private ensureNotBuilt(): void {
    if (this.built) {
      throw new Error('EqualityDeleteBuilder has already been built');
    }
  }
}

// ============================================================================
// Delete Manifest Generator
// ============================================================================

/**
 * Generates delete manifest files.
 *
 * Delete manifests track position and equality delete files, separate from
 * data file manifests. They use content type 1 in the manifest list.
 */
export class DeleteManifestGenerator {
  private entries: ManifestEntry[] = [];
  private readonly sequenceNumber: number;
  private readonly snapshotId: number;

  constructor(options: { sequenceNumber: number; snapshotId: number }) {
    this.sequenceNumber = options.sequenceNumber;
    this.snapshotId = options.snapshotId;
  }

  /**
   * Add a position delete file to the manifest.
   */
  addPositionDeleteFile(
    file: Omit<PositionDeleteFile, 'content'>,
    status: ManifestEntryStatus = 1
  ): this {
    this.entries.push({
      status,
      'snapshot-id': this.snapshotId,
      'sequence-number': this.sequenceNumber,
      'file-sequence-number': this.sequenceNumber,
      'data-file': {
        content: CONTENT_POSITION_DELETES,
        ...file,
      } as DataFile,
    });
    return this;
  }

  /**
   * Add an equality delete file to the manifest.
   */
  addEqualityDeleteFile(
    file: Omit<EqualityDeleteFile, 'content'>,
    status: ManifestEntryStatus = 1
  ): this {
    this.entries.push({
      status,
      'snapshot-id': this.snapshotId,
      'sequence-number': this.sequenceNumber,
      'file-sequence-number': this.sequenceNumber,
      'data-file': {
        content: CONTENT_EQUALITY_DELETES,
        ...file,
      } as DataFile,
    });
    return this;
  }

  /**
   * Add a generic delete file to the manifest.
   */
  addDeleteFile(file: DeleteFile, status: ManifestEntryStatus = 1): this {
    this.entries.push({
      status,
      'snapshot-id': this.snapshotId,
      'sequence-number': this.sequenceNumber,
      'file-sequence-number': this.sequenceNumber,
      'data-file': file,
    });
    return this;
  }

  /**
   * Generate the manifest content.
   */
  generate(): {
    entries: ManifestEntry[];
    summary: {
      addedDeleteFiles: number;
      existingDeleteFiles: number;
      removedDeleteFiles: number;
      addedDeleteRows: number;
      existingDeleteRows: number;
      removedDeleteRows: number;
      positionDeleteFiles: number;
      equalityDeleteFiles: number;
    };
  } {
    let addedDeleteFiles = 0;
    let existingDeleteFiles = 0;
    let removedDeleteFiles = 0;
    let addedDeleteRows = 0;
    let existingDeleteRows = 0;
    let removedDeleteRows = 0;
    let positionDeleteFiles = 0;
    let equalityDeleteFiles = 0;

    for (const entry of this.entries) {
      const recordCount = entry['data-file']['record-count'];
      const content = entry['data-file'].content;

      if (content === CONTENT_POSITION_DELETES) {
        positionDeleteFiles++;
      } else if (content === CONTENT_EQUALITY_DELETES) {
        equalityDeleteFiles++;
      }

      switch (entry.status) {
        case 0: // EXISTING
          existingDeleteFiles++;
          existingDeleteRows += recordCount;
          break;
        case 1: // ADDED
          addedDeleteFiles++;
          addedDeleteRows += recordCount;
          break;
        case 2: // DELETED
          removedDeleteFiles++;
          removedDeleteRows += recordCount;
          break;
      }
    }

    return {
      entries: this.entries,
      summary: {
        addedDeleteFiles,
        existingDeleteFiles,
        removedDeleteFiles,
        addedDeleteRows,
        existingDeleteRows,
        removedDeleteRows,
        positionDeleteFiles,
        equalityDeleteFiles,
      },
    };
  }

  /**
   * Get the entries.
   */
  getEntries(): ManifestEntry[] {
    return this.entries;
  }

  /**
   * Get entry count.
   */
  get entryCount(): number {
    return this.entries.length;
  }

  /**
   * Serialize to JSON.
   */
  toJSON(): string {
    return JSON.stringify(this.generate(), null, 2);
  }
}

// ============================================================================
// Delete Application Utilities
// ============================================================================

/**
 * Position delete lookup for efficient application.
 * Maps file path -> set of positions to delete.
 */
export class PositionDeleteLookup {
  private readonly lookup = new Map<string, Set<number>>();
  private readonly sequenceNumber: number;

  constructor(sequenceNumber: number) {
    this.sequenceNumber = sequenceNumber;
  }

  /**
   * Add position deletes from a delete file.
   */
  addDeletes(filePath: string, positions: number[]): void {
    let posSet = this.lookup.get(filePath);
    if (!posSet) {
      posSet = new Set();
      this.lookup.set(filePath, posSet);
    }
    for (const pos of positions) {
      posSet.add(pos);
    }
  }

  /**
   * Check if a row should be deleted.
   */
  isDeleted(filePath: string, position: number): boolean {
    const positions = this.lookup.get(filePath);
    return positions?.has(position) ?? false;
  }

  /**
   * Get all deleted positions for a file.
   */
  getDeletedPositions(filePath: string): Set<number> | undefined {
    return this.lookup.get(filePath);
  }

  /**
   * Get all files with deletes.
   */
  getFilesWithDeletes(): string[] {
    return Array.from(this.lookup.keys());
  }

  /**
   * Get total number of position deletes.
   */
  getTotalDeleteCount(): number {
    let count = 0;
    for (const positions of this.lookup.values()) {
      count += positions.size;
    }
    return count;
  }

  /**
   * Get the sequence number for these deletes.
   */
  getSequenceNumber(): number {
    return this.sequenceNumber;
  }
}

/**
 * Equality delete lookup for efficient application.
 * Uses JSON-serialized values as keys for deduplication.
 */
export class EqualityDeleteLookup {
  private readonly lookup = new Set<string>();
  private readonly fieldNames: string[];
  private readonly sequenceNumber: number;

  constructor(fieldNames: string[], sequenceNumber: number) {
    this.fieldNames = fieldNames;
    this.sequenceNumber = sequenceNumber;
  }

  /**
   * Add an equality delete entry.
   */
  addDelete(values: Record<string, unknown>): void {
    const key = this.makeKey(values);
    this.lookup.add(key);
  }

  /**
   * Add multiple equality delete entries.
   */
  addDeletes(entries: Array<Record<string, unknown>>): void {
    for (const entry of entries) {
      this.addDelete(entry);
    }
  }

  /**
   * Check if a row should be deleted.
   */
  isDeleted(rowValues: Record<string, unknown>): boolean {
    const key = this.makeKey(rowValues);
    return this.lookup.has(key);
  }

  /**
   * Make a lookup key from values.
   */
  private makeKey(values: Record<string, unknown>): string {
    const keyValues: Record<string, unknown> = {};
    for (const fieldName of this.fieldNames) {
      keyValues[fieldName] = values[fieldName];
    }
    return JSON.stringify(keyValues);
  }

  /**
   * Get the number of unique delete entries.
   */
  getDeleteCount(): number {
    return this.lookup.size;
  }

  /**
   * Get the field names for this lookup.
   */
  getFieldNames(): string[] {
    return [...this.fieldNames];
  }

  /**
   * Get the sequence number for these deletes.
   */
  getSequenceNumber(): number {
    return this.sequenceNumber;
  }
}

// ============================================================================
// Delete Merger
// ============================================================================

/**
 * Configuration for delete merger.
 */
export interface DeleteMergerOptions {
  /** Table schema */
  schema: IcebergSchema;
  /** Maximum entries per merged file */
  maxEntriesPerFile?: number;
  /** Target file size in bytes */
  targetFileSizeBytes?: number;
}

/**
 * Merges multiple delete files for compaction.
 *
 * Over time, tables can accumulate many small delete files. This class
 * helps merge them into larger, more efficient delete files while
 * deduplicating entries.
 */
export class DeleteMerger {
  private readonly options: Required<DeleteMergerOptions>;
  private readonly positionDeletes = new Map<string, Set<number>>();
  private readonly equalityDeletes = new Map<string, Set<string>>(); // fieldIds -> JSON values
  private readonly equalityFieldIdSets = new Map<string, number[]>();

  constructor(options: DeleteMergerOptions) {
    this.options = {
      maxEntriesPerFile: 1_000_000,
      targetFileSizeBytes: 128 * 1024 * 1024, // 128MB
      ...options,
    };
  }

  /**
   * Add position delete entries to merge.
   */
  addPositionDeletes(entries: PositionDelete[]): void {
    for (const entry of entries) {
      let positions = this.positionDeletes.get(entry.filePath);
      if (!positions) {
        positions = new Set();
        this.positionDeletes.set(entry.filePath, positions);
      }
      positions.add(entry.pos);
    }
  }

  /**
   * Add equality delete entries to merge.
   */
  addEqualityDeletes(
    entries: Array<Record<string, unknown>>,
    equalityFieldIds: number[]
  ): void {
    const key = [...equalityFieldIds].sort().join(',');

    if (!this.equalityDeletes.has(key)) {
      this.equalityDeletes.set(key, new Set());
      this.equalityFieldIdSets.set(key, equalityFieldIds);
    }

    const deleteSet = this.equalityDeletes.get(key)!;
    for (const entry of entries) {
      deleteSet.add(JSON.stringify(entry));
    }
  }

  /**
   * Get count of unique position deletes.
   */
  getPositionDeleteCount(): number {
    let count = 0;
    for (const positions of this.positionDeletes.values()) {
      count += positions.size;
    }
    return count;
  }

  /**
   * Get count of unique equality deletes.
   */
  getEqualityDeleteCount(): number {
    let count = 0;
    for (const deletes of this.equalityDeletes.values()) {
      count += deletes.size;
    }
    return count;
  }

  /**
   * Merge and build delete files.
   */
  merge(
    snapshotId: number,
    sequenceNumber: number
  ): {
    positionDeleteFiles: Array<DeleteFileResult<PositionDeleteFile> & { statistics: PositionDeleteStatistics }>;
    equalityDeleteFiles: Array<DeleteFileResult<EqualityDeleteFile> & { statistics: EqualityDeleteStatistics }>;
    statistics: {
      inputPositionDeleteCount: number;
      inputEqualityDeleteCount: number;
      outputPositionDeleteFiles: number;
      outputEqualityDeleteFiles: number;
    };
  } {
    const positionResults: Array<DeleteFileResult<PositionDeleteFile> & { statistics: PositionDeleteStatistics }> = [];
    const equalityResults: Array<DeleteFileResult<EqualityDeleteFile> & { statistics: EqualityDeleteStatistics }> = [];

    // Merge position deletes
    if (this.positionDeletes.size > 0) {
      const allEntries: PositionDelete[] = [];
      for (const [filePath, positions] of this.positionDeletes) {
        for (const pos of positions) {
          allEntries.push({ filePath, pos });
        }
      }

      // Sort and chunk
      allEntries.sort((a, b) => {
        const pathCmp = a.filePath.localeCompare(b.filePath);
        return pathCmp !== 0 ? pathCmp : a.pos - b.pos;
      });

      const chunks = this.chunkEntries(allEntries);
      for (const chunk of chunks) {
        const builder = new PositionDeleteBuilder({ sequenceNumber, snapshotId });
        builder.addDeletes(chunk);
        positionResults.push(builder.build());
      }
    }

    // Merge equality deletes
    for (const [key, deleteSet] of this.equalityDeletes) {
      const fieldIds = this.equalityFieldIdSets.get(key)!;
      const entries = Array.from(deleteSet).map((json) => JSON.parse(json) as Record<string, unknown>);

      const chunks = this.chunkEntries(entries);
      for (const chunk of chunks) {
        const builder = new EqualityDeleteBuilder({
          schema: this.options.schema,
          equalityFieldIds: fieldIds,
          sequenceNumber,
          snapshotId,
        });
        builder.addDeletes(chunk);
        equalityResults.push(builder.build());
      }
    }

    return {
      positionDeleteFiles: positionResults,
      equalityDeleteFiles: equalityResults,
      statistics: {
        inputPositionDeleteCount: this.getPositionDeleteCount(),
        inputEqualityDeleteCount: this.getEqualityDeleteCount(),
        outputPositionDeleteFiles: positionResults.length,
        outputEqualityDeleteFiles: equalityResults.length,
      },
    };
  }

  /**
   * Clear all accumulated deletes.
   */
  clear(): void {
    this.positionDeletes.clear();
    this.equalityDeletes.clear();
    this.equalityFieldIdSets.clear();
  }

  /**
   * Chunk entries into groups of maxEntriesPerFile.
   */
  private chunkEntries<T>(entries: T[]): T[][] {
    const maxPerChunk = this.options.maxEntriesPerFile;
    if (entries.length <= maxPerChunk) {
      return [entries];
    }

    const chunks: T[][] = [];
    for (let i = 0; i < entries.length; i += maxPerChunk) {
      chunks.push(entries.slice(i, i + maxPerChunk));
    }
    return chunks;
  }
}

// ============================================================================
// Delete File Reader (Parsing)
// ============================================================================

/**
 * Parse position delete file content.
 * Expects JSON format from PositionDeleteBuilder.
 */
export function parsePositionDeleteFile(data: Uint8Array): PositionDelete[] {
  const text = new TextDecoder().decode(data);
  const parsed = JSON.parse(text) as {
    schema: string;
    entries: Array<{ file_path: string; pos: number }>;
  };

  if (parsed.schema !== 'position_delete') {
    throw new Error(`Invalid position delete file schema: ${parsed.schema}`);
  }

  return parsed.entries.map((e) => ({
    filePath: e.file_path,
    pos: e.pos,
  }));
}

/**
 * Parse equality delete file content.
 * Expects JSON format from EqualityDeleteBuilder.
 */
export function parseEqualityDeleteFile(data: Uint8Array): {
  equalityFieldIds: number[];
  fieldNames: string[];
  entries: Array<Record<string, unknown>>;
} {
  const text = new TextDecoder().decode(data);
  const parsed = JSON.parse(text) as {
    schema: string;
    equalityFieldIds: number[];
    fieldNames: string[];
    entries: Array<Record<string, unknown>>;
  };

  if (parsed.schema !== 'equality_delete') {
    throw new Error(`Invalid equality delete file schema: ${parsed.schema}`);
  }

  return {
    equalityFieldIds: parsed.equalityFieldIds,
    fieldNames: parsed.fieldNames,
    entries: parsed.entries,
  };
}

// ============================================================================
// Delete Application
// ============================================================================

/**
 * Apply deletes to a set of rows.
 *
 * This function filters rows based on position and equality deletes,
 * respecting sequence number ordering.
 *
 * @param rows - Array of rows with their metadata
 * @param dataFileSequenceNumber - Sequence number of the data file
 * @param positionLookups - Position delete lookups to apply
 * @param equalityLookups - Equality delete lookups to apply
 * @returns Rows that passed (not deleted) and statistics
 */
export function applyDeletes<T extends Record<string, unknown>>(
  rows: Array<{ row: T; position: number }>,
  dataFilePath: string,
  dataFileSequenceNumber: number,
  positionLookups: PositionDeleteLookup[],
  equalityLookups: EqualityDeleteLookup[]
): {
  rows: T[];
  result: DeleteApplicationResult;
} {
  const passedRows: T[] = [];
  let positionDeletedRows = 0;
  let equalityDeletedRows = 0;

  for (const { row, position } of rows) {
    let deleted = false;

    // Check position deletes (only if delete seq >= data file seq)
    for (const lookup of positionLookups) {
      if (lookup.getSequenceNumber() >= dataFileSequenceNumber) {
        if (lookup.isDeleted(dataFilePath, position)) {
          deleted = true;
          positionDeletedRows++;
          break;
        }
      }
    }

    // Check equality deletes if not already deleted by position
    if (!deleted) {
      for (const lookup of equalityLookups) {
        if (lookup.getSequenceNumber() >= dataFileSequenceNumber) {
          if (lookup.isDeleted(row)) {
            deleted = true;
            equalityDeletedRows++;
            break;
          }
        }
      }
    }

    if (!deleted) {
      passedRows.push(row);
    }
  }

  return {
    rows: passedRows,
    result: {
      passedRows: passedRows.length,
      positionDeletedRows,
      equalityDeletedRows,
    },
  };
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Check if a data file is a delete file.
 */
export function isDeleteFile(file: DataFile): file is DeleteFile {
  return file.content === CONTENT_POSITION_DELETES || file.content === CONTENT_EQUALITY_DELETES;
}

/**
 * Check if a data file is a position delete file.
 */
export function isPositionDeleteFile(file: DataFile): file is PositionDeleteFile {
  return file.content === CONTENT_POSITION_DELETES;
}

/**
 * Check if a data file is an equality delete file.
 */
export function isEqualityDeleteFile(file: DataFile): file is EqualityDeleteFile {
  return file.content === CONTENT_EQUALITY_DELETES;
}

/**
 * Get the delete content type name.
 */
export function getDeleteContentTypeName(content: number): string {
  switch (content) {
    case CONTENT_DATA:
      return 'data';
    case CONTENT_POSITION_DELETES:
      return 'position-deletes';
    case CONTENT_EQUALITY_DELETES:
      return 'equality-deletes';
    default:
      return `unknown(${content})`;
  }
}

/**
 * Create a schema for equality delete files based on field IDs.
 */
export function createEqualityDeleteSchema(
  tableSchema: IcebergSchema,
  equalityFieldIds: number[]
): IcebergSchema {
  const fields: IcebergStructField[] = [];

  for (const fieldId of equalityFieldIds) {
    const sourceField = tableSchema.fields.find((f) => f.id === fieldId);
    if (!sourceField) {
      throw new Error(`Field ID ${fieldId} not found in schema`);
    }
    fields.push({
      id: sourceField.id,
      name: sourceField.name,
      required: true, // Equality delete fields are always required
      type: sourceField.type,
      doc: sourceField.doc,
    });
  }

  return {
    'schema-id': EQUALITY_DELETE_SCHEMA_ID,
    type: 'struct',
    fields,
  };
}
