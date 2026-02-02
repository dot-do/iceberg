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
import type { DataFile, ManifestEntry, ManifestEntryStatus, IcebergSchema, FileFormat } from './types.js';
import { CONTENT_DATA, CONTENT_POSITION_DELETES, CONTENT_EQUALITY_DELETES, MANIFEST_CONTENT_DATA, MANIFEST_CONTENT_DELETES } from './constants.js';
export { CONTENT_DATA, CONTENT_POSITION_DELETES, CONTENT_EQUALITY_DELETES, MANIFEST_CONTENT_DATA, MANIFEST_CONTENT_DELETES, };
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
/**
 * Iceberg position delete schema.
 * Position delete files have a fixed schema with two required columns.
 */
export declare const POSITION_DELETE_SCHEMA: IcebergSchema;
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
export declare class PositionDeleteBuilder {
    private readonly options;
    private readonly entries;
    private built;
    constructor(options: PositionDeleteBuilderOptions);
    /**
     * Add a position delete entry.
     *
     * @param filePath - Path to the data file containing the row to delete
     * @param pos - Zero-based position of the row within the file
     * @throws Error if pos is negative or if builder has already been built
     */
    addDelete(filePath: string, pos: number): this;
    /**
     * Add multiple position deletes for the same file.
     *
     * @param filePath - Path to the data file
     * @param positions - Array of row positions to delete
     */
    addDeletesForFile(filePath: string, positions: number[]): this;
    /**
     * Add multiple position delete entries.
     *
     * @param entries - Array of position delete entries
     */
    addDeletes(entries: PositionDelete[]): this;
    /**
     * Get the current number of entries.
     */
    getEntryCount(): number;
    /**
     * Check if the builder has any entries.
     */
    hasEntries(): boolean;
    /**
     * Build the position delete file.
     *
     * @returns The delete file metadata, serialized data, and statistics
     */
    build(): DeleteFileResult<PositionDeleteFile> & {
        statistics: PositionDeleteStatistics;
    };
    /**
     * Serialize position delete entries.
     * In production, this would write Parquet format.
     */
    private serializeEntries;
    private ensureNotBuilt;
}
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
export declare class EqualityDeleteBuilder {
    private readonly options;
    private readonly entries;
    private readonly equalityFields;
    private built;
    constructor(options: EqualityDeleteBuilderOptions);
    /**
     * Resolve field IDs to field names and types.
     */
    private resolveEqualityFields;
    /**
     * Add an equality delete entry.
     *
     * @param values - Column values that identify rows to delete
     * @throws Error if required equality fields are missing
     */
    addDelete(values: Record<string, unknown>): this;
    /**
     * Add multiple equality delete entries.
     *
     * @param entries - Array of value records to delete
     */
    addDeletes(entries: Array<Record<string, unknown>>): this;
    /**
     * Get the current number of entries.
     */
    getEntryCount(): number;
    /**
     * Get the equality field IDs.
     */
    getEqualityFieldIds(): number[];
    /**
     * Get the equality field names.
     */
    getEqualityFieldNames(): string[];
    /**
     * Check if the builder has any entries.
     */
    hasEntries(): boolean;
    /**
     * Build the equality delete file.
     *
     * @returns The delete file metadata, serialized data, and statistics
     */
    build(): DeleteFileResult<EqualityDeleteFile> & {
        statistics: EqualityDeleteStatistics;
    };
    /**
     * Compute statistics for equality delete entries.
     */
    private computeStatistics;
    /**
     * Encode bounds to binary format for DataFile.
     */
    private encodeBounds;
    /**
     * Serialize equality delete entries.
     * In production, this would write Parquet format.
     */
    private serializeEntries;
    private ensureNotBuilt;
}
/**
 * Generates delete manifest files.
 *
 * Delete manifests track position and equality delete files, separate from
 * data file manifests. They use content type 1 in the manifest list.
 */
export declare class DeleteManifestGenerator {
    private entries;
    private readonly sequenceNumber;
    private readonly snapshotId;
    constructor(options: {
        sequenceNumber: number;
        snapshotId: number;
    });
    /**
     * Add a position delete file to the manifest.
     */
    addPositionDeleteFile(file: Omit<PositionDeleteFile, 'content'>, status?: ManifestEntryStatus): this;
    /**
     * Add an equality delete file to the manifest.
     */
    addEqualityDeleteFile(file: Omit<EqualityDeleteFile, 'content'>, status?: ManifestEntryStatus): this;
    /**
     * Add a generic delete file to the manifest.
     */
    addDeleteFile(file: DeleteFile, status?: ManifestEntryStatus): this;
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
    };
    /**
     * Get the entries.
     */
    getEntries(): ManifestEntry[];
    /**
     * Get entry count.
     */
    get entryCount(): number;
    /**
     * Serialize to JSON.
     */
    toJSON(): string;
}
/**
 * Position delete lookup for efficient application.
 * Maps file path -> set of positions to delete.
 */
export declare class PositionDeleteLookup {
    private readonly lookup;
    private readonly sequenceNumber;
    constructor(sequenceNumber: number);
    /**
     * Add position deletes from a delete file.
     */
    addDeletes(filePath: string, positions: number[]): void;
    /**
     * Check if a row should be deleted.
     */
    isDeleted(filePath: string, position: number): boolean;
    /**
     * Get all deleted positions for a file.
     */
    getDeletedPositions(filePath: string): Set<number> | undefined;
    /**
     * Get all files with deletes.
     */
    getFilesWithDeletes(): string[];
    /**
     * Get total number of position deletes.
     */
    getTotalDeleteCount(): number;
    /**
     * Get the sequence number for these deletes.
     */
    getSequenceNumber(): number;
}
/**
 * Equality delete lookup for efficient application.
 * Uses JSON-serialized values as keys for deduplication.
 */
export declare class EqualityDeleteLookup {
    private readonly lookup;
    private readonly fieldNames;
    private readonly sequenceNumber;
    constructor(fieldNames: string[], sequenceNumber: number);
    /**
     * Add an equality delete entry.
     */
    addDelete(values: Record<string, unknown>): void;
    /**
     * Add multiple equality delete entries.
     */
    addDeletes(entries: Array<Record<string, unknown>>): void;
    /**
     * Check if a row should be deleted.
     */
    isDeleted(rowValues: Record<string, unknown>): boolean;
    /**
     * Make a lookup key from values.
     */
    private makeKey;
    /**
     * Get the number of unique delete entries.
     */
    getDeleteCount(): number;
    /**
     * Get the field names for this lookup.
     */
    getFieldNames(): string[];
    /**
     * Get the sequence number for these deletes.
     */
    getSequenceNumber(): number;
}
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
export declare class DeleteMerger {
    private readonly options;
    private readonly positionDeletes;
    private readonly equalityDeletes;
    private readonly equalityFieldIdSets;
    constructor(options: DeleteMergerOptions);
    /**
     * Add position delete entries to merge.
     */
    addPositionDeletes(entries: PositionDelete[]): void;
    /**
     * Add equality delete entries to merge.
     */
    addEqualityDeletes(entries: Array<Record<string, unknown>>, equalityFieldIds: number[]): void;
    /**
     * Get count of unique position deletes.
     */
    getPositionDeleteCount(): number;
    /**
     * Get count of unique equality deletes.
     */
    getEqualityDeleteCount(): number;
    /**
     * Merge and build delete files.
     */
    merge(snapshotId: number, sequenceNumber: number): {
        positionDeleteFiles: Array<DeleteFileResult<PositionDeleteFile> & {
            statistics: PositionDeleteStatistics;
        }>;
        equalityDeleteFiles: Array<DeleteFileResult<EqualityDeleteFile> & {
            statistics: EqualityDeleteStatistics;
        }>;
        statistics: {
            inputPositionDeleteCount: number;
            inputEqualityDeleteCount: number;
            outputPositionDeleteFiles: number;
            outputEqualityDeleteFiles: number;
        };
    };
    /**
     * Clear all accumulated deletes.
     */
    clear(): void;
    /**
     * Chunk entries into groups of maxEntriesPerFile.
     */
    private chunkEntries;
}
/**
 * Parse position delete file content.
 * Expects JSON format from PositionDeleteBuilder.
 */
export declare function parsePositionDeleteFile(data: Uint8Array): PositionDelete[];
/**
 * Parse equality delete file content.
 * Expects JSON format from EqualityDeleteBuilder.
 */
export declare function parseEqualityDeleteFile(data: Uint8Array): {
    equalityFieldIds: number[];
    fieldNames: string[];
    entries: Array<Record<string, unknown>>;
};
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
export declare function applyDeletes<T extends Record<string, unknown>>(rows: Array<{
    row: T;
    position: number;
}>, dataFilePath: string, dataFileSequenceNumber: number, positionLookups: PositionDeleteLookup[], equalityLookups: EqualityDeleteLookup[]): {
    rows: T[];
    result: DeleteApplicationResult;
};
/**
 * Check if a data file is a delete file.
 */
export declare function isDeleteFile(file: DataFile): file is DeleteFile;
/**
 * Check if a data file is a position delete file.
 */
export declare function isPositionDeleteFile(file: DataFile): file is PositionDeleteFile;
/**
 * Check if a data file is an equality delete file.
 */
export declare function isEqualityDeleteFile(file: DataFile): file is EqualityDeleteFile;
/**
 * Get the delete content type name.
 */
export declare function getDeleteContentTypeName(content: number): string;
/**
 * Create a schema for equality delete files based on field IDs.
 */
export declare function createEqualityDeleteSchema(tableSchema: IcebergSchema, equalityFieldIds: number[]): IcebergSchema;
//# sourceMappingURL=deletes.d.ts.map