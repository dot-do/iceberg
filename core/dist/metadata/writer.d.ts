/**
 * Iceberg Metadata Writer
 *
 * Generates and writes Iceberg metadata.json files to storage.
 * Follows the Apache Iceberg v2 specification.
 *
 * @see https://iceberg.apache.org/spec/
 */
import type { StorageBackend, IcebergSchema, PartitionSpec, SortOrder, TableMetadata, Snapshot, EncryptionKey } from './types.js';
/**
 * Options for creating table metadata.
 */
export interface MetadataWriterOptions {
    /** Table location (base path for data and metadata files) */
    location: string;
    /** Table UUID (auto-generated if not provided) */
    tableUuid?: string;
    /** Table schema (uses default if not provided) */
    schema?: IcebergSchema;
    /** Partition specification (unpartitioned if not provided) */
    partitionSpec?: PartitionSpec;
    /** Sort order (unsorted if not provided) */
    sortOrder?: SortOrder;
    /** Table properties */
    properties?: Record<string, string>;
    /** Format version (2 or 3). Defaults to 2 for backward compatibility. */
    formatVersion?: 2 | 3;
    /** Initial next-row-id value (v3 only). Defaults to 0 for v3 tables. */
    nextRowId?: number;
    /** Optional encryption keys for table encryption. */
    encryptionKeys?: EncryptionKey[];
}
/**
 * Result of a metadata write operation.
 */
export interface MetadataWriteResult {
    /** Path where the metadata file was written */
    metadataLocation: string;
    /** Version number of the metadata file */
    version: number;
    /** The complete table metadata that was written */
    metadata: TableMetadata;
}
/**
 * Writes Iceberg metadata.json files to storage.
 *
 * The MetadataWriter generates complete Iceberg v2 metadata files including:
 * - format-version
 * - table-uuid
 * - location
 * - last-sequence-number
 * - last-updated-ms
 * - last-column-id
 * - schemas
 * - current-schema-id
 * - partition-specs
 * - default-spec-id
 * - last-partition-id
 * - properties
 * - snapshots
 * - current-snapshot-id
 * - snapshot-log
 *
 * @example
 * ```ts
 * const storage = createStorage({ type: 'filesystem', basePath: '.data' });
 * const writer = new MetadataWriter(storage);
 *
 * // Create new table metadata
 * const result = await writer.writeNewTable({
 *   location: 's3://bucket/warehouse/db/table',
 *   properties: { 'app.collection': 'users' },
 * });
 *
 * // Update existing metadata with a new snapshot
 * const updated = await writer.writeWithSnapshot(
 *   result.metadata,
 *   snapshot,
 *   previousMetadataLocation
 * );
 * ```
 */
export declare class MetadataWriter {
    private readonly storage;
    constructor(storage: StorageBackend);
    /**
     * Create a new, empty table metadata object.
     *
     * This creates valid Iceberg v2 metadata with:
     * - format-version: 2
     * - A unique table-uuid
     * - Default schema (if not provided)
     * - Unpartitioned partition spec
     * - Unsorted sort order
     * - Empty snapshots list
     * - null current-snapshot-id
     */
    createTableMetadata(options: MetadataWriterOptions): TableMetadata;
    /**
     * Write new table metadata to storage.
     *
     * Creates a new metadata file at `{location}/metadata/v1.metadata.json`
     * and a version hint file at `{location}/metadata/version-hint.text`.
     *
     * @throws Error if metadata already exists at the location
     */
    writeNewTable(options: MetadataWriterOptions): Promise<MetadataWriteResult>;
    /**
     * Write table metadata with an updated snapshot.
     *
     * This:
     * 1. Adds the snapshot to the snapshots list
     * 2. Updates current-snapshot-id
     * 3. Updates last-sequence-number
     * 4. Updates last-updated-ms
     * 5. Updates snapshot-log
     * 6. Updates refs (main branch)
     * 7. Optionally adds previous metadata to metadata-log
     * 8. Writes a new versioned metadata file
     *
     * @param currentMetadata - The current table metadata
     * @param snapshot - The new snapshot to add
     * @param previousMetadataLocation - Optional path to previous metadata file (for metadata-log)
     */
    writeWithSnapshot(currentMetadata: TableMetadata, snapshot: Snapshot, previousMetadataLocation?: string): Promise<MetadataWriteResult>;
    /**
     * Write metadata if it doesn't already exist.
     *
     * This is an idempotent operation - if metadata already exists,
     * it returns the existing metadata without modification.
     *
     * @returns The existing or newly created metadata
     */
    writeIfMissing(options: MetadataWriterOptions): Promise<MetadataWriteResult>;
    /**
     * Serialize table metadata to JSON string.
     *
     * This produces a properly formatted metadata.json file content
     * that can be read by any Iceberg-compatible query engine.
     */
    serializeMetadata(metadata: TableMetadata): string;
    /**
     * Validate that metadata contains all required fields.
     *
     * @throws Error if validation fails
     */
    validateMetadata(metadata: TableMetadata): void;
    /**
     * Find the maximum field ID in a schema.
     */
    private findMaxFieldId;
    /**
     * Find the maximum partition field ID in a partition spec.
     */
    private findMaxPartitionFieldId;
    /**
     * Get the next version number for metadata files.
     */
    private getNextVersion;
    /**
     * Add a snapshot to existing metadata.
     */
    private addSnapshotToMetadata;
}
/**
 * Create and write new table metadata in one call.
 *
 * @example
 * ```ts
 * const storage = createStorage({ type: 'filesystem', basePath: '.data' });
 * const result = await writeNewTableMetadata(storage, {
 *   location: 's3://bucket/warehouse/db/table',
 * });
 * ```
 */
export declare function writeNewTableMetadata(storage: StorageBackend, options: MetadataWriterOptions): Promise<MetadataWriteResult>;
/**
 * Write metadata if it doesn't already exist.
 *
 * This is an idempotent operation useful for ensuring a table exists
 * before writing data to it.
 */
export declare function writeMetadataIfMissing(storage: StorageBackend, options: MetadataWriterOptions): Promise<MetadataWriteResult>;
//# sourceMappingURL=writer.d.ts.map