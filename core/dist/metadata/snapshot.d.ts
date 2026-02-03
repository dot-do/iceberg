/**
 * Iceberg Snapshot Management
 *
 * Creates and manages Iceberg snapshots for tables.
 * A snapshot represents the state of a table at a specific point in time.
 *
 * @see https://iceberg.apache.org/spec/
 */
import type { IcebergSchema, PartitionSpec, SortOrder, SnapshotRef, SnapshotLogEntry, ManifestFile, TableMetadata, Snapshot, EncryptionKey } from './types.js';
/** Options for creating a new snapshot */
export interface CreateSnapshotOptions {
    parentSnapshotId?: number;
    operation: 'append' | 'replace' | 'overwrite' | 'delete';
    manifestListPath: string;
    manifests?: ManifestFile[];
    additionalSummary?: Record<string, string>;
}
/** Options for creating table metadata */
export interface CreateTableOptions {
    tableUuid?: string;
    location: string;
    schema?: IcebergSchema;
    partitionSpec?: PartitionSpec;
    sortOrder?: SortOrder;
    properties?: Record<string, string>;
    /** Format version (2 or 3). Defaults to 2 for backward compatibility. */
    formatVersion?: 2 | 3;
    /** Initial next-row-id value (v3 only). Defaults to 0 for v3 tables. */
    nextRowId?: number;
    /** Optional encryption keys for table encryption. */
    encryptionKeys?: EncryptionKey[];
}
/** Snapshot retention policy configuration */
export interface SnapshotRetentionPolicy {
    /** Maximum age of snapshots in milliseconds */
    maxSnapshotAgeMs?: number;
    /** Maximum age of snapshot references in milliseconds */
    maxRefAgeMs?: number;
    /** Minimum number of snapshots to keep regardless of age */
    minSnapshotsToKeep?: number;
}
/** Result of snapshot expiration operation */
export interface ExpireSnapshotsResult {
    /** IDs of snapshots that were expired */
    expiredSnapshotIds: number[];
    /** IDs of snapshots that were kept */
    keptSnapshotIds: number[];
    /** Number of data files that can be deleted */
    deletedDataFilesCount: number;
    /** Number of manifest files that can be deleted */
    deletedManifestFilesCount: number;
}
/**
 * Generate a UUID v4 using crypto API.
 */
export declare function generateUUID(): string;
/**
 * Builder for creating Iceberg snapshots.
 *
 * Snapshots represent the state of a table at a specific point in time.
 * Each snapshot contains a reference to a manifest list that indexes
 * all the data files in the table.
 */
export declare class SnapshotBuilder {
    private sequenceNumber;
    private snapshotId;
    private parentSnapshotId?;
    private timestampMs;
    private operation;
    private manifestListPath;
    private schemaId;
    private summaryStats;
    private formatVersion;
    private firstRowId?;
    private addedRows?;
    private keyId?;
    constructor(options: {
        sequenceNumber: number;
        snapshotId?: number;
        parentSnapshotId?: number;
        timestampMs?: number;
        operation?: 'append' | 'replace' | 'overwrite' | 'delete';
        manifestListPath: string;
        schemaId?: number;
        /** Format version (2 or 3). Defaults to 2 for backward compatibility. */
        formatVersion?: 2 | 3;
        /** First row ID for v3 snapshots (required for v3, optional for v2). */
        firstRowId?: number;
        /** Number of rows added by this snapshot (required for v3, optional for v2). */
        addedRows?: number;
        /** ID of the encryption key that encrypts the manifest list key metadata. */
        keyId?: number;
    });
    /**
     * Set summary statistics from manifest list.
     */
    setSummary(addedDataFiles: number, deletedDataFiles: number, addedRecords: number, deletedRecords: number, addedFilesSize: number, removedFilesSize: number, totalRecords: number, totalFilesSize: number, totalDataFiles: number): this;
    /**
     * Add custom summary properties.
     */
    addSummaryProperty(key: string, value: string): this;
    /**
     * Build the snapshot object.
     */
    build(): Snapshot;
    /**
     * Get the snapshot ID.
     */
    getSnapshotId(): number;
    /**
     * Get the sequence number.
     */
    getSequenceNumber(): number;
}
/**
 * Builder for creating and updating Iceberg table metadata.
 *
 * Table metadata is the root of Iceberg's metadata hierarchy, containing
 * schema definitions, partition specs, and snapshot references.
 */
export declare class TableMetadataBuilder {
    private metadata;
    constructor(options: CreateTableOptions);
    /**
     * Options for creating a builder from existing metadata.
     */
    static fromMetadataOptions?: {
        /**
         * Upgrade the table to a new format version.
         * If specified and different from the current version, performs an upgrade.
         */
        formatVersion?: 2 | 3;
    };
    /**
     * Create a builder from existing metadata.
     *
     * @param metadata - The existing table metadata
     * @param options - Optional settings for the builder, including format version upgrade
     * @returns A new TableMetadataBuilder
     *
     * @example
     * ```ts
     * // Simple copy
     * const builder = TableMetadataBuilder.fromMetadata(existingMetadata);
     *
     * // Upgrade from v2 to v3
     * const builder = TableMetadataBuilder.fromMetadata(v2Metadata, { formatVersion: 3 });
     * ```
     */
    static fromMetadata(metadata: TableMetadata, options?: {
        formatVersion?: 2 | 3;
    }): TableMetadataBuilder;
    /**
     * Find the maximum field ID in a schema.
     */
    private findMaxFieldId;
    /**
     * Find the maximum partition field ID in a partition spec.
     */
    private findMaxPartitionFieldId;
    /**
     * Add a new snapshot to the table.
     */
    addSnapshot(snapshot: Snapshot): this;
    /**
     * Add a schema to the table.
     */
    addSchema(schema: IcebergSchema): this;
    /**
     * Set the current schema.
     */
    setCurrentSchema(schemaId: number): this;
    /**
     * Add a partition spec to the table.
     */
    addPartitionSpec(spec: PartitionSpec): this;
    /**
     * Set the default partition spec.
     */
    setDefaultPartitionSpec(specId: number): this;
    /**
     * Add a sort order to the table.
     */
    addSortOrder(order: SortOrder): this;
    /**
     * Set a table property.
     */
    setProperty(key: string, value: string): this;
    /**
     * Remove a table property.
     */
    removeProperty(key: string): this;
    /**
     * Add an encryption key to the table.
     */
    addEncryptionKey(key: EncryptionKey): this;
    /**
     * Add a snapshot reference (branch or tag).
     */
    addRef(name: string, ref: SnapshotRef): this;
    /**
     * Create a tag pointing to a specific snapshot.
     */
    createTag(name: string, snapshotId: number): this;
    /**
     * Create a branch pointing to a specific snapshot.
     */
    createBranch(name: string, snapshotId: number): this;
    /**
     * Add a metadata log entry (for tracking previous metadata files).
     */
    addMetadataLogEntry(metadataFile: string, timestampMs?: number): this;
    /**
     * Get the current snapshot ID.
     */
    getCurrentSnapshotId(): number | null;
    /**
     * Get the next sequence number for a new snapshot.
     */
    getNextSequenceNumber(): number;
    /**
     * Get the table UUID.
     */
    getTableUuid(): string;
    /**
     * Get the table location.
     */
    getLocation(): string;
    /**
     * Build the table metadata object.
     */
    build(): TableMetadata;
    /**
     * Serialize to JSON string.
     */
    toJSON(): string;
    /**
     * Get a snapshot by ID.
     */
    getSnapshot(snapshotId: number): Snapshot | undefined;
    /**
     * Get the current snapshot.
     */
    getCurrentSnapshot(): Snapshot | undefined;
    /**
     * Get all snapshots.
     */
    getSnapshots(): readonly Snapshot[];
    /**
     * Get snapshot history for time-travel queries.
     */
    getSnapshotHistory(): readonly SnapshotLogEntry[];
}
/**
 * Manages Iceberg snapshot lifecycle including creation, tracking, and expiration.
 *
 * The SnapshotManager provides a high-level API for:
 * - Creating new snapshots with proper metadata
 * - Tracking snapshot history and parent relationships
 * - Expiring old snapshots based on retention policies
 * - Managing snapshot references (branches and tags)
 */
export declare class SnapshotManager {
    private metadata;
    private readonly retentionPolicy;
    constructor(metadata: TableMetadata, retentionPolicy?: SnapshotRetentionPolicy);
    /**
     * Create a SnapshotManager from table metadata with a default retention policy.
     */
    static fromMetadata(metadata: TableMetadata, retentionPolicy?: SnapshotRetentionPolicy): SnapshotManager;
    /**
     * Get the current retention policy.
     */
    getRetentionPolicy(): SnapshotRetentionPolicy;
    /**
     * Update the retention policy.
     */
    setRetentionPolicy(policy: SnapshotRetentionPolicy): void;
    /**
     * Get all snapshots in chronological order.
     */
    getSnapshots(): Snapshot[];
    /**
     * Get the current snapshot.
     */
    getCurrentSnapshot(): Snapshot | undefined;
    /**
     * Get a snapshot by ID.
     */
    getSnapshotById(snapshotId: number): Snapshot | undefined;
    /**
     * Get a snapshot by reference name (branch or tag).
     */
    getSnapshotByRef(refName: string): Snapshot | undefined;
    /**
     * Get a snapshot at a specific timestamp (time-travel).
     */
    getSnapshotAtTimestamp(timestampMs: number): Snapshot | undefined;
    /**
     * Get the snapshot history (log of snapshot changes).
     */
    getSnapshotHistory(): SnapshotLogEntry[];
    /**
     * Get snapshot IDs that are referenced by branches or tags.
     */
    getReferencedSnapshotIds(): Set<number>;
    /**
     * Get the ancestor chain of a snapshot (including the snapshot itself).
     */
    getAncestorChain(snapshotId: number): Snapshot[];
    /**
     * Create a new snapshot using the SnapshotBuilder pattern.
     */
    createSnapshot(options: CreateSnapshotOptions): Snapshot;
    /**
     * Add a snapshot to the managed metadata.
     * Returns the updated table metadata.
     */
    addSnapshot(snapshot: Snapshot): TableMetadata;
    /**
     * Identify snapshots that should be expired based on the retention policy.
     */
    findExpiredSnapshots(asOfTimestampMs?: number): Snapshot[];
    /**
     * Expire snapshots based on the retention policy.
     * Returns the result of the expiration operation.
     *
     * Note: This method only removes snapshots from metadata. Actual file deletion
     * should be performed separately using the returned information.
     */
    expireSnapshots(asOfTimestampMs?: number): ExpireSnapshotsResult;
    /**
     * Remove a specific snapshot by ID.
     * Returns true if the snapshot was removed, false if it was not found or could not be removed.
     */
    removeSnapshot(snapshotId: number): boolean;
    /**
     * Set a snapshot reference (branch or tag).
     */
    setRef(name: string, snapshotId: number, type: 'branch' | 'tag', options?: {
        maxRefAgeMs?: number;
        maxSnapshotAgeMs?: number;
        minSnapshotsToKeep?: number;
    }): void;
    /**
     * Remove a snapshot reference.
     */
    removeRef(name: string): boolean;
    /**
     * Get the current table metadata.
     */
    getMetadata(): TableMetadata;
    /**
     * Get statistics about the snapshot collection.
     */
    getStats(): {
        totalSnapshots: number;
        currentSnapshotId: number | null;
        oldestSnapshotTimestamp: number | null;
        newestSnapshotTimestamp: number | null;
        referencedSnapshotCount: number;
        branchCount: number;
        tagCount: number;
    };
}
/**
 * Create a new table metadata with an initial snapshot.
 */
export declare function createTableWithSnapshot(options: CreateTableOptions, manifestListPath: string, dataFilesAdded: number, recordsAdded: number, totalFileSize: number): TableMetadata;
//# sourceMappingURL=snapshot.d.ts.map