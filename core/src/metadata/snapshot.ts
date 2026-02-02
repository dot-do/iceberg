/**
 * Iceberg Snapshot Management
 *
 * Creates and manages Iceberg snapshots for tables.
 * A snapshot represents the state of a table at a specific point in time.
 *
 * @see https://iceberg.apache.org/spec/
 */

import type {
  IcebergSchema,
  PartitionSpec,
  SortOrder,
  SnapshotSummary,
  SnapshotRef,
  SnapshotLogEntry,
  ManifestFile,
  TableMetadata,
  Snapshot,
} from './types.js';
import {
  createDefaultSchema,
  createUnpartitionedSpec,
  createUnsortedOrder,
} from './schema.js';
import { FORMAT_VERSION, INITIAL_PARTITION_ID } from './constants.js';

// ============================================================================
// Type Definitions
// ============================================================================

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

// ============================================================================
// UUID Generation
// ============================================================================

/**
 * Generate a UUID v4 using crypto API.
 */
export function generateUUID(): string {
  if (typeof crypto === 'undefined' || typeof crypto.getRandomValues !== 'function') {
    throw new Error('crypto.getRandomValues is not available in this environment');
  }

  let bytes: Uint8Array;
  try {
    bytes = crypto.getRandomValues(new Uint8Array(16));
  } catch (error) {
    throw new Error(
      `Failed to generate random bytes for UUID: ${error instanceof Error ? error.message : String(error)}`
    );
  }

  // Set version (4) and variant (RFC 4122)
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;

  const hex = Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');

  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

// ============================================================================
// Snapshot Builder
// ============================================================================

/**
 * Builder for creating Iceberg snapshots.
 *
 * Snapshots represent the state of a table at a specific point in time.
 * Each snapshot contains a reference to a manifest list that indexes
 * all the data files in the table.
 */
export class SnapshotBuilder {
  private sequenceNumber: number;
  private snapshotId: number;
  private parentSnapshotId?: number;
  private timestampMs: number;
  private operation: 'append' | 'replace' | 'overwrite' | 'delete';
  private manifestListPath: string;
  private schemaId: number;
  private summaryStats: SnapshotSummary;

  constructor(options: {
    sequenceNumber: number;
    snapshotId?: number;
    parentSnapshotId?: number;
    timestampMs?: number;
    operation?: 'append' | 'replace' | 'overwrite' | 'delete';
    manifestListPath: string;
    schemaId?: number;
  }) {
    // Validate sequenceNumber is a non-negative integer
    if (!Number.isInteger(options.sequenceNumber) || options.sequenceNumber < 0) {
      throw new Error('sequenceNumber must be a non-negative integer');
    }

    // Validate manifestListPath is a non-empty string
    if (typeof options.manifestListPath !== 'string' || options.manifestListPath.trim() === '') {
      throw new Error('manifestListPath must be a non-empty string');
    }

    this.sequenceNumber = options.sequenceNumber;
    this.snapshotId = options.snapshotId ?? Date.now();
    this.parentSnapshotId = options.parentSnapshotId;
    this.timestampMs = options.timestampMs ?? Date.now();
    this.operation = options.operation ?? 'append';
    this.manifestListPath = options.manifestListPath;
    this.schemaId = options.schemaId ?? 0;
    this.summaryStats = {
      operation: this.operation,
    };
  }

  /**
   * Set summary statistics from manifest list.
   */
  setSummary(
    addedDataFiles: number,
    deletedDataFiles: number,
    addedRecords: number,
    deletedRecords: number,
    addedFilesSize: number,
    removedFilesSize: number,
    totalRecords: number,
    totalFilesSize: number,
    totalDataFiles: number
  ): this {
    this.summaryStats = {
      operation: this.operation,
      'added-data-files': String(addedDataFiles),
      'deleted-data-files': String(deletedDataFiles),
      'added-records': String(addedRecords),
      'deleted-records': String(deletedRecords),
      'added-files-size': String(addedFilesSize),
      'removed-files-size': String(removedFilesSize),
      'total-records': String(totalRecords),
      'total-files-size': String(totalFilesSize),
      'total-data-files': String(totalDataFiles),
    };
    return this;
  }

  /**
   * Add custom summary properties.
   */
  addSummaryProperty(key: string, value: string): this {
    this.summaryStats = { ...this.summaryStats, [key]: value };
    return this;
  }

  /**
   * Build the snapshot object.
   */
  build(): Snapshot {
    if (this.parentSnapshotId !== undefined) {
      return {
        'snapshot-id': this.snapshotId,
        'parent-snapshot-id': this.parentSnapshotId,
        'sequence-number': this.sequenceNumber,
        'timestamp-ms': this.timestampMs,
        'manifest-list': this.manifestListPath,
        summary: this.summaryStats,
        'schema-id': this.schemaId,
      };
    }

    return {
      'snapshot-id': this.snapshotId,
      'sequence-number': this.sequenceNumber,
      'timestamp-ms': this.timestampMs,
      'manifest-list': this.manifestListPath,
      summary: this.summaryStats,
      'schema-id': this.schemaId,
    };
  }

  /**
   * Get the snapshot ID.
   */
  getSnapshotId(): number {
    return this.snapshotId;
  }

  /**
   * Get the sequence number.
   */
  getSequenceNumber(): number {
    return this.sequenceNumber;
  }
}

// ============================================================================
// Table Metadata Builder
// ============================================================================

/**
 * Builder for creating and updating Iceberg table metadata.
 *
 * Table metadata is the root of Iceberg's metadata hierarchy, containing
 * schema definitions, partition specs, and snapshot references.
 */
export class TableMetadataBuilder {
  private metadata: TableMetadata;

  constructor(options: CreateTableOptions) {
    const schema = options.schema ?? createDefaultSchema();
    const partitionSpec = options.partitionSpec ?? createUnpartitionedSpec();
    const sortOrder = options.sortOrder ?? createUnsortedOrder();

    // Find the highest field ID in the schema
    const lastColumnId = this.findMaxFieldId(schema);

    this.metadata = {
      'format-version': FORMAT_VERSION,
      'table-uuid': options.tableUuid ?? generateUUID(),
      location: options.location,
      'last-sequence-number': 0,
      'last-updated-ms': Date.now(),
      'last-column-id': lastColumnId,
      'current-schema-id': schema['schema-id'],
      schemas: [schema],
      'default-spec-id': partitionSpec['spec-id'],
      'partition-specs': [partitionSpec],
      'last-partition-id': this.findMaxPartitionFieldId(partitionSpec),
      'default-sort-order-id': sortOrder['order-id'],
      'sort-orders': [sortOrder],
      properties: options.properties ?? {},
      'current-snapshot-id': null,
      snapshots: [],
      'snapshot-log': [],
      'metadata-log': [],
      refs: {},
    };
  }

  /**
   * Create a builder from existing metadata.
   */
  static fromMetadata(metadata: TableMetadata): TableMetadataBuilder {
    const builder = new TableMetadataBuilder({
      location: metadata.location,
    });
    builder.metadata = { ...metadata };
    return builder;
  }

  /**
   * Find the maximum field ID in a schema.
   */
  private findMaxFieldId(schema: IcebergSchema): number {
    let maxId = 0;

    function traverse(fields: readonly { id: number; type: unknown }[]): void {
      for (const field of fields) {
        maxId = Math.max(maxId, field.id);
        if (typeof field.type === 'object' && field.type !== null) {
          const complexType = field.type as {
            type: string;
            fields?: readonly { id: number; type: unknown }[];
            'element-id'?: number;
            'key-id'?: number;
            'value-id'?: number;
          };
          if (complexType.type === 'struct' && complexType.fields) {
            traverse(complexType.fields);
          }
          if (complexType['element-id']) {
            maxId = Math.max(maxId, complexType['element-id']);
          }
          if (complexType['key-id']) {
            maxId = Math.max(maxId, complexType['key-id']);
          }
          if (complexType['value-id']) {
            maxId = Math.max(maxId, complexType['value-id']);
          }
        }
      }
    }

    traverse(schema.fields);
    return maxId;
  }

  /**
   * Find the maximum partition field ID in a partition spec.
   */
  private findMaxPartitionFieldId(spec: PartitionSpec): number {
    if (spec.fields.length === 0) {
      return INITIAL_PARTITION_ID; // Partition field IDs start at PARTITION_FIELD_ID_START
    }
    return Math.max(...spec.fields.map((f) => f['field-id']));
  }

  /**
   * Add a new snapshot to the table.
   */
  addSnapshot(snapshot: Snapshot): this {
    this.metadata = {
      ...this.metadata,
      snapshots: [...this.metadata.snapshots, snapshot],
      'current-snapshot-id': snapshot['snapshot-id'],
      // Per Iceberg spec, last-sequence-number must be the highest seen
      'last-sequence-number': Math.max(
        this.metadata['last-sequence-number'],
        snapshot['sequence-number']
      ),
      'last-updated-ms': snapshot['timestamp-ms'],
      // Update snapshot log
      'snapshot-log': [
        ...this.metadata['snapshot-log'],
        {
          'timestamp-ms': snapshot['timestamp-ms'],
          'snapshot-id': snapshot['snapshot-id'],
        },
      ],
      // Update main branch reference
      refs: {
        ...this.metadata.refs,
        main: {
          'snapshot-id': snapshot['snapshot-id'],
          type: 'branch',
        },
      },
    };

    return this;
  }

  /**
   * Add a schema to the table.
   */
  addSchema(schema: IcebergSchema): this {
    const maxFieldId = this.findMaxFieldId(schema);
    this.metadata = {
      ...this.metadata,
      schemas: [...this.metadata.schemas, schema],
      'last-column-id': maxFieldId > this.metadata['last-column-id']
        ? maxFieldId
        : this.metadata['last-column-id'],
    };
    return this;
  }

  /**
   * Set the current schema.
   */
  setCurrentSchema(schemaId: number): this {
    const schema = this.metadata.schemas.find((s) => s['schema-id'] === schemaId);
    if (!schema) {
      throw new Error(`Schema with ID ${schemaId} not found`);
    }
    this.metadata = {
      ...this.metadata,
      'current-schema-id': schemaId,
    };
    return this;
  }

  /**
   * Add a partition spec to the table.
   */
  addPartitionSpec(spec: PartitionSpec): this {
    const maxPartitionId = this.findMaxPartitionFieldId(spec);
    this.metadata = {
      ...this.metadata,
      'partition-specs': [...this.metadata['partition-specs'], spec],
      'last-partition-id': maxPartitionId > this.metadata['last-partition-id']
        ? maxPartitionId
        : this.metadata['last-partition-id'],
    };
    return this;
  }

  /**
   * Set the default partition spec.
   */
  setDefaultPartitionSpec(specId: number): this {
    const spec = this.metadata['partition-specs'].find((s) => s['spec-id'] === specId);
    if (!spec) {
      throw new Error(`Partition spec with ID ${specId} not found`);
    }
    this.metadata = {
      ...this.metadata,
      'default-spec-id': specId,
    };
    return this;
  }

  /**
   * Add a sort order to the table.
   */
  addSortOrder(order: SortOrder): this {
    this.metadata = {
      ...this.metadata,
      'sort-orders': [...this.metadata['sort-orders'], order],
    };
    return this;
  }

  /**
   * Set a table property.
   */
  setProperty(key: string, value: string): this {
    this.metadata = {
      ...this.metadata,
      properties: { ...this.metadata.properties, [key]: value },
    };
    return this;
  }

  /**
   * Remove a table property.
   */
  removeProperty(key: string): this {
    const { [key]: _, ...remainingProperties } = this.metadata.properties;
    this.metadata = {
      ...this.metadata,
      properties: remainingProperties,
    };
    return this;
  }

  /**
   * Add a snapshot reference (branch or tag).
   */
  addRef(name: string, ref: SnapshotRef): this {
    this.metadata = {
      ...this.metadata,
      refs: { ...this.metadata.refs, [name]: ref },
    };
    return this;
  }

  /**
   * Create a tag pointing to a specific snapshot.
   */
  createTag(name: string, snapshotId: number): this {
    return this.addRef(name, {
      'snapshot-id': snapshotId,
      type: 'tag',
    });
  }

  /**
   * Create a branch pointing to a specific snapshot.
   */
  createBranch(name: string, snapshotId: number): this {
    return this.addRef(name, {
      'snapshot-id': snapshotId,
      type: 'branch',
    });
  }

  /**
   * Add a metadata log entry (for tracking previous metadata files).
   */
  addMetadataLogEntry(metadataFile: string, timestampMs?: number): this {
    this.metadata = {
      ...this.metadata,
      'metadata-log': [
        ...this.metadata['metadata-log'],
        {
          'timestamp-ms': timestampMs ?? Date.now(),
          'metadata-file': metadataFile,
        },
      ],
    };
    return this;
  }

  /**
   * Get the current snapshot ID.
   */
  getCurrentSnapshotId(): number | null {
    return this.metadata['current-snapshot-id'];
  }

  /**
   * Get the next sequence number for a new snapshot.
   */
  getNextSequenceNumber(): number {
    return this.metadata['last-sequence-number'] + 1;
  }

  /**
   * Get the table UUID.
   */
  getTableUuid(): string {
    return this.metadata['table-uuid'];
  }

  /**
   * Get the table location.
   */
  getLocation(): string {
    return this.metadata.location;
  }

  /**
   * Build the table metadata object.
   */
  build(): TableMetadata {
    return { ...this.metadata };
  }

  /**
   * Serialize to JSON string.
   */
  toJSON(): string {
    return JSON.stringify(this.metadata, null, 2);
  }

  /**
   * Get a snapshot by ID.
   */
  getSnapshot(snapshotId: number): Snapshot | undefined {
    return this.metadata.snapshots.find((s) => s['snapshot-id'] === snapshotId);
  }

  /**
   * Get the current snapshot.
   */
  getCurrentSnapshot(): Snapshot | undefined {
    if (this.metadata['current-snapshot-id'] === null) {
      return undefined;
    }
    return this.getSnapshot(this.metadata['current-snapshot-id']);
  }

  /**
   * Get all snapshots.
   */
  getSnapshots(): readonly Snapshot[] {
    return this.metadata.snapshots;
  }

  /**
   * Get snapshot history for time-travel queries.
   */
  getSnapshotHistory(): readonly SnapshotLogEntry[] {
    return this.metadata['snapshot-log'];
  }
}

// ============================================================================
// Snapshot Manager
// ============================================================================

/**
 * Manages Iceberg snapshot lifecycle including creation, tracking, and expiration.
 *
 * The SnapshotManager provides a high-level API for:
 * - Creating new snapshots with proper metadata
 * - Tracking snapshot history and parent relationships
 * - Expiring old snapshots based on retention policies
 * - Managing snapshot references (branches and tags)
 */
export class SnapshotManager {
  private metadata: TableMetadata;
  private readonly retentionPolicy: SnapshotRetentionPolicy;

  constructor(
    metadata: TableMetadata,
    retentionPolicy: SnapshotRetentionPolicy = {}
  ) {
    this.metadata = { ...metadata };
    this.retentionPolicy = {
      maxSnapshotAgeMs: retentionPolicy.maxSnapshotAgeMs,
      maxRefAgeMs: retentionPolicy.maxRefAgeMs,
      minSnapshotsToKeep: retentionPolicy.minSnapshotsToKeep ?? 1,
    };
  }

  /**
   * Create a SnapshotManager from table metadata with a default retention policy.
   */
  static fromMetadata(
    metadata: TableMetadata,
    retentionPolicy?: SnapshotRetentionPolicy
  ): SnapshotManager {
    return new SnapshotManager(metadata, retentionPolicy);
  }

  /**
   * Get the current retention policy.
   */
  getRetentionPolicy(): SnapshotRetentionPolicy {
    return { ...this.retentionPolicy };
  }

  /**
   * Update the retention policy.
   */
  setRetentionPolicy(policy: SnapshotRetentionPolicy): void {
    Object.assign(this.retentionPolicy, policy);
  }

  /**
   * Get all snapshots in chronological order.
   */
  getSnapshots(): Snapshot[] {
    return [...this.metadata.snapshots].sort(
      (a, b) => a['timestamp-ms'] - b['timestamp-ms']
    );
  }

  /**
   * Get the current snapshot.
   */
  getCurrentSnapshot(): Snapshot | undefined {
    if (this.metadata['current-snapshot-id'] === null) {
      return undefined;
    }
    return this.getSnapshotById(this.metadata['current-snapshot-id']);
  }

  /**
   * Get a snapshot by ID.
   */
  getSnapshotById(snapshotId: number): Snapshot | undefined {
    return this.metadata.snapshots.find((s) => s['snapshot-id'] === snapshotId);
  }

  /**
   * Get a snapshot by reference name (branch or tag).
   */
  getSnapshotByRef(refName: string): Snapshot | undefined {
    const ref = this.metadata.refs[refName];
    if (!ref) {
      return undefined;
    }
    return this.metadata.snapshots.find((s) => s['snapshot-id'] === ref['snapshot-id']);
  }

  /**
   * Get a snapshot at a specific timestamp (time-travel).
   */
  getSnapshotAtTimestamp(timestampMs: number): Snapshot | undefined {
    const validSnapshots = this.metadata.snapshots
      .filter((s) => s['timestamp-ms'] <= timestampMs)
      .sort((a, b) => b['timestamp-ms'] - a['timestamp-ms']);

    return validSnapshots[0];
  }

  /**
   * Get the snapshot history (log of snapshot changes).
   */
  getSnapshotHistory(): SnapshotLogEntry[] {
    return [...this.metadata['snapshot-log']];
  }

  /**
   * Get snapshot IDs that are referenced by branches or tags.
   */
  getReferencedSnapshotIds(): Set<number> {
    const referenced = new Set<number>();
    for (const ref of Object.values(this.metadata.refs)) {
      referenced.add(ref['snapshot-id']);
    }
    return referenced;
  }

  /**
   * Get the ancestor chain of a snapshot (including the snapshot itself).
   */
  getAncestorChain(snapshotId: number): Snapshot[] {
    const chain: Snapshot[] = [];
    let currentId: number | undefined = snapshotId;

    while (currentId !== undefined) {
      const snapshot = this.getSnapshotById(currentId);
      if (!snapshot) break;
      chain.push(snapshot);
      currentId = snapshot['parent-snapshot-id'];
    }

    return chain;
  }

  /**
   * Create a new snapshot using the SnapshotBuilder pattern.
   */
  createSnapshot(options: CreateSnapshotOptions): Snapshot {
    const parentSnapshotId = options.parentSnapshotId ?? this.metadata['current-snapshot-id'] ?? undefined;
    const sequenceNumber = this.metadata['last-sequence-number'] + 1;

    const builder = new SnapshotBuilder({
      sequenceNumber,
      parentSnapshotId,
      manifestListPath: options.manifestListPath,
      operation: options.operation,
      schemaId: this.metadata['current-schema-id'],
    });

    // Calculate summary from manifests if provided
    if (options.manifests) {
      let addedFiles = 0;
      let deletedFiles = 0;
      let addedRecords = 0;
      let deletedRecords = 0;
      const addedSize = 0;
      const removedSize = 0;

      for (const manifest of options.manifests) {
        addedFiles += manifest['added-files-count'];
        deletedFiles += manifest['deleted-files-count'];
        addedRecords += manifest['added-rows-count'];
        deletedRecords += manifest['deleted-rows-count'];
      }

      // Get totals from previous snapshot if exists
      const parentSnapshot = parentSnapshotId ? this.getSnapshotById(parentSnapshotId) : undefined;
      const prevTotalRecords = parseInt(parentSnapshot?.summary['total-records'] ?? '0', 10);
      const prevTotalSize = parseInt(parentSnapshot?.summary['total-files-size'] ?? '0', 10);
      const prevTotalFiles = parseInt(parentSnapshot?.summary['total-data-files'] ?? '0', 10);

      builder.setSummary(
        addedFiles,
        deletedFiles,
        addedRecords,
        deletedRecords,
        addedSize,
        removedSize,
        prevTotalRecords + addedRecords - deletedRecords,
        prevTotalSize + addedSize - removedSize,
        prevTotalFiles + addedFiles - deletedFiles
      );
    }

    // Add any additional summary properties
    if (options.additionalSummary) {
      for (const [key, value] of Object.entries(options.additionalSummary)) {
        builder.addSummaryProperty(key, value);
      }
    }

    return builder.build();
  }

  /**
   * Add a snapshot to the managed metadata.
   * Returns the updated table metadata.
   */
  addSnapshot(snapshot: Snapshot): TableMetadata {
    const builder = TableMetadataBuilder.fromMetadata(this.metadata);
    builder.addSnapshot(snapshot);
    this.metadata = builder.build();
    return this.metadata;
  }

  /**
   * Identify snapshots that should be expired based on the retention policy.
   */
  findExpiredSnapshots(asOfTimestampMs: number = Date.now()): Snapshot[] {
    const { maxSnapshotAgeMs, minSnapshotsToKeep } = this.retentionPolicy;

    // Get all snapshots sorted by timestamp (newest first)
    const snapshots = [...this.metadata.snapshots].sort(
      (a, b) => b['timestamp-ms'] - a['timestamp-ms']
    );

    if (snapshots.length === 0) {
      return [];
    }

    // Get IDs that are referenced by branches/tags - these cannot be expired
    const referencedIds = this.getReferencedSnapshotIds();

    // Find snapshot IDs that are ancestors of referenced snapshots
    const ancestorIds = new Set<number>();
    for (const refId of referencedIds) {
      const chain = this.getAncestorChain(refId);
      for (const s of chain) {
        ancestorIds.add(s['snapshot-id']);
      }
    }

    // Snapshots to keep: referenced ones and their ancestors
    const mustKeepIds = new Set([...referencedIds, ...ancestorIds]);

    // Find candidates for expiration (not referenced, not ancestors of referenced)
    const candidates = snapshots.filter((s) => !mustKeepIds.has(s['snapshot-id']));

    // Apply age-based expiration if configured
    let expired: Snapshot[] = [];
    if (maxSnapshotAgeMs !== undefined) {
      const cutoffTime = asOfTimestampMs - maxSnapshotAgeMs;
      expired = candidates.filter((s) => s['timestamp-ms'] < cutoffTime);
    }

    // Ensure we keep at least minSnapshotsToKeep
    const totalToKeep = Math.max(minSnapshotsToKeep ?? 1, mustKeepIds.size);
    const canExpireCount = Math.max(0, snapshots.length - totalToKeep);

    // Only return as many expired snapshots as we're allowed to remove
    return expired.slice(0, canExpireCount);
  }

  /**
   * Expire snapshots based on the retention policy.
   * Returns the result of the expiration operation.
   *
   * Note: This method only removes snapshots from metadata. Actual file deletion
   * should be performed separately using the returned information.
   */
  expireSnapshots(asOfTimestampMs: number = Date.now()): ExpireSnapshotsResult {
    const toExpire = this.findExpiredSnapshots(asOfTimestampMs);
    const expiredIds = new Set(toExpire.map((s) => s['snapshot-id']));

    // Remove expired snapshots from metadata
    const keptSnapshots = this.metadata.snapshots.filter(
      (s) => !expiredIds.has(s['snapshot-id'])
    );

    // Remove expired entries from snapshot log
    const keptLog = this.metadata['snapshot-log'].filter(
      (entry) => !expiredIds.has(entry['snapshot-id'])
    );

    // Update metadata
    this.metadata = {
      ...this.metadata,
      snapshots: keptSnapshots,
      'snapshot-log': keptLog,
    };

    // Calculate files that can be deleted
    const deletedManifestFilesCount = toExpire.length; // One manifest list per snapshot
    const deletedDataFilesCount = 0; // Would need manifest analysis

    return {
      expiredSnapshotIds: Array.from(expiredIds),
      keptSnapshotIds: keptSnapshots.map((s) => s['snapshot-id']),
      deletedDataFilesCount,
      deletedManifestFilesCount,
    };
  }

  /**
   * Remove a specific snapshot by ID.
   * Returns true if the snapshot was removed, false if it was not found or could not be removed.
   */
  removeSnapshot(snapshotId: number): boolean {
    // Cannot remove referenced snapshots
    const referencedIds = this.getReferencedSnapshotIds();
    if (referencedIds.has(snapshotId)) {
      return false;
    }

    // Find and remove the snapshot
    const index = this.metadata.snapshots.findIndex(
      (s) => s['snapshot-id'] === snapshotId
    );

    if (index === -1) {
      return false;
    }

    // Update metadata
    this.metadata = {
      ...this.metadata,
      snapshots: this.metadata.snapshots.filter(
        (s) => s['snapshot-id'] !== snapshotId
      ),
      'snapshot-log': this.metadata['snapshot-log'].filter(
        (entry) => entry['snapshot-id'] !== snapshotId
      ),
    };

    return true;
  }

  /**
   * Set a snapshot reference (branch or tag).
   */
  setRef(
    name: string,
    snapshotId: number,
    type: 'branch' | 'tag',
    options?: {
      maxRefAgeMs?: number;
      maxSnapshotAgeMs?: number;
      minSnapshotsToKeep?: number;
    }
  ): void {
    const snapshot = this.getSnapshotById(snapshotId);
    if (!snapshot) {
      throw new Error(`Snapshot ${snapshotId} not found`);
    }

    this.metadata = {
      ...this.metadata,
      refs: {
        ...this.metadata.refs,
        [name]: {
          'snapshot-id': snapshotId,
          type,
          ...(options?.maxRefAgeMs !== undefined && { 'max-ref-age-ms': options.maxRefAgeMs }),
          ...(options?.maxSnapshotAgeMs !== undefined && { 'max-snapshot-age-ms': options.maxSnapshotAgeMs }),
          ...(options?.minSnapshotsToKeep !== undefined && { 'min-snapshots-to-keep': options.minSnapshotsToKeep }),
        },
      },
    };
  }

  /**
   * Remove a snapshot reference.
   */
  removeRef(name: string): boolean {
    if (!(name in this.metadata.refs)) {
      return false;
    }

    const { [name]: _, ...remainingRefs } = this.metadata.refs;
    this.metadata = {
      ...this.metadata,
      refs: remainingRefs,
    };

    return true;
  }

  /**
   * Get the current table metadata.
   */
  getMetadata(): TableMetadata {
    return { ...this.metadata };
  }

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
  } {
    const snapshots = this.metadata.snapshots;
    const refs = Object.values(this.metadata.refs);

    const timestamps = snapshots.map((s) => s['timestamp-ms']);

    return {
      totalSnapshots: snapshots.length,
      currentSnapshotId: this.metadata['current-snapshot-id'],
      oldestSnapshotTimestamp: timestamps.length > 0 ? Math.min(...timestamps) : null,
      newestSnapshotTimestamp: timestamps.length > 0 ? Math.max(...timestamps) : null,
      referencedSnapshotCount: this.getReferencedSnapshotIds().size,
      branchCount: refs.filter((r) => r.type === 'branch').length,
      tagCount: refs.filter((r) => r.type === 'tag').length,
    };
  }
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Create a new table metadata with an initial snapshot.
 */
export function createTableWithSnapshot(
  options: CreateTableOptions,
  manifestListPath: string,
  dataFilesAdded: number,
  recordsAdded: number,
  totalFileSize: number
): TableMetadata {
  const builder = new TableMetadataBuilder(options);
  const sequenceNumber = builder.getNextSequenceNumber();

  const snapshotBuilder = new SnapshotBuilder({
    sequenceNumber,
    manifestListPath,
    operation: 'append',
  });

  snapshotBuilder.setSummary(
    dataFilesAdded, // added-data-files
    0, // deleted-data-files
    recordsAdded, // added-records
    0, // deleted-records
    totalFileSize, // added-files-size
    0, // removed-files-size
    recordsAdded, // total-records
    totalFileSize, // total-files-size
    dataFilesAdded // total-data-files
  );

  const snapshot = snapshotBuilder.build();
  builder.addSnapshot(snapshot);

  return builder.build();
}
