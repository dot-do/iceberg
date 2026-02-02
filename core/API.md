# @dotdo/iceberg API Documentation

Apache Iceberg implementation for TypeScript. Provides metadata, manifest, and catalog operations for Iceberg tables.

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Classes](#core-classes)
  - [MetadataWriter](#metadatawriter)
  - [TableMetadataBuilder](#tablemetadatabuilder)
  - [SnapshotBuilder](#snapshotbuilder)
  - [SnapshotManager](#snapshotmanager)
  - [ManifestGenerator](#manifestgenerator)
  - [ManifestListGenerator](#manifestlistgenerator)
  - [AtomicCommitter](#atomiccommitter)
  - [SchemaEvolutionBuilder](#schemaevolutionbuilder)
  - [PartitionSpecBuilder](#partitionspecbuilder)
  - [BloomFilter](#bloomfilter)
  - [BloomFilterWriter](#bloomfilterwriter)
- [Reader Functions](#reader-functions)
- [Schema Utilities](#schema-utilities)
- [Partition Transform Functions](#partition-transform-functions)
- [Column Statistics](#column-statistics)
- [Row-Level Delete Utilities](#row-level-delete-utilities)
- [Error Classes](#error-classes)
- [Types and Interfaces](#types-and-interfaces)
- [Constants](#constants)

---

## Overview

`@dotdo/iceberg` provides a complete TypeScript implementation of the Apache Iceberg v2 table format specification. It enables:

- Creating and managing Iceberg table metadata
- Building snapshots and manifest files
- Schema evolution with backward compatibility
- Partition specification and transforms
- Atomic commits with optimistic concurrency control
- Bloom filters for efficient file skipping
- Row-level deletes (position and equality)
- Time-travel queries

## Installation

```bash
npm install @dotdo/iceberg
# or
pnpm add @dotdo/iceberg
```

## Quick Start

```typescript
import {
  MetadataWriter,
  ManifestGenerator,
  ManifestListGenerator,
  SnapshotBuilder,
  createDefaultSchema,
} from '@dotdo/iceberg';

// 1. Create table metadata
const writer = new MetadataWriter(storage);
const result = await writer.writeNewTable({
  location: 's3://bucket/warehouse/db/table',
  properties: { 'app.collection': 'users' },
});

// 2. Create a manifest with data files
const manifest = new ManifestGenerator({
  sequenceNumber: 1,
  snapshotId: Date.now(),
});

manifest.addDataFile({
  'file-path': 's3://bucket/data/file.parquet',
  'file-format': 'parquet',
  'record-count': 1000,
  'file-size-in-bytes': 4096,
  partition: {},
});

// 3. Create a manifest list
const manifestList = new ManifestListGenerator({
  snapshotId: Date.now(),
  sequenceNumber: 1,
});

manifestList.addManifestWithStats(
  's3://bucket/metadata/manifest.avro',
  1024,
  0,
  manifest.generate().summary
);

// 4. Build a snapshot
const snapshot = new SnapshotBuilder({
  sequenceNumber: 1,
  manifestListPath: 's3://bucket/metadata/snap-1.avro',
})
  .setSummary(1, 0, 1000, 0, 4096, 0, 1000, 4096, 1)
  .build();
```

---

## Core Classes

### MetadataWriter

Writes Iceberg metadata.json files to storage. Generates complete Iceberg v2 metadata files.

#### Constructor

```typescript
constructor(storage: StorageBackend)
```

**Parameters:**
- `storage` - Storage backend implementation for reading/writing files

#### Methods

##### `createTableMetadata(options: MetadataWriterOptions): TableMetadata`

Creates a new, empty table metadata object.

```typescript
const metadata = writer.createTableMetadata({
  location: 's3://bucket/warehouse/db/table',
  schema: customSchema,        // optional, defaults to createDefaultSchema()
  partitionSpec: customSpec,   // optional, defaults to unpartitioned
  sortOrder: customOrder,      // optional, defaults to unsorted
  properties: { key: 'value' } // optional
});
```

##### `writeNewTable(options: MetadataWriterOptions): Promise<MetadataWriteResult>`

Creates a new metadata file at `{location}/metadata/v1.metadata.json`.

```typescript
const result = await writer.writeNewTable({
  location: 's3://bucket/warehouse/db/table',
});
// result.metadataLocation: path to metadata file
// result.version: 1
// result.metadata: TableMetadata object
```

**Throws:** Error if metadata already exists at the location.

##### `writeWithSnapshot(currentMetadata: TableMetadata, snapshot: Snapshot, previousMetadataLocation?: string): Promise<MetadataWriteResult>`

Writes table metadata with an updated snapshot.

```typescript
const updated = await writer.writeWithSnapshot(
  result.metadata,
  snapshot,
  result.metadataLocation
);
```

##### `writeIfMissing(options: MetadataWriterOptions): Promise<MetadataWriteResult>`

Writes metadata if it doesn't already exist (idempotent operation).

```typescript
const result = await writer.writeIfMissing({
  location: 's3://bucket/warehouse/db/table',
});
```

##### `validateMetadata(metadata: TableMetadata): void`

Validates that metadata contains all required fields.

```typescript
writer.validateMetadata(metadata); // throws if invalid
```

---

### TableMetadataBuilder

Builder for creating and updating Iceberg table metadata. Table metadata is the root of Iceberg's metadata hierarchy.

#### Constructor

```typescript
constructor(options: CreateTableOptions)
```

**Options:**
- `location` (required) - Table location (base path)
- `tableUuid` - Optional UUID (auto-generated if not provided)
- `schema` - Optional schema (uses default if not provided)
- `partitionSpec` - Optional partition spec
- `sortOrder` - Optional sort order
- `properties` - Optional table properties

#### Static Methods

##### `fromMetadata(metadata: TableMetadata): TableMetadataBuilder`

Creates a builder from existing metadata.

```typescript
const builder = TableMetadataBuilder.fromMetadata(existingMetadata);
```

#### Methods

##### `addSnapshot(snapshot: Snapshot): this`

Adds a new snapshot to the table and updates `current-snapshot-id`.

```typescript
builder.addSnapshot(snapshot);
```

##### `addSchema(schema: IcebergSchema): this`

Adds a new schema to the table.

```typescript
builder.addSchema(newSchema);
```

##### `setCurrentSchema(schemaId: number): this`

Sets the current schema by ID.

```typescript
builder.setCurrentSchema(1);
```

##### `addPartitionSpec(spec: PartitionSpec): this`

Adds a partition specification.

```typescript
builder.addPartitionSpec(newSpec);
```

##### `setDefaultPartitionSpec(specId: number): this`

Sets the default partition spec by ID.

##### `addSortOrder(order: SortOrder): this`

Adds a sort order to the table.

##### `setProperty(key: string, value: string): this`

Sets a table property.

```typescript
builder.setProperty('write.parquet.compression', 'zstd');
```

##### `removeProperty(key: string): this`

Removes a table property.

##### `addRef(name: string, ref: SnapshotRef): this`

Adds a snapshot reference (branch or tag).

##### `createTag(name: string, snapshotId: number): this`

Creates a tag pointing to a specific snapshot.

```typescript
builder.createTag('v1.0', snapshotId);
```

##### `createBranch(name: string, snapshotId: number): this`

Creates a branch pointing to a specific snapshot.

```typescript
builder.createBranch('feature-branch', snapshotId);
```

##### `getNextSequenceNumber(): number`

Returns the next sequence number for a new snapshot.

##### `getCurrentSnapshot(): Snapshot | undefined`

Returns the current snapshot.

##### `build(): TableMetadata`

Builds the table metadata object.

```typescript
const metadata = builder.build();
```

##### `toJSON(): string`

Serializes to JSON string.

---

### SnapshotBuilder

Builder for creating Iceberg snapshots. Snapshots represent the state of a table at a specific point in time.

#### Constructor

```typescript
constructor(options: {
  sequenceNumber: number;        // Required, must be non-negative
  snapshotId?: number;           // Defaults to Date.now()
  parentSnapshotId?: number;     // Optional parent snapshot
  timestampMs?: number;          // Defaults to Date.now()
  operation?: 'append' | 'replace' | 'overwrite' | 'delete';  // Defaults to 'append'
  manifestListPath: string;      // Required, path to manifest list
  schemaId?: number;             // Defaults to 0
})
```

#### Methods

##### `setSummary(addedDataFiles, deletedDataFiles, addedRecords, deletedRecords, addedFilesSize, removedFilesSize, totalRecords, totalFilesSize, totalDataFiles): this`

Sets summary statistics.

```typescript
builder.setSummary(
  10,    // added-data-files
  0,     // deleted-data-files
  10000, // added-records
  0,     // deleted-records
  40960, // added-files-size
  0,     // removed-files-size
  10000, // total-records
  40960, // total-files-size
  10     // total-data-files
);
```

##### `addSummaryProperty(key: string, value: string): this`

Adds a custom summary property.

```typescript
builder.addSummaryProperty('custom.property', 'value');
```

##### `build(): Snapshot`

Builds the snapshot object.

```typescript
const snapshot = builder.build();
```

##### `getSnapshotId(): number`

Returns the snapshot ID.

##### `getSequenceNumber(): number`

Returns the sequence number.

---

### SnapshotManager

Manages Iceberg snapshot lifecycle including creation, tracking, and expiration.

#### Constructor

```typescript
constructor(metadata: TableMetadata, retentionPolicy?: SnapshotRetentionPolicy)
```

**SnapshotRetentionPolicy:**
```typescript
interface SnapshotRetentionPolicy {
  maxSnapshotAgeMs?: number;     // Maximum age of snapshots
  maxRefAgeMs?: number;          // Maximum age of references
  minSnapshotsToKeep?: number;   // Minimum snapshots to retain (default: 1)
}
```

#### Static Methods

##### `fromMetadata(metadata: TableMetadata, retentionPolicy?: SnapshotRetentionPolicy): SnapshotManager`

Creates a SnapshotManager from table metadata.

#### Methods

##### `getSnapshots(): Snapshot[]`

Returns all snapshots in chronological order.

##### `getCurrentSnapshot(): Snapshot | undefined`

Returns the current snapshot.

##### `getSnapshotById(snapshotId: number): Snapshot | undefined`

Gets a snapshot by ID.

##### `getSnapshotByRef(refName: string): Snapshot | undefined`

Gets a snapshot by reference name (branch or tag).

##### `getSnapshotAtTimestamp(timestampMs: number): Snapshot | undefined`

Gets a snapshot at a specific timestamp (time-travel).

```typescript
const snapshot = manager.getSnapshotAtTimestamp(
  Date.now() - 24 * 60 * 60 * 1000 // 24 hours ago
);
```

##### `getAncestorChain(snapshotId: number): Snapshot[]`

Gets the ancestor chain of a snapshot.

##### `createSnapshot(options: CreateSnapshotOptions): Snapshot`

Creates a new snapshot.

```typescript
const snapshot = manager.createSnapshot({
  operation: 'append',
  manifestListPath: 's3://bucket/metadata/snap.avro',
  manifests: manifestFiles, // optional
});
```

##### `addSnapshot(snapshot: Snapshot): TableMetadata`

Adds a snapshot to the managed metadata.

##### `findExpiredSnapshots(asOfTimestampMs?: number): Snapshot[]`

Identifies snapshots that should be expired based on retention policy.

##### `expireSnapshots(asOfTimestampMs?: number): ExpireSnapshotsResult`

Expires snapshots based on the retention policy.

```typescript
const result = manager.expireSnapshots();
// result.expiredSnapshotIds: IDs that were expired
// result.keptSnapshotIds: IDs that were kept
// result.deletedManifestFilesCount: manifests to clean up
```

##### `setRef(name: string, snapshotId: number, type: 'branch' | 'tag', options?): void`

Sets a snapshot reference.

##### `removeRef(name: string): boolean`

Removes a snapshot reference.

##### `getMetadata(): TableMetadata`

Returns the current table metadata.

##### `getStats(): object`

Returns statistics about the snapshot collection.

---

### ManifestGenerator

Generates Iceberg manifest files from data file metadata. Manifests track individual data files along with their partition values and statistics.

#### Constructor

```typescript
constructor(options: { sequenceNumber: number; snapshotId: number })
```

#### Methods

##### `addDataFile(file: Omit<DataFile, 'content'>, status?: ManifestEntryStatus): void`

Adds a data file to the manifest.

```typescript
manifest.addDataFile({
  'file-path': 's3://bucket/data/part-00000.parquet',
  'file-format': 'parquet',
  'record-count': 1000,
  'file-size-in-bytes': 4096,
  partition: { year: 2024, month: 1 },
  'column-sizes': { 1: 1024, 2: 2048 },
  'lower-bounds': { 1: new Uint8Array([...]) },
  'upper-bounds': { 1: new Uint8Array([...]) },
}, 1); // 0=EXISTING, 1=ADDED, 2=DELETED
```

##### `addDataFileWithStats(file: Omit<DataFile, 'content'>, stats: ComputedFileStats, status?: ManifestEntryStatus): void`

Adds a data file with pre-computed column statistics.

##### `addPositionDeleteFile(file: Omit<DataFile, 'content'>, status?: ManifestEntryStatus): void`

Adds a position delete file to the manifest.

##### `addEqualityDeleteFile(file: Omit<DataFile, 'content'>, equalityFieldIds: number[], status?: ManifestEntryStatus): void`

Adds an equality delete file to the manifest.

##### `generate(): { entries: ManifestEntry[]; summary: object }`

Generates the manifest content with summary statistics.

```typescript
const { entries, summary } = manifest.generate();
// summary.addedFiles, summary.deletedFiles, summary.addedRows, etc.
```

##### `getEntries(): ManifestEntry[]`

Returns all manifest entries.

##### `toJSON(): string`

Serializes to JSON string.

##### `entryCount: number` (getter)

Returns the number of entries.

---

### ManifestListGenerator

Generates manifest list files that index multiple manifests.

#### Constructor

```typescript
constructor(options: { snapshotId: number; sequenceNumber: number })
```

#### Methods

##### `addManifest(manifest: Omit<ManifestFile, 'added-snapshot-id' | 'sequence-number' | 'min-sequence-number'>): void`

Adds a manifest file reference.

##### `addManifestWithStats(path: string, length: number, partitionSpecId: number, summary: object, isDeleteManifest?: boolean, partitionSummaries?: PartitionFieldSummary[]): void`

Adds a manifest file with computed statistics.

```typescript
manifestList.addManifestWithStats(
  's3://bucket/metadata/manifest-001.avro',
  4096,
  0,
  { addedFiles: 10, existingFiles: 0, deletedFiles: 0, addedRows: 10000, existingRows: 0, deletedRows: 0 },
  false, // isDeleteManifest
  partitionSummaries
);
```

##### `generate(): ManifestFile[]`

Generates the manifest list content.

##### `getTotals(): { totalFiles, totalRows, addedFiles, deletedFiles }`

Returns total counts across all manifests.

##### `manifestCount: number` (getter)

Returns the number of manifests.

---

### AtomicCommitter

Handles atomic snapshot commits with optimistic locking. Implements the Iceberg commit protocol with retry logic.

#### Constructor

```typescript
constructor(storage: StorageBackend, tableLocation: string)
```

#### Methods

##### `commit(buildCommit: (metadata: TableMetadata | null) => Promise<PendingCommit>, options?: CommitOptions): Promise<CommitResult>`

Commits a new snapshot with retry logic.

```typescript
const result = await committer.commit(async (metadata) => {
  if (!metadata) throw new Error('Table not initialized');

  const snapshot = new SnapshotBuilder({
    sequenceNumber: metadata['last-sequence-number'] + 1,
    manifestListPath: 's3://bucket/metadata/snap.avro',
  }).build();

  return {
    baseMetadata: metadata,
    snapshot,
    previousMetadataPath: currentPath,
  };
}, {
  maxRetries: 5,
  baseRetryDelayMs: 100,
  maxRetryDelayMs: 5000,
});
```

**CommitOptions:**
```typescript
interface CommitOptions {
  maxRetries?: number;        // Default: 5
  baseRetryDelayMs?: number;  // Default: 100
  maxRetryDelayMs?: number;   // Default: 5000
  retryJitter?: number;       // Default: 0.2
  cleanupOnFailure?: boolean; // Default: true
}
```

**CommitResult:**
```typescript
interface CommitResult {
  snapshot: Snapshot;
  metadataVersion: number;
  metadataPath: string;
  attempts: number;
  conflictResolved: boolean;
  cleanedUpFiles?: string[];
}
```

##### `commitSnapshot(manifestListPath: string, summary: SnapshotSummary, options?): Promise<CommitResult>`

Simplified commit interface.

```typescript
const result = await committer.commitSnapshot(
  's3://bucket/metadata/snap.avro',
  { operation: 'append', 'added-data-files': '10', 'added-records': '1000' }
);
```

##### `loadMetadata(): Promise<TableMetadata | null>`

Loads the current table metadata.

##### `getCurrentVersion(): Promise<number | null>`

Reads the current version from version-hint.text.

##### `cleanupOldMetadata(options?: MetadataCleanupOptions): Promise<string[]>`

Cleans up old metadata files based on retention policy.

```typescript
const deleted = await committer.cleanupOldMetadata({
  retainVersions: 10,
  maxAgeMs: 7 * 24 * 60 * 60 * 1000, // 7 days
});
```

---

### SchemaEvolutionBuilder

Builder for schema evolution operations. Allows modifying table schemas while maintaining backward compatibility.

#### Constructor

```typescript
constructor(baseSchema: IcebergSchema, lastFieldId?: number)
```

#### Methods

##### `addColumn(name: string, type: IcebergType, required?: boolean, doc?: string, position?: ColumnPosition): this`

Adds a new column.

```typescript
builder.addColumn('email', 'string', false, 'User email');
builder.addColumn('nested.field', 'int', false, 'Nested field');
```

##### `dropColumn(name: string): this`

Drops a column by name.

##### `renameColumn(oldName: string, newName: string): this`

Renames a column.

##### `updateColumnType(name: string, newType: IcebergType): this`

Updates a column's type (must be a valid promotion).

```typescript
builder.updateColumnType('count', 'long'); // int -> long is valid
```

##### `makeColumnOptional(name: string): this`

Makes a required column optional.

##### `makeColumnRequired(name: string): this`

Makes an optional column required (breaking change).

##### `updateColumnDoc(name: string, doc: string): this`

Updates a column's documentation.

##### `moveColumn(name: string, position: ColumnPosition): this`

Moves a column to a new position.

```typescript
builder.moveColumn('email', { type: 'after', reference: 'name' });
```

##### `validate(): SchemaValidationResult`

Validates pending operations without applying them.

##### `build(): SchemaEvolutionResult`

Applies operations and returns the new schema.

```typescript
const result = builder.build();
// result.schema: new IcebergSchema
// result.schemaId: new schema ID
// result.changes: list of applied changes
```

---

### PartitionSpecBuilder

Builder for creating partition specifications.

#### Constructor

```typescript
constructor(schema: IcebergSchema, options?: CreatePartitionSpecOptions)
```

**Options:**
```typescript
interface CreatePartitionSpecOptions {
  specId?: number;           // Default: 0
  startingFieldId?: number;  // Default: 1000
}
```

#### Methods

##### `identity(sourceFieldName: string, partitionName?: string): this`

Adds an identity partition field.

```typescript
builder.identity('region');
```

##### `bucket(sourceFieldName: string, numBuckets: number, partitionName?: string): this`

Adds a bucket partition field.

```typescript
builder.bucket('user_id', 16);
```

##### `truncate(sourceFieldName: string, width: number, partitionName?: string): this`

Adds a truncate partition field.

```typescript
builder.truncate('name', 5); // First 5 characters
```

##### `year(sourceFieldName: string, partitionName?: string): this`

Adds a year partition field.

##### `month(sourceFieldName: string, partitionName?: string): this`

Adds a month partition field.

##### `day(sourceFieldName: string, partitionName?: string): this`

Adds a day partition field.

##### `hour(sourceFieldName: string, partitionName?: string): this`

Adds an hour partition field.

##### `void(sourceFieldName: string, partitionName?: string): this`

Adds a void partition field (always null).

##### `build(): PartitionSpec`

Builds the partition specification.

```typescript
const spec = new PartitionSpecBuilder(schema)
  .identity('region')
  .day('created_at')
  .bucket('user_id', 16)
  .build();
```

---

### BloomFilter

Split-Block Bloom Filter implementation per Parquet specification for efficient file skipping.

#### Constructor

```typescript
constructor(options?: BloomFilterOptions)
```

**Options:**
```typescript
interface BloomFilterOptions {
  expectedItems?: number;       // Default: 10000
  falsePositiveRate?: number;   // Default: 0.01
  maxBytes?: number;            // Default: 1MB
  numBlocks?: number;           // Override calculated value
}
```

#### Methods

##### `add(value: string | number | bigint | Uint8Array): void`

Adds a value to the bloom filter.

```typescript
filter.add('user@example.com');
filter.add(12345);
```

##### `addAll(values: Iterable<string | number | bigint | Uint8Array>): void`

Adds multiple values.

##### `mightContain(value: string | number | bigint | Uint8Array): boolean`

Checks if a value might be in the set.

```typescript
if (!filter.mightContain('user@example.com')) {
  // Definitely not in the file - skip reading
}
```

##### `merge(other: BloomFilter): void`

Merges another bloom filter (union operation).

##### `serialize(): Uint8Array`

Serializes the filter to bytes.

##### `clear(): void`

Clears the bloom filter.

#### Static Methods

##### `deserialize(data: Uint8Array): BloomFilter`

Creates a bloom filter from serialized bytes.

##### `fromRawData(blockData: Uint8Array, itemCount?: number, fpp?: number): BloomFilter`

Creates from raw block data.

#### Properties

- `blockCount: number` - Number of blocks
- `count: number` - Number of items added
- `falsePositiveRate: number` - Target FPR
- `estimatedFalsePositiveRate: number` - Actual estimated FPR
- `sizeInBytes: number` - Filter size

---

### BloomFilterWriter

Writer for creating bloom filter files for Iceberg data files.

#### Constructor

```typescript
constructor(options: BloomFilterWriterOptions)
```

**Options:**
```typescript
interface BloomFilterWriterOptions {
  basePath: string;
  expectedItemsPerColumn?: number;  // Default: 10000
  falsePositiveRate?: number;       // Default: 0.01
  maxBytesPerFilter?: number;       // Default: 1MB
}
```

#### Methods

##### `getOrCreateFilter(fieldId: number, columnName: string): BloomFilter`

Gets or creates a bloom filter for a column.

##### `addValue(fieldId: number, columnName: string, value: string | number | bigint | Uint8Array): void`

Adds a value to a column's filter.

##### `addValues(fieldId: number, columnName: string, values: Iterable<...>): void`

Adds multiple values to a column's filter.

##### `finalize(): { data: Uint8Array; metadata: BloomFilterMetadata[] }`

Finalizes and serializes all filters.

```typescript
const { data, metadata } = writer.finalize();
await storage.put(bloomFilterPath, data);
```

---

## Reader Functions

### `readTableMetadata(storage: StorageBackend, location: string): Promise<TableMetadata | null>`

Reads table metadata from storage.

```typescript
const metadata = await readTableMetadata(storage, 's3://bucket/warehouse/db/table');
```

### `readMetadataFromPath(storage: StorageBackend, metadataPath: string): Promise<TableMetadata>`

Reads metadata from a specific path.

```typescript
const metadata = await readMetadataFromPath(storage, 's3://bucket/metadata/v5.metadata.json');
```

### `parseTableMetadata(json: string): TableMetadata`

Parses table metadata from JSON string.

### `getCurrentVersion(storage: StorageBackend, location: string): Promise<number | null>`

Gets the current version from version-hint.text.

### `getCurrentSnapshot(metadata: TableMetadata): Snapshot | undefined`

Gets the current snapshot from metadata.

### `getSnapshotById(metadata: TableMetadata, snapshotId: number): Snapshot | undefined`

Gets a snapshot by ID.

### `getSnapshotByRef(metadata: TableMetadata, refName: string): Snapshot | undefined`

Gets a snapshot by reference name.

### `getSnapshotAtTimestamp(metadata: TableMetadata, timestampMs: number): Snapshot | undefined`

Gets a snapshot at a specific timestamp for time-travel queries.

```typescript
const historicalSnapshot = getSnapshotAtTimestamp(metadata, Date.parse('2024-01-15'));
```

### `listMetadataFiles(storage: StorageBackend, location: string): Promise<string[]>`

Lists all metadata files in a table.

---

## Schema Utilities

### `createDefaultSchema(): IcebergSchema`

Creates the default schema with `_id`, `_seq`, `_op`, `_data` columns.

### `createUnpartitionedSpec(): PartitionSpec`

Creates a default unpartitioned specification.

### `createIdentityPartitionSpec(sourceFieldId: number, fieldName: string, specId?: number): PartitionSpec`

Creates a partition spec with identity transform.

### `createBucketPartitionSpec(sourceFieldId: number, fieldName: string, numBuckets: number, specId?: number): PartitionSpec`

Creates a bucket partition spec.

### `createTimePartitionSpec(sourceFieldId: number, fieldName: string, transform: 'year' | 'month' | 'day' | 'hour', specId?: number): PartitionSpec`

Creates a time-based partition spec.

### `createUnsortedOrder(): SortOrder`

Creates an unsorted sort order.

### `createSortOrder(sourceFieldId: number, direction?: 'asc' | 'desc', nullOrder?: 'nulls-first' | 'nulls-last', orderId?: number): SortOrder`

Creates a sort order on a single field.

### `parquetToIcebergType(parquetType: ParquetTypeName, convertedType?: ParquetConvertedType): IcebergType`

Converts Parquet type to Iceberg type.

### `parquetSchemaToIceberg(parquetSchema: ParquetSchemaElement[], startFieldId?: number): IcebergSchema`

Converts a Parquet schema to Iceberg schema.

### `validateSchemaEvolution(oldSchema: IcebergSchema, newSchema: IcebergSchema): SchemaComparisonResult`

Validates schema evolution compatibility.

```typescript
const result = validateSchemaEvolution(oldSchema, newSchema);
if (!result.compatible) {
  console.log('Breaking changes:', result.breakingChanges);
}
```

### `findMaxFieldId(schema: IcebergSchema): number`

Finds the maximum field ID in a schema.

### `generateSchemaId(existingSchemas: IcebergSchema[]): number`

Generates a new schema ID.

---

## Partition Transform Functions

### `applyTransform(value: unknown, transform: PartitionTransform | string, transformArg?: number): unknown`

Applies a partition transform to a value.

```typescript
applyTransform(new Date('2024-06-15'), 'year');  // Returns 54 (years since 1970)
applyTransform(new Date('2024-06-15'), 'month'); // Returns 653
applyTransform(new Date('2024-06-15'), 'day');   // Returns 19889
applyTransform('hello world', 'truncate', 5);    // Returns 'hello'
applyTransform('user123', 'bucket', 16);         // Returns bucket number 0-15
```

### `parseTransform(transform: string): ParsedTransform`

Parses a transform string like `bucket[16]`.

### `formatTransform(parsed: ParsedTransform): string`

Formats a parsed transform back to string.

### `getTransformResultType(sourceType: string, transform: PartitionTransform | string): string`

Gets the result type of a transform.

### `getPartitionData(record: Record<string, unknown>, spec: PartitionSpec, schema: IcebergSchema): Record<string, unknown>`

Gets partition data for a record.

### `getPartitionPath(partitionData: Record<string, unknown>, spec: PartitionSpec): string`

Generates Hive-style partition path.

```typescript
getPartitionPath({ year: 54, month: 6 }, spec);
// Returns: "year=54/month=6"
```

### `parsePartitionPath(path: string): Record<string, unknown>`

Parses a partition path back to data.

---

## Column Statistics

### `createColumnStatsCollector(fieldId: number, fieldType: IcebergType): ColumnStatsCollector`

Creates a collector for column statistics.

### `FileStatsCollector`

Class for collecting statistics for an entire file.

```typescript
const collector = new FileStatsCollector(schema);
for (const record of records) {
  collector.addRecord(record);
}
const stats = collector.getStats();
```

### `encodeFileStats(stats: ComputedFileStats, schema: IcebergSchema): ComputedFileStats`

Encodes statistics for storage.

### `applyStatsToDataFile(dataFile: DataFile, stats: ComputedFileStats): DataFile`

Applies statistics to a data file entry.

### `aggregateColumnStats(statsList: ColumnStatistics[]): ColumnStatistics`

Aggregates statistics from multiple files.

### `computePartitionSummaries(files: DataFile[], spec: PartitionSpec): PartitionFieldSummary[]`

Computes partition summaries for manifest entries.

---

## Row-Level Delete Utilities

### `PositionDeleteBuilder`

Builder for creating position delete files.

```typescript
const builder = new PositionDeleteBuilder({
  targetFilePath: 's3://bucket/data/file.parquet',
});
builder.addDelete(100); // Delete row at position 100
builder.addDelete(200);
const result = builder.build();
```

### `EqualityDeleteBuilder`

Builder for creating equality delete files.

```typescript
const builder = new EqualityDeleteBuilder({
  schema: deleteSchema,
  equalityFieldIds: [1, 2], // Fields to match
});
builder.addDelete({ id: 'user123', timestamp: 1234567890 });
const result = builder.build();
```

### `DeleteManifestGenerator`

Generates delete manifests.

### `PositionDeleteLookup`

Lookup structure for applying position deletes during reads.

```typescript
const lookup = new PositionDeleteLookup();
lookup.addDeleteFile(deleteFile);
if (lookup.isDeleted(filePath, position)) {
  // Skip this row
}
```

### `EqualityDeleteLookup`

Lookup structure for applying equality deletes.

### `applyDeletes(records: Iterable<Record<string, unknown>>, positionLookup?: PositionDeleteLookup, equalityLookup?: EqualityDeleteLookup): DeleteApplicationResult`

Applies deletes to a record stream.

### Type Guards

- `isDeleteFile(dataFile: DataFile): boolean`
- `isPositionDeleteFile(dataFile: DataFile): boolean`
- `isEqualityDeleteFile(dataFile: DataFile): boolean`

---

## Error Classes

### `IcebergError`

Base error class for all Iceberg-related errors.

```typescript
class IcebergError extends Error {
  readonly code: string;
}
```

### `MetadataError`

Error during metadata operations.

```typescript
class MetadataError extends IcebergError {
  readonly code: MetadataErrorCode;
  readonly metadataPath?: string;
}
```

**Error codes:** `METADATA_NOT_FOUND`, `METADATA_PARSE_ERROR`, `METADATA_WRITE_ERROR`, `METADATA_VERSION_MISMATCH`, `INVALID_FORMAT_VERSION`, `MISSING_REQUIRED_FIELD`, `SCHEMA_NOT_FOUND`, `SNAPSHOT_NOT_FOUND`

### `CatalogError`

Error during catalog operations.

**Error codes:** `TABLE_NOT_FOUND`, `TABLE_ALREADY_EXISTS`, `NAMESPACE_NOT_FOUND`, `CATALOG_CONNECTION_ERROR`, `CATALOG_PERMISSION_DENIED`

### `StorageError`

Error during storage operations.

**Error codes:** `STORAGE_READ_ERROR`, `STORAGE_WRITE_ERROR`, `FILE_NOT_FOUND`, `PERMISSION_DENIED`

### `ValidationError`

Error during validation.

**Error codes:** `INVALID_SCHEMA`, `INVALID_PARTITION_SPEC`, `TYPE_MISMATCH`, `DUPLICATE_FIELD_ID`

### `CommitConflictError`

Error when commit fails due to concurrent modification.

```typescript
class CommitConflictError extends IcebergError {
  readonly expectedVersion: number;
  readonly actualVersion: number;
}
```

### `CommitRetryExhaustedError`

Error when max retries are exhausted.

```typescript
class CommitRetryExhaustedError extends IcebergError {
  readonly attempts: number;
  readonly lastError: Error;
}
```

### `SchemaEvolutionError`

Error during schema evolution.

**Error codes:** `FIELD_NOT_FOUND`, `FIELD_EXISTS`, `INCOMPATIBLE_TYPE`, `REQUIRED_FIELD_NO_DEFAULT`, `INVALID_POSITION`

### `TransformError`

Error during partition transform operations.

### Type Guards

```typescript
isIcebergError(error: unknown): error is IcebergError
isMetadataError(error: unknown): error is MetadataError
isCatalogError(error: unknown): error is CatalogError
isStorageError(error: unknown): error is StorageError
isValidationError(error: unknown): error is ValidationError
isCommitConflictError(error: unknown): error is CommitConflictError
isSchemaEvolutionError(error: unknown): error is SchemaEvolutionError
wrapError(error: unknown, defaultMessage?: string): IcebergError
```

---

## Types and Interfaces

### Core Types

```typescript
// Primitive types
type IcebergPrimitiveType =
  | 'boolean' | 'int' | 'long' | 'float' | 'double'
  | 'decimal' | 'date' | 'time' | 'timestamp' | 'timestamptz'
  | 'string' | 'uuid' | 'fixed' | 'binary';

// Complex types
interface IcebergListType {
  type: 'list';
  'element-id': number;
  element: IcebergType;
  'element-required': boolean;
}

interface IcebergMapType {
  type: 'map';
  'key-id': number;
  'value-id': number;
  key: IcebergType;
  value: IcebergType;
  'value-required': boolean;
}

interface IcebergStructField {
  id: number;
  name: string;
  required: boolean;
  type: IcebergType;
  doc?: string;
}

interface IcebergStructType {
  type: 'struct';
  fields: IcebergStructField[];
}

type IcebergType = IcebergPrimitiveType | IcebergListType | IcebergMapType | IcebergStructType;
```

### Schema

```typescript
interface IcebergSchema {
  'schema-id': number;
  type: 'struct';
  fields: IcebergStructField[];
}
```

### Partition and Sort

```typescript
type PartitionTransform = 'identity' | 'bucket' | 'truncate' | 'year' | 'month' | 'day' | 'hour' | 'void';

interface PartitionField {
  'source-id': number;
  'field-id': number;
  name: string;
  transform: PartitionTransform | string;
}

interface PartitionSpec {
  'spec-id': number;
  fields: PartitionField[];
}

interface SortField {
  'source-id': number;
  transform: string;
  direction: 'asc' | 'desc';
  'null-order': 'nulls-first' | 'nulls-last';
}

interface SortOrder {
  'order-id': number;
  fields: SortField[];
}
```

### Data File

```typescript
type ContentType = 'data' | 'position-deletes' | 'equality-deletes';
type FileFormat = 'parquet' | 'avro' | 'orc';

interface DataFile {
  content: number;  // 0=data, 1=position-deletes, 2=equality-deletes
  'file-path': string;
  'file-format': FileFormat;
  partition: Record<string, unknown>;
  'record-count': number;
  'file-size-in-bytes': number;
  'column-sizes'?: Record<number, number>;
  'value-counts'?: Record<number, number>;
  'null-value-counts'?: Record<number, number>;
  'nan-value-counts'?: Record<number, number>;
  'lower-bounds'?: Record<number, Uint8Array | string>;
  'upper-bounds'?: Record<number, Uint8Array | string>;
  'equality-ids'?: number[];
  'sort-order-id'?: number;
}
```

### Manifest

```typescript
type ManifestEntryStatus = 0 | 1 | 2;  // EXISTING, ADDED, DELETED

interface ManifestEntry {
  status: ManifestEntryStatus;
  'snapshot-id': number;
  'sequence-number': number;
  'file-sequence-number': number;
  'data-file': DataFile;
}

interface ManifestFile {
  'manifest-path': string;
  'manifest-length': number;
  'partition-spec-id': number;
  content: number;  // 0=data, 1=deletes
  'sequence-number': number;
  'min-sequence-number': number;
  'added-snapshot-id': number;
  'added-files-count': number;
  'existing-files-count': number;
  'deleted-files-count': number;
  'added-rows-count': number;
  'existing-rows-count': number;
  'deleted-rows-count': number;
  partitions?: PartitionFieldSummary[];
}
```

### Snapshot

```typescript
interface SnapshotSummary {
  operation: 'append' | 'replace' | 'overwrite' | 'delete';
  'added-data-files'?: string;
  'deleted-data-files'?: string;
  'added-records'?: string;
  'deleted-records'?: string;
  'total-records'?: string;
  'total-data-files'?: string;
  [key: string]: string | undefined;
}

interface SnapshotRef {
  'snapshot-id': number;
  type: 'branch' | 'tag';
  'max-ref-age-ms'?: number;
  'max-snapshot-age-ms'?: number;
  'min-snapshots-to-keep'?: number;
}

interface Snapshot {
  'snapshot-id': number;
  'parent-snapshot-id'?: number;
  'sequence-number': number;
  'timestamp-ms': number;
  'manifest-list': string;
  summary: SnapshotSummary;
  'schema-id': number;
}
```

### Table Metadata

```typescript
interface TableMetadata {
  'format-version': 2;
  'table-uuid': string;
  location: string;
  'last-sequence-number': number;
  'last-updated-ms': number;
  'last-column-id': number;
  'current-schema-id': number;
  schemas: IcebergSchema[];
  'default-spec-id': number;
  'partition-specs': PartitionSpec[];
  'last-partition-id': number;
  'default-sort-order-id': number;
  'sort-orders': SortOrder[];
  properties: Record<string, string>;
  'current-snapshot-id': number | null;
  snapshots: Snapshot[];
  'snapshot-log': SnapshotLogEntry[];
  'metadata-log': MetadataLogEntry[];
  refs: Record<string, SnapshotRef>;
}
```

### Storage Backend

```typescript
interface StorageBackend {
  get(key: string): Promise<Uint8Array | null>;
  put(key: string, data: Uint8Array): Promise<void>;
  delete(key: string): Promise<void>;
  list(prefix: string): Promise<string[]>;
  exists(key: string): Promise<boolean>;
}
```

---

## Constants

```typescript
// Format version
const FORMAT_VERSION = 2;

// Metadata directory
const METADATA_DIR = 'metadata';
const VERSION_HINT_FILENAME = 'version-hint.text';

// Field IDs
const PARTITION_FIELD_ID_START = 1000;
const INITIAL_PARTITION_ID = 999;

// Content types
const CONTENT_DATA = 0;
const CONTENT_POSITION_DELETES = 1;
const CONTENT_EQUALITY_DELETES = 2;

// Manifest content types
const MANIFEST_CONTENT_DATA = 0;
const MANIFEST_CONTENT_DELETES = 1;

// Manifest entry status
const MANIFEST_ENTRY_STATUS_EXISTING = 0;
const MANIFEST_ENTRY_STATUS_ADDED = 1;
const MANIFEST_ENTRY_STATUS_DELETED = 2;

// Default IDs
const DEFAULT_SCHEMA_ID = 0;
const DEFAULT_SPEC_ID = 0;
const DEFAULT_SORT_ORDER_ID = 0;

// Commit constants
const COMMIT_MAX_RETRIES = 5;
const DEFAULT_BASE_RETRY_DELAY_MS = 100;
const DEFAULT_MAX_RETRY_DELAY_MS = 5000;
const DEFAULT_RETRY_JITTER = 0.2;
const METADATA_RETAIN_VERSIONS = 10;
const METADATA_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000; // 7 days

// Time constants
const MS_PER_DAY = 86400000;
const MS_PER_HOUR = 3600000;
const EPOCH_YEAR = 1970;
```

---

## Additional Resources

- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [Schema Evolution](https://iceberg.apache.org/spec/#schema-evolution)
- [Partitioning](https://iceberg.apache.org/spec/#partitioning)
- [Parquet Bloom Filter Specification](https://parquet.apache.org/docs/file-format/bloomfilter/)
