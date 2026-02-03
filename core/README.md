# @dotdo/iceberg

TypeScript implementation of the [Apache Iceberg](https://iceberg.apache.org/) table format.

## Installation

```bash
npm install @dotdo/iceberg
# or
pnpm add @dotdo/iceberg
```

## Features

- **Metadata** - Read/write Iceberg metadata.json files
- **Manifests** - Generate manifest files and manifest lists with Avro encoding
- **Snapshots** - Create and manage table snapshots with time travel
- **Schema Evolution** - Add, drop, rename columns with validation
- **Variant Shredding** - Decompose semi-structured data for efficient queries
- **Catalogs** - FileSystem, Memory, and R2 Data Catalog implementations

## Quick Start

```typescript
import { MetadataWriter } from '@dotdo/iceberg';

// Create a storage backend (implement the Storage interface)
const storage = {
  get: async (key: string) => /* ... */,
  put: async (key: string, data: Uint8Array) => /* ... */,
  delete: async (key: string) => /* ... */,
  list: async (prefix: string) => /* ... */,
  exists: async (key: string) => /* ... */,
};

// Create a new table
const writer = new MetadataWriter(storage);
const result = await writer.writeNewTable({
  location: 's3://my-bucket/warehouse/db/my-table',
});

console.log('Table UUID:', result.metadata['table-uuid']);
```

## Common Operations

### Creating a Table with Custom Schema

```typescript
import { MetadataWriter, createTimePartitionSpec } from '@dotdo/iceberg';

const schema = {
  'schema-id': 0,
  type: 'struct' as const,
  fields: [
    { id: 1, name: 'user_id', required: true, type: 'long' },
    { id: 2, name: 'created_at', required: true, type: 'timestamp' },
    { id: 3, name: 'event_type', required: true, type: 'string' },
  ],
};

const partitionSpec = createTimePartitionSpec(2, 'created_day', 'day');

const result = await writer.writeNewTable({
  location: 's3://bucket/warehouse/events',
  schema,
  partitionSpec,
});
```

### Adding a Snapshot

```typescript
import { ManifestGenerator, SnapshotBuilder } from '@dotdo/iceberg';

// Create a manifest with data files
const manifest = new ManifestGenerator({
  sequenceNumber: 1,
  snapshotId: Date.now(),
});

manifest.addDataFile({
  'file-path': 's3://bucket/data/part-00000.parquet',
  'file-format': 'parquet',
  'record-count': 10000,
  'file-size-in-bytes': 102400,
  partition: { created_day: 19890 },
});

// Build the snapshot
const snapshot = new SnapshotBuilder({
  sequenceNumber: 1,
  manifestListPath: 's3://bucket/metadata/snap-001.avro',
})
  .setSummary(1, 0, 10000, 0, 102400, 0, 10000, 102400, 1)
  .build();
```

### Schema Evolution

```typescript
import { SchemaEvolutionBuilder } from '@dotdo/iceberg';

const builder = new SchemaEvolutionBuilder(existingSchema);
builder.addColumn('email', 'string', false, 'User email');
builder.renameColumn('payload', 'data');
const result = builder.build();
```

### Time Travel

```typescript
import { getSnapshotAtTimestamp, readTableMetadata } from '@dotdo/iceberg';

const metadata = await readTableMetadata(storage, 's3://bucket/warehouse/events');
const snapshot = getSnapshotAtTimestamp(metadata, Date.now() - 24 * 60 * 60 * 1000);
```

## Variant Shredding

Decompose semi-structured JSON data into typed columns for efficient querying:

```typescript
import { setupVariantShredding, filterDataFilesWithStats } from '@dotdo/iceberg';

const { configs, fieldIdMap } = setupVariantShredding([
  {
    columnName: '$data',
    fields: ['event_type', 'user_id', 'amount'],
    fieldTypes: {
      event_type: 'string',
      user_id: 'long',
      amount: 'double',
    },
  },
]);

// Filter files using predicate pushdown
const filter = { '$data.amount': { $gt: 100 } };
const { files } = filterDataFilesWithStats(dataFiles, filter, configs, fieldIdMap);
```

## Catalog Implementations

```typescript
import { FileSystemCatalog, MemoryCatalog } from '@dotdo/iceberg';

// In-memory catalog for testing
const memoryCatalog = new MemoryCatalog();

// FileSystem catalog for production
const fsCatalog = new FileSystemCatalog({
  storage,
  warehouseLocation: 's3://bucket/warehouse',
});
```

## Documentation

- [Full Documentation](https://github.com/dot-do/iceberg#readme)
- [API Reference](https://github.com/dot-do/iceberg/blob/main/core/API.md)
- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)

## Related

- [iceberg.do](https://iceberg.do) - Managed Iceberg REST Catalog service

## License

MIT
