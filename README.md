# Iceberg

Apache Iceberg implementation for JavaScript/TypeScript with a managed REST Catalog service.

## Overview

[Apache Iceberg](https://iceberg.apache.org/) is an open table format for huge analytic datasets. It provides:

- **Time travel** - Query data at any point in history
- **Schema evolution** - Add, drop, rename, or reorder columns without rewriting data
- **Partition evolution** - Change partitioning schemes without data migration
- **Hidden partitioning** - Users query logical columns; Iceberg handles physical partitioning
- **ACID transactions** - Reliable multi-reader, single-writer concurrent operations

This repository provides:

1. **@dotdo/iceberg** - TypeScript implementation for Iceberg metadata operations
2. **iceberg.do** - Managed Iceberg REST Catalog service on Cloudflare Workers

## Packages

### @dotdo/iceberg (core/)

TypeScript library for Apache Iceberg table format operations:

- **Metadata** - Read/write Iceberg metadata.json files
- **Manifests** - Generate manifest files and manifest lists
- **Snapshots** - Create and manage table snapshots with time travel
- **Schema** - Schema creation, conversion (Parquet to Iceberg), and evolution validation
- **Avro** - Binary encoding for manifest files per Iceberg spec
- **Catalog** - Catalog implementations (filesystem, memory, R2 Data Catalog client)

```typescript
import {
  MetadataWriter,
  ManifestGenerator,
  SnapshotBuilder,
  createDefaultSchema,
} from '@dotdo/iceberg';

// Create table metadata
const writer = new MetadataWriter(storage);
const result = await writer.writeNewTable({
  location: 's3://bucket/warehouse/db/table',
});

// Create a manifest
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
```

### iceberg.do (service/)

Managed Iceberg REST Catalog service running on Cloudflare Workers:

- **Iceberg REST Catalog API** - Full implementation of the Iceberg REST spec
- **Durable Object SQLite** - Fast metadata storage with SQLite in Durable Objects
- **R2 Integration** - Store table data in Cloudflare R2
- **oauth.do Integration** - User-level authentication with FGA/RBAC
- **No API Keys Required** - Alternative to R2 Data Catalog with user-based auth

```bash
# Deploy to Cloudflare Workers
cd service
pnpm install
pnpm run deploy
```

## Architecture

### Three-Tier Catalog Access

1. **Tier 1: Direct Metadata** - Read/write metadata files directly to storage (S3/R2/filesystem)
2. **Tier 2: R2 Data Catalog Client** - Use Cloudflare's R2 Data Catalog API for managed access
3. **Tier 3: iceberg.do REST Server** - Full REST Catalog with auth, running on Cloudflare Workers

### Why iceberg.do?

| Feature | R2 Data Catalog | iceberg.do |
|---------|-----------------|------------|
| Authentication | API Key only | OAuth + User-level auth |
| Authorization | Account-level | Fine-grained RBAC/FGA |
| Metadata Store | R2 | DO SQLite (faster) |
| Query Engine Support | DuckDB, Spark, Trino | Same + any REST client |
| Multi-tenant | No | Yes |

## Getting Started

### Installation

```bash
npm install @dotdo/iceberg
# or
pnpm add @dotdo/iceberg
```

### Quick Start

Here's a minimal working example to create an Iceberg table:

```typescript
import { MetadataWriter, MemoryCatalog } from '@dotdo/iceberg';

// Create a simple in-memory storage backend
const storage = new Map<string, Uint8Array>();
const storageBackend = {
  get: async (key: string) => storage.get(key) ?? null,
  put: async (key: string, data: Uint8Array) => { storage.set(key, data); },
  delete: async (key: string) => { storage.delete(key); },
  list: async (prefix: string) => [...storage.keys()].filter(k => k.startsWith(prefix)),
  exists: async (key: string) => storage.has(key),
};

// Create a new table
const writer = new MetadataWriter(storageBackend);
const result = await writer.writeNewTable({
  location: 's3://my-bucket/warehouse/db/my-table',
});

console.log('Table created:', result.metadata['table-uuid']);
console.log('Metadata location:', result.metadataLocation);
```

### Common Operations

#### Creating a Table with Custom Schema

```typescript
import {
  MetadataWriter,
  TableMetadataBuilder,
  createTimePartitionSpec,
} from '@dotdo/iceberg';

// Define a custom schema
const schema = {
  'schema-id': 0,
  type: 'struct' as const,
  fields: [
    { id: 1, name: 'user_id', required: true, type: 'long' },
    { id: 2, name: 'created_at', required: true, type: 'timestamp' },
    { id: 3, name: 'event_type', required: true, type: 'string' },
    { id: 4, name: 'payload', required: false, type: 'string' },
  ],
};

// Create a day-partitioned table
const partitionSpec = createTimePartitionSpec(2, 'created_day', 'day');

const writer = new MetadataWriter(storageBackend);
const result = await writer.writeNewTable({
  location: 's3://bucket/warehouse/events',
  schema,
  partitionSpec,
  properties: { 'write.parquet.compression': 'zstd' },
});
```

#### Adding a Snapshot

```typescript
import {
  ManifestGenerator,
  ManifestListGenerator,
  SnapshotBuilder,
  TableMetadataBuilder,
} from '@dotdo/iceberg';

// 1. Create a manifest with data files
const manifest = new ManifestGenerator({
  sequenceNumber: 1,
  snapshotId: Date.now(),
});

manifest.addDataFile({
  'file-path': 's3://bucket/data/part-00000.parquet',
  'file-format': 'parquet',
  'record-count': 10000,
  'file-size-in-bytes': 102400,
  partition: { created_day: 19890 }, // days since epoch
});

// 2. Create a manifest list
const manifestList = new ManifestListGenerator({
  snapshotId: Date.now(),
  sequenceNumber: 1,
});

manifestList.addManifestWithStats(
  's3://bucket/metadata/manifest-001.avro',
  4096,
  0,
  manifest.generate().summary
);

// 3. Build the snapshot
const snapshot = new SnapshotBuilder({
  sequenceNumber: 1,
  manifestListPath: 's3://bucket/metadata/snap-001.avro',
})
  .setSummary(1, 0, 10000, 0, 102400, 0, 10000, 102400, 1)
  .build();

// 4. Write updated metadata
const updated = await writer.writeWithSnapshot(
  result.metadata,
  snapshot,
  result.metadataLocation
);
```

#### Schema Evolution

```typescript
import { SchemaEvolutionBuilder } from '@dotdo/iceberg';

const builder = new SchemaEvolutionBuilder(existingSchema);

// Add a new column
builder.addColumn('email', 'string', false, 'User email address');

// Rename a column
builder.renameColumn('payload', 'data');

// Widen a type (int -> long)
builder.updateColumnType('count', 'long');

// Apply changes
const result = builder.build();
console.log('New schema ID:', result.schemaId);
```

#### Time Travel Queries

```typescript
import { getSnapshotAtTimestamp, getSnapshotById, readTableMetadata } from '@dotdo/iceberg';

const metadata = await readTableMetadata(storageBackend, 's3://bucket/warehouse/events');

// Query at a specific timestamp (24 hours ago)
const historicalSnapshot = getSnapshotAtTimestamp(
  metadata,
  Date.now() - 24 * 60 * 60 * 1000
);

// Or query by snapshot ID
const specificSnapshot = getSnapshotById(metadata, 1234567890);

console.log('Snapshot manifest list:', historicalSnapshot?.['manifest-list']);
```

### Using with iceberg.do REST Catalog

[iceberg.do](https://iceberg.do) is a managed Iceberg REST Catalog service with OAuth authentication.

#### Authentication

```typescript
// Using fetch with OAuth token
const response = await fetch('https://iceberg.do/v1/namespaces', {
  headers: {
    'Authorization': `Bearer ${accessToken}`,
  },
});
```

#### Creating Namespaces and Tables via REST API

```typescript
// Create a namespace
await fetch('https://iceberg.do/v1/namespaces', {
  method: 'POST',
  headers: {
    'Authorization': `Bearer ${accessToken}`,
    'Content-Type': 'application/json',
  },
  body: JSON.stringify({
    namespace: ['analytics'],
    properties: { owner: 'data-team' },
  }),
});

// Create a table
await fetch('https://iceberg.do/v1/namespaces/analytics/tables', {
  method: 'POST',
  headers: {
    'Authorization': `Bearer ${accessToken}`,
    'Content-Type': 'application/json',
  },
  body: JSON.stringify({
    name: 'events',
    schema: {
      'schema-id': 0,
      type: 'struct',
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'data', required: false, type: 'string' },
      ],
    },
  }),
});

// Load table metadata
const tableResponse = await fetch(
  'https://iceberg.do/v1/namespaces/analytics/tables/events',
  { headers: { 'Authorization': `Bearer ${accessToken}` } }
);
const tableMetadata = await tableResponse.json();
```

### Next Steps

- **[API Reference](./core/API.md)** - Complete API documentation with all classes and methods
- **[Changelog](./CHANGELOG.md)** - Version history and release notes
- **[Apache Iceberg Spec](https://iceberg.apache.org/spec/)** - Official Iceberg specification

### Development

```bash
# Clone and install
git clone https://github.com/dot-do/iceberg.git
cd iceberg
pnpm install

# Build all packages
pnpm build

# Run tests
pnpm test

# Start the REST catalog service locally
cd service
pnpm dev
```

## Project Structure

```
iceberg/
├── core/                     # @dotdo/iceberg package
│   ├── src/
│   │   ├── index.ts          # Main exports
│   │   ├── metadata/         # Metadata operations
│   │   │   ├── types.ts      # Type definitions
│   │   │   ├── schema.ts     # Schema utilities
│   │   │   ├── reader.ts     # Metadata reader
│   │   │   ├── writer.ts     # Metadata writer
│   │   │   ├── manifest.ts   # Manifest generation
│   │   │   └── snapshot.ts   # Snapshot management
│   │   ├── avro/             # Avro encoding
│   │   └── catalog/          # Catalog implementations
│   └── tests/
├── service/                  # iceberg.do managed service
│   ├── src/
│   │   ├── index.ts          # Worker entry point
│   │   ├── routes.ts         # REST API routes
│   │   ├── catalog/          # DO SQLite backend
│   │   └── auth/             # oauth.do integration
│   ├── wrangler.toml
│   └── tests/
├── package.json              # Workspace root
└── pnpm-workspace.yaml
```

## API Reference

### Core Library

#### Schema Creation

```typescript
import {
  createDefaultSchema,
  createIdentityPartitionSpec,
  createTimePartitionSpec,
  createSortOrder,
} from '@dotdo/iceberg';

// Create a custom schema
const schema = {
  'schema-id': 0,
  type: 'struct',
  fields: [
    { id: 1, name: 'user_id', required: true, type: 'long' },
    { id: 2, name: 'created_at', required: true, type: 'timestamp' },
    { id: 3, name: 'data', required: false, type: 'string' },
  ],
};

// Partition by day
const partitionSpec = createTimePartitionSpec(2, 'created_day', 'day');
```

#### Table Operations

```typescript
import { TableMetadataBuilder, SnapshotBuilder } from '@dotdo/iceberg';

// Create table metadata
const builder = new TableMetadataBuilder({
  location: 's3://bucket/warehouse/db/users',
  schema,
  partitionSpec,
});

// Add a snapshot
const snapshot = new SnapshotBuilder({
  sequenceNumber: 1,
  manifestListPath: 's3://bucket/warehouse/db/users/metadata/snap-1.avro',
});
snapshot.setSummary(10, 0, 10000, 0, 102400, 0, 10000, 102400, 10);

builder.addSnapshot(snapshot.build());
const metadata = builder.build();
```

### REST Catalog API

The service implements the [Iceberg REST Catalog specification](https://iceberg.apache.org/spec/#iceberg-rest-catalog):

```
GET  /v1/config                              # Get catalog config
GET  /v1/namespaces                          # List namespaces
POST /v1/namespaces                          # Create namespace
GET  /v1/namespaces/{namespace}              # Get namespace
DELETE /v1/namespaces/{namespace}            # Drop namespace
GET  /v1/namespaces/{namespace}/tables       # List tables
POST /v1/namespaces/{namespace}/tables       # Create table
GET  /v1/namespaces/{namespace}/tables/{t}   # Load table
POST /v1/namespaces/{namespace}/tables/{t}   # Commit changes
DELETE /v1/namespaces/{namespace}/tables/{t} # Drop table
POST /v1/tables/rename                       # Rename table
```

## License

MIT

## Links

- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [Iceberg REST Catalog API](https://iceberg.apache.org/spec/#iceberg-rest-catalog)
- [iceberg.do](https://iceberg.do)
- [oauth.do](https://oauth.do)
