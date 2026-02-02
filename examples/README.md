# @dotdo/iceberg Examples

This directory contains runnable examples demonstrating the key features of `@dotdo/iceberg`.

## Prerequisites

Before running the examples, ensure you have:

1. Node.js 18+ installed
2. The project dependencies installed:

```bash
# From the repository root
pnpm install
pnpm build
```

## Import Paths

The examples in this directory use relative imports for local development:

```typescript
// Local development (in this repo)
import { TableMetadataBuilder } from '../core/src/index.js';
```

When using `@dotdo/iceberg` as an installed package, use the package imports:

```typescript
// Installed package
import { TableMetadataBuilder } from '@dotdo/iceberg';
import { MemoryCatalog } from '@dotdo/iceberg/catalog';
```

## Running Examples

Each example is a self-contained TypeScript file that can be run directly with `tsx`:

```bash
# Run from the repository root
npx tsx examples/01-basic-table.ts
npx tsx examples/02-schema-evolution.ts
npx tsx examples/03-snapshots-time-travel.ts
npx tsx examples/04-partition-management.ts
npx tsx examples/05-catalog-operations.ts
```

Or run all examples:

```bash
for f in examples/0*.ts; do echo "=== Running $f ==="; npx tsx "$f"; echo; done
```

## Example Descriptions

### 01-basic-table.ts - Basic Table Creation

Demonstrates the fundamentals of creating an Iceberg table:

- Creating a storage backend (in-memory for demonstration)
- Defining a custom schema with various Iceberg types
- Building table metadata using `TableMetadataBuilder`
- Writing metadata using `MetadataWriter`
- Reading back and verifying stored metadata

**Key concepts:** `TableMetadataBuilder`, `MetadataWriter`, `IcebergSchema`, `StorageBackend`

### 02-schema-evolution.ts - Schema Evolution

Shows how to evolve Iceberg table schemas while maintaining compatibility:

- Adding new columns to a schema
- Renaming existing columns
- Dropping columns
- Promoting column types (e.g., `int` to `long`)
- Viewing schema history
- Comparing schemas to detect changes
- Checking backward compatibility

**Key concepts:** `SchemaEvolutionBuilder`, `compareSchemas`, `isBackwardCompatible`, `getSchemaHistory`

### 03-snapshots-time-travel.ts - Snapshots and Time Travel

Demonstrates Iceberg's snapshot-based versioning for time travel queries:

- Creating snapshots with `SnapshotBuilder`
- Adding snapshots to table metadata
- Creating branches and tags
- Time travel queries (query at specific timestamp)
- Querying by reference (branch/tag)
- Querying by snapshot ID
- Viewing snapshot history and statistics
- Using `SnapshotManager` for advanced operations

**Key concepts:** `SnapshotBuilder`, `SnapshotManager`, `getSnapshotAtTimestamp`, `getSnapshotByRef`

### 04-partition-management.ts - Partition Transforms

Shows how to partition data efficiently using Iceberg transforms:

- Understanding partition transforms:
  - `identity`: Exact value partitioning
  - `bucket[N]`: Hash partitioning into N buckets
  - `truncate[W]`: Truncation partitioning
  - `year/month/day/hour`: Temporal partitioning
- Building partition specs with `PartitionSpecBuilder`
- Generating partition data for records
- Creating partition paths (Hive-style)
- Parsing partition paths back to data
- Multi-level partitioning strategies

**Key concepts:** `PartitionSpecBuilder`, `applyTransform`, `getPartitionData`, `getPartitionPath`

### 05-catalog-operations.ts - Catalog Usage

Demonstrates the Iceberg catalog for managing tables:

- Creating and configuring a catalog
- Namespace management:
  - Create namespaces with properties
  - List namespaces (hierarchical)
  - Get/update namespace properties
  - Drop namespaces
- Table management:
  - Create tables with schemas and partition specs
  - List tables in a namespace
  - Load table metadata
  - Check table existence
  - Rename tables
  - Drop tables
- Atomic commits for updating table properties

**Key concepts:** `MemoryCatalog`, `TableIdentifier`, `CreateTableRequest`, `commitTable`

## API Quick Reference

### Core Classes

| Class | Purpose |
|-------|---------|
| `TableMetadataBuilder` | Build new table metadata |
| `MetadataWriter` | Write metadata to storage |
| `SnapshotBuilder` | Build individual snapshots |
| `SnapshotManager` | Manage snapshot lifecycle |
| `SchemaEvolutionBuilder` | Evolve schemas safely |
| `PartitionSpecBuilder` | Build partition specifications |
| `MemoryCatalog` | In-memory catalog for testing |
| `FileSystemCatalog` | Filesystem-based catalog |

### Key Types

| Type | Description |
|------|-------------|
| `IcebergSchema` | Table schema definition |
| `TableMetadata` | Complete table metadata |
| `Snapshot` | Immutable table state |
| `PartitionSpec` | Partition specification |
| `StorageBackend` | Storage abstraction interface |
| `TableIdentifier` | Namespace + table name |

### Common Operations

```typescript
// Create a table
const builder = new TableMetadataBuilder({
  location: 'warehouse/db/table',
  schema: mySchema,
});
const metadata = builder.build();

// Evolve a schema
const evolution = new SchemaEvolutionBuilder(currentSchema, metadata);
evolution.addColumn('new_field', 'string');
const result = evolution.buildWithMetadata();

// Build partition spec
const spec = new PartitionSpecBuilder(schema)
  .day('timestamp')
  .bucket('user_id', 16)
  .build();

// Create snapshot
const snapshot = new SnapshotBuilder({
  sequenceNumber: 1,
  manifestListPath: 'path/to/manifest-list.avro',
})
  .setSummary(addedFiles, 0, addedRecords, 0, size, 0, total, size, files)
  .build();

// Use catalog
const catalog = new MemoryCatalog({ name: 'my-catalog' });
await catalog.createNamespace(['db']);
await catalog.createTable(['db'], { name: 'users', schema: userSchema });
```

## Further Reading

- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [Iceberg Schema Evolution](https://iceberg.apache.org/spec/#schema-evolution)
- [Iceberg Partitioning](https://iceberg.apache.org/spec/#partitioning)
- [Iceberg Snapshots](https://iceberg.apache.org/spec/#snapshots)
