# MongoLake Integration Guide

This document describes the integration between MongoLake and @dotdo/iceberg, including the exact dependencies, required exports, and migration steps.

## Current State

### MongoLake's Iceberg Dependencies

MongoLake currently has an embedded Iceberg implementation at `mongolake/src/iceberg/`. It imports the following from `src/iceberg/snapshot.ts`:

```typescript
// File: mongolake/src/client/index.ts
import {
  SnapshotManager,
  type Snapshot,
  type TableMetadata,
} from '../iceberg/snapshot.js';
```

### Files in MongoLake's src/iceberg/

| File | Purpose |
|------|---------|
| `snapshot.ts` | SnapshotManager, SnapshotBuilder, TableMetadataBuilder, Snapshot/TableMetadata types |
| `metadata.ts` | IcebergSchema, PartitionSpec, SortOrder, ManifestGenerator, schema utilities |

## Required Exports from @dotdo/iceberg

The @dotdo/iceberg package must export all types and classes that MongoLake currently uses:

### Primary Exports (Required for MongoLake)

```typescript
// Classes
export { SnapshotManager } from '@dotdo/iceberg';

// Types
export type { Snapshot, TableMetadata } from '@dotdo/iceberg';
```

### Verification

These are already exported from `@dotdo/iceberg` in `core/src/index.ts`:

```typescript
// Snapshot (line 111-122)
export {
  SnapshotBuilder,
  TableMetadataBuilder,
  SnapshotManager,      // <-- Required
  generateUUID,
  createTableWithSnapshot,
  type CreateSnapshotOptions,
  type CreateTableOptions,
  type SnapshotRetentionPolicy,
  type ExpireSnapshotsResult,
} from './metadata/index.js';

// Types (line 44-78)
export type {
  // ...
  Snapshot,          // <-- Required
  TableMetadata,     // <-- Required
  // ...
} from './metadata/index.js';
```

## Migration Steps for MongoLake

### Step 1: Add Dependency

```bash
cd mongolake
pnpm add @dotdo/iceberg  # Or: "@dotdo/iceberg": "workspace:*" for monorepo
```

### Step 2: Update Imports

Change the import in `src/client/index.ts`:

```typescript
// Before
import {
  SnapshotManager,
  type Snapshot,
  type TableMetadata,
} from '../iceberg/snapshot.js';

// After
import {
  SnapshotManager,
  type Snapshot,
  type TableMetadata,
} from '@dotdo/iceberg';
```

### Step 3: Search for Other Imports

Search the MongoLake codebase for any other iceberg imports:

```bash
grep -r "from.*iceberg" mongolake/src/
```

Update any additional imports found.

### Step 4: Verify Build

```bash
cd mongolake
pnpm build
pnpm test
```

### Step 5: Remove Embedded Code

Once all tests pass:

```bash
rm -rf mongolake/src/iceberg/
```

### Step 6: Final Verification

```bash
pnpm build
pnpm test
```

## API Compatibility

### SnapshotManager

The `SnapshotManager` class in @dotdo/iceberg is API-compatible with MongoLake's embedded version:

| Method | MongoLake | @dotdo/iceberg |
|--------|-----------|----------------|
| `constructor(metadata, policy)` | Yes | Yes |
| `fromMetadata(metadata, policy)` | Yes | Yes |
| `getSnapshots()` | Yes | Yes |
| `getCurrentSnapshot()` | Yes | Yes |
| `getSnapshotById(id)` | Yes | Yes |
| `getSnapshotByRef(name)` | Yes | Yes |
| `getSnapshotAtTimestamp(ts)` | Yes | Yes |
| `createSnapshot(options)` | Yes | Yes |
| `addSnapshot(snapshot)` | Yes | Yes |
| `expireSnapshots(ts)` | Yes | Yes |
| `setRef(name, id, type)` | Yes | Yes |
| `getMetadata()` | Yes | Yes |

### Snapshot Type

The `Snapshot` interface is identical:

```typescript
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

### TableMetadata Type

The `TableMetadata` interface is identical:

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

## Additional Exports Available

MongoLake may optionally use these additional exports from @dotdo/iceberg:

### Schema Utilities
```typescript
import {
  createDefaultSchema,
  createUnpartitionedSpec,
  createUnsortedOrder,
  parquetToIcebergType,
  parquetSchemaToIceberg,
} from '@dotdo/iceberg';
```

### Manifest Generation
```typescript
import {
  ManifestGenerator,
  ManifestListGenerator,
} from '@dotdo/iceberg';
```

### Snapshot Building
```typescript
import {
  SnapshotBuilder,
  TableMetadataBuilder,
  createTableWithSnapshot,
} from '@dotdo/iceberg';
```

### Metadata I/O
```typescript
import {
  MetadataWriter,
  readTableMetadata,
  parseTableMetadata,
} from '@dotdo/iceberg';
```

## Related Issues

- [iceberg-2z9] [Epic] Core Package (@dotdo/iceberg) - Complete Extraction
- [iceberg-0yr] [Epic] MongoLake Integration
- [iceberg-9o4] Create migration plan document
- [iceberg-dub] Add @dotdo/iceberg as MongoLake dependency
- [iceberg-3zj] Update imports in MongoLake
- [iceberg-9vq] Remove src/iceberg/ directory from MongoLake
- [iceberg-7j6] Create integration tests
