/**
 * Integration Tests for @dotdo/iceberg Ecosystem Interoperability
 *
 * These tests verify that all components of the @dotdo/iceberg package work
 * together correctly, simulating real-world usage scenarios.
 *
 * Test Areas:
 * 1. Type compatibility - verify types are used consistently
 * 2. Storage backend abstraction - test MemoryStorage for catalog operations
 * 3. Round-trip serialization - create, serialize, parse, verify equality
 * 4. Snapshot workflow - create table, add snapshots, time travel
 * 5. Schema evolution workflow - evolve schema, verify old snapshots readable
 */

import { describe, it, expect, beforeEach } from 'vitest';

// Import from main package entry point to test public API
import {
  // Types
  type TableMetadata,
  type IcebergSchema,
  type PartitionSpec,
  type Snapshot,
  type StorageBackend,
  type DataFile,
  type ManifestFile,

  // Reader/Parser
  parseTableMetadata,
  getCurrentSnapshot,
  getSnapshotById,
  getSnapshotAtTimestamp,
  getSnapshotByRef,

  // Writer
  MetadataWriter,

  // Snapshot Management
  SnapshotBuilder,
  TableMetadataBuilder,
  SnapshotManager,
  generateUUID,

  // Schema utilities
  createDefaultSchema,
  createUnpartitionedSpec,
  createIdentityPartitionSpec,
  createBucketPartitionSpec,
  createTimePartitionSpec,
  createUnsortedOrder,
  createSortOrder,
  findMaxFieldId,

  // Schema Evolution
  SchemaEvolutionBuilder,
  evolveSchema,
  compareSchemas,
  isBackwardCompatible,
  isForwardCompatible,
  getSchemaForSnapshot,
  getSchemaHistory,

  // Constants
  FORMAT_VERSION,
} from '../../src/index.js';

// Import catalog components
import {
  MemoryCatalog,
  FileSystemCatalog,
  createCatalog,
  type TableIdentifier,
  type CreateTableRequest,
} from '../../src/catalog/index.js';

// ============================================================================
// Test Fixtures and Utilities
// ============================================================================

/**
 * Create an in-memory storage backend for testing.
 */
function createMemoryStorage(): StorageBackend & {
  data: Map<string, Uint8Array>;
  clear: () => void;
} {
  const data = new Map<string, Uint8Array>();

  /**
   * Helper to compare Uint8Array values.
   */
  function areEqual(a: Uint8Array, b: Uint8Array): boolean {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
      if (a[i] !== b[i]) return false;
    }
    return true;
  }

  return {
    data,

    async get(key: string): Promise<Uint8Array | null> {
      return data.get(key) ?? null;
    },

    async put(key: string, value: Uint8Array): Promise<void> {
      data.set(key, value);
    },

    async delete(key: string): Promise<void> {
      data.delete(key);
    },

    async list(prefix: string): Promise<string[]> {
      const results: string[] = [];
      for (const key of data.keys()) {
        if (key.startsWith(prefix)) {
          results.push(key);
        }
      }
      return results.sort();
    },

    async exists(key: string): Promise<boolean> {
      return data.has(key);
    },

    async putIfAbsent(key: string, value: Uint8Array): Promise<boolean> {
      if (data.has(key)) {
        return false;
      }
      data.set(key, value);
      return true;
    },

    async compareAndSwap(key: string, expected: Uint8Array | null, value: Uint8Array): Promise<boolean> {
      const current = data.get(key) ?? null;
      if (expected === null) {
        // Key must not exist
        if (current !== null) {
          return false;
        }
      } else {
        // Key must exist and match expected value
        if (current === null || !areEqual(current, expected)) {
          return false;
        }
      }
      data.set(key, value);
      return true;
    },

    clear(): void {
      data.clear();
    },
  };
}

/**
 * Create a test schema for users table.
 */
function createUsersSchema(): IcebergSchema {
  return {
    'schema-id': 0,
    type: 'struct',
    fields: [
      { id: 1, name: 'id', required: true, type: 'long' },
      { id: 2, name: 'name', required: true, type: 'string' },
      { id: 3, name: 'email', required: false, type: 'string' },
      { id: 4, name: 'created_at', required: true, type: 'timestamptz' },
    ],
  };
}

/**
 * Create a test schema for events table with more complex types.
 */
function createEventsSchema(): IcebergSchema {
  return {
    'schema-id': 0,
    type: 'struct',
    fields: [
      { id: 1, name: 'event_id', required: true, type: 'uuid' },
      { id: 2, name: 'user_id', required: true, type: 'long' },
      { id: 3, name: 'event_type', required: true, type: 'string' },
      { id: 4, name: 'event_time', required: true, type: 'timestamptz' },
      { id: 5, name: 'event_count', required: false, type: 'int' },
      { id: 6, name: 'event_value', required: false, type: 'float' },
      {
        id: 7,
        name: 'metadata',
        required: false,
        type: {
          type: 'struct',
          fields: [
            { id: 8, name: 'source', required: true, type: 'string' },
            { id: 9, name: 'version', required: false, type: 'string' },
          ],
        },
      },
    ],
  };
}

/**
 * Create a mock manifest file for testing.
 */
function createMockManifestFile(snapshotId: number, addedFiles: number = 1): ManifestFile {
  return {
    'manifest-path': `s3://bucket/metadata/snap-${snapshotId}-manifest.avro`,
    'manifest-length': 4096,
    'partition-spec-id': 0,
    content: 0,
    'sequence-number': 1,
    'min-sequence-number': 1,
    'added-snapshot-id': snapshotId,
    'added-files-count': addedFiles,
    'existing-files-count': 0,
    'deleted-files-count': 0,
    'added-rows-count': addedFiles * 1000,
    'existing-rows-count': 0,
    'deleted-rows-count': 0,
  };
}

// ============================================================================
// 1. Type Compatibility Tests
// ============================================================================

describe('Type Compatibility', () => {
  describe('IcebergSchema type consistency', () => {
    it('should create schemas with valid primitive types', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'bool_col', required: true, type: 'boolean' },
          { id: 2, name: 'int_col', required: true, type: 'int' },
          { id: 3, name: 'long_col', required: true, type: 'long' },
          { id: 4, name: 'float_col', required: true, type: 'float' },
          { id: 5, name: 'double_col', required: true, type: 'double' },
          { id: 6, name: 'decimal_col', required: true, type: 'decimal' },
          { id: 7, name: 'date_col', required: true, type: 'date' },
          { id: 8, name: 'time_col', required: true, type: 'time' },
          { id: 9, name: 'timestamp_col', required: true, type: 'timestamp' },
          { id: 10, name: 'timestamptz_col', required: true, type: 'timestamptz' },
          { id: 11, name: 'string_col', required: true, type: 'string' },
          { id: 12, name: 'uuid_col', required: true, type: 'uuid' },
          { id: 13, name: 'fixed_col', required: true, type: 'fixed' },
          { id: 14, name: 'binary_col', required: true, type: 'binary' },
        ],
      };

      expect(schema['schema-id']).toBe(0);
      expect(schema.fields).toHaveLength(14);
      expect(schema.type).toBe('struct');
    });

    it('should create schemas with complex types', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'list_col',
            required: false,
            type: {
              type: 'list',
              'element-id': 2,
              element: 'string',
              'element-required': false,
            },
          },
          {
            id: 3,
            name: 'map_col',
            required: false,
            type: {
              type: 'map',
              'key-id': 4,
              'value-id': 5,
              key: 'string',
              value: 'long',
              'value-required': false,
            },
          },
          {
            id: 6,
            name: 'struct_col',
            required: false,
            type: {
              type: 'struct',
              fields: [
                { id: 7, name: 'nested_a', required: true, type: 'string' },
                { id: 8, name: 'nested_b', required: false, type: 'int' },
              ],
            },
          },
        ],
      };

      expect(schema.fields).toHaveLength(3);
      expect(schema.fields[0].type).toHaveProperty('type', 'list');
      expect(schema.fields[1].type).toHaveProperty('type', 'map');
      expect(schema.fields[2].type).toHaveProperty('type', 'struct');
    });
  });

  describe('TableMetadata type consistency', () => {
    it('should create valid TableMetadata with all required fields', () => {
      const schema = createUsersSchema();
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/users',
        schema,
      });

      const metadata: TableMetadata = builder.build();

      // Verify all required fields exist and have correct types
      expect(metadata['format-version']).toBe(FORMAT_VERSION);
      expect(typeof metadata['table-uuid']).toBe('string');
      expect(metadata['table-uuid'].length).toBe(36); // UUID format
      expect(metadata.location).toBe('s3://bucket/warehouse/db/users');
      expect(metadata['last-sequence-number']).toBe(0);
      expect(typeof metadata['last-updated-ms']).toBe('number');
      expect(metadata['last-column-id']).toBeGreaterThan(0);
      expect(metadata['current-schema-id']).toBe(0);
      expect(metadata.schemas).toHaveLength(1);
      expect(metadata['default-spec-id']).toBe(0);
      expect(metadata['partition-specs']).toHaveLength(1);
      expect(metadata['last-partition-id']).toBeGreaterThanOrEqual(0);
      expect(metadata['default-sort-order-id']).toBe(0);
      expect(metadata['sort-orders']).toHaveLength(1);
      expect(metadata['current-snapshot-id']).toBeNull();
      expect(metadata.snapshots).toHaveLength(0);
      expect(metadata['snapshot-log']).toHaveLength(0);
      expect(metadata['metadata-log']).toHaveLength(0);
      expect(metadata.refs).toEqual({});
    });

    it('should maintain type consistency across builder operations', () => {
      const schema = createUsersSchema();
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/users',
        schema,
        partitionSpec: createIdentityPartitionSpec(1, 'id'),
        sortOrder: createSortOrder(4, 'desc', 'nulls-last'),
        properties: { 'write.format.default': 'parquet' },
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        manifestListPath: 's3://bucket/metadata/snap-1-manifest-list.avro',
        operation: 'append',
      }).build();

      builder.addSnapshot(snapshot);

      const metadata = builder.build();

      // Verify snapshot was added correctly
      expect(metadata.snapshots).toHaveLength(1);
      expect(metadata['current-snapshot-id']).toBe(snapshot['snapshot-id']);
      expect(metadata['last-sequence-number']).toBe(1);
      expect(metadata.refs['main']).toBeDefined();
      expect(metadata.refs['main']['snapshot-id']).toBe(snapshot['snapshot-id']);
    });
  });

  describe('PartitionSpec type consistency', () => {
    it('should create valid unpartitioned spec', () => {
      const spec: PartitionSpec = createUnpartitionedSpec();

      expect(spec['spec-id']).toBe(0);
      expect(spec.fields).toHaveLength(0);
    });

    it('should create valid identity partition spec', () => {
      const spec = createIdentityPartitionSpec(1, 'id', 1);

      expect(spec['spec-id']).toBe(1);
      expect(spec.fields).toHaveLength(1);
      expect(spec.fields[0]['source-id']).toBe(1);
      expect(spec.fields[0].transform).toBe('identity');
    });

    it('should create valid bucket partition spec', () => {
      const spec = createBucketPartitionSpec(1, 'id_bucket', 16, 2);

      expect(spec['spec-id']).toBe(2);
      expect(spec.fields).toHaveLength(1);
      expect(spec.fields[0].transform).toBe('bucket[16]');
    });

    it('should create valid time partition specs', () => {
      const yearSpec = createTimePartitionSpec(4, 'year', 'year');
      const monthSpec = createTimePartitionSpec(4, 'month', 'month');
      const daySpec = createTimePartitionSpec(4, 'day', 'day');
      const hourSpec = createTimePartitionSpec(4, 'hour', 'hour');

      expect(yearSpec.fields[0].transform).toBe('year');
      expect(monthSpec.fields[0].transform).toBe('month');
      expect(daySpec.fields[0].transform).toBe('day');
      expect(hourSpec.fields[0].transform).toBe('hour');
    });
  });
});

// ============================================================================
// 2. Storage Backend Abstraction Tests
// ============================================================================

describe('Storage Backend Abstraction', () => {
  let storage: ReturnType<typeof createMemoryStorage>;
  let catalog: FileSystemCatalog;

  beforeEach(() => {
    storage = createMemoryStorage();
    catalog = new FileSystemCatalog({
      name: 'test-catalog',
      warehouse: 's3://bucket/warehouse',
      storage,
    });
  });

  describe('MemoryStorage implements StorageBackend correctly', () => {
    it('should store and retrieve data', async () => {
      const key = 'test/key';
      const data = new TextEncoder().encode('test data');

      await storage.put(key, data);
      const retrieved = await storage.get(key);

      expect(retrieved).not.toBeNull();
      expect(new TextDecoder().decode(retrieved!)).toBe('test data');
    });

    it('should return null for non-existent keys', async () => {
      const result = await storage.get('non-existent');
      expect(result).toBeNull();
    });

    it('should delete data', async () => {
      const key = 'test/key';
      await storage.put(key, new TextEncoder().encode('data'));

      await storage.delete(key);

      expect(await storage.exists(key)).toBe(false);
    });

    it('should list keys by prefix', async () => {
      await storage.put('prefix/a', new TextEncoder().encode('a'));
      await storage.put('prefix/b', new TextEncoder().encode('b'));
      await storage.put('other/c', new TextEncoder().encode('c'));

      const results = await storage.list('prefix/');

      expect(results).toHaveLength(2);
      expect(results).toContain('prefix/a');
      expect(results).toContain('prefix/b');
    });

    it('should check existence correctly', async () => {
      await storage.put('exists', new TextEncoder().encode('data'));

      expect(await storage.exists('exists')).toBe(true);
      expect(await storage.exists('not-exists')).toBe(false);
    });
  });

  describe('FileSystemCatalog uses StorageBackend for operations', () => {
    it('should create namespaces using storage backend', async () => {
      await catalog.createNamespace(['db1']);

      // Verify namespace properties file was created in storage
      const propsPath = 's3://bucket/warehouse/db1/.namespace-properties.json';
      expect(await storage.exists(propsPath)).toBe(true);
    });

    it('should create tables using storage backend', async () => {
      await catalog.createNamespace(['db1']);
      await catalog.createTable(['db1'], {
        name: 'users',
        schema: createUsersSchema(),
      });

      // Verify metadata file was created in storage
      const metadataPath = 's3://bucket/warehouse/db1/users/metadata/v1.metadata.json';
      expect(await storage.exists(metadataPath)).toBe(true);

      // Verify version hint was created
      const versionHintPath = 's3://bucket/warehouse/db1/users/metadata/version-hint.text';
      expect(await storage.exists(versionHintPath)).toBe(true);
    });

    it('should load tables from storage backend', async () => {
      await catalog.createNamespace(['db1']);
      await catalog.createTable(['db1'], {
        name: 'users',
        schema: createUsersSchema(),
      });

      const metadata = await catalog.loadTable({ namespace: ['db1'], name: 'users' });

      expect(metadata['format-version']).toBe(FORMAT_VERSION);
      expect(metadata.schemas[0].fields).toHaveLength(4);
    });

    it('should commit table updates to storage backend', async () => {
      await catalog.createNamespace(['db1']);
      await catalog.createTable(['db1'], {
        name: 'users',
        schema: createUsersSchema(),
      });

      await catalog.commitTable({
        identifier: { namespace: ['db1'], name: 'users' },
        requirements: [],
        updates: [
          { action: 'set-properties', updates: { 'test.key': 'test.value' } },
        ],
      });

      // Verify v2 metadata file was created
      const v2Path = 's3://bucket/warehouse/db1/users/metadata/v2.metadata.json';
      expect(await storage.exists(v2Path)).toBe(true);

      // Verify version hint was updated
      const versionHintPath = 's3://bucket/warehouse/db1/users/metadata/version-hint.text';
      const versionHint = await storage.get(versionHintPath);
      expect(new TextDecoder().decode(versionHint!)).toBe('2');
    });
  });

  describe('MemoryCatalog provides fast in-memory operations', () => {
    let memoryCatalog: MemoryCatalog;

    beforeEach(() => {
      memoryCatalog = new MemoryCatalog({ name: 'memory-catalog' });
    });

    it('should create and load tables without I/O', async () => {
      await memoryCatalog.createNamespace(['db1']);
      await memoryCatalog.createTable(['db1'], {
        name: 'users',
        schema: createUsersSchema(),
      });

      const metadata = await memoryCatalog.loadTable({
        namespace: ['db1'],
        name: 'users',
      });

      expect(metadata['format-version']).toBe(FORMAT_VERSION);
    });

    it('should handle many tables quickly', async () => {
      await memoryCatalog.createNamespace(['db1']);

      const start = Date.now();
      for (let i = 0; i < 50; i++) {
        await memoryCatalog.createTable(['db1'], {
          name: `table${i}`,
          schema: createUsersSchema(),
        });
      }
      const elapsed = Date.now() - start;

      // Memory operations should be fast
      expect(elapsed).toBeLessThan(500);

      const tables = await memoryCatalog.listTables(['db1']);
      expect(tables).toHaveLength(50);
    });
  });
});

// ============================================================================
// 3. Round-Trip Serialization Tests
// ============================================================================

describe('Round-Trip Serialization', () => {
  describe('TableMetadata JSON serialization', () => {
    it('should serialize and parse simple table metadata', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
        schema: createUsersSchema(),
      });

      const original = builder.build();
      const json = JSON.stringify(original, null, 2);
      const parsed = parseTableMetadata(json);

      // Verify all fields match
      expect(parsed['format-version']).toBe(original['format-version']);
      expect(parsed['table-uuid']).toBe(original['table-uuid']);
      expect(parsed.location).toBe(original.location);
      expect(parsed['last-sequence-number']).toBe(original['last-sequence-number']);
      expect(parsed['current-schema-id']).toBe(original['current-schema-id']);
      expect(parsed.schemas).toEqual(original.schemas);
      expect(parsed['partition-specs']).toEqual(original['partition-specs']);
      expect(parsed['sort-orders']).toEqual(original['sort-orders']);
      expect(parsed.snapshots).toEqual(original.snapshots);
    });

    it('should serialize and parse metadata with snapshots', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
        schema: createUsersSchema(),
      });

      // Add multiple snapshots
      for (let i = 1; i <= 3; i++) {
        const snapshot = new SnapshotBuilder({
          sequenceNumber: i,
          snapshotId: Date.now() + i,
          parentSnapshotId: i > 1 ? Date.now() + i - 1 : undefined,
          manifestListPath: `s3://bucket/metadata/snap-${i}-manifest-list.avro`,
          operation: 'append',
        }).build();
        builder.addSnapshot(snapshot);
      }

      const original = builder.build();
      const json = JSON.stringify(original, null, 2);
      const parsed = parseTableMetadata(json);

      expect(parsed.snapshots).toHaveLength(3);
      expect(parsed['current-snapshot-id']).toBe(original['current-snapshot-id']);
      expect(parsed['snapshot-log']).toHaveLength(3);
      expect(parsed.refs['main']).toBeDefined();
    });

    it('should serialize and parse metadata with multiple schemas', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
        schema: createUsersSchema(),
      });

      // Add evolved schema
      const evolvedSchema: IcebergSchema = {
        'schema-id': 1,
        type: 'struct',
        fields: [
          ...createUsersSchema().fields,
          { id: 5, name: 'updated_at', required: false, type: 'timestamptz' },
        ],
      };
      builder.addSchema(evolvedSchema);
      builder.setCurrentSchema(1);

      const original = builder.build();
      const json = JSON.stringify(original, null, 2);
      const parsed = parseTableMetadata(json);

      expect(parsed.schemas).toHaveLength(2);
      expect(parsed['current-schema-id']).toBe(1);
      expect(parsed.schemas[1].fields).toHaveLength(5);
    });

    it('should serialize and parse metadata with partition specs', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
        schema: createEventsSchema(),
        partitionSpec: createTimePartitionSpec(4, 'event_day', 'day'),
      });

      const original = builder.build();
      const json = JSON.stringify(original, null, 2);
      const parsed = parseTableMetadata(json);

      expect(parsed['partition-specs']).toHaveLength(1);
      expect(parsed['partition-specs'][0].fields[0].transform).toBe('day');
    });

    it('should preserve all properties through serialization', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
        schema: createUsersSchema(),
        properties: {
          'write.format.default': 'parquet',
          'write.parquet.compression-codec': 'zstd',
          'write.metadata.compression-codec': 'gzip',
          'custom.property': 'custom.value',
        },
      });

      const original = builder.build();
      const json = JSON.stringify(original, null, 2);
      const parsed = parseTableMetadata(json);

      expect(parsed.properties).toEqual(original.properties);
      expect(parsed.properties['write.format.default']).toBe('parquet');
      expect(parsed.properties['custom.property']).toBe('custom.value');
    });
  });

  describe('Schema serialization with complex types', () => {
    it('should serialize and parse nested struct types', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          {
            id: 2,
            name: 'address',
            required: false,
            type: {
              type: 'struct',
              fields: [
                { id: 3, name: 'street', required: true, type: 'string' },
                { id: 4, name: 'city', required: true, type: 'string' },
                { id: 5, name: 'zip', required: false, type: 'string' },
              ],
            },
          },
        ],
      };

      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
        schema,
      });

      const original = builder.build();
      const json = JSON.stringify(original, null, 2);
      const parsed = parseTableMetadata(json);

      const parsedSchema = parsed.schemas[0];
      expect(parsedSchema.fields[1].type).toHaveProperty('type', 'struct');

      const nestedStruct = parsedSchema.fields[1].type as { type: 'struct'; fields: any[] };
      expect(nestedStruct.fields).toHaveLength(3);
    });

    it('should serialize and parse list and map types', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          {
            id: 2,
            name: 'tags',
            required: false,
            type: {
              type: 'list',
              'element-id': 3,
              element: 'string',
              'element-required': false,
            },
          },
          {
            id: 4,
            name: 'attributes',
            required: false,
            type: {
              type: 'map',
              'key-id': 5,
              'value-id': 6,
              key: 'string',
              value: 'string',
              'value-required': false,
            },
          },
        ],
      };

      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
        schema,
      });

      const original = builder.build();
      const json = JSON.stringify(original, null, 2);
      const parsed = parseTableMetadata(json);

      const parsedSchema = parsed.schemas[0];
      expect(parsedSchema.fields[1].type).toHaveProperty('type', 'list');
      expect(parsedSchema.fields[2].type).toHaveProperty('type', 'map');
    });
  });
});

// ============================================================================
// 4. Snapshot Workflow Tests
// ============================================================================

describe('Snapshot Workflow', () => {
  let builder: TableMetadataBuilder;
  let manager: SnapshotManager;

  beforeEach(() => {
    builder = new TableMetadataBuilder({
      location: 's3://bucket/warehouse/db/events',
      schema: createEventsSchema(),
      partitionSpec: createTimePartitionSpec(4, 'event_day', 'day'),
    });
  });

  describe('Creating snapshots', () => {
    it('should create initial snapshot with correct sequence number', () => {
      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        manifestListPath: 's3://bucket/metadata/snap-1-manifest-list.avro',
        operation: 'append',
      }).build();

      expect(snapshot['sequence-number']).toBe(1);
      expect(snapshot['manifest-list']).toBe('s3://bucket/metadata/snap-1-manifest-list.avro');
      expect(snapshot.summary.operation).toBe('append');
    });

    it('should create snapshot with summary statistics', () => {
      const snapshotBuilder = new SnapshotBuilder({
        sequenceNumber: 1,
        manifestListPath: 's3://bucket/metadata/snap-1-manifest-list.avro',
        operation: 'append',
      });

      snapshotBuilder.setSummary(
        10,    // added-data-files
        0,     // deleted-data-files
        10000, // added-records
        0,     // deleted-records
        1024 * 1024 * 100, // added-files-size (100MB)
        0,     // removed-files-size
        10000, // total-records
        1024 * 1024 * 100, // total-files-size
        10     // total-data-files
      );

      const snapshot = snapshotBuilder.build();

      expect(snapshot.summary['added-data-files']).toBe('10');
      expect(snapshot.summary['added-records']).toBe('10000');
      expect(snapshot.summary['total-records']).toBe('10000');
    });

    it('should create snapshot with parent reference', () => {
      const parentId = Date.now();
      const childId = Date.now() + 1;

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 2,
        snapshotId: childId,
        parentSnapshotId: parentId,
        manifestListPath: 's3://bucket/metadata/snap-2-manifest-list.avro',
        operation: 'append',
      }).build();

      expect(snapshot['parent-snapshot-id']).toBe(parentId);
    });
  });

  describe('Adding snapshots to table metadata', () => {
    it('should add snapshot and update current-snapshot-id', () => {
      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        manifestListPath: 's3://bucket/metadata/snap-1-manifest-list.avro',
        operation: 'append',
      }).build();

      builder.addSnapshot(snapshot);
      const metadata = builder.build();

      expect(metadata['current-snapshot-id']).toBe(snapshot['snapshot-id']);
      expect(metadata.snapshots).toHaveLength(1);
      expect(metadata['last-sequence-number']).toBe(1);
    });

    it('should update snapshot-log when adding snapshots', () => {
      const snapshot1 = new SnapshotBuilder({
        sequenceNumber: 1,
        manifestListPath: 's3://bucket/metadata/snap-1-manifest-list.avro',
        operation: 'append',
      }).build();

      const snapshot2 = new SnapshotBuilder({
        sequenceNumber: 2,
        parentSnapshotId: snapshot1['snapshot-id'],
        manifestListPath: 's3://bucket/metadata/snap-2-manifest-list.avro',
        operation: 'append',
      }).build();

      builder.addSnapshot(snapshot1);
      builder.addSnapshot(snapshot2);
      const metadata = builder.build();

      expect(metadata['snapshot-log']).toHaveLength(2);
      expect(metadata['snapshot-log'][0]['snapshot-id']).toBe(snapshot1['snapshot-id']);
      expect(metadata['snapshot-log'][1]['snapshot-id']).toBe(snapshot2['snapshot-id']);
    });

    it('should update main branch ref when adding snapshots', () => {
      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        manifestListPath: 's3://bucket/metadata/snap-1-manifest-list.avro',
        operation: 'append',
      }).build();

      builder.addSnapshot(snapshot);
      const metadata = builder.build();

      expect(metadata.refs['main']).toBeDefined();
      expect(metadata.refs['main']['snapshot-id']).toBe(snapshot['snapshot-id']);
      expect(metadata.refs['main'].type).toBe('branch');
    });
  });

  describe('Time travel with snapshots', () => {
    let metadata: TableMetadata;
    let snapshot1: Snapshot;
    let snapshot2: Snapshot;
    let snapshot3: Snapshot;

    beforeEach(() => {
      const baseTime = 1700000000000;

      snapshot1 = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 1001,
        timestampMs: baseTime,
        manifestListPath: 's3://bucket/metadata/snap-1-manifest-list.avro',
        operation: 'append',
      }).build();

      snapshot2 = new SnapshotBuilder({
        sequenceNumber: 2,
        snapshotId: 1002,
        parentSnapshotId: 1001,
        timestampMs: baseTime + 3600000, // 1 hour later
        manifestListPath: 's3://bucket/metadata/snap-2-manifest-list.avro',
        operation: 'append',
      }).build();

      snapshot3 = new SnapshotBuilder({
        sequenceNumber: 3,
        snapshotId: 1003,
        parentSnapshotId: 1002,
        timestampMs: baseTime + 7200000, // 2 hours later
        manifestListPath: 's3://bucket/metadata/snap-3-manifest-list.avro',
        operation: 'append',
      }).build();

      builder.addSnapshot(snapshot1);
      builder.addSnapshot(snapshot2);
      builder.addSnapshot(snapshot3);
      metadata = builder.build();
    });

    it('should get current snapshot', () => {
      const current = getCurrentSnapshot(metadata);

      expect(current).toBeDefined();
      expect(current!['snapshot-id']).toBe(1003);
    });

    it('should get snapshot by ID', () => {
      const snapshot = getSnapshotById(metadata, 1002);

      expect(snapshot).toBeDefined();
      expect(snapshot!['sequence-number']).toBe(2);
    });

    it('should get snapshot at specific timestamp', () => {
      const baseTime = 1700000000000;

      // Get snapshot at time between snapshot1 and snapshot2
      const snapshotAtTime = getSnapshotAtTimestamp(metadata, baseTime + 1800000);

      expect(snapshotAtTime).toBeDefined();
      expect(snapshotAtTime!['snapshot-id']).toBe(1001); // Should return snapshot1
    });

    it('should get snapshot by reference name', () => {
      const snapshot = getSnapshotByRef(metadata, 'main');

      expect(snapshot).toBeDefined();
      expect(snapshot!['snapshot-id']).toBe(1003); // Current snapshot
    });

    it('should return undefined for non-existent snapshot ID', () => {
      const snapshot = getSnapshotById(metadata, 9999);
      expect(snapshot).toBeUndefined();
    });

    it('should return undefined for non-existent reference', () => {
      const snapshot = getSnapshotByRef(metadata, 'nonexistent');
      expect(snapshot).toBeUndefined();
    });
  });

  describe('SnapshotManager operations', () => {
    let metadata: TableMetadata;

    beforeEach(() => {
      const baseTime = 1700000000000;

      for (let i = 1; i <= 5; i++) {
        const snapshot = new SnapshotBuilder({
          sequenceNumber: i,
          snapshotId: 1000 + i,
          parentSnapshotId: i > 1 ? 1000 + i - 1 : undefined,
          timestampMs: baseTime + i * 3600000,
          manifestListPath: `s3://bucket/metadata/snap-${i}-manifest-list.avro`,
          operation: 'append',
        }).build();
        builder.addSnapshot(snapshot);
      }

      metadata = builder.build();
      manager = SnapshotManager.fromMetadata(metadata);
    });

    it('should get all snapshots in chronological order', () => {
      const snapshots = manager.getSnapshots();

      expect(snapshots).toHaveLength(5);
      expect(snapshots[0]['snapshot-id']).toBe(1001);
      expect(snapshots[4]['snapshot-id']).toBe(1005);
    });

    it('should get snapshot history', () => {
      const history = manager.getSnapshotHistory();

      expect(history).toHaveLength(5);
      expect(history[0]['snapshot-id']).toBe(1001);
    });

    it('should get ancestor chain for a snapshot', () => {
      const chain = manager.getAncestorChain(1005);

      expect(chain).toHaveLength(5);
      expect(chain[0]['snapshot-id']).toBe(1005);
      expect(chain[4]['snapshot-id']).toBe(1001);
    });

    it('should create new snapshot through manager', () => {
      const newSnapshot = manager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/metadata/snap-6-manifest-list.avro',
      });

      expect(newSnapshot['sequence-number']).toBe(6);
      expect(newSnapshot['parent-snapshot-id']).toBe(1005);
    });

    it('should add snapshot and return updated metadata', () => {
      const newSnapshot = manager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/metadata/snap-6-manifest-list.avro',
      });

      const updatedMetadata = manager.addSnapshot(newSnapshot);

      expect(updatedMetadata['current-snapshot-id']).toBe(newSnapshot['snapshot-id']);
      expect(updatedMetadata.snapshots).toHaveLength(6);
    });

    it('should set and use snapshot references (branches and tags)', () => {
      manager.setRef('release-v1', 1003, 'tag');
      manager.setRef('feature-branch', 1004, 'branch');

      const tagSnapshot = manager.getSnapshotByRef('release-v1');
      const branchSnapshot = manager.getSnapshotByRef('feature-branch');

      expect(tagSnapshot!['snapshot-id']).toBe(1003);
      expect(branchSnapshot!['snapshot-id']).toBe(1004);
    });

    it('should get statistics about snapshot collection', () => {
      manager.setRef('tag-v1', 1002, 'tag');

      const stats = manager.getStats();

      expect(stats.totalSnapshots).toBe(5);
      expect(stats.currentSnapshotId).toBe(1005);
      expect(stats.branchCount).toBe(1); // main branch
      expect(stats.tagCount).toBe(1); // tag-v1
    });
  });
});

// ============================================================================
// 5. Schema Evolution Workflow Tests
// ============================================================================

describe('Schema Evolution Workflow', () => {
  let initialSchema: IcebergSchema;
  let builder: TableMetadataBuilder;

  beforeEach(() => {
    initialSchema = createUsersSchema();
    builder = new TableMetadataBuilder({
      location: 's3://bucket/warehouse/db/users',
      schema: initialSchema,
    });
  });

  describe('Adding columns', () => {
    it('should add a new optional column', () => {
      const metadata = builder.build();
      const evolutionBuilder = evolveSchema(metadata);

      evolutionBuilder.addColumn('phone', 'string', { required: false });

      const result = evolutionBuilder.buildWithMetadata();

      expect(result.schema.fields).toHaveLength(5);
      expect(result.schema.fields[4].name).toBe('phone');
      expect(result.schema.fields[4].required).toBe(false);
      expect(result.schema['schema-id']).toBe(1);
    });

    it('should add column at specific position', () => {
      const metadata = builder.build();
      const evolutionBuilder = evolveSchema(metadata);

      evolutionBuilder.addColumn('middle_name', 'string', {
        required: false,
        position: { type: 'after', column: 'name' },
      });

      const result = evolutionBuilder.buildWithMetadata();
      const names = result.schema.fields.map((f) => f.name);

      expect(names.indexOf('middle_name')).toBe(names.indexOf('name') + 1);
    });

    it('should add column with documentation', () => {
      const metadata = builder.build();
      const evolutionBuilder = evolveSchema(metadata);

      evolutionBuilder.addColumn('status', 'string', {
        required: false,
        doc: 'User account status',
      });

      const result = evolutionBuilder.buildWithMetadata();
      const statusField = result.schema.fields.find((f) => f.name === 'status');

      expect(statusField!.doc).toBe('User account status');
    });

    it('should assign monotonically increasing field IDs', () => {
      const metadata = builder.build();
      const evolutionBuilder = evolveSchema(metadata);

      evolutionBuilder
        .addColumn('field_a', 'string')
        .addColumn('field_b', 'string')
        .addColumn('field_c', 'string');

      const result = evolutionBuilder.buildWithMetadata();

      const fieldA = result.schema.fields.find((f) => f.name === 'field_a')!;
      const fieldB = result.schema.fields.find((f) => f.name === 'field_b')!;
      const fieldC = result.schema.fields.find((f) => f.name === 'field_c')!;

      expect(fieldA.id).toBeLessThan(fieldB.id);
      expect(fieldB.id).toBeLessThan(fieldC.id);
    });
  });

  describe('Dropping columns', () => {
    it('should drop an optional column', () => {
      const metadata = builder.build();
      const evolutionBuilder = evolveSchema(metadata);

      evolutionBuilder.dropColumn('email');

      const result = evolutionBuilder.buildWithMetadata();

      expect(result.schema.fields).toHaveLength(3);
      expect(result.schema.fields.map((f) => f.name)).not.toContain('email');
    });

    it('should throw when dropping non-existent column', () => {
      const metadata = builder.build();
      const evolutionBuilder = evolveSchema(metadata);

      evolutionBuilder.dropColumn('nonexistent');

      const validation = evolutionBuilder.validate();

      expect(validation.valid).toBe(false);
      expect(validation.errors[0]).toContain('not found');
    });
  });

  describe('Renaming columns', () => {
    it('should rename a column', () => {
      const metadata = builder.build();
      const evolutionBuilder = evolveSchema(metadata);

      evolutionBuilder.renameColumn('name', 'full_name');

      const result = evolutionBuilder.buildWithMetadata();
      const renamedField = result.schema.fields.find((f) => f.name === 'full_name');

      expect(renamedField).toBeDefined();
      expect(result.schema.fields.map((f) => f.name)).not.toContain('name');
    });

    it('should preserve field ID when renaming', () => {
      const metadata = builder.build();
      const originalField = metadata.schemas[0].fields.find((f) => f.name === 'name')!;
      const evolutionBuilder = evolveSchema(metadata);

      evolutionBuilder.renameColumn('name', 'full_name');

      const result = evolutionBuilder.buildWithMetadata();
      const renamedField = result.schema.fields.find((f) => f.name === 'full_name')!;

      expect(renamedField.id).toBe(originalField.id);
    });
  });

  describe('Type widening', () => {
    it('should widen int to long', () => {
      // Create schema with int field
      const schemaWithInt: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'count', required: true, type: 'int' },
        ],
      };

      const metadata = new TableMetadataBuilder({
        location: 's3://bucket/table',
        schema: schemaWithInt,
      }).build();

      const evolutionBuilder = evolveSchema(metadata);
      evolutionBuilder.updateColumnType('count', 'long');

      const result = evolutionBuilder.buildWithMetadata();
      const countField = result.schema.fields.find((f) => f.name === 'count')!;

      expect(countField.type).toBe('long');
    });

    it('should widen float to double', () => {
      const schemaWithFloat: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'value', required: true, type: 'float' },
        ],
      };

      const metadata = new TableMetadataBuilder({
        location: 's3://bucket/table',
        schema: schemaWithFloat,
      }).build();

      const evolutionBuilder = evolveSchema(metadata);
      evolutionBuilder.updateColumnType('value', 'double');

      const result = evolutionBuilder.buildWithMetadata();
      const valueField = result.schema.fields.find((f) => f.name === 'value')!;

      expect(valueField.type).toBe('double');
    });

    it('should reject incompatible type changes', () => {
      const metadata = builder.build();
      const evolutionBuilder = evolveSchema(metadata);

      evolutionBuilder.updateColumnType('name', 'long'); // string -> long is not allowed

      const validation = evolutionBuilder.validate();

      expect(validation.valid).toBe(false);
      expect(validation.errors[0]).toContain('Cannot');
    });
  });

  describe('Making columns optional/required', () => {
    it('should make required column optional', () => {
      const metadata = builder.build();
      const evolutionBuilder = evolveSchema(metadata);

      evolutionBuilder.makeColumnOptional('name');

      const result = evolutionBuilder.buildWithMetadata();
      const nameField = result.schema.fields.find((f) => f.name === 'name')!;

      expect(nameField.required).toBe(false);
    });

    it('should make optional column required', () => {
      const metadata = builder.build();
      const evolutionBuilder = evolveSchema(metadata);

      evolutionBuilder.makeColumnRequired('email');

      const result = evolutionBuilder.buildWithMetadata();
      const emailField = result.schema.fields.find((f) => f.name === 'email')!;

      expect(emailField.required).toBe(true);
    });
  });

  describe('Schema comparison and compatibility', () => {
    it('should detect added columns', () => {
      const metadata = builder.build();
      const evolutionBuilder = evolveSchema(metadata);

      evolutionBuilder.addColumn('phone', 'string');

      const newSchema = evolutionBuilder.build();
      const changes = compareSchemas(initialSchema, newSchema);

      const addedChanges = changes.filter((c) => c.type === 'added');
      expect(addedChanges).toHaveLength(1);
      expect(addedChanges[0].fieldName).toBe('phone');
    });

    it('should detect removed columns', () => {
      const metadata = builder.build();
      const evolutionBuilder = evolveSchema(metadata);

      evolutionBuilder.dropColumn('email');

      const newSchema = evolutionBuilder.build();
      const changes = compareSchemas(initialSchema, newSchema);

      const removedChanges = changes.filter((c) => c.type === 'removed');
      expect(removedChanges).toHaveLength(1);
      expect(removedChanges[0].fieldName).toBe('email');
    });

    it('should detect renamed columns', () => {
      const metadata = builder.build();
      const evolutionBuilder = evolveSchema(metadata);

      evolutionBuilder.renameColumn('name', 'full_name');

      const newSchema = evolutionBuilder.build();
      const changes = compareSchemas(initialSchema, newSchema);

      const renamedChanges = changes.filter((c) => c.type === 'renamed');
      expect(renamedChanges).toHaveLength(1);
      expect(renamedChanges[0].oldValue).toBe('name');
      expect(renamedChanges[0].newValue).toBe('full_name');
    });

    it('should determine backward compatibility', () => {
      const metadata = builder.build();
      const evolutionBuilder = evolveSchema(metadata);

      // Adding optional columns is backward compatible
      evolutionBuilder.addColumn('phone', 'string', { required: false });

      const newSchema = evolutionBuilder.build();
      const changes = compareSchemas(initialSchema, newSchema);
      const compatibility = isBackwardCompatible(changes);

      expect(compatibility.compatible).toBe(true);
    });

    it('should determine forward compatibility', () => {
      const metadata = builder.build();
      const evolutionBuilder = evolveSchema(metadata);

      // Dropping columns may break forward compatibility
      evolutionBuilder.dropColumn('email');

      const newSchema = evolutionBuilder.build();
      const changes = compareSchemas(initialSchema, newSchema);
      const compatibility = isForwardCompatible(changes);

      // Removing optional fields is generally forward compatible
      expect(compatibility.compatible).toBe(true);
    });
  });

  describe('Schema history tracking', () => {
    it('should maintain schema history across evolutions', () => {
      // Start with initial schema
      let metadata = builder.build();

      // First evolution: add phone
      const evolution1 = evolveSchema(metadata);
      evolution1.addColumn('phone', 'string', { required: false });
      const result1 = evolution1.buildWithMetadata();
      metadata = result1.metadata;

      // Second evolution: add address
      const evolution2 = evolveSchema(metadata);
      evolution2.addColumn('address', 'string', { required: false });
      const result2 = evolution2.buildWithMetadata();
      metadata = result2.metadata;

      const history = getSchemaHistory(metadata);

      expect(history).toHaveLength(3);
      expect(history[0].schemaId).toBe(0);
      expect(history[1].schemaId).toBe(1);
      expect(history[2].schemaId).toBe(2);
    });

    it('should track last-column-id correctly', () => {
      let metadata = builder.build();
      const initialLastColumnId = metadata['last-column-id'];

      // Add multiple columns
      const evolution = evolveSchema(metadata);
      evolution
        .addColumn('field1', 'string')
        .addColumn('field2', 'string')
        .addColumn('field3', 'string');

      const result = evolution.buildWithMetadata();

      expect(result.metadata['last-column-id']).toBe(initialLastColumnId + 3);
    });
  });

  describe('Old snapshots with evolved schema', () => {
    it('should get schema for specific snapshot', () => {
      let metadata = builder.build();

      // Create snapshot with initial schema
      const snapshot1 = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 1001,
        manifestListPath: 's3://bucket/metadata/snap-1-manifest-list.avro',
        operation: 'append',
        schemaId: 0,
      }).build();

      const metadataBuilder = TableMetadataBuilder.fromMetadata(metadata);
      metadataBuilder.addSnapshot(snapshot1);
      metadata = metadataBuilder.build();

      // Evolve schema
      const evolution = evolveSchema(metadata);
      evolution.addColumn('phone', 'string');
      const result = evolution.buildWithMetadata();
      metadata = result.metadata;

      // Create snapshot with new schema
      const snapshot2 = new SnapshotBuilder({
        sequenceNumber: 2,
        snapshotId: 1002,
        parentSnapshotId: 1001,
        manifestListPath: 's3://bucket/metadata/snap-2-manifest-list.avro',
        operation: 'append',
        schemaId: 1,
      }).build();

      const metadataBuilder2 = TableMetadataBuilder.fromMetadata(metadata);
      metadataBuilder2.addSnapshot(snapshot2);
      metadata = metadataBuilder2.build();

      // Get schema for each snapshot
      const schema1 = getSchemaForSnapshot(metadata, 1001);
      const schema2 = getSchemaForSnapshot(metadata, 1002);

      expect(schema1!.fields).toHaveLength(4); // Original schema
      expect(schema2!.fields).toHaveLength(5); // Evolved schema with phone
    });

    it('should read old snapshots with new schema (backward compatibility)', () => {
      let metadata = builder.build();

      // Add initial snapshot
      const snapshot1 = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 1001,
        manifestListPath: 's3://bucket/metadata/snap-1-manifest-list.avro',
        operation: 'append',
        schemaId: 0,
      }).build();

      let metadataBuilder = TableMetadataBuilder.fromMetadata(metadata);
      metadataBuilder.addSnapshot(snapshot1);
      metadata = metadataBuilder.build();

      // Evolve schema (backward compatible changes)
      const evolution = evolveSchema(metadata);
      evolution
        .addColumn('phone', 'string', { required: false }) // Optional field
        .makeColumnOptional('name'); // required -> optional is backward compatible

      const result = evolution.buildWithMetadata();
      metadata = result.metadata;

      // Verify old snapshot is still accessible and uses correct schema
      const oldSnapshot = getSnapshotById(metadata, 1001);
      const oldSchemaId = oldSnapshot!['schema-id'];
      const oldSchema = getSchemaForSnapshot(metadata, 1001);

      expect(oldSchemaId).toBe(0);
      expect(oldSchema).not.toBeNull();
      expect(oldSchema!.fields).toHaveLength(4);

      // Verify schema compatibility
      const currentSchema = metadata.schemas.find(
        (s) => s['schema-id'] === metadata['current-schema-id']
      )!;
      const changes = compareSchemas(oldSchema!, currentSchema);
      const compatibility = isBackwardCompatible(changes);

      expect(compatibility.compatible).toBe(true);
    });
  });

  describe('Complex schema evolution scenarios', () => {
    it('should handle multiple operations in single evolution', () => {
      const metadata = builder.build();
      const evolution = evolveSchema(metadata);

      evolution
        .addColumn('phone', 'string', { required: false })
        .renameColumn('name', 'full_name')
        .makeColumnOptional('created_at')
        .addColumn('updated_at', 'timestamptz', { required: false });

      const validation = evolution.validate();
      expect(validation.valid).toBe(true);

      const result = evolution.buildWithMetadata();

      expect(result.schema.fields).toHaveLength(6); // 4 original + 2 new
      expect(result.schema.fields.map((f) => f.name)).toContain('phone');
      expect(result.schema.fields.map((f) => f.name)).toContain('full_name');
      expect(result.schema.fields.map((f) => f.name)).not.toContain('name');
    });

    it('should validate operations before applying', () => {
      const metadata = builder.build();
      const evolution = evolveSchema(metadata);

      // Add invalid operations
      evolution
        .dropColumn('nonexistent')
        .renameColumn('also_nonexistent', 'new_name')
        .addColumn('email', 'string'); // email already exists

      const validation = evolution.validate();

      expect(validation.valid).toBe(false);
      expect(validation.errors.length).toBeGreaterThan(0);
    });

    it('should track schema evolution across table commits', async () => {
      const storage = createMemoryStorage();
      const catalog = new FileSystemCatalog({
        name: 'test-catalog',
        warehouse: 's3://bucket/warehouse',
        storage,
      });

      await catalog.createNamespace(['db1']);
      await catalog.createTable(['db1'], {
        name: 'users',
        schema: initialSchema,
      });

      // First commit: add phone column
      const table1 = await catalog.loadTable({ namespace: ['db1'], name: 'users' });
      const evolution1 = evolveSchema(table1);
      evolution1.addColumn('phone', 'string', { required: false });
      const result1 = evolution1.buildWithMetadata();

      await catalog.commitTable({
        identifier: { namespace: ['db1'], name: 'users' },
        requirements: [],
        updates: [
          { action: 'add-schema', schema: result1.schema, 'last-column-id': result1.nextColumnId },
          { action: 'set-current-schema', 'schema-id': result1.schema['schema-id'] },
        ],
      });

      // Second commit: add address column
      const table2 = await catalog.loadTable({ namespace: ['db1'], name: 'users' });
      const evolution2 = evolveSchema(table2);
      evolution2.addColumn('address', 'string', { required: false });
      const result2 = evolution2.buildWithMetadata();

      await catalog.commitTable({
        identifier: { namespace: ['db1'], name: 'users' },
        requirements: [],
        updates: [
          { action: 'add-schema', schema: result2.schema, 'last-column-id': result2.nextColumnId },
          { action: 'set-current-schema', 'schema-id': result2.schema['schema-id'] },
        ],
      });

      // Load final table and verify schema history
      const finalTable = await catalog.loadTable({ namespace: ['db1'], name: 'users' });

      expect(finalTable.schemas).toHaveLength(3);
      expect(finalTable['current-schema-id']).toBe(2);
      expect(finalTable.schemas[2].fields).toHaveLength(6);
    });
  });
});

// ============================================================================
// End-to-End Workflow Test
// ============================================================================

describe('End-to-End Workflow', () => {
  it('should support complete table lifecycle with schema evolution and snapshots', async () => {
    // 1. Create storage and catalog
    const storage = createMemoryStorage();
    const catalog = new FileSystemCatalog({
      name: 'e2e-catalog',
      warehouse: 's3://bucket/warehouse',
      storage,
      defaultProperties: {
        'write.format.default': 'parquet',
      },
    });

    // 2. Create namespace and table
    await catalog.createNamespace(['analytics']);
    const initialMetadata = await catalog.createTable(['analytics'], {
      name: 'events',
      schema: createEventsSchema(),
      partitionSpec: createTimePartitionSpec(4, 'event_day', 'day'),
      properties: {
        'write.target-file-size-bytes': '134217728',
      },
    });

    expect(initialMetadata['format-version']).toBe(FORMAT_VERSION);
    expect(initialMetadata.schemas).toHaveLength(1);

    // 3. Add first snapshot
    const identifier: TableIdentifier = { namespace: ['analytics'], name: 'events' };
    const snapshot1 = new SnapshotBuilder({
      sequenceNumber: 1,
      snapshotId: 2001,
      timestampMs: Date.now(),
      manifestListPath: 's3://bucket/warehouse/analytics/events/metadata/snap-1.avro',
      operation: 'append',
      schemaId: 0,
    }).build();

    await catalog.commitTable({
      identifier,
      requirements: [],
      updates: [
        { action: 'add-snapshot', snapshot: snapshot1 },
        { action: 'set-snapshot-ref', 'ref-name': 'main', type: 'branch', 'snapshot-id': 2001 },
      ],
    });

    let metadata = await catalog.loadTable(identifier);
    expect(metadata.snapshots).toHaveLength(1);
    expect(metadata['current-snapshot-id']).toBe(2001);

    // 4. Evolve schema (add new columns)
    const evolution = evolveSchema(metadata);
    evolution
      .addColumn('device_type', 'string', { required: false, doc: 'Device type identifier' })
      .addColumn('session_id', 'uuid', { required: false });

    const evolutionResult = evolution.buildWithMetadata();

    await catalog.commitTable({
      identifier,
      requirements: [],
      updates: [
        {
          action: 'add-schema',
          schema: evolutionResult.schema,
          'last-column-id': evolutionResult.nextColumnId,
        },
        { action: 'set-current-schema', 'schema-id': evolutionResult.schema['schema-id'] },
      ],
    });

    metadata = await catalog.loadTable(identifier);
    expect(metadata.schemas).toHaveLength(2);
    expect(metadata['current-schema-id']).toBe(1);

    // 5. Add second snapshot with new schema
    const snapshot2 = new SnapshotBuilder({
      sequenceNumber: 2,
      snapshotId: 2002,
      parentSnapshotId: 2001,
      timestampMs: Date.now(),
      manifestListPath: 's3://bucket/warehouse/analytics/events/metadata/snap-2.avro',
      operation: 'append',
      schemaId: 1,
    }).build();

    await catalog.commitTable({
      identifier,
      requirements: [],
      updates: [
        { action: 'add-snapshot', snapshot: snapshot2 },
        { action: 'set-snapshot-ref', 'ref-name': 'main', type: 'branch', 'snapshot-id': 2002 },
      ],
    });

    // 6. Create a tag for the current version
    await catalog.commitTable({
      identifier,
      requirements: [],
      updates: [
        { action: 'set-snapshot-ref', 'ref-name': 'v1.0', type: 'tag', 'snapshot-id': 2002 },
      ],
    });

    metadata = await catalog.loadTable(identifier);
    expect(metadata.refs['v1.0']).toBeDefined();
    expect(metadata.refs['v1.0'].type).toBe('tag');

    // 7. Verify time travel works
    const manager = SnapshotManager.fromMetadata(metadata);

    // Get schema for old snapshot
    const schema1 = getSchemaForSnapshot(metadata, 2001);
    expect(schema1!.fields).toHaveLength(7); // Original events schema has 7 top-level fields

    // Get schema for new snapshot
    const schema2 = getSchemaForSnapshot(metadata, 2002);
    expect(schema2!.fields).toHaveLength(9); // Evolved schema with 2 new fields

    // 8. Verify ancestor chain
    const ancestorChain = manager.getAncestorChain(2002);
    expect(ancestorChain).toHaveLength(2);
    expect(ancestorChain[0]['snapshot-id']).toBe(2002);
    expect(ancestorChain[1]['snapshot-id']).toBe(2001);

    // 9. Serialize and parse (round-trip)
    const jsonMetadata = JSON.stringify(metadata, null, 2);
    const parsedMetadata = parseTableMetadata(jsonMetadata);

    expect(parsedMetadata['table-uuid']).toBe(metadata['table-uuid']);
    expect(parsedMetadata.schemas).toHaveLength(2);
    expect(parsedMetadata.snapshots).toHaveLength(2);
    expect(parsedMetadata.refs).toHaveProperty('main');
    expect(parsedMetadata.refs).toHaveProperty('v1.0');

    // 10. Verify backward compatibility
    const changes = compareSchemas(schema1!, schema2!);
    const compatibility = isBackwardCompatible(changes);
    expect(compatibility.compatible).toBe(true);

    // 11. Clean up
    await catalog.dropTable(identifier, true);
    const exists = await catalog.tableExists(identifier);
    expect(exists).toBe(false);
  });
});
