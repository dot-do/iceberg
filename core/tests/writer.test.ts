import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  MetadataWriter,
  writeNewTableMetadata,
  writeMetadataIfMissing,
  createDefaultSchema,
  createUnpartitionedSpec,
  createIdentityPartitionSpec,
  createUnsortedOrder,
  createSortOrder,
  SnapshotBuilder,
  FORMAT_VERSION,
  METADATA_DIR,
  VERSION_HINT_FILENAME,
  INITIAL_PARTITION_ID,
  type StorageBackend,
  type TableMetadata,
  type MetadataWriterOptions,
  type IcebergSchema,
  type PartitionSpec,
  type SortOrder,
} from '../src/index.js';

// ============================================================================
// Mock Storage Backend
// ============================================================================

/**
 * Create an in-memory storage backend for testing.
 */
function createMockStorage(): StorageBackend & {
  data: Map<string, Uint8Array>;
  clear: () => void;
  getCallCount: Map<string, number>;
  failOnKey?: string;
  delayMs?: number;
} {
  const data = new Map<string, Uint8Array>();
  const getCallCount = new Map<string, number>();

  const storage: StorageBackend & {
    data: Map<string, Uint8Array>;
    clear: () => void;
    getCallCount: Map<string, number>;
    failOnKey?: string;
    delayMs?: number;
  } = {
    data,
    getCallCount,
    failOnKey: undefined,
    delayMs: undefined,

    async get(key: string): Promise<Uint8Array | null> {
      getCallCount.set('get', (getCallCount.get('get') || 0) + 1);
      if (storage.failOnKey === key) {
        throw new Error(`Simulated failure on key: ${key}`);
      }
      if (storage.delayMs) {
        await new Promise((resolve) => setTimeout(resolve, storage.delayMs));
      }
      return data.get(key) ?? null;
    },

    async put(key: string, value: Uint8Array): Promise<void> {
      getCallCount.set('put', (getCallCount.get('put') || 0) + 1);
      if (storage.failOnKey === key) {
        throw new Error(`Simulated failure on key: ${key}`);
      }
      if (storage.delayMs) {
        await new Promise((resolve) => setTimeout(resolve, storage.delayMs));
      }
      data.set(key, value);
    },

    async delete(key: string): Promise<void> {
      getCallCount.set('delete', (getCallCount.get('delete') || 0) + 1);
      if (storage.failOnKey === key) {
        throw new Error(`Simulated failure on key: ${key}`);
      }
      data.delete(key);
    },

    async list(prefix: string): Promise<string[]> {
      getCallCount.set('list', (getCallCount.get('list') || 0) + 1);
      const results: string[] = [];
      for (const key of data.keys()) {
        if (key.startsWith(prefix)) {
          results.push(key);
        }
      }
      return results.sort();
    },

    async exists(key: string): Promise<boolean> {
      getCallCount.set('exists', (getCallCount.get('exists') || 0) + 1);
      if (storage.failOnKey === key) {
        throw new Error(`Simulated failure on key: ${key}`);
      }
      return data.has(key);
    },

    clear(): void {
      data.clear();
      getCallCount.clear();
      storage.failOnKey = undefined;
      storage.delayMs = undefined;
    },
  };

  return storage;
}

/**
 * Create a test snapshot.
 */
function createTestSnapshot(
  sequenceNumber: number,
  parentSnapshotId?: number
): ReturnType<SnapshotBuilder['build']> {
  const builder = new SnapshotBuilder({
    sequenceNumber,
    parentSnapshotId,
    manifestListPath: `s3://bucket/metadata/snap-${Date.now()}.avro`,
    operation: 'append',
  });
  builder.setSummary(1, 0, 100, 0, 1024, 0, 100, 1024, 1);
  return builder.build();
}

// ============================================================================
// MetadataWriter Class Initialization Tests
// ============================================================================

describe('MetadataWriter', () => {
  describe('constructor', () => {
    it('should create a MetadataWriter instance with storage backend', () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);
      expect(writer).toBeInstanceOf(MetadataWriter);
    });

    it('should accept any StorageBackend implementation', () => {
      const customStorage: StorageBackend = {
        get: async () => null,
        put: async () => {},
        delete: async () => {},
        list: async () => [],
        exists: async () => false,
      };
      const writer = new MetadataWriter(customStorage);
      expect(writer).toBeInstanceOf(MetadataWriter);
    });
  });
});

// ============================================================================
// createTableMetadata Tests
// ============================================================================

describe('createTableMetadata', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let writer: MetadataWriter;

  beforeEach(() => {
    storage = createMockStorage();
    writer = new MetadataWriter(storage);
  });

  it('should create metadata with required fields', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/warehouse/db/table',
    });

    expect(metadata['format-version']).toBe(FORMAT_VERSION);
    expect(metadata.location).toBe('s3://bucket/warehouse/db/table');
    expect(metadata['table-uuid']).toBeTruthy();
    expect(typeof metadata['table-uuid']).toBe('string');
    expect(metadata['table-uuid']).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
    );
  });

  it('should use provided tableUuid', () => {
    const customUuid = '12345678-1234-4123-8123-123456789abc';
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
      tableUuid: customUuid,
    });

    expect(metadata['table-uuid']).toBe(customUuid);
  });

  it('should use default schema when not provided', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    expect(metadata.schemas).toHaveLength(1);
    expect(metadata.schemas[0]['schema-id']).toBe(0);
    expect(metadata.schemas[0].type).toBe('struct');
    expect(metadata['current-schema-id']).toBe(0);
  });

  it('should use provided schema', () => {
    const customSchema: IcebergSchema = {
      'schema-id': 5,
      type: 'struct',
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'name', required: false, type: 'string' },
      ],
    };

    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
      schema: customSchema,
    });

    expect(metadata.schemas).toHaveLength(1);
    expect(metadata.schemas[0]).toEqual(customSchema);
    expect(metadata['current-schema-id']).toBe(5);
    expect(metadata['last-column-id']).toBe(2);
  });

  it('should use unpartitioned spec when not provided', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    expect(metadata['partition-specs']).toHaveLength(1);
    expect(metadata['partition-specs'][0]['spec-id']).toBe(0);
    expect(metadata['partition-specs'][0].fields).toHaveLength(0);
    expect(metadata['default-spec-id']).toBe(0);
    expect(metadata['last-partition-id']).toBe(INITIAL_PARTITION_ID);
  });

  it('should use provided partition spec', () => {
    const customSpec = createIdentityPartitionSpec(1, 'date_col');

    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
      partitionSpec: customSpec,
    });

    expect(metadata['partition-specs']).toHaveLength(1);
    expect(metadata['partition-specs'][0]).toEqual(customSpec);
    expect(metadata['default-spec-id']).toBe(0);
    expect(metadata['last-partition-id']).toBe(customSpec.fields[0]['field-id']);
  });

  it('should use unsorted order when not provided', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    expect(metadata['sort-orders']).toHaveLength(1);
    expect(metadata['sort-orders'][0]['order-id']).toBe(0);
    expect(metadata['sort-orders'][0].fields).toHaveLength(0);
    expect(metadata['default-sort-order-id']).toBe(0);
  });

  it('should use provided sort order', () => {
    const customOrder = createSortOrder(1, 'asc', 'nulls-first', 1);

    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
      sortOrder: customOrder,
    });

    expect(metadata['sort-orders']).toHaveLength(1);
    expect(metadata['sort-orders'][0]).toEqual(customOrder);
    expect(metadata['default-sort-order-id']).toBe(1);
  });

  it('should use empty properties when not provided', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    expect(metadata.properties).toEqual({});
  });

  it('should use provided properties', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
      properties: {
        'app.name': 'test-app',
        'write.format.default': 'parquet',
      },
    });

    expect(metadata.properties['app.name']).toBe('test-app');
    expect(metadata.properties['write.format.default']).toBe('parquet');
  });

  it('should initialize with empty snapshot state', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    expect(metadata['current-snapshot-id']).toBeNull();
    expect(metadata.snapshots).toHaveLength(0);
    expect(metadata['snapshot-log']).toHaveLength(0);
    expect(metadata['metadata-log']).toHaveLength(0);
    expect(metadata.refs).toEqual({});
  });

  it('should set last-sequence-number to 0', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    expect(metadata['last-sequence-number']).toBe(0);
  });

  it('should set last-updated-ms to current time', () => {
    const before = Date.now();
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });
    const after = Date.now();

    expect(metadata['last-updated-ms']).toBeGreaterThanOrEqual(before);
    expect(metadata['last-updated-ms']).toBeLessThanOrEqual(after);
  });

  it('should calculate last-column-id from schema fields', () => {
    const schemaWithHighId: IcebergSchema = {
      'schema-id': 0,
      type: 'struct',
      fields: [
        { id: 10, name: 'a', required: true, type: 'int' },
        { id: 25, name: 'b', required: false, type: 'string' },
        { id: 5, name: 'c', required: true, type: 'long' },
      ],
    };

    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
      schema: schemaWithHighId,
    });

    expect(metadata['last-column-id']).toBe(25);
  });

  it('should handle nested struct types for last-column-id', () => {
    const nestedSchema: IcebergSchema = {
      'schema-id': 0,
      type: 'struct',
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        {
          id: 2,
          name: 'nested',
          required: false,
          type: {
            type: 'struct',
            fields: [
              { id: 3, name: 'inner_a', required: true, type: 'string' },
              { id: 100, name: 'inner_b', required: false, type: 'int' },
            ],
          },
        },
      ],
    };

    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
      schema: nestedSchema,
    });

    expect(metadata['last-column-id']).toBe(100);
  });
});

// ============================================================================
// writeNewTable Tests
// ============================================================================

describe('writeNewTable', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let writer: MetadataWriter;
  const tableLocation = 's3://bucket/warehouse/db/table';

  beforeEach(() => {
    storage = createMockStorage();
    writer = new MetadataWriter(storage);
  });

  it('should write metadata file and version hint', async () => {
    const result = await writer.writeNewTable({
      location: tableLocation,
    });

    expect(result.version).toBe(1);
    expect(result.metadataLocation).toBe(`${tableLocation}/${METADATA_DIR}/v1.metadata.json`);
    expect(result.metadata['format-version']).toBe(FORMAT_VERSION);

    // Verify metadata file was written
    const metadataExists = await storage.exists(result.metadataLocation);
    expect(metadataExists).toBe(true);

    // Verify version hint was written
    const versionHintPath = `${tableLocation}/${METADATA_DIR}/${VERSION_HINT_FILENAME}`;
    const versionHintExists = await storage.exists(versionHintPath);
    expect(versionHintExists).toBe(true);

    const versionHintData = await storage.get(versionHintPath);
    expect(new TextDecoder().decode(versionHintData!)).toBe('1');
  });

  it('should throw error if table already exists', async () => {
    // Create initial table
    await writer.writeNewTable({ location: tableLocation });

    // Attempt to create again
    await expect(writer.writeNewTable({ location: tableLocation })).rejects.toThrow(
      `Table already exists at ${tableLocation}`
    );
  });

  it('should write valid JSON metadata', async () => {
    const result = await writer.writeNewTable({
      location: tableLocation,
      properties: { 'app.collection': 'users' },
    });

    const metadataData = await storage.get(result.metadataLocation);
    const metadata = JSON.parse(new TextDecoder().decode(metadataData!)) as TableMetadata;

    expect(metadata['format-version']).toBe(FORMAT_VERSION);
    expect(metadata.location).toBe(tableLocation);
    expect(metadata.properties['app.collection']).toBe('users');
  });

  it('should use correct metadata path format', async () => {
    const result = await writer.writeNewTable({ location: tableLocation });

    expect(result.metadataLocation).toMatch(/\/metadata\/v1\.metadata\.json$/);
  });

  it('should write pretty-printed JSON', async () => {
    const result = await writer.writeNewTable({ location: tableLocation });

    const metadataData = await storage.get(result.metadataLocation);
    const jsonString = new TextDecoder().decode(metadataData!);

    // Pretty-printed JSON has newlines and indentation
    expect(jsonString).toContain('\n');
    expect(jsonString).toContain('  ');
  });

  it('should return complete metadata in result', async () => {
    const result = await writer.writeNewTable({
      location: tableLocation,
      properties: { key: 'value' },
    });

    expect(result.metadata['format-version']).toBe(FORMAT_VERSION);
    expect(result.metadata.location).toBe(tableLocation);
    expect(result.metadata.properties.key).toBe('value');
    expect(result.metadata.schemas).toHaveLength(1);
    expect(result.metadata['partition-specs']).toHaveLength(1);
    expect(result.metadata['sort-orders']).toHaveLength(1);
  });
});

// ============================================================================
// writeWithSnapshot Tests
// ============================================================================

describe('writeWithSnapshot', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let writer: MetadataWriter;
  const tableLocation = 's3://bucket/warehouse/db/table';
  let initialMetadata: TableMetadata;
  let initialMetadataPath: string;

  beforeEach(async () => {
    storage = createMockStorage();
    writer = new MetadataWriter(storage);

    // Create initial table
    const result = await writer.writeNewTable({ location: tableLocation });
    initialMetadata = result.metadata;
    initialMetadataPath = result.metadataLocation;
  });

  it('should write new metadata with snapshot', async () => {
    const snapshot = createTestSnapshot(1);

    const result = await writer.writeWithSnapshot(initialMetadata, snapshot, initialMetadataPath);

    expect(result.version).toBe(2);
    expect(result.metadataLocation).toBe(`${tableLocation}/${METADATA_DIR}/v2.metadata.json`);
    expect(result.metadata.snapshots).toHaveLength(1);
    expect(result.metadata['current-snapshot-id']).toBe(snapshot['snapshot-id']);
  });

  it('should update last-sequence-number', async () => {
    const snapshot = createTestSnapshot(5);

    const result = await writer.writeWithSnapshot(initialMetadata, snapshot);

    expect(result.metadata['last-sequence-number']).toBe(5);
  });

  it('should update last-updated-ms', async () => {
    const before = Date.now();
    const snapshot = createTestSnapshot(1);

    const result = await writer.writeWithSnapshot(initialMetadata, snapshot);

    expect(result.metadata['last-updated-ms']).toBeGreaterThanOrEqual(before);
  });

  it('should add snapshot to snapshots array', async () => {
    const snapshot = createTestSnapshot(1);

    const result = await writer.writeWithSnapshot(initialMetadata, snapshot);

    expect(result.metadata.snapshots).toHaveLength(1);
    expect(result.metadata.snapshots[0]['snapshot-id']).toBe(snapshot['snapshot-id']);
    expect(result.metadata.snapshots[0]['sequence-number']).toBe(1);
  });

  it('should update snapshot-log', async () => {
    const snapshot = createTestSnapshot(1);

    const result = await writer.writeWithSnapshot(initialMetadata, snapshot);

    expect(result.metadata['snapshot-log']).toHaveLength(1);
    expect(result.metadata['snapshot-log'][0]['snapshot-id']).toBe(snapshot['snapshot-id']);
    expect(result.metadata['snapshot-log'][0]['timestamp-ms']).toBe(snapshot['timestamp-ms']);
  });

  it('should update refs with main branch', async () => {
    const snapshot = createTestSnapshot(1);

    const result = await writer.writeWithSnapshot(initialMetadata, snapshot);

    expect(result.metadata.refs.main).toBeDefined();
    expect(result.metadata.refs.main['snapshot-id']).toBe(snapshot['snapshot-id']);
    expect(result.metadata.refs.main.type).toBe('branch');
  });

  it('should add to metadata-log when previousMetadataLocation provided', async () => {
    const snapshot = createTestSnapshot(1);

    const result = await writer.writeWithSnapshot(initialMetadata, snapshot, initialMetadataPath);

    expect(result.metadata['metadata-log']).toHaveLength(1);
    expect(result.metadata['metadata-log'][0]['metadata-file']).toBe(initialMetadataPath);
    expect(result.metadata['metadata-log'][0]['timestamp-ms']).toBe(
      initialMetadata['last-updated-ms']
    );
  });

  it('should not add to metadata-log when previousMetadataLocation not provided', async () => {
    const snapshot = createTestSnapshot(1);

    const result = await writer.writeWithSnapshot(initialMetadata, snapshot);

    expect(result.metadata['metadata-log']).toHaveLength(0);
  });

  it('should update version hint', async () => {
    const snapshot = createTestSnapshot(1);

    await writer.writeWithSnapshot(initialMetadata, snapshot);

    const versionHintPath = `${tableLocation}/${METADATA_DIR}/${VERSION_HINT_FILENAME}`;
    const versionHintData = await storage.get(versionHintPath);
    expect(new TextDecoder().decode(versionHintData!)).toBe('2');
  });

  it('should handle multiple sequential snapshots', async () => {
    const snapshot1 = createTestSnapshot(1);
    const result1 = await writer.writeWithSnapshot(
      initialMetadata,
      snapshot1,
      initialMetadataPath
    );

    const snapshot2 = createTestSnapshot(2, snapshot1['snapshot-id']);
    const result2 = await writer.writeWithSnapshot(
      result1.metadata,
      snapshot2,
      result1.metadataLocation
    );

    expect(result2.version).toBe(3);
    expect(result2.metadata.snapshots).toHaveLength(2);
    expect(result2.metadata['current-snapshot-id']).toBe(snapshot2['snapshot-id']);
    expect(result2.metadata['last-sequence-number']).toBe(2);
    expect(result2.metadata['snapshot-log']).toHaveLength(2);
    expect(result2.metadata['metadata-log']).toHaveLength(2);
  });
});

// ============================================================================
// validateMetadata Tests
// ============================================================================

describe('validateMetadata', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let writer: MetadataWriter;

  beforeEach(() => {
    storage = createMockStorage();
    writer = new MetadataWriter(storage);
  });

  it('should pass validation for valid metadata', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    expect(() => writer.validateMetadata(metadata)).not.toThrow();
  });

  it('should throw for invalid format-version', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    const invalidMetadata = {
      ...metadata,
      'format-version': 1 as const,
    } as unknown as TableMetadata;

    expect(() => writer.validateMetadata(invalidMetadata)).toThrow(
      'Invalid format-version: expected 2 or 3, got 1'
    );
  });

  it('should throw for missing table-uuid', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    const invalidMetadata = { ...metadata, 'table-uuid': '' } as TableMetadata;

    expect(() => writer.validateMetadata(invalidMetadata)).toThrow(
      'Missing required field: table-uuid'
    );
  });

  it('should throw for missing location', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    const invalidMetadata = { ...metadata, location: '' } as TableMetadata;

    expect(() => writer.validateMetadata(invalidMetadata)).toThrow(
      'Missing required field: location'
    );
  });

  it('should throw for missing last-sequence-number', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    const invalidMetadata = { ...metadata } as Record<string, unknown>;
    delete invalidMetadata['last-sequence-number'];

    expect(() => writer.validateMetadata(invalidMetadata as TableMetadata)).toThrow(
      'Missing or invalid field: last-sequence-number'
    );
  });

  it('should throw for missing last-updated-ms', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    const invalidMetadata = { ...metadata } as Record<string, unknown>;
    delete invalidMetadata['last-updated-ms'];

    expect(() => writer.validateMetadata(invalidMetadata as TableMetadata)).toThrow(
      'Missing or invalid field: last-updated-ms'
    );
  });

  it('should throw for missing last-column-id', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    const invalidMetadata = { ...metadata } as Record<string, unknown>;
    delete invalidMetadata['last-column-id'];

    expect(() => writer.validateMetadata(invalidMetadata as TableMetadata)).toThrow(
      'Missing or invalid field: last-column-id'
    );
  });

  it('should throw for missing current-schema-id', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    const invalidMetadata = { ...metadata } as Record<string, unknown>;
    delete invalidMetadata['current-schema-id'];

    expect(() => writer.validateMetadata(invalidMetadata as TableMetadata)).toThrow(
      'Missing or invalid field: current-schema-id'
    );
  });

  it('should throw for empty schemas array', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    const invalidMetadata = { ...metadata, schemas: [] } as unknown as TableMetadata;

    expect(() => writer.validateMetadata(invalidMetadata)).toThrow(
      'Missing or empty field: schemas'
    );
  });

  it('should throw for empty partition-specs array', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    const invalidMetadata = {
      ...metadata,
      'partition-specs': [],
    } as unknown as TableMetadata;

    expect(() => writer.validateMetadata(invalidMetadata)).toThrow(
      'Missing or empty field: partition-specs'
    );
  });

  it('should throw for empty sort-orders array', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    const invalidMetadata = { ...metadata, 'sort-orders': [] } as unknown as TableMetadata;

    expect(() => writer.validateMetadata(invalidMetadata)).toThrow(
      'Missing or empty field: sort-orders'
    );
  });

  it('should throw for invalid current-snapshot-id reference', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    const invalidMetadata = {
      ...metadata,
      'current-snapshot-id': 999999,
    } as TableMetadata;

    expect(() => writer.validateMetadata(invalidMetadata)).toThrow(
      'current-snapshot-id 999999 not found in snapshots'
    );
  });

  it('should throw for invalid current-schema-id reference', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    const invalidMetadata = { ...metadata, 'current-schema-id': 999 } as TableMetadata;

    expect(() => writer.validateMetadata(invalidMetadata)).toThrow(
      'current-schema-id 999 not found in schemas'
    );
  });

  it('should throw for invalid default-spec-id reference', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    const invalidMetadata = { ...metadata, 'default-spec-id': 999 } as TableMetadata;

    expect(() => writer.validateMetadata(invalidMetadata)).toThrow(
      'default-spec-id 999 not found in partition-specs'
    );
  });

  it('should pass validation with valid snapshot', async () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    const snapshot = createTestSnapshot(1);
    const metadataWithSnapshot = {
      ...metadata,
      'current-snapshot-id': snapshot['snapshot-id'],
      snapshots: [snapshot],
    } as TableMetadata;

    expect(() => writer.validateMetadata(metadataWithSnapshot)).not.toThrow();
  });

  it('should throw for invalid properties type', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    const invalidMetadata = { ...metadata, properties: null } as unknown as TableMetadata;

    expect(() => writer.validateMetadata(invalidMetadata)).toThrow(
      'Missing or invalid field: properties'
    );
  });

  it('should throw for missing snapshots array', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    const invalidMetadata = { ...metadata } as Record<string, unknown>;
    delete invalidMetadata['snapshots'];

    expect(() => writer.validateMetadata(invalidMetadata as TableMetadata)).toThrow(
      'Missing field: snapshots'
    );
  });

  it('should throw for missing snapshot-log array', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    const invalidMetadata = { ...metadata } as Record<string, unknown>;
    delete invalidMetadata['snapshot-log'];

    expect(() => writer.validateMetadata(invalidMetadata as TableMetadata)).toThrow(
      'Missing field: snapshot-log'
    );
  });
});

// ============================================================================
// Version Numbering Tests
// ============================================================================

describe('Version Numbering', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let writer: MetadataWriter;
  const tableLocation = 's3://bucket/warehouse/db/table';

  beforeEach(() => {
    storage = createMockStorage();
    writer = new MetadataWriter(storage);
  });

  it('should start at v1 for new tables', async () => {
    const result = await writer.writeNewTable({ location: tableLocation });

    expect(result.version).toBe(1);
    expect(result.metadataLocation).toContain('v1.metadata.json');
  });

  it('should increment version correctly', async () => {
    const result1 = await writer.writeNewTable({ location: tableLocation });
    expect(result1.version).toBe(1);

    const snapshot1 = createTestSnapshot(1);
    const result2 = await writer.writeWithSnapshot(result1.metadata, snapshot1);
    expect(result2.version).toBe(2);
    expect(result2.metadataLocation).toContain('v2.metadata.json');

    const snapshot2 = createTestSnapshot(2);
    const result3 = await writer.writeWithSnapshot(result2.metadata, snapshot2);
    expect(result3.version).toBe(3);
    expect(result3.metadataLocation).toContain('v3.metadata.json');
  });

  it('should handle large version numbers', async () => {
    // Pre-create many metadata files
    const metadataDir = `${tableLocation}/${METADATA_DIR}`;
    for (let i = 1; i <= 100; i++) {
      await storage.put(
        `${metadataDir}/v${i}.metadata.json`,
        new TextEncoder().encode('{}')
      );
    }

    const metadata = writer.createTableMetadata({ location: tableLocation });
    const snapshot = createTestSnapshot(1);

    const result = await writer.writeWithSnapshot(metadata, snapshot);

    expect(result.version).toBe(101);
    expect(result.metadataLocation).toContain('v101.metadata.json');
  });

  it('should find max version from existing files', async () => {
    // Create files with non-sequential versions
    const metadataDir = `${tableLocation}/${METADATA_DIR}`;
    await storage.put(`${metadataDir}/v1.metadata.json`, new TextEncoder().encode('{}'));
    await storage.put(`${metadataDir}/v5.metadata.json`, new TextEncoder().encode('{}'));
    await storage.put(`${metadataDir}/v10.metadata.json`, new TextEncoder().encode('{}'));
    await storage.put(`${metadataDir}/v3.metadata.json`, new TextEncoder().encode('{}'));

    const metadata = writer.createTableMetadata({ location: tableLocation });
    const snapshot = createTestSnapshot(1);

    const result = await writer.writeWithSnapshot(metadata, snapshot);

    expect(result.version).toBe(11);
  });
});

// ============================================================================
// Version Hint Updates Tests
// ============================================================================

describe('Version Hint Updates', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let writer: MetadataWriter;
  const tableLocation = 's3://bucket/warehouse/db/table';

  beforeEach(() => {
    storage = createMockStorage();
    writer = new MetadataWriter(storage);
  });

  it('should create version hint for new table', async () => {
    await writer.writeNewTable({ location: tableLocation });

    const versionHintPath = `${tableLocation}/${METADATA_DIR}/${VERSION_HINT_FILENAME}`;
    const exists = await storage.exists(versionHintPath);
    expect(exists).toBe(true);

    const data = await storage.get(versionHintPath);
    expect(new TextDecoder().decode(data!)).toBe('1');
  });

  it('should update version hint on each write', async () => {
    const versionHintPath = `${tableLocation}/${METADATA_DIR}/${VERSION_HINT_FILENAME}`;

    const result1 = await writer.writeNewTable({ location: tableLocation });
    let data = await storage.get(versionHintPath);
    expect(new TextDecoder().decode(data!)).toBe('1');

    const snapshot1 = createTestSnapshot(1);
    const result2 = await writer.writeWithSnapshot(result1.metadata, snapshot1);
    data = await storage.get(versionHintPath);
    expect(new TextDecoder().decode(data!)).toBe('2');

    const snapshot2 = createTestSnapshot(2);
    await writer.writeWithSnapshot(result2.metadata, snapshot2);
    data = await storage.get(versionHintPath);
    expect(new TextDecoder().decode(data!)).toBe('3');
  });

  it('should store version as plain number string', async () => {
    await writer.writeNewTable({ location: tableLocation });

    const versionHintPath = `${tableLocation}/${METADATA_DIR}/${VERSION_HINT_FILENAME}`;
    const data = await storage.get(versionHintPath);
    const content = new TextDecoder().decode(data!);

    // Should be just the number, no path or JSON
    expect(content).toBe('1');
    expect(parseInt(content, 10)).toBe(1);
  });
});

// ============================================================================
// Metadata File Content Verification Tests
// ============================================================================

describe('Metadata File Content Verification', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let writer: MetadataWriter;
  const tableLocation = 's3://bucket/warehouse/db/table';

  beforeEach(() => {
    storage = createMockStorage();
    writer = new MetadataWriter(storage);
  });

  it('should write all required Iceberg v2 fields', async () => {
    const result = await writer.writeNewTable({ location: tableLocation });

    const metadataData = await storage.get(result.metadataLocation);
    const metadata = JSON.parse(new TextDecoder().decode(metadataData!));

    // Required fields per Iceberg spec
    expect(metadata).toHaveProperty('format-version');
    expect(metadata).toHaveProperty('table-uuid');
    expect(metadata).toHaveProperty('location');
    expect(metadata).toHaveProperty('last-sequence-number');
    expect(metadata).toHaveProperty('last-updated-ms');
    expect(metadata).toHaveProperty('last-column-id');
    expect(metadata).toHaveProperty('current-schema-id');
    expect(metadata).toHaveProperty('schemas');
    expect(metadata).toHaveProperty('default-spec-id');
    expect(metadata).toHaveProperty('partition-specs');
    expect(metadata).toHaveProperty('last-partition-id');
    expect(metadata).toHaveProperty('default-sort-order-id');
    expect(metadata).toHaveProperty('sort-orders');
    expect(metadata).toHaveProperty('properties');
    expect(metadata).toHaveProperty('current-snapshot-id');
    expect(metadata).toHaveProperty('snapshots');
    expect(metadata).toHaveProperty('snapshot-log');
    expect(metadata).toHaveProperty('metadata-log');
    expect(metadata).toHaveProperty('refs');
  });

  it('should write correct field types', async () => {
    const result = await writer.writeNewTable({ location: tableLocation });

    const metadataData = await storage.get(result.metadataLocation);
    const metadata = JSON.parse(new TextDecoder().decode(metadataData!));

    expect(typeof metadata['format-version']).toBe('number');
    expect(typeof metadata['table-uuid']).toBe('string');
    expect(typeof metadata['location']).toBe('string');
    expect(typeof metadata['last-sequence-number']).toBe('number');
    expect(typeof metadata['last-updated-ms']).toBe('number');
    expect(typeof metadata['last-column-id']).toBe('number');
    expect(typeof metadata['current-schema-id']).toBe('number');
    expect(Array.isArray(metadata['schemas'])).toBe(true);
    expect(typeof metadata['default-spec-id']).toBe('number');
    expect(Array.isArray(metadata['partition-specs'])).toBe(true);
    expect(typeof metadata['last-partition-id']).toBe('number');
    expect(typeof metadata['default-sort-order-id']).toBe('number');
    expect(Array.isArray(metadata['sort-orders'])).toBe(true);
    expect(typeof metadata['properties']).toBe('object');
    expect(metadata['current-snapshot-id']).toBeNull();
    expect(Array.isArray(metadata['snapshots'])).toBe(true);
    expect(Array.isArray(metadata['snapshot-log'])).toBe(true);
    expect(Array.isArray(metadata['metadata-log'])).toBe(true);
    expect(typeof metadata['refs']).toBe('object');
  });

  it('should serialize schema correctly', async () => {
    const customSchema: IcebergSchema = {
      'schema-id': 0,
      type: 'struct',
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'name', required: false, type: 'string' },
        { id: 3, name: 'created', required: true, type: 'timestamp' },
      ],
    };

    const result = await writer.writeNewTable({
      location: tableLocation,
      schema: customSchema,
    });

    const metadataData = await storage.get(result.metadataLocation);
    const metadata = JSON.parse(new TextDecoder().decode(metadataData!));

    expect(metadata.schemas).toHaveLength(1);
    expect(metadata.schemas[0]['schema-id']).toBe(0);
    expect(metadata.schemas[0].type).toBe('struct');
    expect(metadata.schemas[0].fields).toHaveLength(3);
    expect(metadata.schemas[0].fields[0].name).toBe('id');
    expect(metadata.schemas[0].fields[0].type).toBe('long');
    expect(metadata.schemas[0].fields[0].required).toBe(true);
  });

  it('should serialize snapshot correctly', async () => {
    const result1 = await writer.writeNewTable({ location: tableLocation });

    const snapshot = createTestSnapshot(1);
    const result2 = await writer.writeWithSnapshot(result1.metadata, snapshot);

    const metadataData = await storage.get(result2.metadataLocation);
    const metadata = JSON.parse(new TextDecoder().decode(metadataData!));

    expect(metadata.snapshots).toHaveLength(1);
    const savedSnapshot = metadata.snapshots[0];

    expect(savedSnapshot['snapshot-id']).toBe(snapshot['snapshot-id']);
    expect(savedSnapshot['sequence-number']).toBe(snapshot['sequence-number']);
    expect(savedSnapshot['timestamp-ms']).toBe(snapshot['timestamp-ms']);
    expect(savedSnapshot['manifest-list']).toBe(snapshot['manifest-list']);
    expect(savedSnapshot.summary.operation).toBe('append');
  });

  it('should be parseable as valid JSON', async () => {
    const result = await writer.writeNewTable({ location: tableLocation });

    const metadataData = await storage.get(result.metadataLocation);
    const jsonString = new TextDecoder().decode(metadataData!);

    expect(() => JSON.parse(jsonString)).not.toThrow();
  });
});

// ============================================================================
// writeIfMissing Tests
// ============================================================================

describe('writeIfMissing', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let writer: MetadataWriter;
  const tableLocation = 's3://bucket/warehouse/db/table';

  beforeEach(() => {
    storage = createMockStorage();
    writer = new MetadataWriter(storage);
  });

  it('should create table if it does not exist', async () => {
    const result = await writer.writeIfMissing({ location: tableLocation });

    expect(result.version).toBe(1);
    expect(result.metadata.location).toBe(tableLocation);

    const exists = await storage.exists(result.metadataLocation);
    expect(exists).toBe(true);
  });

  it('should return existing metadata if table exists', async () => {
    // Create initial table with properties
    const original = await writer.writeNewTable({
      location: tableLocation,
      properties: { 'original.key': 'original.value' },
    });

    // Try to write again with different properties
    const result = await writer.writeIfMissing({
      location: tableLocation,
      properties: { 'new.key': 'new.value' },
    });

    // Should return original metadata
    expect(result.version).toBe(1);
    expect(result.metadata.properties['original.key']).toBe('original.value');
    expect(result.metadata.properties['new.key']).toBeUndefined();
  });

  it('should be idempotent', async () => {
    const result1 = await writer.writeIfMissing({ location: tableLocation });
    const result2 = await writer.writeIfMissing({ location: tableLocation });
    const result3 = await writer.writeIfMissing({ location: tableLocation });

    expect(result1.version).toBe(result2.version);
    expect(result2.version).toBe(result3.version);
    expect(result1.metadata['table-uuid']).toBe(result2.metadata['table-uuid']);
  });
});

// ============================================================================
// serializeMetadata Tests
// ============================================================================

describe('serializeMetadata', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let writer: MetadataWriter;

  beforeEach(() => {
    storage = createMockStorage();
    writer = new MetadataWriter(storage);
  });

  it('should serialize metadata to JSON string', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    const json = writer.serializeMetadata(metadata);

    expect(typeof json).toBe('string');
    expect(() => JSON.parse(json)).not.toThrow();
  });

  it('should produce pretty-printed JSON', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    const json = writer.serializeMetadata(metadata);

    expect(json).toContain('\n');
    expect(json).toContain('  ');
  });

  it('should be reversible', () => {
    const metadata = writer.createTableMetadata({
      location: 's3://bucket/table',
      properties: { key: 'value' },
    });

    const json = writer.serializeMetadata(metadata);
    const parsed = JSON.parse(json) as TableMetadata;

    expect(parsed.location).toBe(metadata.location);
    expect(parsed['table-uuid']).toBe(metadata['table-uuid']);
    expect(parsed.properties.key).toBe(metadata.properties.key);
  });
});

// ============================================================================
// Edge Cases Tests
// ============================================================================

describe('Edge Cases', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let writer: MetadataWriter;

  beforeEach(() => {
    storage = createMockStorage();
    writer = new MetadataWriter(storage);
  });

  describe('Invalid locations', () => {
    it('should handle empty location string', async () => {
      const result = await writer.writeNewTable({ location: '' });

      // Writer accepts empty string but creates metadata with empty location
      expect(result.metadata.location).toBe('');
    });

    it('should handle location with special characters', async () => {
      const location = 's3://bucket/path/with spaces/and-dashes/and_underscores';
      const result = await writer.writeNewTable({ location });

      expect(result.metadata.location).toBe(location);
    });

    it('should handle very long location paths', async () => {
      const longPath = 's3://bucket/' + 'a'.repeat(1000);
      const result = await writer.writeNewTable({ location: longPath });

      expect(result.metadata.location).toBe(longPath);
    });

    it('should handle location with trailing slash', async () => {
      const location = 's3://bucket/table/';
      const result = await writer.writeNewTable({ location });

      expect(result.metadata.location).toBe(location);
      expect(result.metadataLocation).toContain('table//metadata/');
    });
  });

  describe('Missing required options', () => {
    it('should work with minimal options (only location)', async () => {
      const result = await writer.writeNewTable({
        location: 's3://bucket/table',
      });

      expect(result.metadata['format-version']).toBe(FORMAT_VERSION);
      expect(result.metadata.location).toBe('s3://bucket/table');
      expect(result.metadata['table-uuid']).toBeTruthy();
    });
  });

  describe('Storage failures', () => {
    it('should propagate error when storage.put fails', async () => {
      storage.failOnKey = 's3://bucket/table/metadata/v1.metadata.json';

      await expect(
        writer.writeNewTable({ location: 's3://bucket/table' })
      ).rejects.toThrow('Simulated failure');
    });

    it('should propagate error when storage.exists fails', async () => {
      storage.failOnKey = 's3://bucket/table/metadata/v1.metadata.json';

      await expect(
        writer.writeNewTable({ location: 's3://bucket/table' })
      ).rejects.toThrow('Simulated failure');
    });

    it('should propagate error when version hint write fails', async () => {
      // Need to let metadata file write succeed but fail on version hint
      const originalPut = storage.put.bind(storage);
      let callCount = 0;

      storage.put = async (key: string, value: Uint8Array): Promise<void> => {
        callCount++;
        if (callCount === 2) {
          // Fail on second put (version hint)
          throw new Error('Version hint write failed');
        }
        return originalPut(key, value);
      };

      await expect(
        writer.writeNewTable({ location: 's3://bucket/table' })
      ).rejects.toThrow('Version hint write failed');
    });
  });

  describe('Cleanup on failure', () => {
    it('should not leave partial state when metadata write fails', async () => {
      storage.failOnKey = 's3://bucket/table/metadata/v1.metadata.json';

      try {
        await writer.writeNewTable({ location: 's3://bucket/table' });
      } catch {
        // Expected
      }

      // Storage should not have any files for this table
      const files = await storage.list('s3://bucket/table/metadata/');
      expect(files).toHaveLength(0);
    });
  });

  describe('Concurrent writes', () => {
    it('should handle race condition when table already exists', async () => {
      // Simulate race condition where table is created between exists check and put
      let existsCalled = false;
      const originalExists = storage.exists.bind(storage);

      storage.exists = async (key: string): Promise<boolean> => {
        const result = await originalExists(key);
        if (!existsCalled && key.includes('v1.metadata.json')) {
          existsCalled = true;
          // Simulate another process creating the table
          await storage.put(key, new TextEncoder().encode('{}'));
        }
        return result;
      };

      // First write should succeed
      const result = await writer.writeNewTable({ location: 's3://bucket/table1' });
      expect(result.version).toBe(1);

      // The simulated race only affects first call, so we test
      // that the writer properly checks existence
      await expect(
        writer.writeNewTable({ location: 's3://bucket/table1' })
      ).rejects.toThrow('Table already exists');
    });

    it('should handle concurrent version detection', async () => {
      const tableLocation = 's3://bucket/concurrent-table';
      const result1 = await writer.writeNewTable({ location: tableLocation });

      // Simulate two concurrent writes by manually writing a file
      const metadataDir = `${tableLocation}/${METADATA_DIR}`;
      await storage.put(`${metadataDir}/v2.metadata.json`, new TextEncoder().encode('{}'));

      // Next write should detect the existing v2 and create v3
      const snapshot = createTestSnapshot(1);
      const result2 = await writer.writeWithSnapshot(result1.metadata, snapshot);

      expect(result2.version).toBe(3);
    });
  });

  describe('Special property values', () => {
    it('should handle properties with empty string values', async () => {
      const result = await writer.writeNewTable({
        location: 's3://bucket/table',
        properties: { 'empty.key': '' },
      });

      expect(result.metadata.properties['empty.key']).toBe('');
    });

    it('should handle properties with special characters', async () => {
      const result = await writer.writeNewTable({
        location: 's3://bucket/table',
        properties: {
          'key.with.dots': 'value',
          'key-with-dashes': 'value',
          'key_with_underscores': 'value',
          'key:with:colons': 'value',
        },
      });

      expect(result.metadata.properties['key.with.dots']).toBe('value');
      expect(result.metadata.properties['key-with-dashes']).toBe('value');
      expect(result.metadata.properties['key_with_underscores']).toBe('value');
      expect(result.metadata.properties['key:with:colons']).toBe('value');
    });

    it('should handle large number of properties', async () => {
      const properties: Record<string, string> = {};
      for (let i = 0; i < 1000; i++) {
        properties[`key.${i}`] = `value-${i}`;
      }

      const result = await writer.writeNewTable({
        location: 's3://bucket/table',
        properties,
      });

      expect(Object.keys(result.metadata.properties)).toHaveLength(1000);
    });
  });
});

// ============================================================================
// Convenience Function Tests
// ============================================================================

describe('Convenience Functions', () => {
  let storage: ReturnType<typeof createMockStorage>;
  const tableLocation = 's3://bucket/warehouse/db/table';

  beforeEach(() => {
    storage = createMockStorage();
  });

  describe('writeNewTableMetadata', () => {
    it('should create and write new table metadata', async () => {
      const result = await writeNewTableMetadata(storage, {
        location: tableLocation,
      });

      expect(result.version).toBe(1);
      expect(result.metadata.location).toBe(tableLocation);

      const exists = await storage.exists(result.metadataLocation);
      expect(exists).toBe(true);
    });

    it('should accept all MetadataWriterOptions', async () => {
      const result = await writeNewTableMetadata(storage, {
        location: tableLocation,
        tableUuid: 'custom-uuid-1234',
        schema: createDefaultSchema(),
        partitionSpec: createUnpartitionedSpec(),
        sortOrder: createUnsortedOrder(),
        properties: { test: 'value' },
      });

      expect(result.metadata['table-uuid']).toBe('custom-uuid-1234');
      expect(result.metadata.properties.test).toBe('value');
    });
  });

  describe('writeMetadataIfMissing', () => {
    it('should create table if not exists', async () => {
      const result = await writeMetadataIfMissing(storage, {
        location: tableLocation,
      });

      expect(result.version).toBe(1);
    });

    it('should return existing table if exists', async () => {
      await writeNewTableMetadata(storage, {
        location: tableLocation,
        properties: { original: 'true' },
      });

      const result = await writeMetadataIfMissing(storage, {
        location: tableLocation,
        properties: { different: 'true' },
      });

      expect(result.metadata.properties.original).toBe('true');
      expect(result.metadata.properties.different).toBeUndefined();
    });
  });
});
