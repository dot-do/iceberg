/**
 * Tests for Iceberg Table Encryption Keys
 *
 * @see https://iceberg.apache.org/spec/#table-metadata
 *
 * Iceberg supports optional encryption key management via the encryption-keys field:
 *
 * TableMetadata fields:
 * - encryption-keys: Optional list of encryption keys used for table encryption
 *   - Each key has a unique key-id (integer)
 *   - Each key has key-metadata (base64 encoded string containing encrypted key material)
 */

import { describe, it, expect } from 'vitest';
import {
  TableMetadataBuilder,
  MetadataWriter,
  type TableMetadata,
  type StorageBackend,
  type EncryptionKey,
} from '../../../src/index.js';

// Mock storage backend for testing
function createMockStorage(): StorageBackend & { files: Map<string, Uint8Array> } {
  const files = new Map<string, Uint8Array>();
  return {
    files,
    async get(key: string) {
      return files.get(key) ?? null;
    },
    async put(key: string, data: Uint8Array) {
      files.set(key, data);
    },
    async delete(key: string) {
      files.delete(key);
    },
    async list(prefix: string) {
      return Array.from(files.keys()).filter((k) => k.startsWith(prefix));
    },
    async exists(key: string) {
      return files.has(key);
    },
  };
}

describe('Encryption Keys: Type Tests', () => {
  it('should allow encryption-keys field in TableMetadata', () => {
    // TableMetadata should accept an optional encryption-keys field
    const metadata: TableMetadata = {
      'format-version': 2,
      'table-uuid': 'test-uuid',
      location: 's3://bucket/table',
      'last-sequence-number': 0,
      'last-updated-ms': Date.now(),
      'last-column-id': 1,
      'current-schema-id': 0,
      schemas: [{ 'schema-id': 0, type: 'struct', fields: [] }],
      'default-spec-id': 0,
      'partition-specs': [{ 'spec-id': 0, fields: [] }],
      'last-partition-id': 999,
      'default-sort-order-id': 0,
      'sort-orders': [{ 'order-id': 0, fields: [] }],
      properties: {},
      'current-snapshot-id': null,
      snapshots: [],
      'snapshot-log': [],
      'metadata-log': [],
      refs: {},
      'encryption-keys': [
        { 'key-id': 1, 'key-metadata': 'YmFzZTY0LWVuY29kZWQta2V5LW1ldGFkYXRh' },
      ],
    };

    expect(metadata['encryption-keys']).toBeDefined();
    expect(metadata['encryption-keys']).toHaveLength(1);
  });

  it('should allow encryption-keys to be undefined (no encryption)', () => {
    // encryption-keys is optional - tables without encryption should not have it
    const metadata: TableMetadata = {
      'format-version': 2,
      'table-uuid': 'test-uuid',
      location: 's3://bucket/table',
      'last-sequence-number': 0,
      'last-updated-ms': Date.now(),
      'last-column-id': 1,
      'current-schema-id': 0,
      schemas: [{ 'schema-id': 0, type: 'struct', fields: [] }],
      'default-spec-id': 0,
      'partition-specs': [{ 'spec-id': 0, fields: [] }],
      'last-partition-id': 999,
      'default-sort-order-id': 0,
      'sort-orders': [{ 'order-id': 0, fields: [] }],
      properties: {},
      'current-snapshot-id': null,
      snapshots: [],
      'snapshot-log': [],
      'metadata-log': [],
      refs: {},
    };

    expect(metadata['encryption-keys']).toBeUndefined();
  });

  it('should accept encryption-keys as an array/list', () => {
    const encryptionKeys: readonly EncryptionKey[] = [
      { 'key-id': 1, 'key-metadata': 'a2V5LW1ldGFkYXRhLTE=' },
      { 'key-id': 2, 'key-metadata': 'a2V5LW1ldGFkYXRhLTI=' },
    ];

    expect(Array.isArray(encryptionKeys)).toBe(true);
    expect(encryptionKeys).toHaveLength(2);
  });

  it('should have correct encryption key structure (key-id and key-metadata)', () => {
    const key: EncryptionKey = {
      'key-id': 42,
      'key-metadata': 'c2VjcmV0LWtleS1kYXRh',
    };

    expect(key['key-id']).toBe(42);
    expect(typeof key['key-id']).toBe('number');
    expect(key['key-metadata']).toBe('c2VjcmV0LWtleS1kYXRh');
    expect(typeof key['key-metadata']).toBe('string');
  });

  it('should allow multiple encryption keys', () => {
    const metadata: TableMetadata = {
      'format-version': 2,
      'table-uuid': 'test-uuid',
      location: 's3://bucket/table',
      'last-sequence-number': 0,
      'last-updated-ms': Date.now(),
      'last-column-id': 1,
      'current-schema-id': 0,
      schemas: [{ 'schema-id': 0, type: 'struct', fields: [] }],
      'default-spec-id': 0,
      'partition-specs': [{ 'spec-id': 0, fields: [] }],
      'last-partition-id': 999,
      'default-sort-order-id': 0,
      'sort-orders': [{ 'order-id': 0, fields: [] }],
      properties: {},
      'current-snapshot-id': null,
      snapshots: [],
      'snapshot-log': [],
      'metadata-log': [],
      refs: {},
      'encryption-keys': [
        { 'key-id': 1, 'key-metadata': 'a2V5MQ==' },
        { 'key-id': 2, 'key-metadata': 'a2V5Mg==' },
        { 'key-id': 3, 'key-metadata': 'a2V5Mw==' },
      ],
    };

    expect(metadata['encryption-keys']).toHaveLength(3);
    expect(metadata['encryption-keys']![0]['key-id']).toBe(1);
    expect(metadata['encryption-keys']![1]['key-id']).toBe(2);
    expect(metadata['encryption-keys']![2]['key-id']).toBe(3);
  });
});

describe('Encryption Keys: TableMetadataBuilder Tests', () => {
  it('should create table without encryption keys by default', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/table',
    });
    const metadata = builder.build();

    expect(metadata['encryption-keys']).toBeUndefined();
  });

  it('should create table with encryption keys when provided', () => {
    const encryptionKeys: EncryptionKey[] = [
      { 'key-id': 1, 'key-metadata': 'ZW5jcnlwdGVkLWtleS1tYXRlcmlhbA==' },
    ];

    const builder = new TableMetadataBuilder({
      location: 's3://bucket/table',
      encryptionKeys,
    });
    const metadata = builder.build();

    expect(metadata['encryption-keys']).toBeDefined();
    expect(metadata['encryption-keys']).toHaveLength(1);
    expect(metadata['encryption-keys']![0]['key-id']).toBe(1);
    expect(metadata['encryption-keys']![0]['key-metadata']).toBe('ZW5jcnlwdGVkLWtleS1tYXRlcmlhbA==');
  });

  it('should allow adding encryption keys to existing metadata', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/table',
    });

    builder.addEncryptionKey({
      'key-id': 1,
      'key-metadata': 'bmV3LWtleQ==',
    });

    const metadata = builder.build();

    expect(metadata['encryption-keys']).toBeDefined();
    expect(metadata['encryption-keys']).toHaveLength(1);
    expect(metadata['encryption-keys']![0]['key-id']).toBe(1);
  });

  it('should allow adding multiple encryption keys', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/table',
    });

    builder.addEncryptionKey({ 'key-id': 1, 'key-metadata': 'a2V5MQ==' });
    builder.addEncryptionKey({ 'key-id': 2, 'key-metadata': 'a2V5Mg==' });

    const metadata = builder.build();

    expect(metadata['encryption-keys']).toHaveLength(2);
  });

  it('should preserve encryption keys when rebuilding from existing metadata', () => {
    const encryptionKeys: EncryptionKey[] = [
      { 'key-id': 1, 'key-metadata': 'cHJlc2VydmVkLWtleQ==' },
    ];

    const originalBuilder = new TableMetadataBuilder({
      location: 's3://bucket/table',
      encryptionKeys,
    });
    const original = originalBuilder.build();

    const rebuiltBuilder = TableMetadataBuilder.fromMetadata(original);
    const rebuilt = rebuiltBuilder.build();

    expect(rebuilt['encryption-keys']).toBeDefined();
    expect(rebuilt['encryption-keys']).toHaveLength(1);
    expect(rebuilt['encryption-keys']![0]['key-id']).toBe(1);
    expect(rebuilt['encryption-keys']![0]['key-metadata']).toBe('cHJlc2VydmVkLWtleQ==');
  });
});

describe('Encryption Keys: JSON Serialization Tests', () => {
  it('should serialize encryption-keys in metadata JSON', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/table',
      encryptionKeys: [{ 'key-id': 1, 'key-metadata': 'c2VyaWFsaXplZA==' }],
    });
    const json = builder.toJSON();

    expect(json).toContain('"encryption-keys"');
    expect(json).toContain('"key-id": 1');
    expect(json).toContain('"key-metadata": "c2VyaWFsaXplZA=="');
  });

  it('should not include encryption-keys in JSON when not provided', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/table',
    });
    const json = builder.toJSON();

    expect(json).not.toContain('"encryption-keys"');
  });

  it('should round-trip serialize encryption-keys', () => {
    const originalKeys: EncryptionKey[] = [
      { 'key-id': 1, 'key-metadata': 'cm91bmQtdHJpcA==' },
      { 'key-id': 2, 'key-metadata': 'c2VyaWFsaXphdGlvbg==' },
    ];

    const builder = new TableMetadataBuilder({
      location: 's3://bucket/table',
      encryptionKeys: originalKeys,
    });
    const original = builder.build();
    const json = JSON.stringify(original);
    const parsed = JSON.parse(json) as TableMetadata;

    expect(parsed['encryption-keys']).toBeDefined();
    expect(parsed['encryption-keys']).toHaveLength(2);
    expect(parsed['encryption-keys']![0]['key-id']).toBe(1);
    expect(parsed['encryption-keys']![0]['key-metadata']).toBe('cm91bmQtdHJpcA==');
    expect(parsed['encryption-keys']![1]['key-id']).toBe(2);
    expect(parsed['encryption-keys']![1]['key-metadata']).toBe('c2VyaWFsaXphdGlvbg==');
  });

  it('should preserve all metadata fields through JSON round-trip with encryption keys', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/table',
      properties: { 'test.property': 'value' },
      encryptionKeys: [{ 'key-id': 42, 'key-metadata': 'ZnVsbC1yb3VuZC10cmlw' }],
    });
    const original = builder.build();
    const json = JSON.stringify(original, null, 2);
    const parsed = JSON.parse(json) as TableMetadata;

    expect(parsed['format-version']).toBe(2);
    expect(parsed.location).toBe('s3://bucket/table');
    expect(parsed.properties['test.property']).toBe('value');
    expect(parsed['encryption-keys']).toHaveLength(1);
    expect(parsed['encryption-keys']![0]['key-id']).toBe(42);
  });
});

describe('Encryption Keys: Validation Tests', () => {
  it('should fail validation for duplicate encryption key IDs', () => {
    const storage = createMockStorage();
    const writer = new MetadataWriter(storage);

    // Create metadata with duplicate key IDs
    const invalidMetadata = {
      'format-version': 2,
      'table-uuid': 'test-uuid',
      location: 's3://bucket/table',
      'last-sequence-number': 0,
      'last-updated-ms': Date.now(),
      'last-column-id': 1,
      'current-schema-id': 0,
      schemas: [{ 'schema-id': 0, type: 'struct', fields: [] }],
      'default-spec-id': 0,
      'partition-specs': [{ 'spec-id': 0, fields: [] }],
      'last-partition-id': 999,
      'default-sort-order-id': 0,
      'sort-orders': [{ 'order-id': 0, fields: [] }],
      properties: {},
      'current-snapshot-id': null,
      snapshots: [],
      'snapshot-log': [],
      'metadata-log': [],
      refs: {},
      'encryption-keys': [
        { 'key-id': 1, 'key-metadata': 'a2V5MQ==' },
        { 'key-id': 1, 'key-metadata': 'a2V5Mg==' }, // Duplicate key-id
      ],
    } as TableMetadata;

    expect(() => writer.validateMetadata(invalidMetadata)).toThrow(/encryption.*key.*duplicate|key-id.*unique/i);
  });

  it('should pass validation for unique encryption key IDs', () => {
    const storage = createMockStorage();
    const writer = new MetadataWriter(storage);

    const validMetadata = {
      'format-version': 2,
      'table-uuid': 'test-uuid',
      location: 's3://bucket/table',
      'last-sequence-number': 0,
      'last-updated-ms': Date.now(),
      'last-column-id': 1,
      'current-schema-id': 0,
      schemas: [{ 'schema-id': 0, type: 'struct', fields: [] }],
      'default-spec-id': 0,
      'partition-specs': [{ 'spec-id': 0, fields: [] }],
      'last-partition-id': 999,
      'default-sort-order-id': 0,
      'sort-orders': [{ 'order-id': 0, fields: [] }],
      properties: {},
      'current-snapshot-id': null,
      snapshots: [],
      'snapshot-log': [],
      'metadata-log': [],
      refs: {},
      'encryption-keys': [
        { 'key-id': 1, 'key-metadata': 'a2V5MQ==' },
        { 'key-id': 2, 'key-metadata': 'a2V5Mg==' },
      ],
    } as TableMetadata;

    expect(() => writer.validateMetadata(validMetadata)).not.toThrow();
  });

  it('should pass validation for empty encryption keys array', () => {
    const storage = createMockStorage();
    const writer = new MetadataWriter(storage);

    const validMetadata = {
      'format-version': 2,
      'table-uuid': 'test-uuid',
      location: 's3://bucket/table',
      'last-sequence-number': 0,
      'last-updated-ms': Date.now(),
      'last-column-id': 1,
      'current-schema-id': 0,
      schemas: [{ 'schema-id': 0, type: 'struct', fields: [] }],
      'default-spec-id': 0,
      'partition-specs': [{ 'spec-id': 0, fields: [] }],
      'last-partition-id': 999,
      'default-sort-order-id': 0,
      'sort-orders': [{ 'order-id': 0, fields: [] }],
      properties: {},
      'current-snapshot-id': null,
      snapshots: [],
      'snapshot-log': [],
      'metadata-log': [],
      refs: {},
      'encryption-keys': [],
    } as TableMetadata;

    expect(() => writer.validateMetadata(validMetadata)).not.toThrow();
  });

  it('should pass validation when encryption-keys is not provided', () => {
    const storage = createMockStorage();
    const writer = new MetadataWriter(storage);

    const validMetadata = writer.createTableMetadata({
      location: 's3://bucket/table',
    });

    expect(() => writer.validateMetadata(validMetadata)).not.toThrow();
  });

  it('should fail validation for invalid key-metadata (non-string)', () => {
    const storage = createMockStorage();
    const writer = new MetadataWriter(storage);

    // Create metadata with invalid key-metadata type
    const invalidMetadata = {
      'format-version': 2,
      'table-uuid': 'test-uuid',
      location: 's3://bucket/table',
      'last-sequence-number': 0,
      'last-updated-ms': Date.now(),
      'last-column-id': 1,
      'current-schema-id': 0,
      schemas: [{ 'schema-id': 0, type: 'struct', fields: [] }],
      'default-spec-id': 0,
      'partition-specs': [{ 'spec-id': 0, fields: [] }],
      'last-partition-id': 999,
      'default-sort-order-id': 0,
      'sort-orders': [{ 'order-id': 0, fields: [] }],
      properties: {},
      'current-snapshot-id': null,
      snapshots: [],
      'snapshot-log': [],
      'metadata-log': [],
      refs: {},
      'encryption-keys': [
        { 'key-id': 1, 'key-metadata': 12345 }, // Invalid: should be string
      ],
    } as unknown as TableMetadata;

    expect(() => writer.validateMetadata(invalidMetadata)).toThrow(/key-metadata.*string/i);
  });

  it('should fail validation for invalid key-id (non-number)', () => {
    const storage = createMockStorage();
    const writer = new MetadataWriter(storage);

    // Create metadata with invalid key-id type
    const invalidMetadata = {
      'format-version': 2,
      'table-uuid': 'test-uuid',
      location: 's3://bucket/table',
      'last-sequence-number': 0,
      'last-updated-ms': Date.now(),
      'last-column-id': 1,
      'current-schema-id': 0,
      schemas: [{ 'schema-id': 0, type: 'struct', fields: [] }],
      'default-spec-id': 0,
      'partition-specs': [{ 'spec-id': 0, fields: [] }],
      'last-partition-id': 999,
      'default-sort-order-id': 0,
      'sort-orders': [{ 'order-id': 0, fields: [] }],
      properties: {},
      'current-snapshot-id': null,
      snapshots: [],
      'snapshot-log': [],
      'metadata-log': [],
      refs: {},
      'encryption-keys': [
        { 'key-id': 'not-a-number', 'key-metadata': 'dGVzdA==' }, // Invalid: should be number
      ],
    } as unknown as TableMetadata;

    expect(() => writer.validateMetadata(invalidMetadata)).toThrow(/key-id.*number/i);
  });
});

describe('Encryption Keys: Storage Write Tests', () => {
  it('should write encryption-keys to storage', async () => {
    const storage = createMockStorage();
    const writer = new MetadataWriter(storage);

    const result = await writer.writeNewTable({
      location: 's3://bucket/warehouse/db/table',
      encryptionKeys: [{ 'key-id': 1, 'key-metadata': 'c3RvcmFnZS10ZXN0' }],
    });

    const data = storage.files.get(result.metadataLocation);
    expect(data).toBeDefined();

    const parsed = JSON.parse(new TextDecoder().decode(data!));
    expect(parsed['encryption-keys']).toBeDefined();
    expect(parsed['encryption-keys']).toHaveLength(1);
    expect(parsed['encryption-keys'][0]['key-id']).toBe(1);
    expect(parsed['encryption-keys'][0]['key-metadata']).toBe('c3RvcmFnZS10ZXN0');
  });

  it('should not write encryption-keys to storage when not provided', async () => {
    const storage = createMockStorage();
    const writer = new MetadataWriter(storage);

    const result = await writer.writeNewTable({
      location: 's3://bucket/warehouse/db/table',
    });

    const data = storage.files.get(result.metadataLocation);
    expect(data).toBeDefined();

    const parsed = JSON.parse(new TextDecoder().decode(data!));
    expect(parsed['encryption-keys']).toBeUndefined();
  });

  it('should preserve encryption-keys through updates', async () => {
    const storage = createMockStorage();
    const writer = new MetadataWriter(storage);

    const result1 = await writer.writeNewTable({
      location: 's3://bucket/warehouse/db/table',
      encryptionKeys: [{ 'key-id': 1, 'key-metadata': 'cHJlc2VydmVkLWtleQ==' }],
    });

    // Rebuild with modifications
    const builder = TableMetadataBuilder.fromMetadata(result1.metadata);
    builder.setProperty('test.key', 'test.value');
    const modified = builder.build();

    expect(modified['encryption-keys']).toBeDefined();
    expect(modified['encryption-keys']).toHaveLength(1);
    expect(modified['encryption-keys']![0]['key-id']).toBe(1);
    expect(modified['encryption-keys']![0]['key-metadata']).toBe('cHJlc2VydmVkLWtleQ==');
  });
});

// ============================================================================
// Snapshot key-id Tests (iceberg-ehv.16)
// ============================================================================

import { SnapshotBuilder, type Snapshot } from '../../../src/index.js';

/**
 * Tests for Snapshot key-id field for manifest list encryption.
 *
 * The key-id field in Snapshot references an encryption key that encrypts
 * the manifest list key metadata. This is used for table-level encryption.
 *
 * Snapshot field:
 * - key-id: Optional. ID of the encryption key that encrypts the manifest list key metadata.
 *   - Type: number (int)
 *   - References an encryption key from table metadata's encryption-keys list
 *   - Optional for both v2 and v3 tables (only needed when encryption is enabled)
 */

describe('Snapshot key-id: Type Tests', () => {
  it('should allow optional key-id field in Snapshot (number)', () => {
    // Snapshot should accept an optional key-id field
    const snapshot: Snapshot = {
      'snapshot-id': 1234567890,
      'sequence-number': 1,
      'timestamp-ms': Date.now(),
      'manifest-list': 's3://bucket/table/metadata/snap-1234567890-1-manifest-list.avro',
      summary: { operation: 'append' },
      'schema-id': 0,
      'key-id': 1,
    };

    expect(snapshot['key-id']).toBe(1);
    expect(typeof snapshot['key-id']).toBe('number');
  });

  it('should allow snapshot without key-id (no encryption)', () => {
    // key-id is optional - snapshots without encryption do not need it
    const snapshot: Snapshot = {
      'snapshot-id': 1234567890,
      'sequence-number': 1,
      'timestamp-ms': Date.now(),
      'manifest-list': 's3://bucket/table/metadata/snap-1234567890-1-manifest-list.avro',
      summary: { operation: 'append' },
      'schema-id': 0,
      // No key-id - valid for unencrypted tables
    };

    expect(snapshot['key-id']).toBeUndefined();
  });

  it('should allow key-id to reference encryption key from table metadata', () => {
    // key-id references an encryption key from table metadata
    const snapshot: Snapshot = {
      'snapshot-id': 1234567890,
      'sequence-number': 1,
      'timestamp-ms': Date.now(),
      'manifest-list': 's3://bucket/table/metadata/snap-1234567890-1-manifest-list.avro',
      summary: { operation: 'append' },
      'schema-id': 0,
      'key-id': 42, // References encryption key with ID 42
    };

    expect(snapshot['key-id']).toBe(42);
  });
});

describe('Snapshot key-id: Snapshot Creation Tests', () => {
  it('should create snapshot without key-id (default, no encryption)', () => {
    const builder = new SnapshotBuilder({
      sequenceNumber: 1,
      manifestListPath: 's3://bucket/table/metadata/snap-1-manifest-list.avro',
      operation: 'append',
    });
    const snapshot = builder.build();

    // Without encryption, key-id should not be present
    expect(snapshot['key-id']).toBeUndefined();
  });

  it('should create snapshot with key-id when provided', () => {
    const builder = new SnapshotBuilder({
      sequenceNumber: 1,
      manifestListPath: 's3://bucket/table/metadata/snap-1-manifest-list.avro',
      operation: 'append',
      keyId: 1,
    });
    const snapshot = builder.build();

    expect(snapshot['key-id']).toBe(1);
  });

  it('should accept keyId option in SnapshotBuilder', () => {
    const builder = new SnapshotBuilder({
      sequenceNumber: 1,
      manifestListPath: 's3://bucket/table/metadata/snap-1-manifest-list.avro',
      operation: 'append',
      keyId: 100,
    });
    const snapshot = builder.build();

    expect(snapshot['key-id']).toBe(100);
  });

  it('should support v2 snapshot with key-id', () => {
    const builder = new SnapshotBuilder({
      sequenceNumber: 1,
      manifestListPath: 's3://bucket/table/metadata/snap-1-manifest-list.avro',
      operation: 'append',
      formatVersion: 2,
      keyId: 5,
    });
    const snapshot = builder.build();

    expect(snapshot['key-id']).toBe(5);
  });

  it('should support v3 snapshot with key-id', () => {
    const builder = new SnapshotBuilder({
      sequenceNumber: 1,
      manifestListPath: 's3://bucket/table/metadata/snap-1-manifest-list.avro',
      operation: 'append',
      formatVersion: 3,
      firstRowId: 0,
      addedRows: 100,
      keyId: 10,
    });
    const snapshot = builder.build();

    expect(snapshot['key-id']).toBe(10);
    // v3 fields should also be present
    expect(snapshot['first-row-id']).toBe(0);
    expect(snapshot['added-rows']).toBe(100);
  });
});

describe('Snapshot key-id: Validation Tests', () => {
  it('should allow key-id to be optional (no validation error when missing)', () => {
    // key-id is optional - snapshots without it are valid
    const snapshot: Snapshot = {
      'snapshot-id': 1234567890,
      'sequence-number': 1,
      'timestamp-ms': Date.now(),
      'manifest-list': 's3://bucket/table/metadata/snap-1-manifest-list.avro',
      summary: { operation: 'append' },
      'schema-id': 0,
    };

    // This should not throw - key-id is optional
    expect(snapshot['snapshot-id']).toBe(1234567890);
  });

  it('should accept valid key-id values', () => {
    // key-id should be a positive integer referencing an encryption key
    const snapshot: Snapshot = {
      'snapshot-id': 1234567890,
      'sequence-number': 1,
      'timestamp-ms': Date.now(),
      'manifest-list': 's3://bucket/table/metadata/snap-1-manifest-list.avro',
      summary: { operation: 'append' },
      'schema-id': 0,
      'key-id': 1,
    };

    expect(snapshot['key-id']).toBe(1);
  });
});

describe('Snapshot key-id: JSON Serialization Tests', () => {
  it('should serialize key-id in snapshot JSON when provided', () => {
    const snapshot: Snapshot = {
      'snapshot-id': 1234567890,
      'sequence-number': 1,
      'timestamp-ms': Date.now(),
      'manifest-list': 's3://bucket/table/metadata/snap-1234567890-1-manifest-list.avro',
      summary: { operation: 'append' },
      'schema-id': 0,
      'key-id': 42,
    };

    const json = JSON.stringify(snapshot);
    expect(json).toContain('"key-id"');
    expect(json).toContain('42');
  });

  it('should round-trip serialize key-id', () => {
    const original: Snapshot = {
      'snapshot-id': 1234567890,
      'sequence-number': 1,
      'timestamp-ms': Date.now(),
      'manifest-list': 's3://bucket/table/metadata/snap-1234567890-1-manifest-list.avro',
      summary: { operation: 'append' },
      'schema-id': 0,
      'key-id': 99,
    };

    const json = JSON.stringify(original);
    const parsed = JSON.parse(json) as Snapshot;

    expect(parsed['key-id']).toBe(99);
  });

  it('should not include key-id when not provided', () => {
    const snapshot: Snapshot = {
      'snapshot-id': 1234567890,
      'sequence-number': 1,
      'timestamp-ms': Date.now(),
      'manifest-list': 's3://bucket/table/metadata/snap-1234567890-1-manifest-list.avro',
      summary: { operation: 'append' },
      'schema-id': 0,
      // No key-id
    };

    const json = JSON.stringify(snapshot);
    expect(json).not.toContain('"key-id"');
  });

  it('should preserve all snapshot fields through JSON round-trip with key-id', () => {
    const original: Snapshot = {
      'snapshot-id': 1234567890,
      'parent-snapshot-id': 1234567889,
      'sequence-number': 5,
      'timestamp-ms': 1609459200000,
      'manifest-list': 's3://bucket/table/metadata/snap-1234567890-5-manifest-list.avro',
      summary: { operation: 'append', 'added-data-files': '10' },
      'schema-id': 1,
      'key-id': 7,
    };

    const json = JSON.stringify(original, null, 2);
    const parsed = JSON.parse(json) as Snapshot;

    expect(parsed['snapshot-id']).toBe(1234567890);
    expect(parsed['parent-snapshot-id']).toBe(1234567889);
    expect(parsed['sequence-number']).toBe(5);
    expect(parsed['timestamp-ms']).toBe(1609459200000);
    expect(parsed['manifest-list']).toBe('s3://bucket/table/metadata/snap-1234567890-5-manifest-list.avro');
    expect(parsed.summary.operation).toBe('append');
    expect(parsed.summary['added-data-files']).toBe('10');
    expect(parsed['schema-id']).toBe(1);
    expect(parsed['key-id']).toBe(7);
  });
});

describe('Snapshot key-id: SnapshotBuilder with key-id', () => {
  it('should build snapshot with all options including key-id', () => {
    const builder = new SnapshotBuilder({
      sequenceNumber: 3,
      snapshotId: 9999999999,
      parentSnapshotId: 8888888888,
      timestampMs: 1609459200000,
      operation: 'overwrite',
      manifestListPath: 's3://bucket/table/metadata/snap-9999999999-3-manifest-list.avro',
      schemaId: 2,
      keyId: 15,
    });

    builder.setSummary(5, 2, 500, 200, 1024, 512, 1000, 2048, 8);

    const snapshot = builder.build();

    expect(snapshot['snapshot-id']).toBe(9999999999);
    expect(snapshot['parent-snapshot-id']).toBe(8888888888);
    expect(snapshot['sequence-number']).toBe(3);
    expect(snapshot['timestamp-ms']).toBe(1609459200000);
    expect(snapshot['manifest-list']).toBe('s3://bucket/table/metadata/snap-9999999999-3-manifest-list.avro');
    expect(snapshot.summary.operation).toBe('overwrite');
    expect(snapshot['schema-id']).toBe(2);
    expect(snapshot['key-id']).toBe(15);
  });

  it('should allow key-id of 0', () => {
    // key-id of 0 might be valid (first encryption key)
    const builder = new SnapshotBuilder({
      sequenceNumber: 1,
      manifestListPath: 's3://bucket/table/metadata/snap-1-manifest-list.avro',
      operation: 'append',
      keyId: 0,
    });
    const snapshot = builder.build();

    expect(snapshot['key-id']).toBe(0);
  });
});
