/**
 * Tests for Iceberg v3 Row Lineage
 *
 * @see https://iceberg.apache.org/spec/#table-metadata
 * @see https://iceberg.apache.org/spec/#snapshots
 *
 * Iceberg v3 introduces row lineage tracking via:
 *
 * TableMetadata fields:
 * - next-row-id: A counter tracking the next row ID to be assigned
 *   - Required for v3 tables, optional for v2 tables
 *   - Must be non-negative
 *   - Initialized to 0 for new v3 tables
 *
 * Snapshot fields:
 * - first-row-id: The first row ID assigned to rows added by this snapshot
 *   - Required for v3 snapshots, optional for v2 snapshots
 *   - Equals the table's next-row-id at snapshot creation time
 *   - Must be non-negative
 * - added-rows: Total number of rows added by this snapshot
 *   - Required for v3 snapshots, optional for v2 snapshots
 *   - Must be non-negative
 */

import { describe, it, expect } from 'vitest';
import {
  TableMetadataBuilder,
  MetadataWriter,
  SnapshotBuilder,
  type TableMetadata,
  type StorageBackend,
  type Snapshot,
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

describe('Row Lineage: next-row-id in TableMetadata', () => {
  describe('Type Tests', () => {
    it('should allow next-row-id field in TableMetadata', () => {
      // TableMetadata should accept an optional next-row-id field
      const metadata: TableMetadata = {
        'format-version': 3,
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
        'next-row-id': 0,
      };

      expect(metadata['next-row-id']).toBe(0);
    });

    it('should allow next-row-id to be undefined for v2 tables', () => {
      // v2 metadata should not require next-row-id
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

      expect(metadata['next-row-id']).toBeUndefined();
    });

    it('should accept next-row-id as a number type', () => {
      const metadata: TableMetadata = {
        'format-version': 3,
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
        'next-row-id': 12345,
      };

      expect(typeof metadata['next-row-id']).toBe('number');
      expect(metadata['next-row-id']).toBe(12345);
    });
  });

  describe('TableMetadataBuilder Tests', () => {
    it('should initialize next-row-id to 0 for v3 tables by default', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
        formatVersion: 3,
      });
      const metadata = builder.build();

      expect(metadata['format-version']).toBe(3);
      expect(metadata['next-row-id']).toBe(0);
    });

    it('should not include next-row-id for v2 tables', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
        formatVersion: 2,
      });
      const metadata = builder.build();

      expect(metadata['format-version']).toBe(2);
      expect(metadata['next-row-id']).toBeUndefined();
    });

    it('should not include next-row-id for default format version (v2)', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      expect(metadata['format-version']).toBe(2);
      expect(metadata['next-row-id']).toBeUndefined();
    });

    it('should allow setting initial next-row-id value for v3 tables', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
        formatVersion: 3,
        nextRowId: 1000,
      });
      const metadata = builder.build();

      expect(metadata['next-row-id']).toBe(1000);
    });

    it('should preserve next-row-id when rebuilding from existing v3 metadata', () => {
      const originalBuilder = new TableMetadataBuilder({
        location: 's3://bucket/table',
        formatVersion: 3,
        nextRowId: 500,
      });
      const original = originalBuilder.build();

      const rebuiltBuilder = TableMetadataBuilder.fromMetadata(original);
      const rebuilt = rebuiltBuilder.build();

      expect(rebuilt['next-row-id']).toBe(500);
    });
  });

  describe('Validation Tests', () => {
    it('should fail validation for v3 metadata without next-row-id', () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      // Create invalid v3 metadata without next-row-id
      const invalidMetadata = {
        'format-version': 3,
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
        // Missing next-row-id which is required for v3
      } as unknown as TableMetadata;

      expect(() => writer.validateMetadata(invalidMetadata)).toThrow(/next-row-id/);
    });

    it('should pass validation for v2 metadata without next-row-id', () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      const v2Metadata = writer.createTableMetadata({
        location: 's3://bucket/table',
        formatVersion: 2,
      });

      expect(() => writer.validateMetadata(v2Metadata)).not.toThrow();
    });

    it('should fail validation for negative next-row-id', () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      // Create invalid metadata with negative next-row-id
      const invalidMetadata = {
        'format-version': 3,
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
        'next-row-id': -1, // Invalid: must be non-negative
      } as unknown as TableMetadata;

      expect(() => writer.validateMetadata(invalidMetadata)).toThrow(/next-row-id/);
    });

    it('should pass validation for v3 metadata with valid next-row-id', () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      const v3Metadata = writer.createTableMetadata({
        location: 's3://bucket/table',
        formatVersion: 3,
      });

      expect(() => writer.validateMetadata(v3Metadata)).not.toThrow();
      expect(v3Metadata['next-row-id']).toBeDefined();
      expect(v3Metadata['next-row-id']).toBeGreaterThanOrEqual(0);
    });
  });

  describe('JSON Serialization Tests', () => {
    it('should serialize next-row-id in v3 metadata JSON', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
        formatVersion: 3,
      });
      const json = builder.toJSON();

      expect(json).toContain('"next-row-id"');
      expect(json).toContain('"next-row-id": 0');
    });

    it('should not serialize next-row-id in v2 metadata JSON', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
        formatVersion: 2,
      });
      const json = builder.toJSON();

      expect(json).not.toContain('"next-row-id"');
    });

    it('should round-trip serialize next-row-id for v3 metadata', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
        formatVersion: 3,
        nextRowId: 9999,
      });
      const original = builder.build();
      const json = JSON.stringify(original);
      const parsed = JSON.parse(json) as TableMetadata;

      expect(parsed['next-row-id']).toBe(9999);
    });

    it('should preserve all metadata fields through JSON round-trip', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
        formatVersion: 3,
        nextRowId: 42,
        properties: { 'test.property': 'value' },
      });
      const original = builder.build();
      const json = JSON.stringify(original, null, 2);
      const parsed = JSON.parse(json) as TableMetadata;

      expect(parsed['format-version']).toBe(3);
      expect(parsed['next-row-id']).toBe(42);
      expect(parsed.location).toBe('s3://bucket/table');
      expect(parsed.properties['test.property']).toBe('value');
    });
  });

  describe('Storage Write Tests', () => {
    it('should write next-row-id to storage for v3 tables', async () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      const result = await writer.writeNewTable({
        location: 's3://bucket/warehouse/db/table',
        formatVersion: 3,
      });

      const data = storage.files.get(result.metadataLocation);
      expect(data).toBeDefined();

      const parsed = JSON.parse(new TextDecoder().decode(data!));
      expect(parsed['format-version']).toBe(3);
      expect(parsed['next-row-id']).toBe(0);
    });

    it('should not write next-row-id to storage for v2 tables', async () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      const result = await writer.writeNewTable({
        location: 's3://bucket/warehouse/db/table',
        formatVersion: 2,
      });

      const data = storage.files.get(result.metadataLocation);
      expect(data).toBeDefined();

      const parsed = JSON.parse(new TextDecoder().decode(data!));
      expect(parsed['format-version']).toBe(2);
      expect(parsed['next-row-id']).toBeUndefined();
    });

    it('should preserve next-row-id through updates for v3 tables', async () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      const result1 = await writer.writeNewTable({
        location: 's3://bucket/warehouse/db/table',
        formatVersion: 3,
      });

      // Rebuild with modifications
      const builder = TableMetadataBuilder.fromMetadata(result1.metadata);
      builder.setProperty('test.key', 'test.value');
      const modified = builder.build();

      expect(modified['format-version']).toBe(3);
      expect(modified['next-row-id']).toBe(0);
    });
  });

  describe('Upgrade Tests', () => {
    it('should initialize next-row-id to 0 when upgrading v2 to v3', () => {
      // Create v2 metadata
      const v2Builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
        formatVersion: 2,
      });
      const v2Metadata = v2Builder.build();
      expect(v2Metadata['next-row-id']).toBeUndefined();

      // Simulate upgrade by creating v3 metadata from v2 values
      // In a real upgrade, you would use an upgrade function
      const upgradedBuilder = new TableMetadataBuilder({
        location: v2Metadata.location,
        tableUuid: v2Metadata['table-uuid'],
        formatVersion: 3,
        // next-row-id should be initialized to 0 by default
      });
      const upgradedMetadata = upgradedBuilder.build();

      expect(upgradedMetadata['format-version']).toBe(3);
      expect(upgradedMetadata['next-row-id']).toBe(0);
    });
  });
});

describe('Row Lineage: Snapshot fields (first-row-id, added-rows)', () => {
  describe('Type Tests', () => {
    it('should allow first-row-id field in Snapshot (number)', () => {
      // Snapshot should accept an optional first-row-id field
      const snapshot: Snapshot = {
        'snapshot-id': 1234567890,
        'sequence-number': 1,
        'timestamp-ms': Date.now(),
        'manifest-list': 's3://bucket/table/metadata/snap-1234567890-1-manifest-list.avro',
        summary: { operation: 'append' },
        'schema-id': 0,
        'first-row-id': 0,
      };

      expect(snapshot['first-row-id']).toBe(0);
      expect(typeof snapshot['first-row-id']).toBe('number');
    });

    it('should allow added-rows field in Snapshot (number)', () => {
      // Snapshot should accept an optional added-rows field
      const snapshot: Snapshot = {
        'snapshot-id': 1234567890,
        'sequence-number': 1,
        'timestamp-ms': Date.now(),
        'manifest-list': 's3://bucket/table/metadata/snap-1234567890-1-manifest-list.avro',
        summary: { operation: 'append' },
        'schema-id': 0,
        'added-rows': 1000,
      };

      expect(snapshot['added-rows']).toBe(1000);
      expect(typeof snapshot['added-rows']).toBe('number');
    });

    it('should allow both first-row-id and added-rows in Snapshot', () => {
      const snapshot: Snapshot = {
        'snapshot-id': 1234567890,
        'sequence-number': 1,
        'timestamp-ms': Date.now(),
        'manifest-list': 's3://bucket/table/metadata/snap-1234567890-1-manifest-list.avro',
        summary: { operation: 'append' },
        'schema-id': 0,
        'first-row-id': 500,
        'added-rows': 250,
      };

      expect(snapshot['first-row-id']).toBe(500);
      expect(snapshot['added-rows']).toBe(250);
    });

    it('should allow these fields to be optional for v2 snapshots', () => {
      // v2 snapshots should not require first-row-id or added-rows
      const v2Snapshot: Snapshot = {
        'snapshot-id': 1234567890,
        'sequence-number': 1,
        'timestamp-ms': Date.now(),
        'manifest-list': 's3://bucket/table/metadata/snap-1234567890-1-manifest-list.avro',
        summary: { operation: 'append' },
        'schema-id': 0,
        // No first-row-id or added-rows - should be valid for v2
      };

      expect(v2Snapshot['first-row-id']).toBeUndefined();
      expect(v2Snapshot['added-rows']).toBeUndefined();
    });
  });

  describe('SnapshotBuilder Tests', () => {
    it('should include first-row-id in v3 snapshots', () => {
      const builder = new SnapshotBuilder({
        sequenceNumber: 1,
        manifestListPath: 's3://bucket/table/metadata/snap-1-manifest-list.avro',
        operation: 'append',
        formatVersion: 3,
        firstRowId: 0,
      });
      const snapshot = builder.build();

      expect(snapshot['first-row-id']).toBe(0);
    });

    it('should include added-rows in v3 snapshots', () => {
      const builder = new SnapshotBuilder({
        sequenceNumber: 1,
        manifestListPath: 's3://bucket/table/metadata/snap-1-manifest-list.avro',
        operation: 'append',
        formatVersion: 3,
        firstRowId: 0,
        addedRows: 1000,
      });
      const snapshot = builder.build();

      expect(snapshot['added-rows']).toBe(1000);
    });

    it('should set first-row-id equal to table next-row-id at snapshot creation', () => {
      // When creating a v3 snapshot, first-row-id should be the table's current next-row-id
      const tableNextRowId = 5000;
      const builder = new SnapshotBuilder({
        sequenceNumber: 1,
        manifestListPath: 's3://bucket/table/metadata/snap-1-manifest-list.avro',
        operation: 'append',
        formatVersion: 3,
        firstRowId: tableNextRowId,
        addedRows: 100,
      });
      const snapshot = builder.build();

      expect(snapshot['first-row-id']).toBe(tableNextRowId);
    });

    it('should not include first-row-id or added-rows for v2 snapshots', () => {
      const builder = new SnapshotBuilder({
        sequenceNumber: 1,
        manifestListPath: 's3://bucket/table/metadata/snap-1-manifest-list.avro',
        operation: 'append',
        // formatVersion defaults to 2 or not specified
      });
      const snapshot = builder.build();

      expect(snapshot['first-row-id']).toBeUndefined();
      expect(snapshot['added-rows']).toBeUndefined();
    });
  });

  describe('Validation Tests', () => {
    it('should allow v3 snapshot without first-row-id for backward compatibility', () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      // Create v3 metadata with a snapshot missing first-row-id
      // This is allowed for backward compatibility when upgrading from v2
      const metadata = {
        'format-version': 3,
        'table-uuid': 'test-uuid',
        location: 's3://bucket/table',
        'last-sequence-number': 1,
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
        'current-snapshot-id': 1234567890,
        snapshots: [
          {
            'snapshot-id': 1234567890,
            'sequence-number': 1,
            'timestamp-ms': Date.now(),
            'manifest-list': 's3://bucket/table/metadata/snap-1-manifest-list.avro',
            summary: { operation: 'append' },
            'schema-id': 0,
            // Missing first-row-id (allowed for backward compatibility)
            'added-rows': 100,
          },
        ],
        'snapshot-log': [],
        'metadata-log': [],
        refs: {},
        'next-row-id': 100,
      } as unknown as TableMetadata;

      // Should pass validation - missing fields are allowed for compatibility
      expect(() => writer.validateMetadata(metadata)).not.toThrow();
    });

    it('should allow v3 snapshot without added-rows for backward compatibility', () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      // Create v3 metadata with a snapshot missing added-rows
      // This is allowed for backward compatibility when upgrading from v2
      const metadata = {
        'format-version': 3,
        'table-uuid': 'test-uuid',
        location: 's3://bucket/table',
        'last-sequence-number': 1,
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
        'current-snapshot-id': 1234567890,
        snapshots: [
          {
            'snapshot-id': 1234567890,
            'sequence-number': 1,
            'timestamp-ms': Date.now(),
            'manifest-list': 's3://bucket/table/metadata/snap-1-manifest-list.avro',
            summary: { operation: 'append' },
            'schema-id': 0,
            'first-row-id': 0,
            // Missing added-rows (allowed for backward compatibility)
          },
        ],
        'snapshot-log': [],
        'metadata-log': [],
        refs: {},
        'next-row-id': 100,
      } as unknown as TableMetadata;

      // Should pass validation - missing fields are allowed for compatibility
      expect(() => writer.validateMetadata(metadata)).not.toThrow();
    });

    it('should pass validation for v2 snapshot without first-row-id and added-rows', () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      // Create v2 metadata with a snapshot without row lineage fields
      const v2Metadata = {
        'format-version': 2,
        'table-uuid': 'test-uuid',
        location: 's3://bucket/table',
        'last-sequence-number': 1,
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
        'current-snapshot-id': 1234567890,
        snapshots: [
          {
            'snapshot-id': 1234567890,
            'sequence-number': 1,
            'timestamp-ms': Date.now(),
            'manifest-list': 's3://bucket/table/metadata/snap-1-manifest-list.avro',
            summary: { operation: 'append' },
            'schema-id': 0,
            // No first-row-id or added-rows - valid for v2
          },
        ],
        'snapshot-log': [],
        'metadata-log': [],
        refs: {},
      } as TableMetadata;

      expect(() => writer.validateMetadata(v2Metadata)).not.toThrow();
    });

    it('should fail validation for negative first-row-id', () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      const invalidMetadata = {
        'format-version': 3,
        'table-uuid': 'test-uuid',
        location: 's3://bucket/table',
        'last-sequence-number': 1,
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
        'current-snapshot-id': 1234567890,
        snapshots: [
          {
            'snapshot-id': 1234567890,
            'sequence-number': 1,
            'timestamp-ms': Date.now(),
            'manifest-list': 's3://bucket/table/metadata/snap-1-manifest-list.avro',
            summary: { operation: 'append' },
            'schema-id': 0,
            'first-row-id': -1, // Invalid: must be non-negative
            'added-rows': 100,
          },
        ],
        'snapshot-log': [],
        'metadata-log': [],
        refs: {},
        'next-row-id': 100,
      } as unknown as TableMetadata;

      expect(() => writer.validateMetadata(invalidMetadata)).toThrow(/first-row-id/);
    });

    it('should fail validation for negative added-rows', () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      const invalidMetadata = {
        'format-version': 3,
        'table-uuid': 'test-uuid',
        location: 's3://bucket/table',
        'last-sequence-number': 1,
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
        'current-snapshot-id': 1234567890,
        snapshots: [
          {
            'snapshot-id': 1234567890,
            'sequence-number': 1,
            'timestamp-ms': Date.now(),
            'manifest-list': 's3://bucket/table/metadata/snap-1-manifest-list.avro',
            summary: { operation: 'append' },
            'schema-id': 0,
            'first-row-id': 0,
            'added-rows': -5, // Invalid: must be non-negative
          },
        ],
        'snapshot-log': [],
        'metadata-log': [],
        refs: {},
        'next-row-id': 100,
      } as unknown as TableMetadata;

      expect(() => writer.validateMetadata(invalidMetadata)).toThrow(/added-rows/);
    });

    it('should pass validation for v3 snapshot with valid first-row-id and added-rows', () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      const validMetadata = {
        'format-version': 3,
        'table-uuid': 'test-uuid',
        location: 's3://bucket/table',
        'last-sequence-number': 1,
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
        'current-snapshot-id': 1234567890,
        snapshots: [
          {
            'snapshot-id': 1234567890,
            'sequence-number': 1,
            'timestamp-ms': Date.now(),
            'manifest-list': 's3://bucket/table/metadata/snap-1-manifest-list.avro',
            summary: { operation: 'append' },
            'schema-id': 0,
            'first-row-id': 0,
            'added-rows': 100,
          },
        ],
        'snapshot-log': [],
        'metadata-log': [],
        refs: {},
        'next-row-id': 100,
      } as TableMetadata;

      expect(() => writer.validateMetadata(validMetadata)).not.toThrow();
    });
  });

  describe('JSON Serialization Tests', () => {
    it('should serialize first-row-id in v3 snapshot JSON', () => {
      const snapshot: Snapshot = {
        'snapshot-id': 1234567890,
        'sequence-number': 1,
        'timestamp-ms': Date.now(),
        'manifest-list': 's3://bucket/table/metadata/snap-1-manifest-list.avro',
        summary: { operation: 'append' },
        'schema-id': 0,
        'first-row-id': 500,
        'added-rows': 100,
      };

      const json = JSON.stringify(snapshot);
      expect(json).toContain('"first-row-id"');
      expect(json).toContain('500');
    });

    it('should serialize added-rows in v3 snapshot JSON', () => {
      const snapshot: Snapshot = {
        'snapshot-id': 1234567890,
        'sequence-number': 1,
        'timestamp-ms': Date.now(),
        'manifest-list': 's3://bucket/table/metadata/snap-1-manifest-list.avro',
        summary: { operation: 'append' },
        'schema-id': 0,
        'first-row-id': 0,
        'added-rows': 250,
      };

      const json = JSON.stringify(snapshot);
      expect(json).toContain('"added-rows"');
      expect(json).toContain('250');
    });

    it('should round-trip serialize snapshot with row lineage fields', () => {
      const original: Snapshot = {
        'snapshot-id': 1234567890,
        'sequence-number': 1,
        'timestamp-ms': Date.now(),
        'manifest-list': 's3://bucket/table/metadata/snap-1-manifest-list.avro',
        summary: { operation: 'append' },
        'schema-id': 0,
        'first-row-id': 1000,
        'added-rows': 500,
      };

      const json = JSON.stringify(original);
      const parsed = JSON.parse(json) as Snapshot;

      expect(parsed['first-row-id']).toBe(1000);
      expect(parsed['added-rows']).toBe(500);
    });
  });
});

// ============================================================================
// Row Lineage: first-row-id in DataFile (v3)
// ============================================================================

import type { DataFile } from '../../../src/index.js';
import { calculateRowId } from '../../../src/metadata/types.js';

describe('Row Lineage: first-row-id in DataFile', () => {
  describe('Type Tests', () => {
    it('should allow first-row-id field in DataFile (number)', () => {
      // DataFile should accept an optional first-row-id field
      const dataFile: DataFile = {
        content: 0,
        'file-path': 's3://bucket/data/file.parquet',
        'file-format': 'parquet',
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 4096,
        'first-row-id': 0,
      };

      expect(dataFile['first-row-id']).toBe(0);
      expect(typeof dataFile['first-row-id']).toBe('number');
    });

    it('should allow first-row-id to be null (inherits from manifest)', () => {
      // first-row-id can be null to indicate inheritance from manifest context
      const dataFile: DataFile = {
        content: 0,
        'file-path': 's3://bucket/data/file.parquet',
        'file-format': 'parquet',
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 4096,
        'first-row-id': null,
      };

      expect(dataFile['first-row-id']).toBeNull();
    });

    it('should allow first-row-id to be undefined (v2 compatibility)', () => {
      // first-row-id is optional for v2 compatibility
      const dataFile: DataFile = {
        content: 0,
        'file-path': 's3://bucket/data/file.parquet',
        'file-format': 'parquet',
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 4096,
      };

      expect(dataFile['first-row-id']).toBeUndefined();
    });
  });

  describe('Data File first-row-id Semantics', () => {
    it('should allow new data files to have first-row-id set to null (inherits)', () => {
      // ADDED files can use null to inherit first-row-id from manifest context
      const dataFile: DataFile = {
        content: 0,
        'file-path': 's3://bucket/data/new-file.parquet',
        'file-format': 'parquet',
        partition: {},
        'record-count': 500,
        'file-size-in-bytes': 2048,
        'first-row-id': null, // Inherits from manifest's first-row-id + cumulative
      };

      expect(dataFile['first-row-id']).toBeNull();
    });

    it('should allow existing data files to have non-null first-row-id', () => {
      // EXISTING files have explicit first-row-id values
      const dataFile: DataFile = {
        content: 0,
        'file-path': 's3://bucket/data/existing-file.parquet',
        'file-format': 'parquet',
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 4096,
        'first-row-id': 5000, // Explicit first row ID
      };

      expect(dataFile['first-row-id']).toBe(5000);
    });

    it('should support large first-row-id values', () => {
      // first-row-id should support large numbers (long in Iceberg spec)
      const dataFile: DataFile = {
        content: 0,
        'file-path': 's3://bucket/data/file.parquet',
        'file-format': 'parquet',
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 4096,
        'first-row-id': 9007199254740991, // Number.MAX_SAFE_INTEGER
      };

      expect(dataFile['first-row-id']).toBe(9007199254740991);
    });
  });

  describe('Inheritance Tests', () => {
    it('should use null first-row-id for ADDED files (status=1)', () => {
      // ADDED files use null first-row-id, inheriting from manifest context
      const addedFile: DataFile = {
        content: 0,
        'file-path': 's3://bucket/data/added.parquet',
        'file-format': 'parquet',
        partition: {},
        'record-count': 100,
        'file-size-in-bytes': 1024,
        'first-row-id': null,
      };

      expect(addedFile['first-row-id']).toBeNull();
    });

    it('should use explicit first-row-id for EXISTING files (status=0)', () => {
      // EXISTING files must have explicit first-row-id
      const existingFile: DataFile = {
        content: 0,
        'file-path': 's3://bucket/data/existing.parquet',
        'file-format': 'parquet',
        partition: {},
        'record-count': 100,
        'file-size-in-bytes': 1024,
        'first-row-id': 1000, // Must be explicit for EXISTING files
      };

      expect(existingFile['first-row-id']).toBe(1000);
      expect(typeof existingFile['first-row-id']).toBe('number');
    });
  });

  describe('Row ID Calculation Tests', () => {
    it('should calculate _row_id as first_row_id + _pos', () => {
      // _row_id = first_row_id + _pos for each row in the file
      const firstRowId = 5000;
      const rowPosition = 42;

      const rowId = calculateRowId(firstRowId, rowPosition);

      expect(rowId).toBe(5042);
    });

    it('should return null when first_row_id is null', () => {
      // Cannot calculate row ID when first_row_id is null
      const rowId = calculateRowId(null, 10);

      expect(rowId).toBeNull();
    });

    it('should return null when first_row_id is undefined', () => {
      // Cannot calculate row ID when first_row_id is undefined
      const rowId = calculateRowId(undefined, 10);

      expect(rowId).toBeNull();
    });

    it('should handle zero-based row positions', () => {
      const firstRowId = 100;
      const rowPosition = 0;

      const rowId = calculateRowId(firstRowId, rowPosition);

      expect(rowId).toBe(100);
    });

    it('should handle large row positions', () => {
      const firstRowId = 1000000;
      const rowPosition = 999999;

      const rowId = calculateRowId(firstRowId, rowPosition);

      expect(rowId).toBe(1999999);
    });
  });

  describe('JSON Serialization Tests', () => {
    it('should serialize first-row-id in DataFile JSON', () => {
      const dataFile: DataFile = {
        content: 0,
        'file-path': 's3://bucket/data/file.parquet',
        'file-format': 'parquet',
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 4096,
        'first-row-id': 500,
      };

      const json = JSON.stringify(dataFile);

      expect(json).toContain('"first-row-id"');
      expect(json).toContain('500');
    });

    it('should serialize null first-row-id in DataFile JSON', () => {
      const dataFile: DataFile = {
        content: 0,
        'file-path': 's3://bucket/data/file.parquet',
        'file-format': 'parquet',
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 4096,
        'first-row-id': null,
      };

      const json = JSON.stringify(dataFile);

      expect(json).toContain('"first-row-id":null');
    });

    it('should round-trip serialize first-row-id', () => {
      const original: DataFile = {
        content: 0,
        'file-path': 's3://bucket/data/file.parquet',
        'file-format': 'parquet',
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 4096,
        'first-row-id': 12345,
      };

      const json = JSON.stringify(original);
      const parsed = JSON.parse(json) as DataFile;

      expect(parsed['first-row-id']).toBe(12345);
    });

    it('should round-trip serialize null first-row-id', () => {
      const original: DataFile = {
        content: 0,
        'file-path': 's3://bucket/data/file.parquet',
        'file-format': 'parquet',
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 4096,
        'first-row-id': null,
      };

      const json = JSON.stringify(original);
      const parsed = JSON.parse(json) as DataFile;

      expect(parsed['first-row-id']).toBeNull();
    });

    it('should not include first-row-id when undefined (v2 compatibility)', () => {
      const dataFile: DataFile = {
        content: 0,
        'file-path': 's3://bucket/data/file.parquet',
        'file-format': 'parquet',
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 4096,
      };

      const json = JSON.stringify(dataFile);

      expect(json).not.toContain('"first-row-id"');
    });
  });
});

// ============================================================================
// Row Lineage: first-row-id in ManifestFile (v3)
// ============================================================================

import type { ManifestFile } from '../../../src/index.js';
import { ManifestListGenerator } from '../../../src/index.js';

describe('Row Lineage: first-row-id in ManifestFile', () => {
  describe('Type Tests', () => {
    it('should allow first-row-id field in ManifestFile', () => {
      // ManifestFile should accept an optional first-row-id field
      const manifest: ManifestFile = {
        'manifest-path': 's3://bucket/table/metadata/snap-123-manifest.avro',
        'manifest-length': 4096,
        'partition-spec-id': 0,
        content: 0,
        'sequence-number': 1,
        'min-sequence-number': 1,
        'added-snapshot-id': 1234567890,
        'added-files-count': 10,
        'existing-files-count': 0,
        'deleted-files-count': 0,
        'added-rows-count': 1000,
        'existing-rows-count': 0,
        'deleted-rows-count': 0,
        'first-row-id': 0,
      };

      expect(manifest['first-row-id']).toBe(0);
    });

    it('should allow first-row-id to be undefined (for v2 tables or inheritance)', () => {
      // first-row-id is optional - undefined means inherit from manifest list context
      const manifest: ManifestFile = {
        'manifest-path': 's3://bucket/table/metadata/snap-123-manifest.avro',
        'manifest-length': 4096,
        'partition-spec-id': 0,
        content: 0,
        'sequence-number': 1,
        'min-sequence-number': 1,
        'added-snapshot-id': 1234567890,
        'added-files-count': 10,
        'existing-files-count': 0,
        'deleted-files-count': 0,
        'added-rows-count': 1000,
        'existing-rows-count': 0,
        'deleted-rows-count': 0,
      };

      expect(manifest['first-row-id']).toBeUndefined();
    });

    it('should allow first-row-id to be null for explicit inheritance', () => {
      // null explicitly indicates inheritance from manifest list context
      const manifest: ManifestFile = {
        'manifest-path': 's3://bucket/table/metadata/snap-123-manifest.avro',
        'manifest-length': 4096,
        'partition-spec-id': 0,
        content: 0,
        'sequence-number': 1,
        'min-sequence-number': 1,
        'added-snapshot-id': 1234567890,
        'added-files-count': 10,
        'existing-files-count': 0,
        'deleted-files-count': 0,
        'added-rows-count': 1000,
        'existing-rows-count': 0,
        'deleted-rows-count': 0,
        'first-row-id': null,
      };

      expect(manifest['first-row-id']).toBeNull();
    });

    it('should accept first-row-id as a number type', () => {
      const manifest: ManifestFile = {
        'manifest-path': 's3://bucket/table/metadata/snap-123-manifest.avro',
        'manifest-length': 4096,
        'partition-spec-id': 0,
        content: 0,
        'sequence-number': 1,
        'min-sequence-number': 1,
        'added-snapshot-id': 1234567890,
        'added-files-count': 10,
        'existing-files-count': 0,
        'deleted-files-count': 0,
        'added-rows-count': 1000,
        'existing-rows-count': 0,
        'deleted-rows-count': 0,
        'first-row-id': 5000,
      };

      expect(typeof manifest['first-row-id']).toBe('number');
      expect(manifest['first-row-id']).toBe(5000);
    });
  });

  describe('ManifestListGenerator Tests', () => {
    it('should allow adding manifest with first-row-id', () => {
      const generator = new ManifestListGenerator({
        snapshotId: Date.now(),
        sequenceNumber: 1,
      });

      generator.addManifestWithStats(
        's3://bucket/table/metadata/snap-123-manifest.avro',
        4096,
        0,
        {
          addedFiles: 10,
          existingFiles: 0,
          deletedFiles: 0,
          addedRows: 1000,
          existingRows: 0,
          deletedRows: 0,
        },
        false, // isDeleteManifest
        undefined, // partitionSummaries
        0 // firstRowId
      );

      const manifests = generator.getManifests();
      expect(manifests).toHaveLength(1);
      expect(manifests[0]['first-row-id']).toBe(0);
    });

    it('should preserve existing manifest first-row-id values', () => {
      const generator = new ManifestListGenerator({
        snapshotId: Date.now(),
        sequenceNumber: 2,
      });

      // Add first manifest with first-row-id = 0
      generator.addManifestWithStats(
        's3://bucket/table/metadata/snap-1-manifest.avro',
        4096,
        0,
        {
          addedFiles: 10,
          existingFiles: 0,
          deletedFiles: 0,
          addedRows: 1000,
          existingRows: 0,
          deletedRows: 0,
        },
        false,
        undefined,
        0
      );

      // Add second manifest with first-row-id = 1000 (continuing from first)
      generator.addManifestWithStats(
        's3://bucket/table/metadata/snap-2-manifest.avro',
        4096,
        0,
        {
          addedFiles: 5,
          existingFiles: 0,
          deletedFiles: 0,
          addedRows: 500,
          existingRows: 0,
          deletedRows: 0,
        },
        false,
        undefined,
        1000
      );

      const manifests = generator.getManifests();
      expect(manifests).toHaveLength(2);
      expect(manifests[0]['first-row-id']).toBe(0);
      expect(manifests[1]['first-row-id']).toBe(1000);
    });

    it('should support null first-row-id for inheritance', () => {
      const generator = new ManifestListGenerator({
        snapshotId: Date.now(),
        sequenceNumber: 1,
      });

      generator.addManifestWithStats(
        's3://bucket/table/metadata/snap-123-manifest.avro',
        4096,
        0,
        {
          addedFiles: 10,
          existingFiles: 0,
          deletedFiles: 0,
          addedRows: 1000,
          existingRows: 0,
          deletedRows: 0,
        },
        false,
        undefined,
        null // null for inheritance
      );

      const manifests = generator.getManifests();
      expect(manifests).toHaveLength(1);
      expect(manifests[0]['first-row-id']).toBeNull();
    });

    it('should not include first-row-id when not provided (v2 compatibility)', () => {
      const generator = new ManifestListGenerator({
        snapshotId: Date.now(),
        sequenceNumber: 1,
      });

      generator.addManifestWithStats(
        's3://bucket/table/metadata/snap-123-manifest.avro',
        4096,
        0,
        {
          addedFiles: 10,
          existingFiles: 0,
          deletedFiles: 0,
          addedRows: 1000,
          existingRows: 0,
          deletedRows: 0,
        },
        false,
        undefined
        // No firstRowId parameter - v2 compatibility
      );

      const manifests = generator.getManifests();
      expect(manifests).toHaveLength(1);
      expect(manifests[0]['first-row-id']).toBeUndefined();
    });
  });

  describe('JSON Serialization Tests', () => {
    it('should serialize first-row-id in manifest list JSON', () => {
      const generator = new ManifestListGenerator({
        snapshotId: Date.now(),
        sequenceNumber: 1,
      });

      generator.addManifestWithStats(
        's3://bucket/table/metadata/snap-123-manifest.avro',
        4096,
        0,
        {
          addedFiles: 10,
          existingFiles: 0,
          deletedFiles: 0,
          addedRows: 1000,
          existingRows: 0,
          deletedRows: 0,
        },
        false,
        undefined,
        0
      );

      const json = generator.toJSON();
      expect(json).toContain('"first-row-id"');
      expect(json).toContain('"first-row-id": 0');
    });

    it('should round-trip serialize first-row-id', () => {
      const manifest: ManifestFile = {
        'manifest-path': 's3://bucket/table/metadata/snap-123-manifest.avro',
        'manifest-length': 4096,
        'partition-spec-id': 0,
        content: 0,
        'sequence-number': 1,
        'min-sequence-number': 1,
        'added-snapshot-id': 1234567890,
        'added-files-count': 10,
        'existing-files-count': 0,
        'deleted-files-count': 0,
        'added-rows-count': 1000,
        'existing-rows-count': 0,
        'deleted-rows-count': 0,
        'first-row-id': 9999,
      };

      const json = JSON.stringify(manifest);
      const parsed = JSON.parse(json) as ManifestFile;

      expect(parsed['first-row-id']).toBe(9999);
    });

    it('should preserve all manifest fields through JSON round-trip', () => {
      const manifest: ManifestFile = {
        'manifest-path': 's3://bucket/table/metadata/snap-123-manifest.avro',
        'manifest-length': 4096,
        'partition-spec-id': 0,
        content: 0,
        'sequence-number': 1,
        'min-sequence-number': 1,
        'added-snapshot-id': 1234567890,
        'added-files-count': 10,
        'existing-files-count': 5,
        'deleted-files-count': 2,
        'added-rows-count': 1000,
        'existing-rows-count': 500,
        'deleted-rows-count': 200,
        'first-row-id': 42,
        partitions: [
          {
            'contains-null': false,
            'contains-nan': false,
          },
        ],
      };

      const json = JSON.stringify(manifest, null, 2);
      const parsed = JSON.parse(json) as ManifestFile;

      expect(parsed['manifest-path']).toBe(manifest['manifest-path']);
      expect(parsed['manifest-length']).toBe(manifest['manifest-length']);
      expect(parsed['first-row-id']).toBe(42);
      expect(parsed['added-rows-count']).toBe(1000);
      expect(parsed.partitions).toHaveLength(1);
    });
  });

  describe('Inheritance Tests', () => {
    it('should interpret null first-row-id as inheriting from context', () => {
      // When first-row-id is null, it means the manifest inherits its
      // first-row-id from the manifest list context (based on cumulative row counts)
      const manifest: ManifestFile = {
        'manifest-path': 's3://bucket/table/metadata/snap-123-manifest.avro',
        'manifest-length': 4096,
        'partition-spec-id': 0,
        content: 0,
        'sequence-number': 1,
        'min-sequence-number': 1,
        'added-snapshot-id': 1234567890,
        'added-files-count': 10,
        'existing-files-count': 0,
        'deleted-files-count': 0,
        'added-rows-count': 1000,
        'existing-rows-count': 0,
        'deleted-rows-count': 0,
        'first-row-id': null,
      };

      // null indicates inheritance
      expect(manifest['first-row-id']).toBeNull();
      // This is distinct from undefined (which means field not present)
      expect(manifest['first-row-id']).not.toBeUndefined();
    });

    it('should use explicit first-row-id when provided (no inheritance)', () => {
      const manifest: ManifestFile = {
        'manifest-path': 's3://bucket/table/metadata/snap-123-manifest.avro',
        'manifest-length': 4096,
        'partition-spec-id': 0,
        content: 0,
        'sequence-number': 1,
        'min-sequence-number': 1,
        'added-snapshot-id': 1234567890,
        'added-files-count': 10,
        'existing-files-count': 0,
        'deleted-files-count': 0,
        'added-rows-count': 1000,
        'existing-rows-count': 0,
        'deleted-rows-count': 0,
        'first-row-id': 5000, // Explicit value - no inheritance
      };

      // When first-row-id is a number, use it directly
      expect(manifest['first-row-id']).toBe(5000);
      expect(typeof manifest['first-row-id']).toBe('number');
    });
  });
});
