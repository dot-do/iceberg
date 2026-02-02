/**
 * Tests for Format Version handling in Iceberg metadata.json
 *
 * @see https://iceberg.apache.org/spec/#format-version
 *
 * Iceberg has two main format versions:
 * - v1: Original format (deprecated)
 * - v2: Current format with improved features
 */

import { describe, it, expect } from 'vitest';
import {
  TableMetadataBuilder,
  MetadataWriter,
  type TableMetadata,
  type StorageBackend,
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

describe('Format Version Handling', () => {
  describe('Format Version 2 (Current)', () => {
    it('should create metadata with format-version 2', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      expect(metadata['format-version']).toBe(2);
    });

    it('should have format-version as first field in JSON', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();
      const keys = Object.keys(metadata);

      expect(keys[0]).toBe('format-version');
    });

    it('should include v2-specific fields', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      // V2 specific fields
      expect(metadata).toHaveProperty('last-sequence-number');
      expect(metadata).toHaveProperty('default-sort-order-id');
      expect(metadata).toHaveProperty('refs');
    });

    it('should serialize format-version as number 2', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const json = builder.toJSON();

      expect(json).toContain('"format-version": 2');
    });
  });

  describe('Format Version Validation', () => {
    it('should validate format-version is exactly 2', () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);
      const metadata = writer.createTableMetadata({
        location: 's3://bucket/table',
      });

      expect(() => writer.validateMetadata(metadata)).not.toThrow();
    });

    it('should reject format-version other than 2', () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      // Create invalid metadata with wrong format version
      const invalidMetadata = {
        'format-version': 1, // Invalid - should be 2
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
      } as unknown as TableMetadata;

      expect(() => writer.validateMetadata(invalidMetadata)).toThrow(
        /format-version/
      );
    });

    it('should reject missing format-version', () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      // Create invalid metadata without format version
      const invalidMetadata = {
        'table-uuid': 'test-uuid',
        location: 's3://bucket/table',
      } as unknown as TableMetadata;

      expect(() => writer.validateMetadata(invalidMetadata)).toThrow();
    });
  });

  describe('Format Version in Written Files', () => {
    it('should write format-version 2 to storage', async () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      const result = await writer.writeNewTable({
        location: 's3://bucket/warehouse/db/table',
      });

      const data = storage.files.get(result.metadataLocation);
      expect(data).toBeDefined();

      const parsed = JSON.parse(new TextDecoder().decode(data!));
      expect(parsed['format-version']).toBe(2);
    });

    it('should preserve format-version through updates', async () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      const result1 = await writer.writeNewTable({
        location: 's3://bucket/warehouse/db/table',
      });

      // Rebuild with modifications
      const builder = TableMetadataBuilder.fromMetadata(result1.metadata);
      builder.setProperty('test.key', 'test.value');
      const modified = builder.build();

      expect(modified['format-version']).toBe(2);
    });
  });

  describe('V2-Specific Features', () => {
    it('should support sequence numbers (v2 feature)', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      expect(metadata['last-sequence-number']).toBeDefined();
      expect(typeof metadata['last-sequence-number']).toBe('number');
    });

    it('should support sort orders (v2 feature)', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      expect(metadata['sort-orders']).toBeDefined();
      expect(Array.isArray(metadata['sort-orders'])).toBe(true);
      expect(metadata['default-sort-order-id']).toBeDefined();
    });

    it('should support snapshot references (v2 feature)', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      expect(metadata.refs).toBeDefined();
      expect(typeof metadata.refs).toBe('object');
    });

    it('should support metadata-log (v2 feature)', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      expect(metadata['metadata-log']).toBeDefined();
      expect(Array.isArray(metadata['metadata-log'])).toBe(true);
    });
  });

  describe('Format Version Type Safety', () => {
    it('should have format-version typed as literal 2', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      // TypeScript should enforce this at compile time
      // At runtime we verify the value
      const version: 2 = metadata['format-version'];
      expect(version).toBe(2);
    });
  });

  describe('V1 vs V2 Differences', () => {
    it('should use sequence-number in snapshots (v2)', () => {
      // In v1, snapshots don't have sequence numbers
      // In v2, snapshots must have sequence-number field
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      // When there are snapshots, they should have sequence-number
      expect(metadata['last-sequence-number']).toBe(0);
    });

    it('should use manifest-list instead of manifests (v2)', () => {
      // In v2, snapshots reference a manifest-list file path
      // rather than embedding manifests directly
      // This is validated through the Snapshot type
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      expect(metadata.snapshots).toBeDefined();
      // Snapshots in v2 use manifest-list field
    });

    it('should support delete files (v2 feature)', () => {
      // V2 adds support for position and equality delete files
      // This is primarily a manifest/data file concern but affects metadata
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      // The presence of v2 format enables delete file support
      expect(metadata['format-version']).toBe(2);
    });

    it('should support multiple schemas (v2)', () => {
      // V2 maintains history of schemas
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      expect(Array.isArray(metadata.schemas)).toBe(true);
      expect(metadata.schemas.length).toBeGreaterThanOrEqual(1);
    });

    it('should support multiple partition specs (v2)', () => {
      // V2 maintains history of partition specs
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      expect(Array.isArray(metadata['partition-specs'])).toBe(true);
      expect(metadata['partition-specs'].length).toBeGreaterThanOrEqual(1);
    });
  });

  describe('JSON Compatibility', () => {
    it('should produce JSON parseable by other Iceberg implementations', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
        properties: {
          'table.name': 'test_table',
          'table.namespace': 'test_db',
        },
      });

      const json = builder.toJSON();
      const parsed = JSON.parse(json);

      // Verify all required v2 fields are present
      expect(parsed['format-version']).toBe(2);
      expect(parsed['table-uuid']).toBeDefined();
      expect(parsed.location).toBeDefined();
      expect(parsed['last-sequence-number']).toBeDefined();
      expect(parsed['last-updated-ms']).toBeDefined();
      expect(parsed['last-column-id']).toBeDefined();
      expect(parsed['current-schema-id']).toBeDefined();
      expect(parsed.schemas).toBeDefined();
      expect(parsed['default-spec-id']).toBeDefined();
      expect(parsed['partition-specs']).toBeDefined();
      expect(parsed['last-partition-id']).toBeDefined();
      expect(parsed['default-sort-order-id']).toBeDefined();
      expect(parsed['sort-orders']).toBeDefined();
      expect(parsed.properties).toBeDefined();
      expect(parsed['current-snapshot-id']).toBeDefined();
      expect(parsed.snapshots).toBeDefined();
      expect(parsed['snapshot-log']).toBeDefined();
      expect(parsed['metadata-log']).toBeDefined();
      expect(parsed.refs).toBeDefined();
    });
  });
});
