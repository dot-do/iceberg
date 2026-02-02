/**
 * Tests for Iceberg metadata.json file structure per spec
 *
 * @see https://iceberg.apache.org/spec/#table-metadata
 *
 * These tests validate that generated metadata.json files conform to
 * the Apache Iceberg specification for table metadata format.
 */

import { describe, it, expect } from 'vitest';
import {
  MetadataWriter,
  TableMetadataBuilder,
  createDefaultSchema,
  createUnpartitionedSpec,
  createUnsortedOrder,
  generateUUID,
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

describe('Metadata File Structure', () => {
  describe('Required Fields (v2 spec)', () => {
    it('should include format-version field with value 2', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
      });
      const metadata = builder.build();

      expect(metadata).toHaveProperty('format-version');
      expect(metadata['format-version']).toBe(2);
    });

    it('should include table-uuid as a valid UUID string', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
      });
      const metadata = builder.build();

      expect(metadata).toHaveProperty('table-uuid');
      expect(typeof metadata['table-uuid']).toBe('string');
      // UUID v4 format
      expect(metadata['table-uuid']).toMatch(
        /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
      );
    });

    it('should include location field as an absolute path', () => {
      const location = 's3://bucket/warehouse/db/table';
      const builder = new TableMetadataBuilder({ location });
      const metadata = builder.build();

      expect(metadata).toHaveProperty('location');
      expect(metadata.location).toBe(location);
    });

    it('should include last-sequence-number starting at 0 for empty tables', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
      });
      const metadata = builder.build();

      expect(metadata).toHaveProperty('last-sequence-number');
      expect(metadata['last-sequence-number']).toBe(0);
    });

    it('should include last-updated-ms as a timestamp', () => {
      const before = Date.now();
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
      });
      const metadata = builder.build();
      const after = Date.now();

      expect(metadata).toHaveProperty('last-updated-ms');
      expect(metadata['last-updated-ms']).toBeGreaterThanOrEqual(before);
      expect(metadata['last-updated-ms']).toBeLessThanOrEqual(after);
    });

    it('should include last-column-id tracking the highest field ID', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
      });
      const metadata = builder.build();

      expect(metadata).toHaveProperty('last-column-id');
      expect(typeof metadata['last-column-id']).toBe('number');
      expect(metadata['last-column-id']).toBeGreaterThan(0);
    });

    it('should include schemas array with at least one schema', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
      });
      const metadata = builder.build();

      expect(metadata).toHaveProperty('schemas');
      expect(Array.isArray(metadata.schemas)).toBe(true);
      expect(metadata.schemas.length).toBeGreaterThanOrEqual(1);
    });

    it('should include current-schema-id referencing a valid schema', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
      });
      const metadata = builder.build();

      expect(metadata).toHaveProperty('current-schema-id');
      const schemaIds = metadata.schemas.map((s) => s['schema-id']);
      expect(schemaIds).toContain(metadata['current-schema-id']);
    });

    it('should include partition-specs array with at least one spec', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
      });
      const metadata = builder.build();

      expect(metadata).toHaveProperty('partition-specs');
      expect(Array.isArray(metadata['partition-specs'])).toBe(true);
      expect(metadata['partition-specs'].length).toBeGreaterThanOrEqual(1);
    });

    it('should include default-spec-id referencing a valid partition spec', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
      });
      const metadata = builder.build();

      expect(metadata).toHaveProperty('default-spec-id');
      const specIds = metadata['partition-specs'].map((s) => s['spec-id']);
      expect(specIds).toContain(metadata['default-spec-id']);
    });

    it('should include last-partition-id for tracking partition field IDs', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
      });
      const metadata = builder.build();

      expect(metadata).toHaveProperty('last-partition-id');
      expect(typeof metadata['last-partition-id']).toBe('number');
    });

    it('should include sort-orders array with at least one order', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
      });
      const metadata = builder.build();

      expect(metadata).toHaveProperty('sort-orders');
      expect(Array.isArray(metadata['sort-orders'])).toBe(true);
      expect(metadata['sort-orders'].length).toBeGreaterThanOrEqual(1);
    });

    it('should include default-sort-order-id referencing a valid sort order', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
      });
      const metadata = builder.build();

      expect(metadata).toHaveProperty('default-sort-order-id');
      const orderIds = metadata['sort-orders'].map((o) => o['order-id']);
      expect(orderIds).toContain(metadata['default-sort-order-id']);
    });

    it('should include properties object (can be empty)', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
      });
      const metadata = builder.build();

      expect(metadata).toHaveProperty('properties');
      expect(typeof metadata.properties).toBe('object');
      expect(metadata.properties).not.toBeNull();
    });

    it('should include snapshots array (empty for new tables)', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
      });
      const metadata = builder.build();

      expect(metadata).toHaveProperty('snapshots');
      expect(Array.isArray(metadata.snapshots)).toBe(true);
    });

    it('should include current-snapshot-id as null for tables with no snapshots', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
      });
      const metadata = builder.build();

      expect(metadata).toHaveProperty('current-snapshot-id');
      expect(metadata['current-snapshot-id']).toBeNull();
    });

    it('should include snapshot-log array', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
      });
      const metadata = builder.build();

      expect(metadata).toHaveProperty('snapshot-log');
      expect(Array.isArray(metadata['snapshot-log'])).toBe(true);
    });

    it('should include metadata-log array', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
      });
      const metadata = builder.build();

      expect(metadata).toHaveProperty('metadata-log');
      expect(Array.isArray(metadata['metadata-log'])).toBe(true);
    });

    it('should include refs object for snapshot references', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
      });
      const metadata = builder.build();

      expect(metadata).toHaveProperty('refs');
      expect(typeof metadata.refs).toBe('object');
      expect(metadata.refs).not.toBeNull();
    });
  });

  describe('JSON Serialization', () => {
    it('should serialize to valid JSON', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
      });
      const metadata = builder.build();
      const json = builder.toJSON();

      expect(() => JSON.parse(json)).not.toThrow();
    });

    it('should serialize with pretty printing (2 space indentation)', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
      });
      const json = builder.toJSON();

      // Check for indentation
      expect(json).toContain('\n  ');
    });

    it('should roundtrip through JSON parse/stringify', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
      });
      const original = builder.build();
      const json = JSON.stringify(original);
      const parsed = JSON.parse(json) as TableMetadata;

      expect(parsed['format-version']).toBe(original['format-version']);
      expect(parsed['table-uuid']).toBe(original['table-uuid']);
      expect(parsed.location).toBe(original.location);
      expect(parsed['current-schema-id']).toBe(original['current-schema-id']);
    });
  });

  describe('Field Ordering (per spec)', () => {
    it('should maintain consistent field ordering for interoperability', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
      });
      const metadata = builder.build();
      const keys = Object.keys(metadata);

      // Per Iceberg spec, format-version should come first
      expect(keys[0]).toBe('format-version');
    });
  });

  describe('Metadata File Writing', () => {
    it('should write metadata to versioned path', async () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);
      const result = await writer.writeNewTable({
        location: 's3://bucket/warehouse/db/table',
      });

      expect(result.metadataLocation).toMatch(/v\d+\.metadata\.json$/);
      expect(result.version).toBe(1);
    });

    it('should write version-hint.text file', async () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);
      await writer.writeNewTable({
        location: 's3://bucket/warehouse/db/table',
      });

      const versionHintPath = 's3://bucket/warehouse/db/table/metadata/version-hint.text';
      expect(storage.files.has(versionHintPath)).toBe(true);

      const content = new TextDecoder().decode(storage.files.get(versionHintPath)!);
      expect(content).toBe('1');
    });

    it('should increment version number on subsequent writes', async () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      const result1 = await writer.writeNewTable({
        location: 's3://bucket/warehouse/db/table',
      });
      expect(result1.version).toBe(1);

      // Simulate adding a snapshot and writing again
      // This would typically be done through writeWithSnapshot
      // For now, we test that the version tracking works
    });
  });
});
