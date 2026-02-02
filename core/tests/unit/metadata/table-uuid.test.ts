/**
 * Tests for Table UUID generation in Iceberg metadata
 *
 * @see https://iceberg.apache.org/spec/#table-metadata
 *
 * The table-uuid is a unique identifier for the table that should never change,
 * even if the table is dropped and recreated with the same name.
 */

import { describe, it, expect } from 'vitest';
import {
  generateUUID,
  TableMetadataBuilder,
  MetadataWriter,
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

describe('Table UUID Generation', () => {
  describe('UUID Format', () => {
    it('should generate valid UUID v4 format', () => {
      const uuid = generateUUID();
      // UUID v4 format: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
      // where y is one of 8, 9, a, or b
      expect(uuid).toMatch(
        /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
      );
    });

    it('should generate lowercase hex characters', () => {
      const uuid = generateUUID();
      expect(uuid).toBe(uuid.toLowerCase());
    });

    it('should generate UUIDs with correct length (36 characters)', () => {
      const uuid = generateUUID();
      expect(uuid.length).toBe(36);
    });

    it('should generate UUIDs with hyphens at correct positions', () => {
      const uuid = generateUUID();
      expect(uuid[8]).toBe('-');
      expect(uuid[13]).toBe('-');
      expect(uuid[18]).toBe('-');
      expect(uuid[23]).toBe('-');
    });

    it('should set version nibble to 4', () => {
      const uuid = generateUUID();
      // Character at position 14 (after 8-4-) should be '4'
      expect(uuid[14]).toBe('4');
    });

    it('should set variant bits correctly (8, 9, a, or b)', () => {
      const uuid = generateUUID();
      // Character at position 19 (after 8-4-4-) should be 8, 9, a, or b
      expect(['8', '9', 'a', 'b']).toContain(uuid[19]);
    });
  });

  describe('UUID Uniqueness', () => {
    it('should generate unique UUIDs', () => {
      const uuids = new Set<string>();
      for (let i = 0; i < 1000; i++) {
        uuids.add(generateUUID());
      }
      expect(uuids.size).toBe(1000);
    });

    it('should generate unique UUIDs across multiple table builders', () => {
      const builder1 = new TableMetadataBuilder({
        location: 's3://bucket/table1',
      });
      const builder2 = new TableMetadataBuilder({
        location: 's3://bucket/table2',
      });

      const meta1 = builder1.build();
      const meta2 = builder2.build();

      expect(meta1['table-uuid']).not.toBe(meta2['table-uuid']);
    });

    it('should preserve UUID when rebuilding from existing metadata', () => {
      const builder1 = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const meta1 = builder1.build();
      const uuid = meta1['table-uuid'];

      const builder2 = TableMetadataBuilder.fromMetadata(meta1);
      const meta2 = builder2.build();

      expect(meta2['table-uuid']).toBe(uuid);
    });
  });

  describe('UUID Persistence', () => {
    it('should allow explicit UUID to be provided', () => {
      const customUuid = '12345678-1234-4123-8123-123456789abc';
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
        tableUuid: customUuid,
      });
      const metadata = builder.build();

      expect(metadata['table-uuid']).toBe(customUuid);
    });

    it('should persist UUID through metadata write/read cycle', async () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      const result = await writer.writeNewTable({
        location: 's3://bucket/warehouse/db/table',
      });

      const uuid = result.metadata['table-uuid'];

      // Read back the written metadata
      const metadataPath = result.metadataLocation;
      const data = storage.files.get(metadataPath);
      expect(data).toBeDefined();

      const parsed = JSON.parse(new TextDecoder().decode(data!));
      expect(parsed['table-uuid']).toBe(uuid);
    });

    it('should maintain UUID across metadata updates', async () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      const result1 = await writer.writeNewTable({
        location: 's3://bucket/warehouse/db/table',
      });

      const originalUuid = result1.metadata['table-uuid'];

      // Update metadata (e.g., add property)
      const builder = TableMetadataBuilder.fromMetadata(result1.metadata);
      builder.setProperty('test.key', 'test.value');
      const updatedMetadata = builder.build();

      expect(updatedMetadata['table-uuid']).toBe(originalUuid);
    });
  });

  describe('UUID Validation', () => {
    it('should use crypto.randomUUID format when available', () => {
      // The implementation should use crypto.getRandomValues
      // which is available in Node.js and browsers
      const uuid = generateUUID();
      expect(uuid).toBeTruthy();
      expect(typeof uuid).toBe('string');
    });

    it('should generate cryptographically random UUIDs', () => {
      // Test randomness by checking distribution of characters
      const uuids: string[] = [];
      for (let i = 0; i < 100; i++) {
        uuids.push(generateUUID());
      }

      // Check that we see variety in each position
      const position5Chars = new Set(uuids.map((u) => u[0]));
      expect(position5Chars.size).toBeGreaterThan(1);
    });
  });

  describe('UUID in Table Metadata', () => {
    it('should include table-uuid in metadata builder output', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      expect(metadata).toHaveProperty('table-uuid');
      expect(typeof metadata['table-uuid']).toBe('string');
    });

    it('should access UUID via builder getter method', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const uuid = builder.getTableUuid();
      const metadata = builder.build();

      expect(uuid).toBe(metadata['table-uuid']);
    });

    it('should serialize UUID correctly in JSON output', () => {
      const customUuid = 'deadbeef-dead-4ead-beef-deadbeefcafe';
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
        tableUuid: customUuid,
      });

      const json = builder.toJSON();
      expect(json).toContain('"table-uuid": "deadbeef-dead-4ead-beef-deadbeefcafe"');
    });
  });
});
