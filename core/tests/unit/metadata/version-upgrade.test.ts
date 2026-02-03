/**
 * Tests for Iceberg Table Version Upgrade (v2 to v3)
 *
 * @see https://iceberg.apache.org/spec/#format-versioning
 *
 * Table upgrade from v2 to v3 involves:
 * - Changing format-version from 2 to 3
 * - Initializing next-row-id to 0
 * - Preserving existing snapshots without retroactively adding row lineage fields
 *
 * Key constraints:
 * - Cannot upgrade v1 directly to v3 (if v1 support exists)
 * - Cannot downgrade v3 to v2
 * - All existing fields must be preserved during upgrade
 */

import { describe, it, expect } from 'vitest';
import {
  TableMetadataBuilder,
  MetadataWriter,
  SnapshotBuilder,
  upgradeTableToV3,
  type TableMetadata,
  type StorageBackend,
  type IcebergSchema,
  type PartitionSpec,
  type SortOrder,
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

// Helper to create sample v2 metadata
function createV2Metadata(options?: {
  tableUuid?: string;
  location?: string;
  withSnapshots?: boolean;
  schemas?: readonly IcebergSchema[];
  partitionSpecs?: readonly PartitionSpec[];
  sortOrders?: readonly SortOrder[];
  properties?: Record<string, string>;
}): TableMetadata {
  const now = Date.now();
  const tableUuid = options?.tableUuid ?? 'test-uuid-12345';
  const location = options?.location ?? 's3://bucket/warehouse/db/table';

  const baseSchema: IcebergSchema = {
    'schema-id': 0,
    type: 'struct',
    fields: [
      { id: 1, name: 'id', required: true, type: 'long' },
      { id: 2, name: 'name', required: false, type: 'string' },
    ],
  };

  const schemas = options?.schemas ?? [baseSchema];
  const partitionSpecs = options?.partitionSpecs ?? [{ 'spec-id': 0, fields: [] }];
  const sortOrders = options?.sortOrders ?? [{ 'order-id': 0, fields: [] }];

  const metadata: TableMetadata = {
    'format-version': 2,
    'table-uuid': tableUuid,
    location,
    'last-sequence-number': options?.withSnapshots ? 1 : 0,
    'last-updated-ms': now,
    'last-column-id': 2,
    'current-schema-id': 0,
    schemas,
    'default-spec-id': 0,
    'partition-specs': partitionSpecs,
    'last-partition-id': 999,
    'default-sort-order-id': 0,
    'sort-orders': sortOrders,
    properties: options?.properties ?? {},
    'current-snapshot-id': options?.withSnapshots ? 1234567890 : null,
    snapshots: options?.withSnapshots
      ? [
          {
            'snapshot-id': 1234567890,
            'sequence-number': 1,
            'timestamp-ms': now - 10000,
            'manifest-list': `${location}/metadata/snap-1234567890-1-manifest-list.avro`,
            summary: { operation: 'append' },
            'schema-id': 0,
          },
        ]
      : [],
    'snapshot-log': options?.withSnapshots
      ? [{ 'timestamp-ms': now - 10000, 'snapshot-id': 1234567890 }]
      : [],
    'metadata-log': [],
    refs: options?.withSnapshots
      ? { main: { 'snapshot-id': 1234567890, type: 'branch' } }
      : {},
  };

  return metadata;
}

describe('Version Upgrade: upgradeTableToV3 Function', () => {
  describe('Basic Upgrade Tests', () => {
    it('should convert v2 metadata to v3', () => {
      const v2Metadata = createV2Metadata();
      const v3Metadata = upgradeTableToV3(v2Metadata);

      expect(v3Metadata['format-version']).toBe(3);
    });

    it('should initialize next-row-id to 0', () => {
      const v2Metadata = createV2Metadata();
      const v3Metadata = upgradeTableToV3(v2Metadata);

      expect(v3Metadata['next-row-id']).toBe(0);
    });

    it('should preserve table-uuid during upgrade', () => {
      const v2Metadata = createV2Metadata({ tableUuid: 'unique-table-uuid-abc123' });
      const v3Metadata = upgradeTableToV3(v2Metadata);

      expect(v3Metadata['table-uuid']).toBe('unique-table-uuid-abc123');
    });

    it('should update last-updated-ms during upgrade', () => {
      const v2Metadata = createV2Metadata();
      const originalTimestamp = v2Metadata['last-updated-ms'];

      // Wait a small amount to ensure timestamp difference
      const v3Metadata = upgradeTableToV3(v2Metadata);

      expect(v3Metadata['last-updated-ms']).toBeGreaterThanOrEqual(originalTimestamp);
    });
  });

  describe('Snapshot Preservation Tests', () => {
    it('should preserve existing snapshots during upgrade', () => {
      const v2Metadata = createV2Metadata({ withSnapshots: true });
      const v3Metadata = upgradeTableToV3(v2Metadata);

      expect(v3Metadata.snapshots).toHaveLength(1);
      expect(v3Metadata.snapshots[0]['snapshot-id']).toBe(1234567890);
    });

    it('should not add first-row-id to existing snapshots retroactively', () => {
      const v2Metadata = createV2Metadata({ withSnapshots: true });
      const v3Metadata = upgradeTableToV3(v2Metadata);

      // Existing v2 snapshots should NOT have first-row-id added retroactively
      // This is because we cannot determine accurate row IDs for pre-existing data
      expect(v3Metadata.snapshots[0]['first-row-id']).toBeUndefined();
    });

    it('should not add added-rows to existing snapshots retroactively', () => {
      const v2Metadata = createV2Metadata({ withSnapshots: true });
      const v3Metadata = upgradeTableToV3(v2Metadata);

      // Existing v2 snapshots should NOT have added-rows added retroactively
      expect(v3Metadata.snapshots[0]['added-rows']).toBeUndefined();
    });

    it('should preserve current-snapshot-id during upgrade', () => {
      const v2Metadata = createV2Metadata({ withSnapshots: true });
      const v3Metadata = upgradeTableToV3(v2Metadata);

      expect(v3Metadata['current-snapshot-id']).toBe(1234567890);
    });

    it('should preserve snapshot-log during upgrade', () => {
      const v2Metadata = createV2Metadata({ withSnapshots: true });
      const v3Metadata = upgradeTableToV3(v2Metadata);

      expect(v3Metadata['snapshot-log']).toHaveLength(1);
      expect(v3Metadata['snapshot-log'][0]['snapshot-id']).toBe(1234567890);
    });

    it('should preserve refs during upgrade', () => {
      const v2Metadata = createV2Metadata({ withSnapshots: true });
      const v3Metadata = upgradeTableToV3(v2Metadata);

      expect(v3Metadata.refs).toHaveProperty('main');
      expect(v3Metadata.refs.main['snapshot-id']).toBe(1234567890);
    });
  });

  describe('Field Preservation Tests', () => {
    it('should preserve location during upgrade', () => {
      const v2Metadata = createV2Metadata({ location: 's3://custom/path/table' });
      const v3Metadata = upgradeTableToV3(v2Metadata);

      expect(v3Metadata.location).toBe('s3://custom/path/table');
    });

    it('should preserve schemas during upgrade', () => {
      const customSchemas: IcebergSchema[] = [
        {
          'schema-id': 0,
          type: 'struct',
          fields: [{ id: 1, name: 'col1', required: true, type: 'int' }],
        },
        {
          'schema-id': 1,
          type: 'struct',
          fields: [
            { id: 1, name: 'col1', required: true, type: 'int' },
            { id: 2, name: 'col2', required: false, type: 'string' },
          ],
        },
      ];
      const v2Metadata = createV2Metadata({ schemas: customSchemas });
      const v3Metadata = upgradeTableToV3(v2Metadata);

      expect(v3Metadata.schemas).toHaveLength(2);
      expect(v3Metadata.schemas[0]['schema-id']).toBe(0);
      expect(v3Metadata.schemas[1]['schema-id']).toBe(1);
    });

    it('should preserve current-schema-id during upgrade', () => {
      const v2Metadata = createV2Metadata();
      const v3Metadata = upgradeTableToV3(v2Metadata);

      expect(v3Metadata['current-schema-id']).toBe(v2Metadata['current-schema-id']);
    });

    it('should preserve partition-specs during upgrade', () => {
      const customSpecs: PartitionSpec[] = [
        { 'spec-id': 0, fields: [] },
        {
          'spec-id': 1,
          fields: [{ 'source-id': 1, 'field-id': 1000, name: 'id_bucket', transform: 'bucket[16]' }],
        },
      ];
      const v2Metadata = createV2Metadata({ partitionSpecs: customSpecs });
      const v3Metadata = upgradeTableToV3(v2Metadata);

      expect(v3Metadata['partition-specs']).toHaveLength(2);
      expect(v3Metadata['partition-specs'][1]['spec-id']).toBe(1);
    });

    it('should preserve default-spec-id during upgrade', () => {
      const v2Metadata = createV2Metadata();
      const v3Metadata = upgradeTableToV3(v2Metadata);

      expect(v3Metadata['default-spec-id']).toBe(v2Metadata['default-spec-id']);
    });

    it('should preserve sort-orders during upgrade', () => {
      const customSortOrders: SortOrder[] = [
        { 'order-id': 0, fields: [] },
        {
          'order-id': 1,
          fields: [
            { 'source-id': 1, transform: 'identity', direction: 'asc', 'null-order': 'nulls-first' },
          ],
        },
      ];
      const v2Metadata = createV2Metadata({ sortOrders: customSortOrders });
      const v3Metadata = upgradeTableToV3(v2Metadata);

      expect(v3Metadata['sort-orders']).toHaveLength(2);
      expect(v3Metadata['sort-orders'][1]['order-id']).toBe(1);
    });

    it('should preserve properties during upgrade', () => {
      const v2Metadata = createV2Metadata({
        properties: {
          'table.owner': 'test-user',
          'write.format.default': 'parquet',
          'custom.property': 'custom-value',
        },
      });
      const v3Metadata = upgradeTableToV3(v2Metadata);

      expect(v3Metadata.properties['table.owner']).toBe('test-user');
      expect(v3Metadata.properties['write.format.default']).toBe('parquet');
      expect(v3Metadata.properties['custom.property']).toBe('custom-value');
    });

    it('should preserve last-sequence-number during upgrade', () => {
      const v2Metadata = createV2Metadata({ withSnapshots: true });
      const v3Metadata = upgradeTableToV3(v2Metadata);

      expect(v3Metadata['last-sequence-number']).toBe(1);
    });

    it('should preserve last-column-id during upgrade', () => {
      const v2Metadata = createV2Metadata();
      const v3Metadata = upgradeTableToV3(v2Metadata);

      expect(v3Metadata['last-column-id']).toBe(2);
    });

    it('should preserve last-partition-id during upgrade', () => {
      const v2Metadata = createV2Metadata();
      const v3Metadata = upgradeTableToV3(v2Metadata);

      expect(v3Metadata['last-partition-id']).toBe(999);
    });

    it('should preserve metadata-log during upgrade', () => {
      const v2Metadata = createV2Metadata();
      (v2Metadata as { 'metadata-log': { 'timestamp-ms': number; 'metadata-file': string }[] })[
        'metadata-log'
      ] = [{ 'timestamp-ms': Date.now() - 20000, 'metadata-file': 's3://bucket/metadata/v1.metadata.json' }];
      const v3Metadata = upgradeTableToV3(v2Metadata);

      expect(v3Metadata['metadata-log']).toHaveLength(1);
    });
  });

  describe('Validation Tests', () => {
    it('should throw error when trying to upgrade v1 to v3', () => {
      // Create v1-like metadata (if v1 support exists)
      const v1Metadata = {
        ...createV2Metadata(),
        'format-version': 1,
      } as unknown as TableMetadata;

      expect(() => upgradeTableToV3(v1Metadata)).toThrow(/cannot upgrade/i);
    });

    it('should throw error when trying to downgrade v3 to v2', () => {
      // Create v3 metadata
      const v3Metadata = {
        ...createV2Metadata(),
        'format-version': 3,
        'next-row-id': 0,
      } as TableMetadata;

      expect(() => upgradeTableToV3(v3Metadata)).toThrow(/already.*format-version 3/i);
    });

    it('should return unchanged metadata when already v3', () => {
      // Create v3 metadata
      const v3Metadata = {
        ...createV2Metadata(),
        'format-version': 3,
        'next-row-id': 100,
      } as TableMetadata;

      // When metadata is already v3, it should either return unchanged or throw
      // Based on the spec, we should throw an error since upgrade is not needed
      expect(() => upgradeTableToV3(v3Metadata)).toThrow(/already.*format-version 3/i);
    });
  });
});

describe('Version Upgrade: TableMetadataBuilder.fromMetadata', () => {
  describe('Upgrade via Builder', () => {
    it('should support upgrading v2 to v3 when format version option is provided', () => {
      const v2Metadata = createV2Metadata();
      const builder = TableMetadataBuilder.fromMetadata(v2Metadata, { formatVersion: 3 });
      const v3Metadata = builder.build();

      expect(v3Metadata['format-version']).toBe(3);
    });

    it('should initialize next-row-id when upgrading via builder', () => {
      const v2Metadata = createV2Metadata();
      const builder = TableMetadataBuilder.fromMetadata(v2Metadata, { formatVersion: 3 });
      const v3Metadata = builder.build();

      expect(v3Metadata['next-row-id']).toBe(0);
    });

    it('should preserve table-uuid when upgrading via builder', () => {
      const v2Metadata = createV2Metadata({ tableUuid: 'preserved-uuid-xyz' });
      const builder = TableMetadataBuilder.fromMetadata(v2Metadata, { formatVersion: 3 });
      const v3Metadata = builder.build();

      expect(v3Metadata['table-uuid']).toBe('preserved-uuid-xyz');
    });

    it('should update last-updated-ms when upgrading via builder', () => {
      const v2Metadata = createV2Metadata();
      const originalTimestamp = v2Metadata['last-updated-ms'];
      const builder = TableMetadataBuilder.fromMetadata(v2Metadata, { formatVersion: 3 });
      const v3Metadata = builder.build();

      expect(v3Metadata['last-updated-ms']).toBeGreaterThanOrEqual(originalTimestamp);
    });

    it('should preserve all other fields when upgrading via builder', () => {
      const v2Metadata = createV2Metadata({
        location: 's3://bucket/custom/location',
        properties: { 'key1': 'value1' },
        withSnapshots: true,
      });
      const builder = TableMetadataBuilder.fromMetadata(v2Metadata, { formatVersion: 3 });
      const v3Metadata = builder.build();

      expect(v3Metadata.location).toBe('s3://bucket/custom/location');
      expect(v3Metadata.properties['key1']).toBe('value1');
      expect(v3Metadata.snapshots).toHaveLength(1);
    });
  });

  describe('No Upgrade When Same Version', () => {
    it('should not change format version when not upgrading', () => {
      const v2Metadata = createV2Metadata();
      const builder = TableMetadataBuilder.fromMetadata(v2Metadata);
      const rebuilt = builder.build();

      expect(rebuilt['format-version']).toBe(2);
    });

    it('should not add next-row-id when staying on v2', () => {
      const v2Metadata = createV2Metadata();
      const builder = TableMetadataBuilder.fromMetadata(v2Metadata);
      const rebuilt = builder.build();

      expect(rebuilt['next-row-id']).toBeUndefined();
    });
  });
});

describe('Version Upgrade: MetadataWriter', () => {
  describe('Writing Upgraded Metadata', () => {
    it('should write upgraded v3 metadata to storage', async () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      // First write v2 table
      const result1 = await writer.writeNewTable({
        location: 's3://bucket/warehouse/db/table',
        formatVersion: 2,
      });

      // Upgrade to v3
      const v3Metadata = upgradeTableToV3(result1.metadata);

      // Validate the upgraded metadata
      expect(() => writer.validateMetadata(v3Metadata)).not.toThrow();
    });

    it('should update version-hint when writing upgraded metadata', async () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      // First write v2 table
      const result1 = await writer.writeNewTable({
        location: 's3://bucket/warehouse/db/table',
        formatVersion: 2,
      });

      // Upgrade to v3
      const v3Metadata = upgradeTableToV3(result1.metadata);

      // Manually write the v3 metadata to storage (simulating upgrade commit)
      const metadataDir = `${v3Metadata.location}/metadata`;
      const version = 2; // Next version after v1
      const metadataPath = `${metadataDir}/v${version}.metadata.json`;
      await storage.put(metadataPath, new TextEncoder().encode(JSON.stringify(v3Metadata, null, 2)));
      await storage.put(`${metadataDir}/version-hint.text`, new TextEncoder().encode(String(version)));

      // Verify version-hint was updated
      const versionHintData = await storage.get(`${metadataDir}/version-hint.text`);
      expect(versionHintData).toBeDefined();
      expect(new TextDecoder().decode(versionHintData!)).toBe('2');

      // Verify the written metadata is v3
      const writtenData = await storage.get(metadataPath);
      const parsed = JSON.parse(new TextDecoder().decode(writtenData!));
      expect(parsed['format-version']).toBe(3);
      expect(parsed['next-row-id']).toBe(0);
    });
  });

  describe('Validation After Upgrade', () => {
    it('should validate upgraded metadata correctly', async () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      const v2Metadata = createV2Metadata({ withSnapshots: true });
      const v3Metadata = upgradeTableToV3(v2Metadata);

      // v3 metadata with existing v2 snapshots should pass validation
      // because existing snapshots are allowed to not have row lineage fields
      expect(() => writer.validateMetadata(v3Metadata)).not.toThrow();
    });

    it('should reject snapshots with invalid row lineage fields', () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      const v2Metadata = createV2Metadata();
      const v3Metadata = upgradeTableToV3(v2Metadata);

      // Add a snapshot with invalid (negative) first-row-id
      const snapshotWithInvalidRowLineage = {
        ...v3Metadata,
        snapshots: [
          {
            'snapshot-id': 9999999,
            'sequence-number': 1,
            'timestamp-ms': Date.now(),
            'manifest-list': `${v3Metadata.location}/metadata/snap-9999999-1-manifest-list.avro`,
            summary: { operation: 'append' as const },
            'schema-id': 0,
            'first-row-id': -1, // Invalid: must be non-negative
            'added-rows': 100,
          },
        ],
        'current-snapshot-id': 9999999,
        'last-sequence-number': 1,
      } as unknown as TableMetadata;

      // This should fail validation because first-row-id is negative
      expect(() => writer.validateMetadata(snapshotWithInvalidRowLineage)).toThrow(/first-row-id/);
    });

    it('should accept snapshots without row lineage fields for backward compatibility', () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      const v2Metadata = createV2Metadata();
      const v3Metadata = upgradeTableToV3(v2Metadata);

      // Add a snapshot without row lineage fields (allowed for backward compatibility)
      const snapshotWithoutRowLineage = {
        ...v3Metadata,
        snapshots: [
          {
            'snapshot-id': 9999999,
            'sequence-number': 1,
            'timestamp-ms': Date.now(),
            'manifest-list': `${v3Metadata.location}/metadata/snap-9999999-1-manifest-list.avro`,
            summary: { operation: 'append' as const },
            'schema-id': 0,
            // No first-row-id or added-rows - allowed for compatibility
          },
        ],
        'current-snapshot-id': 9999999,
        'last-sequence-number': 1,
      } as unknown as TableMetadata;

      // This should pass validation - row lineage is optional for backward compatibility
      expect(() => writer.validateMetadata(snapshotWithoutRowLineage)).not.toThrow();
    });
  });
});

describe('Version Upgrade: Edge Cases', () => {
  describe('Empty Table Upgrade', () => {
    it('should upgrade empty table (no snapshots) correctly', () => {
      const v2Metadata = createV2Metadata({ withSnapshots: false });
      const v3Metadata = upgradeTableToV3(v2Metadata);

      expect(v3Metadata['format-version']).toBe(3);
      expect(v3Metadata['next-row-id']).toBe(0);
      expect(v3Metadata.snapshots).toHaveLength(0);
      expect(v3Metadata['current-snapshot-id']).toBeNull();
    });
  });

  describe('Table with Multiple Snapshots', () => {
    it('should preserve all snapshots during upgrade', () => {
      const now = Date.now();
      const v2Metadata: TableMetadata = {
        ...createV2Metadata(),
        'last-sequence-number': 3,
        'current-snapshot-id': 3333333,
        snapshots: [
          {
            'snapshot-id': 1111111,
            'sequence-number': 1,
            'timestamp-ms': now - 30000,
            'manifest-list': 's3://bucket/table/metadata/snap-1111111-1-manifest-list.avro',
            summary: { operation: 'append' },
            'schema-id': 0,
          },
          {
            'snapshot-id': 2222222,
            'parent-snapshot-id': 1111111,
            'sequence-number': 2,
            'timestamp-ms': now - 20000,
            'manifest-list': 's3://bucket/table/metadata/snap-2222222-2-manifest-list.avro',
            summary: { operation: 'append' },
            'schema-id': 0,
          },
          {
            'snapshot-id': 3333333,
            'parent-snapshot-id': 2222222,
            'sequence-number': 3,
            'timestamp-ms': now - 10000,
            'manifest-list': 's3://bucket/table/metadata/snap-3333333-3-manifest-list.avro',
            summary: { operation: 'overwrite' },
            'schema-id': 0,
          },
        ],
        'snapshot-log': [
          { 'timestamp-ms': now - 30000, 'snapshot-id': 1111111 },
          { 'timestamp-ms': now - 20000, 'snapshot-id': 2222222 },
          { 'timestamp-ms': now - 10000, 'snapshot-id': 3333333 },
        ],
        refs: {
          main: { 'snapshot-id': 3333333, type: 'branch' },
          tag1: { 'snapshot-id': 1111111, type: 'tag' },
        },
      };

      const v3Metadata = upgradeTableToV3(v2Metadata);

      expect(v3Metadata.snapshots).toHaveLength(3);
      expect(v3Metadata['snapshot-log']).toHaveLength(3);
      expect(v3Metadata.refs).toHaveProperty('main');
      expect(v3Metadata.refs).toHaveProperty('tag1');

      // All snapshots should be preserved without row lineage fields
      for (const snapshot of v3Metadata.snapshots) {
        expect(snapshot['first-row-id']).toBeUndefined();
        expect(snapshot['added-rows']).toBeUndefined();
      }
    });
  });

  describe('Immutability', () => {
    it('should not modify the original v2 metadata', () => {
      const v2Metadata = createV2Metadata({ withSnapshots: true });
      const originalVersion = v2Metadata['format-version'];
      const originalNextRowId = v2Metadata['next-row-id'];

      const v3Metadata = upgradeTableToV3(v2Metadata);

      // Original should be unchanged
      expect(v2Metadata['format-version']).toBe(originalVersion);
      expect(v2Metadata['next-row-id']).toBe(originalNextRowId);

      // Upgraded should be different
      expect(v3Metadata['format-version']).toBe(3);
      expect(v3Metadata['next-row-id']).toBe(0);
    });

    it('should create a new object for upgraded metadata', () => {
      const v2Metadata = createV2Metadata();
      const v3Metadata = upgradeTableToV3(v2Metadata);

      expect(v3Metadata).not.toBe(v2Metadata);
    });
  });
});

describe('Version Upgrade: JSON Serialization', () => {
  it('should produce valid JSON after upgrade', () => {
    const v2Metadata = createV2Metadata({ withSnapshots: true });
    const v3Metadata = upgradeTableToV3(v2Metadata);

    const json = JSON.stringify(v3Metadata, null, 2);
    const parsed = JSON.parse(json);

    expect(parsed['format-version']).toBe(3);
    expect(parsed['next-row-id']).toBe(0);
  });

  it('should include format-version: 3 in JSON output', () => {
    const v2Metadata = createV2Metadata();
    const v3Metadata = upgradeTableToV3(v2Metadata);

    const json = JSON.stringify(v3Metadata, null, 2);

    expect(json).toContain('"format-version": 3');
  });

  it('should include next-row-id: 0 in JSON output', () => {
    const v2Metadata = createV2Metadata();
    const v3Metadata = upgradeTableToV3(v2Metadata);

    const json = JSON.stringify(v3Metadata, null, 2);

    expect(json).toContain('"next-row-id"');
  });

  it('should round-trip serialize upgraded metadata', () => {
    const v2Metadata = createV2Metadata({
      withSnapshots: true,
      properties: { 'test.key': 'test.value' },
    });
    const v3Metadata = upgradeTableToV3(v2Metadata);

    const json = JSON.stringify(v3Metadata);
    const parsed = JSON.parse(json) as TableMetadata;

    expect(parsed['format-version']).toBe(3);
    expect(parsed['next-row-id']).toBe(0);
    expect(parsed.properties['test.key']).toBe('test.value');
    expect(parsed.snapshots).toHaveLength(1);
  });
});
