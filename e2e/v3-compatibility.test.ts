/**
 * Ecosystem Compatibility Tests for Iceberg v3 Tables
 *
 * These tests verify that v3 tables created by @dotdo/iceberg can be read by
 * other Iceberg implementations (Spark, Trino, DuckDB, PyIceberg).
 *
 * The tests are validation-based - we verify our output matches the spec that
 * other implementations follow, rather than actually running those engines.
 *
 * Compatibility Requirements:
 * - Spark 3.5+ (org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0+)
 * - Trino 436+ (with iceberg connector)
 * - DuckDB 0.10+ (with iceberg extension)
 * - PyIceberg 0.6.0+
 *
 * @see https://iceberg.apache.org/spec/
 * @see https://iceberg.apache.org/releases/
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  TableMetadataBuilder,
  SnapshotBuilder,
  MetadataWriter,
  parseTableMetadata,
  createDefaultSchema,
  createUnpartitionedSpec,
  createUnsortedOrder,
  createIdentityPartitionSpec,
  createTimePartitionSpec,
  createSortOrder,
  type TableMetadata,
  type IcebergSchema,
  type StorageBackend,
} from '../core/src/index.js';

// ============================================================================
// Test Utilities
// ============================================================================

/**
 * Create an in-memory storage backend for testing.
 */
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

/**
 * V3 Required Fields per Iceberg Specification
 * @see https://iceberg.apache.org/spec/#table-metadata-fields
 */
const V3_REQUIRED_FIELDS = [
  'format-version',
  'table-uuid',
  'location',
  'last-sequence-number',
  'last-updated-ms',
  'last-column-id',
  'current-schema-id',
  'schemas',
  'default-spec-id',
  'partition-specs',
  'last-partition-id',
  'default-sort-order-id',
  'sort-orders',
  'current-snapshot-id',
  'snapshots',
];

/**
 * V3-specific fields (not in v2)
 */
const V3_NEW_FIELDS = ['next-row-id'];

/**
 * V3-specific primitive types
 */
const V3_PRIMITIVE_TYPES = [
  'timestamp_ns',
  'timestamptz_ns',
  'variant',
  'unknown',
  'geometry',
  'geography',
];

// ============================================================================
// 1. Metadata Format Tests
// ============================================================================

describe('V3 Metadata Format Tests', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let writer: MetadataWriter;

  beforeEach(() => {
    storage = createMockStorage();
    writer = new MetadataWriter(storage);
  });

  describe('Required v3 fields are present', () => {
    it('should have format-version as the first field in JSON', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
        formatVersion: 3,
      });
      const metadata = builder.build();
      const json = builder.toJSON();

      // Parse to get key order
      const parsed = JSON.parse(json);
      const keys = Object.keys(parsed);

      expect(keys[0]).toBe('format-version');
      expect(parsed['format-version']).toBe(3);
    });

    it('should include all required v3 fields', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
        formatVersion: 3,
      });
      const metadata = builder.build();

      for (const field of V3_REQUIRED_FIELDS) {
        expect(metadata).toHaveProperty(field);
      }
    });

    it('should have format-version exactly equal to 3', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
        formatVersion: 3,
      });
      const metadata = builder.build();

      expect(metadata['format-version']).toBe(3);
      expect(typeof metadata['format-version']).toBe('number');
    });

    it('should have valid UUID format for table-uuid', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
        formatVersion: 3,
      });
      const metadata = builder.build();

      // UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
      const uuidPattern =
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
      expect(metadata['table-uuid']).toMatch(uuidPattern);
    });

    it('should have valid timestamp for last-updated-ms', () => {
      const before = Date.now();
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
        formatVersion: 3,
      });
      const metadata = builder.build();
      const after = Date.now();

      expect(metadata['last-updated-ms']).toBeGreaterThanOrEqual(before);
      expect(metadata['last-updated-ms']).toBeLessThanOrEqual(after);
    });

    it('should have current-snapshot-id as null for empty table', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
        formatVersion: 3,
      });
      const metadata = builder.build();

      expect(metadata['current-snapshot-id']).toBeNull();
    });
  });

  describe('V3-specific fields', () => {
    it('should include next-row-id field for v3 tables', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
        formatVersion: 3,
      });
      const metadata = builder.build();

      // next-row-id is required for v3
      expect(metadata).toHaveProperty('next-row-id');
      expect(typeof metadata['next-row-id']).toBe('number');
      expect(metadata['next-row-id']).toBeGreaterThanOrEqual(0);
    });

    it('should NOT include next-row-id for v2 tables', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
        formatVersion: 2,
      });
      const metadata = builder.build();

      // For v2, next-row-id should be undefined or not serialized
      const json = builder.toJSON();
      const parsed = JSON.parse(json);

      expect(parsed['next-row-id']).toBeUndefined();
    });
  });

  describe('Field ordering matches spec', () => {
    it('should have format-version as first field for Spark compatibility', () => {
      /**
       * Spark's Iceberg implementation expects format-version first.
       * This ensures Spark can detect the format version before parsing.
       */
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
        formatVersion: 3,
      });
      const json = builder.toJSON();

      // The JSON should start with format-version
      expect(json.trim()).toMatch(/^\{\s*"format-version":\s*3/);
    });

    it('should serialize numbers as numbers not strings', () => {
      /**
       * Trino and PyIceberg expect format-version to be a number.
       * Some implementations fail if it's a string.
       */
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
        formatVersion: 3,
      });
      const json = builder.toJSON();

      // Should be "format-version": 3 not "format-version": "3"
      expect(json).toContain('"format-version": 3');
      expect(json).not.toContain('"format-version": "3"');
    });
  });
});

// ============================================================================
// 2. Spark Compatibility Tests
// ============================================================================

describe('Spark 3.5+ Compatibility', () => {
  /**
   * Spark 3.5 with Iceberg 1.4+ supports format version 3.
   * @see https://iceberg.apache.org/releases/
   */

  it('should produce metadata valid for Spark 3.5 with Iceberg 1.4+', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/warehouse/db/spark_table',
      formatVersion: 3,
      schema: {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          { id: 2, name: 'data', required: false, type: 'string' },
          { id: 3, name: 'ts', required: false, type: 'timestamptz_ns' }, // v3 type
        ],
      },
      properties: {
        'write.format.default': 'parquet',
      },
    });
    const metadata = builder.build();

    // Spark requirements
    expect(metadata['format-version']).toBe(3);
    expect(metadata.schemas.length).toBeGreaterThanOrEqual(1);
    expect(metadata['partition-specs'].length).toBeGreaterThanOrEqual(1);
    expect(metadata['sort-orders'].length).toBeGreaterThanOrEqual(1);
  });

  it('should support timestamp_ns type for Spark compatibility', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/warehouse/db/table',
      formatVersion: 3,
      schema: {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'event_time', required: true, type: 'timestamp_ns' },
        ],
      },
    });
    const metadata = builder.build();

    expect(metadata.schemas[0].fields[0].type).toBe('timestamp_ns');
  });

  it('should support timestamptz_ns type for Spark compatibility', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/warehouse/db/table',
      formatVersion: 3,
      schema: {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'created_at', required: false, type: 'timestamptz_ns' },
        ],
      },
    });
    const metadata = builder.build();

    expect(metadata.schemas[0].fields[0].type).toBe('timestamptz_ns');
  });

  it('should include snapshot first-row-id for Spark row lineage', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/warehouse/db/table',
      formatVersion: 3,
    });

    const snapshot = new SnapshotBuilder({
      sequenceNumber: 1,
      snapshotId: Date.now(),
      manifestListPath: 's3://bucket/warehouse/db/table/metadata/snap-1.avro',
      operation: 'append',
      formatVersion: 3, // Important: specify v3 for row lineage fields
      firstRowId: 0,
      addedRows: 1000,
    }).build();

    builder.addSnapshot(snapshot);
    const metadata = builder.build();

    // V3 snapshots should have first-row-id and added-rows
    const snap = metadata.snapshots[0];
    expect(snap).toHaveProperty('first-row-id');
    expect(snap).toHaveProperty('added-rows');
    expect(snap['first-row-id']).toBe(0);
    expect(snap['added-rows']).toBe(1000);
  });
});

// ============================================================================
// 3. Trino Compatibility Tests
// ============================================================================

describe('Trino 436+ Compatibility', () => {
  /**
   * Trino 436+ supports Iceberg format version 3.
   * @see https://trino.io/docs/current/connector/iceberg.html
   */

  it('should produce metadata valid for Trino Iceberg connector', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/warehouse/db/trino_table',
      formatVersion: 3,
      schema: {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          { id: 2, name: 'name', required: false, type: 'string' },
        ],
      },
    });
    const metadata = builder.build();
    const json = builder.toJSON();

    // Trino requires format-version to be a number, not string
    const parsed = JSON.parse(json);
    expect(typeof parsed['format-version']).toBe('number');
    expect(parsed['format-version']).toBe(3);

    // Trino requires these fields
    expect(parsed).toHaveProperty('table-uuid');
    expect(parsed).toHaveProperty('schemas');
    expect(parsed).toHaveProperty('partition-specs');
    expect(parsed).toHaveProperty('sort-orders');
  });

  it('should support variant type for Trino semi-structured data', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/warehouse/db/table',
      formatVersion: 3,
      schema: {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          { id: 2, name: 'payload', required: false, type: 'variant' },
        ],
      },
    });
    const metadata = builder.build();

    expect(metadata.schemas[0].fields[1].type).toBe('variant');
  });

  it('should have proper schema structure for Trino', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/warehouse/db/table',
      formatVersion: 3,
    });
    const metadata = builder.build();

    // Each schema must have schema-id, type: struct, and fields
    for (const schema of metadata.schemas) {
      expect(schema).toHaveProperty('schema-id');
      expect(schema.type).toBe('struct');
      expect(Array.isArray(schema.fields)).toBe(true);

      // Each field must have id, name, required, type
      for (const field of schema.fields) {
        expect(field).toHaveProperty('id');
        expect(field).toHaveProperty('name');
        expect(field).toHaveProperty('required');
        expect(field).toHaveProperty('type');
        expect(typeof field.id).toBe('number');
        expect(typeof field.name).toBe('string');
        expect(typeof field.required).toBe('boolean');
      }
    }
  });
});

// ============================================================================
// 4. DuckDB Compatibility Tests
// ============================================================================

describe('DuckDB 0.10+ Compatibility', () => {
  /**
   * DuckDB's Iceberg extension supports format version 3.
   * @see https://duckdb.org/docs/extensions/iceberg.html
   */

  it('should produce metadata readable by DuckDB Iceberg extension', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/warehouse/db/duckdb_table',
      formatVersion: 3,
      schema: {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          { id: 2, name: 'value', required: false, type: 'double' },
        ],
      },
    });
    const metadata = builder.build();
    const json = builder.toJSON();

    // DuckDB parses JSON directly, so validate structure
    const parsed = JSON.parse(json);

    expect(parsed['format-version']).toBe(3);
    expect(parsed.location).toBe('s3://bucket/warehouse/db/duckdb_table');
    expect(Array.isArray(parsed.schemas)).toBe(true);
    expect(Array.isArray(parsed.snapshots)).toBe(true);
  });

  it('should support complex types for DuckDB', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/warehouse/db/table',
      formatVersion: 3,
      schema: {
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
      },
    });
    const metadata = builder.build();

    const listField = metadata.schemas[0].fields.find((f) => f.name === 'tags');
    const mapField = metadata.schemas[0].fields.find(
      (f) => f.name === 'attributes'
    );

    expect(listField?.type).toHaveProperty('type', 'list');
    expect(mapField?.type).toHaveProperty('type', 'map');
  });

  it('should have properly formatted partition specs for DuckDB', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/warehouse/db/table',
      formatVersion: 3,
      schema: {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          { id: 2, name: 'event_date', required: true, type: 'date' },
        ],
      },
      partitionSpec: createTimePartitionSpec(2, 'event_day', 'day'),
    });
    const metadata = builder.build();

    const spec = metadata['partition-specs'][0];
    expect(spec).toHaveProperty('spec-id');
    expect(spec).toHaveProperty('fields');
    expect(Array.isArray(spec.fields)).toBe(true);

    if (spec.fields.length > 0) {
      const field = spec.fields[0];
      expect(field).toHaveProperty('source-id');
      expect(field).toHaveProperty('field-id');
      expect(field).toHaveProperty('name');
      expect(field).toHaveProperty('transform');
    }
  });
});

// ============================================================================
// 5. PyIceberg Compatibility Tests
// ============================================================================

describe('PyIceberg 0.6.0+ Compatibility', () => {
  /**
   * PyIceberg 0.6.0+ supports Iceberg format version 3.
   * @see https://py.iceberg.apache.org/
   */

  it('should produce metadata parseable by PyIceberg', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/warehouse/db/pyiceberg_table',
      formatVersion: 3,
    });
    const metadata = builder.build();
    const json = builder.toJSON();

    // PyIceberg uses pydantic for validation
    // These are the critical fields it checks
    const parsed = JSON.parse(json);

    expect(parsed['format-version']).toBe(3);
    expect(typeof parsed['table-uuid']).toBe('string');
    expect(typeof parsed.location).toBe('string');
    expect(typeof parsed['last-sequence-number']).toBe('number');
    expect(typeof parsed['last-updated-ms']).toBe('number');
    expect(typeof parsed['last-column-id']).toBe('number');
    expect(typeof parsed['current-schema-id']).toBe('number');
    expect(Array.isArray(parsed.schemas)).toBe(true);
  });

  it('should have correct schema field structure for PyIceberg', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/warehouse/db/table',
      formatVersion: 3,
      schema: {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          { id: 2, name: 'name', required: false, type: 'string' },
          { id: 3, name: 'created', required: false, type: 'timestamptz' },
        ],
      },
    });
    const metadata = builder.build();

    // PyIceberg validates field structure strictly
    const schema = metadata.schemas[0];
    expect(schema['schema-id']).toBe(0);
    expect(schema.type).toBe('struct');

    for (const field of schema.fields) {
      // All fields must have these exact properties
      expect(typeof field.id).toBe('number');
      expect(field.id).toBeGreaterThan(0);
      expect(typeof field.name).toBe('string');
      expect(field.name.length).toBeGreaterThan(0);
      expect(typeof field.required).toBe('boolean');
    }
  });

  it('should support nullable (current-snapshot-id: null) for PyIceberg', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/warehouse/db/table',
      formatVersion: 3,
    });
    const metadata = builder.build();
    const json = builder.toJSON();

    // PyIceberg requires current-snapshot-id to be present (even if null)
    const parsed = JSON.parse(json);
    expect('current-snapshot-id' in parsed).toBe(true);
    expect(parsed['current-snapshot-id']).toBeNull();
  });

  it('should serialize snapshots array correctly for PyIceberg', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/warehouse/db/table',
      formatVersion: 3,
    });

    const snapshot = new SnapshotBuilder({
      sequenceNumber: 1,
      snapshotId: 1234567890,
      manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      operation: 'append',
    }).build();

    builder.addSnapshot(snapshot);
    const metadata = builder.build();
    const json = builder.toJSON();
    const parsed = JSON.parse(json);

    // PyIceberg validates snapshot structure
    expect(parsed.snapshots.length).toBe(1);
    const snap = parsed.snapshots[0];
    expect(snap['snapshot-id']).toBe(1234567890);
    expect(snap['sequence-number']).toBe(1);
    expect(typeof snap['timestamp-ms']).toBe('number');
    expect(snap['manifest-list']).toBe('s3://bucket/table/metadata/snap-1.avro');
    expect(snap.summary.operation).toBe('append');
  });
});

// ============================================================================
// 6. Round-Trip Tests
// ============================================================================

describe('Round-Trip Serialization Tests', () => {
  it('should preserve all v3 fields through JSON round-trip', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/warehouse/db/roundtrip_table',
      formatVersion: 3,
      schema: {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          { id: 2, name: 'data', required: false, type: 'string' },
        ],
      },
      properties: {
        'write.format.default': 'parquet',
        'custom.property': 'value',
      },
    });

    const original = builder.build();
    const json = JSON.stringify(original, null, 2);
    const parsed = parseTableMetadata(json);

    // Verify all required fields preserved
    expect(parsed['format-version']).toBe(original['format-version']);
    expect(parsed['table-uuid']).toBe(original['table-uuid']);
    expect(parsed.location).toBe(original.location);
    expect(parsed['last-sequence-number']).toBe(original['last-sequence-number']);
    expect(parsed['last-column-id']).toBe(original['last-column-id']);
    expect(parsed['current-schema-id']).toBe(original['current-schema-id']);
    expect(parsed.schemas).toEqual(original.schemas);
    expect(parsed['partition-specs']).toEqual(original['partition-specs']);
    expect(parsed['sort-orders']).toEqual(original['sort-orders']);
    expect(parsed['current-snapshot-id']).toBe(original['current-snapshot-id']);
    expect(parsed.properties).toEqual(original.properties);
  });

  it('should preserve v3 types through round-trip', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/warehouse/db/table',
      formatVersion: 3,
      schema: {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          { id: 2, name: 'nano_ts', required: false, type: 'timestamp_ns' },
          { id: 3, name: 'nano_tstz', required: false, type: 'timestamptz_ns' },
          { id: 4, name: 'variant_col', required: false, type: 'variant' },
        ],
      },
    });

    const original = builder.build();
    const json = JSON.stringify(original);
    const parsed = parseTableMetadata(json);

    expect(parsed.schemas[0].fields[1].type).toBe('timestamp_ns');
    expect(parsed.schemas[0].fields[2].type).toBe('timestamptz_ns');
    expect(parsed.schemas[0].fields[3].type).toBe('variant');
  });

  it('should preserve snapshots with v3 row lineage through round-trip', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/warehouse/db/table',
      formatVersion: 3,
    });

    const snapshot = new SnapshotBuilder({
      sequenceNumber: 1,
      snapshotId: Date.now(),
      manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      operation: 'append',
      formatVersion: 3, // Important: specify v3 for row lineage fields
      firstRowId: 0,
      addedRows: 5000,
    }).build();

    builder.addSnapshot(snapshot);
    const original = builder.build();
    const json = JSON.stringify(original);
    const parsed = parseTableMetadata(json);

    expect(parsed.snapshots[0]['first-row-id']).toBe(0);
    expect(parsed.snapshots[0]['added-rows']).toBe(5000);
  });

  it('should write and read v3 metadata through storage', async () => {
    const storage = createMockStorage();
    const writer = new MetadataWriter(storage);

    const result = await writer.writeNewTable({
      location: 's3://bucket/warehouse/db/table',
      formatVersion: 3,
      schema: {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
        ],
      },
    });

    // Read back from storage
    const data = storage.files.get(result.metadataLocation);
    expect(data).toBeDefined();

    const parsed = JSON.parse(new TextDecoder().decode(data!));
    expect(parsed['format-version']).toBe(3);
  });
});

// ============================================================================
// 7. v3 Feature-Specific Tests
// ============================================================================

describe('V3 Feature-Specific Tests', () => {
  describe('Row Lineage', () => {
    it('should track next-row-id at table level', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
        formatVersion: 3,
      });

      const snapshot1 = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 1001,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
        operation: 'append',
        formatVersion: 3, // Important: specify v3 for row lineage fields
        firstRowId: 0,
        addedRows: 1000,
      }).build();

      builder.addSnapshot(snapshot1);
      const metadata1 = builder.build();

      // After adding 1000 rows, next-row-id should be 1000
      expect(metadata1['next-row-id']).toBe(1000);
    });

    it('should maintain row lineage across multiple snapshots', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
        formatVersion: 3,
      });

      const snapshot1 = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 1001,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
        operation: 'append',
        formatVersion: 3, // Important: specify v3 for row lineage fields
        firstRowId: 0,
        addedRows: 1000,
      }).build();

      builder.addSnapshot(snapshot1);

      const snapshot2 = new SnapshotBuilder({
        sequenceNumber: 2,
        snapshotId: 1002,
        parentSnapshotId: 1001,
        manifestListPath: 's3://bucket/table/metadata/snap-2.avro',
        operation: 'append',
        formatVersion: 3, // Important: specify v3 for row lineage fields
        firstRowId: 1000,
        addedRows: 500,
      }).build();

      builder.addSnapshot(snapshot2);
      const metadata = builder.build();

      // After adding 1500 total rows, next-row-id should be 1500
      expect(metadata['next-row-id']).toBe(1500);
      expect(metadata.snapshots[0]['first-row-id']).toBe(0);
      expect(metadata.snapshots[1]['first-row-id']).toBe(1000);
    });
  });

  describe('V3 Types', () => {
    it('should support all v3 primitive types', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
        formatVersion: 3,
        schema: {
          'schema-id': 0,
          type: 'struct',
          fields: [
            { id: 1, name: 'nano_ts', required: false, type: 'timestamp_ns' },
            { id: 2, name: 'nano_tstz', required: false, type: 'timestamptz_ns' },
            { id: 3, name: 'variant_col', required: false, type: 'variant' },
            { id: 4, name: 'unknown_col', required: false, type: 'unknown' },
          ],
        },
      });
      const metadata = builder.build();

      expect(metadata.schemas[0].fields[0].type).toBe('timestamp_ns');
      expect(metadata.schemas[0].fields[1].type).toBe('timestamptz_ns');
      expect(metadata.schemas[0].fields[2].type).toBe('variant');
      expect(metadata.schemas[0].fields[3].type).toBe('unknown');
    });

    it('should support geospatial types', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
        formatVersion: 3,
        schema: {
          'schema-id': 0,
          type: 'struct',
          fields: [
            { id: 1, name: 'geo', required: false, type: 'geometry' },
            { id: 2, name: 'earth', required: false, type: 'geography' },
            { id: 3, name: 'geo_crs', required: false, type: 'geometry(EPSG:4326)' },
            {
              id: 4,
              name: 'earth_algo',
              required: false,
              type: 'geography(OGC:CRS84, vincenty)',
            },
          ],
        },
      });
      const metadata = builder.build();

      expect(metadata.schemas[0].fields[0].type).toBe('geometry');
      expect(metadata.schemas[0].fields[1].type).toBe('geography');
      expect(metadata.schemas[0].fields[2].type).toBe('geometry(EPSG:4326)');
      expect(metadata.schemas[0].fields[3].type).toBe(
        'geography(OGC:CRS84, vincenty)'
      );
    });
  });

  describe('Default Values', () => {
    it('should support initial-default on fields', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
        formatVersion: 3,
        schema: {
          'schema-id': 0,
          type: 'struct',
          fields: [
            { id: 1, name: 'id', required: true, type: 'long' },
            {
              id: 2,
              name: 'status',
              required: true,
              type: 'string',
              'initial-default': 'active',
            },
          ],
        },
      });
      const metadata = builder.build();

      const statusField = metadata.schemas[0].fields.find(
        (f) => f.name === 'status'
      );
      expect(statusField?.['initial-default']).toBe('active');
    });

    it('should support write-default on fields', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/warehouse/db/table',
        formatVersion: 3,
        schema: {
          'schema-id': 0,
          type: 'struct',
          fields: [
            { id: 1, name: 'id', required: true, type: 'long' },
            {
              id: 2,
              name: 'version',
              required: false,
              type: 'int',
              'write-default': 1,
            },
          ],
        },
      });
      const metadata = builder.build();

      const versionField = metadata.schemas[0].fields.find(
        (f) => f.name === 'version'
      );
      expect(versionField?.['write-default']).toBe(1);
    });
  });
});

// ============================================================================
// Compatibility Documentation
// ============================================================================

/**
 * Ecosystem Version Requirements for Iceberg v3
 *
 * | Implementation | Minimum Version | Notes                              |
 * |----------------|-----------------|-------------------------------------|
 * | Spark          | 3.5 + Iceberg 1.4 | Full v3 support                  |
 * | Trino          | 436+            | Full v3 support with connector      |
 * | DuckDB         | 0.10+           | Via iceberg extension               |
 * | PyIceberg      | 0.6.0+          | Python client with REST support     |
 * | Snowflake      | Latest          | External tables support v3          |
 * | Flink          | 1.18 + Iceberg 1.4 | Full v3 support                 |
 *
 * V3 New Features:
 * - Row lineage (next-row-id, first-row-id, added-rows)
 * - Nanosecond timestamp types (timestamp_ns, timestamptz_ns)
 * - Variant type for semi-structured data
 * - Unknown type for placeholder columns
 * - Geospatial types (geometry, geography)
 * - Default values (initial-default, write-default)
 */
