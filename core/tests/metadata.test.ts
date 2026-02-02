import { describe, it, expect } from 'vitest';
import {
  createDefaultSchema,
  createUnpartitionedSpec,
  createIdentityPartitionSpec,
  createBucketPartitionSpec,
  createUnsortedOrder,
  createSortOrder,
  generateUUID,
  TableMetadataBuilder,
  SnapshotBuilder,
  ManifestGenerator,
  ManifestListGenerator,
  validateSchemaEvolution,
  findMaxFieldId,
  generateSchemaId,
} from '../src/index.js';

describe('Schema Creation', () => {
  it('should create a default schema', () => {
    const schema = createDefaultSchema();
    expect(schema['schema-id']).toBe(0);
    expect(schema.type).toBe('struct');
    expect(schema.fields).toHaveLength(4);
    expect(schema.fields[0].name).toBe('_id');
    expect(schema.fields[1].name).toBe('_seq');
    expect(schema.fields[2].name).toBe('_op');
    expect(schema.fields[3].name).toBe('_data');
  });

  it('should create an unpartitioned spec', () => {
    const spec = createUnpartitionedSpec();
    expect(spec['spec-id']).toBe(0);
    expect(spec.fields).toHaveLength(0);
  });

  it('should create an identity partition spec', () => {
    const spec = createIdentityPartitionSpec(1, 'date_col');
    expect(spec['spec-id']).toBe(0);
    expect(spec.fields).toHaveLength(1);
    expect(spec.fields[0]['source-id']).toBe(1);
    expect(spec.fields[0].transform).toBe('identity');
  });

  it('should create a bucket partition spec', () => {
    const spec = createBucketPartitionSpec(1, 'user_id', 16);
    expect(spec.fields[0].transform).toBe('bucket[16]');
  });

  it('should create an unsorted order', () => {
    const order = createUnsortedOrder();
    expect(order['order-id']).toBe(0);
    expect(order.fields).toHaveLength(0);
  });

  it('should create a sort order', () => {
    const order = createSortOrder(1, 'asc', 'nulls-first');
    expect(order['order-id']).toBe(1);
    expect(order.fields).toHaveLength(1);
    expect(order.fields[0]['source-id']).toBe(1);
    expect(order.fields[0].direction).toBe('asc');
  });
});

describe('UUID Generation', () => {
  it('should generate valid UUIDs', () => {
    const uuid = generateUUID();
    expect(uuid).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  });

  it('should generate unique UUIDs', () => {
    const uuids = new Set(Array.from({ length: 100 }, () => generateUUID()));
    expect(uuids.size).toBe(100);
  });
});

describe('TableMetadataBuilder', () => {
  it('should create basic table metadata', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/warehouse/db/table',
    });

    const metadata = builder.build();
    expect(metadata['format-version']).toBe(2);
    expect(metadata.location).toBe('s3://bucket/warehouse/db/table');
    expect(metadata['table-uuid']).toBeTruthy();
    expect(metadata.schemas).toHaveLength(1);
    expect(metadata['partition-specs']).toHaveLength(1);
    expect(metadata['sort-orders']).toHaveLength(1);
    expect(metadata.snapshots).toHaveLength(0);
    expect(metadata['current-snapshot-id']).toBeNull();
  });

  it('should add properties', () => {
    const builder = new TableMetadataBuilder({
      location: 's3://bucket/table',
      properties: { 'app.name': 'test' },
    });

    builder.setProperty('custom.key', 'value');
    const metadata = builder.build();

    expect(metadata.properties['app.name']).toBe('test');
    expect(metadata.properties['custom.key']).toBe('value');
  });
});

describe('SnapshotBuilder', () => {
  it('should create a snapshot', () => {
    const builder = new SnapshotBuilder({
      sequenceNumber: 1,
      manifestListPath: 's3://bucket/metadata/snap-123.avro',
    });

    const snapshot = builder.build();
    expect(snapshot['sequence-number']).toBe(1);
    expect(snapshot['manifest-list']).toBe('s3://bucket/metadata/snap-123.avro');
    expect(snapshot.summary.operation).toBe('append');
  });

  it('should set snapshot summary', () => {
    const builder = new SnapshotBuilder({
      sequenceNumber: 1,
      manifestListPath: 's3://bucket/metadata/snap.avro',
      operation: 'append',
    });

    builder.setSummary(10, 0, 1000, 0, 4096, 0, 1000, 4096, 10);
    const snapshot = builder.build();

    expect(snapshot.summary['added-data-files']).toBe('10');
    expect(snapshot.summary['added-records']).toBe('1000');
    expect(snapshot.summary['total-records']).toBe('1000');
    expect(snapshot.summary['total-data-files']).toBe('10');
  });
});

describe('ManifestGenerator', () => {
  it('should create an empty manifest', () => {
    const manifest = new ManifestGenerator({
      sequenceNumber: 1,
      snapshotId: 123456789,
    });

    const result = manifest.generate();
    expect(result.entries).toHaveLength(0);
    expect(result.summary.addedFiles).toBe(0);
  });

  it('should add data files', () => {
    const manifest = new ManifestGenerator({
      sequenceNumber: 1,
      snapshotId: 123456789,
    });

    manifest.addDataFile({
      'file-path': 's3://bucket/data/file1.parquet',
      'file-format': 'parquet',
      'record-count': 1000,
      'file-size-in-bytes': 4096,
      partition: {},
    });

    manifest.addDataFile({
      'file-path': 's3://bucket/data/file2.parquet',
      'file-format': 'parquet',
      'record-count': 2000,
      'file-size-in-bytes': 8192,
      partition: {},
    });

    expect(manifest.entryCount).toBe(2);

    const result = manifest.generate();
    expect(result.summary.addedFiles).toBe(2);
    expect(result.summary.addedRows).toBe(3000);
  });
});

describe('ManifestListGenerator', () => {
  it('should create an empty manifest list', () => {
    const list = new ManifestListGenerator({
      snapshotId: 123456789,
      sequenceNumber: 1,
    });

    expect(list.generate()).toHaveLength(0);
  });

  it('should add manifests with stats', () => {
    const list = new ManifestListGenerator({
      snapshotId: 123456789,
      sequenceNumber: 1,
    });

    list.addManifestWithStats('s3://bucket/metadata/manifest1.avro', 1024, 0, {
      addedFiles: 10,
      existingFiles: 0,
      deletedFiles: 0,
      addedRows: 1000,
      existingRows: 0,
      deletedRows: 0,
    });

    expect(list.manifestCount).toBe(1);

    const totals = list.getTotals();
    expect(totals.totalFiles).toBe(10);
    expect(totals.totalRows).toBe(1000);
  });
});

describe('Schema Evolution', () => {
  it('should detect compatible schema changes', () => {
    const oldSchema = createDefaultSchema();
    const newSchema = {
      ...createDefaultSchema(),
      'schema-id': 1,
      fields: [
        ...createDefaultSchema().fields,
        { id: 5, name: 'new_field', required: false, type: 'string' as const },
      ],
    };

    const result = validateSchemaEvolution(oldSchema, newSchema);
    expect(result.compatible).toBe(true);
    expect(result.changes.some((c) => c.type === 'add-field' && c.fieldId === 5)).toBe(true);
  });

  it('should detect breaking changes', () => {
    const oldSchema = createDefaultSchema();
    const newSchema = {
      ...createDefaultSchema(),
      'schema-id': 1,
      fields: [
        ...createDefaultSchema().fields,
        { id: 5, name: 'required_field', required: true, type: 'string' as const },
      ],
    };

    const result = validateSchemaEvolution(oldSchema, newSchema);
    expect(result.compatible).toBe(false);
    expect(result.breakingChanges.length).toBeGreaterThan(0);
  });

  it('should find max field ID', () => {
    const schema = createDefaultSchema();
    expect(findMaxFieldId(schema)).toBe(4);
  });

  it('should generate new schema ID', () => {
    const schemas = [createDefaultSchema()];
    expect(generateSchemaId(schemas)).toBe(1);
  });
});
