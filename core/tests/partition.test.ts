/**
 * Partition Transform Tests
 *
 * Tests for partition transforms per the Apache Iceberg v2 specification.
 *
 * @see https://iceberg.apache.org/spec/#partitioning
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  // Transform functions
  applyTransform,
  parseTransform,
  formatTransform,
  getTransformResultType,
  // Partition data
  getPartitionData,
  getPartitionPath,
  parsePartitionPath,
  // Builder
  PartitionSpecBuilder,
  createPartitionSpecBuilder,
  createPartitionSpecFromDefinitions,
  // Statistics
  PartitionStatsCollector,
  createPartitionStatsCollector,
  // Evolution
  comparePartitionSpecs,
  findMaxPartitionFieldId,
  generatePartitionSpecId,
  // Schema helpers
  createDefaultSchema,
  createUnpartitionedSpec,
  // Types
  type IcebergSchema,
  type PartitionSpec,
  type PartitionedFile,
} from '../src/index.js';

// ============================================================================
// Test Helpers
// ============================================================================

function createTestSchema(): IcebergSchema {
  return {
    'schema-id': 0,
    type: 'struct',
    fields: [
      { id: 1, name: 'id', required: true, type: 'string' },
      { id: 2, name: 'name', required: false, type: 'string' },
      { id: 3, name: 'created_at', required: true, type: 'timestamptz' },
      { id: 4, name: 'region', required: true, type: 'string' },
      { id: 5, name: 'user_id', required: true, type: 'long' },
      { id: 6, name: 'value', required: false, type: 'long' },
      { id: 7, name: 'price', required: false, type: 'double' },
    ],
  };
}

function createTestFile(
  partitionData: Record<string, unknown>,
  recordCount: number = 1000,
  fileSizeBytes: number = 102400
): PartitionedFile {
  return {
    filePath: `data/part-${Math.random().toString(36).substring(7)}.parquet`,
    partitionData,
    recordCount,
    fileSizeBytes,
  };
}

// ============================================================================
// Transform Parsing Tests
// ============================================================================

describe('Transform Parsing', () => {
  describe('parseTransform', () => {
    it('should parse simple transforms', () => {
      expect(parseTransform('identity')).toEqual({ type: 'identity' });
      expect(parseTransform('year')).toEqual({ type: 'year' });
      expect(parseTransform('month')).toEqual({ type: 'month' });
      expect(parseTransform('day')).toEqual({ type: 'day' });
      expect(parseTransform('hour')).toEqual({ type: 'hour' });
      expect(parseTransform('void')).toEqual({ type: 'void' });
    });

    it('should parse bucket transform with argument', () => {
      expect(parseTransform('bucket[16]')).toEqual({ type: 'bucket', arg: 16 });
      expect(parseTransform('bucket[128]')).toEqual({ type: 'bucket', arg: 128 });
      expect(parseTransform('bucket[1]')).toEqual({ type: 'bucket', arg: 1 });
    });

    it('should parse truncate transform with argument', () => {
      expect(parseTransform('truncate[5]')).toEqual({ type: 'truncate', arg: 5 });
      expect(parseTransform('truncate[100]')).toEqual({ type: 'truncate', arg: 100 });
    });

    it('should throw for unknown transforms', () => {
      expect(() => parseTransform('unknown')).toThrow('Unknown partition transform');
      expect(() => parseTransform('bucket')).toThrow('Unknown partition transform');
    });
  });

  describe('formatTransform', () => {
    it('should format simple transforms', () => {
      expect(formatTransform({ type: 'identity' })).toBe('identity');
      expect(formatTransform({ type: 'year' })).toBe('year');
      expect(formatTransform({ type: 'void' })).toBe('void');
    });

    it('should format parameterized transforms', () => {
      expect(formatTransform({ type: 'bucket', arg: 16 })).toBe('bucket[16]');
      expect(formatTransform({ type: 'truncate', arg: 5 })).toBe('truncate[5]');
    });
  });
});

// ============================================================================
// Identity Transform Tests
// ============================================================================

describe('Identity Transform', () => {
  it('should return string values unchanged', () => {
    expect(applyTransform('test', 'identity')).toBe('test');
    expect(applyTransform('hello world', 'identity')).toBe('hello world');
  });

  it('should return number values unchanged', () => {
    expect(applyTransform(42, 'identity')).toBe(42);
    expect(applyTransform(3.14159, 'identity')).toBe(3.14159);
    expect(applyTransform(-100, 'identity')).toBe(-100);
  });

  it('should return boolean values unchanged', () => {
    expect(applyTransform(true, 'identity')).toBe(true);
    expect(applyTransform(false, 'identity')).toBe(false);
  });

  it('should return null for null values', () => {
    expect(applyTransform(null, 'identity')).toBe(null);
    expect(applyTransform(undefined, 'identity')).toBe(null);
  });
});

// ============================================================================
// Bucket Transform Tests
// ============================================================================

describe('Bucket Transform', () => {
  it('should hash string values into buckets', () => {
    const bucket = applyTransform('user123', 'bucket', 16) as number;
    expect(typeof bucket).toBe('number');
    expect(bucket).toBeGreaterThanOrEqual(0);
    expect(bucket).toBeLessThan(16);
  });

  it('should hash number values into buckets', () => {
    const bucket = applyTransform(12345, 'bucket', 16) as number;
    expect(typeof bucket).toBe('number');
    expect(bucket).toBeGreaterThanOrEqual(0);
    expect(bucket).toBeLessThan(16);
  });

  it('should be deterministic', () => {
    const bucket1 = applyTransform('user123', 'bucket', 16);
    const bucket2 = applyTransform('user123', 'bucket', 16);
    const bucket3 = applyTransform('user123', 'bucket', 16);
    expect(bucket1).toBe(bucket2);
    expect(bucket2).toBe(bucket3);
  });

  it('should distribute values across buckets', () => {
    const buckets = new Set<number>();
    for (let i = 0; i < 100; i++) {
      const bucket = applyTransform(`user${i}`, 'bucket', 16) as number;
      buckets.add(bucket);
    }
    // Should use multiple buckets (statistically very likely with 100 values)
    expect(buckets.size).toBeGreaterThan(1);
  });

  it('should handle different bucket counts', () => {
    const bucket4 = applyTransform('test', 'bucket', 4) as number;
    const bucket256 = applyTransform('test', 'bucket', 256) as number;
    expect(bucket4).toBeGreaterThanOrEqual(0);
    expect(bucket4).toBeLessThan(4);
    expect(bucket256).toBeGreaterThanOrEqual(0);
    expect(bucket256).toBeLessThan(256);
  });

  it('should return null for null values', () => {
    expect(applyTransform(null, 'bucket', 16)).toBe(null);
    expect(applyTransform(undefined, 'bucket', 16)).toBe(null);
  });

  it('should throw without bucket count', () => {
    expect(() => applyTransform('test', 'bucket')).toThrow('requires number of buckets');
  });

  it('should parse bucket transform string', () => {
    const bucket = applyTransform('test', 'bucket[16]') as number;
    expect(bucket).toBeGreaterThanOrEqual(0);
    expect(bucket).toBeLessThan(16);
  });
});

// ============================================================================
// Truncate Transform Tests
// ============================================================================

describe('Truncate Transform', () => {
  describe('String truncation', () => {
    it('should truncate strings to specified width', () => {
      expect(applyTransform('hello world', 'truncate', 5)).toBe('hello');
      expect(applyTransform('hello', 'truncate', 5)).toBe('hello');
      expect(applyTransform('hi', 'truncate', 5)).toBe('hi');
    });

    it('should handle empty strings', () => {
      expect(applyTransform('', 'truncate', 5)).toBe('');
    });

    it('should handle width of 1', () => {
      expect(applyTransform('hello', 'truncate', 1)).toBe('h');
    });
  });

  describe('Integer truncation', () => {
    it('should truncate integers to nearest multiple', () => {
      expect(applyTransform(123, 'truncate', 100)).toBe(100);
      expect(applyTransform(99, 'truncate', 100)).toBe(0);
      expect(applyTransform(250, 'truncate', 100)).toBe(200);
      expect(applyTransform(1000, 'truncate', 100)).toBe(1000);
    });

    it('should handle negative integers', () => {
      expect(applyTransform(-123, 'truncate', 100)).toBe(-200);
      expect(applyTransform(-99, 'truncate', 100)).toBe(-100);
    });

    it('should handle width of 1', () => {
      expect(applyTransform(123, 'truncate', 1)).toBe(123);
    });

    it('should handle width of 10', () => {
      expect(applyTransform(123, 'truncate', 10)).toBe(120);
      expect(applyTransform(125, 'truncate', 10)).toBe(120);
    });
  });

  it('should return null for null values', () => {
    expect(applyTransform(null, 'truncate', 5)).toBe(null);
  });

  it('should throw without width', () => {
    expect(() => applyTransform('test', 'truncate')).toThrow('requires width');
  });

  it('should parse truncate transform string', () => {
    expect(applyTransform('hello world', 'truncate[5]')).toBe('hello');
  });
});

// ============================================================================
// Temporal Transform Tests
// ============================================================================

describe('Temporal Transforms', () => {
  // Test date: 2024-06-15T14:30:00Z
  const testTimestamp = new Date('2024-06-15T14:30:00Z').getTime();
  const testDate = new Date('2024-06-15T14:30:00Z');

  describe('Year transform', () => {
    it('should extract years since epoch', () => {
      const year = applyTransform(testTimestamp, 'year') as number;
      // 2024 - 1970 = 54
      expect(year).toBe(54);
    });

    it('should handle Date objects', () => {
      const year = applyTransform(testDate, 'year') as number;
      expect(year).toBe(54);
    });

    it('should handle date strings', () => {
      const year = applyTransform('2024-06-15T14:30:00Z', 'year') as number;
      expect(year).toBe(54);
    });

    it('should return null for null values', () => {
      expect(applyTransform(null, 'year')).toBe(null);
    });

    it('should handle epoch date (1970)', () => {
      const epoch = new Date('1970-01-01T00:00:00Z').getTime();
      expect(applyTransform(epoch, 'year')).toBe(0);
    });

    it('should handle pre-epoch dates', () => {
      const date = new Date('1969-06-15T00:00:00Z').getTime();
      expect(applyTransform(date, 'year')).toBe(-1);
    });
  });

  describe('Month transform', () => {
    it('should extract months since epoch', () => {
      const month = applyTransform(testTimestamp, 'month') as number;
      // 54 years * 12 + 5 (June is index 5) = 653
      expect(month).toBe(54 * 12 + 5);
    });

    it('should handle January of epoch year', () => {
      const jan1970 = new Date('1970-01-15T00:00:00Z').getTime();
      expect(applyTransform(jan1970, 'month')).toBe(0);
    });

    it('should handle December of epoch year', () => {
      const dec1970 = new Date('1970-12-15T00:00:00Z').getTime();
      expect(applyTransform(dec1970, 'month')).toBe(11);
    });

    it('should return null for null values', () => {
      expect(applyTransform(null, 'month')).toBe(null);
    });
  });

  describe('Day transform', () => {
    it('should extract days since epoch', () => {
      const day = applyTransform(testTimestamp, 'day') as number;
      expect(typeof day).toBe('number');
      expect(day).toBeGreaterThan(0);
    });

    it('should return 0 for epoch date', () => {
      const epoch = new Date('1970-01-01T00:00:00Z').getTime();
      expect(applyTransform(epoch, 'day')).toBe(0);
    });

    it('should return 1 for second day of epoch', () => {
      const day2 = new Date('1970-01-02T00:00:00Z').getTime();
      expect(applyTransform(day2, 'day')).toBe(1);
    });

    it('should handle any time within a day', () => {
      const morning = new Date('2024-06-15T06:00:00Z').getTime();
      const evening = new Date('2024-06-15T23:59:59Z').getTime();
      expect(applyTransform(morning, 'day')).toBe(applyTransform(evening, 'day'));
    });

    it('should return null for null values', () => {
      expect(applyTransform(null, 'day')).toBe(null);
    });
  });

  describe('Hour transform', () => {
    it('should extract hours since epoch', () => {
      const hour = applyTransform(testTimestamp, 'hour') as number;
      expect(typeof hour).toBe('number');
      expect(hour).toBeGreaterThan(0);
    });

    it('should return 0 for epoch hour', () => {
      const epoch = new Date('1970-01-01T00:00:00Z').getTime();
      expect(applyTransform(epoch, 'hour')).toBe(0);
    });

    it('should return 1 for second hour of epoch', () => {
      const hour2 = new Date('1970-01-01T01:00:00Z').getTime();
      expect(applyTransform(hour2, 'hour')).toBe(1);
    });

    it('should handle any minute within an hour', () => {
      const minute0 = new Date('2024-06-15T14:00:00Z').getTime();
      const minute59 = new Date('2024-06-15T14:59:59Z').getTime();
      expect(applyTransform(minute0, 'hour')).toBe(applyTransform(minute59, 'hour'));
    });

    it('should return null for null values', () => {
      expect(applyTransform(null, 'hour')).toBe(null);
    });
  });
});

// ============================================================================
// Void Transform Tests
// ============================================================================

describe('Void Transform', () => {
  it('should always return null', () => {
    expect(applyTransform('test', 'void')).toBe(null);
    expect(applyTransform(42, 'void')).toBe(null);
    expect(applyTransform(true, 'void')).toBe(null);
    expect(applyTransform({}, 'void')).toBe(null);
    expect(applyTransform(null, 'void')).toBe(null);
  });
});

// ============================================================================
// Transform Result Type Tests
// ============================================================================

describe('getTransformResultType', () => {
  it('should return source type for identity', () => {
    expect(getTransformResultType('string', 'identity')).toBe('string');
    expect(getTransformResultType('long', 'identity')).toBe('long');
  });

  it('should return int for bucket', () => {
    expect(getTransformResultType('string', 'bucket')).toBe('int');
    expect(getTransformResultType('long', 'bucket[16]')).toBe('int');
  });

  it('should return source type for truncate', () => {
    expect(getTransformResultType('string', 'truncate')).toBe('string');
    expect(getTransformResultType('long', 'truncate[100]')).toBe('long');
  });

  it('should return int for temporal transforms', () => {
    expect(getTransformResultType('timestamptz', 'year')).toBe('int');
    expect(getTransformResultType('timestamptz', 'month')).toBe('int');
    expect(getTransformResultType('timestamptz', 'day')).toBe('int');
    expect(getTransformResultType('timestamptz', 'hour')).toBe('int');
  });
});

// ============================================================================
// PartitionSpecBuilder Tests
// ============================================================================

describe('PartitionSpecBuilder', () => {
  let schema: IcebergSchema;

  beforeEach(() => {
    schema = createTestSchema();
  });

  describe('Basic partition field creation', () => {
    it('should create an identity partition field', () => {
      const builder = createPartitionSpecBuilder(schema);
      const spec = builder.identity('region').build();

      expect(spec['spec-id']).toBe(0);
      expect(spec.fields).toHaveLength(1);
      expect(spec.fields[0]['source-id']).toBe(4);
      expect(spec.fields[0]['field-id']).toBe(1000);
      expect(spec.fields[0].name).toBe('region');
      expect(spec.fields[0].transform).toBe('identity');
    });

    it('should create a bucket partition field', () => {
      const builder = createPartitionSpecBuilder(schema);
      const spec = builder.bucket('user_id', 16).build();

      expect(spec.fields).toHaveLength(1);
      expect(spec.fields[0]['source-id']).toBe(5);
      expect(spec.fields[0].name).toBe('user_id_bucket');
      expect(spec.fields[0].transform).toBe('bucket[16]');
    });

    it('should create a truncate partition field', () => {
      const builder = createPartitionSpecBuilder(schema);
      const spec = builder.truncate('name', 5).build();

      expect(spec.fields).toHaveLength(1);
      expect(spec.fields[0]['source-id']).toBe(2);
      expect(spec.fields[0].name).toBe('name_trunc');
      expect(spec.fields[0].transform).toBe('truncate[5]');
    });

    it('should create time-based partition fields', () => {
      const builder = createPartitionSpecBuilder(schema);
      const spec = builder
        .year('created_at')
        .month('created_at', 'created_month')
        .day('created_at', 'created_day')
        .hour('created_at', 'created_hour')
        .build();

      expect(spec.fields).toHaveLength(4);
      expect(spec.fields[0].transform).toBe('year');
      expect(spec.fields[0].name).toBe('created_at_year');
      expect(spec.fields[1].transform).toBe('month');
      expect(spec.fields[1].name).toBe('created_month');
      expect(spec.fields[2].transform).toBe('day');
      expect(spec.fields[2].name).toBe('created_day');
      expect(spec.fields[3].transform).toBe('hour');
      expect(spec.fields[3].name).toBe('created_hour');
    });

    it('should create a void partition field', () => {
      const builder = createPartitionSpecBuilder(schema);
      const spec = builder.void('region').build();

      expect(spec.fields).toHaveLength(1);
      expect(spec.fields[0].transform).toBe('void');
      expect(spec.fields[0].name).toBe('region_void');
    });
  });

  describe('Multiple partition fields', () => {
    it('should create a composite partition spec', () => {
      const builder = createPartitionSpecBuilder(schema);
      const spec = builder.identity('region').day('created_at').bucket('user_id', 8).build();

      expect(spec.fields).toHaveLength(3);
      expect(spec.fields.map((f) => f.name)).toEqual([
        'region',
        'created_at_day',
        'user_id_bucket',
      ]);
    });

    it('should assign sequential field IDs', () => {
      const builder = createPartitionSpecBuilder(schema);
      const spec = builder.identity('region').day('created_at').build();

      expect(spec.fields[0]['field-id']).toBe(1000);
      expect(spec.fields[1]['field-id']).toBe(1001);
    });

    it('should support custom starting field ID', () => {
      const builder = createPartitionSpecBuilder(schema, { startingFieldId: 2000 });
      const spec = builder.identity('region').day('created_at').build();

      expect(spec.fields[0]['field-id']).toBe(2000);
      expect(spec.fields[1]['field-id']).toBe(2001);
    });

    it('should support custom spec ID', () => {
      const builder = createPartitionSpecBuilder(schema, { specId: 5 });
      const spec = builder.identity('region').build();

      expect(spec['spec-id']).toBe(5);
    });
  });

  describe('Custom partition field names', () => {
    it('should use custom name when provided', () => {
      const builder = createPartitionSpecBuilder(schema);
      const spec = builder.identity('region', 'custom_region').build();

      expect(spec.fields[0].name).toBe('custom_region');
    });

    it('should generate default names based on transform', () => {
      const builder = createPartitionSpecBuilder(schema);
      const spec = builder
        .identity('region')
        .bucket('user_id', 16)
        .truncate('name', 5)
        .year('created_at')
        .build();

      expect(spec.fields.map((f) => f.name)).toEqual([
        'region',
        'user_id_bucket',
        'name_trunc',
        'created_at_year',
      ]);
    });
  });

  describe('Error handling', () => {
    it('should throw error for non-existent source field', () => {
      const builder = createPartitionSpecBuilder(schema);

      expect(() => builder.identity('nonexistent')).toThrow(
        "Source field 'nonexistent' not found in schema"
      );
    });

    it('should throw error when bucket transform lacks argument', () => {
      const builder = createPartitionSpecBuilder(schema);

      expect(() => (builder as any).addField('user_id', 'bucket', undefined, undefined)).toThrow(
        "Transform 'bucket' requires a transform argument"
      );
    });

    it('should throw error when truncate transform lacks argument', () => {
      const builder = createPartitionSpecBuilder(schema);

      expect(() => (builder as any).addField('name', 'truncate', undefined, undefined)).toThrow(
        "Transform 'truncate' requires a transform argument"
      );
    });
  });
});

// ============================================================================
// createPartitionSpecFromDefinitions Tests
// ============================================================================

describe('createPartitionSpecFromDefinitions', () => {
  it('should create partition spec from field definitions', () => {
    const schema = createTestSchema();
    const spec = createPartitionSpecFromDefinitions(schema, [
      { sourceField: 'region', transform: 'identity' },
      { sourceField: 'created_at', transform: 'day' },
      { sourceField: 'user_id', transform: 'bucket', transformArg: 16 },
    ]);

    expect(spec.fields).toHaveLength(3);
    expect(spec.fields[0].transform).toBe('identity');
    expect(spec.fields[1].transform).toBe('day');
    expect(spec.fields[2].transform).toBe('bucket[16]');
  });

  it('should support custom names in field definitions', () => {
    const schema = createTestSchema();
    const spec = createPartitionSpecFromDefinitions(schema, [
      { sourceField: 'region', transform: 'identity', name: 'custom_region' },
    ]);

    expect(spec.fields[0].name).toBe('custom_region');
  });
});

// ============================================================================
// Partition Data Extraction Tests
// ============================================================================

describe('getPartitionData', () => {
  it('should compute partition data from a record', () => {
    const schema = createTestSchema();
    const spec = createPartitionSpecFromDefinitions(schema, [
      { sourceField: 'region', transform: 'identity' },
      { sourceField: 'created_at', transform: 'day', name: 'created_at_day' },
    ]);

    const timestamp = new Date('2024-01-15').getTime();
    const record = {
      id: '123',
      name: 'Test',
      created_at: timestamp,
      region: 'us-west',
      user_id: 42,
    };

    const partitionData = getPartitionData(record, spec, schema);

    expect(partitionData.region).toBe('us-west');
    expect(typeof partitionData.created_at_day).toBe('number');
  });

  it('should handle null values', () => {
    const schema = createTestSchema();
    const spec = createPartitionSpecFromDefinitions(schema, [
      { sourceField: 'name', transform: 'truncate', transformArg: 5 },
    ]);

    const record = {
      id: '123',
      name: null,
      created_at: Date.now(),
      region: 'us-west',
      user_id: 42,
    };

    const partitionData = getPartitionData(record, spec, schema);
    expect(partitionData.name_trunc).toBe(null);
  });
});

// ============================================================================
// Partition Path Tests
// ============================================================================

describe('getPartitionPath', () => {
  it('should generate partition path string', () => {
    const schema = createTestSchema();
    const spec = createPartitionSpecFromDefinitions(schema, [
      { sourceField: 'region', transform: 'identity' },
      { sourceField: 'created_at', transform: 'day', name: 'created_at_day' },
    ]);

    const partitionData = { region: 'us-west', created_at_day: 19750 };
    const path = getPartitionPath(partitionData, spec);

    expect(path).toBe('region=us-west/created_at_day=19750');
  });

  it('should handle null values with __HIVE_DEFAULT_PARTITION__', () => {
    const schema = createTestSchema();
    const spec = createPartitionSpecFromDefinitions(schema, [
      { sourceField: 'region', transform: 'identity' },
    ]);

    const partitionData = { region: null };
    const path = getPartitionPath(partitionData, spec);

    expect(path).toBe('region=__HIVE_DEFAULT_PARTITION__');
  });
});

describe('parsePartitionPath', () => {
  it('should parse partition path back to data', () => {
    const data = parsePartitionPath('region=us-west/created_at_day=19750');

    expect(data.region).toBe('us-west');
    expect(data.created_at_day).toBe(19750);
  });

  it('should handle null values', () => {
    const data = parsePartitionPath('region=__HIVE_DEFAULT_PARTITION__');
    expect(data.region).toBe(null);
  });

  it('should handle numeric values', () => {
    const data = parsePartitionPath('year=54/bucket=7');
    expect(data.year).toBe(54);
    expect(data.bucket).toBe(7);
  });
});

// ============================================================================
// PartitionStatsCollector Tests
// ============================================================================

describe('PartitionStatsCollector', () => {
  let schema: IcebergSchema;
  let spec: PartitionSpec;
  let collector: PartitionStatsCollector;

  beforeEach(() => {
    schema = createTestSchema();
    spec = createPartitionSpecFromDefinitions(schema, [
      { sourceField: 'region', transform: 'identity' },
      { sourceField: 'created_at', transform: 'day', name: 'created_at_day' },
    ]);
    collector = createPartitionStatsCollector(spec);
  });

  describe('Adding files', () => {
    it('should track statistics for a single file', () => {
      const file = createTestFile({ region: 'us-west', created_at_day: 19750 }, 1000, 102400);

      collector.addFile(file);

      const stats = collector.getStats();
      expect(stats.partitionCount).toBe(1);
      expect(stats.totalFileCount).toBe(1);
      expect(stats.totalRowCount).toBe(1000);
      expect(stats.totalSizeBytes).toBe(102400);
    });

    it('should aggregate statistics for multiple files in same partition', () => {
      collector.addFile(createTestFile({ region: 'us-west', created_at_day: 19750 }, 1000, 100000));
      collector.addFile(createTestFile({ region: 'us-west', created_at_day: 19750 }, 2000, 200000));

      const stats = collector.getStats();
      expect(stats.partitionCount).toBe(1);
      expect(stats.totalFileCount).toBe(2);
      expect(stats.totalRowCount).toBe(3000);
      expect(stats.totalSizeBytes).toBe(300000);
    });

    it('should track separate statistics for different partitions', () => {
      collector.addFile(createTestFile({ region: 'us-west', created_at_day: 19750 }, 1000, 100000));
      collector.addFile(createTestFile({ region: 'us-east', created_at_day: 19750 }, 2000, 200000));

      const stats = collector.getStats();
      expect(stats.partitionCount).toBe(2);
      expect(stats.totalFileCount).toBe(2);
    });
  });

  describe('Removing files', () => {
    it('should decrement statistics when removing a file', () => {
      const file1 = createTestFile({ region: 'us-west', created_at_day: 19750 }, 1000, 100000);
      const file2 = createTestFile({ region: 'us-west', created_at_day: 19750 }, 2000, 200000);

      collector.addFile(file1);
      collector.addFile(file2);
      collector.removeFile(file1);

      const stats = collector.getStats();
      expect(stats.totalFileCount).toBe(1);
      expect(stats.totalRowCount).toBe(2000);
      expect(stats.totalSizeBytes).toBe(200000);
    });

    it('should remove partition when all files are removed', () => {
      const file = createTestFile({ region: 'us-west', created_at_day: 19750 }, 1000, 100000);

      collector.addFile(file);
      collector.removeFile(file);

      const stats = collector.getStats();
      expect(stats.partitionCount).toBe(0);
    });
  });

  describe('Per-partition statistics', () => {
    it('should return statistics for a specific partition', () => {
      collector.addFile(createTestFile({ region: 'us-west', created_at_day: 19750 }, 1000, 100000));
      collector.addFile(createTestFile({ region: 'us-east', created_at_day: 19750 }, 2000, 200000));

      const partitionStats = collector.getPartitionStats({
        region: 'us-west',
        created_at_day: 19750,
      });

      expect(partitionStats).toBeDefined();
      expect(partitionStats!.fileCount).toBe(1);
      expect(partitionStats!.rowCount).toBe(1000);
    });

    it('should return undefined for non-existent partition', () => {
      const partitionStats = collector.getPartitionStats({
        region: 'nonexistent',
        created_at_day: 19750,
      });

      expect(partitionStats).toBeUndefined();
    });
  });

  describe('Aggregate statistics by field', () => {
    it('should track distinct values per field', () => {
      collector.addFile(createTestFile({ region: 'us-west', created_at_day: 19750 }));
      collector.addFile(createTestFile({ region: 'us-east', created_at_day: 19750 }));
      collector.addFile(createTestFile({ region: 'us-west', created_at_day: 19751 }));

      const stats = collector.getStats();

      expect(stats.byField['region']?.distinctValues).toBe(2);
      expect(stats.byField['created_at_day']?.distinctValues).toBe(2);
    });

    it('should track min/max values per field', () => {
      collector.addFile(createTestFile({ region: 'us-west', created_at_day: 19750 }));
      collector.addFile(createTestFile({ region: 'us-east', created_at_day: 19752 }));
      collector.addFile(createTestFile({ region: 'eu-west', created_at_day: 19749 }));

      const stats = collector.getStats();

      expect(stats.byField['created_at_day']?.minValue).toBe(19749);
      expect(stats.byField['created_at_day']?.maxValue).toBe(19752);
    });
  });

  describe('Utility methods', () => {
    it('should list all partition keys', () => {
      collector.addFile(createTestFile({ region: 'us-west', created_at_day: 19750 }));
      collector.addFile(createTestFile({ region: 'us-east', created_at_day: 19751 }));

      const keys = collector.getPartitionKeys();

      expect(keys).toHaveLength(2);
      expect(keys.every((k) => k.includes('region=') && k.includes('created_at_day='))).toBe(true);
    });

    it('should clear all statistics', () => {
      collector.addFile(createTestFile({ region: 'us-west', created_at_day: 19750 }));
      collector.addFile(createTestFile({ region: 'us-east', created_at_day: 19751 }));

      collector.clear();

      const stats = collector.getStats();
      expect(stats.partitionCount).toBe(0);
      expect(stats.totalFileCount).toBe(0);
    });
  });
});

// ============================================================================
// Partition Spec Evolution Tests
// ============================================================================

describe('Partition Spec Evolution', () => {
  describe('comparePartitionSpecs', () => {
    it('should detect added fields', () => {
      const oldSpec: PartitionSpec = {
        'spec-id': 0,
        fields: [{ 'source-id': 1, 'field-id': 1000, name: 'region', transform: 'identity' }],
      };

      const newSpec: PartitionSpec = {
        'spec-id': 1,
        fields: [
          { 'source-id': 1, 'field-id': 1000, name: 'region', transform: 'identity' },
          { 'source-id': 3, 'field-id': 1001, name: 'created_at_day', transform: 'day' },
        ],
      };

      const result = comparePartitionSpecs(oldSpec, newSpec);

      expect(result.changes).toHaveLength(1);
      expect(result.changes[0].type).toBe('add-field');
      expect(result.changes[0].fieldId).toBe(1001);
    });

    it('should detect removed fields', () => {
      const oldSpec: PartitionSpec = {
        'spec-id': 0,
        fields: [
          { 'source-id': 1, 'field-id': 1000, name: 'region', transform: 'identity' },
          { 'source-id': 3, 'field-id': 1001, name: 'created_at_day', transform: 'day' },
        ],
      };

      const newSpec: PartitionSpec = {
        'spec-id': 1,
        fields: [{ 'source-id': 1, 'field-id': 1000, name: 'region', transform: 'identity' }],
      };

      const result = comparePartitionSpecs(oldSpec, newSpec);

      expect(result.changes).toHaveLength(1);
      expect(result.changes[0].type).toBe('remove-field');
      expect(result.changes[0].fieldId).toBe(1001);
    });

    it('should detect renamed fields', () => {
      const oldSpec: PartitionSpec = {
        'spec-id': 0,
        fields: [{ 'source-id': 1, 'field-id': 1000, name: 'region', transform: 'identity' }],
      };

      const newSpec: PartitionSpec = {
        'spec-id': 1,
        fields: [{ 'source-id': 1, 'field-id': 1000, name: 'region_name', transform: 'identity' }],
      };

      const result = comparePartitionSpecs(oldSpec, newSpec);

      expect(result.changes).toHaveLength(1);
      expect(result.changes[0].type).toBe('rename-field');
      expect(result.changes[0].previousName).toBe('region');
      expect(result.changes[0].fieldName).toBe('region_name');
    });

    it('should detect transform changes', () => {
      const oldSpec: PartitionSpec = {
        'spec-id': 0,
        fields: [{ 'source-id': 3, 'field-id': 1000, name: 'created_at_day', transform: 'day' }],
      };

      const newSpec: PartitionSpec = {
        'spec-id': 1,
        fields: [{ 'source-id': 3, 'field-id': 1000, name: 'created_at_day', transform: 'hour' }],
      };

      const result = comparePartitionSpecs(oldSpec, newSpec);

      expect(result.changes).toHaveLength(1);
      expect(result.changes[0].type).toBe('change-transform');
      expect(result.changes[0].previousTransform).toBe('day');
      expect(result.changes[0].newTransform).toBe('hour');
    });

    it('should report no changes for identical specs', () => {
      const spec: PartitionSpec = {
        'spec-id': 0,
        fields: [{ 'source-id': 1, 'field-id': 1000, name: 'region', transform: 'identity' }],
      };

      const result = comparePartitionSpecs(spec, spec);

      expect(result.changes).toHaveLength(0);
      expect(result.compatible).toBe(true);
    });
  });

  describe('findMaxPartitionFieldId', () => {
    it('should find max field ID', () => {
      const spec: PartitionSpec = {
        'spec-id': 0,
        fields: [
          { 'source-id': 1, 'field-id': 1000, name: 'region', transform: 'identity' },
          { 'source-id': 3, 'field-id': 1005, name: 'created_at_day', transform: 'day' },
          { 'source-id': 5, 'field-id': 1002, name: 'user_bucket', transform: 'bucket[16]' },
        ],
      };

      expect(findMaxPartitionFieldId(spec)).toBe(1005);
    });

    it('should return 999 for empty spec', () => {
      const spec = createUnpartitionedSpec();
      expect(findMaxPartitionFieldId(spec)).toBe(999);
    });
  });

  describe('generatePartitionSpecId', () => {
    it('should generate sequential spec ID', () => {
      const specs: PartitionSpec[] = [
        { 'spec-id': 0, fields: [] },
        { 'spec-id': 1, fields: [] },
        { 'spec-id': 2, fields: [] },
      ];

      expect(generatePartitionSpecId(specs)).toBe(3);
    });

    it('should return 0 for empty list', () => {
      expect(generatePartitionSpecId([])).toBe(0);
    });
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('Partition Integration Tests', () => {
  it('should work end-to-end: spec creation, data extraction, and stats', () => {
    // Step 1: Create schema and partition spec
    const schema = createTestSchema();
    const spec = createPartitionSpecBuilder(schema)
      .identity('region')
      .day('created_at')
      .bucket('user_id', 4)
      .build();

    expect(spec.fields).toHaveLength(3);

    // Step 2: Create test records
    const records = [
      {
        id: '1',
        created_at: new Date('2024-01-15').getTime(),
        region: 'us-west',
        user_id: 100,
      },
      {
        id: '2',
        created_at: new Date('2024-01-15').getTime(),
        region: 'us-west',
        user_id: 200,
      },
      {
        id: '3',
        created_at: new Date('2024-01-16').getTime(),
        region: 'us-east',
        user_id: 100,
      },
      {
        id: '4',
        created_at: new Date('2024-01-16').getTime(),
        region: 'eu-west',
        user_id: 300,
      },
    ];

    // Step 3: Extract partition data for each record
    const partitionedRecords = records.map((record) => ({
      record,
      partitionData: getPartitionData(record, spec, schema),
    }));

    // Verify partition data extraction
    expect(partitionedRecords[0].partitionData.region).toBe('us-west');
    expect(typeof partitionedRecords[0].partitionData.created_at_day).toBe('number');
    expect(typeof partitionedRecords[0].partitionData.user_id_bucket).toBe('number');

    // Step 4: Create data files and collect stats
    const collector = createPartitionStatsCollector(spec);
    const files: PartitionedFile[] = partitionedRecords.map(({ partitionData }) =>
      createTestFile(partitionData, 1000, 100000)
    );

    files.forEach((file) => collector.addFile(file));

    const stats = collector.getStats();
    expect(stats.totalFileCount).toBe(4);
    expect(stats.totalRowCount).toBe(4000);

    // Step 5: Generate partition paths
    const paths = files.map((f) => getPartitionPath(f.partitionData, spec));
    expect(paths.every((p) => p.includes('region='))).toBe(true);
    expect(paths.every((p) => p.includes('created_at_day='))).toBe(true);
    expect(paths.every((p) => p.includes('user_id_bucket='))).toBe(true);
  });
});
