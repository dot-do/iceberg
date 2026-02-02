import { describe, it, expect, beforeEach } from 'vitest';
import {
  createColumnStatsCollector,
  FileStatsCollector,
  encodeFileStats,
  applyStatsToDataFile,
  aggregateColumnStats,
  computePartitionSummaries,
  createZoneMapFromStats,
  canPruneZoneMap,
  getPrimitiveType,
  getComparator,
  estimateValueSize,
  truncateUpperBound,
  type ColumnStatistics,
  type ComputedFileStats,
  createDefaultSchema,
  type IcebergSchema,
  type DataFile,
  ManifestGenerator,
} from '../src/index.js';

// ============================================================================
// Test Schema
// ============================================================================

function createTestSchema(): IcebergSchema {
  return {
    'schema-id': 0,
    type: 'struct',
    fields: [
      { id: 1, name: 'id', required: true, type: 'long' },
      { id: 2, name: 'name', required: false, type: 'string' },
      { id: 3, name: 'amount', required: false, type: 'double' },
      { id: 4, name: 'count', required: false, type: 'int' },
      { id: 5, name: 'active', required: false, type: 'boolean' },
      { id: 6, name: 'created_at', required: false, type: 'timestamptz' },
    ],
  };
}

// ============================================================================
// Column Stats Collector Tests
// ============================================================================

describe('createColumnStatsCollector', () => {
  it('should collect statistics for integer values', () => {
    const collector = createColumnStatsCollector(1, 'int');

    collector.add(10);
    collector.add(5);
    collector.add(20);
    collector.add(null);
    collector.add(15);

    const stats = collector.getStats();
    expect(stats.fieldId).toBe(1);
    expect(stats.valueCount).toBe(5);
    expect(stats.nullCount).toBe(1);
    expect(stats.lowerBound).toBe(5);
    expect(stats.upperBound).toBe(20);
  });

  it('should collect statistics for string values', () => {
    const collector = createColumnStatsCollector(2, 'string');

    collector.add('apple');
    collector.add('banana');
    collector.add('cherry');
    collector.add(null);

    const stats = collector.getStats();
    expect(stats.fieldId).toBe(2);
    expect(stats.valueCount).toBe(4);
    expect(stats.nullCount).toBe(1);
    expect(stats.lowerBound).toBe('apple');
    expect(stats.upperBound).toBe('cherry');
  });

  it('should collect statistics for floating point values with NaN', () => {
    const collector = createColumnStatsCollector(3, 'double');

    collector.add(1.5);
    collector.add(NaN);
    collector.add(3.14);
    collector.add(null);
    collector.add(2.7);
    collector.add(NaN);

    const stats = collector.getStats();
    expect(stats.fieldId).toBe(3);
    expect(stats.valueCount).toBe(6);
    expect(stats.nullCount).toBe(1);
    expect(stats.nanCount).toBe(2);
    expect(stats.lowerBound).toBe(1.5);
    expect(stats.upperBound).toBe(3.14);
  });

  it('should handle all null values', () => {
    const collector = createColumnStatsCollector(1, 'int');

    collector.add(null);
    collector.add(undefined);
    collector.add(null);

    const stats = collector.getStats();
    expect(stats.valueCount).toBe(3);
    expect(stats.nullCount).toBe(3);
    expect(stats.lowerBound).toBeUndefined();
    expect(stats.upperBound).toBeUndefined();
  });

  it('should truncate long strings', () => {
    const collector = createColumnStatsCollector(2, 'string', 8);

    collector.add('verylongstringvalue');
    collector.add('short');

    const stats = collector.getStats();
    expect(stats.lowerBound).toBe('short');
    // Upper bound should be truncated and incremented
    expect((stats.upperBound as string).length).toBeLessThanOrEqual(8);
  });

  it('should reset collector state', () => {
    const collector = createColumnStatsCollector(1, 'int');

    collector.add(10);
    collector.add(20);
    collector.reset();

    const stats = collector.getStats();
    expect(stats.valueCount).toBe(0);
    expect(stats.nullCount).toBe(0);
    expect(stats.lowerBound).toBeUndefined();
    expect(stats.upperBound).toBeUndefined();
  });

  it('should collect boolean statistics', () => {
    const collector = createColumnStatsCollector(5, 'boolean');

    collector.add(true);
    collector.add(false);
    collector.add(true);

    const stats = collector.getStats();
    expect(stats.valueCount).toBe(3);
    expect(stats.lowerBound).toBe(false);
    expect(stats.upperBound).toBe(true);
  });
});

// ============================================================================
// FileStatsCollector Tests
// ============================================================================

describe('FileStatsCollector', () => {
  let schema: IcebergSchema;
  let collector: FileStatsCollector;

  beforeEach(() => {
    schema = createTestSchema();
    collector = new FileStatsCollector({ schema });
  });

  it('should collect stats from row objects', () => {
    collector.addRow({ id: 1, name: 'Alice', amount: 100.5, count: 10 });
    collector.addRow({ id: 2, name: 'Bob', amount: 200.0, count: 20 });
    collector.addRow({ id: 3, name: null, amount: 150.0, count: 15 });

    const stats = collector.getStats();
    expect(stats.length).toBeGreaterThan(0);

    const idStats = stats.find((s) => s.fieldId === 1);
    expect(idStats).toBeDefined();
    expect(idStats?.valueCount).toBe(3);
    expect(idStats?.lowerBound).toBe(1);
    expect(idStats?.upperBound).toBe(3);

    const nameStats = stats.find((s) => s.fieldId === 2);
    expect(nameStats?.nullCount).toBe(1);
  });

  it('should add values by field ID', () => {
    collector.addValue(1, 100);
    collector.addValue(1, 200);
    collector.addValue(1, 50);

    const stats = collector.getStats();
    const idStats = stats.find((s) => s.fieldId === 1);
    expect(idStats?.lowerBound).toBe(50);
    expect(idStats?.upperBound).toBe(200);
  });

  it('should respect includeFieldIds option', () => {
    const limitedCollector = new FileStatsCollector({
      schema,
      includeFieldIds: [1, 2],
    });

    limitedCollector.addRow({ id: 1, name: 'Test', amount: 100, count: 10 });

    const stats = limitedCollector.getStats();
    const fieldIds = stats.map((s) => s.fieldId);
    expect(fieldIds).toContain(1);
    expect(fieldIds).toContain(2);
    expect(fieldIds).not.toContain(3);
    expect(fieldIds).not.toContain(4);
  });

  it('should respect excludeFieldIds option', () => {
    const limitedCollector = new FileStatsCollector({
      schema,
      excludeFieldIds: [3, 4],
    });

    limitedCollector.addRow({ id: 1, name: 'Test', amount: 100, count: 10 });

    const stats = limitedCollector.getStats();
    const fieldIds = stats.map((s) => s.fieldId);
    expect(fieldIds).toContain(1);
    expect(fieldIds).toContain(2);
    expect(fieldIds).not.toContain(3);
    expect(fieldIds).not.toContain(4);
  });

  it('should produce encoded stats', () => {
    collector.addRow({ id: 1, name: 'Alice', amount: 100.5 });
    collector.addRow({ id: 2, name: 'Bob', amount: 200.0 });

    const encodedStats = collector.getEncodedStats();
    expect(encodedStats.valueCounts).toBeDefined();
    expect(encodedStats.lowerBounds).toBeDefined();
    expect(encodedStats.upperBounds).toBeDefined();

    // Check that bounds are Uint8Array
    for (const bound of Object.values(encodedStats.lowerBounds)) {
      expect(bound).toBeInstanceOf(Uint8Array);
    }
  });

  it('should reset all collectors', () => {
    collector.addRow({ id: 1, name: 'Test' });
    collector.reset();

    const stats = collector.getStats();
    for (const stat of stats) {
      expect(stat.valueCount).toBe(0);
    }
  });
});

// ============================================================================
// encodeFileStats Tests
// ============================================================================

describe('encodeFileStats', () => {
  it('should encode column statistics to binary format', () => {
    const schema = createTestSchema();
    const stats: ColumnStatistics[] = [
      { fieldId: 1, valueCount: 100, nullCount: 5, lowerBound: 1, upperBound: 1000 },
      { fieldId: 2, valueCount: 100, nullCount: 10, lowerBound: 'aaa', upperBound: 'zzz' },
    ];

    const encoded = encodeFileStats(stats, schema);

    expect(encoded.valueCounts[1]).toBe(100);
    expect(encoded.valueCounts[2]).toBe(100);
    expect(encoded.nullValueCounts[1]).toBe(5);
    expect(encoded.nullValueCounts[2]).toBe(10);
    expect(encoded.lowerBounds[1]).toBeInstanceOf(Uint8Array);
    expect(encoded.lowerBounds[2]).toBeInstanceOf(Uint8Array);
    expect(encoded.upperBounds[1]).toBeInstanceOf(Uint8Array);
    expect(encoded.upperBounds[2]).toBeInstanceOf(Uint8Array);
  });

  it('should encode long values correctly', () => {
    const schema = createTestSchema();
    const stats: ColumnStatistics[] = [
      { fieldId: 1, valueCount: 10, lowerBound: 12345, upperBound: 67890 },
    ];

    const encoded = encodeFileStats(stats, schema);

    // Long values are 8 bytes
    expect(encoded.lowerBounds[1].length).toBe(8);
    expect(encoded.upperBounds[1].length).toBe(8);

    // Decode and verify (little-endian)
    const view = new DataView(encoded.lowerBounds[1].buffer);
    expect(view.getBigInt64(0, true)).toBe(BigInt(12345));
  });

  it('should encode string values as UTF-8', () => {
    const schema = createTestSchema();
    const stats: ColumnStatistics[] = [
      { fieldId: 2, valueCount: 10, lowerBound: 'hello', upperBound: 'world' },
    ];

    const encoded = encodeFileStats(stats, schema);

    const decoder = new TextDecoder();
    expect(decoder.decode(encoded.lowerBounds[2])).toBe('hello');
    expect(decoder.decode(encoded.upperBounds[2])).toBe('world');
  });

  it('should include NaN counts for floating point columns', () => {
    const schema = createTestSchema();
    const stats: ColumnStatistics[] = [
      { fieldId: 3, valueCount: 100, nullCount: 5, nanCount: 3, lowerBound: 1.0, upperBound: 100.0 },
    ];

    const encoded = encodeFileStats(stats, schema);
    expect(encoded.nanValueCounts[3]).toBe(3);
  });

  it('should not include empty counts', () => {
    const schema = createTestSchema();
    const stats: ColumnStatistics[] = [
      { fieldId: 1, valueCount: 0, nullCount: 0 },
    ];

    const encoded = encodeFileStats(stats, schema);
    expect(encoded.valueCounts[1]).toBeUndefined();
    expect(encoded.nullValueCounts[1]).toBe(0);
  });
});

// ============================================================================
// applyStatsToDataFile Tests
// ============================================================================

describe('applyStatsToDataFile', () => {
  it('should apply computed stats to a data file', () => {
    const dataFile: DataFile = {
      content: 0,
      'file-path': 's3://bucket/data/file.parquet',
      'file-format': 'parquet',
      partition: {},
      'record-count': 1000,
      'file-size-in-bytes': 4096,
    };

    const stats: ComputedFileStats = {
      valueCounts: { 1: 1000, 2: 950 },
      nullValueCounts: { 1: 0, 2: 50 },
      nanValueCounts: {},
      columnSizes: { 1: 8000, 2: 2000 },
      lowerBounds: { 1: new Uint8Array([1, 0, 0, 0, 0, 0, 0, 0]) },
      upperBounds: { 1: new Uint8Array([232, 3, 0, 0, 0, 0, 0, 0]) },
    };

    const result = applyStatsToDataFile(dataFile, stats);

    expect(result['value-counts']).toEqual({ 1: 1000, 2: 950 });
    expect(result['null-value-counts']).toEqual({ 1: 0, 2: 50 });
    expect(result['column-sizes']).toEqual({ 1: 8000, 2: 2000 });
    expect(result['lower-bounds']).toBeDefined();
    expect(result['upper-bounds']).toBeDefined();
  });

  it('should not include empty stat objects', () => {
    const dataFile: DataFile = {
      content: 0,
      'file-path': 's3://bucket/data/file.parquet',
      'file-format': 'parquet',
      partition: {},
      'record-count': 1000,
      'file-size-in-bytes': 4096,
    };

    const stats: ComputedFileStats = {
      valueCounts: {},
      nullValueCounts: {},
      nanValueCounts: {},
      columnSizes: {},
      lowerBounds: {},
      upperBounds: {},
    };

    const result = applyStatsToDataFile(dataFile, stats);

    expect(result['value-counts']).toBeUndefined();
    expect(result['null-value-counts']).toBeUndefined();
    expect(result['nan-value-counts']).toBeUndefined();
    expect(result['column-sizes']).toBeUndefined();
  });
});

// ============================================================================
// aggregateColumnStats Tests
// ============================================================================

describe('aggregateColumnStats', () => {
  it('should aggregate stats across multiple files', () => {
    const schema = createTestSchema();
    const file1Stats: ColumnStatistics[] = [
      { fieldId: 1, valueCount: 100, nullCount: 5, lowerBound: 1, upperBound: 500 },
      { fieldId: 2, valueCount: 100, nullCount: 10, lowerBound: 'apple', upperBound: 'mango' },
    ];
    const file2Stats: ColumnStatistics[] = [
      { fieldId: 1, valueCount: 200, nullCount: 10, lowerBound: 100, upperBound: 1000 },
      { fieldId: 2, valueCount: 200, nullCount: 5, lowerBound: 'banana', upperBound: 'zebra' },
    ];

    const aggregated = aggregateColumnStats([file1Stats, file2Stats], schema);

    const idStats = aggregated.find((s) => s.fieldId === 1);
    expect(idStats?.valueCount).toBe(300);
    expect(idStats?.nullCount).toBe(15);
    expect(idStats?.lowerBound).toBe(1);
    expect(idStats?.upperBound).toBe(1000);

    const nameStats = aggregated.find((s) => s.fieldId === 2);
    expect(nameStats?.valueCount).toBe(300);
    expect(nameStats?.nullCount).toBe(15);
    expect(nameStats?.lowerBound).toBe('apple');
    expect(nameStats?.upperBound).toBe('zebra');
  });

  it('should aggregate NaN counts', () => {
    const schema = createTestSchema();
    const file1Stats: ColumnStatistics[] = [
      { fieldId: 3, valueCount: 100, nanCount: 5, lowerBound: 1.0, upperBound: 100.0 },
    ];
    const file2Stats: ColumnStatistics[] = [
      { fieldId: 3, valueCount: 100, nanCount: 3, lowerBound: 50.0, upperBound: 200.0 },
    ];

    const aggregated = aggregateColumnStats([file1Stats, file2Stats], schema);
    const amountStats = aggregated.find((s) => s.fieldId === 3);

    expect(amountStats?.nanCount).toBe(8);
    expect(amountStats?.lowerBound).toBe(1.0);
    expect(amountStats?.upperBound).toBe(200.0);
  });

  it('should handle files with different column sets', () => {
    const schema = createTestSchema();
    const file1Stats: ColumnStatistics[] = [
      { fieldId: 1, valueCount: 100, lowerBound: 1, upperBound: 100 },
    ];
    const file2Stats: ColumnStatistics[] = [
      { fieldId: 2, valueCount: 100, lowerBound: 'a', upperBound: 'z' },
    ];

    const aggregated = aggregateColumnStats([file1Stats, file2Stats], schema);

    expect(aggregated.length).toBe(2);
    expect(aggregated.find((s) => s.fieldId === 1)).toBeDefined();
    expect(aggregated.find((s) => s.fieldId === 2)).toBeDefined();
  });
});

// ============================================================================
// computePartitionSummaries Tests
// ============================================================================

describe('computePartitionSummaries', () => {
  it('should compute partition summaries', () => {
    const partitionValues = [
      { date: '2024-01-01', region: 'us-east' },
      { date: '2024-01-02', region: 'us-west' },
      { date: '2024-01-03', region: 'us-east' },
    ];

    const summaries = computePartitionSummaries(partitionValues, {
      date: 'string',
      region: 'string',
    });

    expect(summaries.length).toBe(2);

    const dateSummary = summaries[0];
    expect(dateSummary['contains-null']).toBe(false);
    expect(dateSummary['lower-bound']).toBeDefined();
    expect(dateSummary['upper-bound']).toBeDefined();
  });

  it('should detect null values in partitions', () => {
    const partitionValues = [
      { date: '2024-01-01' },
      { date: null },
      { date: '2024-01-03' },
    ];

    const summaries = computePartitionSummaries(partitionValues, {
      date: 'string',
    });

    expect(summaries[0]['contains-null']).toBe(true);
  });

  it('should detect NaN values in floating point partitions', () => {
    const partitionValues = [
      { score: 0.5 },
      { score: NaN },
      { score: 0.8 },
    ];

    const summaries = computePartitionSummaries(partitionValues, {
      score: 'double',
    });

    expect(summaries[0]['contains-nan']).toBe(true);
  });
});

// ============================================================================
// Zone Map Tests
// ============================================================================

describe('Zone Map', () => {
  describe('createZoneMapFromStats', () => {
    it('should create a zone map from column statistics', () => {
      const stats: ColumnStatistics[] = [
        { fieldId: 1, valueCount: 1000, nullCount: 10, lowerBound: 1, upperBound: 1000 },
        { fieldId: 2, valueCount: 1000, nullCount: 50, lowerBound: 'apple', upperBound: 'zebra' },
      ];

      const zoneMap = createZoneMapFromStats(stats);

      expect(zoneMap.recordCount).toBe(1000);
      expect(zoneMap.bounds.get(1)).toEqual({ min: 1, max: 1000 });
      expect(zoneMap.bounds.get(2)).toEqual({ min: 'apple', max: 'zebra' });
      expect(zoneMap.nullCounts.get(1)).toBe(10);
      expect(zoneMap.nullCounts.get(2)).toBe(50);
    });
  });

  describe('canPruneZoneMap', () => {
    const stats: ColumnStatistics[] = [
      { fieldId: 1, valueCount: 1000, lowerBound: 100, upperBound: 500 },
    ];
    const zoneMap = createZoneMapFromStats(stats);

    it('should prune when equality value is outside bounds', () => {
      expect(canPruneZoneMap(zoneMap, 1, '=', 50, 'int')).toBe(true);
      expect(canPruneZoneMap(zoneMap, 1, '=', 600, 'int')).toBe(true);
      expect(canPruneZoneMap(zoneMap, 1, '=', 300, 'int')).toBe(false);
    });

    it('should prune for less than when min >= value', () => {
      expect(canPruneZoneMap(zoneMap, 1, '<', 50, 'int')).toBe(true);
      expect(canPruneZoneMap(zoneMap, 1, '<', 100, 'int')).toBe(true);
      expect(canPruneZoneMap(zoneMap, 1, '<', 150, 'int')).toBe(false);
    });

    it('should prune for less than or equal when min > value', () => {
      expect(canPruneZoneMap(zoneMap, 1, '<=', 50, 'int')).toBe(true);
      expect(canPruneZoneMap(zoneMap, 1, '<=', 99, 'int')).toBe(true);
      expect(canPruneZoneMap(zoneMap, 1, '<=', 100, 'int')).toBe(false);
    });

    it('should prune for greater than when max <= value', () => {
      expect(canPruneZoneMap(zoneMap, 1, '>', 600, 'int')).toBe(true);
      expect(canPruneZoneMap(zoneMap, 1, '>', 500, 'int')).toBe(true);
      expect(canPruneZoneMap(zoneMap, 1, '>', 400, 'int')).toBe(false);
    });

    it('should prune for greater than or equal when max < value', () => {
      expect(canPruneZoneMap(zoneMap, 1, '>=', 600, 'int')).toBe(true);
      expect(canPruneZoneMap(zoneMap, 1, '>=', 501, 'int')).toBe(true);
      expect(canPruneZoneMap(zoneMap, 1, '>=', 500, 'int')).toBe(false);
    });

    it('should not prune when bounds are missing', () => {
      const emptyZoneMap = createZoneMapFromStats([]);
      expect(canPruneZoneMap(emptyZoneMap, 1, '=', 100, 'int')).toBe(false);
    });
  });
});

// ============================================================================
// Utility Function Tests
// ============================================================================

describe('Utility Functions', () => {
  describe('getPrimitiveType', () => {
    it('should return primitive type for string types', () => {
      expect(getPrimitiveType('int')).toBe('int');
      expect(getPrimitiveType('long')).toBe('long');
      expect(getPrimitiveType('string')).toBe('string');
    });

    it('should return undefined for complex types', () => {
      expect(getPrimitiveType({ type: 'list', 'element-id': 1, element: 'int', 'element-required': false })).toBeUndefined();
      expect(getPrimitiveType({ type: 'struct', fields: [] })).toBeUndefined();
    });
  });

  describe('getComparator', () => {
    it('should compare integers correctly', () => {
      const compare = getComparator('int');
      expect(compare(5, 10)).toBeLessThan(0);
      expect(compare(10, 5)).toBeGreaterThan(0);
      expect(compare(5, 5)).toBe(0);
    });

    it('should compare strings correctly', () => {
      const compare = getComparator('string');
      expect(compare('apple', 'banana')).toBeLessThan(0);
      expect(compare('banana', 'apple')).toBeGreaterThan(0);
      expect(compare('apple', 'apple')).toBe(0);
    });

    it('should compare booleans correctly', () => {
      const compare = getComparator('boolean');
      expect(compare(false, true)).toBeLessThan(0);
      expect(compare(true, false)).toBeGreaterThan(0);
      expect(compare(true, true)).toBe(0);
    });

    it('should compare doubles correctly', () => {
      const compare = getComparator('double');
      expect(compare(1.5, 2.5)).toBeLessThan(0);
      expect(compare(2.5, 1.5)).toBeGreaterThan(0);
      expect(compare(1.5, 1.5)).toBe(0);
    });
  });

  describe('estimateValueSize', () => {
    it('should estimate size for primitive types', () => {
      expect(estimateValueSize(true, 'boolean')).toBe(1);
      expect(estimateValueSize(42, 'int')).toBe(4);
      expect(estimateValueSize(42, 'long')).toBe(8);
      expect(estimateValueSize(3.14, 'float')).toBe(4);
      expect(estimateValueSize(3.14, 'double')).toBe(8);
    });

    it('should estimate size for strings', () => {
      expect(estimateValueSize('hello', 'string')).toBe(5);
      expect(estimateValueSize('', 'string')).toBe(0);
    });

    it('should estimate size for binary', () => {
      const data = new Uint8Array([1, 2, 3, 4, 5]);
      expect(estimateValueSize(data, 'binary')).toBe(5);
    });
  });

  describe('truncateUpperBound', () => {
    it('should not truncate short strings', () => {
      expect(truncateUpperBound('hello', 10)).toBe('hello');
    });

    it('should truncate and increment long strings', () => {
      const result = truncateUpperBound('abcdefghij', 5);
      expect(result.length).toBeLessThanOrEqual(5);
      // The result should be >= the truncated prefix
      expect(result >= 'abcde').toBe(true);
    });

    it('should handle strings at max length', () => {
      expect(truncateUpperBound('abcde', 5)).toBe('abcde');
    });
  });
});

// ============================================================================
// ManifestGenerator Integration Tests
// ============================================================================

describe('ManifestGenerator with Stats', () => {
  it('should add data file with pre-computed stats', () => {
    const manifest = new ManifestGenerator({
      sequenceNumber: 1,
      snapshotId: 123456789,
    });

    const stats: ComputedFileStats = {
      valueCounts: { 1: 1000, 2: 950 },
      nullValueCounts: { 1: 0, 2: 50 },
      nanValueCounts: {},
      columnSizes: { 1: 8000, 2: 2000 },
      lowerBounds: { 1: new Uint8Array([1, 0, 0, 0, 0, 0, 0, 0]) },
      upperBounds: { 1: new Uint8Array([232, 3, 0, 0, 0, 0, 0, 0]) },
    };

    manifest.addDataFileWithStats(
      {
        'file-path': 's3://bucket/data/file.parquet',
        'file-format': 'parquet',
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 10000,
      },
      stats
    );

    const result = manifest.generate();
    expect(result.entries.length).toBe(1);

    const dataFile = result.entries[0]['data-file'];
    expect(dataFile['value-counts']).toEqual({ 1: 1000, 2: 950 });
    expect(dataFile['null-value-counts']).toEqual({ 1: 0, 2: 50 });
    expect(dataFile['column-sizes']).toEqual({ 1: 8000, 2: 2000 });
    expect(dataFile['lower-bounds']).toBeDefined();
    expect(dataFile['upper-bounds']).toBeDefined();
  });

  it('should work with FileStatsCollector end-to-end', () => {
    const schema = createTestSchema();
    const collector = new FileStatsCollector({ schema });

    // Simulate adding rows
    for (let i = 0; i < 1000; i++) {
      collector.addRow({
        id: i + 1,
        name: `user_${i}`,
        amount: Math.random() * 1000,
        count: Math.floor(Math.random() * 100),
      });
    }

    const encodedStats = collector.getEncodedStats();

    const manifest = new ManifestGenerator({
      sequenceNumber: 1,
      snapshotId: Date.now(),
    });

    manifest.addDataFileWithStats(
      {
        'file-path': 's3://bucket/data/users.parquet',
        'file-format': 'parquet',
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 50000,
      },
      encodedStats
    );

    const result = manifest.generate();
    const dataFile = result.entries[0]['data-file'];

    // Verify stats are present
    expect(dataFile['value-counts']).toBeDefined();
    expect(dataFile['value-counts']?.[1]).toBe(1000);
    expect(dataFile['lower-bounds']?.[1]).toBeDefined();
    expect(dataFile['upper-bounds']?.[1]).toBeDefined();
  });
});
