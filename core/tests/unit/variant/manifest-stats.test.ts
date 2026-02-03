/**
 * Tests for Shredded Column Statistics in Manifests
 *
 * These tests verify that DataFile statistics can include shredded variant paths,
 * enabling efficient predicate pushdown for variant columns.
 *
 * @see https://iceberg.apache.org/spec/#variant-shredding
 */

import { describe, it, expect } from 'vitest';
import type {
  VariantShredPropertyConfig,
  ShreddedColumnStats,
} from '../../../src/index.js';
import {
  getShreddedStatisticsPaths,
  assignShreddedFieldIds,
  serializeShreddedBound,
  deserializeShreddedBound,
  mergeShreddedStats,
  createShreddedColumnStats,
  applyShreddedStatsToDataFile,
} from '../../../src/index.js';

// Alias for readability
type VariantShredConfig = VariantShredPropertyConfig;

describe('Shredded Column Statistics in Manifests', () => {
  // ==========================================================================
  // Statistics Path Extraction Tests
  // ==========================================================================

  describe('getShreddedStatisticsPaths', () => {
    it('should return all typed_value paths from a single config', () => {
      const configs: readonly VariantShredConfig[] = [
        {
          columnName: '$data',
          fields: ['title', 'year', 'rating'],
          fieldTypes: { title: 'string', year: 'int', rating: 'double' },
        },
      ];

      const paths = getShreddedStatisticsPaths(configs);

      expect(paths).toEqual([
        '$data.typed_value.title.typed_value',
        '$data.typed_value.year.typed_value',
        '$data.typed_value.rating.typed_value',
      ]);
    });

    it('should return paths from multiple shredded fields', () => {
      const configs: readonly VariantShredConfig[] = [
        {
          columnName: '$data',
          fields: ['name', 'age', 'email', 'phone'],
          fieldTypes: { name: 'string', age: 'int', email: 'string', phone: 'string' },
        },
      ];

      const paths = getShreddedStatisticsPaths(configs);

      expect(paths).toHaveLength(4);
      expect(paths).toContain('$data.typed_value.name.typed_value');
      expect(paths).toContain('$data.typed_value.age.typed_value');
      expect(paths).toContain('$data.typed_value.email.typed_value');
      expect(paths).toContain('$data.typed_value.phone.typed_value');
    });

    it('should return paths from multiple variant columns', () => {
      const configs: readonly VariantShredConfig[] = [
        {
          columnName: '$data',
          fields: ['title'],
          fieldTypes: { title: 'string' },
        },
        {
          columnName: '$index',
          fields: ['key', 'value'],
          fieldTypes: { key: 'string', value: 'long' },
        },
      ];

      const paths = getShreddedStatisticsPaths(configs);

      expect(paths).toEqual([
        '$data.typed_value.title.typed_value',
        '$index.typed_value.key.typed_value',
        '$index.typed_value.value.typed_value',
      ]);
    });

    it('should return empty array for empty configs', () => {
      const paths = getShreddedStatisticsPaths([]);
      expect(paths).toEqual([]);
    });

    it('should return empty array for config with no fields', () => {
      const configs: readonly VariantShredConfig[] = [
        {
          columnName: '$data',
          fields: [],
          fieldTypes: {},
        },
      ];

      const paths = getShreddedStatisticsPaths(configs);
      expect(paths).toEqual([]);
    });
  });

  // ==========================================================================
  // Statistics Field ID Mapping Tests
  // ==========================================================================

  describe('assignShreddedFieldIds', () => {
    it('should assign unique field IDs to each shredded path', () => {
      const configs: readonly VariantShredConfig[] = [
        {
          columnName: '$data',
          fields: ['title', 'year'],
          fieldTypes: { title: 'string', year: 'int' },
        },
      ];

      const fieldIdMap = assignShreddedFieldIds(configs, 1000);

      expect(fieldIdMap.get('$data.typed_value.title.typed_value')).toBe(1000);
      expect(fieldIdMap.get('$data.typed_value.year.typed_value')).toBe(1001);
    });

    it('should generate unique field IDs for shredded columns across configs', () => {
      const configs: readonly VariantShredConfig[] = [
        {
          columnName: '$data',
          fields: ['a'],
          fieldTypes: { a: 'string' },
        },
        {
          columnName: '$index',
          fields: ['b', 'c'],
          fieldTypes: { b: 'string', c: 'int' },
        },
      ];

      const fieldIdMap = assignShreddedFieldIds(configs, 500);

      expect(fieldIdMap.get('$data.typed_value.a.typed_value')).toBe(500);
      expect(fieldIdMap.get('$index.typed_value.b.typed_value')).toBe(501);
      expect(fieldIdMap.get('$index.typed_value.c.typed_value')).toBe(502);
    });

    it('should maintain consistent field IDs across calls with same starting ID', () => {
      const configs: readonly VariantShredConfig[] = [
        {
          columnName: '$data',
          fields: ['x', 'y'],
          fieldTypes: { x: 'long', y: 'double' },
        },
      ];

      const map1 = assignShreddedFieldIds(configs, 100);
      const map2 = assignShreddedFieldIds(configs, 100);

      expect(map1).toEqual(map2);
    });

    it('should return empty map for empty configs', () => {
      const fieldIdMap = assignShreddedFieldIds([], 1000);
      expect(fieldIdMap.size).toBe(0);
    });

    it('should respect the starting ID parameter', () => {
      const configs: readonly VariantShredConfig[] = [
        {
          columnName: '$data',
          fields: ['field1'],
          fieldTypes: { field1: 'string' },
        },
      ];

      const map1 = assignShreddedFieldIds(configs, 0);
      const map2 = assignShreddedFieldIds(configs, 5000);

      expect(map1.get('$data.typed_value.field1.typed_value')).toBe(0);
      expect(map2.get('$data.typed_value.field1.typed_value')).toBe(5000);
    });
  });

  // ==========================================================================
  // Bounds Serialization Tests
  // ==========================================================================

  describe('serializeShreddedBound', () => {
    it('should serialize string bounds using UTF-8 encoding', () => {
      const result = serializeShreddedBound('hello', 'string');

      expect(result).toBeInstanceOf(Uint8Array);
      expect(new TextDecoder().decode(result)).toBe('hello');
    });

    it('should serialize int bounds using 4-byte little-endian', () => {
      const result = serializeShreddedBound(42, 'int');

      expect(result).toBeInstanceOf(Uint8Array);
      expect(result.length).toBe(4);
      // 42 in little-endian: 0x2A 0x00 0x00 0x00
      expect(result[0]).toBe(42);
      expect(result[1]).toBe(0);
      expect(result[2]).toBe(0);
      expect(result[3]).toBe(0);
    });

    it('should serialize long bounds using 8-byte little-endian', () => {
      const result = serializeShreddedBound(BigInt(1000000), 'long');

      expect(result).toBeInstanceOf(Uint8Array);
      expect(result.length).toBe(8);
    });

    it('should serialize double bounds using IEEE 754 encoding', () => {
      const result = serializeShreddedBound(3.14, 'double');

      expect(result).toBeInstanceOf(Uint8Array);
      expect(result.length).toBe(8);
    });

    it('should serialize float bounds using IEEE 754 encoding', () => {
      const result = serializeShreddedBound(2.5, 'float');

      expect(result).toBeInstanceOf(Uint8Array);
      expect(result.length).toBe(4);
    });

    it('should serialize boolean bounds', () => {
      const trueResult = serializeShreddedBound(true, 'boolean');
      const falseResult = serializeShreddedBound(false, 'boolean');

      expect(trueResult.length).toBe(1);
      expect(trueResult[0]).toBe(1);
      expect(falseResult[0]).toBe(0);
    });

    it('should serialize timestamp bounds using long encoding (microseconds)', () => {
      const timestamp = Date.now() * 1000; // microseconds
      const result = serializeShreddedBound(BigInt(timestamp), 'timestamp');

      expect(result).toBeInstanceOf(Uint8Array);
      expect(result.length).toBe(8);
    });

    it('should serialize date bounds as days since epoch', () => {
      const days = 19000; // days since 1970-01-01
      const result = serializeShreddedBound(days, 'date');

      expect(result).toBeInstanceOf(Uint8Array);
      expect(result.length).toBe(4);
    });
  });

  // ==========================================================================
  // Bounds Deserialization Tests
  // ==========================================================================

  describe('deserializeShreddedBound', () => {
    it('should deserialize string bounds correctly', () => {
      const original = 'test string';
      const encoded = new TextEncoder().encode(original);

      const result = deserializeShreddedBound(encoded, 'string');

      expect(result).toBe(original);
    });

    it('should deserialize int bounds correctly', () => {
      const bytes = new Uint8Array([42, 0, 0, 0]);

      const result = deserializeShreddedBound(bytes, 'int');

      expect(result).toBe(42);
    });

    it('should deserialize long bounds correctly', () => {
      // 1000000 in little-endian 8 bytes
      const bytes = new Uint8Array([64, 66, 15, 0, 0, 0, 0, 0]);

      const result = deserializeShreddedBound(bytes, 'long');

      expect(result).toBe(BigInt(1000000));
    });

    it('should deserialize double bounds correctly', () => {
      const original = 3.14;
      const buffer = new ArrayBuffer(8);
      new DataView(buffer).setFloat64(0, original, true);
      const bytes = new Uint8Array(buffer);

      const result = deserializeShreddedBound(bytes, 'double');

      expect(result).toBeCloseTo(original, 10);
    });

    it('should deserialize boolean bounds correctly', () => {
      const trueBytes = new Uint8Array([1]);
      const falseBytes = new Uint8Array([0]);

      expect(deserializeShreddedBound(trueBytes, 'boolean')).toBe(true);
      expect(deserializeShreddedBound(falseBytes, 'boolean')).toBe(false);
    });

    it('should round-trip string values', () => {
      const original = 'hello world';
      const serialized = serializeShreddedBound(original, 'string');
      const deserialized = deserializeShreddedBound(serialized, 'string');

      expect(deserialized).toBe(original);
    });

    it('should round-trip int values', () => {
      const original = 12345;
      const serialized = serializeShreddedBound(original, 'int');
      const deserialized = deserializeShreddedBound(serialized, 'int');

      expect(deserialized).toBe(original);
    });

    it('should round-trip long values', () => {
      const original = BigInt('9007199254740992');
      const serialized = serializeShreddedBound(original, 'long');
      const deserialized = deserializeShreddedBound(serialized, 'long');

      expect(deserialized).toBe(original);
    });

    it('should round-trip double values', () => {
      const original = 123.456789;
      const serialized = serializeShreddedBound(original, 'double');
      const deserialized = deserializeShreddedBound(serialized, 'double');

      expect(deserialized).toBeCloseTo(original, 10);
    });

    it('should round-trip timestamp values', () => {
      const original = BigInt(Date.now() * 1000);
      const serialized = serializeShreddedBound(original, 'timestamp');
      const deserialized = deserializeShreddedBound(serialized, 'timestamp');

      expect(deserialized).toBe(original);
    });
  });

  // ==========================================================================
  // DataFile Statistics Tests
  // ==========================================================================

  describe('createShreddedColumnStats', () => {
    it('should create stats with lower and upper bounds', () => {
      const stats = createShreddedColumnStats({
        path: '$data.typed_value.title.typed_value',
        fieldId: 1000,
        lowerBound: 'aaa',
        upperBound: 'zzz',
        type: 'string',
      });

      expect(stats.path).toBe('$data.typed_value.title.typed_value');
      expect(stats.fieldId).toBe(1000);
      expect(stats.lowerBound).toBeInstanceOf(Uint8Array);
      expect(stats.upperBound).toBeInstanceOf(Uint8Array);
    });

    it('should create stats with null and value counts', () => {
      const stats = createShreddedColumnStats({
        path: '$data.typed_value.age.typed_value',
        fieldId: 1001,
        lowerBound: 0,
        upperBound: 100,
        type: 'int',
        nullCount: 5,
        valueCount: 1000,
      });

      expect(stats.nullCount).toBe(5);
      expect(stats.valueCount).toBe(1000);
    });

    it('should handle optional bounds', () => {
      const stats = createShreddedColumnStats({
        path: '$data.typed_value.optional.typed_value',
        fieldId: 1002,
        type: 'string',
        nullCount: 100,
        valueCount: 100,
      });

      expect(stats.lowerBound).toBeUndefined();
      expect(stats.upperBound).toBeUndefined();
      expect(stats.nullCount).toBe(100);
    });
  });

  describe('applyShreddedStatsToDataFile', () => {
    it('should add shredded stats to DataFile lower-bounds', () => {
      const dataFile = {
        content: 0 as const,
        'file-path': 's3://bucket/data/file.parquet',
        'file-format': 'parquet' as const,
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 4096,
        'lower-bounds': { 1: new Uint8Array([0, 0, 0, 0]) },
        'upper-bounds': { 1: new Uint8Array([255, 255, 255, 255]) },
      };

      const shreddedStats: ShreddedColumnStats[] = [
        {
          path: '$data.typed_value.title.typed_value',
          fieldId: 1000,
          lowerBound: new TextEncoder().encode('aaa'),
          upperBound: new TextEncoder().encode('zzz'),
        },
      ];

      const result = applyShreddedStatsToDataFile(dataFile, shreddedStats);

      expect(result['lower-bounds']).toBeDefined();
      expect(result['lower-bounds']![1000]).toBeDefined();
      expect(result['upper-bounds']![1000]).toBeDefined();
    });

    it('should add shredded stats to DataFile null-value-counts', () => {
      const dataFile = {
        content: 0 as const,
        'file-path': 's3://bucket/data/file.parquet',
        'file-format': 'parquet' as const,
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 4096,
        'null-value-counts': { 1: 10 },
      };

      const shreddedStats: ShreddedColumnStats[] = [
        {
          path: '$data.typed_value.title.typed_value',
          fieldId: 1000,
          nullCount: 50,
        },
      ];

      const result = applyShreddedStatsToDataFile(dataFile, shreddedStats);

      expect(result['null-value-counts']).toBeDefined();
      expect(result['null-value-counts']![1000]).toBe(50);
    });

    it('should add shredded stats to DataFile value-counts', () => {
      const dataFile = {
        content: 0 as const,
        'file-path': 's3://bucket/data/file.parquet',
        'file-format': 'parquet' as const,
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 4096,
        'value-counts': { 1: 1000 },
      };

      const shreddedStats: ShreddedColumnStats[] = [
        {
          path: '$data.typed_value.title.typed_value',
          fieldId: 1000,
          valueCount: 950,
        },
      ];

      const result = applyShreddedStatsToDataFile(dataFile, shreddedStats);

      expect(result['value-counts']).toBeDefined();
      expect(result['value-counts']![1000]).toBe(950);
    });

    it('should preserve existing stats when adding shredded stats', () => {
      const dataFile = {
        content: 0 as const,
        'file-path': 's3://bucket/data/file.parquet',
        'file-format': 'parquet' as const,
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 4096,
        'lower-bounds': { 1: new Uint8Array([10, 0, 0, 0]) },
        'upper-bounds': { 1: new Uint8Array([100, 0, 0, 0]) },
        'null-value-counts': { 1: 5 },
        'value-counts': { 1: 1000 },
      };

      const shreddedStats: ShreddedColumnStats[] = [
        {
          path: '$data.typed_value.title.typed_value',
          fieldId: 1000,
          lowerBound: new TextEncoder().encode('a'),
          upperBound: new TextEncoder().encode('z'),
          nullCount: 10,
          valueCount: 990,
        },
      ];

      const result = applyShreddedStatsToDataFile(dataFile, shreddedStats);

      // Original stats should be preserved
      expect(result['lower-bounds']![1]).toEqual(new Uint8Array([10, 0, 0, 0]));
      expect(result['upper-bounds']![1]).toEqual(new Uint8Array([100, 0, 0, 0]));
      expect(result['null-value-counts']![1]).toBe(5);
      expect(result['value-counts']![1]).toBe(1000);

      // Shredded stats should be added
      expect(result['lower-bounds']![1000]).toBeDefined();
      expect(result['upper-bounds']![1000]).toBeDefined();
      expect(result['null-value-counts']![1000]).toBe(10);
      expect(result['value-counts']![1000]).toBe(990);
    });
  });

  // ==========================================================================
  // Stats Merge Tests (for manifest compaction)
  // ==========================================================================

  describe('mergeShreddedStats', () => {
    it('should merge stats by taking min of lowers', () => {
      const stats1: ShreddedColumnStats = {
        path: '$data.typed_value.value.typed_value',
        fieldId: 1000,
        lowerBound: new TextEncoder().encode('bbb'),
        upperBound: new TextEncoder().encode('mmm'),
      };

      const stats2: ShreddedColumnStats = {
        path: '$data.typed_value.value.typed_value',
        fieldId: 1000,
        lowerBound: new TextEncoder().encode('aaa'),
        upperBound: new TextEncoder().encode('zzz'),
      };

      const merged = mergeShreddedStats(stats1, stats2, 'string');

      expect(new TextDecoder().decode(merged.lowerBound!)).toBe('aaa');
    });

    it('should merge stats by taking max of uppers', () => {
      const stats1: ShreddedColumnStats = {
        path: '$data.typed_value.value.typed_value',
        fieldId: 1000,
        lowerBound: new TextEncoder().encode('bbb'),
        upperBound: new TextEncoder().encode('mmm'),
      };

      const stats2: ShreddedColumnStats = {
        path: '$data.typed_value.value.typed_value',
        fieldId: 1000,
        lowerBound: new TextEncoder().encode('aaa'),
        upperBound: new TextEncoder().encode('zzz'),
      };

      const merged = mergeShreddedStats(stats1, stats2, 'string');

      expect(new TextDecoder().decode(merged.upperBound!)).toBe('zzz');
    });

    it('should merge stats by summing null counts', () => {
      const stats1: ShreddedColumnStats = {
        path: '$data.typed_value.value.typed_value',
        fieldId: 1000,
        nullCount: 10,
      };

      const stats2: ShreddedColumnStats = {
        path: '$data.typed_value.value.typed_value',
        fieldId: 1000,
        nullCount: 20,
      };

      const merged = mergeShreddedStats(stats1, stats2, 'string');

      expect(merged.nullCount).toBe(30);
    });

    it('should merge stats by summing value counts', () => {
      const stats1: ShreddedColumnStats = {
        path: '$data.typed_value.value.typed_value',
        fieldId: 1000,
        valueCount: 500,
      };

      const stats2: ShreddedColumnStats = {
        path: '$data.typed_value.value.typed_value',
        fieldId: 1000,
        valueCount: 300,
      };

      const merged = mergeShreddedStats(stats1, stats2, 'string');

      expect(merged.valueCount).toBe(800);
    });

    it('should handle merging when one side has undefined bounds', () => {
      const stats1: ShreddedColumnStats = {
        path: '$data.typed_value.value.typed_value',
        fieldId: 1000,
        lowerBound: new TextEncoder().encode('aaa'),
        upperBound: new TextEncoder().encode('zzz'),
      };

      const stats2: ShreddedColumnStats = {
        path: '$data.typed_value.value.typed_value',
        fieldId: 1000,
        // No bounds
      };

      const merged = mergeShreddedStats(stats1, stats2, 'string');

      // Should use the defined bounds
      expect(merged.lowerBound).toEqual(stats1.lowerBound);
      expect(merged.upperBound).toEqual(stats1.upperBound);
    });

    it('should merge integer bounds correctly', () => {
      // Create stats with int bounds
      const lower1 = new Uint8Array([10, 0, 0, 0]); // 10 in little-endian
      const upper1 = new Uint8Array([50, 0, 0, 0]); // 50 in little-endian
      const lower2 = new Uint8Array([5, 0, 0, 0]); // 5 in little-endian
      const upper2 = new Uint8Array([100, 0, 0, 0]); // 100 in little-endian

      const stats1: ShreddedColumnStats = {
        path: '$data.typed_value.age.typed_value',
        fieldId: 1000,
        lowerBound: lower1,
        upperBound: upper1,
      };

      const stats2: ShreddedColumnStats = {
        path: '$data.typed_value.age.typed_value',
        fieldId: 1000,
        lowerBound: lower2,
        upperBound: upper2,
      };

      const merged = mergeShreddedStats(stats1, stats2, 'int');

      // Should take min lower (5) and max upper (100)
      const view = new DataView(merged.lowerBound!.buffer);
      expect(view.getInt32(0, true)).toBe(5);

      const viewUpper = new DataView(merged.upperBound!.buffer);
      expect(viewUpper.getInt32(0, true)).toBe(100);
    });
  });

  // ==========================================================================
  // Integration with Existing Stats Tests
  // ==========================================================================

  describe('Integration with existing column stats', () => {
    it('should work alongside regular column stats', () => {
      const dataFile = {
        content: 0 as const,
        'file-path': 's3://bucket/data/file.parquet',
        'file-format': 'parquet' as const,
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 4096,
        // Regular column stats
        'column-sizes': { 1: 2048, 2: 1024 },
        'value-counts': { 1: 1000, 2: 1000 },
        'null-value-counts': { 1: 0, 2: 50 },
        'lower-bounds': { 1: new Uint8Array([0]), 2: new Uint8Array([1]) },
        'upper-bounds': { 1: new Uint8Array([255]), 2: new Uint8Array([100]) },
      };

      const shreddedStats: ShreddedColumnStats[] = [
        {
          path: '$data.typed_value.variant_field.typed_value',
          fieldId: 1000,
          lowerBound: new TextEncoder().encode('abc'),
          upperBound: new TextEncoder().encode('xyz'),
          nullCount: 25,
          valueCount: 975,
        },
      ];

      const result = applyShreddedStatsToDataFile(dataFile, shreddedStats);

      // Regular stats should be preserved
      expect(Object.keys(result['lower-bounds']!)).toContain('1');
      expect(Object.keys(result['lower-bounds']!)).toContain('2');

      // Shredded stats should be added
      expect(Object.keys(result['lower-bounds']!)).toContain('1000');
    });

    it('should handle multiple shredded columns alongside regular stats', () => {
      const dataFile = {
        content: 0 as const,
        'file-path': 's3://bucket/data/file.parquet',
        'file-format': 'parquet' as const,
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 4096,
        'lower-bounds': { 1: new Uint8Array([0]) },
        'upper-bounds': { 1: new Uint8Array([255]) },
      };

      const shreddedStats: ShreddedColumnStats[] = [
        {
          path: '$data.typed_value.title.typed_value',
          fieldId: 1000,
          lowerBound: new TextEncoder().encode('a'),
          upperBound: new TextEncoder().encode('z'),
        },
        {
          path: '$data.typed_value.year.typed_value',
          fieldId: 1001,
          lowerBound: new Uint8Array([208, 7, 0, 0]), // 2000
          upperBound: new Uint8Array([230, 7, 0, 0]), // 2022
        },
        {
          path: '$index.typed_value.key.typed_value',
          fieldId: 2000,
          lowerBound: new TextEncoder().encode('key-001'),
          upperBound: new TextEncoder().encode('key-999'),
        },
      ];

      const result = applyShreddedStatsToDataFile(dataFile, shreddedStats);

      expect(result['lower-bounds']![1]).toBeDefined();
      expect(result['lower-bounds']![1000]).toBeDefined();
      expect(result['lower-bounds']![1001]).toBeDefined();
      expect(result['lower-bounds']![2000]).toBeDefined();
    });
  });
});
