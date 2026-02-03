/**
 * Tests for Variant Statistics Collection
 *
 * This module tests collectShreddedColumnStats which computes min/max bounds
 * on typed_value columns for shredded variant fields.
 *
 * TDD RED Phase: These tests are written BEFORE the implementation.
 *
 * @see https://iceberg.apache.org/spec/#variant-shredding
 */

import { describe, it, expect } from 'vitest';
import type { VariantShredPropertyConfig, DataFile, IcebergSchema } from '../../../src/index.js';
import { encodeStatValue, ManifestGenerator } from '../../../src/index.js';

// Import the functions we'll implement (will fail until GREEN phase)
import {
  collectShreddedColumnStats,
  computeStringBounds,
  computeNumericBounds,
  computeTimestampBounds,
  computeBooleanBounds,
  addShreddedStatsToDataFile,
  type ColumnValues,
  type CollectedStats,
  type ShreddedColumnStats,
} from '../../../src/variant/stats-collector.js';

// ============================================================================
// Basic Stats Collection Tests
// ============================================================================

describe('collectShreddedColumnStats', () => {
  describe('basic stats collection tests', () => {
    it('should collect stats for string values', () => {
      const columns: readonly ColumnValues[] = [
        {
          path: 'title',
          values: ['The Matrix', 'Inception', 'Avatar', 'Interstellar'],
        },
      ];

      const configs: readonly VariantShredPropertyConfig[] = [
        {
          columnName: '$data',
          fields: ['title'],
          fieldTypes: { title: 'string' },
        },
      ];

      const result = collectShreddedColumnStats(columns, configs, 100);

      expect(result.stats.length).toBe(1);
      expect(result.stats[0].path).toBe('title');
      expect(result.stats[0].type).toBe('string');
      expect(result.stats[0].valueCount).toBe(4);
      expect(result.stats[0].nullCount).toBe(0);
    });

    it('should collect stats for integer values', () => {
      const columns: readonly ColumnValues[] = [
        {
          path: 'year',
          values: [1999, 2010, 2009, 2014],
        },
      ];

      const configs: readonly VariantShredPropertyConfig[] = [
        {
          columnName: '$data',
          fields: ['year'],
          fieldTypes: { year: 'int' },
        },
      ];

      const result = collectShreddedColumnStats(columns, configs, 100);

      expect(result.stats.length).toBe(1);
      expect(result.stats[0].path).toBe('year');
      expect(result.stats[0].type).toBe('int');
      expect(result.stats[0].valueCount).toBe(4);
      expect(result.stats[0].lowerBound).toBe(1999);
      expect(result.stats[0].upperBound).toBe(2014);
    });

    it('should collect stats for double values', () => {
      const columns: readonly ColumnValues[] = [
        {
          path: 'rating',
          values: [8.7, 9.0, 7.8, 8.6],
        },
      ];

      const configs: readonly VariantShredPropertyConfig[] = [
        {
          columnName: '$data',
          fields: ['rating'],
          fieldTypes: { rating: 'double' },
        },
      ];

      const result = collectShreddedColumnStats(columns, configs, 100);

      expect(result.stats.length).toBe(1);
      expect(result.stats[0].path).toBe('rating');
      expect(result.stats[0].type).toBe('double');
      expect(result.stats[0].lowerBound).toBe(7.8);
      expect(result.stats[0].upperBound).toBe(9.0);
    });

    it('should collect stats for timestamp values', () => {
      const timestamps = [
        new Date('2020-01-15T10:30:00Z'),
        new Date('2019-06-20T14:00:00Z'),
        new Date('2021-12-01T08:00:00Z'),
      ];

      const columns: readonly ColumnValues[] = [
        {
          path: 'created_at',
          values: timestamps,
        },
      ];

      const configs: readonly VariantShredPropertyConfig[] = [
        {
          columnName: '$data',
          fields: ['created_at'],
          fieldTypes: { created_at: 'timestamptz' },
        },
      ];

      const result = collectShreddedColumnStats(columns, configs, 100);

      expect(result.stats.length).toBe(1);
      expect(result.stats[0].path).toBe('created_at');
      expect(result.stats[0].type).toBe('timestamptz');
      // Timestamps should be converted to microseconds since epoch
      expect(result.stats[0].lowerBound).toBe(timestamps[1].getTime() * 1000);
      expect(result.stats[0].upperBound).toBe(timestamps[2].getTime() * 1000);
    });

    it('should collect stats for boolean values', () => {
      const columns: readonly ColumnValues[] = [
        {
          path: 'is_active',
          values: [true, false, true, true],
        },
      ];

      const configs: readonly VariantShredPropertyConfig[] = [
        {
          columnName: '$data',
          fields: ['is_active'],
          fieldTypes: { is_active: 'boolean' },
        },
      ];

      const result = collectShreddedColumnStats(columns, configs, 100);

      expect(result.stats.length).toBe(1);
      expect(result.stats[0].path).toBe('is_active');
      expect(result.stats[0].type).toBe('boolean');
      expect(result.stats[0].lowerBound).toBe(false);
      expect(result.stats[0].upperBound).toBe(true);
    });

    it('should collect stats for long values', () => {
      const columns: readonly ColumnValues[] = [
        {
          path: 'big_id',
          values: [BigInt('9007199254740992'), BigInt('9007199254740991'), BigInt('9007199254740993')],
        },
      ];

      const configs: readonly VariantShredPropertyConfig[] = [
        {
          columnName: '$data',
          fields: ['big_id'],
          fieldTypes: { big_id: 'long' },
        },
      ];

      const result = collectShreddedColumnStats(columns, configs, 100);

      expect(result.stats.length).toBe(1);
      expect(result.stats[0].path).toBe('big_id');
      expect(result.stats[0].type).toBe('long');
      expect(result.stats[0].lowerBound).toBe(BigInt('9007199254740991'));
      expect(result.stats[0].upperBound).toBe(BigInt('9007199254740993'));
    });
  });

  // ============================================================================
  // Min/Max Bounds Tests
  // ============================================================================

  describe('min/max bounds tests', () => {
    it('should compute lexicographically correct string bounds', () => {
      const columns: readonly ColumnValues[] = [
        {
          path: 'name',
          values: ['banana', 'apple', 'cherry', 'date'],
        },
      ];

      const configs: readonly VariantShredPropertyConfig[] = [
        {
          columnName: '$data',
          fields: ['name'],
          fieldTypes: { name: 'string' },
        },
      ];

      const result = collectShreddedColumnStats(columns, configs, 100);

      expect(result.stats[0].lowerBound).toBe('apple');
      expect(result.stats[0].upperBound).toBe('date');
    });

    it('should handle negative numbers correctly', () => {
      const columns: readonly ColumnValues[] = [
        {
          path: 'temperature',
          values: [-10, 25, -40, 15, 0],
        },
      ];

      const configs: readonly VariantShredPropertyConfig[] = [
        {
          columnName: '$data',
          fields: ['temperature'],
          fieldTypes: { temperature: 'int' },
        },
      ];

      const result = collectShreddedColumnStats(columns, configs, 100);

      expect(result.stats[0].lowerBound).toBe(-40);
      expect(result.stats[0].upperBound).toBe(25);
    });

    it('should handle negative doubles correctly', () => {
      const columns: readonly ColumnValues[] = [
        {
          path: 'balance',
          values: [-100.5, 200.25, -50.75, 0.0],
        },
      ];

      const configs: readonly VariantShredPropertyConfig[] = [
        {
          columnName: '$data',
          fields: ['balance'],
          fieldTypes: { balance: 'double' },
        },
      ];

      const result = collectShreddedColumnStats(columns, configs, 100);

      expect(result.stats[0].lowerBound).toBe(-100.5);
      expect(result.stats[0].upperBound).toBe(200.25);
    });

    it('should handle timestamp bounds with different timezones correctly', () => {
      // All converted to UTC microseconds
      const timestamps = [
        new Date('2020-01-01T00:00:00Z'),
        new Date('2020-12-31T23:59:59Z'),
      ];

      const columns: readonly ColumnValues[] = [
        {
          path: 'event_time',
          values: timestamps,
        },
      ];

      const configs: readonly VariantShredPropertyConfig[] = [
        {
          columnName: '$data',
          fields: ['event_time'],
          fieldTypes: { event_time: 'timestamptz' },
        },
      ];

      const result = collectShreddedColumnStats(columns, configs, 100);

      // Should be in microseconds
      expect(result.stats[0].lowerBound).toBe(timestamps[0].getTime() * 1000);
      expect(result.stats[0].upperBound).toBe(timestamps[1].getTime() * 1000);
    });

    it('should not have null values affect bounds', () => {
      const columns: readonly ColumnValues[] = [
        {
          path: 'score',
          values: [null, 10, null, 20, null, 5],
        },
      ];

      const configs: readonly VariantShredPropertyConfig[] = [
        {
          columnName: '$data',
          fields: ['score'],
          fieldTypes: { score: 'int' },
        },
      ];

      const result = collectShreddedColumnStats(columns, configs, 100);

      expect(result.stats[0].lowerBound).toBe(5);
      expect(result.stats[0].upperBound).toBe(20);
      expect(result.stats[0].nullCount).toBe(3);
    });
  });

  // ============================================================================
  // Null Handling Tests
  // ============================================================================

  describe('null handling tests', () => {
    it('should track null count correctly', () => {
      const columns: readonly ColumnValues[] = [
        {
          path: 'optional_field',
          values: ['a', null, 'b', null, null, 'c'],
        },
      ];

      const configs: readonly VariantShredPropertyConfig[] = [
        {
          columnName: '$data',
          fields: ['optional_field'],
          fieldTypes: { optional_field: 'string' },
        },
      ];

      const result = collectShreddedColumnStats(columns, configs, 100);

      expect(result.stats[0].nullCount).toBe(3);
      expect(result.stats[0].valueCount).toBe(6);
    });

    it('should handle all-null column with no bounds', () => {
      const columns: readonly ColumnValues[] = [
        {
          path: 'all_nulls',
          values: [null, null, null],
        },
      ];

      const configs: readonly VariantShredPropertyConfig[] = [
        {
          columnName: '$data',
          fields: ['all_nulls'],
          fieldTypes: { all_nulls: 'string' },
        },
      ];

      const result = collectShreddedColumnStats(columns, configs, 100);

      expect(result.stats[0].nullCount).toBe(3);
      expect(result.stats[0].valueCount).toBe(3);
      expect(result.stats[0].lowerBound).toBeUndefined();
      expect(result.stats[0].upperBound).toBeUndefined();
    });

    it('should handle mixed null/value column with correct bounds', () => {
      const columns: readonly ColumnValues[] = [
        {
          path: 'mixed',
          values: [null, 100, null, 50, 200, null],
        },
      ];

      const configs: readonly VariantShredPropertyConfig[] = [
        {
          columnName: '$data',
          fields: ['mixed'],
          fieldTypes: { mixed: 'int' },
        },
      ];

      const result = collectShreddedColumnStats(columns, configs, 100);

      expect(result.stats[0].nullCount).toBe(3);
      expect(result.stats[0].valueCount).toBe(6);
      expect(result.stats[0].lowerBound).toBe(50);
      expect(result.stats[0].upperBound).toBe(200);
    });

    it('should treat undefined as null', () => {
      const columns: readonly ColumnValues[] = [
        {
          path: 'with_undefined',
          values: [undefined, 'value', undefined],
        },
      ];

      const configs: readonly VariantShredPropertyConfig[] = [
        {
          columnName: '$data',
          fields: ['with_undefined'],
          fieldTypes: { with_undefined: 'string' },
        },
      ];

      const result = collectShreddedColumnStats(columns, configs, 100);

      expect(result.stats[0].nullCount).toBe(2);
    });
  });

  // ============================================================================
  // Value Count Tests
  // ============================================================================

  describe('value count tests', () => {
    it('should count values correctly excluding nulls from non-null count', () => {
      const columns: readonly ColumnValues[] = [
        {
          path: 'values',
          values: [1, null, 2, null, 3],
        },
      ];

      const configs: readonly VariantShredPropertyConfig[] = [
        {
          columnName: '$data',
          fields: ['values'],
          fieldTypes: { values: 'int' },
        },
      ];

      const result = collectShreddedColumnStats(columns, configs, 100);

      expect(result.stats[0].valueCount).toBe(5); // Total count
      expect(result.stats[0].nullCount).toBe(2); // Null count
      // Non-null count = valueCount - nullCount = 3
    });

    it('should handle empty column values', () => {
      const columns: readonly ColumnValues[] = [
        {
          path: 'empty',
          values: [],
        },
      ];

      const configs: readonly VariantShredPropertyConfig[] = [
        {
          columnName: '$data',
          fields: ['empty'],
          fieldTypes: { empty: 'string' },
        },
      ];

      const result = collectShreddedColumnStats(columns, configs, 100);

      expect(result.stats[0].valueCount).toBe(0);
      expect(result.stats[0].nullCount).toBe(0);
      expect(result.stats[0].lowerBound).toBeUndefined();
      expect(result.stats[0].upperBound).toBeUndefined();
    });
  });

  // ============================================================================
  // Field ID Assignment Tests
  // ============================================================================

  describe('field ID assignment tests', () => {
    it('should assign sequential field IDs starting from startingFieldId', () => {
      const columns: readonly ColumnValues[] = [
        { path: 'field1', values: ['a'] },
        { path: 'field2', values: [1] },
        { path: 'field3', values: [true] },
      ];

      const configs: readonly VariantShredPropertyConfig[] = [
        {
          columnName: '$data',
          fields: ['field1', 'field2', 'field3'],
          fieldTypes: { field1: 'string', field2: 'int', field3: 'boolean' },
        },
      ];

      const result = collectShreddedColumnStats(columns, configs, 1000);

      expect(result.fieldIdMap.get('field1')).toBe(1000);
      expect(result.fieldIdMap.get('field2')).toBe(1001);
      expect(result.fieldIdMap.get('field3')).toBe(1002);
      expect(result.stats[0].fieldId).toBe(1000);
      expect(result.stats[1].fieldId).toBe(1001);
      expect(result.stats[2].fieldId).toBe(1002);
    });

    it('should only include configured fields', () => {
      const columns: readonly ColumnValues[] = [
        { path: 'included', values: ['a'] },
        { path: 'not_configured', values: ['b'] },
      ];

      const configs: readonly VariantShredPropertyConfig[] = [
        {
          columnName: '$data',
          fields: ['included'], // only 'included' is configured
          fieldTypes: { included: 'string' },
        },
      ];

      const result = collectShreddedColumnStats(columns, configs, 100);

      expect(result.stats.length).toBe(1);
      expect(result.stats[0].path).toBe('included');
    });
  });

  // ============================================================================
  // Multiple Columns Tests
  // ============================================================================

  describe('multiple columns tests', () => {
    it('should collect stats for multiple columns', () => {
      const columns: readonly ColumnValues[] = [
        { path: 'title', values: ['Movie A', 'Movie B'] },
        { path: 'year', values: [2020, 2021] },
        { path: 'rating', values: [8.5, 9.0] },
      ];

      const configs: readonly VariantShredPropertyConfig[] = [
        {
          columnName: '$data',
          fields: ['title', 'year', 'rating'],
          fieldTypes: { title: 'string', year: 'int', rating: 'double' },
        },
      ];

      const result = collectShreddedColumnStats(columns, configs, 100);

      expect(result.stats.length).toBe(3);

      const titleStats = result.stats.find((s) => s.path === 'title');
      const yearStats = result.stats.find((s) => s.path === 'year');
      const ratingStats = result.stats.find((s) => s.path === 'rating');

      expect(titleStats?.lowerBound).toBe('Movie A');
      expect(yearStats?.lowerBound).toBe(2020);
      expect(ratingStats?.lowerBound).toBe(8.5);
    });
  });
});

// ============================================================================
// Individual Compute Functions Tests
// ============================================================================

describe('computeStringBounds', () => {
  it('should compute lexicographic min/max', () => {
    const values: readonly (string | null)[] = ['banana', 'apple', 'cherry'];
    const result = computeStringBounds(values);

    expect(result.lower).toBe('apple');
    expect(result.upper).toBe('cherry');
  });

  it('should truncate bounds to maxLength', () => {
    const values: readonly (string | null)[] = ['verylongstring1', 'verylongstring2'];
    const result = computeStringBounds(values, 8);

    expect(result.lower?.length).toBeLessThanOrEqual(8);
    expect(result.upper?.length).toBeLessThanOrEqual(8);
  });

  it('should return null bounds for all-null values', () => {
    const values: readonly (string | null)[] = [null, null];
    const result = computeStringBounds(values);

    expect(result.lower).toBeNull();
    expect(result.upper).toBeNull();
  });

  it('should handle empty array', () => {
    const values: readonly (string | null)[] = [];
    const result = computeStringBounds(values);

    expect(result.lower).toBeNull();
    expect(result.upper).toBeNull();
  });

  it('should handle single value', () => {
    const values: readonly (string | null)[] = ['only'];
    const result = computeStringBounds(values);

    expect(result.lower).toBe('only');
    expect(result.upper).toBe('only');
  });
});

describe('computeNumericBounds', () => {
  it('should compute min/max for integers', () => {
    const values: readonly (number | null)[] = [5, 10, 3, 8];
    const result = computeNumericBounds(values);

    expect(result.lower).toBe(3);
    expect(result.upper).toBe(10);
  });

  it('should compute min/max for bigints', () => {
    const values: readonly (bigint | null)[] = [BigInt(100), BigInt(50), BigInt(200)];
    const result = computeNumericBounds(values);

    expect(result.lower).toBe(BigInt(50));
    expect(result.upper).toBe(BigInt(200));
  });

  it('should handle negative numbers', () => {
    const values: readonly (number | null)[] = [-5, 10, -20, 0];
    const result = computeNumericBounds(values);

    expect(result.lower).toBe(-20);
    expect(result.upper).toBe(10);
  });

  it('should return null bounds for all-null values', () => {
    const values: readonly (number | null)[] = [null, null];
    const result = computeNumericBounds(values);

    expect(result.lower).toBeNull();
    expect(result.upper).toBeNull();
  });

  it('should ignore NaN values', () => {
    const values: readonly (number | null)[] = [5, NaN, 10, NaN, 3];
    const result = computeNumericBounds(values);

    expect(result.lower).toBe(3);
    expect(result.upper).toBe(10);
  });
});

describe('computeTimestampBounds', () => {
  it('should compute min/max from Date objects', () => {
    const d1 = new Date('2020-01-01T00:00:00Z');
    const d2 = new Date('2020-06-15T12:00:00Z');
    const d3 = new Date('2020-12-31T23:59:59Z');

    const values: readonly (Date | number | null)[] = [d2, d1, d3];
    const result = computeTimestampBounds(values);

    // Should be in microseconds
    expect(result.lower).toBe(d1.getTime() * 1000);
    expect(result.upper).toBe(d3.getTime() * 1000);
  });

  it('should handle numeric timestamps (milliseconds)', () => {
    const values: readonly (Date | number | null)[] = [1577836800000, 1609459199000]; // ms since epoch
    const result = computeTimestampBounds(values);

    // Output should be microseconds
    expect(result.lower).toBe(1577836800000 * 1000);
    expect(result.upper).toBe(1609459199000 * 1000);
  });

  it('should return null bounds for all-null values', () => {
    const values: readonly (Date | number | null)[] = [null, null];
    const result = computeTimestampBounds(values);

    expect(result.lower).toBeNull();
    expect(result.upper).toBeNull();
  });
});

describe('computeBooleanBounds', () => {
  it('should return false/true for mixed values', () => {
    const values: readonly (boolean | null)[] = [true, false, true];
    const result = computeBooleanBounds(values);

    expect(result.lower).toBe(false);
    expect(result.upper).toBe(true);
  });

  it('should return same value for all-true', () => {
    const values: readonly (boolean | null)[] = [true, true, true];
    const result = computeBooleanBounds(values);

    expect(result.lower).toBe(true);
    expect(result.upper).toBe(true);
  });

  it('should return same value for all-false', () => {
    const values: readonly (boolean | null)[] = [false, false, false];
    const result = computeBooleanBounds(values);

    expect(result.lower).toBe(false);
    expect(result.upper).toBe(false);
  });

  it('should return null bounds for all-null values', () => {
    const values: readonly (boolean | null)[] = [null, null];
    const result = computeBooleanBounds(values);

    expect(result.lower).toBeNull();
    expect(result.upper).toBeNull();
  });
});

// ============================================================================
// Type-Specific Serialization Tests
// ============================================================================

describe('type-specific serialization tests', () => {
  it('should serialize string bounds with truncated prefix (default 16 bytes)', () => {
    const columns: readonly ColumnValues[] = [
      {
        path: 'long_string',
        values: ['this_is_a_very_long_string_value'],
      },
    ];

    const configs: readonly VariantShredPropertyConfig[] = [
      {
        columnName: '$data',
        fields: ['long_string'],
        fieldTypes: { long_string: 'string' },
      },
    ];

    const result = collectShreddedColumnStats(columns, configs, 100);

    // Bounds should be truncated
    const lowerBound = result.stats[0].lowerBound as string;
    const upperBound = result.stats[0].upperBound as string;
    expect(lowerBound.length).toBeLessThanOrEqual(16);
    expect(upperBound.length).toBeLessThanOrEqual(16);
  });

  it('should encode long bounds as 8-byte little-endian', () => {
    const longValue = BigInt(12345678901234);

    // Use encodeStatValue from avro module
    const encoded = encodeStatValue(longValue, 'long');

    expect(encoded.length).toBe(8);
    // Verify little-endian encoding
    const view = new DataView(encoded.buffer, encoded.byteOffset, encoded.byteLength);
    expect(view.getBigInt64(0, true)).toBe(longValue);
  });

  it('should encode double bounds as IEEE 754', () => {
    const doubleValue = 3.14159265359;

    const encoded = encodeStatValue(doubleValue, 'double');

    expect(encoded.length).toBe(8);
    const view = new DataView(encoded.buffer, encoded.byteOffset, encoded.byteLength);
    expect(view.getFloat64(0, true)).toBeCloseTo(doubleValue);
  });

  it('should encode boolean bounds as single byte', () => {
    const trueEncoded = encodeStatValue(true, 'boolean');
    const falseEncoded = encodeStatValue(false, 'boolean');

    expect(trueEncoded.length).toBe(1);
    expect(falseEncoded.length).toBe(1);
    expect(trueEncoded[0]).toBe(1);
    expect(falseEncoded[0]).toBe(0);
  });

  it('should encode int bounds as 4-byte little-endian', () => {
    const intValue = 12345;

    const encoded = encodeStatValue(intValue, 'int');

    expect(encoded.length).toBe(4);
    const view = new DataView(encoded.buffer, encoded.byteOffset, encoded.byteLength);
    expect(view.getInt32(0, true)).toBe(intValue);
  });
});

// ============================================================================
// ManifestGenerator Integration Tests
// ============================================================================

describe('ManifestGenerator integration tests', () => {
  it('should accept shredded stats in ManifestGenerator', () => {
    const columns: readonly ColumnValues[] = [
      { path: 'title', values: ['Movie A', 'Movie B'] },
      { path: 'year', values: [2020, 2021] },
    ];

    const configs: readonly VariantShredPropertyConfig[] = [
      {
        columnName: '$data',
        fields: ['title', 'year'],
        fieldTypes: { title: 'string', year: 'int' },
      },
    ];

    const collected = collectShreddedColumnStats(columns, configs, 100);

    const manifest = new ManifestGenerator({
      sequenceNumber: 1,
      snapshotId: 123456789,
    });

    // Create base data file
    const baseDataFile: Omit<DataFile, 'content'> = {
      'file-path': 's3://bucket/data/file.parquet',
      'file-format': 'parquet',
      partition: {},
      'record-count': 2,
      'file-size-in-bytes': 1024,
    };

    // Apply shredded stats and add to manifest
    const dataFileWithStats = addShreddedStatsToDataFile(baseDataFile as DataFile, collected);

    manifest.addDataFile(dataFileWithStats);

    const result = manifest.generate();

    expect(result.entries.length).toBe(1);
    const dataFile = result.entries[0]['data-file'];

    // Verify stats are present
    expect(dataFile['value-counts']).toBeDefined();
    expect(dataFile['lower-bounds']).toBeDefined();
    expect(dataFile['upper-bounds']).toBeDefined();
  });

  it('should have correct field IDs in stats', () => {
    const columns: readonly ColumnValues[] = [
      { path: 'title', values: ['A'] },
      { path: 'year', values: [2020] },
    ];

    const configs: readonly VariantShredPropertyConfig[] = [
      {
        columnName: '$data',
        fields: ['title', 'year'],
        fieldTypes: { title: 'string', year: 'int' },
      },
    ];

    const collected = collectShreddedColumnStats(columns, configs, 500);

    const baseDataFile: Omit<DataFile, 'content'> = {
      'file-path': 's3://bucket/data/file.parquet',
      'file-format': 'parquet',
      partition: {},
      'record-count': 1,
      'file-size-in-bytes': 512,
    };

    const dataFileWithStats = addShreddedStatsToDataFile(baseDataFile as DataFile, collected);

    // Verify field IDs start at 500
    expect(dataFileWithStats['value-counts']?.[500]).toBe(1);
    expect(dataFileWithStats['value-counts']?.[501]).toBe(1);
    expect(dataFileWithStats['lower-bounds']?.[500]).toBeDefined();
    expect(dataFileWithStats['lower-bounds']?.[501]).toBeDefined();
  });

  it('should merge shredded stats with existing data file stats', () => {
    const columns: readonly ColumnValues[] = [
      { path: 'variant_field', values: ['value'] },
    ];

    const configs: readonly VariantShredPropertyConfig[] = [
      {
        columnName: '$data',
        fields: ['variant_field'],
        fieldTypes: { variant_field: 'string' },
      },
    ];

    const collected = collectShreddedColumnStats(columns, configs, 100);

    const baseDataFile: DataFile = {
      content: 0,
      'file-path': 's3://bucket/data/file.parquet',
      'file-format': 'parquet',
      partition: {},
      'record-count': 1,
      'file-size-in-bytes': 512,
      // Existing stats for regular columns
      'value-counts': { 1: 1, 2: 1 },
      'lower-bounds': { 1: new Uint8Array([1, 0, 0, 0, 0, 0, 0, 0]) },
      'upper-bounds': { 1: new Uint8Array([1, 0, 0, 0, 0, 0, 0, 0]) },
    };

    const merged = addShreddedStatsToDataFile(baseDataFile, collected);

    // Should have both existing and shredded stats
    expect(merged['value-counts']?.[1]).toBe(1); // Existing
    expect(merged['value-counts']?.[100]).toBe(1); // Shredded
    expect(merged['lower-bounds']?.[1]).toBeDefined(); // Existing
    expect(merged['lower-bounds']?.[100]).toBeDefined(); // Shredded
  });
});

// ============================================================================
// addShreddedStatsToDataFile Tests
// ============================================================================

describe('addShreddedStatsToDataFile', () => {
  it('should add stats to a data file', () => {
    const columns: readonly ColumnValues[] = [
      { path: 'field1', values: [1, 2, 3] },
    ];

    const configs: readonly VariantShredPropertyConfig[] = [
      {
        columnName: '$data',
        fields: ['field1'],
        fieldTypes: { field1: 'int' },
      },
    ];

    const collected = collectShreddedColumnStats(columns, configs, 100);

    const dataFile: DataFile = {
      content: 0,
      'file-path': 's3://bucket/data/file.parquet',
      'file-format': 'parquet',
      partition: {},
      'record-count': 3,
      'file-size-in-bytes': 1024,
    };

    const result = addShreddedStatsToDataFile(dataFile, collected);

    expect(result['value-counts']).toBeDefined();
    expect(result['null-value-counts']).toBeDefined();
    expect(result['lower-bounds']).toBeDefined();
    expect(result['upper-bounds']).toBeDefined();
  });

  it('should not overwrite existing stats fields', () => {
    const columns: readonly ColumnValues[] = [
      { path: 'field1', values: [1] },
    ];

    const configs: readonly VariantShredPropertyConfig[] = [
      {
        columnName: '$data',
        fields: ['field1'],
        fieldTypes: { field1: 'int' },
      },
    ];

    const collected = collectShreddedColumnStats(columns, configs, 200);

    const dataFile: DataFile = {
      content: 0,
      'file-path': 's3://bucket/data/file.parquet',
      'file-format': 'parquet',
      partition: {},
      'record-count': 1,
      'file-size-in-bytes': 512,
      'value-counts': { 1: 100 },
    };

    const result = addShreddedStatsToDataFile(dataFile, collected);

    // Should keep existing field 1 and add field 200
    expect(result['value-counts']?.[1]).toBe(100);
    expect(result['value-counts']?.[200]).toBe(1);
  });

  it('should encode bounds as Uint8Array', () => {
    const columns: readonly ColumnValues[] = [
      { path: 'num', values: [42] },
    ];

    const configs: readonly VariantShredPropertyConfig[] = [
      {
        columnName: '$data',
        fields: ['num'],
        fieldTypes: { num: 'int' },
      },
    ];

    const collected = collectShreddedColumnStats(columns, configs, 100);

    const dataFile: DataFile = {
      content: 0,
      'file-path': 's3://bucket/data/file.parquet',
      'file-format': 'parquet',
      partition: {},
      'record-count': 1,
      'file-size-in-bytes': 512,
    };

    const result = addShreddedStatsToDataFile(dataFile, collected);

    expect(result['lower-bounds']?.[100]).toBeInstanceOf(Uint8Array);
    expect(result['upper-bounds']?.[100]).toBeInstanceOf(Uint8Array);
  });
});
