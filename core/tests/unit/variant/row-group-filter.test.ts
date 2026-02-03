/**
 * Tests for Row Group Filtering with Variant Statistics
 *
 * These tests verify that DataFile statistics can be used to filter out
 * row groups/files that definitely don't match a query predicate, enabling
 * efficient predicate pushdown for variant columns.
 *
 * @see https://iceberg.apache.org/spec/#variant-shredding
 */

import { describe, it, expect } from 'vitest';
import type { DataFile, IcebergPrimitiveType } from '../../../src/index.js';
import type { VariantShredPropertyConfig } from '../../../src/variant/config.js';
import {
  createRangePredicate,
  evaluateRangePredicate,
  combinePredicatesAnd,
  combinePredicatesOr,
  filterDataFiles,
  filterDataFilesWithStats,
  type RangePredicate,
  type FilterStats,
} from '../../../src/variant/row-group-filter.js';
import { assignShreddedFieldIds } from '../../../src/variant/manifest-stats.js';

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Create a test DataFile with specified bounds for a field.
 */
function createTestDataFile(
  path: string,
  fieldId: number,
  lower: Uint8Array | undefined,
  upper: Uint8Array | undefined
): DataFile {
  return {
    content: 0,
    'file-path': path,
    'file-format': 'parquet',
    partition: {},
    'record-count': 1000,
    'file-size-in-bytes': 4096,
    'lower-bounds': lower ? { [fieldId]: lower } : undefined,
    'upper-bounds': upper ? { [fieldId]: upper } : undefined,
  };
}

/**
 * Encode an integer as a 4-byte little-endian Uint8Array.
 */
function encodeInt(value: number): Uint8Array {
  const buffer = new ArrayBuffer(4);
  new DataView(buffer).setInt32(0, value, true);
  return new Uint8Array(buffer);
}

/**
 * Encode a string as UTF-8 Uint8Array.
 */
function encodeString(value: string): Uint8Array {
  return new TextEncoder().encode(value);
}

/**
 * Encode a long as an 8-byte little-endian Uint8Array.
 */
function encodeLong(value: bigint): Uint8Array {
  const buffer = new ArrayBuffer(8);
  new DataView(buffer).setBigInt64(0, value, true);
  return new Uint8Array(buffer);
}

// ============================================================================
// RangePredicate Type Tests
// ============================================================================

describe('RangePredicate Type', () => {
  describe('createRangePredicate', () => {
    it('should create range predicate from $gt operator', () => {
      const predicate = createRangePredicate('$gt', 10);

      expect(predicate.lowerExclusive).toBe(10);
      expect(predicate.lowerInclusive).toBeUndefined();
      expect(predicate.upperInclusive).toBeUndefined();
      expect(predicate.upperExclusive).toBeUndefined();
      expect(predicate.points).toBeUndefined();
    });

    it('should create range predicate from $gte operator', () => {
      const predicate = createRangePredicate('$gte', 10);

      expect(predicate.lowerInclusive).toBe(10);
      expect(predicate.lowerExclusive).toBeUndefined();
      expect(predicate.upperInclusive).toBeUndefined();
      expect(predicate.upperExclusive).toBeUndefined();
    });

    it('should create range predicate from $lt operator', () => {
      const predicate = createRangePredicate('$lt', 100);

      expect(predicate.upperExclusive).toBe(100);
      expect(predicate.upperInclusive).toBeUndefined();
      expect(predicate.lowerInclusive).toBeUndefined();
      expect(predicate.lowerExclusive).toBeUndefined();
    });

    it('should create range predicate from $lte operator', () => {
      const predicate = createRangePredicate('$lte', 100);

      expect(predicate.upperInclusive).toBe(100);
      expect(predicate.upperExclusive).toBeUndefined();
      expect(predicate.lowerInclusive).toBeUndefined();
      expect(predicate.lowerExclusive).toBeUndefined();
    });

    it('should create range predicate from $eq operator (point range)', () => {
      const predicate = createRangePredicate('$eq', 50);

      // Equality is a point range: [50, 50]
      expect(predicate.lowerInclusive).toBe(50);
      expect(predicate.upperInclusive).toBe(50);
      expect(predicate.lowerExclusive).toBeUndefined();
      expect(predicate.upperExclusive).toBeUndefined();
    });

    it('should create range predicate from $in operator (multiple points)', () => {
      const predicate = createRangePredicate('$in', [10, 20, 30]);

      expect(predicate.points).toEqual([10, 20, 30]);
      expect(predicate.lowerInclusive).toBeUndefined();
      expect(predicate.upperInclusive).toBeUndefined();
    });

    it('should handle string values', () => {
      const predicate = createRangePredicate('$gte', 'abc');

      expect(predicate.lowerInclusive).toBe('abc');
    });

    it('should handle bigint values for long types', () => {
      const predicate = createRangePredicate('$gt', BigInt(1000000));

      expect(predicate.lowerExclusive).toBe(BigInt(1000000));
    });
  });
});

// ============================================================================
// Range Predicate Evaluation Tests
// ============================================================================

describe('Range Predicate Evaluation', () => {
  describe('evaluateRangePredicate', () => {
    it('should return true with overlapping bounds (predicate inside file range)', () => {
      // File has values 0-100, predicate is > 50
      const predicate: RangePredicate = { lowerExclusive: 50 };
      const result = evaluateRangePredicate(predicate, 0, 100, 'int');

      expect(result).toBe(true);
    });

    it('should return true when predicate range contains file range', () => {
      // File has values 40-60, predicate is >= 20 and <= 80
      const predicate: RangePredicate = { lowerInclusive: 20, upperInclusive: 80 };
      const result = evaluateRangePredicate(predicate, 40, 60, 'int');

      expect(result).toBe(true);
    });

    it('should return true when file range contains predicate range', () => {
      // File has values 0-100, predicate is >= 40 and <= 60
      const predicate: RangePredicate = { lowerInclusive: 40, upperInclusive: 60 };
      const result = evaluateRangePredicate(predicate, 0, 100, 'int');

      expect(result).toBe(true);
    });

    it('should return false with non-overlapping bounds (predicate above file range)', () => {
      // File has values 0-50, predicate is > 100
      const predicate: RangePredicate = { lowerExclusive: 100 };
      const result = evaluateRangePredicate(predicate, 0, 50, 'int');

      expect(result).toBe(false);
    });

    it('should return false with non-overlapping bounds (predicate below file range)', () => {
      // File has values 50-100, predicate is < 10
      const predicate: RangePredicate = { upperExclusive: 10 };
      const result = evaluateRangePredicate(predicate, 50, 100, 'int');

      expect(result).toBe(false);
    });

    it('should return false when $eq value is outside file range', () => {
      // File has values 0-50, predicate is = 100
      const predicate: RangePredicate = { lowerInclusive: 100, upperInclusive: 100 };
      const result = evaluateRangePredicate(predicate, 0, 50, 'int');

      expect(result).toBe(false);
    });

    it('should return true when $eq value is at file lower bound', () => {
      // File has values 50-100, predicate is = 50
      const predicate: RangePredicate = { lowerInclusive: 50, upperInclusive: 50 };
      const result = evaluateRangePredicate(predicate, 50, 100, 'int');

      expect(result).toBe(true);
    });

    it('should return true when $eq value is at file upper bound', () => {
      // File has values 50-100, predicate is = 100
      const predicate: RangePredicate = { lowerInclusive: 100, upperInclusive: 100 };
      const result = evaluateRangePredicate(predicate, 50, 100, 'int');

      expect(result).toBe(true);
    });

    it('should return true with missing bounds (safe default)', () => {
      // When file stats are missing, assume match is possible
      const predicate: RangePredicate = { lowerExclusive: 50 };
      const result = evaluateRangePredicate(predicate, undefined, undefined, 'int');

      expect(result).toBe(true);
    });

    it('should return true with missing lower bound only', () => {
      const predicate: RangePredicate = { lowerExclusive: 50 };
      const result = evaluateRangePredicate(predicate, undefined, 100, 'int');

      expect(result).toBe(true);
    });

    it('should return true with missing upper bound only', () => {
      const predicate: RangePredicate = { upperExclusive: 50 };
      const result = evaluateRangePredicate(predicate, 0, undefined, 'int');

      expect(result).toBe(true);
    });

    it('should handle $in with at least one point in range', () => {
      // File has values 40-60, predicate is IN [10, 50, 100]
      const predicate: RangePredicate = { points: [10, 50, 100] };
      const result = evaluateRangePredicate(predicate, 40, 60, 'int');

      expect(result).toBe(true);
    });

    it('should handle $in with no points in range', () => {
      // File has values 40-60, predicate is IN [10, 20, 100]
      const predicate: RangePredicate = { points: [10, 20, 100] };
      const result = evaluateRangePredicate(predicate, 40, 60, 'int');

      expect(result).toBe(false);
    });

    it('should handle string comparisons correctly', () => {
      // File has values 'bbb'-'mmm', predicate is >= 'aaa'
      const predicate: RangePredicate = { lowerInclusive: 'aaa' };
      const result = evaluateRangePredicate(predicate, 'bbb', 'mmm', 'string');

      expect(result).toBe(true);
    });

    it('should handle exclusive bounds at boundary', () => {
      // File has values 50-100, predicate is > 100 (exclusive)
      const predicate: RangePredicate = { lowerExclusive: 100 };
      const result = evaluateRangePredicate(predicate, 50, 100, 'int');

      expect(result).toBe(false);
    });

    it('should handle inclusive bounds at boundary', () => {
      // File has values 50-100, predicate is >= 100 (inclusive)
      const predicate: RangePredicate = { lowerInclusive: 100 };
      const result = evaluateRangePredicate(predicate, 50, 100, 'int');

      expect(result).toBe(true);
    });
  });
});

// ============================================================================
// Multi-Field AND Logic Tests
// ============================================================================

describe('Multi-Field AND Logic', () => {
  describe('combinePredicatesAnd', () => {
    it('should intersect two overlapping ranges', () => {
      // [10, inf) AND (-inf, 100] = [10, 100]
      const p1: RangePredicate = { lowerInclusive: 10 };
      const p2: RangePredicate = { upperInclusive: 100 };

      const combined = combinePredicatesAnd([p1, p2]);

      expect(combined).not.toBeNull();
      expect(combined!.lowerInclusive).toBe(10);
      expect(combined!.upperInclusive).toBe(100);
    });

    it('should return null for non-overlapping ranges (AND produces empty set)', () => {
      // [100, inf) AND (-inf, 10] = empty
      const p1: RangePredicate = { lowerInclusive: 100 };
      const p2: RangePredicate = { upperInclusive: 10 };

      const combined = combinePredicatesAnd([p1, p2]);

      expect(combined).toBeNull();
    });

    it('should intersect multiple ranges', () => {
      // [0, inf) AND (-inf, 100] AND [20, 80] = [20, 80]
      const p1: RangePredicate = { lowerInclusive: 0 };
      const p2: RangePredicate = { upperInclusive: 100 };
      const p3: RangePredicate = { lowerInclusive: 20, upperInclusive: 80 };

      const combined = combinePredicatesAnd([p1, p2, p3]);

      expect(combined).not.toBeNull();
      expect(combined!.lowerInclusive).toBe(20);
      expect(combined!.upperInclusive).toBe(80);
    });

    it('should handle point ranges in AND', () => {
      // [50, 50] AND [0, 100] = [50, 50]
      const p1: RangePredicate = { lowerInclusive: 50, upperInclusive: 50 };
      const p2: RangePredicate = { lowerInclusive: 0, upperInclusive: 100 };

      const combined = combinePredicatesAnd([p1, p2]);

      expect(combined).not.toBeNull();
      expect(combined!.lowerInclusive).toBe(50);
      expect(combined!.upperInclusive).toBe(50);
    });

    it('should return null when point range is outside other range', () => {
      // [50, 50] AND [0, 40] = empty
      const p1: RangePredicate = { lowerInclusive: 50, upperInclusive: 50 };
      const p2: RangePredicate = { lowerInclusive: 0, upperInclusive: 40 };

      const combined = combinePredicatesAnd([p1, p2]);

      expect(combined).toBeNull();
    });

    it('should handle exclusive bounds correctly', () => {
      // (10, inf) AND (-inf, 100) = (10, 100)
      const p1: RangePredicate = { lowerExclusive: 10 };
      const p2: RangePredicate = { upperExclusive: 100 };

      const combined = combinePredicatesAnd([p1, p2]);

      expect(combined).not.toBeNull();
      expect(combined!.lowerExclusive).toBe(10);
      expect(combined!.upperExclusive).toBe(100);
    });

    it('should handle single predicate', () => {
      const p1: RangePredicate = { lowerInclusive: 10 };

      const combined = combinePredicatesAnd([p1]);

      expect(combined).not.toBeNull();
      expect(combined!.lowerInclusive).toBe(10);
    });

    it('should handle empty predicates array', () => {
      const combined = combinePredicatesAnd([]);

      // Empty AND is "all values" - no restriction
      expect(combined).not.toBeNull();
    });
  });
});

// ============================================================================
// Multi-Field OR Logic Tests
// ============================================================================

describe('Multi-Field OR Logic', () => {
  describe('combinePredicatesOr', () => {
    it('should return union of non-overlapping ranges', () => {
      // [0, 10] OR [20, 30] = [[0, 10], [20, 30]]
      const p1: RangePredicate = { lowerInclusive: 0, upperInclusive: 10 };
      const p2: RangePredicate = { lowerInclusive: 20, upperInclusive: 30 };

      const combined = combinePredicatesOr([p1, p2]);

      expect(combined).toHaveLength(2);
    });

    it('should merge overlapping ranges', () => {
      // [0, 50] OR [40, 100] = [0, 100]
      const p1: RangePredicate = { lowerInclusive: 0, upperInclusive: 50 };
      const p2: RangePredicate = { lowerInclusive: 40, upperInclusive: 100 };

      const combined = combinePredicatesOr([p1, p2]);

      expect(combined).toHaveLength(1);
      expect(combined[0].lowerInclusive).toBe(0);
      expect(combined[0].upperInclusive).toBe(100);
    });

    it('should handle single predicate', () => {
      const p1: RangePredicate = { lowerInclusive: 10 };

      const combined = combinePredicatesOr([p1]);

      expect(combined).toHaveLength(1);
      expect(combined[0].lowerInclusive).toBe(10);
    });

    it('should handle $in as OR of points', () => {
      // IN [10, 50, 100] is essentially [10,10] OR [50,50] OR [100,100]
      const p1: RangePredicate = { points: [10, 50, 100] };

      const combined = combinePredicatesOr([p1]);

      // Should preserve the points representation
      expect(combined).toHaveLength(1);
      expect(combined[0].points).toEqual([10, 50, 100]);
    });

    it('should handle empty predicates array', () => {
      const combined = combinePredicatesOr([]);

      // Empty OR means "no values" - nothing matches
      expect(combined).toHaveLength(0);
    });
  });
});

// ============================================================================
// filterDataFiles Tests
// ============================================================================

describe('filterDataFiles', () => {
  const configs: readonly VariantShredPropertyConfig[] = [
    {
      columnName: '$data',
      fields: ['age', 'name', 'score'],
      fieldTypes: { age: 'int', name: 'string', score: 'double' },
    },
  ];

  // Assign field IDs starting at 1000
  const fieldIdMap = assignShreddedFieldIds(configs, 1000);

  it('should return files that might match', () => {
    const dataFiles: DataFile[] = [
      createTestDataFile('file1.parquet', 1000, encodeInt(0), encodeInt(50)),
      createTestDataFile('file2.parquet', 1000, encodeInt(40), encodeInt(100)),
      createTestDataFile('file3.parquet', 1000, encodeInt(80), encodeInt(150)),
    ];

    // Filter: age >= 30 AND age <= 60
    const filter = {
      '$data.age': { $gte: 30, $lte: 60 },
    };

    const result = filterDataFiles(dataFiles, filter, configs, fieldIdMap);

    // file1 (0-50) overlaps [30, 60]
    // file2 (40-100) overlaps [30, 60]
    // file3 (80-150) does NOT overlap [30, 60]
    expect(result).toHaveLength(2);
    expect(result.map((f) => f['file-path'])).toContain('file1.parquet');
    expect(result.map((f) => f['file-path'])).toContain('file2.parquet');
  });

  it('should exclude files that definitely do not match', () => {
    const dataFiles: DataFile[] = [
      createTestDataFile('file1.parquet', 1000, encodeInt(0), encodeInt(20)),
      createTestDataFile('file2.parquet', 1000, encodeInt(100), encodeInt(200)),
    ];

    // Filter: age >= 50 AND age <= 60
    const filter = {
      '$data.age': { $gte: 50, $lte: 60 },
    };

    const result = filterDataFiles(dataFiles, filter, configs, fieldIdMap);

    // file1 (0-20) does NOT overlap [50, 60]
    // file2 (100-200) does NOT overlap [50, 60]
    expect(result).toHaveLength(0);
  });

  it('should return all files with empty filter', () => {
    const dataFiles: DataFile[] = [
      createTestDataFile('file1.parquet', 1000, encodeInt(0), encodeInt(50)),
      createTestDataFile('file2.parquet', 1000, encodeInt(40), encodeInt(100)),
    ];

    const filter = {};

    const result = filterDataFiles(dataFiles, filter, configs, fieldIdMap);

    expect(result).toHaveLength(2);
  });

  it('should return all files with non-shredded field filter', () => {
    const dataFiles: DataFile[] = [
      createTestDataFile('file1.parquet', 1000, encodeInt(0), encodeInt(50)),
      createTestDataFile('file2.parquet', 1000, encodeInt(40), encodeInt(100)),
    ];

    // Filter on a field that is not shredded
    const filter = {
      '$data.unknownField': { $eq: 'value' },
    };

    const result = filterDataFiles(dataFiles, filter, configs, fieldIdMap);

    // Can't prune without stats, return all
    expect(result).toHaveLength(2);
  });

  it('should handle $eq filter', () => {
    const dataFiles: DataFile[] = [
      createTestDataFile('file1.parquet', 1000, encodeInt(0), encodeInt(50)),
      createTestDataFile('file2.parquet', 1000, encodeInt(100), encodeInt(200)),
    ];

    // Filter: age == 75 (not in either file)
    const filter = {
      '$data.age': { $eq: 75 },
    };

    const result = filterDataFiles(dataFiles, filter, configs, fieldIdMap);

    expect(result).toHaveLength(0);
  });

  it('should handle $in filter', () => {
    const dataFiles: DataFile[] = [
      createTestDataFile('file1.parquet', 1000, encodeInt(0), encodeInt(50)),
      createTestDataFile('file2.parquet', 1000, encodeInt(100), encodeInt(200)),
    ];

    // Filter: age IN [25, 150] - one point in file1, one point in file2
    const filter = {
      '$data.age': { $in: [25, 150] },
    };

    const result = filterDataFiles(dataFiles, filter, configs, fieldIdMap);

    expect(result).toHaveLength(2);
  });

  it('should handle multiple shredded fields in filter', () => {
    const ageFieldId = fieldIdMap.get('$data.typed_value.age.typed_value')!;
    const nameFieldId = fieldIdMap.get('$data.typed_value.name.typed_value')!;

    const dataFiles: DataFile[] = [
      {
        content: 0,
        'file-path': 'file1.parquet',
        'file-format': 'parquet',
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 4096,
        'lower-bounds': {
          [ageFieldId]: encodeInt(20),
          [nameFieldId]: encodeString('aaa'),
        },
        'upper-bounds': {
          [ageFieldId]: encodeInt(40),
          [nameFieldId]: encodeString('mmm'),
        },
      },
      {
        content: 0,
        'file-path': 'file2.parquet',
        'file-format': 'parquet',
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 4096,
        'lower-bounds': {
          [ageFieldId]: encodeInt(30),
          [nameFieldId]: encodeString('nnn'),
        },
        'upper-bounds': {
          [ageFieldId]: encodeInt(60),
          [nameFieldId]: encodeString('zzz'),
        },
      },
    ];

    // Filter: age >= 25 AND name >= 'ppp'
    const filter = {
      '$data.age': { $gte: 25 },
      '$data.name': { $gte: 'ppp' },
    };

    const result = filterDataFiles(dataFiles, filter, configs, fieldIdMap);

    // file1: age 20-40 overlaps age>=25, name 'aaa'-'mmm' does NOT overlap name>='ppp'
    // file2: age 30-60 overlaps age>=25, name 'nnn'-'zzz' overlaps name>='ppp'
    expect(result).toHaveLength(1);
    expect(result[0]['file-path']).toBe('file2.parquet');
  });

  it('should handle files without stats (return all)', () => {
    const dataFiles: DataFile[] = [
      {
        content: 0,
        'file-path': 'file1.parquet',
        'file-format': 'parquet',
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 4096,
        // No lower-bounds or upper-bounds
      },
    ];

    const filter = {
      '$data.age': { $gte: 50 },
    };

    const result = filterDataFiles(dataFiles, filter, configs, fieldIdMap);

    // Without stats, we can't prune - return all
    expect(result).toHaveLength(1);
  });
});

// ============================================================================
// filterDataFilesWithStats Tests
// ============================================================================

describe('filterDataFilesWithStats', () => {
  const configs: readonly VariantShredPropertyConfig[] = [
    {
      columnName: '$data',
      fields: ['age', 'name'],
      fieldTypes: { age: 'int', name: 'string' },
    },
  ];

  const fieldIdMap = assignShreddedFieldIds(configs, 1000);

  it('should return filter statistics', () => {
    const dataFiles: DataFile[] = [
      createTestDataFile('file1.parquet', 1000, encodeInt(0), encodeInt(30)),
      createTestDataFile('file2.parquet', 1000, encodeInt(40), encodeInt(80)),
      createTestDataFile('file3.parquet', 1000, encodeInt(90), encodeInt(150)),
    ];

    const filter = {
      '$data.age': { $gte: 50, $lte: 70 },
    };

    const { files, stats } = filterDataFilesWithStats(dataFiles, filter, configs, fieldIdMap);

    expect(files).toHaveLength(1);
    expect(stats.totalFiles).toBe(3);
    expect(stats.skippedFiles).toBe(2);
    expect(stats.skippedByField.get('$data.age')).toBe(2);
  });

  it('should track skips per field', () => {
    const ageFieldId = fieldIdMap.get('$data.typed_value.age.typed_value')!;
    const nameFieldId = fieldIdMap.get('$data.typed_value.name.typed_value')!;

    const dataFiles: DataFile[] = [
      {
        content: 0,
        'file-path': 'file1.parquet',
        'file-format': 'parquet',
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 4096,
        'lower-bounds': {
          [ageFieldId]: encodeInt(0),
          [nameFieldId]: encodeString('aaa'),
        },
        'upper-bounds': {
          [ageFieldId]: encodeInt(20),
          [nameFieldId]: encodeString('ccc'),
        },
      },
      {
        content: 0,
        'file-path': 'file2.parquet',
        'file-format': 'parquet',
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 4096,
        'lower-bounds': {
          [ageFieldId]: encodeInt(50),
          [nameFieldId]: encodeString('xxx'),
        },
        'upper-bounds': {
          [ageFieldId]: encodeInt(80),
          [nameFieldId]: encodeString('zzz'),
        },
      },
    ];

    const filter = {
      '$data.age': { $gte: 30, $lte: 60 },
      '$data.name': { $gte: 'ddd', $lte: 'mmm' },
    };

    const { stats } = filterDataFilesWithStats(dataFiles, filter, configs, fieldIdMap);

    expect(stats.totalFiles).toBe(2);
    // file1 skipped by age (0-20 doesn't overlap 30-60)
    // file2 skipped by name ('xxx'-'zzz' doesn't overlap 'ddd'-'mmm')
    expect(stats.skippedFiles).toBe(2);
  });
});

// ============================================================================
// Performance Tests
// ============================================================================

describe('Performance', () => {
  const configs: readonly VariantShredPropertyConfig[] = [
    {
      columnName: '$data',
      fields: ['id', 'timestamp', 'value'],
      fieldTypes: { id: 'long', timestamp: 'timestamp', value: 'double' },
    },
  ];

  const fieldIdMap = assignShreddedFieldIds(configs, 1000);

  it('should handle large number of files efficiently', () => {
    const numFiles = 10000;
    const dataFiles: DataFile[] = [];

    for (let i = 0; i < numFiles; i++) {
      dataFiles.push(
        createTestDataFile(
          `file${i}.parquet`,
          1000,
          encodeLong(BigInt(i * 1000)),
          encodeLong(BigInt(i * 1000 + 999))
        )
      );
    }

    const filter = {
      '$data.id': { $gte: BigInt(5000000), $lte: BigInt(5001000) },
    };

    const start = performance.now();
    const result = filterDataFiles(dataFiles, filter, configs, fieldIdMap);
    const elapsed = performance.now() - start;

    // Should complete in reasonable time (< 100ms for 10k files)
    expect(elapsed).toBeLessThan(100);
    // Should filter to approximately 2 files
    expect(result.length).toBeLessThan(10);
  });

  it('should use efficient field ID lookup', () => {
    // Verify that field ID map is used efficiently
    const idFieldId = fieldIdMap.get('$data.typed_value.id.typed_value');
    const timestampFieldId = fieldIdMap.get('$data.typed_value.timestamp.typed_value');
    const valueFieldId = fieldIdMap.get('$data.typed_value.value.typed_value');

    expect(idFieldId).toBe(1000);
    expect(timestampFieldId).toBe(1001);
    expect(valueFieldId).toBe(1002);
  });
});

// ============================================================================
// Edge Cases
// ============================================================================

describe('Edge Cases', () => {
  const configs: readonly VariantShredPropertyConfig[] = [
    {
      columnName: '$data',
      fields: ['value'],
      fieldTypes: { value: 'int' },
    },
  ];

  const fieldIdMap = assignShreddedFieldIds(configs, 1000);

  it('should handle empty data files array', () => {
    const filter = { '$data.value': { $eq: 50 } };
    const result = filterDataFiles([], filter, configs, fieldIdMap);
    expect(result).toHaveLength(0);
  });

  it('should handle filter with unrecognized operator', () => {
    const dataFiles: DataFile[] = [
      createTestDataFile('file1.parquet', 1000, encodeInt(0), encodeInt(100)),
    ];

    const filter = {
      '$data.value': { $regex: 'test.*' }, // Not a range operator
    };

    // Unrecognized operators should not prune (safe default)
    const result = filterDataFiles(dataFiles, filter, configs, fieldIdMap);
    expect(result).toHaveLength(1);
  });

  it('should handle $ne operator (cannot prune effectively)', () => {
    const dataFiles: DataFile[] = [
      createTestDataFile('file1.parquet', 1000, encodeInt(50), encodeInt(50)),
      createTestDataFile('file2.parquet', 1000, encodeInt(0), encodeInt(100)),
    ];

    const filter = {
      '$data.value': { $ne: 50 },
    };

    // $ne on a single-value file could prune, but on range files it generally can't
    const result = filterDataFiles(dataFiles, filter, configs, fieldIdMap);
    // file1 contains only 50, so it could be skipped
    // file2 contains other values, so it must be returned
    expect(result).toHaveLength(1);
    expect(result[0]['file-path']).toBe('file2.parquet');
  });

  it('should handle null values in filter', () => {
    const dataFiles: DataFile[] = [
      createTestDataFile('file1.parquet', 1000, encodeInt(0), encodeInt(100)),
    ];

    const filter = {
      '$data.value': null,
    };

    // Null filter should not crash, return all files
    const result = filterDataFiles(dataFiles, filter, configs, fieldIdMap);
    expect(result).toHaveLength(1);
  });

  it('should handle implicit $eq (shorthand notation)', () => {
    const dataFiles: DataFile[] = [
      createTestDataFile('file1.parquet', 1000, encodeInt(0), encodeInt(50)),
      createTestDataFile('file2.parquet', 1000, encodeInt(100), encodeInt(200)),
    ];

    // Shorthand: { field: value } means { field: { $eq: value } }
    const filter = {
      '$data.value': 25,
    };

    const result = filterDataFiles(dataFiles, filter, configs, fieldIdMap);
    expect(result).toHaveLength(1);
    expect(result[0]['file-path']).toBe('file1.parquet');
  });
});
