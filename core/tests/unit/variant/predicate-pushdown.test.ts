/**
 * Tests for Predicate Pushdown with Variant Filters
 *
 * These tests verify that scan planning can use shredded column statistics
 * to skip data files that definitely don't match the filter predicate.
 *
 * @see https://iceberg.apache.org/spec/#variant-shredding
 */

import { describe, it, expect } from 'vitest';
import type {
  DataFile,
  VariantShredPropertyConfig,
  PredicateResult,
} from '../../../src/index.js';
import {
  shouldSkipDataFile,
  boundsOverlapValue,
  evaluateInPredicate,
  serializeShreddedBound,
  assignShreddedFieldIds,
} from '../../../src/index.js';

// Helper to create a basic DataFile for testing
function createDataFile(overrides: Partial<DataFile> = {}): DataFile {
  return {
    content: 0,
    'file-path': 's3://bucket/data/file.parquet',
    'file-format': 'parquet',
    partition: {},
    'record-count': 1000,
    'file-size-in-bytes': 4096,
    ...overrides,
  };
}

// Helper to create variant shred configs
function createConfig(
  columnName: string,
  fields: string[],
  fieldTypes: Record<string, 'string' | 'int' | 'long' | 'double' | 'boolean' | 'timestamp'>
): VariantShredPropertyConfig {
  return { columnName, fields, fieldTypes };
}

describe('Predicate Pushdown for Variant Filters', () => {
  // ==========================================================================
  // Basic shouldSkipDataFile Tests
  // ==========================================================================

  describe('Basic shouldSkipDataFile tests', () => {
    it('should return skip=false when bounds overlap filter value', () => {
      const configs = [createConfig('$data', ['title'], { title: 'string' })];
      const fieldIdMap = assignShreddedFieldIds(configs, 1000);

      const dataFile = createDataFile({
        'lower-bounds': { 1000: serializeShreddedBound('aaa', 'string') },
        'upper-bounds': { 1000: serializeShreddedBound('zzz', 'string') },
      });

      const result = shouldSkipDataFile(
        dataFile,
        { '$data.title': { $eq: 'hello' } },
        configs,
        fieldIdMap
      );

      expect(result.skip).toBe(false);
    });

    it('should return skip=true when bounds do not overlap filter value', () => {
      const configs = [createConfig('$data', ['title'], { title: 'string' })];
      const fieldIdMap = assignShreddedFieldIds(configs, 1000);

      const dataFile = createDataFile({
        'lower-bounds': { 1000: serializeShreddedBound('aaa', 'string') },
        'upper-bounds': { 1000: serializeShreddedBound('bbb', 'string') },
      });

      const result = shouldSkipDataFile(
        dataFile,
        { '$data.title': { $eq: 'zzz' } },
        configs,
        fieldIdMap
      );

      expect(result.skip).toBe(true);
      expect(result.reason).toContain('title');
    });

    it('should return skip=false when no stats available (safe default)', () => {
      const configs = [createConfig('$data', ['title'], { title: 'string' })];
      const fieldIdMap = assignShreddedFieldIds(configs, 1000);

      // DataFile with no bounds stats
      const dataFile = createDataFile({});

      const result = shouldSkipDataFile(
        dataFile,
        { '$data.title': { $eq: 'hello' } },
        configs,
        fieldIdMap
      );

      expect(result.skip).toBe(false);
    });

    it('should return skip=false for non-shredded field filters', () => {
      const configs = [createConfig('$data', ['title'], { title: 'string' })];
      const fieldIdMap = assignShreddedFieldIds(configs, 1000);

      const dataFile = createDataFile({
        'lower-bounds': { 1000: serializeShreddedBound('aaa', 'string') },
        'upper-bounds': { 1000: serializeShreddedBound('bbb', 'string') },
      });

      // Filter on a field that's not shredded
      const result = shouldSkipDataFile(
        dataFile,
        { '$data.unknown_field': { $eq: 'value' } },
        configs,
        fieldIdMap
      );

      expect(result.skip).toBe(false);
    });

    it('should handle direct equality filter (shorthand)', () => {
      const configs = [createConfig('$data', ['title'], { title: 'string' })];
      const fieldIdMap = assignShreddedFieldIds(configs, 1000);

      const dataFile = createDataFile({
        'lower-bounds': { 1000: serializeShreddedBound('aaa', 'string') },
        'upper-bounds': { 1000: serializeShreddedBound('bbb', 'string') },
      });

      // Shorthand filter: { 'path': 'value' } means { 'path': { $eq: 'value' } }
      const result = shouldSkipDataFile(
        dataFile,
        { '$data.title': 'zzz' },
        configs,
        fieldIdMap
      );

      expect(result.skip).toBe(true);
    });
  });

  // ==========================================================================
  // Operator-specific Skip Tests
  // ==========================================================================

  describe('Operator-specific skip tests', () => {
    describe('$eq operator', () => {
      it('should skip if value is below lower bound', () => {
        const configs = [createConfig('$data', ['year'], { year: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(2000, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(2020, 'int') },
        });

        const result = shouldSkipDataFile(
          dataFile,
          { '$data.year': { $eq: 1990 } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(true);
      });

      it('should skip if value is above upper bound', () => {
        const configs = [createConfig('$data', ['year'], { year: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(2000, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(2020, 'int') },
        });

        const result = shouldSkipDataFile(
          dataFile,
          { '$data.year': { $eq: 2030 } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(true);
      });

      it('should not skip if value is within bounds', () => {
        const configs = [createConfig('$data', ['year'], { year: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(2000, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(2020, 'int') },
        });

        const result = shouldSkipDataFile(
          dataFile,
          { '$data.year': { $eq: 2010 } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(false);
      });
    });

    describe('$gt operator', () => {
      it('should skip if upper bound <= value', () => {
        const configs = [createConfig('$data', ['year'], { year: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(2000, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(2020, 'int') },
        });

        const result = shouldSkipDataFile(
          dataFile,
          { '$data.year': { $gt: 2020 } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(true);
      });

      it('should skip if upper bound < value', () => {
        const configs = [createConfig('$data', ['year'], { year: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(2000, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(2020, 'int') },
        });

        const result = shouldSkipDataFile(
          dataFile,
          { '$data.year': { $gt: 2030 } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(true);
      });

      it('should not skip if upper bound > value', () => {
        const configs = [createConfig('$data', ['year'], { year: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(2000, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(2020, 'int') },
        });

        const result = shouldSkipDataFile(
          dataFile,
          { '$data.year': { $gt: 2010 } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(false);
      });
    });

    describe('$gte operator', () => {
      it('should skip if upper bound < value', () => {
        const configs = [createConfig('$data', ['year'], { year: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(2000, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(2020, 'int') },
        });

        const result = shouldSkipDataFile(
          dataFile,
          { '$data.year': { $gte: 2021 } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(true);
      });

      it('should not skip if upper bound == value', () => {
        const configs = [createConfig('$data', ['year'], { year: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(2000, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(2020, 'int') },
        });

        const result = shouldSkipDataFile(
          dataFile,
          { '$data.year': { $gte: 2020 } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(false);
      });

      it('should not skip if upper bound > value', () => {
        const configs = [createConfig('$data', ['year'], { year: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(2000, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(2020, 'int') },
        });

        const result = shouldSkipDataFile(
          dataFile,
          { '$data.year': { $gte: 2010 } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(false);
      });
    });

    describe('$lt operator', () => {
      it('should skip if lower bound >= value', () => {
        const configs = [createConfig('$data', ['year'], { year: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(2000, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(2020, 'int') },
        });

        const result = shouldSkipDataFile(
          dataFile,
          { '$data.year': { $lt: 2000 } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(true);
      });

      it('should skip if lower bound > value', () => {
        const configs = [createConfig('$data', ['year'], { year: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(2000, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(2020, 'int') },
        });

        const result = shouldSkipDataFile(
          dataFile,
          { '$data.year': { $lt: 1990 } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(true);
      });

      it('should not skip if lower bound < value', () => {
        const configs = [createConfig('$data', ['year'], { year: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(2000, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(2020, 'int') },
        });

        const result = shouldSkipDataFile(
          dataFile,
          { '$data.year': { $lt: 2010 } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(false);
      });
    });

    describe('$lte operator', () => {
      it('should skip if lower bound > value', () => {
        const configs = [createConfig('$data', ['year'], { year: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(2000, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(2020, 'int') },
        });

        const result = shouldSkipDataFile(
          dataFile,
          { '$data.year': { $lte: 1999 } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(true);
      });

      it('should not skip if lower bound == value', () => {
        const configs = [createConfig('$data', ['year'], { year: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(2000, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(2020, 'int') },
        });

        const result = shouldSkipDataFile(
          dataFile,
          { '$data.year': { $lte: 2000 } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(false);
      });

      it('should not skip if lower bound < value', () => {
        const configs = [createConfig('$data', ['year'], { year: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(2000, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(2020, 'int') },
        });

        const result = shouldSkipDataFile(
          dataFile,
          { '$data.year': { $lte: 2010 } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(false);
      });
    });

    describe('$ne operator', () => {
      it('should skip if lower == upper == value (all same value)', () => {
        const configs = [createConfig('$data', ['year'], { year: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        // All values in the file are 2010
        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(2010, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(2010, 'int') },
        });

        const result = shouldSkipDataFile(
          dataFile,
          { '$data.year': { $ne: 2010 } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(true);
      });

      it('should not skip if bounds have different values', () => {
        const configs = [createConfig('$data', ['year'], { year: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(2000, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(2020, 'int') },
        });

        const result = shouldSkipDataFile(
          dataFile,
          { '$data.year': { $ne: 2010 } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(false);
      });
    });

    describe('$in operator', () => {
      it('should skip if no values in set overlap bounds', () => {
        const configs = [createConfig('$data', ['year'], { year: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(2000, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(2010, 'int') },
        });

        const result = shouldSkipDataFile(
          dataFile,
          { '$data.year': { $in: [1990, 1995, 2020, 2030] } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(true);
      });

      it('should not skip if any value in set overlaps bounds', () => {
        const configs = [createConfig('$data', ['year'], { year: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(2000, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(2010, 'int') },
        });

        const result = shouldSkipDataFile(
          dataFile,
          { '$data.year': { $in: [1990, 2005, 2030] } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(false);
      });

      it('should not skip if $in array is empty (matches nothing but safe default)', () => {
        const configs = [createConfig('$data', ['year'], { year: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(2000, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(2010, 'int') },
        });

        // Empty $in should skip (no values can match)
        const result = shouldSkipDataFile(
          dataFile,
          { '$data.year': { $in: [] } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(true);
      });
    });
  });

  // ==========================================================================
  // Type-specific Bounds Comparison Tests
  // ==========================================================================

  describe('Type-specific bounds comparison tests', () => {
    describe('String comparison (lexicographic)', () => {
      it('should use lexicographic ordering for strings', () => {
        const configs = [createConfig('$data', ['name'], { name: 'string' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound('apple', 'string') },
          'upper-bounds': { 1000: serializeShreddedBound('banana', 'string') },
        });

        // 'cherry' > 'banana' (lexicographically)
        const result = shouldSkipDataFile(
          dataFile,
          { '$data.name': { $eq: 'cherry' } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(true);
      });

      it('should handle string prefixes correctly', () => {
        const configs = [createConfig('$data', ['name'], { name: 'string' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound('app', 'string') },
          'upper-bounds': { 1000: serializeShreddedBound('application', 'string') },
        });

        // 'apple' is between 'app' and 'application'
        const result = shouldSkipDataFile(
          dataFile,
          { '$data.name': { $eq: 'apple' } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(false);
      });
    });

    describe('Numeric comparison', () => {
      it('should handle negative numbers correctly', () => {
        const configs = [createConfig('$data', ['value'], { value: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(-100, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(-10, 'int') },
        });

        // 0 > -10, so should skip
        const result = shouldSkipDataFile(
          dataFile,
          { '$data.value': { $eq: 0 } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(true);
      });

      it('should handle negative to positive range', () => {
        const configs = [createConfig('$data', ['value'], { value: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(-100, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(100, 'int') },
        });

        const result = shouldSkipDataFile(
          dataFile,
          { '$data.value': { $eq: 0 } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(false);
      });

      it('should handle double precision correctly', () => {
        const configs = [createConfig('$data', ['rating'], { rating: 'double' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(1.5, 'double') },
          'upper-bounds': { 1000: serializeShreddedBound(4.5, 'double') },
        });

        const result = shouldSkipDataFile(
          dataFile,
          { '$data.rating': { $eq: 3.14159 } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(false);
      });

      it('should handle long (bigint) values', () => {
        const configs = [createConfig('$data', ['bigval'], { bigval: 'long' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(BigInt('1000000000000'), 'long') },
          'upper-bounds': { 1000: serializeShreddedBound(BigInt('9000000000000'), 'long') },
        });

        const result = shouldSkipDataFile(
          dataFile,
          { '$data.bigval': { $eq: BigInt('5000000000000') } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(false);
      });
    });

    describe('Timestamp comparison', () => {
      it('should handle microsecond timestamps', () => {
        const configs = [createConfig('$data', ['created_at'], { created_at: 'timestamp' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        // Jan 1 2020 to Dec 31 2020 in microseconds
        const lower = BigInt(1577836800000000); // 2020-01-01
        const upper = BigInt(1609459199000000); // 2020-12-31

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(lower, 'timestamp') },
          'upper-bounds': { 1000: serializeShreddedBound(upper, 'timestamp') },
        });

        // July 1 2020 - should be in range
        const july = BigInt(1593561600000000);
        const result = shouldSkipDataFile(
          dataFile,
          { '$data.created_at': { $eq: july } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(false);
      });

      it('should skip if timestamp is outside range', () => {
        const configs = [createConfig('$data', ['created_at'], { created_at: 'timestamp' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        // Year 2020 in microseconds
        const lower = BigInt(1577836800000000); // 2020-01-01
        const upper = BigInt(1609459199000000); // 2020-12-31

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(lower, 'timestamp') },
          'upper-bounds': { 1000: serializeShreddedBound(upper, 'timestamp') },
        });

        // Year 2019 - should skip
        const early2019 = BigInt(1546300800000000); // 2019-01-01
        const result = shouldSkipDataFile(
          dataFile,
          { '$data.created_at': { $eq: early2019 } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(true);
      });
    });

    describe('Boolean comparison', () => {
      it('should handle boolean bounds (false < true)', () => {
        const configs = [createConfig('$data', ['active'], { active: 'boolean' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        // All values are true
        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(true, 'boolean') },
          'upper-bounds': { 1000: serializeShreddedBound(true, 'boolean') },
        });

        const result = shouldSkipDataFile(
          dataFile,
          { '$data.active': { $eq: false } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(true);
      });

      it('should not skip if boolean range includes both values', () => {
        const configs = [createConfig('$data', ['active'], { active: 'boolean' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        // Range from false to true
        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(false, 'boolean') },
          'upper-bounds': { 1000: serializeShreddedBound(true, 'boolean') },
        });

        const resultTrue = shouldSkipDataFile(
          dataFile,
          { '$data.active': { $eq: true } },
          configs,
          fieldIdMap
        );

        const resultFalse = shouldSkipDataFile(
          dataFile,
          { '$data.active': { $eq: false } },
          configs,
          fieldIdMap
        );

        expect(resultTrue.skip).toBe(false);
        expect(resultFalse.skip).toBe(false);
      });
    });
  });

  // ==========================================================================
  // Multi-field Filter Tests
  // ==========================================================================

  describe('Multi-field filter tests', () => {
    describe('AND filter (implicit)', () => {
      it('should skip if any field does not match (first field fails)', () => {
        const configs = [
          createConfig('$data', ['year', 'rating'], { year: 'int', rating: 'double' }),
        ];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': {
            1000: serializeShreddedBound(2000, 'int'),
            1001: serializeShreddedBound(1.0, 'double'),
          },
          'upper-bounds': {
            1000: serializeShreddedBound(2010, 'int'),
            1001: serializeShreddedBound(5.0, 'double'),
          },
        });

        // year=2020 is outside bounds, rating=3.0 is inside bounds
        const result = shouldSkipDataFile(
          dataFile,
          { '$data.year': { $eq: 2020 }, '$data.rating': { $eq: 3.0 } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(true);
      });

      it('should skip if any field does not match (second field fails)', () => {
        const configs = [
          createConfig('$data', ['year', 'rating'], { year: 'int', rating: 'double' }),
        ];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': {
            1000: serializeShreddedBound(2000, 'int'),
            1001: serializeShreddedBound(1.0, 'double'),
          },
          'upper-bounds': {
            1000: serializeShreddedBound(2010, 'int'),
            1001: serializeShreddedBound(5.0, 'double'),
          },
        });

        // year=2005 is inside bounds, rating=9.0 is outside bounds
        const result = shouldSkipDataFile(
          dataFile,
          { '$data.year': { $eq: 2005 }, '$data.rating': { $eq: 9.0 } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(true);
      });

      it('should not skip if all fields match', () => {
        const configs = [
          createConfig('$data', ['year', 'rating'], { year: 'int', rating: 'double' }),
        ];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': {
            1000: serializeShreddedBound(2000, 'int'),
            1001: serializeShreddedBound(1.0, 'double'),
          },
          'upper-bounds': {
            1000: serializeShreddedBound(2010, 'int'),
            1001: serializeShreddedBound(5.0, 'double'),
          },
        });

        // Both inside bounds
        const result = shouldSkipDataFile(
          dataFile,
          { '$data.year': { $eq: 2005 }, '$data.rating': { $eq: 3.0 } },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(false);
      });
    });

    describe('$and operator (explicit)', () => {
      it('should skip if any branch does not match', () => {
        const configs = [createConfig('$data', ['year'], { year: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(2000, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(2010, 'int') },
        });

        const result = shouldSkipDataFile(
          dataFile,
          { $and: [{ '$data.year': { $gte: 2005 } }, { '$data.year': { $lt: 2000 } }] },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(true);
      });
    });

    describe('$or operator', () => {
      it('should skip only if all branches do not match', () => {
        const configs = [createConfig('$data', ['year'], { year: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(2000, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(2010, 'int') },
        });

        // Both branches fail: year < 1990 OR year > 2020
        const result = shouldSkipDataFile(
          dataFile,
          { $or: [{ '$data.year': { $lt: 1990 } }, { '$data.year': { $gt: 2020 } }] },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(true);
      });

      it('should not skip if any branch matches', () => {
        const configs = [createConfig('$data', ['year'], { year: 'int' })];
        const fieldIdMap = assignShreddedFieldIds(configs, 1000);

        const dataFile = createDataFile({
          'lower-bounds': { 1000: serializeShreddedBound(2000, 'int') },
          'upper-bounds': { 1000: serializeShreddedBound(2010, 'int') },
        });

        // First branch fails (year < 1990), second branch matches (year == 2005)
        const result = shouldSkipDataFile(
          dataFile,
          { $or: [{ '$data.year': { $lt: 1990 } }, { '$data.year': { $eq: 2005 } }] },
          configs,
          fieldIdMap
        );

        expect(result.skip).toBe(false);
      });
    });
  });

  // ==========================================================================
  // Missing Stats Handling Tests
  // ==========================================================================

  describe('Missing stats handling tests', () => {
    it('should handle missing lower bound gracefully', () => {
      const configs = [createConfig('$data', ['year'], { year: 'int' })];
      const fieldIdMap = assignShreddedFieldIds(configs, 1000);

      // Only upper bound, no lower bound
      const dataFile = createDataFile({
        'upper-bounds': { 1000: serializeShreddedBound(2010, 'int') },
      });

      const result = shouldSkipDataFile(
        dataFile,
        { '$data.year': { $eq: 2005 } },
        configs,
        fieldIdMap
      );

      // With missing lower bound, we can't definitively skip
      expect(result.skip).toBe(false);
    });

    it('should handle missing upper bound gracefully', () => {
      const configs = [createConfig('$data', ['year'], { year: 'int' })];
      const fieldIdMap = assignShreddedFieldIds(configs, 1000);

      // Only lower bound, no upper bound
      const dataFile = createDataFile({
        'lower-bounds': { 1000: serializeShreddedBound(2000, 'int') },
      });

      const result = shouldSkipDataFile(
        dataFile,
        { '$data.year': { $eq: 2005 } },
        configs,
        fieldIdMap
      );

      // With missing upper bound, we can't definitively skip
      expect(result.skip).toBe(false);
    });

    it('should handle missing both bounds gracefully', () => {
      const configs = [createConfig('$data', ['year'], { year: 'int' })];
      const fieldIdMap = assignShreddedFieldIds(configs, 1000);

      // No bounds at all
      const dataFile = createDataFile({});

      const result = shouldSkipDataFile(
        dataFile,
        { '$data.year': { $eq: 2005 } },
        configs,
        fieldIdMap
      );

      // With no bounds, we can't skip
      expect(result.skip).toBe(false);
    });

    it('should handle field ID not present in bounds', () => {
      const configs = [createConfig('$data', ['year', 'title'], { year: 'int', title: 'string' })];
      const fieldIdMap = assignShreddedFieldIds(configs, 1000);

      // Only year (1000) has bounds, title (1001) does not
      const dataFile = createDataFile({
        'lower-bounds': { 1000: serializeShreddedBound(2000, 'int') },
        'upper-bounds': { 1000: serializeShreddedBound(2010, 'int') },
      });

      const result = shouldSkipDataFile(
        dataFile,
        { '$data.title': { $eq: 'hello' } },
        configs,
        fieldIdMap
      );

      // Field ID 1001 has no bounds, can't skip
      expect(result.skip).toBe(false);
    });
  });

  // ==========================================================================
  // Null Handling Tests
  // ==========================================================================

  describe('Null handling tests', () => {
    it('should check null counts for filter matching null', () => {
      const configs = [createConfig('$data', ['title'], { title: 'string' })];
      const fieldIdMap = assignShreddedFieldIds(configs, 1000);

      // File has zero null values
      const dataFile = createDataFile({
        'lower-bounds': { 1000: serializeShreddedBound('aaa', 'string') },
        'upper-bounds': { 1000: serializeShreddedBound('zzz', 'string') },
        'null-value-counts': { 1000: 0 },
      });

      // Filtering for null when null count is 0 should skip
      const result = shouldSkipDataFile(
        dataFile,
        { '$data.title': { $eq: null } },
        configs,
        fieldIdMap
      );

      expect(result.skip).toBe(true);
    });

    it('should not skip if null count is positive when filtering for null', () => {
      const configs = [createConfig('$data', ['title'], { title: 'string' })];
      const fieldIdMap = assignShreddedFieldIds(configs, 1000);

      // File has some null values
      const dataFile = createDataFile({
        'lower-bounds': { 1000: serializeShreddedBound('aaa', 'string') },
        'upper-bounds': { 1000: serializeShreddedBound('zzz', 'string') },
        'null-value-counts': { 1000: 10 },
      });

      const result = shouldSkipDataFile(
        dataFile,
        { '$data.title': { $eq: null } },
        configs,
        fieldIdMap
      );

      expect(result.skip).toBe(false);
    });

    it('should not skip when null count info is missing', () => {
      const configs = [createConfig('$data', ['title'], { title: 'string' })];
      const fieldIdMap = assignShreddedFieldIds(configs, 1000);

      // File has no null count info
      const dataFile = createDataFile({
        'lower-bounds': { 1000: serializeShreddedBound('aaa', 'string') },
        'upper-bounds': { 1000: serializeShreddedBound('zzz', 'string') },
      });

      const result = shouldSkipDataFile(
        dataFile,
        { '$data.title': { $eq: null } },
        configs,
        fieldIdMap
      );

      // No null count info, can't skip
      expect(result.skip).toBe(false);
    });

    it('should handle $ne null correctly (skip if all values are null)', () => {
      const configs = [createConfig('$data', ['title'], { title: 'string' })];
      const fieldIdMap = assignShreddedFieldIds(configs, 1000);

      // File where all values are null (no bounds, all nulls)
      const dataFile = createDataFile({
        'null-value-counts': { 1000: 1000 },
        'value-counts': { 1000: 1000 },
      });

      // Looking for non-null when all are null should skip
      const result = shouldSkipDataFile(
        dataFile,
        { '$data.title': { $ne: null } },
        configs,
        fieldIdMap
      );

      expect(result.skip).toBe(true);
    });

    it('should not skip $ne null when some non-null values exist', () => {
      const configs = [createConfig('$data', ['title'], { title: 'string' })];
      const fieldIdMap = assignShreddedFieldIds(configs, 1000);

      // File with some nulls and some non-nulls
      const dataFile = createDataFile({
        'lower-bounds': { 1000: serializeShreddedBound('aaa', 'string') },
        'upper-bounds': { 1000: serializeShreddedBound('zzz', 'string') },
        'null-value-counts': { 1000: 100 },
        'value-counts': { 1000: 1000 },
      });

      const result = shouldSkipDataFile(
        dataFile,
        { '$data.title': { $ne: null } },
        configs,
        fieldIdMap
      );

      expect(result.skip).toBe(false);
    });
  });

  // ==========================================================================
  // boundsOverlapValue Unit Tests
  // ==========================================================================

  describe('boundsOverlapValue', () => {
    it('should return true when value is within bounds', () => {
      expect(boundsOverlapValue(10, 20, '$eq', 15, 'int')).toBe(true);
    });

    it('should return false when value is below lower bound ($eq)', () => {
      expect(boundsOverlapValue(10, 20, '$eq', 5, 'int')).toBe(false);
    });

    it('should return false when value is above upper bound ($eq)', () => {
      expect(boundsOverlapValue(10, 20, '$eq', 25, 'int')).toBe(false);
    });

    it('should return true at lower boundary ($eq)', () => {
      expect(boundsOverlapValue(10, 20, '$eq', 10, 'int')).toBe(true);
    });

    it('should return true at upper boundary ($eq)', () => {
      expect(boundsOverlapValue(10, 20, '$eq', 20, 'int')).toBe(true);
    });
  });

  // ==========================================================================
  // evaluateInPredicate Unit Tests
  // ==========================================================================

  describe('evaluateInPredicate', () => {
    it('should return true if any value overlaps bounds', () => {
      expect(evaluateInPredicate(10, 20, [5, 15, 25], 'int')).toBe(true);
    });

    it('should return false if no values overlap bounds', () => {
      expect(evaluateInPredicate(10, 20, [5, 25, 30], 'int')).toBe(false);
    });

    it('should return false for empty values array', () => {
      expect(evaluateInPredicate(10, 20, [], 'int')).toBe(false);
    });

    it('should handle string values', () => {
      expect(evaluateInPredicate('apple', 'cherry', ['banana', 'date'], 'string')).toBe(true);
    });

    it('should handle boundary values', () => {
      expect(evaluateInPredicate(10, 20, [10, 20], 'int')).toBe(true);
    });
  });
});
