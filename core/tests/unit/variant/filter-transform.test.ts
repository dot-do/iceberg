/**
 * Tests for Variant Filter Transformation
 *
 * These tests verify the transformVariantFilter function which rewrites
 * variant field filters to use statistics paths for query optimization.
 *
 * @see https://iceberg.apache.org/spec/#variant-shredding
 */

import { describe, it, expect } from 'vitest';
import type { VariantShredPropertyConfig, TransformResult } from '../../../src/index.js';
import { transformVariantFilter, isComparisonOperator } from '../../../src/index.js';

// Test fixture: shred config for $data column
const dataConfig: VariantShredPropertyConfig = {
  columnName: '$data',
  fields: ['title', 'year', 'rating', 'genre', 'status'],
  fieldTypes: {
    title: 'string',
    year: 'int',
    rating: 'double',
    genre: 'string',
    status: 'string',
  },
};

// Test fixture: shred config for $index column
const indexConfig: VariantShredPropertyConfig = {
  columnName: '$index',
  fields: ['key', 'value', 'timestamp'],
  fieldTypes: {
    key: 'string',
    value: 'long',
    timestamp: 'timestamp',
  },
};

// Combined configs for multi-column tests
const allConfigs: readonly VariantShredPropertyConfig[] = [dataConfig, indexConfig];

describe('Variant Filter Transformation', () => {
  describe('Basic filter transformation', () => {
    it('should transform simple variant field filter to statistics path', () => {
      const filter = { '$data.title': 'foo' };
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({
        '$data.typed_value.title.typed_value': 'foo',
      });
      expect(result.transformedPaths).toContain('$data.title');
      expect(result.untransformedPaths).toHaveLength(0);
    });

    it('should pass through non-variant fields unchanged', () => {
      const filter = { id: 123, name: 'test' };
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({ id: 123, name: 'test' });
      expect(result.transformedPaths).toHaveLength(0);
      expect(result.untransformedPaths).toHaveLength(0);
    });

    it('should return empty object for empty filter', () => {
      const filter = {};
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({});
      expect(result.transformedPaths).toHaveLength(0);
      expect(result.untransformedPaths).toHaveLength(0);
    });

    it('should handle mixed variant and non-variant fields', () => {
      const filter = { '$data.title': 'foo', id: 123 };
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({
        '$data.typed_value.title.typed_value': 'foo',
        id: 123,
      });
      expect(result.transformedPaths).toContain('$data.title');
    });
  });

  describe('Operator transformation', () => {
    it('should transform $eq operator', () => {
      const filter = { '$data.year': { $eq: 2020 } };
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({
        '$data.typed_value.year.typed_value': { $eq: 2020 },
      });
      expect(result.transformedPaths).toContain('$data.year');
    });

    it('should transform $gt operator', () => {
      const filter = { '$data.year': { $gt: 2000 } };
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({
        '$data.typed_value.year.typed_value': { $gt: 2000 },
      });
    });

    it('should transform $gte operator', () => {
      const filter = { '$data.year': { $gte: 2000 } };
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({
        '$data.typed_value.year.typed_value': { $gte: 2000 },
      });
    });

    it('should transform $lt operator', () => {
      const filter = { '$data.year': { $lt: 2025 } };
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({
        '$data.typed_value.year.typed_value': { $lt: 2025 },
      });
    });

    it('should transform $lte operator', () => {
      const filter = { '$data.year': { $lte: 2025 } };
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({
        '$data.typed_value.year.typed_value': { $lte: 2025 },
      });
    });

    it('should transform $ne operator', () => {
      const filter = { '$data.status': { $ne: 'deleted' } };
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({
        '$data.typed_value.status.typed_value': { $ne: 'deleted' },
      });
    });

    it('should transform $in operator', () => {
      const filter = { '$data.genre': { $in: ['action', 'comedy'] } };
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({
        '$data.typed_value.genre.typed_value': { $in: ['action', 'comedy'] },
      });
    });

    it('should transform $nin operator', () => {
      const filter = { '$data.genre': { $nin: ['horror', 'thriller'] } };
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({
        '$data.typed_value.genre.typed_value': { $nin: ['horror', 'thriller'] },
      });
    });

    it('should transform multiple operators on same field', () => {
      const filter = { '$data.year': { $gte: 2000, $lte: 2025 } };
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({
        '$data.typed_value.year.typed_value': { $gte: 2000, $lte: 2025 },
      });
    });
  });

  describe('Compound filter transformation', () => {
    it('should transform $and with multiple variant fields', () => {
      const filter = {
        $and: [{ '$data.year': { $gt: 2000 } }, { '$data.rating': { $gte: 4.0 } }],
      };
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({
        $and: [
          { '$data.typed_value.year.typed_value': { $gt: 2000 } },
          { '$data.typed_value.rating.typed_value': { $gte: 4.0 } },
        ],
      });
      expect(result.transformedPaths).toContain('$data.year');
      expect(result.transformedPaths).toContain('$data.rating');
    });

    it('should transform $or with variant and non-variant fields', () => {
      const filter = {
        $or: [{ '$data.year': 2020 }, { id: 123 }],
      };
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({
        $or: [{ '$data.typed_value.year.typed_value': 2020 }, { id: 123 }],
      });
    });

    it('should transform nested $and/$or', () => {
      const filter = {
        $and: [
          {
            $or: [{ '$data.year': 2020 }, { '$data.year': 2021 }],
          },
          { '$data.status': 'active' },
        ],
      };
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({
        $and: [
          {
            $or: [
              { '$data.typed_value.year.typed_value': 2020 },
              { '$data.typed_value.year.typed_value': 2021 },
            ],
          },
          { '$data.typed_value.status.typed_value': 'active' },
        ],
      });
    });

    it('should handle deeply nested compound filters', () => {
      const filter = {
        $and: [
          {
            $or: [
              { $and: [{ '$data.year': { $gte: 2000 } }, { '$data.rating': { $gt: 3 } }] },
              { '$data.status': 'featured' },
            ],
          },
          { id: { $ne: 0 } },
        ],
      };
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({
        $and: [
          {
            $or: [
              {
                $and: [
                  { '$data.typed_value.year.typed_value': { $gte: 2000 } },
                  { '$data.typed_value.rating.typed_value': { $gt: 3 } },
                ],
              },
              { '$data.typed_value.status.typed_value': 'featured' },
            ],
          },
          { id: { $ne: 0 } },
        ],
      });
    });

    it('should handle $not operator', () => {
      const filter = {
        $not: { '$data.status': 'deleted' },
      };
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({
        $not: { '$data.typed_value.status.typed_value': 'deleted' },
      });
    });
  });

  describe('Multiple columns transformation', () => {
    it('should transform fields from different variant columns', () => {
      const filter = {
        '$data.title': 'foo',
        '$index.key': 'bar',
      };
      const result = transformVariantFilter(filter, allConfigs);

      expect(result.filter).toEqual({
        '$data.typed_value.title.typed_value': 'foo',
        '$index.typed_value.key.typed_value': 'bar',
      });
      expect(result.transformedPaths).toContain('$data.title');
      expect(result.transformedPaths).toContain('$index.key');
    });

    it('should handle same field name in different columns', () => {
      // Add a config with overlapping field name
      const altConfig: VariantShredPropertyConfig = {
        columnName: '$meta',
        fields: ['title', 'description'],
        fieldTypes: {
          title: 'string',
          description: 'string',
        },
      };

      const filter = {
        '$data.title': 'foo',
        '$meta.title': 'bar',
      };
      const result = transformVariantFilter(filter, [dataConfig, altConfig]);

      expect(result.filter).toEqual({
        '$data.typed_value.title.typed_value': 'foo',
        '$meta.typed_value.title.typed_value': 'bar',
      });
    });

    it('should handle compound filters with multiple columns', () => {
      const filter = {
        $and: [
          { '$data.year': { $gte: 2000 } },
          { '$index.timestamp': { $lt: 1704067200000 } },
        ],
      };
      const result = transformVariantFilter(filter, allConfigs);

      expect(result.filter).toEqual({
        $and: [
          { '$data.typed_value.year.typed_value': { $gte: 2000 } },
          { '$index.typed_value.timestamp.typed_value': { $lt: 1704067200000 } },
        ],
      });
    });
  });

  describe('Non-shredded field handling', () => {
    it('should NOT transform non-shredded variant field (returns original path)', () => {
      const filter = { '$data.nonShredded': 'value' };
      const result = transformVariantFilter(filter, [dataConfig]);

      // Non-shredded fields should keep original path
      expect(result.filter).toEqual({ '$data.nonShredded': 'value' });
      expect(result.untransformedPaths).toContain('$data.nonShredded');
      expect(result.transformedPaths).not.toContain('$data.nonShredded');
    });

    it('should mark non-shredded fields in compound filters', () => {
      const filter = {
        $and: [{ '$data.title': 'foo' }, { '$data.notShredded': 'bar' }],
      };
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({
        $and: [
          { '$data.typed_value.title.typed_value': 'foo' },
          { '$data.notShredded': 'bar' },
        ],
      });
      expect(result.transformedPaths).toContain('$data.title');
      expect(result.untransformedPaths).toContain('$data.notShredded');
    });

    it('should handle unknown column names as non-variant fields', () => {
      const filter = { '$unknown.field': 'value' };
      const result = transformVariantFilter(filter, [dataConfig]);

      // Unknown columns pass through unchanged (not a variant column)
      expect(result.filter).toEqual({ '$unknown.field': 'value' });
      expect(result.untransformedPaths).toHaveLength(0);
      expect(result.transformedPaths).toHaveLength(0);
    });
  });

  describe('Edge cases', () => {
    it('should handle null values in filters', () => {
      const filter = { '$data.title': null };
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({
        '$data.typed_value.title.typed_value': null,
      });
    });

    it('should handle array values (direct equality)', () => {
      const filter = { '$data.genre': ['action', 'comedy'] };
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({
        '$data.typed_value.genre.typed_value': ['action', 'comedy'],
      });
    });

    it('should handle deeply nested field paths', () => {
      // Config with nested field path
      const nestedConfig: VariantShredPropertyConfig = {
        columnName: '$data',
        fields: ['user.name', 'user.email', 'metadata.tags'],
        fieldTypes: {
          'user.name': 'string',
          'user.email': 'string',
          'metadata.tags': 'string',
        },
      };

      const filter = { '$data.user.name': 'John' };
      const result = transformVariantFilter(filter, [nestedConfig]);

      expect(result.filter).toEqual({
        '$data.typed_value.user.name.typed_value': 'John',
      });
    });

    it('should handle boolean values', () => {
      const boolConfig: VariantShredPropertyConfig = {
        columnName: '$data',
        fields: ['active'],
        fieldTypes: { active: 'boolean' },
      };

      const filter = { '$data.active': true };
      const result = transformVariantFilter(filter, [boolConfig]);

      expect(result.filter).toEqual({
        '$data.typed_value.active.typed_value': true,
      });
    });

    it('should handle numeric zero values', () => {
      const filter = { '$data.year': 0 };
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({
        '$data.typed_value.year.typed_value': 0,
      });
    });

    it('should handle empty string values', () => {
      const filter = { '$data.title': '' };
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({
        '$data.typed_value.title.typed_value': '',
      });
    });

    it('should handle empty configs array', () => {
      const filter = { '$data.title': 'foo' };
      const result = transformVariantFilter(filter, []);

      // With no configs, nothing should be transformed
      expect(result.filter).toEqual({ '$data.title': 'foo' });
      expect(result.transformedPaths).toHaveLength(0);
    });

    it('should handle $exists operator', () => {
      const filter = { '$data.title': { $exists: true } };
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({
        '$data.typed_value.title.typed_value': { $exists: true },
      });
    });

    it('should handle $regex operator', () => {
      const filter = { '$data.title': { $regex: '^foo' } };
      const result = transformVariantFilter(filter, [dataConfig]);

      expect(result.filter).toEqual({
        '$data.typed_value.title.typed_value': { $regex: '^foo' },
      });
    });
  });

  describe('isComparisonOperator helper', () => {
    it('should return true for comparison operators', () => {
      expect(isComparisonOperator('$eq')).toBe(true);
      expect(isComparisonOperator('$gt')).toBe(true);
      expect(isComparisonOperator('$gte')).toBe(true);
      expect(isComparisonOperator('$lt')).toBe(true);
      expect(isComparisonOperator('$lte')).toBe(true);
      expect(isComparisonOperator('$ne')).toBe(true);
      expect(isComparisonOperator('$in')).toBe(true);
      expect(isComparisonOperator('$nin')).toBe(true);
    });

    it('should return false for non-comparison operators', () => {
      expect(isComparisonOperator('$and')).toBe(false);
      expect(isComparisonOperator('$or')).toBe(false);
      expect(isComparisonOperator('$not')).toBe(false);
      expect(isComparisonOperator('title')).toBe(false);
      expect(isComparisonOperator('$data.field')).toBe(false);
    });
  });

  describe('TransformResult interface', () => {
    it('should have correct structure', () => {
      const filter = { '$data.title': 'foo', id: 123 };
      const result = transformVariantFilter(filter, [dataConfig]);

      // Verify structure
      expect(result).toHaveProperty('filter');
      expect(result).toHaveProperty('transformedPaths');
      expect(result).toHaveProperty('untransformedPaths');

      // Verify types
      expect(typeof result.filter).toBe('object');
      expect(Array.isArray(result.transformedPaths)).toBe(true);
      expect(Array.isArray(result.untransformedPaths)).toBe(true);
    });
  });
});
