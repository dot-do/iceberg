/**
 * Tests for Variant Statistics Path Mapping Functions
 *
 * These functions map variant field paths to statistics paths for predicate pushdown.
 * This enables efficient filtering on shredded variant fields by mapping filter
 * predicates to the correct column statistics paths.
 *
 * @see https://iceberg.apache.org/spec/#variant-shredding
 */

import { describe, it, expect } from 'vitest';
import type { VariantShredPropertyConfig } from '../../../src/index.js';
import {
  getStatisticsPaths,
  mapFilterPathToStats,
  extractVariantFilterColumns,
  isVariantFilterPath,
  getColumnForFilterPath,
} from '../../../src/index.js';

// Alias for readability
type VariantShredConfig = VariantShredPropertyConfig;

describe('Variant Statistics Path Mapping', () => {
  describe('getStatisticsPaths', () => {
    it('should return statistics paths for multiple fields', () => {
      const result = getStatisticsPaths('$data', ['title', 'year']);

      expect(result).toEqual([
        '$data.typed_value.title.typed_value',
        '$data.typed_value.year.typed_value',
      ]);
    });

    it('should return statistics path for single field', () => {
      const result = getStatisticsPaths('$data', ['title']);

      expect(result).toEqual(['$data.typed_value.title.typed_value']);
    });

    it('should return empty array for empty fields', () => {
      const result = getStatisticsPaths('$data', []);

      expect(result).toEqual([]);
    });

    it('should handle special characters in field names', () => {
      const result = getStatisticsPaths('$data', ['field-name', 'field_name', 'field.name']);

      expect(result).toEqual([
        '$data.typed_value.field-name.typed_value',
        '$data.typed_value.field_name.typed_value',
        '$data.typed_value.field.name.typed_value',
      ]);
    });

    it('should work with different column names', () => {
      const result = getStatisticsPaths('$index', ['key', 'value']);

      expect(result).toEqual([
        '$index.typed_value.key.typed_value',
        '$index.typed_value.value.typed_value',
      ]);
    });

    it('should work with column names without $ prefix', () => {
      const result = getStatisticsPaths('metadata', ['name']);

      expect(result).toEqual(['metadata.typed_value.name.typed_value']);
    });
  });

  describe('mapFilterPathToStats', () => {
    const configs: readonly VariantShredConfig[] = [
      {
        columnName: '$data',
        fields: ['title', 'year', 'rating'],
        fieldTypes: { title: 'string', year: 'int', rating: 'double' },
      },
      {
        columnName: '$index',
        fields: ['key', 'value'],
        fieldTypes: { key: 'string', value: 'long' },
      },
    ];

    it('should map filter path to statistics path', () => {
      const result = mapFilterPathToStats('$data.title', configs);

      expect(result).toBe('$data.typed_value.title.typed_value');
    });

    it('should return null if field not in shredded fields', () => {
      const result = mapFilterPathToStats('$data.unknown', configs);

      expect(result).toBeNull();
    });

    it('should return null if column does not match any config', () => {
      const result = mapFilterPathToStats('$other.title', configs);

      expect(result).toBeNull();
    });

    it('should handle nested paths like $data.user.name', () => {
      const nestedConfigs: readonly VariantShredConfig[] = [
        {
          columnName: '$data',
          fields: ['user.name', 'user.email'],
          fieldTypes: { 'user.name': 'string', 'user.email': 'string' },
        },
      ];

      const result = mapFilterPathToStats('$data.user.name', nestedConfigs);

      expect(result).toBe('$data.typed_value.user.name.typed_value');
    });

    it('should work with second config in array', () => {
      const result = mapFilterPathToStats('$index.key', configs);

      expect(result).toBe('$index.typed_value.key.typed_value');
    });

    it('should return null for empty configs array', () => {
      const result = mapFilterPathToStats('$data.title', []);

      expect(result).toBeNull();
    });

    it('should handle column names without $ prefix', () => {
      const simpleConfigs: readonly VariantShredConfig[] = [
        {
          columnName: 'metadata',
          fields: ['name', 'value'],
          fieldTypes: {},
        },
      ];

      const result = mapFilterPathToStats('metadata.name', simpleConfigs);

      expect(result).toBe('metadata.typed_value.name.typed_value');
    });
  });

  describe('extractVariantFilterColumns', () => {
    const configs: readonly VariantShredConfig[] = [
      {
        columnName: '$data',
        fields: ['title', 'year'],
        fieldTypes: { title: 'string', year: 'int' },
      },
      {
        columnName: '$index',
        fields: ['key'],
        fieldTypes: { key: 'string' },
      },
    ];

    it('should extract columns for single variant field filter', () => {
      const filter = { '$data.title': 'The Matrix' };

      const result = extractVariantFilterColumns(filter, configs);

      expect(result.readColumns).toContain('$data');
      expect(result.statsColumns).toContain('$data.typed_value.title.typed_value');
    });

    it('should extract columns for multiple variant fields in filter', () => {
      const filter = {
        '$data.title': 'The Matrix',
        '$data.year': 1999,
      };

      const result = extractVariantFilterColumns(filter, configs);

      expect(result.readColumns).toContain('$data');
      expect(result.statsColumns).toContain('$data.typed_value.title.typed_value');
      expect(result.statsColumns).toContain('$data.typed_value.year.typed_value');
    });

    it('should extract columns from multiple variant columns', () => {
      const filter = {
        '$data.title': 'The Matrix',
        '$index.key': 'movie-123',
      };

      const result = extractVariantFilterColumns(filter, configs);

      expect(result.readColumns).toContain('$data');
      expect(result.readColumns).toContain('$index');
      expect(result.statsColumns).toContain('$data.typed_value.title.typed_value');
      expect(result.statsColumns).toContain('$index.typed_value.key.typed_value');
    });

    it('should handle mix of variant and non-variant fields', () => {
      const filter = {
        '$data.title': 'The Matrix',
        id: 123,
        created_at: '2024-01-01',
      };

      const result = extractVariantFilterColumns(filter, configs);

      // Should only include the variant fields
      expect(result.readColumns).toEqual(['$data']);
      expect(result.statsColumns).toEqual(['$data.typed_value.title.typed_value']);
    });

    it('should return empty arrays for filters without variant fields', () => {
      const filter = {
        id: 123,
        name: 'test',
      };

      const result = extractVariantFilterColumns(filter, configs);

      expect(result.readColumns).toEqual([]);
      expect(result.statsColumns).toEqual([]);
    });

    it('should return empty arrays for empty filter', () => {
      const filter = {};

      const result = extractVariantFilterColumns(filter, configs);

      expect(result.readColumns).toEqual([]);
      expect(result.statsColumns).toEqual([]);
    });

    it('should deduplicate read columns', () => {
      const filter = {
        '$data.title': 'The Matrix',
        '$data.year': 1999,
      };

      const result = extractVariantFilterColumns(filter, configs);

      // Should only have $data once
      expect(result.readColumns.filter((c) => c === '$data').length).toBe(1);
    });

    it('should ignore unknown variant fields', () => {
      const filter = {
        '$data.unknown': 'value',
        '$data.title': 'The Matrix',
      };

      const result = extractVariantFilterColumns(filter, configs);

      expect(result.readColumns).toEqual(['$data']);
      expect(result.statsColumns).toEqual(['$data.typed_value.title.typed_value']);
    });
  });

  describe('isVariantFilterPath', () => {
    const configs: readonly VariantShredConfig[] = [
      {
        columnName: '$data',
        fields: ['title', 'year'],
        fieldTypes: { title: 'string', year: 'int' },
      },
      {
        columnName: 'metadata',
        fields: ['name'],
        fieldTypes: { name: 'string' },
      },
    ];

    it('should return true for shredded field path', () => {
      const result = isVariantFilterPath('$data.title', configs);

      expect(result).toBe(true);
    });

    it('should return false for non-shredded field', () => {
      const result = isVariantFilterPath('$data.unknown', configs);

      expect(result).toBe(false);
    });

    it('should return false for non-variant column', () => {
      const result = isVariantFilterPath('regular_column', configs);

      expect(result).toBe(false);
    });

    it('should return true for shredded field in second config', () => {
      const result = isVariantFilterPath('metadata.name', configs);

      expect(result).toBe(true);
    });

    it('should return false for path that looks like variant but has wrong column', () => {
      const result = isVariantFilterPath('$other.title', configs);

      expect(result).toBe(false);
    });

    it('should return false for empty configs', () => {
      const result = isVariantFilterPath('$data.title', []);

      expect(result).toBe(false);
    });

    it('should return false for path without dot separator', () => {
      const result = isVariantFilterPath('$data', configs);

      expect(result).toBe(false);
    });
  });

  describe('getColumnForFilterPath', () => {
    it('should extract column from $data.title path', () => {
      const result = getColumnForFilterPath('$data.title');

      expect(result).toBe('$data');
    });

    it('should extract column from $index.id path', () => {
      const result = getColumnForFilterPath('$index.id');

      expect(result).toBe('$index');
    });

    it('should handle deeply nested paths', () => {
      const result = getColumnForFilterPath('$data.user.profile.name');

      expect(result).toBe('$data');
    });

    it('should work with column names without $ prefix', () => {
      const result = getColumnForFilterPath('metadata.name');

      expect(result).toBe('metadata');
    });

    it('should return entire string for paths without dots', () => {
      const result = getColumnForFilterPath('column');

      expect(result).toBe('column');
    });

    it('should handle empty string', () => {
      const result = getColumnForFilterPath('');

      expect(result).toBe('');
    });
  });
});
