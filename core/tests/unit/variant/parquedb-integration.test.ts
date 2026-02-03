/**
 * Tests for ParqueDB Integration Helpers
 *
 * These tests verify compatibility with parquedb/hyparquet variant shredding
 * implementations. The integration helpers provide convenience functions for
 * setting up variant shredding in a way that's compatible with parquedb's
 * variant-filter and hyparquet-writer.
 *
 * @see https://github.com/hyparam/hyparquet
 */

import { describe, it, expect } from 'vitest';
import type { IcebergSchema, IcebergPrimitiveType } from '../../../src/index.js';
import {
  // Config functions
  extractVariantShredConfig,
  toTableProperties,
  // Statistics path functions
  getStatisticsPaths,
  // Filter transformation functions
  transformVariantFilter,
  // Field ID assignment
  assignShreddedFieldIds,
  // Row group filtering
  filterDataFiles,
  filterDataFilesWithStats,
} from '../../../src/index.js';
import type { VariantShredPropertyConfig } from '../../../src/index.js';

// Import integration helpers (to be created)
import {
  parseShredConfig,
  formatShredConfig,
  createVariantSchemaFields,
  getFieldIdForShreddedPath,
  validateConfigWithSchema,
  setupVariantShredding,
  type VariantSchemaField,
  type SetupVariantShreddingOptions,
  type SetupVariantShreddingResult,
} from '../../../src/variant/integration.js';

describe('ParqueDB Integration Helpers', () => {
  // ============================================================================
  // parseShredConfig from table properties tests
  // ============================================================================

  describe('parseShredConfig from table properties', () => {
    it('should extract config from Iceberg table properties', () => {
      const properties: Record<string, string> = {
        'write.variant.shred-columns': '$data',
        'write.variant.$data.shred-fields': 'title,year,rating',
        'write.variant.$data.field-types': 'title:string,year:int,rating:double',
      };

      const configs = parseShredConfig(properties);

      expect(configs).toHaveLength(1);
      expect(configs[0]).toEqual({
        column: '$data',
        fields: ['title', 'year', 'rating'],
        fieldTypes: {
          title: 'string',
          year: 'int',
          rating: 'double',
        },
      });
    });

    it('should handle multiple variant columns', () => {
      const properties: Record<string, string> = {
        'write.variant.shred-columns': '$data,$index',
        'write.variant.$data.shred-fields': 'title',
        'write.variant.$data.field-types': 'title:string',
        'write.variant.$index.shred-fields': 'key,value',
        'write.variant.$index.field-types': 'key:string,value:long',
      };

      const configs = parseShredConfig(properties);

      expect(configs).toHaveLength(2);
      expect(configs[0].column).toBe('$data');
      expect(configs[1].column).toBe('$index');
      expect(configs[1].fields).toEqual(['key', 'value']);
    });

    it('should handle optional fieldTypes', () => {
      const properties: Record<string, string> = {
        'write.variant.shred-columns': 'metadata',
        'write.variant.metadata.shred-fields': 'name,value',
      };

      const configs = parseShredConfig(properties);

      expect(configs).toHaveLength(1);
      expect(configs[0].fieldTypes).toEqual({});
    });

    it('should return empty array for tables without shredding config', () => {
      const properties: Record<string, string> = {
        'some.other.property': 'value',
      };

      const configs = parseShredConfig(properties);

      expect(configs).toEqual([]);
    });

    it('should return empty array for empty properties', () => {
      const configs = parseShredConfig({});
      expect(configs).toEqual([]);
    });
  });

  // ============================================================================
  // formatShredConfig to table properties tests
  // ============================================================================

  describe('formatShredConfig to table properties', () => {
    it('should write config to properties', () => {
      const configs = [
        {
          column: '$data',
          fields: ['title', 'year'] as const,
          fieldTypes: {
            title: 'string' as IcebergPrimitiveType,
            year: 'int' as IcebergPrimitiveType,
          },
        },
      ];

      const properties = formatShredConfig(configs);

      expect(properties['write.variant.shred-columns']).toBe('$data');
      expect(properties['write.variant.$data.shred-fields']).toBe('title,year');
      expect(properties['write.variant.$data.field-types']).toMatch(/title:string/);
      expect(properties['write.variant.$data.field-types']).toMatch(/year:int/);
    });

    it('should round-trip: format then parse returns same config', () => {
      const original = [
        {
          column: '$data',
          fields: ['name', 'age', 'active'] as const,
          fieldTypes: {
            name: 'string' as IcebergPrimitiveType,
            age: 'int' as IcebergPrimitiveType,
            active: 'boolean' as IcebergPrimitiveType,
          },
        },
      ];

      const properties = formatShredConfig(original);
      const parsed = parseShredConfig(properties);

      expect(parsed).toEqual(original);
    });

    it('should handle special characters in field names', () => {
      const configs = [
        {
          column: '$data',
          fields: ['user.name', 'metadata.tags'] as const,
          fieldTypes: {
            'user.name': 'string' as IcebergPrimitiveType,
            'metadata.tags': 'string' as IcebergPrimitiveType,
          },
        },
      ];

      const properties = formatShredConfig(configs);
      const parsed = parseShredConfig(properties);

      expect(parsed[0].fields).toEqual(['user.name', 'metadata.tags']);
    });
  });

  // ============================================================================
  // Field ID assignment compatibility tests
  // ============================================================================

  describe('Field ID assignment compatibility', () => {
    it('should start field IDs from provided starting ID', () => {
      const configs: VariantShredPropertyConfig[] = [
        { columnName: '$data', fields: ['title', 'year'], fieldTypes: {} },
      ];

      const fieldIdMap = assignShreddedFieldIds(configs, 1000);

      expect(fieldIdMap.get('$data.typed_value.title.typed_value')).toBe(1000);
      expect(fieldIdMap.get('$data.typed_value.year.typed_value')).toBe(1001);
    });

    it('should produce stable field IDs across calls with same config', () => {
      const configs: VariantShredPropertyConfig[] = [
        { columnName: '$data', fields: ['a', 'b', 'c'], fieldTypes: {} },
      ];

      const map1 = assignShreddedFieldIds(configs, 100);
      const map2 = assignShreddedFieldIds(configs, 100);

      expect(map1.get('$data.typed_value.a.typed_value')).toBe(map2.get('$data.typed_value.a.typed_value'));
      expect(map1.get('$data.typed_value.b.typed_value')).toBe(map2.get('$data.typed_value.b.typed_value'));
      expect(map1.get('$data.typed_value.c.typed_value')).toBe(map2.get('$data.typed_value.c.typed_value'));
    });

    it("should not conflict with schema field IDs when starting ID is set correctly", () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          { id: 2, name: 'data', required: false, type: 'variant' },
        ],
      };

      const configs: VariantShredPropertyConfig[] = [
        { columnName: '$data', fields: ['title'], fieldTypes: {} },
      ];

      // Start from ID after schema's max field ID
      const result = setupVariantShredding({
        tableProperties: {},
        schema,
        startingFieldId: 1000,
      });

      // Verify no overlap with schema field IDs
      const schemaFieldIds = new Set([1, 2]);
      for (const [_path, id] of result.fieldIdMap) {
        expect(schemaFieldIds.has(id)).toBe(false);
      }
    });
  });

  // ============================================================================
  // hyparquet-writer compatibility tests
  // ============================================================================

  describe('hyparquet-writer compatibility', () => {
    it('should produce statistics paths that match hyparquet-writer output format', () => {
      const paths = getStatisticsPaths('$data', ['title', 'year', 'rating']);

      // hyparquet-writer uses: {column}.typed_value.{field}.typed_value
      expect(paths).toEqual([
        '$data.typed_value.title.typed_value',
        '$data.typed_value.year.typed_value',
        '$data.typed_value.rating.typed_value',
      ]);
    });

    it('should use path format: "{column}.typed_value.{field}.typed_value"', () => {
      const paths = getStatisticsPaths('$index', ['key']);

      expect(paths[0]).toBe('$index.typed_value.key.typed_value');
      expect(paths[0]).toMatch(/^\$\w+\.typed_value\.\w+\.typed_value$/);
    });

    it('should handle nested field names in statistics paths', () => {
      const paths = getStatisticsPaths('$data', ['user.name', 'user.email']);

      expect(paths).toEqual([
        '$data.typed_value.user.name.typed_value',
        '$data.typed_value.user.email.typed_value',
      ]);
    });
  });

  // ============================================================================
  // parquedb variant-filter compatibility tests
  // ============================================================================

  describe('parquedb variant-filter compatibility', () => {
    it('should transform variant filter to produce same paths as parquedb', () => {
      const configs: VariantShredPropertyConfig[] = [
        {
          columnName: '$data',
          fields: ['title', 'year'],
          fieldTypes: { title: 'string', year: 'int' },
        },
      ];

      const result = transformVariantFilter(
        { '$data.title': 'The Matrix', '$data.year': { $gte: 1999 } },
        configs
      );

      // parquedb expects transformed paths in format: column.typed_value.field.typed_value
      expect(result.filter).toHaveProperty('$data.typed_value.title.typed_value');
      expect(result.filter).toHaveProperty('$data.typed_value.year.typed_value');
    });

    it('should preserve filter operators after transformation', () => {
      const configs: VariantShredPropertyConfig[] = [
        {
          columnName: '$data',
          fields: ['age'],
          fieldTypes: { age: 'int' },
        },
      ];

      const result = transformVariantFilter(
        { '$data.age': { $gte: 21, $lte: 65 } },
        configs
      );

      const transformedAge = result.filter['$data.typed_value.age.typed_value'] as Record<string, unknown>;
      expect(transformedAge).toHaveProperty('$gte', 21);
      expect(transformedAge).toHaveProperty('$lte', 65);
    });

    it('should track transformed and untransformed paths', () => {
      const configs: VariantShredPropertyConfig[] = [
        {
          columnName: '$data',
          fields: ['title'],
          fieldTypes: { title: 'string' },
        },
      ];

      const result = transformVariantFilter(
        { '$data.title': 'foo', '$data.unknown': 'bar', regularColumn: 123 },
        configs
      );

      expect(result.transformedPaths).toContain('$data.title');
      expect(result.untransformedPaths).toContain('$data.unknown');
    });
  });

  // ============================================================================
  // Schema integration tests
  // ============================================================================

  describe('Schema integration', () => {
    it('should create variant schema field definitions', () => {
      const configs: VariantShredPropertyConfig[] = [
        {
          columnName: '$data',
          fields: ['title', 'year'],
          fieldTypes: { title: 'string', year: 'int' },
        },
      ];

      const schemaFields = createVariantSchemaFields(configs);

      expect(schemaFields).toHaveLength(1);
      expect(schemaFields[0]).toEqual({
        name: '$data',
        type: 'variant',
        shreddedFields: ['title', 'year'],
        fieldTypes: { title: 'string', year: 'int' },
      });
    });

    it('should handle multiple variant columns in schema fields', () => {
      const configs: VariantShredPropertyConfig[] = [
        { columnName: '$data', fields: ['a'], fieldTypes: { a: 'string' } },
        { columnName: '$index', fields: ['b'], fieldTypes: { b: 'int' } },
      ];

      const schemaFields = createVariantSchemaFields(configs);

      expect(schemaFields).toHaveLength(2);
      expect(schemaFields[0].name).toBe('$data');
      expect(schemaFields[1].name).toBe('$index');
    });

    it('should look up field ID for shredded path in schema', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          { id: 2, name: '$data', required: false, type: 'variant' },
        ],
      };

      // Assume shredded field IDs are tracked in setup
      const setupResult = setupVariantShredding({
        tableProperties: {
          'write.variant.shred-columns': '$data',
          'write.variant.$data.shred-fields': 'title',
          'write.variant.$data.field-types': 'title:string',
        },
        schema,
        startingFieldId: 100,
      });

      const fieldId = getFieldIdForShreddedPath(setupResult.fieldIdMap, '$data.typed_value.title.typed_value');

      expect(fieldId).toBe(100);
    });

    it('should return null for non-shredded paths', () => {
      const fieldIdMap = new Map<string, number>();
      fieldIdMap.set('$data.typed_value.title.typed_value', 100);

      const fieldId = getFieldIdForShreddedPath(fieldIdMap, '$data.typed_value.unknown.typed_value');

      expect(fieldId).toBeNull();
    });

    it('should validate shred configs against table schema', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          { id: 2, name: '$data', required: false, type: 'variant' },
        ],
      };

      const configs: VariantShredPropertyConfig[] = [
        { columnName: '$data', fields: ['title'], fieldTypes: { title: 'string' } },
      ];

      const result = validateConfigWithSchema(configs, schema);

      expect(result.valid).toBe(true);
      expect(result.errors).toEqual([]);
    });

    it('should report error when config references non-existent column', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
        ],
      };

      const configs: VariantShredPropertyConfig[] = [
        { columnName: '$data', fields: ['title'], fieldTypes: { title: 'string' } },
      ];

      const result = validateConfigWithSchema(configs, schema);

      expect(result.valid).toBe(false);
      expect(result.errors).toContain("Column '$data' not found in schema");
    });

    it('should report error when config references non-variant column', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          { id: 2, name: '$data', required: false, type: 'string' }, // Not variant!
        ],
      };

      const configs: VariantShredPropertyConfig[] = [
        { columnName: '$data', fields: ['title'], fieldTypes: { title: 'string' } },
      ];

      const result = validateConfigWithSchema(configs, schema);

      expect(result.valid).toBe(false);
      expect(result.errors[0]).toMatch(/not a variant column/i);
    });
  });

  // ============================================================================
  // setupVariantShredding convenience function tests
  // ============================================================================

  describe('setupVariantShredding convenience function', () => {
    it('should set up variant shredding from table properties', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          { id: 2, name: '$data', required: false, type: 'variant' },
        ],
      };

      const result = setupVariantShredding({
        tableProperties: {
          'write.variant.shred-columns': '$data',
          'write.variant.$data.shred-fields': 'title,year',
          'write.variant.$data.field-types': 'title:string,year:int',
        },
        schema,
        startingFieldId: 1000,
      });

      expect(result.configs).toHaveLength(1);
      expect(result.configs[0].columnName).toBe('$data');
      expect(result.fieldIdMap.size).toBe(2);
      expect(result.statisticsPaths).toContain('$data.typed_value.title.typed_value');
      expect(result.statisticsPaths).toContain('$data.typed_value.year.typed_value');
    });

    it('should return empty results for tables without shredding', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
        ],
      };

      const result = setupVariantShredding({
        tableProperties: {},
        schema,
        startingFieldId: 1000,
      });

      expect(result.configs).toEqual([]);
      expect(result.fieldIdMap.size).toBe(0);
      expect(result.statisticsPaths).toEqual([]);
    });

    it('should handle multiple variant columns', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: '$data', required: false, type: 'variant' },
          { id: 2, name: '$meta', required: false, type: 'variant' },
        ],
      };

      const result = setupVariantShredding({
        tableProperties: {
          'write.variant.shred-columns': '$data,$meta',
          'write.variant.$data.shred-fields': 'title',
          'write.variant.$data.field-types': 'title:string',
          'write.variant.$meta.shred-fields': 'created',
          'write.variant.$meta.field-types': 'created:timestamp',
        },
        schema,
        startingFieldId: 100,
      });

      expect(result.configs).toHaveLength(2);
      expect(result.fieldIdMap.size).toBe(2);
      expect(result.fieldIdMap.get('$data.typed_value.title.typed_value')).toBe(100);
      expect(result.fieldIdMap.get('$meta.typed_value.created.typed_value')).toBe(101);
    });

    it('should provide field IDs for use with filterDataFiles', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: '$data', required: false, type: 'variant' },
        ],
      };

      const setup = setupVariantShredding({
        tableProperties: {
          'write.variant.shred-columns': '$data',
          'write.variant.$data.shred-fields': 'age',
          'write.variant.$data.field-types': 'age:int',
        },
        schema,
        startingFieldId: 100,
      });

      // Verify the field ID map can be used with filterDataFiles
      expect(setup.fieldIdMap.get('$data.typed_value.age.typed_value')).toBeDefined();

      // The map should be usable with existing filterDataFiles function
      const files = filterDataFiles(
        [], // empty array - just testing the setup works
        { '$data.age': { $gte: 21 } },
        setup.configs,
        setup.fieldIdMap
      );

      expect(files).toEqual([]);
    });
  });
});
