/**
 * Tests for Variant Shredding Configuration
 *
 * Variant shredding allows decomposing variant columns into typed sub-columns
 * for better query performance and storage efficiency.
 *
 * @see https://iceberg.apache.org/spec/#variant-shredding
 */

import { describe, it, expect } from 'vitest';
import type { IcebergPrimitiveType, VariantShredPropertyConfig } from '../../../src/index.js';
import {
  // Property key constants
  VARIANT_SHRED_COLUMNS_KEY,
  VARIANT_SHRED_FIELDS_KEY_PREFIX,
  VARIANT_SHRED_FIELDS_KEY_SUFFIX,
  VARIANT_FIELD_TYPES_KEY_SUFFIX,
  // Key generation functions
  getShredFieldsKey,
  getFieldTypesKey,
  // Parsing functions
  parseShredColumnsProperty,
  parseShredFieldsProperty,
  parseFieldTypesProperty,
  extractVariantShredConfig,
  // Serialization functions
  formatShredColumnsProperty,
  formatShredFieldsProperty,
  formatFieldTypesProperty,
  toTableProperties,
  // Validation
  validateShredConfig,
} from '../../../src/index.js';

// Alias for readability in tests
type VariantShredConfig = VariantShredPropertyConfig;

describe('Variant Shredding Configuration', () => {
  describe('Property Key Constants', () => {
    it('should define VARIANT_SHRED_COLUMNS_KEY correctly', () => {
      expect(VARIANT_SHRED_COLUMNS_KEY).toBe('write.variant.shred-columns');
    });

    it('should define VARIANT_SHRED_FIELDS_KEY_PREFIX correctly', () => {
      expect(VARIANT_SHRED_FIELDS_KEY_PREFIX).toBe('write.variant.');
    });

    it('should define VARIANT_SHRED_FIELDS_KEY_SUFFIX correctly', () => {
      expect(VARIANT_SHRED_FIELDS_KEY_SUFFIX).toBe('.shred-fields');
    });

    it('should define VARIANT_FIELD_TYPES_KEY_SUFFIX correctly', () => {
      expect(VARIANT_FIELD_TYPES_KEY_SUFFIX).toBe('.field-types');
    });
  });

  describe('Property Key Generation', () => {
    it('should generate shred-fields key for a column', () => {
      expect(getShredFieldsKey('$data')).toBe('write.variant.$data.shred-fields');
    });

    it('should generate shred-fields key for another column', () => {
      expect(getShredFieldsKey('metadata')).toBe('write.variant.metadata.shred-fields');
    });

    it('should generate field-types key for a column', () => {
      expect(getFieldTypesKey('$data')).toBe('write.variant.$data.field-types');
    });

    it('should generate field-types key for another column', () => {
      expect(getFieldTypesKey('$index')).toBe('write.variant.$index.field-types');
    });
  });

  describe('Config Parsing: shred-columns', () => {
    it('should parse comma-separated column names', () => {
      const result = parseShredColumnsProperty('$data,$index');
      expect(result).toEqual(['$data', '$index']);
    });

    it('should parse single column name', () => {
      const result = parseShredColumnsProperty('metadata');
      expect(result).toEqual(['metadata']);
    });

    it('should return empty array for empty string', () => {
      const result = parseShredColumnsProperty('');
      expect(result).toEqual([]);
    });

    it('should return empty array for undefined', () => {
      const result = parseShredColumnsProperty(undefined);
      expect(result).toEqual([]);
    });

    it('should trim whitespace from column names', () => {
      const result = parseShredColumnsProperty(' $data , $index ');
      expect(result).toEqual(['$data', '$index']);
    });

    it('should filter out empty entries', () => {
      const result = parseShredColumnsProperty('$data,,,$index');
      expect(result).toEqual(['$data', '$index']);
    });
  });

  describe('Config Parsing: shred-fields', () => {
    it('should parse comma-separated field names', () => {
      const result = parseShredFieldsProperty('title,year,rating');
      expect(result).toEqual(['title', 'year', 'rating']);
    });

    it('should parse single field name', () => {
      const result = parseShredFieldsProperty('title');
      expect(result).toEqual(['title']);
    });

    it('should return empty array for empty string', () => {
      const result = parseShredFieldsProperty('');
      expect(result).toEqual([]);
    });

    it('should return empty array for undefined', () => {
      const result = parseShredFieldsProperty(undefined);
      expect(result).toEqual([]);
    });

    it('should handle nested field paths', () => {
      const result = parseShredFieldsProperty('user.name,user.email,metadata.tags');
      expect(result).toEqual(['user.name', 'user.email', 'metadata.tags']);
    });

    it('should trim whitespace from field names', () => {
      const result = parseShredFieldsProperty(' title , year ');
      expect(result).toEqual(['title', 'year']);
    });
  });

  describe('Config Parsing: field-types', () => {
    it('should parse field type pairs', () => {
      const result = parseFieldTypesProperty('title:string,year:int');
      expect(result).toEqual({
        title: 'string',
        year: 'int',
      });
    });

    it('should parse single field type pair', () => {
      const result = parseFieldTypesProperty('rating:double');
      expect(result).toEqual({
        rating: 'double',
      });
    });

    it('should return empty object for empty string', () => {
      const result = parseFieldTypesProperty('');
      expect(result).toEqual({});
    });

    it('should return empty object for undefined', () => {
      const result = parseFieldTypesProperty(undefined);
      expect(result).toEqual({});
    });

    it('should handle all primitive types', () => {
      const result = parseFieldTypesProperty(
        'a:boolean,b:int,c:long,d:float,e:double,f:string,g:date,h:timestamp'
      );
      expect(result).toEqual({
        a: 'boolean',
        b: 'int',
        c: 'long',
        d: 'float',
        e: 'double',
        f: 'string',
        g: 'date',
        h: 'timestamp',
      });
    });

    it('should trim whitespace from pairs', () => {
      const result = parseFieldTypesProperty(' title : string , year : int ');
      expect(result).toEqual({
        title: 'string',
        year: 'int',
      });
    });

    it('should handle nested field paths as keys', () => {
      const result = parseFieldTypesProperty('user.name:string,user.age:int');
      expect(result).toEqual({
        'user.name': 'string',
        'user.age': 'int',
      });
    });
  });

  describe('Config Extraction from Table Properties', () => {
    it('should extract config for single variant column', () => {
      const properties: Record<string, string> = {
        'write.variant.shred-columns': '$data',
        'write.variant.$data.shred-fields': 'title,year',
        'write.variant.$data.field-types': 'title:string,year:int',
      };

      const configs = extractVariantShredConfig(properties);

      expect(configs).toHaveLength(1);
      expect(configs[0]).toEqual({
        columnName: '$data',
        fields: ['title', 'year'],
        fieldTypes: {
          title: 'string',
          year: 'int',
        },
      });
    });

    it('should extract config for multiple variant columns', () => {
      const properties: Record<string, string> = {
        'write.variant.shred-columns': '$data,$index',
        'write.variant.$data.shred-fields': 'title,year',
        'write.variant.$data.field-types': 'title:string,year:int',
        'write.variant.$index.shred-fields': 'key,value',
        'write.variant.$index.field-types': 'key:string,value:long',
      };

      const configs = extractVariantShredConfig(properties);

      expect(configs).toHaveLength(2);
      expect(configs[0].columnName).toBe('$data');
      expect(configs[0].fields).toEqual(['title', 'year']);
      expect(configs[1].columnName).toBe('$index');
      expect(configs[1].fields).toEqual(['key', 'value']);
    });

    it('should handle missing optional fieldTypes', () => {
      const properties: Record<string, string> = {
        'write.variant.shred-columns': 'metadata',
        'write.variant.metadata.shred-fields': 'name,value',
      };

      const configs = extractVariantShredConfig(properties);

      expect(configs).toHaveLength(1);
      expect(configs[0]).toEqual({
        columnName: 'metadata',
        fields: ['name', 'value'],
        fieldTypes: {},
      });
    });

    it('should return empty array when no shred-columns property', () => {
      const properties: Record<string, string> = {
        'some.other.property': 'value',
      };

      const configs = extractVariantShredConfig(properties);

      expect(configs).toEqual([]);
    });

    it('should return empty array for empty properties', () => {
      const configs = extractVariantShredConfig({});
      expect(configs).toEqual([]);
    });
  });

  describe('Config Serialization', () => {
    it('should format shred columns property', () => {
      const configs: VariantShredConfig[] = [
        { columnName: '$data', fields: ['title'], fieldTypes: {} },
        { columnName: '$index', fields: ['key'], fieldTypes: {} },
      ];

      const result = formatShredColumnsProperty(configs);
      expect(result).toBe('$data,$index');
    });

    it('should format single column', () => {
      const configs: VariantShredConfig[] = [
        { columnName: 'metadata', fields: ['name'], fieldTypes: {} },
      ];

      const result = formatShredColumnsProperty(configs);
      expect(result).toBe('metadata');
    });

    it('should format shred fields property', () => {
      const fields = ['title', 'year', 'rating'] as const;
      const result = formatShredFieldsProperty(fields);
      expect(result).toBe('title,year,rating');
    });

    it('should format single field', () => {
      const fields = ['title'] as const;
      const result = formatShredFieldsProperty(fields);
      expect(result).toBe('title');
    });

    it('should format field types property', () => {
      const fieldTypes: Record<string, IcebergPrimitiveType> = {
        title: 'string',
        year: 'int',
        rating: 'double',
      };

      const result = formatFieldTypesProperty(fieldTypes);
      // Note: order may vary, so we check for both possible orders
      expect(result).toMatch(/title:string/);
      expect(result).toMatch(/year:int/);
      expect(result).toMatch(/rating:double/);
    });

    it('should format empty field types', () => {
      const result = formatFieldTypesProperty({});
      expect(result).toBe('');
    });
  });

  describe('Full Properties Serialization', () => {
    it('should convert single config to table properties', () => {
      const configs: VariantShredConfig[] = [
        {
          columnName: '$data',
          fields: ['title', 'year'],
          fieldTypes: {
            title: 'string',
            year: 'int',
          },
        },
      ];

      const properties = toTableProperties(configs);

      expect(properties['write.variant.shred-columns']).toBe('$data');
      expect(properties['write.variant.$data.shred-fields']).toBe('title,year');
      expect(properties['write.variant.$data.field-types']).toMatch(/title:string/);
      expect(properties['write.variant.$data.field-types']).toMatch(/year:int/);
    });

    it('should convert multiple configs to table properties', () => {
      const configs: VariantShredConfig[] = [
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

      const properties = toTableProperties(configs);

      expect(properties['write.variant.shred-columns']).toBe('$data,$index');
      expect(properties['write.variant.$data.shred-fields']).toBe('title');
      expect(properties['write.variant.$index.shred-fields']).toBe('key,value');
    });

    it('should omit field-types property when empty', () => {
      const configs: VariantShredConfig[] = [
        {
          columnName: 'metadata',
          fields: ['name', 'value'],
          fieldTypes: {},
        },
      ];

      const properties = toTableProperties(configs);

      expect(properties['write.variant.shred-columns']).toBe('metadata');
      expect(properties['write.variant.metadata.shred-fields']).toBe('name,value');
      expect(properties['write.variant.metadata.field-types']).toBeUndefined();
    });

    it('should return empty object for empty configs', () => {
      const properties = toTableProperties([]);
      expect(properties).toEqual({});
    });
  });

  describe('Round-Trip Serialization', () => {
    it('should round-trip single config', () => {
      const original: VariantShredConfig[] = [
        {
          columnName: '$data',
          fields: ['title', 'year', 'rating'],
          fieldTypes: {
            title: 'string',
            year: 'int',
            rating: 'double',
          },
        },
      ];

      const properties = toTableProperties(original);
      const parsed = extractVariantShredConfig(properties);

      expect(parsed).toEqual(original);
    });

    it('should round-trip multiple configs', () => {
      const original: VariantShredConfig[] = [
        {
          columnName: '$data',
          fields: ['name', 'age'],
          fieldTypes: { name: 'string', age: 'int' },
        },
        {
          columnName: 'metadata',
          fields: ['key'],
          fieldTypes: { key: 'string' },
        },
      ];

      const properties = toTableProperties(original);
      const parsed = extractVariantShredConfig(properties);

      expect(parsed).toEqual(original);
    });

    it('should round-trip config without field types', () => {
      const original: VariantShredConfig[] = [
        {
          columnName: 'dynamic',
          fields: ['a', 'b', 'c'],
          fieldTypes: {},
        },
      ];

      const properties = toTableProperties(original);
      const parsed = extractVariantShredConfig(properties);

      expect(parsed).toEqual(original);
    });
  });

  describe('Validation', () => {
    it('should pass validation for valid config', () => {
      const config: VariantShredConfig = {
        columnName: '$data',
        fields: ['title', 'year'],
        fieldTypes: {
          title: 'string',
          year: 'int',
        },
      };

      expect(() => validateShredConfig(config)).not.toThrow();
    });

    it('should throw on empty column name', () => {
      const config: VariantShredConfig = {
        columnName: '',
        fields: ['title'],
        fieldTypes: {},
      };

      expect(() => validateShredConfig(config)).toThrow(/column name/i);
    });

    it('should throw on whitespace-only column name', () => {
      const config: VariantShredConfig = {
        columnName: '   ',
        fields: ['title'],
        fieldTypes: {},
      };

      expect(() => validateShredConfig(config)).toThrow(/column name/i);
    });

    it('should throw on empty fields array', () => {
      const config: VariantShredConfig = {
        columnName: '$data',
        fields: [],
        fieldTypes: {},
      };

      expect(() => validateShredConfig(config)).toThrow(/fields/i);
    });

    it('should throw on invalid field type', () => {
      const config: VariantShredConfig = {
        columnName: '$data',
        fields: ['title'],
        fieldTypes: {
          title: 'invalid_type' as IcebergPrimitiveType,
        },
      };

      expect(() => validateShredConfig(config)).toThrow(/field type/i);
    });

    it('should allow all valid primitive types', () => {
      const validTypes: IcebergPrimitiveType[] = [
        'boolean',
        'int',
        'long',
        'float',
        'double',
        'decimal',
        'date',
        'time',
        'timestamp',
        'timestamptz',
        'string',
        'uuid',
        'fixed',
        'binary',
      ];

      for (const type of validTypes) {
        const config: VariantShredConfig = {
          columnName: '$data',
          fields: ['field'],
          fieldTypes: { field: type },
        };

        expect(() => validateShredConfig(config)).not.toThrow();
      }
    });

    it('should validate field types reference existing fields', () => {
      const config: VariantShredConfig = {
        columnName: '$data',
        fields: ['title'],
        fieldTypes: {
          nonexistent: 'string', // This field is not in fields array
        },
      };

      expect(() => validateShredConfig(config)).toThrow(/field.*not.*declared/i);
    });

    it('should allow fieldTypes to be a subset of fields', () => {
      const config: VariantShredConfig = {
        columnName: '$data',
        fields: ['title', 'year', 'rating'],
        fieldTypes: {
          title: 'string', // Only type hint for title, year and rating are dynamic
        },
      };

      expect(() => validateShredConfig(config)).not.toThrow();
    });
  });

  describe('Type Interface', () => {
    it('should have correct VariantShredConfig interface', () => {
      const config: VariantShredConfig = {
        columnName: 'test',
        fields: ['a', 'b'],
        fieldTypes: { a: 'string' },
      };

      // TypeScript compile-time check
      expect(typeof config.columnName).toBe('string');
      expect(Array.isArray(config.fields)).toBe(true);
      expect(typeof config.fieldTypes).toBe('object');
    });
  });
});
