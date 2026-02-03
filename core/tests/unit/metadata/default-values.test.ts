/**
 * Tests for Iceberg v3 Default Values Support
 *
 * @see https://iceberg.apache.org/spec/#default-values
 *
 * Iceberg v3 introduces initial-default and write-default fields to support
 * default values for schema fields.
 *
 * - initial-default: The default value when reading rows that were written
 *   before this field was added. Cannot be changed once set.
 * - write-default: The default value when writing new rows that don't specify
 *   a value for this field. Can be changed through schema evolution.
 *
 * Key constraints:
 * - For new required fields added via schema evolution, initial-default MUST be set
 *   (unless the field has null default, which isn't allowed for required fields)
 * - Fields with type unknown, variant, geometry, or geography MUST have null defaults
 * - Struct defaults must be empty {} or null (sub-field defaults tracked separately)
 */

import { describe, it, expect } from 'vitest';
import type { IcebergSchema, IcebergStructField } from '../../../src/index.js';
import {
  validateFieldDefault,
  validateSchema,
  canChangeWriteDefault,
  canChangeInitialDefault,
} from '../../../src/metadata/schema.js';

describe('Iceberg v3 Default Values', () => {
  describe('Type Definitions', () => {
    it('should allow IcebergStructField to have optional initial-default field', () => {
      const field: IcebergStructField = {
        id: 1,
        name: 'count',
        required: false,
        type: 'int',
        'initial-default': 0,
      };

      expect(field['initial-default']).toBe(0);
    });

    it('should allow IcebergStructField to have optional write-default field', () => {
      const field: IcebergStructField = {
        id: 1,
        name: 'status',
        required: false,
        type: 'string',
        'write-default': 'pending',
      };

      expect(field['write-default']).toBe('pending');
    });

    it('should allow IcebergStructField to have both initial-default and write-default', () => {
      const field: IcebergStructField = {
        id: 1,
        name: 'priority',
        required: false,
        type: 'int',
        'initial-default': 0,
        'write-default': 1,
      };

      expect(field['initial-default']).toBe(0);
      expect(field['write-default']).toBe(1);
    });

    describe('Default value types', () => {
      it('should allow string default value', () => {
        const field: IcebergStructField = {
          id: 1,
          name: 'name',
          required: false,
          type: 'string',
          'initial-default': 'default_name',
        };

        expect(field['initial-default']).toBe('default_name');
      });

      it('should allow number default value', () => {
        const field: IcebergStructField = {
          id: 1,
          name: 'amount',
          required: false,
          type: 'double',
          'initial-default': 3.14,
        };

        expect(field['initial-default']).toBe(3.14);
      });

      it('should allow boolean default value', () => {
        const field: IcebergStructField = {
          id: 1,
          name: 'active',
          required: false,
          type: 'boolean',
          'initial-default': true,
        };

        expect(field['initial-default']).toBe(true);
      });

      it('should allow null default value', () => {
        const field: IcebergStructField = {
          id: 1,
          name: 'optional_field',
          required: false,
          type: 'string',
          'initial-default': null,
        };

        expect(field['initial-default']).toBeNull();
      });

      it('should allow object default value for struct types', () => {
        const field: IcebergStructField = {
          id: 1,
          name: 'metadata',
          required: false,
          type: {
            type: 'struct',
            fields: [
              { id: 2, name: 'key', required: true, type: 'string' },
            ],
          },
          'initial-default': {},
        };

        expect(field['initial-default']).toEqual({});
      });

      it('should allow array default value for list types', () => {
        const field: IcebergStructField = {
          id: 1,
          name: 'tags',
          required: false,
          type: {
            type: 'list',
            'element-id': 2,
            element: 'string',
            'element-required': true,
          },
          'initial-default': [],
        };

        expect(field['initial-default']).toEqual([]);
      });
    });
  });

  describe('Schema Creation with Default Values', () => {
    it('should create schema with field that has initial-default', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'count',
            required: false,
            type: 'int',
            'initial-default': 0,
          },
        ],
      };

      expect(schema.fields[0]['initial-default']).toBe(0);
    });

    it('should create schema with field that has write-default', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'status',
            required: false,
            type: 'string',
            'write-default': 'active',
          },
        ],
      };

      expect(schema.fields[0]['write-default']).toBe('active');
    });

    it('should create schema with field that has both defaults', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'version',
            required: false,
            type: 'int',
            'initial-default': 1,
            'write-default': 2,
          },
        ],
      };

      expect(schema.fields[0]['initial-default']).toBe(1);
      expect(schema.fields[0]['write-default']).toBe(2);
    });

    it('should create schema with multiple fields having different defaults', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'id',
            required: true,
            type: 'long',
          },
          {
            id: 2,
            name: 'name',
            required: false,
            type: 'string',
            'initial-default': 'unknown',
            'write-default': 'new_user',
          },
          {
            id: 3,
            name: 'count',
            required: false,
            type: 'int',
            'initial-default': 0,
          },
        ],
      };

      expect(schema.fields[0]['initial-default']).toBeUndefined();
      expect(schema.fields[1]['initial-default']).toBe('unknown');
      expect(schema.fields[1]['write-default']).toBe('new_user');
      expect(schema.fields[2]['initial-default']).toBe(0);
      expect(schema.fields[2]['write-default']).toBeUndefined();
    });
  });

  describe('Validation: Required Fields and Defaults', () => {
    it('should pass validation for required field with non-null initial-default', () => {
      const field: IcebergStructField = {
        id: 1,
        name: 'count',
        required: true,
        type: 'int',
        'initial-default': 0,
      };

      const result = validateFieldDefault(field);
      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should reject required field with null initial-default for schema evolution', () => {
      const field: IcebergStructField = {
        id: 1,
        name: 'count',
        required: true,
        type: 'int',
        'initial-default': null,
      };

      const result = validateFieldDefault(field, { isNewField: true });
      expect(result.valid).toBe(false);
      expect(result.errors).toContain(
        "Required field 'count' cannot have null as initial-default when adding to existing table"
      );
    });

    it('should reject new required field without initial-default for schema evolution', () => {
      const field: IcebergStructField = {
        id: 1,
        name: 'mandatory',
        required: true,
        type: 'string',
      };

      const result = validateFieldDefault(field, { isNewField: true });
      expect(result.valid).toBe(false);
      expect(result.errors).toContain(
        "Required field 'mandatory' must have initial-default when adding to existing table"
      );
    });

    it('should allow required field without defaults when not adding to existing table', () => {
      const field: IcebergStructField = {
        id: 1,
        name: 'id',
        required: true,
        type: 'long',
      };

      const result = validateFieldDefault(field, { isNewField: false });
      expect(result.valid).toBe(true);
    });

    it('should allow optional field without defaults', () => {
      const field: IcebergStructField = {
        id: 1,
        name: 'optional_field',
        required: false,
        type: 'string',
      };

      const result = validateFieldDefault(field);
      expect(result.valid).toBe(true);
    });
  });

  describe('Validation: Special Type Defaults', () => {
    it('should require null default for unknown type', () => {
      const field: IcebergStructField = {
        id: 1,
        name: 'placeholder',
        required: false,
        type: 'unknown',
        'initial-default': 'some_value',
      };

      const result = validateFieldDefault(field);
      expect(result.valid).toBe(false);
      expect(result.errors).toContain(
        "Field 'placeholder' with type 'unknown' must have null default"
      );
    });

    it('should allow null default for unknown type', () => {
      const field: IcebergStructField = {
        id: 1,
        name: 'placeholder',
        required: false,
        type: 'unknown',
        'initial-default': null,
      };

      const result = validateFieldDefault(field);
      expect(result.valid).toBe(true);
    });

    it('should require null default for variant type', () => {
      const field: IcebergStructField = {
        id: 1,
        name: 'dynamic_data',
        required: false,
        type: 'variant',
        'initial-default': {},
      };

      const result = validateFieldDefault(field);
      expect(result.valid).toBe(false);
      expect(result.errors).toContain(
        "Field 'dynamic_data' with type 'variant' must have null default"
      );
    });

    it('should allow null default for variant type', () => {
      const field: IcebergStructField = {
        id: 1,
        name: 'dynamic_data',
        required: false,
        type: 'variant',
        'initial-default': null,
      };

      const result = validateFieldDefault(field);
      expect(result.valid).toBe(true);
    });

    it('should require null default for geometry type', () => {
      const field: IcebergStructField = {
        id: 1,
        name: 'location',
        required: false,
        type: 'geometry',
        'initial-default': 'POINT(0 0)',
      };

      const result = validateFieldDefault(field);
      expect(result.valid).toBe(false);
      expect(result.errors).toContain(
        "Field 'location' with type 'geometry' must have null default"
      );
    });

    it('should require null default for geography type', () => {
      const field: IcebergStructField = {
        id: 1,
        name: 'earth_location',
        required: false,
        type: 'geography',
        'initial-default': 'some_value',
      };

      const result = validateFieldDefault(field);
      expect(result.valid).toBe(false);
      expect(result.errors).toContain(
        "Field 'earth_location' with type 'geography' must have null default"
      );
    });

    it('should require null default for parameterized geometry type', () => {
      const field: IcebergStructField = {
        id: 1,
        name: 'geo_point',
        required: false,
        type: 'geometry(EPSG:4326)',
        'initial-default': {},
      };

      const result = validateFieldDefault(field);
      expect(result.valid).toBe(false);
      expect(result.errors.some((e) => e.includes('geometry') && e.includes('null default'))).toBe(
        true
      );
    });

    it('should require null default for parameterized geography type', () => {
      const field: IcebergStructField = {
        id: 1,
        name: 'earth_point',
        required: false,
        type: 'geography(OGC:CRS84, vincenty)',
        'initial-default': {},
      };

      const result = validateFieldDefault(field);
      expect(result.valid).toBe(false);
      expect(
        result.errors.some((e) => e.includes('geography') && e.includes('null default'))
      ).toBe(true);
    });
  });

  describe('Validation: Struct Type Defaults', () => {
    it('should allow null default for struct type', () => {
      const field: IcebergStructField = {
        id: 1,
        name: 'nested',
        required: false,
        type: {
          type: 'struct',
          fields: [{ id: 2, name: 'inner', required: true, type: 'string' }],
        },
        'initial-default': null,
      };

      const result = validateFieldDefault(field);
      expect(result.valid).toBe(true);
    });

    it('should allow empty object default for struct type', () => {
      const field: IcebergStructField = {
        id: 1,
        name: 'nested',
        required: false,
        type: {
          type: 'struct',
          fields: [{ id: 2, name: 'inner', required: false, type: 'string' }],
        },
        'initial-default': {},
      };

      const result = validateFieldDefault(field);
      expect(result.valid).toBe(true);
    });

    it('should reject non-empty object default for struct type', () => {
      const field: IcebergStructField = {
        id: 1,
        name: 'nested',
        required: false,
        type: {
          type: 'struct',
          fields: [{ id: 2, name: 'inner', required: false, type: 'string' }],
        },
        'initial-default': { inner: 'value' },
      };

      const result = validateFieldDefault(field);
      expect(result.valid).toBe(false);
      expect(result.errors).toContain(
        "Field 'nested' with struct type must have empty {} or null as default"
      );
    });
  });

  describe('Schema Evolution: initial-default Immutability', () => {
    it('should not allow changing initial-default after it is set', () => {
      const oldField: IcebergStructField = {
        id: 1,
        name: 'count',
        required: false,
        type: 'int',
        'initial-default': 0,
      };

      const newField: IcebergStructField = {
        id: 1,
        name: 'count',
        required: false,
        type: 'int',
        'initial-default': 10, // Trying to change
      };

      const result = canChangeInitialDefault(oldField, newField);
      expect(result.allowed).toBe(false);
      expect(result.reason).toBe("initial-default cannot be changed once set");
    });

    it('should allow setting initial-default when it was not set before', () => {
      const oldField: IcebergStructField = {
        id: 1,
        name: 'count',
        required: false,
        type: 'int',
      };

      const newField: IcebergStructField = {
        id: 1,
        name: 'count',
        required: false,
        type: 'int',
        'initial-default': 0,
      };

      const result = canChangeInitialDefault(oldField, newField);
      expect(result.allowed).toBe(true);
    });

    it('should allow keeping initial-default the same', () => {
      const oldField: IcebergStructField = {
        id: 1,
        name: 'count',
        required: false,
        type: 'int',
        'initial-default': 0,
      };

      const newField: IcebergStructField = {
        id: 1,
        name: 'count',
        required: false,
        type: 'int',
        'initial-default': 0,
      };

      const result = canChangeInitialDefault(oldField, newField);
      expect(result.allowed).toBe(true);
    });
  });

  describe('Schema Evolution: write-default Changes', () => {
    it('should allow changing write-default', () => {
      const oldField: IcebergStructField = {
        id: 1,
        name: 'priority',
        required: false,
        type: 'int',
        'write-default': 0,
      };

      const newField: IcebergStructField = {
        id: 1,
        name: 'priority',
        required: false,
        type: 'int',
        'write-default': 5,
      };

      const result = canChangeWriteDefault(oldField, newField);
      expect(result.allowed).toBe(true);
    });

    it('should allow setting write-default when it was not set before', () => {
      const oldField: IcebergStructField = {
        id: 1,
        name: 'priority',
        required: false,
        type: 'int',
      };

      const newField: IcebergStructField = {
        id: 1,
        name: 'priority',
        required: false,
        type: 'int',
        'write-default': 0,
      };

      const result = canChangeWriteDefault(oldField, newField);
      expect(result.allowed).toBe(true);
    });

    it('should allow removing write-default', () => {
      const oldField: IcebergStructField = {
        id: 1,
        name: 'priority',
        required: false,
        type: 'int',
        'write-default': 0,
      };

      const newField: IcebergStructField = {
        id: 1,
        name: 'priority',
        required: false,
        type: 'int',
      };

      const result = canChangeWriteDefault(oldField, newField);
      expect(result.allowed).toBe(true);
    });
  });

  describe('JSON Serialization', () => {
    it('should serialize initial-default in schema JSON', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'count',
            required: false,
            type: 'int',
            'initial-default': 0,
          },
        ],
      };

      const json = JSON.stringify(schema);
      expect(json).toContain('"initial-default"');
      expect(json).toContain(':0');
    });

    it('should serialize write-default in schema JSON', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'status',
            required: false,
            type: 'string',
            'write-default': 'active',
          },
        ],
      };

      const json = JSON.stringify(schema);
      expect(json).toContain('"write-default"');
      expect(json).toContain('"active"');
    });

    it('should serialize null default correctly', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'optional',
            required: false,
            type: 'string',
            'initial-default': null,
          },
        ],
      };

      const json = JSON.stringify(schema);
      const parsed = JSON.parse(json);

      expect(parsed.fields[0]['initial-default']).toBeNull();
    });

    it('should round-trip serialize defaults correctly', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'version',
            required: false,
            type: 'int',
            'initial-default': 1,
            'write-default': 2,
          },
          {
            id: 2,
            name: 'name',
            required: false,
            type: 'string',
            'initial-default': 'default',
          },
          {
            id: 3,
            name: 'active',
            required: false,
            type: 'boolean',
            'write-default': true,
          },
        ],
      };

      const json = JSON.stringify(schema);
      const parsed = JSON.parse(json) as IcebergSchema;

      expect(parsed.fields[0]['initial-default']).toBe(1);
      expect(parsed.fields[0]['write-default']).toBe(2);
      expect(parsed.fields[1]['initial-default']).toBe('default');
      expect(parsed.fields[1]['write-default']).toBeUndefined();
      expect(parsed.fields[2]['initial-default']).toBeUndefined();
      expect(parsed.fields[2]['write-default']).toBe(true);
    });

    it('should serialize complex defaults correctly', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'tags',
            required: false,
            type: {
              type: 'list',
              'element-id': 2,
              element: 'string',
              'element-required': true,
            },
            'initial-default': [],
          },
          {
            id: 3,
            name: 'metadata',
            required: false,
            type: {
              type: 'struct',
              fields: [{ id: 4, name: 'key', required: false, type: 'string' }],
            },
            'initial-default': {},
          },
        ],
      };

      const json = JSON.stringify(schema);
      const parsed = JSON.parse(json) as IcebergSchema;

      expect(parsed.fields[0]['initial-default']).toEqual([]);
      expect(parsed.fields[1]['initial-default']).toEqual({});
    });
  });

  describe('Full Schema Validation with Defaults', () => {
    it('should validate schema with correct defaults', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          {
            id: 2,
            name: 'name',
            required: false,
            type: 'string',
            'initial-default': 'unknown',
          },
          {
            id: 3,
            name: 'placeholder',
            required: false,
            type: 'unknown',
            'initial-default': null,
          },
        ],
      };

      const result = validateSchema(schema);
      expect(result.valid).toBe(true);
    });

    it('should reject schema with invalid defaults', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          {
            id: 2,
            name: 'dynamic',
            required: false,
            type: 'variant',
            'initial-default': 'not_null', // Invalid!
          },
        ],
      };

      const result = validateSchema(schema);
      expect(result.valid).toBe(false);
      expect(result.errors.some((e) => e.includes('variant') && e.includes('null'))).toBe(true);
    });
  });
});
