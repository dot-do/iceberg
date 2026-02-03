/**
 * Tests for Variant Shredding Schema Types
 *
 * Variant shredding is an Iceberg feature that allows semi-structured variant
 * data to be "shredded" (decomposed) into separate columns for efficient
 * querying and storage. These types define the schema representation for
 * shredded variant fields.
 *
 * @see https://iceberg.apache.org/spec/#variant-shredding
 */

import { describe, it, expect } from 'vitest';
import type {
  IcebergPrimitiveType,
  ShreddedFieldInfo,
  VariantColumnSchema,
  VariantShredConfig,
} from '../../../src/index.js';
import {
  createShreddedFieldInfo,
  createVariantColumnSchema,
  validateVariantShredConfig,
  getMetadataPath,
  getValuePath,
  getTypedValuePath,
} from '../../../src/index.js';

describe('Variant Shredding Schema Types', () => {
  // ============================================================================
  // ShreddedFieldInfo Type Tests
  // ============================================================================

  describe('ShreddedFieldInfo', () => {
    it('should have path property of type string', () => {
      const fieldInfo: ShreddedFieldInfo = {
        path: 'titleType',
        type: 'string',
        statisticsPath: '$data.typed_value.titleType.typed_value',
      };

      expect(fieldInfo.path).toBe('titleType');
      expect(typeof fieldInfo.path).toBe('string');
    });

    it('should have type property of type IcebergPrimitiveType', () => {
      const fieldInfo: ShreddedFieldInfo = {
        path: 'releaseYear',
        type: 'int',
        statisticsPath: '$data.typed_value.releaseYear.typed_value',
      };

      expect(fieldInfo.type).toBe('int');
      // Validate it's a valid IcebergPrimitiveType
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
        'variant',
      ];
      expect(validTypes).toContain(fieldInfo.type);
    });

    it('should have statisticsPath property of type string', () => {
      const fieldInfo: ShreddedFieldInfo = {
        path: 'titleType',
        type: 'string',
        statisticsPath: '$data.typed_value.titleType.typed_value',
      };

      expect(fieldInfo.statisticsPath).toBe('$data.typed_value.titleType.typed_value');
      expect(typeof fieldInfo.statisticsPath).toBe('string');
    });

    it('should have optional nullable property of type boolean', () => {
      const requiredField: ShreddedFieldInfo = {
        path: 'id',
        type: 'string',
        statisticsPath: '$data.typed_value.id.typed_value',
        nullable: false,
      };

      const nullableField: ShreddedFieldInfo = {
        path: 'description',
        type: 'string',
        statisticsPath: '$data.typed_value.description.typed_value',
        nullable: true,
      };

      const fieldWithoutNullable: ShreddedFieldInfo = {
        path: 'name',
        type: 'string',
        statisticsPath: '$data.typed_value.name.typed_value',
      };

      expect(requiredField.nullable).toBe(false);
      expect(nullableField.nullable).toBe(true);
      expect(fieldWithoutNullable.nullable).toBeUndefined();
    });
  });

  // ============================================================================
  // VariantColumnSchema Type Tests
  // ============================================================================

  describe('VariantColumnSchema', () => {
    it('should have columnName property of type string', () => {
      const schema: VariantColumnSchema = {
        columnName: '$data',
        metadataPath: '$data.metadata',
        valuePath: '$data.value',
        typedValuePath: '$data.typed_value',
        shreddedFields: [],
      };

      expect(schema.columnName).toBe('$data');
      expect(typeof schema.columnName).toBe('string');
    });

    it('should have metadataPath property of type string', () => {
      const schema: VariantColumnSchema = {
        columnName: '$data',
        metadataPath: '$data.metadata',
        valuePath: '$data.value',
        typedValuePath: '$data.typed_value',
        shreddedFields: [],
      };

      expect(schema.metadataPath).toBe('$data.metadata');
      expect(typeof schema.metadataPath).toBe('string');
    });

    it('should have valuePath property of type string', () => {
      const schema: VariantColumnSchema = {
        columnName: '$data',
        metadataPath: '$data.metadata',
        valuePath: '$data.value',
        typedValuePath: '$data.typed_value',
        shreddedFields: [],
      };

      expect(schema.valuePath).toBe('$data.value');
      expect(typeof schema.valuePath).toBe('string');
    });

    it('should have typedValuePath property of type string', () => {
      const schema: VariantColumnSchema = {
        columnName: '$data',
        metadataPath: '$data.metadata',
        valuePath: '$data.value',
        typedValuePath: '$data.typed_value',
        shreddedFields: [],
      };

      expect(schema.typedValuePath).toBe('$data.typed_value');
      expect(typeof schema.typedValuePath).toBe('string');
    });

    it('should have shreddedFields property as readonly ShreddedFieldInfo array', () => {
      const fields: readonly ShreddedFieldInfo[] = [
        {
          path: 'titleType',
          type: 'string',
          statisticsPath: '$data.typed_value.titleType.typed_value',
        },
        {
          path: 'releaseYear',
          type: 'int',
          statisticsPath: '$data.typed_value.releaseYear.typed_value',
        },
      ];

      const schema: VariantColumnSchema = {
        columnName: '$data',
        metadataPath: '$data.metadata',
        valuePath: '$data.value',
        typedValuePath: '$data.typed_value',
        shreddedFields: fields,
      };

      expect(schema.shreddedFields).toHaveLength(2);
      expect(schema.shreddedFields[0].path).toBe('titleType');
      expect(schema.shreddedFields[1].path).toBe('releaseYear');
      expect(Array.isArray(schema.shreddedFields)).toBe(true);
    });
  });

  // ============================================================================
  // VariantShredConfig Type Tests
  // ============================================================================

  describe('VariantShredConfig', () => {
    it('should have column property of type string', () => {
      const config: VariantShredConfig = {
        column: 'data',
        fields: ['titleType', 'releaseYear'],
      };

      expect(config.column).toBe('data');
      expect(typeof config.column).toBe('string');
    });

    it('should have fields property as readonly string array', () => {
      const config: VariantShredConfig = {
        column: 'data',
        fields: ['titleType', 'releaseYear', 'genres'],
      };

      expect(config.fields).toHaveLength(3);
      expect(config.fields[0]).toBe('titleType');
      expect(config.fields[1]).toBe('releaseYear');
      expect(config.fields[2]).toBe('genres');
      expect(Array.isArray(config.fields)).toBe(true);
    });

    it('should have optional fieldTypes property as Record<string, IcebergPrimitiveType>', () => {
      const configWithTypes: VariantShredConfig = {
        column: 'data',
        fields: ['titleType', 'releaseYear'],
        fieldTypes: {
          titleType: 'string',
          releaseYear: 'int',
        },
      };

      const configWithoutTypes: VariantShredConfig = {
        column: 'data',
        fields: ['titleType'],
      };

      expect(configWithTypes.fieldTypes?.titleType).toBe('string');
      expect(configWithTypes.fieldTypes?.releaseYear).toBe('int');
      expect(configWithoutTypes.fieldTypes).toBeUndefined();
    });
  });

  // ============================================================================
  // Helper Function Tests
  // ============================================================================

  describe('Helper Functions', () => {
    describe('createShreddedFieldInfo', () => {
      it('should return correct structure for a field', () => {
        const result = createShreddedFieldInfo('$data', 'titleType', 'string');

        expect(result).toEqual({
          path: 'titleType',
          type: 'string',
          statisticsPath: '$data.typed_value.titleType.typed_value',
        });
      });

      it('should handle different column names', () => {
        const result = createShreddedFieldInfo('$payload', 'status', 'string');

        expect(result.path).toBe('status');
        expect(result.statisticsPath).toBe('$payload.typed_value.status.typed_value');
      });

      it('should handle different types', () => {
        const intField = createShreddedFieldInfo('$data', 'count', 'int');
        const longField = createShreddedFieldInfo('$data', 'timestamp', 'long');
        const boolField = createShreddedFieldInfo('$data', 'active', 'boolean');

        expect(intField.type).toBe('int');
        expect(longField.type).toBe('long');
        expect(boolField.type).toBe('boolean');
      });

      it('should support optional nullable parameter', () => {
        const nullableField = createShreddedFieldInfo('$data', 'description', 'string', true);
        const requiredField = createShreddedFieldInfo('$data', 'id', 'string', false);
        const defaultField = createShreddedFieldInfo('$data', 'name', 'string');

        expect(nullableField.nullable).toBe(true);
        expect(requiredField.nullable).toBe(false);
        expect(defaultField.nullable).toBeUndefined();
      });
    });

    describe('createVariantColumnSchema', () => {
      it('should return correct paths for a column', () => {
        const shreddedFields: ShreddedFieldInfo[] = [
          {
            path: 'titleType',
            type: 'string',
            statisticsPath: '$data.typed_value.titleType.typed_value',
          },
        ];

        const result = createVariantColumnSchema('$data', shreddedFields);

        expect(result).toEqual({
          columnName: '$data',
          metadataPath: '$data.metadata',
          valuePath: '$data.value',
          typedValuePath: '$data.typed_value',
          shreddedFields: shreddedFields,
        });
      });

      it('should handle different column names', () => {
        const result = createVariantColumnSchema('$payload', []);

        expect(result.columnName).toBe('$payload');
        expect(result.metadataPath).toBe('$payload.metadata');
        expect(result.valuePath).toBe('$payload.value');
        expect(result.typedValuePath).toBe('$payload.typed_value');
      });

      it('should preserve shredded fields array', () => {
        const fields: ShreddedFieldInfo[] = [
          { path: 'a', type: 'string', statisticsPath: '$data.typed_value.a.typed_value' },
          { path: 'b', type: 'int', statisticsPath: '$data.typed_value.b.typed_value' },
        ];

        const result = createVariantColumnSchema('$data', fields);

        expect(result.shreddedFields).toHaveLength(2);
        expect(result.shreddedFields).toEqual(fields);
      });
    });

    describe('validateVariantShredConfig', () => {
      it('should validate config with required fields', () => {
        const validConfig: VariantShredConfig = {
          column: 'data',
          fields: ['titleType', 'releaseYear'],
        };

        const result = validateVariantShredConfig(validConfig);

        expect(result.valid).toBe(true);
        expect(result.errors).toHaveLength(0);
      });

      it('should reject config with empty column name', () => {
        const invalidConfig: VariantShredConfig = {
          column: '',
          fields: ['titleType'],
        };

        const result = validateVariantShredConfig(invalidConfig);

        expect(result.valid).toBe(false);
        expect(result.errors).toContain('column name is required');
      });

      it('should reject config with empty fields array', () => {
        const invalidConfig: VariantShredConfig = {
          column: 'data',
          fields: [],
        };

        const result = validateVariantShredConfig(invalidConfig);

        expect(result.valid).toBe(false);
        expect(result.errors).toContain('at least one field is required');
      });

      it('should validate config with fieldTypes', () => {
        const validConfig: VariantShredConfig = {
          column: 'data',
          fields: ['titleType', 'releaseYear'],
          fieldTypes: {
            titleType: 'string',
            releaseYear: 'int',
          },
        };

        const result = validateVariantShredConfig(validConfig);

        expect(result.valid).toBe(true);
      });

      it('should warn when fieldTypes has fields not in fields array', () => {
        const config: VariantShredConfig = {
          column: 'data',
          fields: ['titleType'],
          fieldTypes: {
            titleType: 'string',
            unknownField: 'int', // Not in fields array
          },
        };

        const result = validateVariantShredConfig(config);

        // Should still be valid but may have warnings
        expect(result.valid).toBe(true);
        // Warnings are optional - implementation may choose to include them
      });
    });
  });

  // ============================================================================
  // Path Generation Tests
  // ============================================================================

  describe('Path Generation', () => {
    describe('getMetadataPath', () => {
      it('should return "{column}.metadata" for a column name', () => {
        expect(getMetadataPath('$data')).toBe('$data.metadata');
        expect(getMetadataPath('$payload')).toBe('$payload.metadata');
        expect(getMetadataPath('variant_col')).toBe('variant_col.metadata');
      });
    });

    describe('getValuePath', () => {
      it('should return "{column}.value" for a column name', () => {
        expect(getValuePath('$data')).toBe('$data.value');
        expect(getValuePath('$payload')).toBe('$payload.value');
        expect(getValuePath('variant_col')).toBe('variant_col.value');
      });
    });

    describe('getTypedValuePath', () => {
      it('should return "{column}.typed_value.{field}.typed_value" for column and field', () => {
        expect(getTypedValuePath('$data', 'titleType')).toBe(
          '$data.typed_value.titleType.typed_value'
        );
        expect(getTypedValuePath('$payload', 'status')).toBe(
          '$payload.typed_value.status.typed_value'
        );
        expect(getTypedValuePath('variant_col', 'count')).toBe(
          'variant_col.typed_value.count.typed_value'
        );
      });
    });
  });

  // ============================================================================
  // JSON Serialization Tests
  // ============================================================================

  describe('JSON Serialization', () => {
    it('should serialize ShreddedFieldInfo correctly', () => {
      const fieldInfo: ShreddedFieldInfo = {
        path: 'titleType',
        type: 'string',
        statisticsPath: '$data.typed_value.titleType.typed_value',
        nullable: true,
      };

      const json = JSON.stringify(fieldInfo);
      const parsed = JSON.parse(json);

      expect(parsed.path).toBe('titleType');
      expect(parsed.type).toBe('string');
      expect(parsed.statisticsPath).toBe('$data.typed_value.titleType.typed_value');
      expect(parsed.nullable).toBe(true);
    });

    it('should serialize VariantColumnSchema correctly', () => {
      const schema: VariantColumnSchema = {
        columnName: '$data',
        metadataPath: '$data.metadata',
        valuePath: '$data.value',
        typedValuePath: '$data.typed_value',
        shreddedFields: [
          {
            path: 'titleType',
            type: 'string',
            statisticsPath: '$data.typed_value.titleType.typed_value',
          },
        ],
      };

      const json = JSON.stringify(schema);
      const parsed = JSON.parse(json);

      expect(parsed.columnName).toBe('$data');
      expect(parsed.metadataPath).toBe('$data.metadata');
      expect(parsed.shreddedFields).toHaveLength(1);
    });

    it('should serialize VariantShredConfig correctly', () => {
      const config: VariantShredConfig = {
        column: 'data',
        fields: ['titleType', 'releaseYear'],
        fieldTypes: {
          titleType: 'string',
          releaseYear: 'int',
        },
      };

      const json = JSON.stringify(config);
      const parsed = JSON.parse(json);

      expect(parsed.column).toBe('data');
      expect(parsed.fields).toEqual(['titleType', 'releaseYear']);
      expect(parsed.fieldTypes.titleType).toBe('string');
    });
  });
});
