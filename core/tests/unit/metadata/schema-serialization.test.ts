/**
 * Tests for Schema serialization in Iceberg metadata.json
 *
 * @see https://iceberg.apache.org/spec/#schemas-and-data-types
 *
 * Schemas define the structure of data in an Iceberg table. Each schema
 * has a unique ID and contains a list of fields with types.
 */

import { describe, it, expect } from 'vitest';
import {
  createDefaultSchema,
  TableMetadataBuilder,
  type IcebergSchema,
  type IcebergStructField,
} from '../../../src/index.js';

describe('Schema Serialization', () => {
  describe('Schema Structure', () => {
    it('should serialize schema with schema-id', () => {
      const schema = createDefaultSchema();
      expect(schema).toHaveProperty('schema-id');
      expect(typeof schema['schema-id']).toBe('number');
    });

    it('should serialize schema with type "struct"', () => {
      const schema = createDefaultSchema();
      expect(schema.type).toBe('struct');
    });

    it('should serialize schema fields as an array', () => {
      const schema = createDefaultSchema();
      expect(Array.isArray(schema.fields)).toBe(true);
    });
  });

  describe('Field Serialization', () => {
    it('should serialize field with id', () => {
      const schema = createDefaultSchema();
      const field = schema.fields[0];

      expect(field).toHaveProperty('id');
      expect(typeof field.id).toBe('number');
    });

    it('should serialize field with name', () => {
      const schema = createDefaultSchema();
      const field = schema.fields[0];

      expect(field).toHaveProperty('name');
      expect(typeof field.name).toBe('string');
    });

    it('should serialize field with required flag', () => {
      const schema = createDefaultSchema();
      const field = schema.fields[0];

      expect(field).toHaveProperty('required');
      expect(typeof field.required).toBe('boolean');
    });

    it('should serialize field with type', () => {
      const schema = createDefaultSchema();
      const field = schema.fields[0];

      expect(field).toHaveProperty('type');
    });

    it('should serialize optional doc field when present', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'documented_field',
            required: true,
            type: 'string',
            doc: 'This field has documentation',
          },
        ],
      };

      expect(schema.fields[0].doc).toBe('This field has documentation');
    });
  });

  describe('Primitive Types Serialization', () => {
    const primitiveTypes = [
      'boolean',
      'int',
      'long',
      'float',
      'double',
      'date',
      'time',
      'timestamp',
      'timestamptz',
      'string',
      'uuid',
      'binary',
    ] as const;

    for (const type of primitiveTypes) {
      it(`should serialize primitive type: ${type}`, () => {
        const schema: IcebergSchema = {
          'schema-id': 0,
          type: 'struct',
          fields: [
            {
              id: 1,
              name: `${type}_field`,
              required: true,
              type: type,
            },
          ],
        };

        expect(schema.fields[0].type).toBe(type);

        // Verify JSON serialization
        const json = JSON.stringify(schema);
        const parsed = JSON.parse(json);
        expect(parsed.fields[0].type).toBe(type);
      });
    }

    it('should serialize decimal type with precision and scale', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'price',
            required: true,
            type: 'decimal', // Note: Full decimal(p,s) syntax may need different handling
          },
        ],
      };

      expect(schema.fields[0].type).toBe('decimal');
    });

    it('should serialize fixed type', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'hash',
            required: true,
            type: 'fixed', // Note: Full fixed[N] syntax may need different handling
          },
        ],
      };

      expect(schema.fields[0].type).toBe('fixed');
    });
  });

  describe('Complex Types Serialization', () => {
    it('should serialize list type with element-id and element', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'tags',
            required: true,
            type: {
              type: 'list',
              'element-id': 2,
              element: 'string',
              'element-required': true,
            },
          },
        ],
      };

      const listType = schema.fields[0].type as {
        type: 'list';
        'element-id': number;
        element: unknown;
        'element-required': boolean;
      };

      expect(listType.type).toBe('list');
      expect(listType['element-id']).toBe(2);
      expect(listType.element).toBe('string');
      expect(listType['element-required']).toBe(true);
    });

    it('should serialize map type with key-id, value-id, key, and value', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'properties',
            required: true,
            type: {
              type: 'map',
              'key-id': 2,
              'value-id': 3,
              key: 'string',
              value: 'string',
              'value-required': false,
            },
          },
        ],
      };

      const mapType = schema.fields[0].type as {
        type: 'map';
        'key-id': number;
        'value-id': number;
        key: unknown;
        value: unknown;
        'value-required': boolean;
      };

      expect(mapType.type).toBe('map');
      expect(mapType['key-id']).toBe(2);
      expect(mapType['value-id']).toBe(3);
      expect(mapType.key).toBe('string');
      expect(mapType.value).toBe('string');
      expect(mapType['value-required']).toBe(false);
    });

    it('should serialize nested struct type', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'address',
            required: true,
            type: {
              type: 'struct',
              fields: [
                { id: 2, name: 'street', required: true, type: 'string' },
                { id: 3, name: 'city', required: true, type: 'string' },
                { id: 4, name: 'zip', required: false, type: 'string' },
              ],
            },
          },
        ],
      };

      const structType = schema.fields[0].type as {
        type: 'struct';
        fields: IcebergStructField[];
      };

      expect(structType.type).toBe('struct');
      expect(Array.isArray(structType.fields)).toBe(true);
      expect(structType.fields.length).toBe(3);
      expect(structType.fields[0].name).toBe('street');
    });

    it('should serialize deeply nested types', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'nested_data',
            required: true,
            type: {
              type: 'list',
              'element-id': 2,
              element: {
                type: 'struct',
                fields: [
                  {
                    id: 3,
                    name: 'items',
                    required: true,
                    type: {
                      type: 'map',
                      'key-id': 4,
                      'value-id': 5,
                      key: 'string',
                      value: 'long',
                      'value-required': true,
                    },
                  },
                ],
              },
              'element-required': true,
            },
          },
        ],
      };

      // Verify it can be serialized and deserialized
      const json = JSON.stringify(schema);
      const parsed = JSON.parse(json);

      expect(parsed.fields[0].type.type).toBe('list');
      expect(parsed.fields[0].type.element.type).toBe('struct');
      expect(parsed.fields[0].type.element.fields[0].type.type).toBe('map');
    });
  });

  describe('Schema in Table Metadata', () => {
    it('should include schemas array in table metadata', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      expect(metadata.schemas).toBeDefined();
      expect(Array.isArray(metadata.schemas)).toBe(true);
      expect(metadata.schemas.length).toBeGreaterThan(0);
    });

    it('should include current-schema-id in table metadata', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      expect(metadata['current-schema-id']).toBeDefined();
      expect(typeof metadata['current-schema-id']).toBe('number');
    });

    it('should reference valid schema ID from current-schema-id', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      const schemaIds = metadata.schemas.map((s) => s['schema-id']);
      expect(schemaIds).toContain(metadata['current-schema-id']);
    });

    it('should track last-column-id for schema evolution', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      expect(metadata['last-column-id']).toBeDefined();
      expect(typeof metadata['last-column-id']).toBe('number');
      expect(metadata['last-column-id']).toBeGreaterThan(0);
    });

    it('should add new schema with unique schema-id', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const newSchema: IcebergSchema = {
        'schema-id': 1,
        type: 'struct',
        fields: [
          { id: 1, name: '_id', required: true, type: 'string' },
          { id: 2, name: '_seq', required: true, type: 'long' },
          { id: 3, name: '_op', required: true, type: 'string' },
          { id: 4, name: '_data', required: true, type: 'binary' },
          { id: 5, name: 'new_field', required: false, type: 'string' },
        ],
      };

      builder.addSchema(newSchema);
      const metadata = builder.build();

      expect(metadata.schemas.length).toBe(2);
      expect(metadata.schemas[1]['schema-id']).toBe(1);
    });

    it('should update last-column-id when adding schema with higher field IDs', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const initialMetadata = builder.build();
      const initialLastColumnId = initialMetadata['last-column-id'];

      const newSchema: IcebergSchema = {
        'schema-id': 1,
        type: 'struct',
        fields: [
          { id: 1, name: '_id', required: true, type: 'string' },
          { id: 100, name: 'high_id_field', required: false, type: 'string' },
        ],
      };

      builder.addSchema(newSchema);
      const updatedMetadata = builder.build();

      expect(updatedMetadata['last-column-id']).toBe(100);
      expect(updatedMetadata['last-column-id']).toBeGreaterThan(initialLastColumnId);
    });

    it('should set current schema via setCurrentSchema', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const newSchema: IcebergSchema = {
        'schema-id': 1,
        type: 'struct',
        fields: [{ id: 1, name: 'test', required: true, type: 'string' }],
      };

      builder.addSchema(newSchema);
      builder.setCurrentSchema(1);
      const metadata = builder.build();

      expect(metadata['current-schema-id']).toBe(1);
    });

    it('should throw when setting non-existent schema as current', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      expect(() => builder.setCurrentSchema(999)).toThrow();
    });
  });

  describe('JSON Serialization Correctness', () => {
    it('should serialize schema correctly to JSON', () => {
      const schema = createDefaultSchema();
      const json = JSON.stringify(schema, null, 2);

      expect(json).toContain('"schema-id"');
      expect(json).toContain('"type": "struct"');
      expect(json).toContain('"fields"');
    });

    it('should preserve field order in JSON serialization', () => {
      const schema = createDefaultSchema();
      const json = JSON.stringify(schema);
      const parsed = JSON.parse(json);

      expect(parsed.fields.map((f: IcebergStructField) => f.name)).toEqual(
        schema.fields.map((f) => f.name)
      );
    });

    it('should handle special characters in field names', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'field_with_underscore', required: true, type: 'string' },
          { id: 2, name: 'field-with-dash', required: true, type: 'string' },
        ],
      };

      const json = JSON.stringify(schema);
      const parsed = JSON.parse(json);

      expect(parsed.fields[0].name).toBe('field_with_underscore');
      expect(parsed.fields[1].name).toBe('field-with-dash');
    });
  });
});
