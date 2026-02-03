/**
 * Tests for Iceberg v3 timestamp types (nanosecond precision).
 *
 * @see https://iceberg.apache.org/spec/#primitive-types
 *
 * Iceberg v3 introduces nanosecond precision timestamps:
 * - timestamp_ns: timestamp without timezone (nanoseconds)
 * - timestamptz_ns: timestamp with timezone (nanoseconds)
 */

import { describe, it, expect } from 'vitest';
import type {
  IcebergPrimitiveType,
  IcebergSchema,
  IcebergStructField,
} from '../../../src/index.js';

describe('Iceberg v3 Timestamp Types', () => {
  describe('Primitive Type Definitions', () => {
    it('should accept timestamp_ns as a valid IcebergPrimitiveType', () => {
      const type: IcebergPrimitiveType = 'timestamp_ns';
      expect(type).toBe('timestamp_ns');
    });

    it('should accept timestamptz_ns as a valid IcebergPrimitiveType', () => {
      const type: IcebergPrimitiveType = 'timestamptz_ns';
      expect(type).toBe('timestamptz_ns');
    });
  });

  describe('Schema Creation with Nanosecond Timestamps', () => {
    it('should create schema with timestamp_ns field', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'event_time',
            required: true,
            type: 'timestamp_ns',
          },
        ],
      };

      expect(schema.fields[0].type).toBe('timestamp_ns');
      expect(schema.fields[0].name).toBe('event_time');
      expect(schema.fields[0].required).toBe(true);
    });

    it('should create schema with timestamptz_ns field', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'created_at',
            required: false,
            type: 'timestamptz_ns',
          },
        ],
      };

      expect(schema.fields[0].type).toBe('timestamptz_ns');
      expect(schema.fields[0].name).toBe('created_at');
      expect(schema.fields[0].required).toBe(false);
    });

    it('should create schema with multiple nanosecond timestamp fields', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'local_time',
            required: true,
            type: 'timestamp_ns',
          },
          {
            id: 2,
            name: 'utc_time',
            required: true,
            type: 'timestamptz_ns',
          },
        ],
      };

      expect(schema.fields).toHaveLength(2);
      expect(schema.fields[0].type).toBe('timestamp_ns');
      expect(schema.fields[1].type).toBe('timestamptz_ns');
    });

    it('should create schema with mixed timestamp precision fields', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'micro_ts',
            required: true,
            type: 'timestamp',
          },
          {
            id: 2,
            name: 'micro_tstz',
            required: true,
            type: 'timestamptz',
          },
          {
            id: 3,
            name: 'nano_ts',
            required: true,
            type: 'timestamp_ns',
          },
          {
            id: 4,
            name: 'nano_tstz',
            required: true,
            type: 'timestamptz_ns',
          },
        ],
      };

      expect(schema.fields).toHaveLength(4);
      expect(schema.fields[0].type).toBe('timestamp');
      expect(schema.fields[1].type).toBe('timestamptz');
      expect(schema.fields[2].type).toBe('timestamp_ns');
      expect(schema.fields[3].type).toBe('timestamptz_ns');
    });
  });

  describe('Struct Fields with Nanosecond Timestamps', () => {
    it('should use timestamp_ns in struct field', () => {
      const field: IcebergStructField = {
        id: 1,
        name: 'event_timestamp',
        required: true,
        type: 'timestamp_ns',
        doc: 'Event timestamp with nanosecond precision',
      };

      expect(field.type).toBe('timestamp_ns');
      expect(field.doc).toBe('Event timestamp with nanosecond precision');
    });

    it('should use timestamptz_ns in struct field', () => {
      const field: IcebergStructField = {
        id: 1,
        name: 'created_at',
        required: false,
        type: 'timestamptz_ns',
        doc: 'Creation timestamp with timezone and nanosecond precision',
      };

      expect(field.type).toBe('timestamptz_ns');
      expect(field.doc).toBe('Creation timestamp with timezone and nanosecond precision');
    });

    it('should use nanosecond timestamps in nested struct', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'event',
            required: true,
            type: {
              type: 'struct',
              fields: [
                {
                  id: 2,
                  name: 'timestamp',
                  required: true,
                  type: 'timestamp_ns',
                },
                {
                  id: 3,
                  name: 'timestamp_utc',
                  required: true,
                  type: 'timestamptz_ns',
                },
              ],
            },
          },
        ],
      };

      const nestedStruct = schema.fields[0].type as { type: 'struct'; fields: IcebergStructField[] };
      expect(nestedStruct.type).toBe('struct');
      expect(nestedStruct.fields[0].type).toBe('timestamp_ns');
      expect(nestedStruct.fields[1].type).toBe('timestamptz_ns');
    });
  });

  describe('JSON Serialization', () => {
    it('should serialize timestamp_ns correctly to JSON', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'ns_timestamp',
            required: true,
            type: 'timestamp_ns',
          },
        ],
      };

      const json = JSON.stringify(schema);
      const parsed = JSON.parse(json);

      expect(parsed.fields[0].type).toBe('timestamp_ns');
    });

    it('should serialize timestamptz_ns correctly to JSON', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'ns_timestamp_tz',
            required: true,
            type: 'timestamptz_ns',
          },
        ],
      };

      const json = JSON.stringify(schema);
      const parsed = JSON.parse(json);

      expect(parsed.fields[0].type).toBe('timestamptz_ns');
    });

    it('should deserialize schema with nanosecond timestamp types', () => {
      const jsonString = JSON.stringify({
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'ts_ns', required: true, type: 'timestamp_ns' },
          { id: 2, name: 'tstz_ns', required: true, type: 'timestamptz_ns' },
        ],
      });

      const parsed: IcebergSchema = JSON.parse(jsonString);

      expect(parsed.fields[0].type).toBe('timestamp_ns');
      expect(parsed.fields[1].type).toBe('timestamptz_ns');
    });

    it('should round-trip serialize/deserialize nanosecond timestamps', () => {
      const originalSchema: IcebergSchema = {
        'schema-id': 1,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          { id: 2, name: 'created_ns', required: true, type: 'timestamp_ns' },
          { id: 3, name: 'updated_ns', required: false, type: 'timestamptz_ns' },
        ],
      };

      const json = JSON.stringify(originalSchema);
      const parsed: IcebergSchema = JSON.parse(json);

      expect(parsed).toEqual(originalSchema);
    });
  });

  describe('All Primitive Types Including Nanosecond Timestamps', () => {
    const allPrimitiveTypes: IcebergPrimitiveType[] = [
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
      'timestamp_ns',
      'timestamptz_ns',
      'string',
      'uuid',
      'fixed',
      'binary',
    ];

    it('should include timestamp_ns in primitive types', () => {
      expect(allPrimitiveTypes).toContain('timestamp_ns');
    });

    it('should include timestamptz_ns in primitive types', () => {
      expect(allPrimitiveTypes).toContain('timestamptz_ns');
    });

    for (const type of allPrimitiveTypes) {
      it(`should be able to use ${type} as a field type`, () => {
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
      });
    }
  });
});

describe('Iceberg v3 Variant Type', () => {
  describe('Type Recognition', () => {
    it('should recognize variant as a valid IcebergPrimitiveType', () => {
      // 'variant' should be a valid primitive type in Iceberg v3
      const variantType: IcebergPrimitiveType = 'variant';
      expect(variantType).toBe('variant');
    });

    it('should include variant in primitive type union', () => {
      // This tests that TypeScript accepts 'variant' as IcebergPrimitiveType
      const primitiveTypes: IcebergPrimitiveType[] = [
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
        'timestamp_ns',
        'timestamptz_ns',
        'string',
        'uuid',
        'fixed',
        'binary',
        'variant', // v3 type
      ];

      expect(primitiveTypes).toContain('variant');
    });
  });

  describe('Schema Creation', () => {
    it('should create schema with variant field', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'data',
            required: true,
            type: 'variant',
          },
        ],
      };

      expect(schema.fields[0].type).toBe('variant');
      expect(schema.fields[0].name).toBe('data');
    });

    it('should create schema with multiple variant fields', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'json_data', required: true, type: 'variant' },
          { id: 2, name: 'metadata', required: false, type: 'variant' },
          { id: 3, name: 'raw_payload', required: true, type: 'variant' },
        ],
      };

      expect(schema.fields.length).toBe(3);
      expect(schema.fields.every((f) => f.type === 'variant')).toBe(true);
    });

    it('should create schema with variant alongside other types', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          { id: 2, name: 'name', required: true, type: 'string' },
          { id: 3, name: 'semi_structured', required: false, type: 'variant' },
          { id: 4, name: 'created_at', required: true, type: 'timestamptz' },
        ],
      };

      expect(schema.fields[2].type).toBe('variant');
      expect(schema.fields[0].type).toBe('long');
      expect(schema.fields[1].type).toBe('string');
      expect(schema.fields[3].type).toBe('timestamptz');
    });
  });

  describe('Required and Optional', () => {
    it('should allow variant fields to be required', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'required_variant',
            required: true,
            type: 'variant',
          },
        ],
      };

      expect(schema.fields[0].required).toBe(true);
    });

    it('should allow variant fields to be optional', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'optional_variant',
            required: false,
            type: 'variant',
          },
        ],
      };

      expect(schema.fields[0].required).toBe(false);
    });
  });

  describe('Variant in Complex Types', () => {
    it('should allow variant as list element type', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'variant_list',
            required: true,
            type: {
              type: 'list',
              'element-id': 2,
              element: 'variant',
              'element-required': false,
            },
          },
        ],
      };

      const listType = schema.fields[0].type as {
        type: 'list';
        element: IcebergPrimitiveType;
      };
      expect(listType.element).toBe('variant');
    });

    it('should allow variant as map value type', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'variant_map',
            required: true,
            type: {
              type: 'map',
              'key-id': 2,
              'value-id': 3,
              key: 'string',
              value: 'variant',
              'value-required': false,
            },
          },
        ],
      };

      const mapType = schema.fields[0].type as {
        type: 'map';
        value: IcebergPrimitiveType;
      };
      expect(mapType.value).toBe('variant');
    });
  });

  describe('JSON Serialization', () => {
    it('should serialize variant type correctly to JSON', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'variant_field',
            required: true,
            type: 'variant',
          },
        ],
      };

      const json = JSON.stringify(schema);
      const parsed = JSON.parse(json);

      expect(parsed.fields[0].type).toBe('variant');
    });

    it('should preserve variant type through JSON round-trip', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          { id: 2, name: 'payload', required: false, type: 'variant' },
        ],
      };

      const json = JSON.stringify(schema, null, 2);
      const parsed = JSON.parse(json) as IcebergSchema;

      expect(parsed.fields[1].type).toBe('variant');
      expect(parsed.fields[1].name).toBe('payload');
      expect(parsed.fields[1].required).toBe(false);
    });

    it('should serialize schema with variant in nested types', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'nested',
            required: true,
            type: {
              type: 'struct',
              fields: [
                { id: 2, name: 'inner_variant', required: false, type: 'variant' },
              ],
            },
          },
        ],
      };

      const json = JSON.stringify(schema);
      const parsed = JSON.parse(json);

      expect(parsed.fields[0].type.fields[0].type).toBe('variant');
    });
  });

  describe('V3-Only Type Recognition', () => {
    it('should identify variant as a v3-only type', () => {
      // V3-only types that were not in v2
      const v3OnlyTypes: IcebergPrimitiveType[] = ['variant', 'timestamp_ns', 'timestamptz_ns'];

      // V2 types
      const v2Types = [
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

      // Variant should NOT be in v2 types
      expect(v2Types).not.toContain('variant');

      // Variant should be in v3-only types
      expect(v3OnlyTypes).toContain('variant');
    });

    it('should list all v3 primitive types including variant', () => {
      // Complete list of v3 primitive types
      const v3PrimitiveTypes: IcebergPrimitiveType[] = [
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
        'timestamp_ns',
        'timestamptz_ns',
        'string',
        'uuid',
        'fixed',
        'binary',
        'variant',
      ];

      expect(v3PrimitiveTypes.length).toBe(17);
      expect(v3PrimitiveTypes).toContain('variant');
    });
  });

  describe('Documentation', () => {
    it('should allow doc field on variant fields', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'dynamic_data',
            required: false,
            type: 'variant',
            doc: 'Semi-structured JSON-like data stored as variant',
          },
        ],
      };

      expect(schema.fields[0].doc).toBe(
        'Semi-structured JSON-like data stored as variant'
      );
    });
  });
});

/**
 * Tests for Iceberg v3 Geospatial Types
 *
 * @see https://iceberg.apache.org/spec/#primitive-types
 *
 * Iceberg format version 3 introduces geometry and geography types
 * for geospatial data with CRS (Coordinate Reference System) parameters.
 *
 * - geometry(C): Geospatial coordinates on a plane with CRS parameter C
 * - geography(C, A): Geospatial coordinates on a sphere with CRS parameter C
 *   and edge-interpolation algorithm A
 *
 * Default values:
 * - CRS: OGC:CRS84
 * - Algorithm: spherical
 *
 * Valid edge-interpolation algorithms:
 * - spherical (default)
 * - vincenty
 * - thomas
 * - andoyer
 * - karney
 */

import {
  isGeospatialType,
  parseGeometryType,
  parseGeographyType,
  isValidEdgeInterpolationAlgorithm,
  serializeGeometryType,
  serializeGeographyType,
  GEOSPATIAL_DEFAULT_CRS,
  GEOSPATIAL_DEFAULT_ALGORITHM,
  VALID_EDGE_INTERPOLATION_ALGORITHMS,
  type GeometryTypeInfo,
  type GeographyTypeInfo,
} from '../../../src/metadata/types.js';

describe('Iceberg v3 Geospatial Types', () => {
  describe('Type Recognition', () => {
    it('should recognize "geometry" as a geospatial base type', () => {
      expect(isGeospatialType('geometry')).toBe(true);
    });

    it('should recognize "geography" as a geospatial base type', () => {
      expect(isGeospatialType('geography')).toBe(true);
    });

    it('should recognize parameterized geometry type', () => {
      expect(isGeospatialType('geometry(OGC:CRS84)')).toBe(true);
    });

    it('should recognize parameterized geography type', () => {
      expect(isGeospatialType('geography(OGC:CRS84, spherical)')).toBe(true);
    });

    it('should not recognize non-geospatial types', () => {
      expect(isGeospatialType('string')).toBe(false);
      expect(isGeospatialType('int')).toBe(false);
      expect(isGeospatialType('binary')).toBe(false);
    });
  });

  describe('Geometry Type Parsing', () => {
    it('should parse geometry with default CRS when no parameter', () => {
      const result = parseGeometryType('geometry');
      expect(result.crs).toBe(GEOSPATIAL_DEFAULT_CRS);
    });

    it('should parse geometry with explicit default CRS', () => {
      const result = parseGeometryType('geometry(OGC:CRS84)');
      expect(result.crs).toBe('OGC:CRS84');
    });

    it('should parse geometry with custom CRS parameter', () => {
      const result = parseGeometryType('geometry(EPSG:4326)');
      expect(result.crs).toBe('EPSG:4326');
    });

    it('should parse geometry with CRS containing special characters', () => {
      const result = parseGeometryType('geometry(urn:ogc:def:crs:OGC:1.3:CRS84)');
      expect(result.crs).toBe('urn:ogc:def:crs:OGC:1.3:CRS84');
    });

    it('should return null for non-geometry types', () => {
      const result = parseGeometryType('geography(OGC:CRS84, spherical)');
      expect(result).toBeNull();
    });
  });

  describe('Geography Type Parsing', () => {
    it('should parse geography with defaults when no parameters', () => {
      const result = parseGeographyType('geography');
      expect(result.crs).toBe(GEOSPATIAL_DEFAULT_CRS);
      expect(result.algorithm).toBe(GEOSPATIAL_DEFAULT_ALGORITHM);
    });

    it('should parse geography with explicit default CRS and algorithm', () => {
      const result = parseGeographyType('geography(OGC:CRS84, spherical)');
      expect(result.crs).toBe('OGC:CRS84');
      expect(result.algorithm).toBe('spherical');
    });

    it('should parse geography with custom CRS', () => {
      const result = parseGeographyType('geography(EPSG:4326, spherical)');
      expect(result.crs).toBe('EPSG:4326');
      expect(result.algorithm).toBe('spherical');
    });

    it('should parse geography with vincenty algorithm', () => {
      const result = parseGeographyType('geography(OGC:CRS84, vincenty)');
      expect(result.crs).toBe('OGC:CRS84');
      expect(result.algorithm).toBe('vincenty');
    });

    it('should parse geography with thomas algorithm', () => {
      const result = parseGeographyType('geography(OGC:CRS84, thomas)');
      expect(result.crs).toBe('OGC:CRS84');
      expect(result.algorithm).toBe('thomas');
    });

    it('should parse geography with andoyer algorithm', () => {
      const result = parseGeographyType('geography(OGC:CRS84, andoyer)');
      expect(result.crs).toBe('OGC:CRS84');
      expect(result.algorithm).toBe('andoyer');
    });

    it('should parse geography with karney algorithm', () => {
      const result = parseGeographyType('geography(OGC:CRS84, karney)');
      expect(result.crs).toBe('OGC:CRS84');
      expect(result.algorithm).toBe('karney');
    });

    it('should return null for non-geography types', () => {
      const result = parseGeographyType('geometry(OGC:CRS84)');
      expect(result).toBeNull();
    });
  });

  describe('Edge Interpolation Algorithm Validation', () => {
    it('should validate spherical as valid algorithm', () => {
      expect(isValidEdgeInterpolationAlgorithm('spherical')).toBe(true);
    });

    it('should validate vincenty as valid algorithm', () => {
      expect(isValidEdgeInterpolationAlgorithm('vincenty')).toBe(true);
    });

    it('should validate thomas as valid algorithm', () => {
      expect(isValidEdgeInterpolationAlgorithm('thomas')).toBe(true);
    });

    it('should validate andoyer as valid algorithm', () => {
      expect(isValidEdgeInterpolationAlgorithm('andoyer')).toBe(true);
    });

    it('should validate karney as valid algorithm', () => {
      expect(isValidEdgeInterpolationAlgorithm('karney')).toBe(true);
    });

    it('should reject invalid algorithm names', () => {
      expect(isValidEdgeInterpolationAlgorithm('invalid')).toBe(false);
      expect(isValidEdgeInterpolationAlgorithm('SPHERICAL')).toBe(false); // case sensitive
      expect(isValidEdgeInterpolationAlgorithm('')).toBe(false);
    });

    it('should export all valid algorithms as a constant', () => {
      expect(VALID_EDGE_INTERPOLATION_ALGORITHMS).toContain('spherical');
      expect(VALID_EDGE_INTERPOLATION_ALGORITHMS).toContain('vincenty');
      expect(VALID_EDGE_INTERPOLATION_ALGORITHMS).toContain('thomas');
      expect(VALID_EDGE_INTERPOLATION_ALGORITHMS).toContain('andoyer');
      expect(VALID_EDGE_INTERPOLATION_ALGORITHMS).toContain('karney');
      expect(VALID_EDGE_INTERPOLATION_ALGORITHMS.length).toBe(5);
    });
  });

  describe('Geospatial Default Values', () => {
    it('should have OGC:CRS84 as default CRS', () => {
      expect(GEOSPATIAL_DEFAULT_CRS).toBe('OGC:CRS84');
    });

    it('should have spherical as default algorithm', () => {
      expect(GEOSPATIAL_DEFAULT_ALGORITHM).toBe('spherical');
    });
  });

  describe('Geospatial Field Default Values', () => {
    it('should require null default for geometry field', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'location',
            required: false,
            type: 'geometry',
          },
        ],
      };

      // Geospatial fields cannot have non-null default values
      // The default must be null (field is optional or has null as implicit default)
      expect(schema.fields[0].type).toBe('geometry');
      // Note: In Iceberg spec, defaults are handled separately via default-value
      // This test validates the pattern that geospatial fields should be optional
    });

    it('should require null default for geography field', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'earth_location',
            required: false,
            type: 'geography',
          },
        ],
      };

      expect(schema.fields[0].type).toBe('geography');
    });
  });

  describe('JSON Serialization', () => {
    it('should serialize geometry type with default CRS', () => {
      const result = serializeGeometryType({ crs: 'OGC:CRS84' });
      expect(result).toBe('geometry');
    });

    it('should serialize geometry type with custom CRS', () => {
      const result = serializeGeometryType({ crs: 'EPSG:4326' });
      expect(result).toBe('geometry(EPSG:4326)');
    });

    it('should serialize geography type with defaults', () => {
      const result = serializeGeographyType({
        crs: 'OGC:CRS84',
        algorithm: 'spherical',
      });
      expect(result).toBe('geography');
    });

    it('should serialize geography type with custom CRS', () => {
      const result = serializeGeographyType({
        crs: 'EPSG:4326',
        algorithm: 'spherical',
      });
      expect(result).toBe('geography(EPSG:4326, spherical)');
    });

    it('should serialize geography type with custom algorithm', () => {
      const result = serializeGeographyType({
        crs: 'OGC:CRS84',
        algorithm: 'vincenty',
      });
      expect(result).toBe('geography(OGC:CRS84, vincenty)');
    });

    it('should serialize geography type with custom CRS and algorithm', () => {
      const result = serializeGeographyType({
        crs: 'EPSG:4326',
        algorithm: 'karney',
      });
      expect(result).toBe('geography(EPSG:4326, karney)');
    });

    it('should preserve parameters in JSON round-trip for geometry', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'location',
            required: false,
            type: 'geometry(EPSG:4326)',
          },
        ],
      };

      const json = JSON.stringify(schema);
      const parsed = JSON.parse(json);

      expect(parsed.fields[0].type).toBe('geometry(EPSG:4326)');
    });

    it('should preserve parameters in JSON round-trip for geography', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'earth_location',
            required: false,
            type: 'geography(EPSG:4326, karney)',
          },
        ],
      };

      const json = JSON.stringify(schema);
      const parsed = JSON.parse(json);

      expect(parsed.fields[0].type).toBe('geography(EPSG:4326, karney)');
    });
  });

  describe('Type Info Structures', () => {
    it('should return correct GeometryTypeInfo structure', () => {
      const result = parseGeometryType('geometry(EPSG:4326)');
      expect(result).toEqual({
        crs: 'EPSG:4326',
      } satisfies GeometryTypeInfo);
    });

    it('should return correct GeographyTypeInfo structure', () => {
      const result = parseGeographyType('geography(EPSG:4326, vincenty)');
      expect(result).toEqual({
        crs: 'EPSG:4326',
        algorithm: 'vincenty',
      } satisfies GeographyTypeInfo);
    });
  });
});

/**
 * Tests for Iceberg v3 Unknown Type
 *
 * The 'unknown' type is a special primitive type in Iceberg v3 that represents
 * columns with unknown or indeterminate types. This is useful for:
 * - Default/null columns where the type is not yet known
 * - Schema evolution when adding placeholder columns
 *
 * Key constraints:
 * - Unknown type fields MUST be optional (required: false)
 * - Unknown type fields MUST always have null values
 * - Unknown type is NOT stored in data files (no data is written for these columns)
 *
 * @see https://iceberg.apache.org/spec/#primitive-types
 */
import { validateUnknownTypeField, validateSchema } from '../../../src/metadata/schema.js';

describe('Iceberg v3 Types - Unknown Type', () => {
  describe('Type Definition', () => {
    it('should recognize unknown as a valid IcebergPrimitiveType', () => {
      // 'unknown' should be a valid primitive type in the type system
      const primitiveType: IcebergPrimitiveType = 'unknown';
      expect(primitiveType).toBe('unknown');
    });

    it('should allow unknown type in IcebergStructField', () => {
      const field: IcebergStructField = {
        id: 1,
        name: 'placeholder',
        required: false,
        type: 'unknown',
      };

      expect(field.type).toBe('unknown');
      expect(field.required).toBe(false);
    });

    it('should allow unknown type in schema definition', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'known_field',
            required: true,
            type: 'string',
          },
          {
            id: 2,
            name: 'unknown_field',
            required: false,
            type: 'unknown',
          },
        ],
      };

      const unknownField = schema.fields.find((f) => f.name === 'unknown_field');
      expect(unknownField).toBeDefined();
      expect(unknownField?.type).toBe('unknown');
    });
  });

  describe('Validation: Unknown Fields Must Be Optional', () => {
    it('should validate that unknown type fields are optional', () => {
      const validField: IcebergStructField = {
        id: 1,
        name: 'placeholder',
        required: false,
        type: 'unknown',
      };

      const result = validateUnknownTypeField(validField);
      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should reject required unknown type fields', () => {
      const invalidField: IcebergStructField = {
        id: 1,
        name: 'bad_placeholder',
        required: true, // This is NOT allowed for unknown type
        type: 'unknown',
      };

      const result = validateUnknownTypeField(invalidField);
      expect(result.valid).toBe(false);
      expect(result.errors).toContain(
        "Field 'bad_placeholder' with type 'unknown' must be optional (required: false)"
      );
    });

    it('should not validate non-unknown fields as invalid', () => {
      const normalField: IcebergStructField = {
        id: 1,
        name: 'normal',
        required: true,
        type: 'string',
      };

      const result = validateUnknownTypeField(normalField);
      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });
  });

  describe('Schema Validation', () => {
    it('should validate schema with properly configured unknown field', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          { id: 2, name: 'placeholder', required: false, type: 'unknown' },
        ],
      };

      const result = validateSchema(schema);
      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should reject schema with required unknown field', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          { id: 2, name: 'bad_field', required: true, type: 'unknown' }, // Invalid!
        ],
      };

      const result = validateSchema(schema);
      expect(result.valid).toBe(false);
      expect(result.errors.some((e) => e.includes("type 'unknown' must be optional"))).toBe(true);
    });

    it('should validate schema with multiple unknown fields', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'string' },
          { id: 2, name: 'placeholder1', required: false, type: 'unknown' },
          { id: 3, name: 'placeholder2', required: false, type: 'unknown' },
        ],
      };

      const result = validateSchema(schema);
      expect(result.valid).toBe(true);
    });

    it('should reject schema with any required unknown fields among multiple', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'string' },
          { id: 2, name: 'good_unknown', required: false, type: 'unknown' },
          { id: 3, name: 'bad_unknown', required: true, type: 'unknown' }, // Invalid!
        ],
      };

      const result = validateSchema(schema);
      expect(result.valid).toBe(false);
      expect(result.errors.some((e) => e.includes('bad_unknown'))).toBe(true);
    });
  });

  describe('Unknown Type in Nested Structures', () => {
    it('should validate unknown type in nested struct fields', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'nested',
            required: true,
            type: {
              type: 'struct',
              fields: [
                { id: 2, name: 'nested_unknown', required: false, type: 'unknown' },
              ],
            },
          },
        ],
      };

      const result = validateSchema(schema);
      expect(result.valid).toBe(true);
    });

    it('should reject required unknown type in nested struct fields', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'nested',
            required: true,
            type: {
              type: 'struct',
              fields: [
                { id: 2, name: 'nested_bad', required: true, type: 'unknown' }, // Invalid!
              ],
            },
          },
        ],
      };

      const result = validateSchema(schema);
      expect(result.valid).toBe(false);
      expect(result.errors.some((e) => e.includes('nested_bad'))).toBe(true);
    });
  });

  describe('Data File Behavior Documentation', () => {
    /**
     * Note: Unknown type columns are NOT stored in data files.
     * When writing Parquet/Avro files, columns with 'unknown' type should be
     * skipped entirely. When reading, they should always return null.
     *
     * This test documents the expected behavior without actually writing files.
     */
    it('should document that unknown type columns are not written to data files', () => {
      // This is a documentation test - the actual implementation is in the
      // Parquet/Avro writers which should skip unknown type columns.
      //
      // Expected behavior:
      // 1. When writing: Skip columns with type 'unknown'
      // 2. When reading: Return null for all rows in 'unknown' columns
      // 3. Column statistics: Should not include unknown columns
      // 4. Column sizes: Should be 0 for unknown columns

      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'data', required: true, type: 'string' },
          {
            id: 2,
            name: 'future_column',
            required: false,
            type: 'unknown',
            doc: 'Placeholder for future expansion - not stored in data files',
          },
        ],
      };

      // The unknown field should have these characteristics:
      const unknownField = schema.fields.find((f) => f.type === 'unknown');
      expect(unknownField).toBeDefined();
      expect(unknownField?.required).toBe(false);
      // Unknown fields are always null when read
    });
  });

  describe('JSON Serialization', () => {
    it('should serialize unknown type to JSON correctly', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'placeholder', required: false, type: 'unknown' },
        ],
      };

      const json = JSON.stringify(schema);
      const parsed = JSON.parse(json);

      expect(parsed.fields[0].type).toBe('unknown');
    });

    it('should deserialize unknown type from JSON correctly', () => {
      const json = `{
        "schema-id": 0,
        "type": "struct",
        "fields": [
          { "id": 1, "name": "placeholder", "required": false, "type": "unknown" }
        ]
      }`;

      const schema = JSON.parse(json) as IcebergSchema;
      expect(schema.fields[0].type).toBe('unknown');
    });
  });

  describe('Type Promotion', () => {
    it('should not allow type promotion from unknown to other types', () => {
      // Unknown type cannot be promoted to any other type
      // because we don't know the actual data type
      // This would need to be tested with the type promotion utilities
      const unknownType: IcebergPrimitiveType = 'unknown';
      expect(unknownType).toBe('unknown');
      // Type promotions are tested in schema-evolution.test.ts
      // Unknown should not appear in TYPE_PROMOTIONS
    });
  });
});
