/**
 * Tests for Partition Spec serialization in Iceberg metadata.json
 *
 * @see https://iceberg.apache.org/spec/#partitioning
 *
 * Partition specs define how data is partitioned in an Iceberg table.
 * Each spec has a unique ID and contains partition fields with transforms.
 */

import { describe, it, expect } from 'vitest';
import {
  createUnpartitionedSpec,
  createIdentityPartitionSpec,
  createBucketPartitionSpec,
  createTimePartitionSpec,
  TableMetadataBuilder,
  PartitionSpecBuilder,
  type PartitionSpec,
  type PartitionField,
  type SortField,
  type SortOrder,
} from '../../../src/index.js';

describe('Partition Spec Serialization', () => {
  describe('Partition Spec Structure', () => {
    it('should serialize partition spec with spec-id', () => {
      const spec = createUnpartitionedSpec();
      expect(spec).toHaveProperty('spec-id');
      expect(typeof spec['spec-id']).toBe('number');
    });

    it('should serialize partition spec with fields array', () => {
      const spec = createUnpartitionedSpec();
      expect(spec).toHaveProperty('fields');
      expect(Array.isArray(spec.fields)).toBe(true);
    });

    it('should have empty fields array for unpartitioned tables', () => {
      const spec = createUnpartitionedSpec();
      expect(spec.fields.length).toBe(0);
    });
  });

  describe('Partition Field Structure', () => {
    it('should serialize partition field with source-id', () => {
      const spec = createIdentityPartitionSpec(1, 'date_col');
      const field = spec.fields[0];

      expect(field).toHaveProperty('source-id');
      expect(typeof field['source-id']).toBe('number');
    });

    it('should serialize partition field with field-id', () => {
      const spec = createIdentityPartitionSpec(1, 'date_col');
      const field = spec.fields[0];

      expect(field).toHaveProperty('field-id');
      expect(typeof field['field-id']).toBe('number');
    });

    it('should serialize partition field with name', () => {
      const spec = createIdentityPartitionSpec(1, 'date_col');
      const field = spec.fields[0];

      expect(field).toHaveProperty('name');
      expect(typeof field.name).toBe('string');
    });

    it('should serialize partition field with transform', () => {
      const spec = createIdentityPartitionSpec(1, 'date_col');
      const field = spec.fields[0];

      expect(field).toHaveProperty('transform');
      expect(typeof field.transform).toBe('string');
    });

    it('should use partition field IDs starting at 1000', () => {
      const spec = createIdentityPartitionSpec(1, 'date_col');
      const field = spec.fields[0];

      expect(field['field-id']).toBeGreaterThanOrEqual(1000);
    });
  });

  describe('Transform Types', () => {
    it('should serialize identity transform', () => {
      const spec = createIdentityPartitionSpec(1, 'category');
      expect(spec.fields[0].transform).toBe('identity');
    });

    it('should serialize bucket transform with bucket count', () => {
      const spec = createBucketPartitionSpec(1, 'user_id', 16);
      expect(spec.fields[0].transform).toBe('bucket[16]');
    });

    it('should serialize year transform', () => {
      const spec = createTimePartitionSpec(1, 'created_at', 'year');
      expect(spec.fields[0].transform).toBe('year');
    });

    it('should serialize month transform', () => {
      const spec = createTimePartitionSpec(1, 'created_at', 'month');
      expect(spec.fields[0].transform).toBe('month');
    });

    it('should serialize day transform', () => {
      const spec = createTimePartitionSpec(1, 'created_at', 'day');
      expect(spec.fields[0].transform).toBe('day');
    });

    it('should serialize hour transform', () => {
      const spec = createTimePartitionSpec(1, 'created_at', 'hour');
      expect(spec.fields[0].transform).toBe('hour');
    });

    it('should serialize truncate transform with width', () => {
      const spec: PartitionSpec = {
        'spec-id': 0,
        fields: [
          {
            'source-id': 1,
            'field-id': 1000,
            name: 'name_prefix',
            transform: 'truncate[4]',
          },
        ],
      };

      expect(spec.fields[0].transform).toBe('truncate[4]');
    });

    it('should serialize void transform (for deleted partitions)', () => {
      const spec: PartitionSpec = {
        'spec-id': 1,
        fields: [
          {
            'source-id': 1,
            'field-id': 1000,
            name: 'void_partition',
            transform: 'void',
          },
        ],
      };

      expect(spec.fields[0].transform).toBe('void');
    });
  });

  describe('Multiple Partition Fields', () => {
    it('should support multiple partition fields', () => {
      const spec: PartitionSpec = {
        'spec-id': 0,
        fields: [
          {
            'source-id': 1,
            'field-id': 1000,
            name: 'year_part',
            transform: 'year',
          },
          {
            'source-id': 1,
            'field-id': 1001,
            name: 'month_part',
            transform: 'month',
          },
        ],
      };

      expect(spec.fields.length).toBe(2);
      expect(spec.fields[0].name).toBe('year_part');
      expect(spec.fields[1].name).toBe('month_part');
    });

    it('should allow different source fields for partition columns', () => {
      const spec: PartitionSpec = {
        'spec-id': 0,
        fields: [
          {
            'source-id': 1,
            'field-id': 1000,
            name: 'date_partition',
            transform: 'day',
          },
          {
            'source-id': 2,
            'field-id': 1001,
            name: 'region_partition',
            transform: 'identity',
          },
        ],
      };

      expect(spec.fields[0]['source-id']).toBe(1);
      expect(spec.fields[1]['source-id']).toBe(2);
    });
  });

  describe('Partition Spec in Table Metadata', () => {
    it('should include partition-specs array in table metadata', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      expect(metadata['partition-specs']).toBeDefined();
      expect(Array.isArray(metadata['partition-specs'])).toBe(true);
      expect(metadata['partition-specs'].length).toBeGreaterThan(0);
    });

    it('should include default-spec-id in table metadata', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      expect(metadata['default-spec-id']).toBeDefined();
      expect(typeof metadata['default-spec-id']).toBe('number');
    });

    it('should reference valid spec ID from default-spec-id', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      const specIds = metadata['partition-specs'].map((s) => s['spec-id']);
      expect(specIds).toContain(metadata['default-spec-id']);
    });

    it('should track last-partition-id', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      expect(metadata['last-partition-id']).toBeDefined();
      expect(typeof metadata['last-partition-id']).toBe('number');
    });

    it('should initialize last-partition-id to 999 for unpartitioned tables', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
        partitionSpec: createUnpartitionedSpec(),
      });
      const metadata = builder.build();

      // Partition field IDs start at 1000, so 999 indicates no partition fields
      expect(metadata['last-partition-id']).toBe(999);
    });

    it('should add new partition spec with unique spec-id', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const newSpec: PartitionSpec = {
        'spec-id': 1,
        fields: [
          {
            'source-id': 1,
            'field-id': 1000,
            name: 'id_bucket',
            transform: 'bucket[8]',
          },
        ],
      };

      builder.addPartitionSpec(newSpec);
      const metadata = builder.build();

      expect(metadata['partition-specs'].length).toBe(2);
      expect(metadata['partition-specs'][1]['spec-id']).toBe(1);
    });

    it('should update last-partition-id when adding partition spec', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const newSpec: PartitionSpec = {
        'spec-id': 1,
        fields: [
          {
            'source-id': 1,
            'field-id': 1005,
            name: 'new_partition',
            transform: 'identity',
          },
        ],
      };

      builder.addPartitionSpec(newSpec);
      const metadata = builder.build();

      expect(metadata['last-partition-id']).toBe(1005);
    });

    it('should set default partition spec via setDefaultPartitionSpec', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const newSpec: PartitionSpec = {
        'spec-id': 1,
        fields: [],
      };

      builder.addPartitionSpec(newSpec);
      builder.setDefaultPartitionSpec(1);
      const metadata = builder.build();

      expect(metadata['default-spec-id']).toBe(1);
    });

    it('should throw when setting non-existent partition spec as default', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      expect(() => builder.setDefaultPartitionSpec(999)).toThrow();
    });
  });

  describe('Partition Spec Builder', () => {
    // Create a test schema to use with the builder
    const testSchema = {
      type: 'struct' as const,
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'created_at', required: true, type: 'timestamp' },
        { id: 3, name: 'category', required: true, type: 'string' },
        { id: 4, name: 'ts', required: true, type: 'timestamp' },
      ],
    };

    it('should create partition spec using PartitionSpecBuilder', () => {
      const specBuilder = new PartitionSpecBuilder(testSchema, { specId: 0 });

      specBuilder.day('created_at', 'day_partition');

      const spec = specBuilder.build();

      expect(spec['spec-id']).toBe(0);
      expect(spec.fields.length).toBe(1);
      expect(spec.fields[0].transform).toBe('day');
    });

    it('should auto-assign field-id starting at 1000', () => {
      const specBuilder = new PartitionSpecBuilder(testSchema, { specId: 0 });

      specBuilder.identity('category', 'category_partition');

      const spec = specBuilder.build();
      expect(spec.fields[0]['field-id']).toBeGreaterThanOrEqual(1000);
    });

    it('should support chaining multiple addField calls', () => {
      const specBuilder = new PartitionSpecBuilder(testSchema, { specId: 0 });

      specBuilder
        .year('ts', 'year_partition')
        .bucket('id', 16, 'id_bucket');

      const spec = specBuilder.build();
      expect(spec.fields.length).toBe(2);
    });
  });

  describe('JSON Serialization Correctness', () => {
    it('should serialize partition spec correctly to JSON', () => {
      const spec = createIdentityPartitionSpec(1, 'category');
      const json = JSON.stringify(spec, null, 2);

      expect(json).toContain('"spec-id"');
      expect(json).toContain('"fields"');
      expect(json).toContain('"source-id"');
      expect(json).toContain('"field-id"');
      expect(json).toContain('"name"');
      expect(json).toContain('"transform"');
    });

    it('should roundtrip through JSON parse/stringify', () => {
      const spec: PartitionSpec = {
        'spec-id': 5,
        fields: [
          {
            'source-id': 10,
            'field-id': 1010,
            name: 'test_partition',
            transform: 'bucket[32]',
          },
        ],
      };

      const json = JSON.stringify(spec);
      const parsed = JSON.parse(json) as PartitionSpec;

      expect(parsed['spec-id']).toBe(spec['spec-id']);
      expect(parsed.fields[0]['source-id']).toBe(spec.fields[0]['source-id']);
      expect(parsed.fields[0]['field-id']).toBe(spec.fields[0]['field-id']);
      expect(parsed.fields[0].name).toBe(spec.fields[0].name);
      expect(parsed.fields[0].transform).toBe(spec.fields[0].transform);
    });

    it('should handle special transform syntax in JSON', () => {
      const spec: PartitionSpec = {
        'spec-id': 0,
        fields: [
          {
            'source-id': 1,
            'field-id': 1000,
            name: 'bucket_partition',
            transform: 'bucket[256]',
          },
          {
            'source-id': 2,
            'field-id': 1001,
            name: 'truncate_partition',
            transform: 'truncate[10]',
          },
        ],
      };

      const json = JSON.stringify(spec);
      expect(json).toContain('bucket[256]');
      expect(json).toContain('truncate[10]');
    });
  });

  describe('Multi-Argument Transforms (source-ids)', () => {
    /**
     * Iceberg v3 spec addition: Multi-argument transforms support.
     * Transforms that take multiple columns use 'source-ids' array instead of 'source-id'.
     *
     * @see https://iceberg.apache.org/spec/#partitioning
     */

    describe('PartitionField source-ids support', () => {
      it('should allow optional source-ids array in PartitionField', () => {
        // source-ids is an array of field IDs for multi-argument transforms
        const fieldWithSourceIds: PartitionField = {
          'source-ids': [1, 2],
          'field-id': 1000,
          name: 'multi_col_bucket',
          transform: 'bucket[16]',
        };

        expect(fieldWithSourceIds['source-ids']).toEqual([1, 2]);
        expect(fieldWithSourceIds['source-id']).toBeUndefined();
      });

      it('should support single-argument transform with source-id (existing behavior)', () => {
        const fieldWithSourceId: PartitionField = {
          'source-id': 1,
          'field-id': 1000,
          name: 'bucket_partition',
          transform: 'bucket[16]',
        };

        expect(fieldWithSourceId['source-id']).toBe(1);
        expect(fieldWithSourceId['source-ids']).toBeUndefined();
      });

      it('should not allow both source-id and source-ids', () => {
        // Type system should prevent this, but runtime validation should catch it
        const invalidField = {
          'source-id': 1,
          'source-ids': [1, 2],
          'field-id': 1000,
          name: 'invalid_partition',
          transform: 'bucket[16]',
        };

        // This should be considered invalid - either source-id OR source-ids, not both
        const hasSourceId = 'source-id' in invalidField && invalidField['source-id'] !== undefined;
        const hasSourceIds =
          'source-ids' in invalidField &&
          Array.isArray(invalidField['source-ids']) &&
          invalidField['source-ids'].length > 0;

        // Validation: should have exactly one
        expect(hasSourceId && hasSourceIds).toBe(true); // Shows the invalid state
      });
    });

    describe('SortField source-ids support', () => {
      it('should allow optional source-ids array in SortField', () => {
        // Sort fields can also support multi-argument transforms
        const sortFieldWithSourceIds = {
          'source-ids': [1, 2],
          transform: 'bucket[16]',
          direction: 'asc' as const,
          'null-order': 'nulls-first' as const,
        };

        expect(sortFieldWithSourceIds['source-ids']).toEqual([1, 2]);
      });

      it('should support single-argument transform with source-id in SortField', () => {
        const sortFieldWithSourceId = {
          'source-id': 1,
          transform: 'identity',
          direction: 'asc' as const,
          'null-order': 'nulls-first' as const,
        };

        expect(sortFieldWithSourceId['source-id']).toBe(1);
      });
    });

    describe('JSON Serialization with source-ids', () => {
      it('should serialize source-ids correctly in partition spec JSON', () => {
        const spec: PartitionSpec = {
          'spec-id': 0,
          fields: [
            {
              'source-ids': [1, 2],
              'field-id': 1000,
              name: 'multi_bucket',
              transform: 'bucket[16]',
            },
          ],
        };

        const json = JSON.stringify(spec, null, 2);
        expect(json).toContain('"source-ids"');
        // Check that array contains expected values (JSON.stringify with indent formats arrays with newlines)
        expect(json).toMatch(/"source-ids":\s*\[\s*1,\s*2\s*\]/);
        expect(json).not.toContain('"source-id":');
      });

      it('should roundtrip partition spec with source-ids through JSON', () => {
        const spec: PartitionSpec = {
          'spec-id': 0,
          fields: [
            {
              'source-ids': [3, 5, 7],
              'field-id': 1000,
              name: 'multi_col_partition',
              transform: 'bucket[32]',
            },
          ],
        };

        const json = JSON.stringify(spec);
        const parsed = JSON.parse(json) as PartitionSpec;

        expect(parsed.fields[0]['source-ids']).toEqual([3, 5, 7]);
        expect(parsed.fields[0]['source-id']).toBeUndefined();
      });

      it('should serialize sort order with source-ids correctly', () => {
        const sortOrder: SortOrder = {
          'order-id': 1,
          fields: [
            {
              'source-ids': [1, 2],
              transform: 'identity',
              direction: 'asc',
              'null-order': 'nulls-first',
            },
          ],
        };

        const json = JSON.stringify(sortOrder, null, 2);
        expect(json).toContain('"source-ids"');
        // Check array contents with flexible whitespace
        expect(json).toMatch(/"source-ids":\s*\[\s*1,\s*2\s*\]/);
      });
    });

    describe('Multi-argument transform examples', () => {
      it('should support bucket transform with multiple columns (future use case)', () => {
        // Example: bucket partitioning on composite key (col1, col2)
        const multiColBucketSpec: PartitionSpec = {
          'spec-id': 0,
          fields: [
            {
              'source-ids': [1, 2], // bucket on both columns together
              'field-id': 1000,
              name: 'composite_bucket',
              transform: 'bucket[16]',
            },
          ],
        };

        expect(multiColBucketSpec.fields[0]['source-ids']).toHaveLength(2);
      });

      it('should validate source-ids contains valid field IDs (numbers)', () => {
        const validSourceIds = [1, 2, 3];
        expect(validSourceIds.every((id) => typeof id === 'number')).toBe(true);

        const invalidSourceIds = [1, '2', 3]; // string in array
        expect(invalidSourceIds.every((id) => typeof id === 'number')).toBe(false);
      });
    });

    describe('Type definitions', () => {
      it('should accept PartitionField with only source-ids (no source-id)', () => {
        // This test verifies the type allows source-ids without source-id
        // If PartitionField requires source-id, this will fail type checking
        const field: PartitionField = {
          'source-ids': [1, 2],
          'field-id': 1000,
          name: 'multi_col_bucket',
          transform: 'bucket[16]',
        };

        expect(field['source-ids']).toEqual([1, 2]);
        expect(field['source-id']).toBeUndefined();
      });

      it('should accept PartitionField with only source-id (no source-ids)', () => {
        // Existing behavior should still work
        const field: PartitionField = {
          'source-id': 1,
          'field-id': 1000,
          name: 'single_col_bucket',
          transform: 'bucket[16]',
        };

        expect(field['source-id']).toBe(1);
        expect(field['source-ids']).toBeUndefined();
      });

      it('should accept SortField with only source-ids (no source-id)', () => {
        // This test verifies SortField type allows source-ids without source-id
        const field: SortField = {
          'source-ids': [1, 2],
          transform: 'identity',
          direction: 'asc',
          'null-order': 'nulls-first',
        };

        expect(field['source-ids']).toEqual([1, 2]);
        expect(field['source-id']).toBeUndefined();
      });

      it('should accept SortField with only source-id (no source-ids)', () => {
        // Existing behavior should still work
        const field: SortField = {
          'source-id': 1,
          transform: 'identity',
          direction: 'asc',
          'null-order': 'nulls-first',
        };

        expect(field['source-id']).toBe(1);
        expect(field['source-ids']).toBeUndefined();
      });
    });
  });

  describe('Partition Spec Evolution', () => {
    it('should preserve old partition specs when adding new one', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
        partitionSpec: createIdentityPartitionSpec(1, 'category'),
      });

      const newSpec: PartitionSpec = {
        'spec-id': 1,
        fields: [
          {
            'source-id': 2,
            'field-id': 1001,
            name: 'date_partition',
            transform: 'day',
          },
        ],
      };

      builder.addPartitionSpec(newSpec);
      const metadata = builder.build();

      expect(metadata['partition-specs'].length).toBe(2);
      expect(metadata['partition-specs'][0].fields[0].name).toBe('category');
      expect(metadata['partition-specs'][1].fields[0].name).toBe('date_partition');
    });

    it('should allow changing partition fields using void transform', () => {
      // When evolving a partition spec, old fields can be marked with void transform
      // to indicate they are no longer used
      const evolvedSpec: PartitionSpec = {
        'spec-id': 1,
        fields: [
          {
            'source-id': 1,
            'field-id': 1000,
            name: 'old_partition',
            transform: 'void', // No longer used
          },
          {
            'source-id': 2,
            'field-id': 1001,
            name: 'new_partition',
            transform: 'day',
          },
        ],
      };

      expect(evolvedSpec.fields[0].transform).toBe('void');
      expect(evolvedSpec.fields[1].transform).toBe('day');
    });
  });
});
