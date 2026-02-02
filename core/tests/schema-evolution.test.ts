import { describe, it, expect } from 'vitest';
import {
  // Type compatibility
  isTypePromotionAllowed,
  areTypesCompatible,
  // Error handling
  SchemaEvolutionError,
  // Builder
  SchemaEvolutionBuilder,
  // Schema comparison
  compareSchemas,
  // Compatibility checking
  isBackwardCompatible,
  isForwardCompatible,
  isFullyCompatible,
  // Schema history
  getSchemaHistory,
  getSchemaForSnapshot,
  getSchemaChangesBetween,
  // Field utilities
  findFieldByName,
  findFieldById,
  getAllFieldIds,
  // Helper functions
  evolveSchema,
  applySchemaEvolution,
  evolveNestedStruct,
  // Field ID management
  FieldIdManager,
  // Types
  type IcebergSchema,
  type TableMetadata,
  type IcebergStructField,
  // Helpers
  createDefaultSchema,
  TableMetadataBuilder,
} from '../src/index.js';

// =============================================================================
// Test Helpers
// =============================================================================

function createTestSchema(): IcebergSchema {
  return {
    'schema-id': 0,
    type: 'struct',
    fields: [
      { id: 1, name: 'id', required: true, type: 'long' },
      { id: 2, name: 'name', required: true, type: 'string' },
      { id: 3, name: 'email', required: false, type: 'string' },
      { id: 4, name: 'age', required: false, type: 'int' },
    ],
  };
}

function createTestMetadata(): TableMetadata {
  const builder = new TableMetadataBuilder({
    location: 's3://bucket/warehouse/db/table',
    schema: createTestSchema(),
  });
  return builder.build();
}

// =============================================================================
// Type Promotion Tests
// =============================================================================

describe('Type Promotion', () => {
  describe('isTypePromotionAllowed', () => {
    it('should allow int to long promotion', () => {
      expect(isTypePromotionAllowed('int', 'long')).toBe(true);
    });

    it('should allow float to double promotion', () => {
      expect(isTypePromotionAllowed('float', 'double')).toBe(true);
    });

    it('should allow same type (identity)', () => {
      expect(isTypePromotionAllowed('string', 'string')).toBe(true);
      expect(isTypePromotionAllowed('long', 'long')).toBe(true);
      expect(isTypePromotionAllowed('boolean', 'boolean')).toBe(true);
    });

    it('should not allow long to int (narrowing)', () => {
      expect(isTypePromotionAllowed('long', 'int')).toBe(false);
    });

    it('should not allow double to float (narrowing)', () => {
      expect(isTypePromotionAllowed('double', 'float')).toBe(false);
    });

    it('should not allow incompatible type changes', () => {
      expect(isTypePromotionAllowed('string', 'int')).toBe(false);
      expect(isTypePromotionAllowed('boolean', 'string')).toBe(false);
      expect(isTypePromotionAllowed('timestamp', 'long')).toBe(false);
    });

    it('should allow fixed to binary promotion', () => {
      expect(isTypePromotionAllowed('fixed', 'binary')).toBe(true);
    });

    it('should allow decimal to decimal (widening)', () => {
      expect(isTypePromotionAllowed('decimal', 'decimal')).toBe(true);
    });
  });

  describe('areTypesCompatible', () => {
    it('should return compatible for primitive promotions', () => {
      const result = areTypesCompatible('int', 'long');
      expect(result.compatible).toBe(true);
    });

    it('should return incompatible with reason for invalid promotions', () => {
      const result = areTypesCompatible('string', 'int');
      expect(result.compatible).toBe(false);
      expect(result.reason).toContain('Cannot promote');
    });

    it('should handle struct type compatibility', () => {
      const oldType = { type: 'struct' as const, fields: [] };
      const newType = { type: 'struct' as const, fields: [] };
      const result = areTypesCompatible(oldType, newType);
      expect(result.compatible).toBe(true);
    });

    it('should handle list type compatibility', () => {
      const oldType = {
        type: 'list' as const,
        'element-id': 1,
        element: 'int' as const,
        'element-required': true,
      };
      const newType = {
        type: 'list' as const,
        'element-id': 1,
        element: 'long' as const,
        'element-required': true,
      };
      const result = areTypesCompatible(oldType, newType);
      expect(result.compatible).toBe(true);
    });

    it('should handle map type compatibility', () => {
      const oldType = {
        type: 'map' as const,
        'key-id': 1,
        'value-id': 2,
        key: 'string' as const,
        value: 'int' as const,
        'value-required': true,
      };
      const newType = {
        type: 'map' as const,
        'key-id': 1,
        'value-id': 2,
        key: 'string' as const,
        value: 'long' as const,
        'value-required': true,
      };
      const result = areTypesCompatible(oldType, newType);
      expect(result.compatible).toBe(true);
    });

    it('should reject incompatible type categories', () => {
      const structType = { type: 'struct' as const, fields: [] };
      const result = areTypesCompatible('string', structType);
      expect(result.compatible).toBe(false);
      expect(result.reason).toContain('different type categories');
    });
  });
});

// =============================================================================
// Schema Evolution Builder Tests
// =============================================================================

describe('SchemaEvolutionBuilder', () => {
  describe('addColumn', () => {
    it('should add a new optional column', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.addColumn('phone', 'string');
      const newSchema = builder.build();

      expect(newSchema.fields).toHaveLength(5);
      const phoneField = newSchema.fields.find((f) => f.name === 'phone');
      expect(phoneField).toBeDefined();
      expect(phoneField?.required).toBe(false);
      expect(phoneField?.type).toBe('string');
      expect(phoneField?.id).toBe(5); // Next ID after 4
    });

    it('should add a new required column', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.addColumn('created_at', 'timestamp', { required: true });
      const newSchema = builder.build();

      const field = newSchema.fields.find((f) => f.name === 'created_at');
      expect(field?.required).toBe(true);
    });

    it('should add column with documentation', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.addColumn('score', 'double', { doc: 'User score from 0 to 100' });
      const newSchema = builder.build();

      const field = newSchema.fields.find((f) => f.name === 'score');
      expect(field?.doc).toBe('User score from 0 to 100');
    });

    it('should add column at specific position', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.addColumn('middle_name', 'string', {
        position: { type: 'after', column: 'name' },
      });
      const newSchema = builder.build();

      const nameIndex = newSchema.fields.findIndex((f) => f.name === 'name');
      const middleNameIndex = newSchema.fields.findIndex((f) => f.name === 'middle_name');
      expect(middleNameIndex).toBe(nameIndex + 1);
    });

    it('should add column at first position', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.addColumn('record_id', 'uuid', { position: { type: 'first' } });
      const newSchema = builder.build();

      expect(newSchema.fields[0].name).toBe('record_id');
    });

    it('should throw error when adding duplicate column', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.addColumn('name', 'string');

      expect(() => builder.build()).toThrow(SchemaEvolutionError);
      expect(() => builder.build()).toThrow('already exists');
    });

    it('should assign monotonically increasing field IDs', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.addColumn('field_a', 'string');
      builder.addColumn('field_b', 'string');
      builder.addColumn('field_c', 'string');
      const newSchema = builder.build();

      const fieldA = newSchema.fields.find((f) => f.name === 'field_a');
      const fieldB = newSchema.fields.find((f) => f.name === 'field_b');
      const fieldC = newSchema.fields.find((f) => f.name === 'field_c');

      expect(fieldA?.id).toBe(5);
      expect(fieldB?.id).toBe(6);
      expect(fieldC?.id).toBe(7);
    });
  });

  describe('dropColumn', () => {
    it('should drop an existing column', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.dropColumn('email');
      const newSchema = builder.build();

      expect(newSchema.fields).toHaveLength(3);
      expect(newSchema.fields.find((f) => f.name === 'email')).toBeUndefined();
    });

    it('should throw error when dropping non-existent column', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.dropColumn('nonexistent');

      expect(() => builder.build()).toThrow(SchemaEvolutionError);
      expect(() => builder.build()).toThrow('not found');
    });

    it('should throw error when dropping identifier column', () => {
      const schema: IcebergSchema & { 'identifier-field-ids'?: number[] } = {
        ...createTestSchema(),
        'identifier-field-ids': [1],
      };
      const builder = new SchemaEvolutionBuilder(schema);

      builder.dropColumn('id');

      expect(() => builder.build()).toThrow(SchemaEvolutionError);
      expect(() => builder.build()).toThrow('identifier');
    });
  });

  describe('renameColumn', () => {
    it('should rename a column', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.renameColumn('name', 'full_name');
      const newSchema = builder.build();

      expect(newSchema.fields.find((f) => f.name === 'name')).toBeUndefined();
      const renamedField = newSchema.fields.find((f) => f.name === 'full_name');
      expect(renamedField).toBeDefined();
      expect(renamedField?.id).toBe(2); // ID should be preserved
    });

    it('should throw error when renaming to existing name', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.renameColumn('name', 'email');

      expect(() => builder.build()).toThrow(SchemaEvolutionError);
      expect(() => builder.build()).toThrow('already exists');
    });

    it('should throw error when renaming non-existent column', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.renameColumn('nonexistent', 'new_name');

      expect(() => builder.build()).toThrow(SchemaEvolutionError);
    });
  });

  describe('updateColumnType', () => {
    it('should update column type with valid promotion', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.updateColumnType('age', 'long');
      const newSchema = builder.build();

      const ageField = newSchema.fields.find((f) => f.name === 'age');
      expect(ageField?.type).toBe('long');
    });

    it('should throw error for invalid type promotion', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.updateColumnType('name', 'int');

      expect(() => builder.build()).toThrow(SchemaEvolutionError);
      expect(() => builder.build()).toThrow('Cannot change type');
    });
  });

  describe('makeColumnOptional', () => {
    it('should make a required column optional', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.makeColumnOptional('name');
      const newSchema = builder.build();

      const nameField = newSchema.fields.find((f) => f.name === 'name');
      expect(nameField?.required).toBe(false);
    });
  });

  describe('makeColumnRequired', () => {
    it('should make an optional column required', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.makeColumnRequired('email');
      const newSchema = builder.build();

      const emailField = newSchema.fields.find((f) => f.name === 'email');
      expect(emailField?.required).toBe(true);
    });
  });

  describe('updateColumnDoc', () => {
    it('should update column documentation', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.updateColumnDoc('name', 'Full name of the user');
      const newSchema = builder.build();

      const nameField = newSchema.fields.find((f) => f.name === 'name');
      expect(nameField?.doc).toBe('Full name of the user');
    });

    it('should remove documentation when set to undefined', () => {
      const schema: IcebergSchema = {
        ...createTestSchema(),
        fields: createTestSchema().fields.map((f) =>
          f.name === 'name' ? { ...f, doc: 'Old doc' } : f
        ),
      };
      const builder = new SchemaEvolutionBuilder(schema);

      builder.updateColumnDoc('name', undefined);
      const newSchema = builder.build();

      const nameField = newSchema.fields.find((f) => f.name === 'name');
      expect(nameField?.doc).toBeUndefined();
    });
  });

  describe('moveColumn', () => {
    it('should move column to first position', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.moveColumn('email', { type: 'first' });
      const newSchema = builder.build();

      expect(newSchema.fields[0].name).toBe('email');
    });

    it('should move column to last position', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.moveColumn('id', { type: 'last' });
      const newSchema = builder.build();

      expect(newSchema.fields[newSchema.fields.length - 1].name).toBe('id');
    });

    it('should move column after another column', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.moveColumn('age', { type: 'after', column: 'id' });
      const newSchema = builder.build();

      const idIndex = newSchema.fields.findIndex((f) => f.name === 'id');
      const ageIndex = newSchema.fields.findIndex((f) => f.name === 'age');
      expect(ageIndex).toBe(idIndex + 1);
    });

    it('should move column before another column', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.moveColumn('age', { type: 'before', column: 'name' });
      const newSchema = builder.build();

      const nameIndex = newSchema.fields.findIndex((f) => f.name === 'name');
      const ageIndex = newSchema.fields.findIndex((f) => f.name === 'age');
      expect(ageIndex).toBe(nameIndex - 1);
    });

    it('should throw error for invalid position reference', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.moveColumn('age', { type: 'after', column: 'nonexistent' });

      expect(() => builder.build()).toThrow(SchemaEvolutionError);
      expect(() => builder.build()).toThrow('not found');
    });
  });

  describe('validate', () => {
    it('should return valid for correct operations', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.addColumn('phone', 'string');
      builder.renameColumn('name', 'full_name');

      const result = builder.validate();
      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should return errors for invalid operations', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.addColumn('name', 'string'); // Duplicate
      builder.dropColumn('nonexistent'); // Not found

      const result = builder.validate();
      expect(result.valid).toBe(false);
      expect(result.errors.length).toBeGreaterThan(0);
    });
  });

  describe('build', () => {
    it('should increment schema ID', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.addColumn('phone', 'string');
      const newSchema = builder.build();

      expect(newSchema['schema-id']).toBe(1);
    });

    it('should support chained operations', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      const newSchema = builder
        .addColumn('phone', 'string')
        .renameColumn('name', 'full_name')
        .dropColumn('age')
        .updateColumnType('id', 'long') // id is already long, so this is valid
        .build();

      expect(newSchema.fields.find((f) => f.name === 'phone')).toBeDefined();
      expect(newSchema.fields.find((f) => f.name === 'full_name')).toBeDefined();
      expect(newSchema.fields.find((f) => f.name === 'name')).toBeUndefined();
      expect(newSchema.fields.find((f) => f.name === 'age')).toBeUndefined();
    });
  });

  describe('buildWithMetadata', () => {
    it('should update metadata with new schema', () => {
      const metadata = createTestMetadata();
      const builder = new SchemaEvolutionBuilder(metadata.schemas[0], metadata);

      builder.addColumn('phone', 'string');
      const result = builder.buildWithMetadata();

      expect(result.metadata.schemas).toHaveLength(2);
      expect(result.metadata['current-schema-id']).toBe(1);
      expect(result.metadata['last-column-id']).toBe(5);
      expect(result.nextColumnId).toBe(5);
    });

    it('should throw error when no metadata provided', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.addColumn('phone', 'string');

      expect(() => builder.buildWithMetadata()).toThrow('No metadata provided');
    });
  });

  describe('clear', () => {
    it('should clear all pending operations', () => {
      const schema = createTestSchema();
      const builder = new SchemaEvolutionBuilder(schema);

      builder.addColumn('phone', 'string');
      builder.renameColumn('name', 'full_name');
      builder.clear();

      expect(builder.getOperations()).toHaveLength(0);

      const newSchema = builder.build();
      expect(newSchema.fields).toHaveLength(4); // Original count
      expect(newSchema['schema-id']).toBe(1); // Still increments
    });
  });
});

// =============================================================================
// Schema Comparison Tests
// =============================================================================

describe('compareSchemas', () => {
  it('should detect added fields', () => {
    const oldSchema = createTestSchema();
    const newSchema: IcebergSchema = {
      ...oldSchema,
      'schema-id': 1,
      fields: [
        ...oldSchema.fields,
        { id: 5, name: 'phone', required: false, type: 'string' },
      ],
    };

    const changes = compareSchemas(oldSchema, newSchema);

    expect(changes.some((c) => c.type === 'added' && c.fieldName === 'phone')).toBe(true);
  });

  it('should detect removed fields', () => {
    const oldSchema = createTestSchema();
    const newSchema: IcebergSchema = {
      ...oldSchema,
      'schema-id': 1,
      fields: oldSchema.fields.filter((f) => f.name !== 'email'),
    };

    const changes = compareSchemas(oldSchema, newSchema);

    expect(changes.some((c) => c.type === 'removed' && c.fieldName === 'email')).toBe(true);
  });

  it('should detect renamed fields', () => {
    const oldSchema = createTestSchema();
    const newSchema: IcebergSchema = {
      ...oldSchema,
      'schema-id': 1,
      fields: oldSchema.fields.map((f) =>
        f.id === 2 ? { ...f, name: 'full_name' } : f
      ),
    };

    const changes = compareSchemas(oldSchema, newSchema);

    const renameChange = changes.find((c) => c.type === 'renamed');
    expect(renameChange).toBeDefined();
    expect(renameChange?.oldValue).toBe('name');
    expect(renameChange?.newValue).toBe('full_name');
  });

  it('should detect type changes', () => {
    const oldSchema = createTestSchema();
    const newSchema: IcebergSchema = {
      ...oldSchema,
      'schema-id': 1,
      fields: oldSchema.fields.map((f) =>
        f.id === 4 ? { ...f, type: 'long' as const } : f
      ),
    };

    const changes = compareSchemas(oldSchema, newSchema);

    const typeChange = changes.find((c) => c.type === 'type-changed');
    expect(typeChange).toBeDefined();
    expect(typeChange?.oldValue).toBe('int');
    expect(typeChange?.newValue).toBe('long');
  });

  it('should detect nullability changes', () => {
    const oldSchema = createTestSchema();
    const newSchema: IcebergSchema = {
      ...oldSchema,
      'schema-id': 1,
      fields: oldSchema.fields.map((f) =>
        f.id === 3 ? { ...f, required: true } : f
      ),
    };

    const changes = compareSchemas(oldSchema, newSchema);

    const nullabilityChange = changes.find((c) => c.type === 'nullability-changed');
    expect(nullabilityChange).toBeDefined();
    expect(nullabilityChange?.oldValue).toBe(false);
    expect(nullabilityChange?.newValue).toBe(true);
  });

  it('should detect documentation changes', () => {
    const oldSchema = createTestSchema();
    const newSchema: IcebergSchema = {
      ...oldSchema,
      'schema-id': 1,
      fields: oldSchema.fields.map((f) =>
        f.id === 2 ? { ...f, doc: 'User full name' } : f
      ),
    };

    const changes = compareSchemas(oldSchema, newSchema);

    const docChange = changes.find((c) => c.type === 'doc-changed');
    expect(docChange).toBeDefined();
    expect(docChange?.newValue).toBe('User full name');
  });
});

// =============================================================================
// Compatibility Checking Tests
// =============================================================================

describe('Compatibility Checking', () => {
  describe('isBackwardCompatible', () => {
    it('should be backward compatible for adding optional fields', () => {
      const changes = [{ type: 'added' as const, fieldId: 5, fieldName: 'phone' }];
      const result = isBackwardCompatible(changes);
      expect(result.compatible).toBe(true);
    });

    it('should be backward compatible for removing fields', () => {
      const changes = [{ type: 'removed' as const, fieldId: 3, fieldName: 'email' }];
      const result = isBackwardCompatible(changes);
      expect(result.compatible).toBe(true);
    });

    it('should be backward compatible for renaming fields', () => {
      const changes = [
        {
          type: 'renamed' as const,
          fieldId: 2,
          fieldName: 'full_name',
          oldValue: 'name',
          newValue: 'full_name',
        },
      ];
      const result = isBackwardCompatible(changes);
      expect(result.compatible).toBe(true);
    });

    it('should be backward compatible for type promotions', () => {
      const changes = [
        {
          type: 'type-changed' as const,
          fieldId: 4,
          fieldName: 'age',
          oldValue: 'int',
          newValue: 'long',
        },
      ];
      const result = isBackwardCompatible(changes);
      expect(result.compatible).toBe(true);
    });

    it('should not be backward compatible for invalid type changes', () => {
      const changes = [
        {
          type: 'type-changed' as const,
          fieldId: 2,
          fieldName: 'name',
          oldValue: 'string',
          newValue: 'int',
        },
      ];
      const result = isBackwardCompatible(changes);
      expect(result.compatible).toBe(false);
      expect(result.incompatibleChanges).toHaveLength(1);
    });

    it('should not be backward compatible for optional -> required', () => {
      const changes = [
        {
          type: 'nullability-changed' as const,
          fieldId: 3,
          fieldName: 'email',
          oldValue: false,
          newValue: true,
        },
      ];
      const result = isBackwardCompatible(changes);
      expect(result.compatible).toBe(false);
    });

    it('should be backward compatible for required -> optional', () => {
      const changes = [
        {
          type: 'nullability-changed' as const,
          fieldId: 2,
          fieldName: 'name',
          oldValue: true,
          newValue: false,
        },
      ];
      const result = isBackwardCompatible(changes);
      expect(result.compatible).toBe(true);
    });
  });

  describe('isForwardCompatible', () => {
    it('should not be forward compatible for type changes', () => {
      const changes = [
        {
          type: 'type-changed' as const,
          fieldId: 4,
          fieldName: 'age',
          oldValue: 'int',
          newValue: 'long',
        },
      ];
      const result = isForwardCompatible(changes);
      expect(result.compatible).toBe(false);
    });

    it('should not be forward compatible for required -> optional', () => {
      const changes = [
        {
          type: 'nullability-changed' as const,
          fieldId: 2,
          fieldName: 'name',
          oldValue: true,
          newValue: false,
        },
      ];
      const result = isForwardCompatible(changes);
      expect(result.compatible).toBe(false);
    });
  });

  describe('isFullyCompatible', () => {
    it('should be fully compatible for documentation changes', () => {
      const changes = [
        {
          type: 'doc-changed' as const,
          fieldId: 2,
          fieldName: 'name',
          oldValue: undefined,
          newValue: 'User name',
        },
      ];
      const result = isFullyCompatible(changes);
      expect(result.compatible).toBe(true);
    });

    it('should not be fully compatible for type changes', () => {
      const changes = [
        {
          type: 'type-changed' as const,
          fieldId: 4,
          fieldName: 'age',
          oldValue: 'int',
          newValue: 'long',
        },
      ];
      const result = isFullyCompatible(changes);
      expect(result.compatible).toBe(false);
    });
  });
});

// =============================================================================
// Schema History Tests
// =============================================================================

describe('Schema History', () => {
  describe('getSchemaHistory', () => {
    it('should return schema history ordered by ID', () => {
      const metadata = createTestMetadata();
      // Add another schema
      metadata.schemas.push({
        ...metadata.schemas[0],
        'schema-id': 1,
        fields: [
          ...metadata.schemas[0].fields,
          { id: 5, name: 'phone', required: false, type: 'string' },
        ],
      });

      const history = getSchemaHistory(metadata);

      expect(history).toHaveLength(2);
      expect(history[0].schemaId).toBe(0);
      expect(history[1].schemaId).toBe(1);
    });
  });

  describe('getSchemaForSnapshot', () => {
    it('should return schema for a snapshot', () => {
      const metadata = createTestMetadata();
      // Add a snapshot
      metadata.snapshots.push({
        'snapshot-id': 123,
        'sequence-number': 1,
        'timestamp-ms': Date.now(),
        'manifest-list': 's3://bucket/metadata/snap.avro',
        summary: { operation: 'append' },
        'schema-id': 0,
      });

      const schema = getSchemaForSnapshot(metadata, 123);

      expect(schema).toBeDefined();
      expect(schema?.['schema-id']).toBe(0);
    });

    it('should return null for non-existent snapshot', () => {
      const metadata = createTestMetadata();
      const schema = getSchemaForSnapshot(metadata, 999);
      expect(schema).toBeNull();
    });
  });

  describe('getSchemaChangesBetween', () => {
    it('should return changes between two schemas', () => {
      const metadata = createTestMetadata();
      metadata.schemas.push({
        ...metadata.schemas[0],
        'schema-id': 1,
        fields: metadata.schemas[0].fields.map((f) =>
          f.id === 4 ? { ...f, type: 'long' as const } : f
        ),
      });

      const changes = getSchemaChangesBetween(metadata, 0, 1);

      expect(changes.some((c) => c.type === 'type-changed')).toBe(true);
    });

    it('should throw error for non-existent schema', () => {
      const metadata = createTestMetadata();
      expect(() => getSchemaChangesBetween(metadata, 0, 99)).toThrow('Schema not found');
    });
  });
});

// =============================================================================
// Field Utility Tests
// =============================================================================

describe('Field Utilities', () => {
  describe('findFieldByName', () => {
    it('should find field by name', () => {
      const schema = createTestSchema();
      const field = findFieldByName(schema, 'email');
      expect(field).toBeDefined();
      expect(field?.id).toBe(3);
    });

    it('should return undefined for non-existent field', () => {
      const schema = createTestSchema();
      const field = findFieldByName(schema, 'nonexistent');
      expect(field).toBeUndefined();
    });
  });

  describe('findFieldById', () => {
    it('should find field by ID', () => {
      const schema = createTestSchema();
      const field = findFieldById(schema, 2);
      expect(field).toBeDefined();
      expect(field?.name).toBe('name');
    });

    it('should return undefined for non-existent ID', () => {
      const schema = createTestSchema();
      const field = findFieldById(schema, 99);
      expect(field).toBeUndefined();
    });
  });

  describe('getAllFieldIds', () => {
    it('should return all field IDs', () => {
      const schema = createTestSchema();
      const ids = getAllFieldIds(schema);
      expect(ids.size).toBe(4);
      expect(ids.has(1)).toBe(true);
      expect(ids.has(2)).toBe(true);
      expect(ids.has(3)).toBe(true);
      expect(ids.has(4)).toBe(true);
    });

    it('should include nested struct field IDs', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          {
            id: 2,
            name: 'address',
            required: false,
            type: {
              type: 'struct',
              fields: [
                { id: 3, name: 'street', required: true, type: 'string' },
                { id: 4, name: 'city', required: true, type: 'string' },
              ],
            },
          },
        ],
      };

      const ids = getAllFieldIds(schema);
      expect(ids.size).toBe(4);
      expect(ids.has(3)).toBe(true);
      expect(ids.has(4)).toBe(true);
    });

    it('should include list element IDs', () => {
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
          },
        ],
      };

      const ids = getAllFieldIds(schema);
      expect(ids.has(1)).toBe(true);
      expect(ids.has(2)).toBe(true);
    });

    it('should include map key/value IDs', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          {
            id: 1,
            name: 'metadata',
            required: false,
            type: {
              type: 'map',
              'key-id': 2,
              'value-id': 3,
              key: 'string',
              value: 'string',
              'value-required': true,
            },
          },
        ],
      };

      const ids = getAllFieldIds(schema);
      expect(ids.has(1)).toBe(true);
      expect(ids.has(2)).toBe(true);
      expect(ids.has(3)).toBe(true);
    });
  });
});

// =============================================================================
// Helper Function Tests
// =============================================================================

describe('Helper Functions', () => {
  describe('evolveSchema', () => {
    it('should create builder from metadata', () => {
      const metadata = createTestMetadata();
      const builder = evolveSchema(metadata);

      builder.addColumn('phone', 'string');
      const result = builder.buildWithMetadata();

      expect(result.schema['schema-id']).toBe(1);
    });

    it('should throw error when current schema not found', () => {
      const metadata = createTestMetadata();
      metadata['current-schema-id'] = 99;

      expect(() => evolveSchema(metadata)).toThrow('Current schema');
    });
  });

  describe('applySchemaEvolution', () => {
    it('should apply evolution and return updated metadata', () => {
      const metadata = createTestMetadata();
      const builder = evolveSchema(metadata);

      builder.addColumn('phone', 'string');
      const updatedMetadata = applySchemaEvolution(metadata, builder);

      expect(updatedMetadata.schemas).toHaveLength(2);
      expect(updatedMetadata['current-schema-id']).toBe(1);
    });
  });

  describe('evolveNestedStruct', () => {
    it('should create builder for nested struct', () => {
      const schema: IcebergSchema = {
        'schema-id': 0,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          {
            id: 2,
            name: 'address',
            required: false,
            type: {
              type: 'struct',
              fields: [
                { id: 3, name: 'street', required: true, type: 'string' },
                { id: 4, name: 'city', required: true, type: 'string' },
              ],
            },
          },
        ],
      };

      const builder = evolveNestedStruct(schema, ['address']);

      builder.addColumn('zip', 'string');
      const newNestedSchema = builder.build();

      expect(newNestedSchema.fields).toHaveLength(3);
      expect(newNestedSchema.fields.find((f) => f.name === 'zip')).toBeDefined();
    });

    it('should throw error for non-existent path', () => {
      const schema = createTestSchema();
      expect(() => evolveNestedStruct(schema, ['nonexistent'])).toThrow('not found');
    });

    it('should throw error for non-struct field', () => {
      const schema = createTestSchema();
      expect(() => evolveNestedStruct(schema, ['name'])).toThrow('not a struct type');
    });
  });
});

// =============================================================================
// Field ID Manager Tests
// =============================================================================

describe('FieldIdManager', () => {
  describe('constructor', () => {
    it('should initialize with starting ID', () => {
      const manager = new FieldIdManager(10);
      expect(manager.getLastId()).toBe(10);
    });
  });

  describe('fromMetadata', () => {
    it('should create from metadata', () => {
      const metadata = createTestMetadata();
      const manager = FieldIdManager.fromMetadata(metadata);
      expect(manager.getLastId()).toBe(metadata['last-column-id']);
    });
  });

  describe('getNextId', () => {
    it('should return incrementing IDs', () => {
      const manager = new FieldIdManager(5);
      expect(manager.getNextId()).toBe(6);
      expect(manager.getNextId()).toBe(7);
      expect(manager.getNextId()).toBe(8);
    });
  });

  describe('peekNextId', () => {
    it('should return next ID without incrementing', () => {
      const manager = new FieldIdManager(5);
      expect(manager.peekNextId()).toBe(6);
      expect(manager.peekNextId()).toBe(6);
      expect(manager.getLastId()).toBe(5);
    });
  });

  describe('reserveIds', () => {
    it('should reserve multiple IDs', () => {
      const manager = new FieldIdManager(5);
      const ids = manager.reserveIds(3);
      expect(ids).toEqual([6, 7, 8]);
      expect(manager.getLastId()).toBe(8);
    });
  });
});

// =============================================================================
// Integration Tests
// =============================================================================

describe('Integration Tests', () => {
  it('should handle complete schema evolution workflow', () => {
    // Create initial metadata
    const metadata = createTestMetadata();

    // Evolve the schema
    const builder = evolveSchema(metadata);
    builder
      .addColumn('phone', 'string', { doc: 'Phone number' })
      .renameColumn('name', 'full_name')
      .updateColumnType('age', 'long')
      .makeColumnOptional('id')
      .updateColumnDoc('email', 'Primary email address');

    // Validate before applying
    const validation = builder.validate();
    expect(validation.valid).toBe(true);

    // Apply evolution
    const result = builder.buildWithMetadata();

    // Verify schema changes
    expect(result.schema['schema-id']).toBe(1);
    expect(result.schema.fields.find((f) => f.name === 'phone')).toBeDefined();
    expect(result.schema.fields.find((f) => f.name === 'full_name')).toBeDefined();
    expect(result.schema.fields.find((f) => f.name === 'name')).toBeUndefined();
    expect(result.schema.fields.find((f) => f.name === 'age')?.type).toBe('long');
    expect(result.schema.fields.find((f) => f.name === 'id')?.required).toBe(false);

    // Verify metadata updates
    expect(result.metadata.schemas).toHaveLength(2);
    expect(result.metadata['current-schema-id']).toBe(1);
    expect(result.metadata['last-column-id']).toBe(5);

    // Compare schemas
    const changes = compareSchemas(metadata.schemas[0], result.schema);
    expect(changes.length).toBeGreaterThan(0);

    // Check backward compatibility
    const compat = isBackwardCompatible(changes);
    expect(compat.compatible).toBe(true);
  });

  it('should preserve field IDs across multiple evolutions', () => {
    let metadata = createTestMetadata();

    // First evolution: add phone
    let builder = evolveSchema(metadata);
    builder.addColumn('phone', 'string');
    metadata = applySchemaEvolution(metadata, builder);

    const phoneId = findFieldByName(metadata.schemas[1], 'phone')?.id;
    expect(phoneId).toBe(5);

    // Second evolution: add address, rename phone
    builder = evolveSchema(metadata);
    builder.addColumn('address', 'string').renameColumn('phone', 'mobile');
    metadata = applySchemaEvolution(metadata, builder);

    // Phone ID should be preserved after rename
    const mobileField = findFieldByName(metadata.schemas[2], 'mobile');
    expect(mobileField?.id).toBe(phoneId);

    // Address should get the next ID
    const addressField = findFieldByName(metadata.schemas[2], 'address');
    expect(addressField?.id).toBe(6);
  });
});
