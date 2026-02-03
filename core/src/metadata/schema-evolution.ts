/**
 * Iceberg Schema Evolution
 *
 * Implements schema evolution for Iceberg tables following the Apache Iceberg
 * specification. Schema evolution allows modifying table schemas while maintaining
 * backward compatibility with existing Parquet files.
 *
 * Key features:
 * - Add/drop/rename/reorder columns
 * - Type widening (compatible type promotions)
 * - Required/optional changes
 * - Nested field modifications
 * - Schema versioning and history tracking
 * - Field ID management (monotonically increasing)
 *
 * @see https://iceberg.apache.org/spec/#schema-evolution
 */

import type {
  IcebergSchema,
  IcebergStructField,
  IcebergType,
  IcebergPrimitiveType,
  TableMetadata,
} from './types.js';

// =============================================================================
// Type Promotion Rules
// =============================================================================

/**
 * Type promotion rules following Iceberg specification.
 * A type can be promoted to another if it's in the allowed promotions list.
 *
 * @see https://iceberg.apache.org/spec/#schema-evolution
 */
const TYPE_PROMOTIONS: Record<IcebergPrimitiveType, IcebergPrimitiveType[]> = {
  // Integer promotions
  int: ['long'],
  long: [],

  // Floating point promotions
  float: ['double'],
  double: [],

  // Decimal can be widened (precision/scale increase)
  decimal: ['decimal'],

  // No promotions for these types
  boolean: [],
  date: [],
  time: [],
  timestamp: [],
  timestamptz: [],
  timestamp_ns: [], // Iceberg v3: nanosecond precision timestamp
  timestamptz_ns: [], // Iceberg v3: nanosecond precision timestamp with timezone
  string: [],
  uuid: [],
  fixed: ['binary'], // fixed -> binary is allowed
  binary: [],
  variant: [], // Iceberg v3: semi-structured JSON-like data, no promotions
  unknown: [], // Iceberg v3: unknown type - cannot be promoted to any other type
};

/**
 * Check if a type promotion is allowed.
 *
 * Valid promotions:
 * - int -> long
 * - float -> double
 * - decimal(P, S) -> decimal(P', S) where P' > P (same scale)
 * - fixed(L) -> binary
 */
export function isTypePromotionAllowed(
  fromType: IcebergPrimitiveType,
  toType: IcebergPrimitiveType
): boolean {
  if (fromType === toType) return true;
  return TYPE_PROMOTIONS[fromType]?.includes(toType) ?? false;
}

/**
 * Check if two types are compatible for evolution.
 * Returns a compatibility result with details about any incompatibility.
 */
export function areTypesCompatible(
  oldType: IcebergType,
  newType: IcebergType
): TypeCompatibilityResult {
  // Handle primitive types
  if (typeof oldType === 'string' && typeof newType === 'string') {
    if (isTypePromotionAllowed(oldType, newType)) {
      return { compatible: true };
    }
    return {
      compatible: false,
      reason: `Cannot promote type '${oldType}' to '${newType}'`,
    };
  }

  // Handle struct types
  if (
    typeof oldType === 'object' &&
    typeof newType === 'object' &&
    oldType.type === 'struct' &&
    newType.type === 'struct'
  ) {
    // Struct evolution is handled separately via field operations
    return { compatible: true };
  }

  // Handle list types
  if (
    typeof oldType === 'object' &&
    typeof newType === 'object' &&
    oldType.type === 'list' &&
    newType.type === 'list'
  ) {
    return areTypesCompatible(oldType.element, newType.element);
  }

  // Handle map types
  if (
    typeof oldType === 'object' &&
    typeof newType === 'object' &&
    oldType.type === 'map' &&
    newType.type === 'map'
  ) {
    const keyCompatible = areTypesCompatible(oldType.key, newType.key);
    if (!keyCompatible.compatible) return keyCompatible;

    return areTypesCompatible(oldType.value, newType.value);
  }

  // Different type categories are not compatible
  return {
    compatible: false,
    reason: `Cannot convert between different type categories`,
  };
}

/** Result of type compatibility check */
export interface TypeCompatibilityResult {
  compatible: boolean;
  reason?: string;
}

// =============================================================================
// Schema Evolution Error
// =============================================================================

/** Error codes for schema evolution operations */
export type SchemaEvolutionErrorCode =
  | 'FIELD_NOT_FOUND'
  | 'FIELD_EXISTS'
  | 'INCOMPATIBLE_TYPE'
  | 'REQUIRED_FIELD_NO_DEFAULT'
  | 'INVALID_OPERATION'
  | 'INVALID_POSITION'
  | 'IDENTIFIER_FIELD';

/**
 * Error thrown during schema evolution operations.
 */
export class SchemaEvolutionError extends Error {
  constructor(
    message: string,
    public readonly code: SchemaEvolutionErrorCode
  ) {
    super(message);
    this.name = 'SchemaEvolutionError';
  }
}

// =============================================================================
// Schema Evolution Operations
// =============================================================================

/** Schema evolution operation types */
export type SchemaEvolutionOperation =
  | AddColumnOperation
  | DropColumnOperation
  | RenameColumnOperation
  | UpdateColumnTypeOperation
  | MakeColumnOptionalOperation
  | MakeColumnRequiredOperation
  | UpdateColumnDocOperation
  | MoveColumnOperation;

/** Add a new column to the schema */
export interface AddColumnOperation {
  type: 'add-column';
  /** Column name (use dot notation for nested fields, e.g., "address.city") */
  name: string;
  /** The Iceberg type for the new column */
  fieldType: IcebergType;
  /** Whether the column is required (default: false) */
  required: boolean;
  /** Optional documentation for the column */
  doc?: string;
  /** Optional position specification */
  position?: ColumnPosition;
}

/** Drop a column from the schema */
export interface DropColumnOperation {
  type: 'drop-column';
  /** Column name to drop */
  name: string;
}

/** Rename a column */
export interface RenameColumnOperation {
  type: 'rename-column';
  /** Current column name */
  oldName: string;
  /** New column name */
  newName: string;
}

/** Update a column's type (only compatible promotions allowed) */
export interface UpdateColumnTypeOperation {
  type: 'update-column-type';
  /** Column name */
  name: string;
  /** New type (must be compatible with current type) */
  newType: IcebergType;
}

/** Make a required column optional */
export interface MakeColumnOptionalOperation {
  type: 'make-column-optional';
  /** Column name */
  name: string;
}

/** Make an optional column required */
export interface MakeColumnRequiredOperation {
  type: 'make-column-required';
  /** Column name */
  name: string;
}

/** Update a column's documentation */
export interface UpdateColumnDocOperation {
  type: 'update-column-doc';
  /** Column name */
  name: string;
  /** New documentation (undefined to remove) */
  doc: string | undefined;
}

/** Move a column to a new position */
export interface MoveColumnOperation {
  type: 'move-column';
  /** Column name */
  name: string;
  /** New position */
  position: ColumnPosition;
}

/**
 * Column position specification.
 * Used when adding or moving columns.
 */
export type ColumnPosition =
  | { type: 'first' }
  | { type: 'last' }
  | { type: 'after'; column: string }
  | { type: 'before'; column: string };

// =============================================================================
// Schema Evolution Builder
// =============================================================================

/**
 * Builder for schema evolution operations.
 * Provides a fluent API for modifying Iceberg schemas.
 *
 * @example
 * ```ts
 * const builder = new SchemaEvolutionBuilder(currentSchema, metadata);
 *
 * builder
 *   .addColumn('email', 'string', { required: false })
 *   .renameColumn('user_name', 'username')
 *   .dropColumn('deprecated_field')
 *   .updateColumnType('count', 'long')
 *   .moveColumn('email', { type: 'after', column: 'name' });
 *
 * const { valid, errors } = builder.validate();
 * if (valid) {
 *   const newSchema = builder.build();
 * }
 * ```
 */
export class SchemaEvolutionBuilder {
  private operations: SchemaEvolutionOperation[] = [];
  private currentSchema: IcebergSchema;
  private metadata: TableMetadata | null = null;

  /**
   * Create a new schema evolution builder.
   *
   * @param schema - The current schema to evolve
   * @param metadata - Optional table metadata for field ID tracking
   */
  constructor(schema: IcebergSchema, metadata?: TableMetadata) {
    // Deep clone the schema to avoid mutations
    this.currentSchema = JSON.parse(JSON.stringify(schema));
    this.metadata = metadata ?? null;
  }

  /**
   * Add a new column to the schema.
   *
   * @param name - Column name (use dot notation for nested fields)
   * @param type - Iceberg type for the column
   * @param options - Optional settings (required, doc, position)
   */
  addColumn(
    name: string,
    type: IcebergType,
    options?: {
      required?: boolean;
      doc?: string;
      position?: ColumnPosition;
    }
  ): this {
    this.operations.push({
      type: 'add-column',
      name,
      fieldType: type,
      required: options?.required ?? false, // New columns default to optional
      doc: options?.doc,
      position: options?.position,
    });
    return this;
  }

  /**
   * Drop a column from the schema.
   *
   * Note: The field ID is preserved for historical data compatibility.
   * Dropping identifier fields is not allowed.
   *
   * @param name - Column name to drop
   */
  dropColumn(name: string): this {
    this.operations.push({
      type: 'drop-column',
      name,
    });
    return this;
  }

  /**
   * Rename a column.
   *
   * Note: Renaming preserves the field ID, so this is always compatible
   * with existing data files.
   *
   * @param oldName - Current column name
   * @param newName - New column name
   */
  renameColumn(oldName: string, newName: string): this {
    this.operations.push({
      type: 'rename-column',
      oldName,
      newName,
    });
    return this;
  }

  /**
   * Update a column's type.
   *
   * Only compatible type promotions are allowed:
   * - int -> long
   * - float -> double
   * - decimal(P, S) -> decimal(P', S) where P' > P
   * - fixed(L) -> binary
   *
   * @param name - Column name
   * @param newType - New type (must be compatible)
   */
  updateColumnType(name: string, newType: IcebergType): this {
    this.operations.push({
      type: 'update-column-type',
      name,
      newType,
    });
    return this;
  }

  /**
   * Make a required column optional.
   *
   * This is always a safe operation for backward compatibility.
   *
   * @param name - Column name
   */
  makeColumnOptional(name: string): this {
    this.operations.push({
      type: 'make-column-optional',
      name,
    });
    return this;
  }

  /**
   * Make an optional column required.
   *
   * Warning: This is only safe if all existing data has non-null values
   * for this column. Otherwise, reads of historical data may fail.
   *
   * @param name - Column name
   */
  makeColumnRequired(name: string): this {
    this.operations.push({
      type: 'make-column-required',
      name,
    });
    return this;
  }

  /**
   * Update a column's documentation.
   *
   * @param name - Column name
   * @param doc - New documentation (undefined to remove)
   */
  updateColumnDoc(name: string, doc: string | undefined): this {
    this.operations.push({
      type: 'update-column-doc',
      name,
      doc,
    });
    return this;
  }

  /**
   * Move a column to a new position.
   *
   * Note: Column ordering is metadata-only; it does not affect data files.
   *
   * @param name - Column name
   * @param position - New position specification
   */
  moveColumn(name: string, position: ColumnPosition): this {
    this.operations.push({
      type: 'move-column',
      name,
      position,
    });
    return this;
  }

  /**
   * Get the list of pending operations.
   */
  getOperations(): SchemaEvolutionOperation[] {
    return [...this.operations];
  }

  /**
   * Clear all pending operations.
   */
  clear(): this {
    this.operations = [];
    return this;
  }

  /**
   * Validate all pending operations without applying them.
   *
   * @returns Validation result with any errors
   */
  validate(): SchemaValidationResult {
    const errors: string[] = [];
    let workingSchema = JSON.parse(JSON.stringify(this.currentSchema)) as IcebergSchema;

    for (const op of this.operations) {
      try {
        workingSchema = this.applyOperationToSchema(workingSchema, op, true);
      } catch (err) {
        if (err instanceof SchemaEvolutionError) {
          errors.push(`${op.type}: ${err.message}`);
        } else {
          errors.push(`${op.type}: ${String(err)}`);
        }
      }
    }

    return { valid: errors.length === 0, errors };
  }

  /**
   * Build the new schema with all operations applied.
   *
   * @throws SchemaEvolutionError if any operation fails
   * @returns The evolved schema with incremented schema-id
   */
  build(): IcebergSchema {
    let newSchema = JSON.parse(JSON.stringify(this.currentSchema)) as IcebergSchema;

    // Apply all operations
    for (const op of this.operations) {
      newSchema = this.applyOperationToSchema(newSchema, op, false);
    }

    // Increment schema ID
    return {
      ...newSchema,
      'schema-id': newSchema['schema-id'] + 1,
    };
  }

  /**
   * Build the evolved schema and update table metadata.
   *
   * This method:
   * 1. Applies all operations to create a new schema
   * 2. Assigns monotonically increasing field IDs
   * 3. Updates the table metadata with the new schema
   * 4. Preserves schema history
   *
   * @throws Error if no metadata was provided to the builder
   * @returns Object containing the new schema, updated metadata, and next column ID
   */
  buildWithMetadata(): SchemaEvolutionResult {
    if (!this.metadata) {
      throw new Error('No metadata provided to builder');
    }

    const newSchema = this.build();

    // Calculate the next column ID based on operations
    let nextColumnId = this.metadata['last-column-id'];
    for (const op of this.operations) {
      if (op.type === 'add-column') {
        nextColumnId++;
      }
    }

    // Update metadata with new schema
    const updatedMetadata: TableMetadata = {
      ...this.metadata,
      'last-updated-ms': Date.now(),
      'last-column-id': nextColumnId,
      'current-schema-id': newSchema['schema-id'],
      schemas: [...this.metadata.schemas, newSchema],
    };

    return {
      schema: newSchema,
      metadata: updatedMetadata,
      nextColumnId,
    };
  }

  /**
   * Apply a single operation to a schema.
   * Returns a new schema with the operation applied.
   */
  private applyOperationToSchema(
    schema: IcebergSchema,
    op: SchemaEvolutionOperation,
    validateOnly: boolean
  ): IcebergSchema {
    switch (op.type) {
      case 'add-column':
        return this.applyAddColumn(schema, op, validateOnly);
      case 'drop-column':
        return this.applyDropColumn(schema, op, validateOnly);
      case 'rename-column':
        return this.applyRenameColumn(schema, op, validateOnly);
      case 'update-column-type':
        return this.applyUpdateColumnType(schema, op, validateOnly);
      case 'make-column-optional':
        return this.applyMakeColumnOptional(schema, op, validateOnly);
      case 'make-column-required':
        return this.applyMakeColumnRequired(schema, op, validateOnly);
      case 'update-column-doc':
        return this.applyUpdateColumnDoc(schema, op, validateOnly);
      case 'move-column':
        return this.applyMoveColumn(schema, op, validateOnly);
    }
  }

  private applyAddColumn(
    schema: IcebergSchema,
    op: AddColumnOperation,
    validateOnly: boolean
  ): IcebergSchema {
    // Check if field already exists
    const existing = schema.fields.find((f) => f.name === op.name);
    if (existing) {
      throw new SchemaEvolutionError(
        `Column '${op.name}' already exists`,
        'FIELD_EXISTS'
      );
    }

    if (!validateOnly) {
      // Assign new field ID (monotonically increasing)
      // Use the original schema's max ID to avoid double-counting when
      // previous add operations have already modified the working schema
      const baseMaxId = this.findMaxFieldIdInFields([...this.currentSchema.fields]);
      const lastColumnId = this.metadata?.['last-column-id'] ?? baseMaxId;
      const previousAdds = this.countPreviousAddOperations(op);
      const newId = lastColumnId + previousAdds + 1;

      const newField: IcebergStructField = {
        id: newId,
        name: op.name,
        type: op.fieldType,
        required: op.required,
        doc: op.doc,
      };

      // Insert at the specified position
      let newFields: IcebergStructField[];
      if (op.position) {
        const index = this.resolvePosition([...schema.fields], op.position);
        newFields = [...schema.fields.slice(0, index), newField, ...schema.fields.slice(index)];
      } else {
        // Default: add at end
        newFields = [...schema.fields, newField];
      }
      return { ...schema, fields: newFields };
    }
    return schema;
  }

  private applyDropColumn(
    schema: IcebergSchema,
    op: DropColumnOperation,
    validateOnly: boolean
  ): IcebergSchema {
    const index = schema.fields.findIndex((f) => f.name === op.name);
    if (index === -1) {
      throw new SchemaEvolutionError(
        `Column '${op.name}' not found`,
        'FIELD_NOT_FOUND'
      );
    }

    // Check if this field is in identifier-field-ids
    const field = schema.fields[index];
    const identifierFieldIds = (schema as IcebergSchema & { 'identifier-field-ids'?: number[] })[
      'identifier-field-ids'
    ];
    if (identifierFieldIds?.includes(field.id)) {
      throw new SchemaEvolutionError(
        `Cannot drop identifier column '${op.name}'`,
        'IDENTIFIER_FIELD'
      );
    }

    if (!validateOnly) {
      const newFields = [...schema.fields.slice(0, index), ...schema.fields.slice(index + 1)];
      return { ...schema, fields: newFields };
    }
    return schema;
  }

  private applyRenameColumn(
    schema: IcebergSchema,
    op: RenameColumnOperation,
    validateOnly: boolean
  ): IcebergSchema {
    const fieldIndex = schema.fields.findIndex((f) => f.name === op.oldName);
    if (fieldIndex === -1) {
      throw new SchemaEvolutionError(
        `Column '${op.oldName}' not found`,
        'FIELD_NOT_FOUND'
      );
    }

    // Check new name doesn't conflict
    const conflict = schema.fields.find((f) => f.name === op.newName);
    if (conflict) {
      throw new SchemaEvolutionError(
        `Column '${op.newName}' already exists`,
        'FIELD_EXISTS'
      );
    }

    if (!validateOnly) {
      const field = schema.fields[fieldIndex];
      const newField = { ...field, name: op.newName };
      const newFields = [
        ...schema.fields.slice(0, fieldIndex),
        newField,
        ...schema.fields.slice(fieldIndex + 1),
      ];
      return { ...schema, fields: newFields };
    }
    return schema;
  }

  private applyUpdateColumnType(
    schema: IcebergSchema,
    op: UpdateColumnTypeOperation,
    validateOnly: boolean
  ): IcebergSchema {
    const fieldIndex = schema.fields.findIndex((f) => f.name === op.name);
    if (fieldIndex === -1) {
      throw new SchemaEvolutionError(
        `Column '${op.name}' not found`,
        'FIELD_NOT_FOUND'
      );
    }

    const field = schema.fields[fieldIndex];
    // Check type compatibility
    const compatibility = areTypesCompatible(field.type, op.newType);
    if (!compatibility.compatible) {
      throw new SchemaEvolutionError(
        `Cannot change type of column '${op.name}': ${compatibility.reason}`,
        'INCOMPATIBLE_TYPE'
      );
    }

    if (!validateOnly) {
      const newField = { ...field, type: op.newType };
      const newFields = [
        ...schema.fields.slice(0, fieldIndex),
        newField,
        ...schema.fields.slice(fieldIndex + 1),
      ];
      return { ...schema, fields: newFields };
    }
    return schema;
  }

  private applyMakeColumnOptional(
    schema: IcebergSchema,
    op: MakeColumnOptionalOperation,
    validateOnly: boolean
  ): IcebergSchema {
    const fieldIndex = schema.fields.findIndex((f) => f.name === op.name);
    if (fieldIndex === -1) {
      throw new SchemaEvolutionError(
        `Column '${op.name}' not found`,
        'FIELD_NOT_FOUND'
      );
    }

    if (!validateOnly) {
      const field = schema.fields[fieldIndex];
      const newField = { ...field, required: false };
      const newFields = [
        ...schema.fields.slice(0, fieldIndex),
        newField,
        ...schema.fields.slice(fieldIndex + 1),
      ];
      return { ...schema, fields: newFields };
    }
    return schema;
  }

  private applyMakeColumnRequired(
    schema: IcebergSchema,
    op: MakeColumnRequiredOperation,
    validateOnly: boolean
  ): IcebergSchema {
    const fieldIndex = schema.fields.findIndex((f) => f.name === op.name);
    if (fieldIndex === -1) {
      throw new SchemaEvolutionError(
        `Column '${op.name}' not found`,
        'FIELD_NOT_FOUND'
      );
    }

    if (!validateOnly) {
      const field = schema.fields[fieldIndex];
      const newField = { ...field, required: true };
      const newFields = [
        ...schema.fields.slice(0, fieldIndex),
        newField,
        ...schema.fields.slice(fieldIndex + 1),
      ];
      return { ...schema, fields: newFields };
    }
    return schema;
  }

  private applyUpdateColumnDoc(
    schema: IcebergSchema,
    op: UpdateColumnDocOperation,
    validateOnly: boolean
  ): IcebergSchema {
    const fieldIndex = schema.fields.findIndex((f) => f.name === op.name);
    if (fieldIndex === -1) {
      throw new SchemaEvolutionError(
        `Column '${op.name}' not found`,
        'FIELD_NOT_FOUND'
      );
    }

    if (!validateOnly) {
      const field = schema.fields[fieldIndex];
      let newField: IcebergStructField;
      if (op.doc === undefined) {
        // Remove doc property by destructuring and omitting it
        const { doc: _, ...rest } = field;
        newField = rest;
      } else {
        newField = { ...field, doc: op.doc };
      }
      const newFields = [
        ...schema.fields.slice(0, fieldIndex),
        newField,
        ...schema.fields.slice(fieldIndex + 1),
      ];
      return { ...schema, fields: newFields };
    }
    return schema;
  }

  private applyMoveColumn(
    schema: IcebergSchema,
    op: MoveColumnOperation,
    validateOnly: boolean
  ): IcebergSchema {
    const index = schema.fields.findIndex((f) => f.name === op.name);
    if (index === -1) {
      throw new SchemaEvolutionError(
        `Column '${op.name}' not found`,
        'FIELD_NOT_FOUND'
      );
    }

    if (!validateOnly) {
      const field = schema.fields[index];
      // Remove the field from its current position
      const fieldsWithoutField = [...schema.fields.slice(0, index), ...schema.fields.slice(index + 1)];
      // Resolve new position in the array without the field
      const newIndex = this.resolvePosition(fieldsWithoutField, op.position);
      // Insert at the new position
      const newFields = [...fieldsWithoutField.slice(0, newIndex), field, ...fieldsWithoutField.slice(newIndex)];
      return { ...schema, fields: newFields };
    }
    return schema;
  }

  /**
   * Count add-column operations before the given operation.
   */
  private countPreviousAddOperations(currentOp: AddColumnOperation): number {
    let count = 0;
    for (const op of this.operations) {
      if (op === currentOp) break;
      if (op.type === 'add-column') count++;
    }
    return count;
  }

  /**
   * Resolve a position specification to an array index.
   */
  private resolvePosition(fields: IcebergStructField[], position: ColumnPosition): number {
    switch (position.type) {
      case 'first':
        return 0;
      case 'last':
        return fields.length;
      case 'after': {
        const afterIndex = fields.findIndex((f) => f.name === position.column);
        if (afterIndex === -1) {
          throw new SchemaEvolutionError(
            `Reference column '${position.column}' not found for 'after' position`,
            'INVALID_POSITION'
          );
        }
        return afterIndex + 1;
      }
      case 'before': {
        const beforeIndex = fields.findIndex((f) => f.name === position.column);
        if (beforeIndex === -1) {
          throw new SchemaEvolutionError(
            `Reference column '${position.column}' not found for 'before' position`,
            'INVALID_POSITION'
          );
        }
        return beforeIndex;
      }
    }
  }

  /**
   * Find the maximum field ID in a list of fields (recursive for nested types).
   */
  private findMaxFieldIdInFields(fields: readonly IcebergStructField[]): number {
    let maxId = 0;
    for (const field of fields) {
      maxId = Math.max(maxId, field.id);
      if (typeof field.type === 'object') {
        if (field.type.type === 'struct') {
          maxId = Math.max(maxId, this.findMaxFieldIdInFields(field.type.fields));
        } else if (field.type.type === 'list') {
          maxId = Math.max(maxId, field.type['element-id']);
        } else if (field.type.type === 'map') {
          maxId = Math.max(maxId, field.type['key-id'], field.type['value-id']);
        }
      }
    }
    return maxId;
  }
}

/** Result of schema validation */
export interface SchemaValidationResult {
  valid: boolean;
  errors: string[];
}

/** Result of schema evolution with metadata */
export interface SchemaEvolutionResult {
  schema: IcebergSchema;
  metadata: TableMetadata;
  nextColumnId: number;
}

// =============================================================================
// Schema Comparison
// =============================================================================

/** Types of schema changes detected in comparison */
export type SchemaChangeKind =
  | 'added'
  | 'removed'
  | 'renamed'
  | 'type-changed'
  | 'nullability-changed'
  | 'doc-changed'
  | 'reordered';

/** A single change detected between two schemas */
export interface SchemaChangeSummary {
  /** Type of change */
  type: SchemaChangeKind;
  /** Field ID affected */
  fieldId: number;
  /** Current field name */
  fieldName: string;
  /** Previous value (varies by change type) */
  oldValue?: unknown;
  /** New value (varies by change type) */
  newValue?: unknown;
}

/**
 * Compare two schemas and return the differences.
 *
 * @param oldSchema - The original schema
 * @param newSchema - The new/evolved schema
 * @returns Array of changes detected between schemas
 */
export function compareSchemas(
  oldSchema: IcebergSchema,
  newSchema: IcebergSchema
): SchemaChangeSummary[] {
  const changes: SchemaChangeSummary[] = [];

  const oldFieldsById = new Map(oldSchema.fields.map((f) => [f.id, f]));
  const newFieldsById = new Map(newSchema.fields.map((f) => [f.id, f]));

  // Find removed fields
  for (const [id, field] of oldFieldsById) {
    if (!newFieldsById.has(id)) {
      changes.push({
        type: 'removed',
        fieldId: id,
        fieldName: field.name,
      });
    }
  }

  // Find added and changed fields
  for (const [id, newField] of newFieldsById) {
    const oldField = oldFieldsById.get(id);

    if (!oldField) {
      changes.push({
        type: 'added',
        fieldId: id,
        fieldName: newField.name,
      });
      continue;
    }

    // Check for renames
    if (oldField.name !== newField.name) {
      changes.push({
        type: 'renamed',
        fieldId: id,
        fieldName: newField.name,
        oldValue: oldField.name,
        newValue: newField.name,
      });
    }

    // Check for type changes
    if (JSON.stringify(oldField.type) !== JSON.stringify(newField.type)) {
      changes.push({
        type: 'type-changed',
        fieldId: id,
        fieldName: newField.name,
        oldValue: oldField.type,
        newValue: newField.type,
      });
    }

    // Check for nullability changes
    if (oldField.required !== newField.required) {
      changes.push({
        type: 'nullability-changed',
        fieldId: id,
        fieldName: newField.name,
        oldValue: oldField.required,
        newValue: newField.required,
      });
    }

    // Check for doc changes
    if (oldField.doc !== newField.doc) {
      changes.push({
        type: 'doc-changed',
        fieldId: id,
        fieldName: newField.name,
        oldValue: oldField.doc,
        newValue: newField.doc,
      });
    }
  }

  // Check for reordering (by position in array)
  const oldOrder = oldSchema.fields.map((f) => f.id);
  const newOrder = newSchema.fields.filter((f) => oldFieldsById.has(f.id)).map((f) => f.id);
  const commonIds = oldOrder.filter((id) => newFieldsById.has(id));

  if (commonIds.length > 1) {
    // Check if relative order changed
    for (let i = 0; i < commonIds.length - 1; i++) {
      const oldIdx1 = oldOrder.indexOf(commonIds[i]);
      const oldIdx2 = oldOrder.indexOf(commonIds[i + 1]);
      const newIdx1 = newOrder.indexOf(commonIds[i]);
      const newIdx2 = newOrder.indexOf(commonIds[i + 1]);

      if ((oldIdx1 < oldIdx2) !== (newIdx1 < newIdx2)) {
        const field = newFieldsById.get(commonIds[i + 1])!;
        changes.push({
          type: 'reordered',
          fieldId: field.id,
          fieldName: field.name,
          oldValue: oldOrder.indexOf(field.id),
          newValue: newOrder.indexOf(field.id),
        });
      }
    }
  }

  return changes;
}

// =============================================================================
// Compatibility Checking
// =============================================================================

/** Result of backward compatibility check */
export interface CompatibilityResult {
  /** Whether the evolution is backward compatible */
  compatible: boolean;
  /** List of incompatible changes */
  incompatibleChanges: SchemaChangeSummary[];
}

/**
 * Check if a schema change is backward compatible.
 *
 * Backward compatibility means new readers can read data written with the old schema.
 *
 * The following changes are backward compatible:
 * - Adding optional fields
 * - Removing fields (readers ignore unknown fields)
 * - Renaming fields (field ID is preserved)
 * - Making required fields optional
 * - Type promotions (int->long, float->double)
 * - Documentation changes
 *
 * The following changes are NOT backward compatible:
 * - Adding required fields (old data won't have values)
 * - Making optional fields required (old data may have nulls)
 * - Incompatible type changes
 *
 * @param changes - Array of schema changes to check
 * @returns Compatibility result with list of incompatible changes
 */
export function isBackwardCompatible(changes: SchemaChangeSummary[]): CompatibilityResult {
  const incompatibleChanges: SchemaChangeSummary[] = [];

  for (const change of changes) {
    switch (change.type) {
      case 'removed':
        // Removing fields is backward compatible (readers can ignore missing fields)
        break;

      case 'added':
        // Adding fields is checked elsewhere (required vs optional)
        break;

      case 'renamed':
        // Renaming preserves the field ID, so it's compatible
        break;

      case 'type-changed':
        // Type changes need to be checked for promotion compatibility
        if (typeof change.oldValue === 'string' && typeof change.newValue === 'string') {
          if (
            !isTypePromotionAllowed(
              change.oldValue as IcebergPrimitiveType,
              change.newValue as IcebergPrimitiveType
            )
          ) {
            incompatibleChanges.push(change);
          }
        } else {
          // Complex type changes need deeper analysis
          const compat = areTypesCompatible(
            change.oldValue as IcebergType,
            change.newValue as IcebergType
          );
          if (!compat.compatible) {
            incompatibleChanges.push(change);
          }
        }
        break;

      case 'nullability-changed':
        // optional -> required is not backward compatible
        if (change.oldValue === false && change.newValue === true) {
          incompatibleChanges.push(change);
        }
        break;

      case 'doc-changed':
      case 'reordered':
        // Documentation and ordering changes are always compatible
        break;
    }
  }

  return {
    compatible: incompatibleChanges.length === 0,
    incompatibleChanges,
  };
}

/**
 * Check if a schema change is forward compatible.
 *
 * Forward compatibility means old readers can read data written with the new schema.
 *
 * The following changes are forward compatible:
 * - Removing fields (old readers may expect them but can handle defaults)
 * - Adding optional fields (old readers will ignore)
 * - Renaming fields (field ID is preserved)
 * - Making required fields optional
 * - Documentation changes
 *
 * The following changes are NOT forward compatible:
 * - Adding required fields that old readers expect to be optional
 * - Making optional fields required
 * - Type changes (old readers may not handle new types)
 *
 * @param changes - Array of schema changes to check
 * @returns Compatibility result with list of incompatible changes
 */
export function isForwardCompatible(changes: SchemaChangeSummary[]): CompatibilityResult {
  const incompatibleChanges: SchemaChangeSummary[] = [];

  for (const change of changes) {
    switch (change.type) {
      case 'added':
        // Adding fields requires old readers to ignore unknown fields
        // This is generally supported but implementation-dependent
        break;

      case 'removed':
        // Removing fields may break old readers that expect them
        // However, Iceberg readers should handle missing fields
        break;

      case 'renamed':
        // Renaming preserves the field ID, so it's forward compatible
        break;

      case 'type-changed':
        // Type changes may not be forward compatible
        // Old readers may not be able to read new types
        if (typeof change.oldValue === 'string' && typeof change.newValue === 'string') {
          if (change.oldValue !== change.newValue) {
            // Any type change is potentially not forward compatible
            // unless it's a valid reverse promotion (which is rare)
            incompatibleChanges.push(change);
          }
        } else {
          incompatibleChanges.push(change);
        }
        break;

      case 'nullability-changed':
        // required -> optional is not forward compatible
        // Old readers expecting required may fail on null
        if (change.oldValue === true && change.newValue === false) {
          incompatibleChanges.push(change);
        }
        break;

      case 'doc-changed':
      case 'reordered':
        // Documentation and ordering changes are always compatible
        break;
    }
  }

  return {
    compatible: incompatibleChanges.length === 0,
    incompatibleChanges,
  };
}

/**
 * Check if a schema change is fully compatible (both backward and forward).
 *
 * Full compatibility means:
 * - New readers can read old data (backward compatible)
 * - Old readers can read new data (forward compatible)
 *
 * @param changes - Array of schema changes to check
 * @returns Compatibility result
 */
export function isFullyCompatible(changes: SchemaChangeSummary[]): CompatibilityResult {
  const backward = isBackwardCompatible(changes);
  const forward = isForwardCompatible(changes);

  const allIncompatible = [
    ...backward.incompatibleChanges,
    ...forward.incompatibleChanges.filter(
      (c) => !backward.incompatibleChanges.some((bc) => bc.fieldId === c.fieldId && bc.type === c.type)
    ),
  ];

  return {
    compatible: backward.compatible && forward.compatible,
    incompatibleChanges: allIncompatible,
  };
}

// =============================================================================
// Schema History
// =============================================================================

/** Schema history entry */
export interface SchemaHistoryEntry {
  schemaId: number;
  fields: readonly IcebergStructField[];
  createdAt?: number;
}

/**
 * Get the schema history for a table.
 *
 * @param metadata - Table metadata containing schema history
 * @returns Array of schema history entries, ordered by schema ID
 */
export function getSchemaHistory(metadata: TableMetadata): SchemaHistoryEntry[] {
  return [...metadata.schemas]
    .map((s) => ({
      schemaId: s['schema-id'],
      fields: s.fields,
    }))
    .sort((a, b) => a.schemaId - b.schemaId);
}

/**
 * Get the schema for a specific snapshot.
 *
 * @param metadata - Table metadata
 * @param snapshotId - The snapshot ID to get schema for
 * @returns The schema for the snapshot, or null if not found
 */
export function getSchemaForSnapshot(
  metadata: TableMetadata,
  snapshotId: number
): IcebergSchema | null {
  const snapshot = metadata.snapshots?.find((s) => s['snapshot-id'] === snapshotId);
  if (!snapshot) return null;

  const schemaId = snapshot['schema-id'] ?? metadata['current-schema-id'];
  return metadata.schemas.find((s) => s['schema-id'] === schemaId) ?? null;
}

/**
 * Get the changes between two schema versions.
 *
 * @param metadata - Table metadata
 * @param fromSchemaId - Starting schema ID
 * @param toSchemaId - Ending schema ID
 * @returns Array of changes between the schemas
 */
export function getSchemaChangesBetween(
  metadata: TableMetadata,
  fromSchemaId: number,
  toSchemaId: number
): SchemaChangeSummary[] {
  const fromSchema = metadata.schemas.find((s) => s['schema-id'] === fromSchemaId);
  const toSchema = metadata.schemas.find((s) => s['schema-id'] === toSchemaId);

  if (!fromSchema || !toSchema) {
    throw new Error(`Schema not found: ${!fromSchema ? fromSchemaId : toSchemaId}`);
  }

  return compareSchemas(fromSchema, toSchema);
}

// =============================================================================
// Field Utilities
// =============================================================================

/**
 * Find a field by name in a schema.
 *
 * @param schema - The schema to search
 * @param name - Field name to find
 * @returns The field or undefined if not found
 */
export function findFieldByName(
  schema: IcebergSchema,
  name: string
): IcebergStructField | undefined {
  return schema.fields.find((f) => f.name === name);
}

/**
 * Find a field by ID in a schema.
 *
 * @param schema - The schema to search
 * @param id - Field ID to find
 * @returns The field or undefined if not found
 */
export function findFieldById(
  schema: IcebergSchema,
  id: number
): IcebergStructField | undefined {
  return schema.fields.find((f) => f.id === id);
}

/**
 * Get all field IDs in a schema (including nested fields).
 *
 * @param schema - The schema to traverse
 * @returns Set of all field IDs
 */
export function getAllFieldIds(schema: IcebergSchema): Set<number> {
  const ids = new Set<number>();

  function traverse(fields: readonly IcebergStructField[]): void {
    for (const field of fields) {
      ids.add(field.id);
      if (typeof field.type === 'object') {
        if (field.type.type === 'struct') {
          traverse(field.type.fields);
        } else if (field.type.type === 'list') {
          ids.add(field.type['element-id']);
          if (typeof field.type.element === 'object' && field.type.element.type === 'struct') {
            traverse(field.type.element.fields);
          }
        } else if (field.type.type === 'map') {
          ids.add(field.type['key-id']);
          ids.add(field.type['value-id']);
          if (typeof field.type.key === 'object' && field.type.key.type === 'struct') {
            traverse(field.type.key.fields);
          }
          if (typeof field.type.value === 'object' && field.type.value.type === 'struct') {
            traverse(field.type.value.fields);
          }
        }
      }
    }
  }

  traverse(schema.fields);
  return ids;
}

// =============================================================================
// Schema Evolution Helper Functions
// =============================================================================

/**
 * Create a schema evolution builder for a table's current schema.
 *
 * @param metadata - Table metadata
 * @returns A new SchemaEvolutionBuilder
 */
export function evolveSchema(metadata: TableMetadata): SchemaEvolutionBuilder {
  const currentSchema = metadata.schemas.find(
    (s) => s['schema-id'] === metadata['current-schema-id']
  );
  if (!currentSchema) {
    throw new Error(`Current schema with ID ${metadata['current-schema-id']} not found`);
  }
  return new SchemaEvolutionBuilder(currentSchema, metadata);
}

/**
 * Apply schema evolution operations to metadata.
 *
 * @param metadata - Table metadata
 * @param builder - Schema evolution builder with pending operations
 * @returns Updated table metadata
 */
export function applySchemaEvolution(
  _metadata: TableMetadata,
  builder: SchemaEvolutionBuilder
): TableMetadata {
  const result = builder.buildWithMetadata();
  return result.metadata;
}

// =============================================================================
// Nested Field Evolution
// =============================================================================

/**
 * Create a builder for evolving nested struct fields.
 *
 * @param schema - The parent schema
 * @param path - Path to the nested struct (array of field names)
 * @returns A SchemaEvolutionBuilder for the nested struct
 */
export function evolveNestedStruct(
  schema: IcebergSchema,
  path: string[]
): SchemaEvolutionBuilder {
  // Navigate to the nested struct
  let currentFields: readonly IcebergStructField[] = schema.fields;
  let targetField: IcebergStructField | undefined;

  for (let i = 0; i < path.length; i++) {
    targetField = currentFields.find((f) => f.name === path[i]);
    if (!targetField) {
      throw new Error(`Field '${path[i]}' not found in path`);
    }

    if (i < path.length - 1) {
      if (typeof targetField.type !== 'object' || targetField.type.type !== 'struct') {
        throw new Error(`Field '${path[i]}' is not a struct type`);
      }
      currentFields = targetField.type.fields;
    }
  }

  if (!targetField || typeof targetField.type !== 'object' || targetField.type.type !== 'struct') {
    throw new Error('Target field is not a struct type');
  }

  // Create a temporary schema from the nested struct
  const nestedSchema: IcebergSchema = {
    'schema-id': schema['schema-id'],
    type: 'struct',
    fields: targetField.type.fields,
  };

  return new SchemaEvolutionBuilder(nestedSchema);
}

// =============================================================================
// Field ID Management
// =============================================================================

/**
 * Manages monotonically increasing field IDs for a table.
 */
export class FieldIdManager {
  private nextId: number;

  /**
   * Create a new field ID manager.
   *
   * @param startingId - The starting ID (typically last-column-id from metadata)
   */
  constructor(startingId: number) {
    this.nextId = startingId;
  }

  /**
   * Create from table metadata.
   */
  static fromMetadata(metadata: TableMetadata): FieldIdManager {
    return new FieldIdManager(metadata['last-column-id']);
  }

  /**
   * Get the next field ID and increment the counter.
   */
  getNextId(): number {
    return ++this.nextId;
  }

  /**
   * Get the current next ID without incrementing.
   */
  peekNextId(): number {
    return this.nextId + 1;
  }

  /**
   * Get the last assigned ID.
   */
  getLastId(): number {
    return this.nextId;
  }

  /**
   * Reserve multiple IDs at once (for complex types with multiple field IDs).
   *
   * @param count - Number of IDs to reserve
   * @returns Array of reserved IDs
   */
  reserveIds(count: number): number[] {
    const ids: number[] = [];
    for (let i = 0; i < count; i++) {
      ids.push(++this.nextId);
    }
    return ids;
  }
}
