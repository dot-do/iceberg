/**
 * Iceberg Schema Utilities
 *
 * Schema creation, conversion, and evolution utilities.
 *
 * @see https://iceberg.apache.org/spec/#schema-evolution
 */

import type {
  IcebergSchema,
  IcebergStructField,
  IcebergType,
  PartitionSpec,
  SortOrder,
} from './types.js';
import { PARTITION_FIELD_ID_START } from './constants.js';

// ============================================================================
// Default Schema Creation
// ============================================================================

/**
 * Create the default Iceberg schema with _id, _seq, _op, _data columns.
 * This is a generic schema suitable for document-style data.
 */
export function createDefaultSchema(): IcebergSchema {
  return {
    'schema-id': 0,
    type: 'struct',
    fields: [
      {
        id: 1,
        name: '_id',
        required: true,
        type: 'string',
      },
      {
        id: 2,
        name: '_seq',
        required: true,
        type: 'long',
      },
      {
        id: 3,
        name: '_op',
        required: true,
        type: 'string',
      },
      {
        id: 4,
        name: '_data',
        required: true,
        type: 'binary', // Variant-encoded document
      },
    ],
  };
}

// ============================================================================
// Partition Spec Creation
// ============================================================================

/**
 * Create a default (unpartitioned) partition spec.
 */
export function createUnpartitionedSpec(): PartitionSpec {
  return {
    'spec-id': 0,
    fields: [],
  };
}

/**
 * Create a partition spec with a single identity transform.
 */
export function createIdentityPartitionSpec(
  sourceFieldId: number,
  fieldName: string,
  specId: number = 0
): PartitionSpec {
  return {
    'spec-id': specId,
    fields: [
      {
        'source-id': sourceFieldId,
        'field-id': PARTITION_FIELD_ID_START + sourceFieldId, // Partition field IDs start at PARTITION_FIELD_ID_START
        name: fieldName,
        transform: 'identity',
      },
    ],
  };
}

/**
 * Create a bucket partition spec.
 */
export function createBucketPartitionSpec(
  sourceFieldId: number,
  fieldName: string,
  numBuckets: number,
  specId: number = 0
): PartitionSpec {
  return {
    'spec-id': specId,
    fields: [
      {
        'source-id': sourceFieldId,
        'field-id': PARTITION_FIELD_ID_START + sourceFieldId,
        name: fieldName,
        transform: `bucket[${numBuckets}]`,
      },
    ],
  };
}

/**
 * Create a time-based partition spec (year, month, day, hour).
 */
export function createTimePartitionSpec(
  sourceFieldId: number,
  fieldName: string,
  transform: 'year' | 'month' | 'day' | 'hour',
  specId: number = 0
): PartitionSpec {
  return {
    'spec-id': specId,
    fields: [
      {
        'source-id': sourceFieldId,
        'field-id': PARTITION_FIELD_ID_START + sourceFieldId,
        name: fieldName,
        transform,
      },
    ],
  };
}

// ============================================================================
// Sort Order Creation
// ============================================================================

/**
 * Create an unsorted sort order.
 */
export function createUnsortedOrder(): SortOrder {
  return {
    'order-id': 0,
    fields: [],
  };
}

/**
 * Create a sort order on a single field.
 */
export function createSortOrder(
  sourceFieldId: number,
  direction: 'asc' | 'desc' = 'asc',
  nullOrder: 'nulls-first' | 'nulls-last' = 'nulls-first',
  orderId: number = 1
): SortOrder {
  return {
    'order-id': orderId,
    fields: [
      {
        'source-id': sourceFieldId,
        transform: 'identity',
        direction,
        'null-order': nullOrder,
      },
    ],
  };
}

// ============================================================================
// Schema Conversion (Parquet -> Iceberg)
// ============================================================================

/** Parquet type names (for conversion) */
export type ParquetTypeName =
  | 'BOOLEAN'
  | 'INT32'
  | 'INT64'
  | 'INT96'
  | 'FLOAT'
  | 'DOUBLE'
  | 'BYTE_ARRAY'
  | 'FIXED_LEN_BYTE_ARRAY';

/** Parquet converted type names */
export type ParquetConvertedType =
  | 'UTF8'
  | 'DATE'
  | 'TIMESTAMP_MILLIS'
  | 'TIMESTAMP_MICROS'
  | 'INT_8'
  | 'INT_16'
  | 'INT_32'
  | 'UINT_8'
  | 'UINT_16'
  | 'UINT_32'
  | 'INT_64'
  | 'UINT_64'
  | 'DECIMAL'
  | 'JSON'
  | 'BSON';

/** Parquet schema element (simplified) */
export interface ParquetSchemaElement {
  name: string;
  type?: ParquetTypeName;
  convertedType?: ParquetConvertedType;
  repetitionType?: 'REQUIRED' | 'OPTIONAL' | 'REPEATED';
  children?: ParquetSchemaElement[];
}

/**
 * Convert Parquet schema to Iceberg schema format.
 */
export function parquetToIcebergType(
  parquetType: ParquetTypeName,
  convertedType?: ParquetConvertedType
): IcebergType {
  // Handle converted types first
  if (convertedType) {
    switch (convertedType) {
      case 'UTF8':
        return 'string';
      case 'DATE':
        return 'date';
      case 'TIMESTAMP_MILLIS':
      case 'TIMESTAMP_MICROS':
        return 'timestamp';
      case 'INT_8':
      case 'INT_16':
      case 'INT_32':
      case 'UINT_8':
      case 'UINT_16':
      case 'UINT_32':
        return 'int';
      case 'INT_64':
      case 'UINT_64':
        return 'long';
      case 'DECIMAL':
        return 'decimal';
      case 'JSON':
      case 'BSON':
        return 'string'; // Store as string for JSON/BSON
    }
  }

  // Handle physical types
  switch (parquetType) {
    case 'BOOLEAN':
      return 'boolean';
    case 'INT32':
      return 'int';
    case 'INT64':
      return 'long';
    case 'INT96':
      return 'timestamp'; // INT96 is typically timestamp
    case 'FLOAT':
      return 'float';
    case 'DOUBLE':
      return 'double';
    case 'BYTE_ARRAY':
      return 'binary';
    case 'FIXED_LEN_BYTE_ARRAY':
      return 'fixed';
    default:
      return 'binary'; // Fallback
  }
}

/**
 * Convert a Parquet SchemaElement array to Iceberg schema.
 */
export function parquetSchemaToIceberg(
  parquetSchema: ParquetSchemaElement[],
  startFieldId: number = 1
): IcebergSchema {
  let nextFieldId = startFieldId;
  const fields: IcebergStructField[] = [];

  // Skip the root "schema" element if present
  const elements = parquetSchema[0]?.name === 'schema' ? parquetSchema.slice(1) : parquetSchema;

  function convertElement(element: ParquetSchemaElement): IcebergStructField {
    const fieldId = nextFieldId++;
    const required = element.repetitionType === 'REQUIRED';

    // Handle nested structures
    if (element.children && element.children.length > 0) {
      const childFields = element.children.map(convertElement);
      return {
        id: fieldId,
        name: element.name,
        required,
        type: {
          type: 'struct',
          fields: childFields,
        },
      };
    }

    // Handle primitive types
    const icebergType = parquetToIcebergType(
      element.type || 'BYTE_ARRAY',
      element.convertedType
    );

    return {
      id: fieldId,
      name: element.name,
      required,
      type: icebergType,
    };
  }

  for (const element of elements) {
    fields.push(convertElement(element));
  }

  return {
    'schema-id': 0,
    type: 'struct',
    fields,
  };
}

// ============================================================================
// Schema Evolution
// ============================================================================

/** Types of schema changes that can occur */
export type SchemaChangeType =
  | 'add-field'
  | 'remove-field'
  | 'make-optional'
  | 'rename-field'
  | 'update-doc'
  | 'widen-type';

/** A single schema change */
export interface SchemaChange {
  /** The type of change */
  type: SchemaChangeType;
  /** Field ID affected */
  fieldId: number;
  /** Field name (for add/rename operations) */
  fieldName?: string;
  /** Previous field name (for rename operations) */
  previousName?: string;
  /** Parent field ID (for nested fields, -1 for root) */
  parentFieldId: number;
  /** The new type (for add/widen operations) */
  newType?: IcebergType;
  /** The previous type (for widen operations) */
  previousType?: IcebergType;
  /** Whether the field is required */
  required?: boolean;
  /** Documentation string */
  doc?: string;
  /** Timestamp when the change occurred */
  timestampMs: number;
  /** Snapshot ID where this change was introduced */
  snapshotId?: number;
}

/** Result of schema comparison */
export interface SchemaComparisonResult {
  /** Whether the schemas are compatible (new schema can read old data) */
  compatible: boolean;
  /** List of changes between schemas */
  changes: SchemaChange[];
  /** Breaking changes that prevent compatibility */
  breakingChanges: SchemaChange[];
}

/**
 * Validate that a schema evolution is backwards compatible.
 */
export function validateSchemaEvolution(
  oldSchema: IcebergSchema,
  newSchema: IcebergSchema
): SchemaComparisonResult {
  const changes: SchemaChange[] = [];
  const breakingChanges: SchemaChange[] = [];

  const oldFields = flattenFields(oldSchema.fields);
  const newFields = flattenFields(newSchema.fields);

  const oldFieldIds = new Set(oldFields.map((f) => f.field.id));
  const newFieldIds = new Set(newFields.map((f) => f.field.id));

  // Check for removed fields
  for (const { field, parentId } of oldFields) {
    if (!newFieldIds.has(field.id)) {
      const change: SchemaChange = {
        type: 'remove-field',
        fieldId: field.id,
        fieldName: field.name,
        parentFieldId: parentId,
        previousType: field.type,
        timestampMs: Date.now(),
      };
      changes.push(change);

      // Removing a required field is a breaking change
      if (field.required) {
        breakingChanges.push(change);
      }
    }
  }

  // Check for added fields
  for (const { field, parentId } of newFields) {
    if (!oldFieldIds.has(field.id)) {
      const change: SchemaChange = {
        type: 'add-field',
        fieldId: field.id,
        fieldName: field.name,
        parentFieldId: parentId,
        newType: field.type,
        required: field.required,
        doc: field.doc,
        timestampMs: Date.now(),
      };
      changes.push(change);

      // Adding a required field is a breaking change
      if (field.required) {
        breakingChanges.push(change);
      }
    }
  }

  // Check for modified fields
  for (const { field: oldField, parentId } of oldFields) {
    const newFieldEntry = newFields.find((f) => f.field.id === oldField.id);
    if (!newFieldEntry) continue;

    const newField = newFieldEntry.field;

    // Check for name change
    if (oldField.name !== newField.name) {
      changes.push({
        type: 'rename-field',
        fieldId: oldField.id,
        fieldName: newField.name,
        previousName: oldField.name,
        parentFieldId: parentId,
        timestampMs: Date.now(),
      });
    }

    // Check for required -> optional change
    if (oldField.required && !newField.required) {
      changes.push({
        type: 'make-optional',
        fieldId: oldField.id,
        fieldName: newField.name,
        parentFieldId: parentId,
        required: false,
        timestampMs: Date.now(),
      });
    }

    // Check for optional -> required change (breaking)
    if (!oldField.required && newField.required) {
      const change: SchemaChange = {
        type: 'add-field', // Treated as adding a required constraint
        fieldId: oldField.id,
        fieldName: newField.name,
        parentFieldId: parentId,
        required: true,
        timestampMs: Date.now(),
      };
      changes.push(change);
      breakingChanges.push(change);
    }

    // Check for type widening
    if (isTypeWidened(oldField.type, newField.type)) {
      changes.push({
        type: 'widen-type',
        fieldId: oldField.id,
        fieldName: newField.name,
        parentFieldId: parentId,
        previousType: oldField.type,
        newType: newField.type,
        timestampMs: Date.now(),
      });
    }
  }

  return {
    compatible: breakingChanges.length === 0,
    changes,
    breakingChanges,
  };
}

/**
 * Generate a new schema ID based on existing schemas.
 */
export function generateSchemaId(existingSchemas: IcebergSchema[]): number {
  if (existingSchemas.length === 0) {
    return 0;
  }
  const maxId = Math.max(...existingSchemas.map((s) => s['schema-id']));
  return maxId + 1;
}

/**
 * Find the maximum field ID in a schema.
 */
export function findMaxFieldId(schema: IcebergSchema): number {
  let maxId = 0;

  function traverse(fields: readonly IcebergStructField[]): void {
    for (const field of fields) {
      maxId = Math.max(maxId, field.id);
      if (typeof field.type === 'object' && field.type !== null) {
        const complexType = field.type as {
          type: string;
          fields?: readonly IcebergStructField[];
          'element-id'?: number;
          'key-id'?: number;
          'value-id'?: number;
        };
        if (complexType.type === 'struct' && complexType.fields) {
          traverse(complexType.fields);
        }
        if (complexType['element-id']) {
          maxId = Math.max(maxId, complexType['element-id']);
        }
        if (complexType['key-id']) {
          maxId = Math.max(maxId, complexType['key-id']);
        }
        if (complexType['value-id']) {
          maxId = Math.max(maxId, complexType['value-id']);
        }
      }
    }
  }

  traverse(schema.fields);
  return maxId;
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Flatten a nested field structure with parent tracking.
 */
function flattenFields(
  fields: readonly IcebergStructField[],
  parentId: number = -1
): Array<{ field: IcebergStructField; parentId: number }> {
  const result: Array<{ field: IcebergStructField; parentId: number }> = [];

  for (const field of fields) {
    result.push({ field, parentId });

    if (typeof field.type === 'object' && field.type.type === 'struct') {
      const nested = flattenFields(
        (field.type as { type: 'struct'; fields: readonly IcebergStructField[] }).fields,
        field.id
      );
      result.push(...nested);
    }
  }

  return result;
}

/**
 * Check if a type change represents type widening.
 */
function isTypeWidened(oldType: IcebergType, newType: IcebergType): boolean {
  // Only primitive types can be widened
  if (typeof oldType !== 'string' || typeof newType !== 'string') {
    return false;
  }

  // int -> long is allowed
  if (oldType === 'int' && newType === 'long') {
    return true;
  }

  // float -> double is allowed
  if (oldType === 'float' && newType === 'double') {
    return true;
  }

  return false;
}

// ============================================================================
// Unknown Type Validation (Iceberg v3)
// ============================================================================

/** Validation result for field or schema validation */
export interface FieldValidationResult {
  /** Whether the validation passed */
  valid: boolean;
  /** List of validation errors */
  errors: string[];
}

/**
 * Validate a field with 'unknown' type.
 *
 * Per the Iceberg v3 spec, unknown type fields:
 * - MUST be optional (required: false)
 * - Always have null values (not stored in data files)
 *
 * @param field - The struct field to validate
 * @returns Validation result with any errors
 */
export function validateUnknownTypeField(field: IcebergStructField): FieldValidationResult {
  const errors: string[] = [];

  // Only validate if the type is 'unknown'
  if (field.type === 'unknown') {
    // Unknown type fields MUST be optional
    if (field.required === true) {
      errors.push(
        `Field '${field.name}' with type 'unknown' must be optional (required: false)`
      );
    }
  }

  return {
    valid: errors.length === 0,
    errors,
  };
}

/**
 * Validate all fields in a schema, including nested struct fields.
 *
 * Validates:
 * - Unknown type fields must be optional
 * - Default values for special types (unknown, variant, geometry, geography)
 * - Nested struct fields are validated recursively
 *
 * @param schema - The schema to validate
 * @returns Validation result with any errors
 */
export function validateSchema(schema: IcebergSchema): FieldValidationResult {
  const errors: string[] = [];

  function validateFields(fields: readonly IcebergStructField[]): void {
    for (const field of fields) {
      // Validate unknown type fields
      const unknownResult = validateUnknownTypeField(field);
      if (!unknownResult.valid) {
        errors.push(...unknownResult.errors);
      }

      // Validate default values
      const defaultResult = validateFieldDefault(field);
      if (!defaultResult.valid) {
        errors.push(...defaultResult.errors);
      }

      // Recursively validate nested struct fields
      if (typeof field.type === 'object' && field.type.type === 'struct') {
        validateFields(field.type.fields);
      }
    }
  }

  validateFields(schema.fields);

  return {
    valid: errors.length === 0,
    errors,
  };
}

// ============================================================================
// Default Values Validation (Iceberg v3)
// ============================================================================

import { isGeospatialType } from './types.js';

/** Options for default value validation */
export interface FieldDefaultValidationOptions {
  /**
   * Whether this field is being added to an existing table.
   * Required fields being added must have an initial-default.
   */
  isNewField?: boolean;
}

/**
 * Check if a type requires null default (unknown, variant, geometry, geography).
 */
function requiresNullDefault(type: IcebergType): boolean {
  if (typeof type === 'string') {
    if (type === 'unknown' || type === 'variant') {
      return true;
    }
    // Check for geospatial types (geometry and geography, including parameterized forms)
    if (isGeospatialType(type)) {
      return true;
    }
  }
  return false;
}

/**
 * Check if a default value is an empty object (for struct types).
 */
function isEmptyObject(value: unknown): boolean {
  return (
    typeof value === 'object' &&
    value !== null &&
    !Array.isArray(value) &&
    Object.keys(value).length === 0
  );
}

/**
 * Validate default value for a field.
 *
 * Per the Iceberg spec:
 * - Fields with type unknown, variant, geometry, or geography MUST have null defaults
 * - Struct defaults must be empty {} or null (sub-field defaults tracked separately)
 * - Required fields being added to an existing table must have initial-default
 *
 * @param field - The struct field to validate
 * @param options - Validation options
 * @returns Validation result with any errors
 */
export function validateFieldDefault(
  field: IcebergStructField,
  options: FieldDefaultValidationOptions = {}
): FieldValidationResult {
  const errors: string[] = [];
  const { isNewField = false } = options;

  const hasInitialDefault = 'initial-default' in field;
  const hasWriteDefault = 'write-default' in field;
  const initialDefault = field['initial-default'];
  const writeDefault = field['write-default'];

  // Check types that require null defaults
  if (requiresNullDefault(field.type)) {
    const typeName =
      typeof field.type === 'string' ? field.type : (field.type as { type: string }).type;

    if (hasInitialDefault && initialDefault !== null) {
      errors.push(`Field '${field.name}' with type '${typeName}' must have null default`);
    }
    if (hasWriteDefault && writeDefault !== null) {
      errors.push(`Field '${field.name}' with type '${typeName}' must have null default`);
    }
  }

  // Check struct type defaults (must be {} or null)
  if (typeof field.type === 'object' && field.type.type === 'struct') {
    if (hasInitialDefault && initialDefault !== null && !isEmptyObject(initialDefault)) {
      errors.push(`Field '${field.name}' with struct type must have empty {} or null as default`);
    }
    if (hasWriteDefault && writeDefault !== null && !isEmptyObject(writeDefault)) {
      errors.push(`Field '${field.name}' with struct type must have empty {} or null as default`);
    }
  }

  // Check required field defaults when adding to existing table
  if (isNewField && field.required) {
    if (!hasInitialDefault) {
      errors.push(
        `Required field '${field.name}' must have initial-default when adding to existing table`
      );
    } else if (initialDefault === null) {
      errors.push(
        `Required field '${field.name}' cannot have null as initial-default when adding to existing table`
      );
    }
  }

  return {
    valid: errors.length === 0,
    errors,
  };
}

/** Result of checking if a default value can be changed */
export interface DefaultChangeResult {
  /** Whether the change is allowed */
  allowed: boolean;
  /** Reason if not allowed */
  reason?: string;
}

/**
 * Check if initial-default can be changed between schema versions.
 *
 * Per the Iceberg spec, initial-default cannot be changed once set.
 *
 * @param oldField - The old field definition
 * @param newField - The new field definition
 * @returns Whether the change is allowed
 */
export function canChangeInitialDefault(
  oldField: IcebergStructField,
  newField: IcebergStructField
): DefaultChangeResult {
  const hadInitialDefault = 'initial-default' in oldField;
  const hasInitialDefault = 'initial-default' in newField;

  // If old field didn't have initial-default, can set it now
  if (!hadInitialDefault) {
    return { allowed: true };
  }

  // If old field had initial-default but new doesn't, that's an error
  // (initial-default cannot be removed once set)
  if (hadInitialDefault && !hasInitialDefault) {
    return { allowed: false, reason: 'initial-default cannot be removed once set' };
  }

  // If both have initial-default, they must be the same
  const oldDefault = oldField['initial-default'];
  const newDefault = newField['initial-default'];

  if (JSON.stringify(oldDefault) !== JSON.stringify(newDefault)) {
    return { allowed: false, reason: 'initial-default cannot be changed once set' };
  }

  return { allowed: true };
}

/**
 * Check if write-default can be changed between schema versions.
 *
 * Per the Iceberg spec, write-default can be changed through schema evolution.
 *
 * @param _oldField - The old field definition (unused, write-default can always change)
 * @param _newField - The new field definition (unused)
 * @returns Whether the change is allowed (always true)
 */
export function canChangeWriteDefault(
  _oldField: IcebergStructField,
  _newField: IcebergStructField
): DefaultChangeResult {
  // write-default can always be changed, added, or removed
  return { allowed: true };
}
