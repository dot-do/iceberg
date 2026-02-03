/**
 * Variant Shredding Configuration
 *
 * Defines table property keys and utilities for configuring variant shredding.
 * Variant shredding allows decomposing variant columns into typed sub-columns
 * for better query performance and storage efficiency.
 *
 * @see https://iceberg.apache.org/spec/#variant-shredding
 */

import type { IcebergPrimitiveType } from '../metadata/types.js';

// ============================================================================
// Property Key Constants
// ============================================================================

/**
 * Table property key that lists which variant columns should be shredded.
 * Value is a comma-separated list of column names.
 *
 * @example
 * ```
 * properties['write.variant.shred-columns'] = '$data,$index'
 * ```
 */
export const VARIANT_SHRED_COLUMNS_KEY = 'write.variant.shred-columns';

/**
 * Prefix for variant column-specific properties.
 * Combined with column name and suffix to form full property keys.
 */
export const VARIANT_SHRED_FIELDS_KEY_PREFIX = 'write.variant.';

/**
 * Suffix for the shred-fields property of a specific column.
 * Lists which fields within the variant to shred.
 *
 * @example
 * Full key: 'write.variant.$data.shred-fields' = 'title,year,rating'
 */
export const VARIANT_SHRED_FIELDS_KEY_SUFFIX = '.shred-fields';

/**
 * Suffix for the field-types property of a specific column.
 * Maps field names to their expected Iceberg primitive types.
 *
 * @example
 * Full key: 'write.variant.$data.field-types' = 'title:string,year:int'
 */
export const VARIANT_FIELD_TYPES_KEY_SUFFIX = '.field-types';

// ============================================================================
// Types
// ============================================================================

/**
 * Configuration for shredding a single variant column stored in table properties.
 *
 * This type is used for serializing/deserializing shred configuration
 * to/from Iceberg table properties.
 */
export interface VariantShredPropertyConfig {
  /** Name of the variant column to shred */
  readonly columnName: string;
  /** List of field paths to extract from the variant */
  readonly fields: readonly string[];
  /** Optional type hints for extracted fields */
  readonly fieldTypes: Readonly<Record<string, IcebergPrimitiveType>>;
}

/**
 * @deprecated Use VariantShredPropertyConfig instead
 */
export type VariantShredConfig = VariantShredPropertyConfig;

// ============================================================================
// Key Generation Functions
// ============================================================================

/**
 * Generate the property key for shred-fields of a specific column.
 *
 * @param columnName - Name of the variant column
 * @returns Property key like 'write.variant.$data.shred-fields'
 */
export function getShredFieldsKey(columnName: string): string {
  return `${VARIANT_SHRED_FIELDS_KEY_PREFIX}${columnName}${VARIANT_SHRED_FIELDS_KEY_SUFFIX}`;
}

/**
 * Generate the property key for field-types of a specific column.
 *
 * @param columnName - Name of the variant column
 * @returns Property key like 'write.variant.$data.field-types'
 */
export function getFieldTypesKey(columnName: string): string {
  return `${VARIANT_SHRED_FIELDS_KEY_PREFIX}${columnName}${VARIANT_FIELD_TYPES_KEY_SUFFIX}`;
}

// ============================================================================
// Parsing Functions
// ============================================================================

/**
 * Parse the shred-columns property value into an array of column names.
 *
 * @param value - Comma-separated column names or undefined
 * @returns Array of column names (empty array if value is empty/undefined)
 */
export function parseShredColumnsProperty(value: string | undefined): string[] {
  if (!value || value.trim() === '') {
    return [];
  }
  return value
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

/**
 * Parse a shred-fields property value into an array of field names.
 *
 * @param value - Comma-separated field names or undefined
 * @returns Array of field names (empty array if value is empty/undefined)
 */
export function parseShredFieldsProperty(value: string | undefined): string[] {
  if (!value || value.trim() === '') {
    return [];
  }
  return value
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

/**
 * Valid Iceberg primitive types for field type validation.
 */
const VALID_PRIMITIVE_TYPES: ReadonlySet<string> = new Set<IcebergPrimitiveType>([
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
  'unknown',
]);

/**
 * Parse a field-types property value into a record of field names to types.
 *
 * @param value - Comma-separated "field:type" pairs or undefined
 * @returns Record mapping field names to Iceberg primitive types
 */
export function parseFieldTypesProperty(
  value: string | undefined
): Record<string, IcebergPrimitiveType> {
  if (!value || value.trim() === '') {
    return {};
  }

  const result: Record<string, IcebergPrimitiveType> = {};
  const pairs = value.split(',');

  for (const pair of pairs) {
    const trimmed = pair.trim();
    if (!trimmed) continue;

    const colonIndex = trimmed.lastIndexOf(':');
    if (colonIndex === -1) continue;

    const fieldName = trimmed.substring(0, colonIndex).trim();
    const typeName = trimmed.substring(colonIndex + 1).trim() as IcebergPrimitiveType;

    if (fieldName && typeName) {
      result[fieldName] = typeName;
    }
  }

  return result;
}

/**
 * Extract variant shred configurations from table properties.
 *
 * Reads Iceberg table properties to reconstruct the variant shredding
 * configuration. This is typically used when opening an existing table.
 *
 * @param properties - Table properties record
 * @returns Array of VariantShredConfig for each configured variant column
 *
 * @example
 * ```ts
 * // Read shredding config from existing table properties
 * const tableMetadata = await catalog.loadTable('db.movies');
 * const configs = extractVariantShredConfig(tableMetadata.properties);
 *
 * // configs[0] = {
 * //   columnName: '$data',
 * //   fields: ['title', 'year'],
 * //   fieldTypes: { title: 'string', year: 'int' }
 * // }
 * ```
 */
export function extractVariantShredConfig(
  properties: Record<string, string>
): VariantShredConfig[] {
  const columnsValue = properties[VARIANT_SHRED_COLUMNS_KEY];
  const columns = parseShredColumnsProperty(columnsValue);

  if (columns.length === 0) {
    return [];
  }

  const configs: VariantShredConfig[] = [];

  for (const columnName of columns) {
    const fieldsKey = getShredFieldsKey(columnName);
    const typesKey = getFieldTypesKey(columnName);

    const fields = parseShredFieldsProperty(properties[fieldsKey]);
    const fieldTypes = parseFieldTypesProperty(properties[typesKey]);

    configs.push({
      columnName,
      fields,
      fieldTypes,
    });
  }

  return configs;
}

// ============================================================================
// Serialization Functions
// ============================================================================

/**
 * Format an array of configs into the shred-columns property value.
 *
 * @param configs - Array of variant shred configurations
 * @returns Comma-separated column names
 */
export function formatShredColumnsProperty(configs: readonly VariantShredConfig[]): string {
  return configs.map((c) => c.columnName).join(',');
}

/**
 * Format an array of field names into a shred-fields property value.
 *
 * @param fields - Array of field names
 * @returns Comma-separated field names
 */
export function formatShredFieldsProperty(fields: readonly string[]): string {
  return fields.join(',');
}

/**
 * Format a field types record into a field-types property value.
 *
 * @param fieldTypes - Record of field names to types
 * @returns Comma-separated "field:type" pairs
 */
export function formatFieldTypesProperty(
  fieldTypes: Readonly<Record<string, IcebergPrimitiveType>>
): string {
  const entries = Object.entries(fieldTypes);
  if (entries.length === 0) {
    return '';
  }
  return entries.map(([field, type]) => `${field}:${type}`).join(',');
}

/**
 * Convert variant shred configurations to table properties.
 *
 * Serializes shredding configurations to Iceberg table property format.
 * These properties should be included when creating or updating table metadata.
 *
 * @param configs - Array of variant shred configurations
 * @returns Record of table property key-value pairs
 *
 * @example
 * ```ts
 * const config: VariantShredPropertyConfig = {
 *   columnName: '$data',
 *   fields: ['title', 'year', 'rating'],
 *   fieldTypes: { title: 'string', year: 'int', rating: 'double' }
 * };
 *
 * const properties = toTableProperties([config]);
 * // {
 * //   'write.variant.shred-columns': '$data',
 * //   'write.variant.$data.shred-fields': 'title,year,rating',
 * //   'write.variant.$data.field-types': 'title:string,year:int,rating:double'
 * // }
 *
 * // Use in table creation
 * await catalog.createTable({
 *   name: 'movies',
 *   schema: tableSchema,
 *   properties: { ...otherProperties, ...properties }
 * });
 * ```
 */
export function toTableProperties(
  configs: readonly VariantShredConfig[]
): Record<string, string> {
  if (configs.length === 0) {
    return {};
  }

  const properties: Record<string, string> = {};

  // Set the shred-columns property
  properties[VARIANT_SHRED_COLUMNS_KEY] = formatShredColumnsProperty(configs);

  // Set per-column properties
  for (const config of configs) {
    const fieldsKey = getShredFieldsKey(config.columnName);
    properties[fieldsKey] = formatShredFieldsProperty(config.fields);

    // Only include field-types if non-empty
    const fieldTypesValue = formatFieldTypesProperty(config.fieldTypes);
    if (fieldTypesValue) {
      const typesKey = getFieldTypesKey(config.columnName);
      properties[typesKey] = fieldTypesValue;
    }
  }

  return properties;
}

// ============================================================================
// Validation Types
// ============================================================================

/**
 * Result of validating a shred configuration.
 */
export interface ShredConfigValidationResult {
  /** Whether the configuration is valid */
  readonly valid: boolean;
  /** List of validation errors (empty if valid) */
  readonly errors: readonly string[];
}

// ============================================================================
// Validation Functions
// ============================================================================

/**
 * Validate a variant shred configuration.
 *
 * Returns a validation result object instead of throwing errors,
 * allowing callers to handle validation failures as they see fit.
 *
 * @param config - Configuration to validate
 * @returns Validation result with valid flag and any errors
 *
 * @example
 * ```ts
 * const result = validateShredConfig(config);
 * if (!result.valid) {
 *   console.error('Validation errors:', result.errors);
 * }
 * ```
 */
export function validateShredConfig(config: VariantShredConfig): ShredConfigValidationResult {
  const errors: string[] = [];

  // Validate column name
  if (!config.columnName || config.columnName.trim() === '') {
    errors.push('Column name is required');
  }

  // Validate fields array
  if (!config.fields || config.fields.length === 0) {
    errors.push(
      config.columnName
        ? `At least one field is required for column '${config.columnName}'`
        : 'At least one field is required'
    );
  }

  // Create a set of declared field names for validation
  const declaredFields = new Set(config.fields);

  // Validate field types
  for (const [fieldName, typeName] of Object.entries(config.fieldTypes)) {
    // Check that field type is valid
    if (!VALID_PRIMITIVE_TYPES.has(typeName)) {
      errors.push(
        `Invalid field type '${typeName}' for field '${fieldName}'${config.columnName ? ` in column '${config.columnName}'` : ''}`
      );
    }

    // Check that field type references an existing field
    if (!declaredFields.has(fieldName)) {
      errors.push(
        `Field type declared for field '${fieldName}' which is not declared in fields array${config.columnName ? ` for column '${config.columnName}'` : ''}`
      );
    }
  }

  return {
    valid: errors.length === 0,
    errors,
  };
}
