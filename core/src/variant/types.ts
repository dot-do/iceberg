/**
 * Variant Shredding Schema Types
 *
 * Variant shredding is an Iceberg feature that allows semi-structured variant
 * data to be "shredded" (decomposed) into separate columns for efficient
 * querying and storage. These types define the schema representation for
 * shredded variant fields.
 *
 * @see https://iceberg.apache.org/spec/#variant-shredding
 */

import type { IcebergPrimitiveType } from '../metadata/types.js';

// ============================================================================
// Core Types
// ============================================================================

/**
 * Information about a single shredded field from a variant column.
 *
 * When variant data is shredded, specific fields are extracted and stored
 * in separate columns. This interface describes one such extracted field.
 *
 * @example
 * ```ts
 * const fieldInfo: ShreddedFieldInfo = {
 *   path: 'titleType',
 *   type: 'string',
 *   statisticsPath: '$data.typed_value.titleType.typed_value',
 *   nullable: true,
 * };
 * ```
 */
export interface ShreddedFieldInfo {
  /** The field path/name within the variant (e.g., "titleType") */
  readonly path: string;
  /** The Iceberg primitive type of the shredded field */
  readonly type: IcebergPrimitiveType;
  /** The path used for column statistics (e.g., "$data.typed_value.titleType.typed_value") */
  readonly statisticsPath: string;
  /** Whether the field can contain null values (optional, defaults to true if not specified) */
  readonly nullable?: boolean;
}

/**
 * Schema definition for a variant column with shredded fields.
 *
 * Defines the structure of a variant column including the paths to its
 * metadata, value, typed_value components, and the list of shredded fields.
 *
 * @example
 * ```ts
 * const schema: VariantColumnSchema = {
 *   columnName: '$data',
 *   metadataPath: '$data.metadata',
 *   valuePath: '$data.value',
 *   typedValuePath: '$data.typed_value',
 *   shreddedFields: [
 *     { path: 'titleType', type: 'string', statisticsPath: '...' },
 *   ],
 * };
 * ```
 */
export interface VariantColumnSchema {
  /** The variant column name (e.g., "$data") */
  readonly columnName: string;
  /** Path to the variant metadata (e.g., "$data.metadata") */
  readonly metadataPath: string;
  /** Path to the variant value (e.g., "$data.value") */
  readonly valuePath: string;
  /** Path to the typed value container (e.g., "$data.typed_value") */
  readonly typedValuePath: string;
  /** List of shredded field definitions */
  readonly shreddedFields: readonly ShreddedFieldInfo[];
}

/**
 * Configuration for shredding a variant column.
 *
 * Specifies which column to shred and which fields to extract.
 *
 * @example
 * ```ts
 * const config: VariantShredConfig = {
 *   column: 'data',
 *   fields: ['titleType', 'releaseYear'],
 *   fieldTypes: {
 *     titleType: 'string',
 *     releaseYear: 'int',
 *   },
 * };
 * ```
 */
export interface VariantShredConfig {
  /** The column name to shred */
  readonly column: string;
  /** The field names to extract from the variant */
  readonly fields: readonly string[];
  /** Optional explicit type mapping for fields */
  readonly fieldTypes?: Record<string, IcebergPrimitiveType>;
}

// ============================================================================
// Validation Result Type
// ============================================================================

/**
 * Result of validating a variant shred configuration.
 */
export interface VariantShredConfigValidationResult {
  /** Whether the configuration is valid */
  readonly valid: boolean;
  /** List of validation errors (empty if valid) */
  readonly errors: readonly string[];
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Create a ShreddedFieldInfo object for a field.
 *
 * @param columnName - The variant column name (e.g., "$data")
 * @param fieldName - The field name within the variant (e.g., "titleType")
 * @param type - The Iceberg primitive type for the field
 * @param nullable - Optional flag indicating if the field is nullable
 * @returns A ShreddedFieldInfo object with computed statistics path
 *
 * @example
 * ```ts
 * const fieldInfo = createShreddedFieldInfo('$data', 'titleType', 'string');
 * // Returns:
 * // {
 * //   path: 'titleType',
 * //   type: 'string',
 * //   statisticsPath: '$data.typed_value.titleType.typed_value',
 * // }
 * ```
 */
export function createShreddedFieldInfo(
  columnName: string,
  fieldName: string,
  type: IcebergPrimitiveType,
  nullable?: boolean
): ShreddedFieldInfo {
  const result: ShreddedFieldInfo = {
    path: fieldName,
    type,
    statisticsPath: getTypedValuePath(columnName, fieldName),
  };

  if (nullable !== undefined) {
    return { ...result, nullable };
  }

  return result;
}

/**
 * Create a VariantColumnSchema for a variant column with shredded fields.
 *
 * @param columnName - The variant column name (e.g., "$data")
 * @param shreddedFields - Array of shredded field definitions
 * @returns A VariantColumnSchema with computed paths
 *
 * @example
 * ```ts
 * const schema = createVariantColumnSchema('$data', [
 *   { path: 'titleType', type: 'string', statisticsPath: '...' },
 * ]);
 * // Returns schema with metadataPath, valuePath, typedValuePath computed
 * ```
 */
export function createVariantColumnSchema(
  columnName: string,
  shreddedFields: readonly ShreddedFieldInfo[]
): VariantColumnSchema {
  return {
    columnName,
    metadataPath: getMetadataPath(columnName),
    valuePath: getValuePath(columnName),
    typedValuePath: `${columnName}.typed_value`,
    shreddedFields,
  };
}

/**
 * Validate a VariantShredConfig.
 *
 * @param config - The configuration to validate
 * @returns Validation result with valid flag and any errors
 *
 * @example
 * ```ts
 * const result = validateVariantShredConfig({
 *   column: 'data',
 *   fields: ['titleType'],
 * });
 * if (!result.valid) {
 *   console.error(result.errors);
 * }
 * ```
 */
export function validateVariantShredConfig(
  config: VariantShredConfig
): VariantShredConfigValidationResult {
  const errors: string[] = [];

  // Validate column name
  if (!config.column || config.column.trim() === '') {
    errors.push('column name is required');
  }

  // Validate fields array
  if (!config.fields || config.fields.length === 0) {
    errors.push('at least one field is required');
  }

  return {
    valid: errors.length === 0,
    errors,
  };
}

// ============================================================================
// Path Generation Functions
// ============================================================================

/**
 * Get the metadata path for a variant column.
 *
 * @param columnName - The variant column name
 * @returns The metadata path (e.g., "$data.metadata")
 */
export function getMetadataPath(columnName: string): string {
  return `${columnName}.metadata`;
}

/**
 * Get the value path for a variant column.
 *
 * @param columnName - The variant column name
 * @returns The value path (e.g., "$data.value")
 */
export function getValuePath(columnName: string): string {
  return `${columnName}.value`;
}

/**
 * Get the typed value path for a specific field in a variant column.
 *
 * @param columnName - The variant column name
 * @param fieldName - The field name within the variant
 * @returns The typed value path (e.g., "$data.typed_value.titleType.typed_value")
 */
export function getTypedValuePath(columnName: string, fieldName: string): string {
  return `${columnName}.typed_value.${fieldName}.typed_value`;
}
