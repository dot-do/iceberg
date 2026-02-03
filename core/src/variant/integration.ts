/**
 * ParqueDB/Hyparquet Integration Helpers
 *
 * Provides convenience functions for setting up variant shredding that are
 * compatible with parquedb's variant-filter and hyparquet-writer implementations.
 *
 * This module consolidates variant shredding configuration, field ID assignment,
 * and schema integration into a unified API for easier integration with external
 * tools and libraries.
 *
 * @see https://github.com/hyparam/hyparquet
 * @see https://iceberg.apache.org/spec/#variant-shredding
 */

import type { IcebergPrimitiveType, IcebergSchema } from '../metadata/types.js';
import {
  extractVariantShredConfig,
  toTableProperties,
  type VariantShredPropertyConfig,
} from './config.js';
import { assignShreddedFieldIds, getShreddedStatisticsPaths } from './manifest-stats.js';

// Re-export commonly used functions for convenience
export { extractVariantShredConfig, toTableProperties } from './config.js';
export { getStatisticsPaths, mapFilterPathToStats } from './statistics.js';
export { transformVariantFilter } from './filter.js';
export { filterDataFiles, filterDataFilesWithStats } from './row-group-filter.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Configuration for shredding a variant column.
 *
 * This is a simplified interface that uses more intuitive names compared
 * to the internal VariantShredPropertyConfig type.
 *
 * @example
 * ```ts
 * const config: VariantShredConfigSimple = {
 *   column: '$data',
 *   fields: ['title', 'year', 'rating'],
 *   fieldTypes: {
 *     title: 'string',
 *     year: 'int',
 *     rating: 'double',
 *   },
 * };
 * ```
 */
export interface VariantShredConfigSimple {
  /** The variant column name (e.g., "$data") */
  readonly column: string;
  /** Field names to extract from the variant */
  readonly fields: readonly string[];
  /** Optional type mapping for fields */
  readonly fieldTypes?: Record<string, IcebergPrimitiveType>;
}

/**
 * Schema field definition for a variant column.
 *
 * Represents a variant column in a table schema with optional
 * information about which fields are shredded.
 */
export interface VariantSchemaField {
  /** The column name (e.g., "$data") */
  readonly name: string;
  /** The column type - always 'variant' for variant columns */
  readonly type: 'variant';
  /** List of field names that are shredded */
  readonly shreddedFields?: readonly string[];
  /** Type mapping for shredded fields */
  readonly fieldTypes?: Record<string, IcebergPrimitiveType>;
}

/**
 * Options for the setupVariantShredding function.
 */
export interface SetupVariantShreddingOptions {
  /** Table properties containing variant shred configuration */
  readonly tableProperties: Record<string, string>;
  /** The table schema for validation */
  readonly schema: IcebergSchema;
  /** The starting field ID for shredded columns (should be > max schema field ID) */
  readonly startingFieldId: number;
}

/**
 * Result of the setupVariantShredding function.
 */
export interface SetupVariantShreddingResult {
  /** Extracted variant shred configurations */
  readonly configs: VariantShredPropertyConfig[];
  /** Map from statistics path to field ID */
  readonly fieldIdMap: Map<string, number>;
  /** All statistics paths for shredded fields */
  readonly statisticsPaths: string[];
}

/**
 * Result of validating a shred config against a schema.
 */
export interface ConfigSchemaValidationResult {
  /** Whether the configuration is valid */
  readonly valid: boolean;
  /** List of validation errors */
  readonly errors: string[];
}

// ============================================================================
// Configuration Parsing/Formatting
// ============================================================================

/**
 * Parse variant shred configuration from table properties.
 *
 * Converts table properties to a simplified configuration format
 * that uses 'column' instead of 'columnName'.
 *
 * @param properties - Iceberg table properties
 * @returns Array of simplified variant shred configurations
 *
 * @example
 * ```ts
 * const properties = {
 *   'write.variant.shred-columns': '$data',
 *   'write.variant.$data.shred-fields': 'title,year',
 *   'write.variant.$data.field-types': 'title:string,year:int',
 * };
 * const configs = parseShredConfig(properties);
 * // [{ column: '$data', fields: ['title', 'year'], fieldTypes: {...} }]
 * ```
 */
export function parseShredConfig(properties: Record<string, string>): VariantShredConfigSimple[] {
  const internalConfigs = extractVariantShredConfig(properties);

  return internalConfigs.map((config) => ({
    column: config.columnName,
    fields: [...config.fields],
    fieldTypes: { ...config.fieldTypes },
  }));
}

/**
 * Format variant shred configurations to table properties.
 *
 * Converts simplified configuration format to table properties
 * that can be stored in Iceberg table metadata.
 *
 * @param configs - Array of simplified variant shred configurations
 * @returns Table properties record
 *
 * @example
 * ```ts
 * const configs = [{
 *   column: '$data',
 *   fields: ['title', 'year'],
 *   fieldTypes: { title: 'string', year: 'int' },
 * }];
 * const properties = formatShredConfig(configs);
 * // { 'write.variant.shred-columns': '$data', ... }
 * ```
 */
export function formatShredConfig(
  configs: readonly VariantShredConfigSimple[]
): Record<string, string> {
  const internalConfigs: VariantShredPropertyConfig[] = configs.map((config) => ({
    columnName: config.column,
    fields: [...config.fields],
    fieldTypes: config.fieldTypes ?? {},
  }));

  return toTableProperties(internalConfigs);
}

// ============================================================================
// Schema Integration
// ============================================================================

/**
 * Create variant schema field definitions from shred configurations.
 *
 * Converts internal configuration format to schema field definitions
 * that can be used to understand the structure of variant columns.
 *
 * @param configs - Array of variant shred property configurations
 * @returns Array of variant schema field definitions
 *
 * @example
 * ```ts
 * const configs = [{ columnName: '$data', fields: ['title'], fieldTypes: {...} }];
 * const fields = createVariantSchemaFields(configs);
 * // [{ name: '$data', type: 'variant', shreddedFields: ['title'], ... }]
 * ```
 */
export function createVariantSchemaFields(
  configs: readonly VariantShredPropertyConfig[]
): VariantSchemaField[] {
  return configs.map((config) => ({
    name: config.columnName,
    type: 'variant' as const,
    shreddedFields: [...config.fields],
    fieldTypes: { ...config.fieldTypes },
  }));
}

/**
 * Look up a field ID for a shredded statistics path.
 *
 * @param fieldIdMap - Map from statistics paths to field IDs
 * @param shreddedPath - The statistics path to look up
 * @returns The field ID, or null if not found
 *
 * @example
 * ```ts
 * const fieldIdMap = new Map([['$data.typed_value.title.typed_value', 100]]);
 * const id = getFieldIdForShreddedPath(fieldIdMap, '$data.typed_value.title.typed_value');
 * // 100
 * ```
 */
export function getFieldIdForShreddedPath(
  fieldIdMap: Map<string, number>,
  shreddedPath: string
): number | null {
  const id = fieldIdMap.get(shreddedPath);
  return id !== undefined ? id : null;
}

/**
 * Validate variant shred configurations against a table schema.
 *
 * Checks that:
 * - All referenced columns exist in the schema
 * - All referenced columns are of type 'variant'
 *
 * @param configs - Array of variant shred property configurations
 * @param schema - The Iceberg schema to validate against
 * @returns Validation result with valid flag and any errors
 *
 * @example
 * ```ts
 * const configs = [{ columnName: '$data', fields: ['title'], fieldTypes: {} }];
 * const schema = { fields: [{ name: '$data', type: 'variant', ... }] };
 * const result = validateConfigWithSchema(configs, schema);
 * // { valid: true, errors: [] }
 * ```
 */
export function validateConfigWithSchema(
  configs: readonly VariantShredPropertyConfig[],
  schema: IcebergSchema
): ConfigSchemaValidationResult {
  const errors: string[] = [];

  // Build a map of schema fields for quick lookup
  const schemaFields = new Map<string, { type: string }>();
  for (const field of schema.fields) {
    const fieldType = typeof field.type === 'string' ? field.type : field.type.type;
    schemaFields.set(field.name, { type: fieldType });
  }

  for (const config of configs) {
    const schemaField = schemaFields.get(config.columnName);

    if (!schemaField) {
      errors.push(`Column '${config.columnName}' not found in schema`);
      continue;
    }

    if (schemaField.type !== 'variant') {
      errors.push(`Column '${config.columnName}' is not a variant column (type: ${schemaField.type})`);
    }
  }

  return {
    valid: errors.length === 0,
    errors,
  };
}

// ============================================================================
// Convenience Setup Function
// ============================================================================

/**
 * Set up variant shredding from table properties.
 *
 * This is a one-stop convenience function that:
 * 1. Extracts shred configurations from table properties
 * 2. Assigns field IDs to shredded columns
 * 3. Generates statistics paths for all shredded fields
 *
 * The result can be used directly with filterDataFiles and other
 * variant filtering functions.
 *
 * @param options - Setup options
 * @returns Setup result with configs, field ID map, and statistics paths
 *
 * @example
 * ```ts
 * const result = setupVariantShredding({
 *   tableProperties: {
 *     'write.variant.shred-columns': '$data',
 *     'write.variant.$data.shred-fields': 'title,year',
 *   },
 *   schema: tableSchema,
 *   startingFieldId: 1000,
 * });
 *
 * // Use with filterDataFiles
 * const filtered = filterDataFiles(
 *   dataFiles,
 *   filter,
 *   result.configs,
 *   result.fieldIdMap
 * );
 * ```
 */
export function setupVariantShredding(
  options: SetupVariantShreddingOptions
): SetupVariantShreddingResult {
  const { tableProperties, startingFieldId } = options;

  // Extract configurations from table properties
  const configs = extractVariantShredConfig(tableProperties);

  // If no configs, return empty result
  if (configs.length === 0) {
    return {
      configs: [],
      fieldIdMap: new Map(),
      statisticsPaths: [],
    };
  }

  // Assign field IDs to shredded columns
  const fieldIdMap = assignShreddedFieldIds(configs, startingFieldId);

  // Get all statistics paths
  const statisticsPaths = getShreddedStatisticsPaths(configs);

  return {
    configs,
    fieldIdMap,
    statisticsPaths,
  };
}
