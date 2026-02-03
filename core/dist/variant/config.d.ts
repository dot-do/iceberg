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
/**
 * Table property key that lists which variant columns should be shredded.
 * Value is a comma-separated list of column names.
 *
 * @example
 * ```
 * properties['write.variant.shred-columns'] = '$data,$index'
 * ```
 */
export declare const VARIANT_SHRED_COLUMNS_KEY = "write.variant.shred-columns";
/**
 * Prefix for variant column-specific properties.
 * Combined with column name and suffix to form full property keys.
 */
export declare const VARIANT_SHRED_FIELDS_KEY_PREFIX = "write.variant.";
/**
 * Suffix for the shred-fields property of a specific column.
 * Lists which fields within the variant to shred.
 *
 * @example
 * Full key: 'write.variant.$data.shred-fields' = 'title,year,rating'
 */
export declare const VARIANT_SHRED_FIELDS_KEY_SUFFIX = ".shred-fields";
/**
 * Suffix for the field-types property of a specific column.
 * Maps field names to their expected Iceberg primitive types.
 *
 * @example
 * Full key: 'write.variant.$data.field-types' = 'title:string,year:int'
 */
export declare const VARIANT_FIELD_TYPES_KEY_SUFFIX = ".field-types";
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
/**
 * Generate the property key for shred-fields of a specific column.
 *
 * @param columnName - Name of the variant column
 * @returns Property key like 'write.variant.$data.shred-fields'
 */
export declare function getShredFieldsKey(columnName: string): string;
/**
 * Generate the property key for field-types of a specific column.
 *
 * @param columnName - Name of the variant column
 * @returns Property key like 'write.variant.$data.field-types'
 */
export declare function getFieldTypesKey(columnName: string): string;
/**
 * Parse the shred-columns property value into an array of column names.
 *
 * @param value - Comma-separated column names or undefined
 * @returns Array of column names (empty array if value is empty/undefined)
 */
export declare function parseShredColumnsProperty(value: string | undefined): string[];
/**
 * Parse a shred-fields property value into an array of field names.
 *
 * @param value - Comma-separated field names or undefined
 * @returns Array of field names (empty array if value is empty/undefined)
 */
export declare function parseShredFieldsProperty(value: string | undefined): string[];
/**
 * Parse a field-types property value into a record of field names to types.
 *
 * @param value - Comma-separated "field:type" pairs or undefined
 * @returns Record mapping field names to Iceberg primitive types
 */
export declare function parseFieldTypesProperty(value: string | undefined): Record<string, IcebergPrimitiveType>;
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
export declare function extractVariantShredConfig(properties: Record<string, string>): VariantShredConfig[];
/**
 * Format an array of configs into the shred-columns property value.
 *
 * @param configs - Array of variant shred configurations
 * @returns Comma-separated column names
 */
export declare function formatShredColumnsProperty(configs: readonly VariantShredConfig[]): string;
/**
 * Format an array of field names into a shred-fields property value.
 *
 * @param fields - Array of field names
 * @returns Comma-separated field names
 */
export declare function formatShredFieldsProperty(fields: readonly string[]): string;
/**
 * Format a field types record into a field-types property value.
 *
 * @param fieldTypes - Record of field names to types
 * @returns Comma-separated "field:type" pairs
 */
export declare function formatFieldTypesProperty(fieldTypes: Readonly<Record<string, IcebergPrimitiveType>>): string;
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
export declare function toTableProperties(configs: readonly VariantShredConfig[]): Record<string, string>;
/**
 * Result of validating a shred configuration.
 */
export interface ShredConfigValidationResult {
    /** Whether the configuration is valid */
    readonly valid: boolean;
    /** List of validation errors (empty if valid) */
    readonly errors: readonly string[];
}
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
export declare function validateShredConfig(config: VariantShredConfig): ShredConfigValidationResult;
//# sourceMappingURL=config.d.ts.map