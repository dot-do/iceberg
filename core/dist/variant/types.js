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
export function createShreddedFieldInfo(columnName, fieldName, type, nullable) {
    const result = {
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
export function createVariantColumnSchema(columnName, shreddedFields) {
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
export function validateVariantShredConfig(config) {
    const errors = [];
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
export function getMetadataPath(columnName) {
    return `${columnName}.metadata`;
}
/**
 * Get the value path for a variant column.
 *
 * @param columnName - The variant column name
 * @returns The value path (e.g., "$data.value")
 */
export function getValuePath(columnName) {
    return `${columnName}.value`;
}
/**
 * Get the typed value path for a specific field in a variant column.
 *
 * @param columnName - The variant column name
 * @param fieldName - The field name within the variant
 * @returns The typed value path (e.g., "$data.typed_value.titleType.typed_value")
 */
export function getTypedValuePath(columnName, fieldName) {
    return `${columnName}.typed_value.${fieldName}.typed_value`;
}
//# sourceMappingURL=types.js.map