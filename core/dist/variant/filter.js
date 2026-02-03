/**
 * Variant Filter Transformation
 *
 * Provides utilities for rewriting variant field filters to use statistics paths.
 * When variant columns are shredded, queries on specific fields can be optimized
 * by directing them to the shredded column statistics paths.
 *
 * @see https://iceberg.apache.org/spec/#variant-shredding
 */
import { getTypedValuePath } from './types.js';
import { isPlainObject, parseVariantPath } from './utils.js';
// ============================================================================
// Constants
// ============================================================================
/**
 * Comparison operators that can be used in filters.
 */
const COMPARISON_OPERATORS = new Set([
    '$eq',
    '$gt',
    '$gte',
    '$lt',
    '$lte',
    '$ne',
    '$in',
    '$nin',
    '$exists',
    '$regex',
]);
/**
 * Logical operators that contain nested filters.
 */
const LOGICAL_OPERATORS = new Set(['$and', '$or', '$not', '$nor']);
// ============================================================================
// Helper Functions
// ============================================================================
/**
 * Check if a key is a comparison operator.
 *
 * @param key - The key to check
 * @returns True if the key is a comparison operator
 */
export function isComparisonOperator(key) {
    return COMPARISON_OPERATORS.has(key);
}
/**
 * Check if a key is a logical operator.
 *
 * @param key - The key to check
 * @returns True if the key is a logical operator
 */
export function isLogicalOperator(key) {
    return LOGICAL_OPERATORS.has(key);
}
/**
 * Find the config for a given column name.
 *
 * @param columnName - The column name to find
 * @param configs - Array of variant shred configurations
 * @returns The matching config or undefined
 */
function findConfigForColumn(columnName, configs) {
    return configs.find((c) => c.columnName === columnName);
}
/**
 * Check if a field is shredded in the given config.
 *
 * @param fieldName - The field name to check
 * @param config - The variant shred configuration
 * @returns True if the field is in the shredded fields list
 */
function isFieldShredded(fieldName, config) {
    return config.fields.includes(fieldName);
}
/**
 * Get the statistics path for a shredded field.
 *
 * @param columnName - The variant column name
 * @param fieldName - The field name within the variant
 * @returns The statistics path for the field
 */
function getStatisticsPath(columnName, fieldName) {
    return getTypedValuePath(columnName, fieldName);
}
/**
 * Transform a filter value recursively.
 *
 * @param value - The value to transform
 * @param ctx - The transformation context
 * @returns The transformed value
 */
function transformValue(value, ctx) {
    if (value === null || value === undefined) {
        return value;
    }
    if (Array.isArray(value)) {
        // Arrays can be filter arrays in $and/$or or just value arrays
        return value.map((item) => {
            if (isPlainObject(item)) {
                return transformFilterObject(item, ctx);
            }
            return item;
        });
    }
    if (isPlainObject(value)) {
        return transformFilterObject(value, ctx);
    }
    return value;
}
/**
 * Transform a filter object, rewriting variant paths to statistics paths.
 *
 * @param filter - The filter object to transform
 * @param ctx - The transformation context
 * @returns The transformed filter object
 */
function transformFilterObject(filter, ctx) {
    const result = {};
    for (const [key, value] of Object.entries(filter)) {
        // Handle logical operators ($and, $or, $not, $nor)
        if (isLogicalOperator(key)) {
            if (key === '$not' && isPlainObject(value)) {
                // $not contains a single filter object
                result[key] = transformFilterObject(value, ctx);
            }
            else if (Array.isArray(value)) {
                // $and, $or, $nor contain arrays of filters
                result[key] = value.map((item) => {
                    if (isPlainObject(item)) {
                        return transformFilterObject(item, ctx);
                    }
                    return item;
                });
            }
            else {
                result[key] = value;
            }
            continue;
        }
        // Try to parse as variant path
        const parsed = parseVariantPath(key);
        if (!parsed) {
            // Not a variant path - check if value needs transformation (for nested operators)
            if (isPlainObject(value)) {
                result[key] = transformOperatorObject(value, ctx);
            }
            else {
                result[key] = value;
            }
            continue;
        }
        // Found a variant path - check if we have a config for this column
        const config = findConfigForColumn(parsed.columnName, ctx.configs);
        if (!config) {
            // Unknown column - pass through unchanged (not a configured variant column)
            if (isPlainObject(value)) {
                result[key] = transformOperatorObject(value, ctx);
            }
            else {
                result[key] = value;
            }
            continue;
        }
        // Check if field is shredded
        if (isFieldShredded(parsed.fieldName, config)) {
            // Transform to statistics path
            const statsPath = getStatisticsPath(parsed.columnName, parsed.fieldName);
            ctx.transformedPaths.push(key);
            if (isPlainObject(value)) {
                result[statsPath] = transformOperatorObject(value, ctx);
            }
            else {
                result[statsPath] = value;
            }
        }
        else {
            // Non-shredded field - keep original path
            ctx.untransformedPaths.push(key);
            if (isPlainObject(value)) {
                result[key] = transformOperatorObject(value, ctx);
            }
            else {
                result[key] = value;
            }
        }
    }
    return result;
}
/**
 * Transform an operator object (contains $eq, $gt, etc.).
 *
 * @param obj - The operator object
 * @param ctx - The transformation context
 * @returns The transformed operator object
 */
function transformOperatorObject(obj, ctx) {
    const result = {};
    for (const [key, value] of Object.entries(obj)) {
        if (isComparisonOperator(key)) {
            // Comparison operator - preserve the value
            result[key] = value;
        }
        else if (isLogicalOperator(key)) {
            // Nested logical operator
            result[key] = transformValue(value, ctx);
        }
        else {
            // Could be a nested field path in the operator value
            result[key] = value;
        }
    }
    return result;
}
// ============================================================================
// Main Export
// ============================================================================
/**
 * Transform a filter object, rewriting variant field paths to statistics paths.
 *
 * When variant columns are shredded, specific fields are extracted into separate
 * columns. This function rewrites filter paths to point to the shredded column
 * statistics paths, enabling efficient query pruning.
 *
 * @param filter - The filter object to transform
 * @param configs - Array of variant shred configurations
 * @returns TransformResult with transformed filter and path information
 *
 * @example Basic equality filter
 * ```ts
 * const config: VariantShredPropertyConfig = {
 *   columnName: '$data',
 *   fields: ['title', 'year'],
 *   fieldTypes: { title: 'string', year: 'int' },
 * };
 *
 * const result = transformVariantFilter(
 *   { '$data.title': 'The Matrix' },
 *   [config]
 * );
 *
 * // result.filter = { '$data.typed_value.title.typed_value': 'The Matrix' }
 * // result.transformedPaths = ['$data.title']
 * // result.untransformedPaths = []
 * ```
 *
 * @example Range filter with operators
 * ```ts
 * const result = transformVariantFilter(
 *   { '$data.year': { $gte: 2010, $lte: 2020 } },
 *   [config]
 * );
 *
 * // result.filter = {
 * //   '$data.typed_value.year.typed_value': { $gte: 2010, $lte: 2020 }
 * // }
 * ```
 *
 * @example Mixed filter with shredded and non-shredded fields
 * ```ts
 * const result = transformVariantFilter(
 *   { '$data.title': 'foo', '$data.director': 'Nolan', id: 123 },
 *   [config]
 * );
 *
 * // Shredded field transformed, others preserved as-is
 * // result.filter = {
 * //   '$data.typed_value.title.typed_value': 'foo',
 * //   '$data.director': 'Nolan',
 * //   id: 123
 * // }
 * // result.transformedPaths = ['$data.title']
 * // result.untransformedPaths = ['$data.director']
 * ```
 */
export function transformVariantFilter(filter, configs) {
    const ctx = {
        configs,
        transformedPaths: [],
        untransformedPaths: [],
    };
    const transformedFilter = transformFilterObject(filter, ctx);
    return {
        filter: transformedFilter,
        transformedPaths: ctx.transformedPaths,
        untransformedPaths: ctx.untransformedPaths,
    };
}
//# sourceMappingURL=filter.js.map