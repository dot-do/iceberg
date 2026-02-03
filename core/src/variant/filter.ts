/**
 * Variant Filter Transformation
 *
 * Provides utilities for rewriting variant field filters to use statistics paths.
 * When variant columns are shredded, queries on specific fields can be optimized
 * by directing them to the shredded column statistics paths.
 *
 * @see https://iceberg.apache.org/spec/#variant-shredding
 */

import type { VariantShredPropertyConfig } from './config.js';
import { getTypedValuePath } from './types.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Result of transforming a variant filter.
 *
 * Contains the transformed filter and information about which paths were
 * transformed and which could not be transformed.
 */
export interface TransformResult {
  /** The transformed filter with variant paths rewritten to statistics paths */
  readonly filter: Record<string, unknown>;
  /** List of variant field paths that were transformed to statistics paths */
  readonly transformedPaths: readonly string[];
  /** List of variant field paths that could not be transformed (non-shredded fields) */
  readonly untransformedPaths: readonly string[];
}

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
export function isComparisonOperator(key: string): boolean {
  return COMPARISON_OPERATORS.has(key);
}

/**
 * Check if a key is a logical operator.
 *
 * @param key - The key to check
 * @returns True if the key is a logical operator
 */
export function isLogicalOperator(key: string): boolean {
  return LOGICAL_OPERATORS.has(key);
}

/**
 * Parse a variant field path to extract column name and field name.
 *
 * @param path - The full path (e.g., "$data.title" or "$data.user.name")
 * @returns Object with columnName and fieldName, or null if not a valid variant path
 */
function parseVariantPath(path: string): { columnName: string; fieldName: string } | null {
  // Must start with $ to be a variant column
  if (!path.startsWith('$')) {
    return null;
  }

  const dotIndex = path.indexOf('.');
  if (dotIndex === -1) {
    return null;
  }

  const columnName = path.substring(0, dotIndex);
  const fieldName = path.substring(dotIndex + 1);

  if (!columnName || !fieldName) {
    return null;
  }

  return { columnName, fieldName };
}

/**
 * Find the config for a given column name.
 *
 * @param columnName - The column name to find
 * @param configs - Array of variant shred configurations
 * @returns The matching config or undefined
 */
function findConfigForColumn(
  columnName: string,
  configs: readonly VariantShredPropertyConfig[]
): VariantShredPropertyConfig | undefined {
  return configs.find((c) => c.columnName === columnName);
}

/**
 * Check if a field is shredded in the given config.
 *
 * @param fieldName - The field name to check
 * @param config - The variant shred configuration
 * @returns True if the field is in the shredded fields list
 */
function isFieldShredded(fieldName: string, config: VariantShredPropertyConfig): boolean {
  return config.fields.includes(fieldName);
}

/**
 * Get the statistics path for a shredded field.
 *
 * @param columnName - The variant column name
 * @param fieldName - The field name within the variant
 * @returns The statistics path for the field
 */
function getStatisticsPath(columnName: string, fieldName: string): string {
  return getTypedValuePath(columnName, fieldName);
}

// ============================================================================
// Main Transformation Functions
// ============================================================================

/**
 * Context object passed through the transformation.
 */
interface TransformContext {
  readonly configs: readonly VariantShredPropertyConfig[];
  readonly transformedPaths: string[];
  readonly untransformedPaths: string[];
}

/**
 * Transform a filter value recursively.
 *
 * @param value - The value to transform
 * @param ctx - The transformation context
 * @returns The transformed value
 */
function transformValue(value: unknown, ctx: TransformContext): unknown {
  if (value === null || value === undefined) {
    return value;
  }

  if (Array.isArray(value)) {
    // Arrays can be filter arrays in $and/$or or just value arrays
    return value.map((item) => {
      if (isPlainObject(item)) {
        return transformFilterObject(item as Record<string, unknown>, ctx);
      }
      return item;
    });
  }

  if (isPlainObject(value)) {
    return transformFilterObject(value as Record<string, unknown>, ctx);
  }

  return value;
}

/**
 * Check if a value is a plain object (not array, null, etc.).
 *
 * @param value - The value to check
 * @returns True if value is a plain object
 */
function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Transform a filter object, rewriting variant paths to statistics paths.
 *
 * @param filter - The filter object to transform
 * @param ctx - The transformation context
 * @returns The transformed filter object
 */
function transformFilterObject(
  filter: Record<string, unknown>,
  ctx: TransformContext
): Record<string, unknown> {
  const result: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(filter)) {
    // Handle logical operators ($and, $or, $not, $nor)
    if (isLogicalOperator(key)) {
      if (key === '$not' && isPlainObject(value)) {
        // $not contains a single filter object
        result[key] = transformFilterObject(value as Record<string, unknown>, ctx);
      } else if (Array.isArray(value)) {
        // $and, $or, $nor contain arrays of filters
        result[key] = value.map((item) => {
          if (isPlainObject(item)) {
            return transformFilterObject(item as Record<string, unknown>, ctx);
          }
          return item;
        });
      } else {
        result[key] = value;
      }
      continue;
    }

    // Try to parse as variant path
    const parsed = parseVariantPath(key);

    if (!parsed) {
      // Not a variant path - check if value needs transformation (for nested operators)
      if (isPlainObject(value)) {
        result[key] = transformOperatorObject(value as Record<string, unknown>, ctx);
      } else {
        result[key] = value;
      }
      continue;
    }

    // Found a variant path - check if we have a config for this column
    const config = findConfigForColumn(parsed.columnName, ctx.configs);

    if (!config) {
      // Unknown column - pass through unchanged (not a configured variant column)
      if (isPlainObject(value)) {
        result[key] = transformOperatorObject(value as Record<string, unknown>, ctx);
      } else {
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
        result[statsPath] = transformOperatorObject(value as Record<string, unknown>, ctx);
      } else {
        result[statsPath] = value;
      }
    } else {
      // Non-shredded field - keep original path
      ctx.untransformedPaths.push(key);

      if (isPlainObject(value)) {
        result[key] = transformOperatorObject(value as Record<string, unknown>, ctx);
      } else {
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
function transformOperatorObject(
  obj: Record<string, unknown>,
  ctx: TransformContext
): Record<string, unknown> {
  const result: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(obj)) {
    if (isComparisonOperator(key)) {
      // Comparison operator - preserve the value
      result[key] = value;
    } else if (isLogicalOperator(key)) {
      // Nested logical operator
      result[key] = transformValue(value, ctx);
    } else {
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
export function transformVariantFilter(
  filter: Record<string, unknown>,
  configs: readonly VariantShredPropertyConfig[]
): TransformResult {
  const ctx: TransformContext = {
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
