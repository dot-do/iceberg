/**
 * Predicate Pushdown for Variant Filters
 *
 * Provides utilities for determining whether a data file can be skipped
 * during scan planning based on shredded column statistics. When variant
 * columns are shredded, their statistics (min/max bounds, null counts) can
 * be used to prune data files that definitely don't match filter predicates.
 *
 * @see https://iceberg.apache.org/spec/#variant-shredding
 */

import type { DataFile, IcebergPrimitiveType } from '../metadata/types.js';
import type { VariantShredPropertyConfig } from './config.js';
import { deserializeShreddedBound } from './manifest-stats.js';
import { compareValues, isPlainObject, parseVariantPath } from './utils.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Result of evaluating a predicate against data file statistics.
 */
export interface PredicateResult {
  /** Whether to skip this data file (true = definitely no matches) */
  readonly skip: boolean;
  /** Human-readable reason for the skip decision (useful for debugging) */
  readonly reason?: string;
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Get the statistics path for a field.
 */
function getStatisticsPath(columnName: string, fieldName: string): string {
  return `${columnName}.typed_value.${fieldName}.typed_value`;
}

/**
 * Find the field type from configs.
 */
function getFieldType(
  columnName: string,
  fieldName: string,
  configs: readonly VariantShredPropertyConfig[]
): IcebergPrimitiveType | undefined {
  const config = configs.find((c) => c.columnName === columnName);
  if (!config) return undefined;
  if (!config.fields.includes(fieldName)) return undefined;
  return config.fieldTypes[fieldName];
}

/**
 * Get the field ID for a statistics path.
 */
function getFieldIdForPath(
  statsPath: string,
  fieldIdMap: Map<string, number>
): number | undefined {
  return fieldIdMap.get(statsPath);
}

/**
 * Get bounds from a data file for a given field ID.
 */
function getBoundsFromDataFile(
  dataFile: DataFile,
  fieldId: number,
  type: IcebergPrimitiveType
): { lower: unknown; upper: unknown } | null {
  const lowerBounds = dataFile['lower-bounds'];
  const upperBounds = dataFile['upper-bounds'];

  if (!lowerBounds || !upperBounds) {
    return null;
  }

  const lowerBound = lowerBounds[fieldId];
  const upperBound = upperBounds[fieldId];

  if (lowerBound === undefined || upperBound === undefined) {
    return null;
  }

  // Deserialize bounds
  const lower =
    lowerBound instanceof Uint8Array
      ? deserializeShreddedBound(lowerBound, type)
      : lowerBound;
  const upper =
    upperBound instanceof Uint8Array
      ? deserializeShreddedBound(upperBound, type)
      : upperBound;

  return { lower, upper };
}

// ============================================================================
// Core Evaluation Functions
// ============================================================================

/**
 * Check if bounds overlap a value for a given operator.
 *
 * This is the core function that determines whether a data file's statistics
 * bounds could possibly contain a value that matches the given operator.
 *
 * @param lower - Lower bound of values in the data file
 * @param upper - Upper bound of values in the data file
 * @param operator - The comparison operator ($eq, $gt, $gte, $lt, $lte, $ne)
 * @param value - The value being compared against
 * @param type - The Iceberg primitive type for comparison
 * @returns True if bounds could overlap (don't skip), false if definitely no overlap (skip)
 */
export function boundsOverlapValue(
  lower: unknown,
  upper: unknown,
  operator: string,
  value: unknown,
  type: IcebergPrimitiveType
): boolean {
  switch (operator) {
    case '$eq': {
      // Value must be within [lower, upper] to potentially match
      const cmpLower = compareValues(value, lower, type);
      const cmpUpper = compareValues(value, upper, type);
      // value >= lower AND value <= upper
      return cmpLower >= 0 && cmpUpper <= 0;
    }

    case '$gt': {
      // Need some value > filter value
      // Can skip if upper <= value (all values in file <= filter value)
      const cmpUpper = compareValues(upper, value, type);
      return cmpUpper > 0;
    }

    case '$gte': {
      // Need some value >= filter value
      // Can skip if upper < value
      const cmpUpper = compareValues(upper, value, type);
      return cmpUpper >= 0;
    }

    case '$lt': {
      // Need some value < filter value
      // Can skip if lower >= value (all values in file >= filter value)
      const cmpLower = compareValues(lower, value, type);
      return cmpLower < 0;
    }

    case '$lte': {
      // Need some value <= filter value
      // Can skip if lower > value
      const cmpLower = compareValues(lower, value, type);
      return cmpLower <= 0;
    }

    case '$ne': {
      // Need some value != filter value
      // Can only skip if ALL values are exactly the filter value
      // This happens when lower == upper == value
      const cmpLower = compareValues(lower, value, type);
      const cmpUpper = compareValues(upper, value, type);
      // If lower == upper == value, then all values are the same as filter
      return !(cmpLower === 0 && cmpUpper === 0);
    }

    default:
      // Unknown operator, don't skip
      return true;
  }
}

/**
 * Evaluate an $in predicate against bounds.
 *
 * @param lower - Lower bound of values in the data file
 * @param upper - Upper bound of values in the data file
 * @param values - Array of values in the $in set
 * @param type - The Iceberg primitive type for comparison
 * @returns True if any value overlaps bounds (don't skip), false if none overlap (skip)
 */
export function evaluateInPredicate(
  lower: unknown,
  upper: unknown,
  values: unknown[],
  type: IcebergPrimitiveType
): boolean {
  // If values array is empty, nothing can match
  if (values.length === 0) {
    return false;
  }

  // Check if any value in the set overlaps the bounds
  for (const value of values) {
    if (boundsOverlapValue(lower, upper, '$eq', value, type)) {
      return true;
    }
  }

  return false;
}

// ============================================================================
// Filter Evaluation
// ============================================================================

/**
 * Evaluate a single field filter against data file stats.
 *
 * @returns PredicateResult indicating whether to skip
 */
function evaluateFieldFilter(
  dataFile: DataFile,
  fieldPath: string,
  filterValue: unknown,
  configs: readonly VariantShredPropertyConfig[],
  fieldIdMap: Map<string, number>
): PredicateResult {
  // Parse the variant path
  const parsed = parseVariantPath(fieldPath);
  if (!parsed) {
    // Not a variant path, can't evaluate
    return { skip: false };
  }

  const { columnName, fieldName } = parsed;

  // Get the field type
  const type = getFieldType(columnName, fieldName, configs);
  if (!type) {
    // Field is not shredded, can't evaluate
    return { skip: false };
  }

  // Get the statistics path and field ID
  const statsPath = getStatisticsPath(columnName, fieldName);
  const fieldId = getFieldIdForPath(statsPath, fieldIdMap);
  if (fieldId === undefined) {
    // No field ID mapping, can't evaluate
    return { skip: false };
  }

  // Handle direct equality (shorthand filter)
  if (!isPlainObject(filterValue)) {
    return evaluateOperatorFilter(dataFile, fieldId, fieldName, '$eq', filterValue, type);
  }

  // Filter value is an object with operators
  const operatorFilter = filterValue as Record<string, unknown>;

  // Evaluate each operator
  for (const [operator, value] of Object.entries(operatorFilter)) {
    const result = evaluateOperatorFilter(dataFile, fieldId, fieldName, operator, value, type);
    if (result.skip) {
      return result;
    }
  }

  return { skip: false };
}

/**
 * Evaluate a single operator against data file stats.
 */
function evaluateOperatorFilter(
  dataFile: DataFile,
  fieldId: number,
  fieldName: string,
  operator: string,
  value: unknown,
  type: IcebergPrimitiveType
): PredicateResult {
  // Handle null comparisons specially
  if (value === null) {
    return evaluateNullFilter(dataFile, fieldId, fieldName, operator);
  }

  // Handle $in specially
  if (operator === '$in') {
    if (!Array.isArray(value)) {
      return { skip: false };
    }
    return evaluateInFilter(dataFile, fieldId, fieldName, value, type);
  }

  // Handle $ne null specially
  if (operator === '$ne') {
    return evaluateNeFilter(dataFile, fieldId, fieldName, value, type);
  }

  // Get bounds
  const bounds = getBoundsFromDataFile(dataFile, fieldId, type);
  if (!bounds) {
    // No bounds available, can't skip
    return { skip: false };
  }

  // Evaluate the operator
  const overlaps = boundsOverlapValue(bounds.lower, bounds.upper, operator, value, type);
  if (!overlaps) {
    return {
      skip: true,
      reason: `Field '${fieldName}' bounds [${bounds.lower}, ${bounds.upper}] do not overlap ${operator} ${value}`,
    };
  }

  return { skip: false };
}

/**
 * Evaluate null comparisons.
 */
function evaluateNullFilter(
  dataFile: DataFile,
  fieldId: number,
  fieldName: string,
  operator: string
): PredicateResult {
  const nullCounts = dataFile['null-value-counts'];

  // $eq null - looking for null values
  if (operator === '$eq') {
    if (nullCounts && nullCounts[fieldId] === 0) {
      return {
        skip: true,
        reason: `Field '${fieldName}' has no null values`,
      };
    }
    // If null count is positive or unknown, don't skip
    return { skip: false };
  }

  // $ne null - looking for non-null values
  if (operator === '$ne') {
    // Check if all values are null
    const valueCounts = dataFile['value-counts'];
    if (nullCounts && valueCounts) {
      const nullCount = nullCounts[fieldId] ?? 0;
      const valueCount = valueCounts[fieldId] ?? 0;
      if (nullCount === valueCount && valueCount > 0) {
        // All values are null
        return {
          skip: true,
          reason: `Field '${fieldName}' has all null values`,
        };
      }
    }
    return { skip: false };
  }

  // Other operators with null - can't skip
  return { skip: false };
}

/**
 * Evaluate $ne filter.
 */
function evaluateNeFilter(
  dataFile: DataFile,
  fieldId: number,
  fieldName: string,
  value: unknown,
  type: IcebergPrimitiveType
): PredicateResult {
  // Handle $ne null
  if (value === null) {
    return evaluateNullFilter(dataFile, fieldId, fieldName, '$ne');
  }

  // Get bounds
  const bounds = getBoundsFromDataFile(dataFile, fieldId, type);
  if (!bounds) {
    // No bounds available, can't skip
    return { skip: false };
  }

  // Can skip if lower == upper == value (all values are the filter value)
  const cmpLower = compareValues(bounds.lower, value, type);
  const cmpUpper = compareValues(bounds.upper, value, type);

  if (cmpLower === 0 && cmpUpper === 0) {
    return {
      skip: true,
      reason: `Field '${fieldName}' has all values equal to ${value}`,
    };
  }

  return { skip: false };
}

/**
 * Evaluate $in filter.
 */
function evaluateInFilter(
  dataFile: DataFile,
  fieldId: number,
  fieldName: string,
  values: unknown[],
  type: IcebergPrimitiveType
): PredicateResult {
  // Empty array means nothing can match
  if (values.length === 0) {
    return {
      skip: true,
      reason: `Field '${fieldName}' $in with empty array`,
    };
  }

  // Get bounds
  const bounds = getBoundsFromDataFile(dataFile, fieldId, type);
  if (!bounds) {
    // No bounds available, can't skip
    return { skip: false };
  }

  const overlaps = evaluateInPredicate(bounds.lower, bounds.upper, values, type);
  if (!overlaps) {
    return {
      skip: true,
      reason: `Field '${fieldName}' bounds do not overlap any $in values`,
    };
  }

  return { skip: false };
}

/**
 * Evaluate a filter object against data file stats.
 */
function evaluateFilter(
  dataFile: DataFile,
  filter: Record<string, unknown>,
  configs: readonly VariantShredPropertyConfig[],
  fieldIdMap: Map<string, number>
): PredicateResult {
  for (const [key, value] of Object.entries(filter)) {
    // Handle logical operators
    if (key === '$and') {
      if (!Array.isArray(value)) continue;
      // AND: skip if any sub-filter can skip
      for (const subFilter of value) {
        if (!isPlainObject(subFilter)) continue;
        const result = evaluateFilter(dataFile, subFilter, configs, fieldIdMap);
        if (result.skip) {
          return result;
        }
      }
      continue;
    }

    if (key === '$or') {
      if (!Array.isArray(value)) continue;
      // OR: skip only if ALL sub-filters can skip
      let allSkip = true;
      let skipReason: string | undefined;

      for (const subFilter of value) {
        if (!isPlainObject(subFilter)) {
          allSkip = false;
          break;
        }
        const result = evaluateFilter(dataFile, subFilter, configs, fieldIdMap);
        if (!result.skip) {
          allSkip = false;
          break;
        }
        skipReason = result.reason;
      }

      if (allSkip && value.length > 0) {
        return { skip: true, reason: skipReason ?? 'All $or branches eliminated' };
      }
      continue;
    }

    // Skip other logical operators for now ($not, $nor)
    // But NOT variant field paths like '$data.title' - those have a dot
    if (key.startsWith('$') && !key.includes('.')) {
      continue;
    }

    // Regular field filter
    const result = evaluateFieldFilter(dataFile, key, value, configs, fieldIdMap);
    if (result.skip) {
      return result;
    }
  }

  return { skip: false };
}

// ============================================================================
// Main Export
// ============================================================================

/**
 * Determine whether a data file can be skipped during scan planning.
 *
 * Evaluates a filter predicate against the data file's shredded column
 * statistics. If the statistics definitively show that no rows in the
 * file can match the filter, returns skip=true.
 *
 * @param dataFile - The data file to evaluate
 * @param filter - The filter predicate (MongoDB-style query syntax)
 * @param configs - Variant shred configurations
 * @param fieldIdMap - Map from statistics paths to field IDs
 * @returns PredicateResult indicating whether to skip the file
 *
 * @example
 * ```ts
 * const configs = [{ columnName: '$data', fields: ['year'], fieldTypes: { year: 'int' } }];
 * const fieldIdMap = assignShreddedFieldIds(configs, 1000);
 *
 * const result = shouldSkipDataFile(
 *   dataFile,
 *   { '$data.year': { $gt: 2020 } },
 *   configs,
 *   fieldIdMap
 * );
 *
 * if (result.skip) {
 *   console.log('Skipping file:', result.reason);
 * }
 * ```
 */
export function shouldSkipDataFile(
  dataFile: DataFile,
  filter: Record<string, unknown>,
  configs: readonly VariantShredPropertyConfig[],
  fieldIdMap: Map<string, number>
): PredicateResult {
  return evaluateFilter(dataFile, filter, configs, fieldIdMap);
}
