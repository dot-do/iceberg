/**
 * Row Group Filtering with Variant Statistics
 *
 * This module provides utilities for filtering data files based on column statistics.
 * When variant columns are shredded, their statistics can be used to prune files
 * that definitely don't contain matching rows, enabling efficient predicate pushdown.
 *
 * @see https://iceberg.apache.org/spec/#variant-shredding
 */

import type { DataFile, IcebergPrimitiveType } from '../metadata/types.js';
import type { VariantShredPropertyConfig } from './config.js';
import { deserializeShreddedBound } from './manifest-stats.js';
import { compareValues } from './utils.js';

// ============================================================================
// Types
// ============================================================================

/**
 * A range predicate representing a filter condition.
 *
 * Supports lower and upper bounds with inclusive or exclusive semantics,
 * as well as point sets for $in operators.
 */
export interface RangePredicate {
  /** Lower bound (inclusive): value >= lowerInclusive */
  readonly lowerInclusive?: unknown;
  /** Lower bound (exclusive): value > lowerExclusive */
  readonly lowerExclusive?: unknown;
  /** Upper bound (inclusive): value <= upperInclusive */
  readonly upperInclusive?: unknown;
  /** Upper bound (exclusive): value < upperExclusive */
  readonly upperExclusive?: unknown;
  /** Set of specific values for $in operator */
  readonly points?: readonly unknown[];
}

/**
 * Statistics about filtered files.
 */
export interface FilterStats {
  /** Total number of files before filtering */
  readonly totalFiles: number;
  /** Number of files skipped (pruned) */
  readonly skippedFiles: number;
  /** Map from field path to number of files skipped by that field */
  readonly skippedByField: Map<string, number>;
}

// ============================================================================
// Predicate Creation
// ============================================================================

/**
 * Create a range predicate from a filter operator and value.
 *
 * @param operator - The filter operator ($gt, $gte, $lt, $lte, $eq, $in)
 * @param value - The value to compare against
 * @returns A RangePredicate representing the filter condition
 *
 * @example
 * ```ts
 * const pred = createRangePredicate('$gte', 10);
 * // { lowerInclusive: 10 }
 *
 * const eqPred = createRangePredicate('$eq', 50);
 * // { lowerInclusive: 50, upperInclusive: 50 }
 * ```
 */
export function createRangePredicate(operator: string, value: unknown): RangePredicate {
  switch (operator) {
    case '$gt':
      return { lowerExclusive: value };

    case '$gte':
      return { lowerInclusive: value };

    case '$lt':
      return { upperExclusive: value };

    case '$lte':
      return { upperInclusive: value };

    case '$eq':
      // Equality is a point range: [value, value]
      return { lowerInclusive: value, upperInclusive: value };

    case '$in':
      // $in is a set of discrete points
      return { points: Array.isArray(value) ? value : [value] };

    default:
      // Unknown operator - return empty predicate (matches all)
      return {};
  }
}

// ============================================================================
// Predicate Evaluation
// ============================================================================

/**
 * Check if a value is within a range [lower, upper].
 */
function isInRange(value: unknown, lower: unknown, upper: unknown): boolean {
  if (lower !== undefined && compareValues(value, lower) < 0) {
    return false;
  }
  if (upper !== undefined && compareValues(value, upper) > 0) {
    return false;
  }
  return true;
}

/**
 * Evaluate a range predicate against file statistics bounds.
 *
 * Returns true if the predicate range overlaps with the file's value range,
 * meaning the file might contain matching rows. Returns false if the ranges
 * definitely don't overlap, meaning the file can be safely skipped.
 *
 * @param predicate - The range predicate to evaluate
 * @param fileLower - Lower bound of values in the file (undefined if unknown)
 * @param fileUpper - Upper bound of values in the file (undefined if unknown)
 * @param type - The Iceberg primitive type for comparison
 * @returns true if the file might contain matching rows, false if it definitely doesn't
 *
 * @example
 * ```ts
 * // File has values 0-100, predicate is > 50
 * const pred = { lowerExclusive: 50 };
 * const result = evaluateRangePredicate(pred, 0, 100, 'int');
 * // true - file might contain values > 50
 * ```
 */
export function evaluateRangePredicate(
  predicate: RangePredicate,
  fileLower: unknown,
  fileUpper: unknown,
  _type: IcebergPrimitiveType
): boolean {
  // If file bounds are missing, assume it might match (safe default)
  if (fileLower === undefined && fileUpper === undefined) {
    return true;
  }

  // Handle $in (points) - check if any point is within file range
  if (predicate.points !== undefined && predicate.points.length > 0) {
    // If either bound is missing, we can't prune based on points
    if (fileLower === undefined || fileUpper === undefined) {
      return true;
    }

    for (const point of predicate.points) {
      if (isInRange(point, fileLower, fileUpper)) {
        return true;
      }
    }
    return false;
  }

  // Get predicate bounds
  const predLower = predicate.lowerInclusive ?? predicate.lowerExclusive;
  const predLowerInclusive = predicate.lowerInclusive !== undefined;
  const predUpper = predicate.upperInclusive ?? predicate.upperExclusive;
  const predUpperInclusive = predicate.upperInclusive !== undefined;

  // Check if ranges overlap
  // Two ranges [a, b] and [c, d] overlap if a <= d AND c <= b

  // Check: predicate lower vs file upper
  if (predLower !== undefined && fileUpper !== undefined) {
    const cmp = compareValues(predLower, fileUpper);
    if (cmp > 0) {
      // predLower > fileUpper - no overlap
      return false;
    }
    if (cmp === 0 && !predLowerInclusive) {
      // predLower == fileUpper but exclusive - no overlap
      return false;
    }
  }

  // Check: file lower vs predicate upper
  if (fileLower !== undefined && predUpper !== undefined) {
    const cmp = compareValues(fileLower, predUpper);
    if (cmp > 0) {
      // fileLower > predUpper - no overlap
      return false;
    }
    if (cmp === 0 && !predUpperInclusive) {
      // fileLower == predUpper but exclusive - no overlap
      return false;
    }
  }

  return true;
}

// ============================================================================
// Predicate Combination
// ============================================================================

/**
 * Combine multiple predicates with AND semantics (intersection).
 *
 * @param predicates - Array of predicates to combine
 * @returns Combined predicate, or null if the intersection is empty
 *
 * @example
 * ```ts
 * const p1 = { lowerInclusive: 10 };  // x >= 10
 * const p2 = { upperInclusive: 100 }; // x <= 100
 * const combined = combinePredicatesAnd([p1, p2]);
 * // { lowerInclusive: 10, upperInclusive: 100 }
 * ```
 */
export function combinePredicatesAnd(predicates: readonly RangePredicate[]): RangePredicate | null {
  if (predicates.length === 0) {
    // Empty AND is "all values" - no restriction
    return {};
  }

  if (predicates.length === 1) {
    return predicates[0];
  }

  let lowerInclusive: unknown;
  let lowerExclusive: unknown;
  let upperInclusive: unknown;
  let upperExclusive: unknown;

  for (const pred of predicates) {
    // Handle points - convert to range
    if (pred.points !== undefined && pred.points.length > 0) {
      // $in with AND - find overlapping points
      // For simplicity, treat as range from min to max of points
      // This is an approximation; exact handling would filter individual points
      const sorted = [...pred.points].sort((a, b) => compareValues(a, b));
      const minPoint = sorted[0];
      const maxPoint = sorted[sorted.length - 1];

      if (lowerInclusive === undefined || compareValues(minPoint, lowerInclusive) > 0) {
        lowerInclusive = minPoint;
      }
      if (upperInclusive === undefined || compareValues(maxPoint, upperInclusive) < 0) {
        upperInclusive = maxPoint;
      }
      continue;
    }

    // Handle lower bounds (take the maximum)
    if (pred.lowerInclusive !== undefined) {
      if (
        lowerInclusive === undefined ||
        compareValues(pred.lowerInclusive, lowerInclusive) > 0
      ) {
        lowerInclusive = pred.lowerInclusive;
        lowerExclusive = undefined; // Inclusive takes precedence at same value
      } else if (
        compareValues(pred.lowerInclusive, lowerInclusive) === 0 &&
        lowerExclusive !== undefined
      ) {
        // Same value, switch to inclusive
        lowerExclusive = undefined;
      }
    }

    if (pred.lowerExclusive !== undefined) {
      const existingLower = lowerInclusive ?? lowerExclusive;
      if (existingLower === undefined || compareValues(pred.lowerExclusive, existingLower) > 0) {
        lowerExclusive = pred.lowerExclusive;
        lowerInclusive = undefined;
      } else if (
        lowerInclusive !== undefined &&
        compareValues(pred.lowerExclusive, lowerInclusive) === 0
      ) {
        // Exclusive bound equals inclusive - exclusive is stricter
        lowerExclusive = pred.lowerExclusive;
        lowerInclusive = undefined;
      }
    }

    // Handle upper bounds (take the minimum)
    if (pred.upperInclusive !== undefined) {
      if (
        upperInclusive === undefined ||
        compareValues(pred.upperInclusive, upperInclusive) < 0
      ) {
        upperInclusive = pred.upperInclusive;
        upperExclusive = undefined;
      } else if (
        compareValues(pred.upperInclusive, upperInclusive) === 0 &&
        upperExclusive !== undefined
      ) {
        upperExclusive = undefined;
      }
    }

    if (pred.upperExclusive !== undefined) {
      const existingUpper = upperInclusive ?? upperExclusive;
      if (existingUpper === undefined || compareValues(pred.upperExclusive, existingUpper) < 0) {
        upperExclusive = pred.upperExclusive;
        upperInclusive = undefined;
      } else if (
        upperInclusive !== undefined &&
        compareValues(pred.upperExclusive, upperInclusive) === 0
      ) {
        upperExclusive = pred.upperExclusive;
        upperInclusive = undefined;
      }
    }
  }

  // Check if the resulting range is valid (non-empty)
  const lower = lowerInclusive ?? lowerExclusive;
  const upper = upperInclusive ?? upperExclusive;

  if (lower !== undefined && upper !== undefined) {
    const cmp = compareValues(lower, upper);
    if (cmp > 0) {
      // lower > upper - empty range
      return null;
    }
    if (cmp === 0) {
      // lower == upper - must both be inclusive for non-empty
      if (lowerExclusive !== undefined || upperExclusive !== undefined) {
        return null;
      }
    }
  }

  return {
    lowerInclusive,
    lowerExclusive,
    upperInclusive,
    upperExclusive,
  };
}

/**
 * Combine multiple predicates with OR semantics (union).
 *
 * @param predicates - Array of predicates to combine
 * @returns Array of predicates representing the union (may not be contiguous)
 *
 * @example
 * ```ts
 * const p1 = { lowerInclusive: 0, upperInclusive: 10 };
 * const p2 = { lowerInclusive: 20, upperInclusive: 30 };
 * const combined = combinePredicatesOr([p1, p2]);
 * // [{ lowerInclusive: 0, upperInclusive: 10 }, { lowerInclusive: 20, upperInclusive: 30 }]
 * ```
 */
export function combinePredicatesOr(predicates: readonly RangePredicate[]): RangePredicate[] {
  if (predicates.length === 0) {
    return [];
  }

  if (predicates.length === 1) {
    return [...predicates];
  }

  // For OR, we return the union which may be multiple disjoint ranges
  // For simplicity, we try to merge overlapping ranges

  // Convert predicates to sortable form
  interface SortableRange {
    lower: unknown;
    lowerInclusive: boolean;
    upper: unknown;
    upperInclusive: boolean;
    points?: readonly unknown[];
  }

  const ranges: SortableRange[] = predicates.map((pred) => {
    // Handle points
    if (pred.points !== undefined) {
      return { lower: undefined, lowerInclusive: false, upper: undefined, upperInclusive: false, points: pred.points };
    }

    return {
      lower: pred.lowerInclusive ?? pred.lowerExclusive,
      lowerInclusive: pred.lowerInclusive !== undefined,
      upper: pred.upperInclusive ?? pred.upperExclusive,
      upperInclusive: pred.upperInclusive !== undefined,
    };
  });

  // Separate points predicates
  const pointsPredicates = ranges.filter((r) => r.points !== undefined);
  const rangePredicates = ranges.filter((r) => r.points === undefined);

  // Sort ranges by lower bound
  rangePredicates.sort((a, b) => {
    if (a.lower === undefined) return -1;
    if (b.lower === undefined) return 1;
    return compareValues(a.lower, b.lower);
  });

  // Merge overlapping ranges
  const merged: SortableRange[] = [];

  for (const range of rangePredicates) {
    if (merged.length === 0) {
      merged.push(range);
      continue;
    }

    const last = merged[merged.length - 1];

    // Check if ranges overlap or are adjacent
    const canMerge =
      last.upper === undefined ||
      range.lower === undefined ||
      compareValues(last.upper, range.lower) >= 0 ||
      (compareValues(last.upper, range.lower) === -1 &&
        ((last.upperInclusive && range.lowerInclusive) || compareValues(last.upper, range.lower) === 0));

    if (canMerge) {
      // Merge: take min lower, max upper
      merged[merged.length - 1] = {
        lower: last.lower === undefined ? last.lower : (range.lower === undefined ? range.lower : (compareValues(last.lower, range.lower) <= 0 ? last.lower : range.lower)),
        lowerInclusive: last.lower === undefined || range.lower === undefined ? true : (compareValues(last.lower, range.lower) < 0 ? last.lowerInclusive : (compareValues(last.lower, range.lower) > 0 ? range.lowerInclusive : (last.lowerInclusive || range.lowerInclusive))),
        upper: last.upper === undefined ? last.upper : (range.upper === undefined ? range.upper : (compareValues(last.upper, range.upper) >= 0 ? last.upper : range.upper)),
        upperInclusive: last.upper === undefined || range.upper === undefined ? true : (compareValues(last.upper, range.upper) > 0 ? last.upperInclusive : (compareValues(last.upper, range.upper) < 0 ? range.upperInclusive : (last.upperInclusive || range.upperInclusive))),
      };
    } else {
      merged.push(range);
    }
  }

  // Convert back to RangePredicate
  const result: RangePredicate[] = merged.map((r) => ({
    lowerInclusive: r.lowerInclusive ? r.lower : undefined,
    lowerExclusive: !r.lowerInclusive ? r.lower : undefined,
    upperInclusive: r.upperInclusive ? r.upper : undefined,
    upperExclusive: !r.upperInclusive ? r.upper : undefined,
  }));

  // Add back points predicates
  for (const p of pointsPredicates) {
    result.push({ points: p.points });
  }

  return result;
}

// ============================================================================
// Data File Filtering
// ============================================================================

/**
 * Parse a filter path to extract column name and field name.
 *
 * @param path - Filter path like "$data.age" or "$data.nested.field"
 * @returns Object with columnName and fieldName, or null if invalid
 */
function parseFilterPath(path: string): { columnName: string; fieldName: string } | null {
  if (!path.startsWith('$')) {
    return null;
  }

  const dotIndex = path.indexOf('.');
  if (dotIndex === -1) {
    return null;
  }

  return {
    columnName: path.substring(0, dotIndex),
    fieldName: path.substring(dotIndex + 1),
  };
}

/**
 * Get the type for a field from configs.
 */
function getFieldType(
  columnName: string,
  fieldName: string,
  configs: readonly VariantShredPropertyConfig[]
): IcebergPrimitiveType | undefined {
  const config = configs.find((c) => c.columnName === columnName);
  if (!config) return undefined;

  return config.fieldTypes[fieldName];
}

/**
 * Get the statistics path for a field.
 */
function getStatsPath(columnName: string, fieldName: string): string {
  return `${columnName}.typed_value.${fieldName}.typed_value`;
}

/**
 * Parse filter operators into predicates.
 */
function parseFilterOperators(
  value: unknown
): { predicates: RangePredicate[]; hasUnknownOperators: boolean } {
  const predicates: RangePredicate[] = [];
  let hasUnknownOperators = false;

  if (value === null || value === undefined) {
    return { predicates: [], hasUnknownOperators: true };
  }

  if (typeof value !== 'object') {
    // Implicit $eq
    predicates.push(createRangePredicate('$eq', value));
    return { predicates, hasUnknownOperators };
  }

  const filterObj = value as Record<string, unknown>;

  for (const [op, opValue] of Object.entries(filterObj)) {
    switch (op) {
      case '$eq':
      case '$gt':
      case '$gte':
      case '$lt':
      case '$lte':
      case '$in':
        predicates.push(createRangePredicate(op, opValue));
        break;

      case '$ne':
        // $ne can only prune if the file contains only the excluded value
        predicates.push({ points: [opValue] }); // Special handling needed
        hasUnknownOperators = true; // Mark as needing special handling
        break;

      default:
        // Unknown operator - can't prune safely
        hasUnknownOperators = true;
    }
  }

  return { predicates, hasUnknownOperators };
}

/**
 * Evaluate filter against a data file for a specific field.
 *
 * @returns true if file might match, false if it definitely doesn't
 */
function evaluateFieldFilter(
  dataFile: DataFile,
  fieldId: number,
  predicates: readonly RangePredicate[],
  type: IcebergPrimitiveType,
  hasNeOperator: boolean,
  neValue?: unknown
): boolean {
  // Get file bounds
  const lowerBounds = dataFile['lower-bounds'];
  const upperBounds = dataFile['upper-bounds'];

  const lowerBytes = lowerBounds?.[fieldId];
  const upperBytes = upperBounds?.[fieldId];

  // Deserialize bounds
  const fileLower =
    lowerBytes instanceof Uint8Array
      ? deserializeShreddedBound(lowerBytes, type)
      : typeof lowerBytes === 'string'
        ? lowerBytes
        : undefined;

  const fileUpper =
    upperBytes instanceof Uint8Array
      ? deserializeShreddedBound(upperBytes, type)
      : typeof upperBytes === 'string'
        ? upperBytes
        : undefined;

  // Special handling for $ne
  if (hasNeOperator && neValue !== undefined && fileLower !== undefined && fileUpper !== undefined) {
    // If file contains only the excluded value, we can skip it
    if (compareValues(fileLower, fileUpper) === 0 && compareValues(fileLower, neValue) === 0) {
      return false;
    }
    return true;
  }

  // Combine predicates with AND (all must match for file to be included)
  const combined = combinePredicatesAnd(predicates);
  if (combined === null) {
    // Empty intersection - no rows can match
    return false;
  }

  return evaluateRangePredicate(combined, fileLower, fileUpper, type);
}

/**
 * Filter data files based on column statistics and a query filter.
 *
 * Returns files that might contain matching rows. Files that definitely
 * don't match based on their statistics are excluded. This is the main
 * entry point for predicate pushdown during scan planning.
 *
 * @param dataFiles - Array of data files to filter
 * @param filter - Query filter object with field paths as keys
 * @param configs - Variant shred configurations
 * @param fieldIdMap - Map from statistics paths to field IDs
 * @returns Array of data files that might contain matching rows
 *
 * @example Complete predicate pushdown workflow
 * ```ts
 * import {
 *   extractVariantShredConfig,
 *   assignShreddedFieldIds,
 *   filterDataFiles
 * } from '@dotdo/iceberg';
 *
 * // 1. Get shredding config from table properties
 * const configs = extractVariantShredConfig(tableProperties);
 *
 * // 2. Assign field IDs (use same starting ID as when writing)
 * const fieldIdMap = assignShreddedFieldIds(configs, 1000);
 *
 * // 3. Filter files based on query predicate
 * const filter = { '$data.year': { $gte: 2020 }, '$data.rating': { $gt: 8.0 } };
 * const filesToScan = filterDataFiles(dataFiles, filter, configs, fieldIdMap);
 *
 * // Only scan files that might contain matching rows
 * for (const file of filesToScan) {
 *   await scanParquetFile(file['file-path']);
 * }
 * ```
 *
 * @example With filtering statistics
 * ```ts
 * const { files, stats } = filterDataFilesWithStats(
 *   dataFiles,
 *   { '$data.year': { $gte: 2020 } },
 *   configs,
 *   fieldIdMap
 * );
 *
 * console.log(`Scanning ${files.length} of ${stats.totalFiles} files`);
 * console.log(`Skipped ${stats.skippedFiles} files via predicate pushdown`);
 * ```
 */
export function filterDataFiles(
  dataFiles: readonly DataFile[],
  filter: Record<string, unknown>,
  configs: readonly VariantShredPropertyConfig[],
  fieldIdMap: Map<string, number>
): DataFile[] {
  if (dataFiles.length === 0) {
    return [];
  }

  // If no filter, return all files
  const filterKeys = Object.keys(filter);
  if (filterKeys.length === 0) {
    return [...dataFiles];
  }

  // Parse filter into field predicates
  interface FieldFilter {
    path: string;
    fieldId: number;
    type: IcebergPrimitiveType;
    predicates: RangePredicate[];
    hasUnknownOperators: boolean;
    neValue?: unknown;
  }

  const fieldFilters: FieldFilter[] = [];

  for (const [path, value] of Object.entries(filter)) {
    const parsed = parseFilterPath(path);
    if (!parsed) {
      // Not a variant path - can't filter, skip
      continue;
    }

    const statsPath = getStatsPath(parsed.columnName, parsed.fieldName);
    const fieldId = fieldIdMap.get(statsPath);
    if (fieldId === undefined) {
      // Field not in map - not shredded, can't filter
      continue;
    }

    const type = getFieldType(parsed.columnName, parsed.fieldName, configs);
    if (!type) {
      continue;
    }

    const { predicates, hasUnknownOperators } = parseFilterOperators(value);

    // Extract $ne value if present
    let neValue: unknown;
    if (
      typeof value === 'object' &&
      value !== null &&
      '$ne' in (value as Record<string, unknown>)
    ) {
      neValue = (value as Record<string, unknown>)['$ne'];
    }

    if (predicates.length === 0 && !hasUnknownOperators) {
      continue;
    }

    fieldFilters.push({
      path,
      fieldId,
      type,
      predicates,
      hasUnknownOperators,
      neValue,
    });
  }

  // If no usable field filters, return all files
  if (fieldFilters.length === 0) {
    return [...dataFiles];
  }

  // Filter data files
  return dataFiles.filter((dataFile) => {
    // All field filters must pass (AND semantics)
    for (const ff of fieldFilters) {
      // Skip if file has no stats and we need them
      if (!dataFile['lower-bounds'] && !dataFile['upper-bounds']) {
        // No stats - can't prune, assume match
        continue;
      }

      const matches = evaluateFieldFilter(
        dataFile,
        ff.fieldId,
        ff.predicates,
        ff.type,
        ff.hasUnknownOperators && ff.neValue !== undefined,
        ff.neValue
      );

      if (!matches) {
        return false;
      }
    }

    return true;
  });
}

/**
 * Filter data files and return statistics about the filtering.
 *
 * @param dataFiles - Array of data files to filter
 * @param filter - Query filter object
 * @param configs - Variant shred configurations
 * @param fieldIdMap - Map from statistics paths to field IDs
 * @returns Object with filtered files and filtering statistics
 */
export function filterDataFilesWithStats(
  dataFiles: readonly DataFile[],
  filter: Record<string, unknown>,
  configs: readonly VariantShredPropertyConfig[],
  fieldIdMap: Map<string, number>
): { files: DataFile[]; stats: FilterStats } {
  const skippedByField = new Map<string, number>();
  const totalFiles = dataFiles.length;

  if (dataFiles.length === 0) {
    return {
      files: [],
      stats: { totalFiles: 0, skippedFiles: 0, skippedByField },
    };
  }

  const filterKeys = Object.keys(filter);
  if (filterKeys.length === 0) {
    return {
      files: [...dataFiles],
      stats: { totalFiles, skippedFiles: 0, skippedByField },
    };
  }

  // Parse filter into field predicates
  interface FieldFilter {
    path: string;
    fieldId: number;
    type: IcebergPrimitiveType;
    predicates: RangePredicate[];
    hasUnknownOperators: boolean;
    neValue?: unknown;
  }

  const fieldFilters: FieldFilter[] = [];

  for (const [path, value] of Object.entries(filter)) {
    const parsed = parseFilterPath(path);
    if (!parsed) continue;

    const statsPath = getStatsPath(parsed.columnName, parsed.fieldName);
    const fieldId = fieldIdMap.get(statsPath);
    if (fieldId === undefined) continue;

    const type = getFieldType(parsed.columnName, parsed.fieldName, configs);
    if (!type) continue;

    const { predicates, hasUnknownOperators } = parseFilterOperators(value);

    let neValue: unknown;
    if (typeof value === 'object' && value !== null && '$ne' in (value as Record<string, unknown>)) {
      neValue = (value as Record<string, unknown>)['$ne'];
    }

    if (predicates.length === 0 && !hasUnknownOperators) continue;

    fieldFilters.push({ path, fieldId, type, predicates, hasUnknownOperators, neValue });
    skippedByField.set(path, 0);
  }

  if (fieldFilters.length === 0) {
    return {
      files: [...dataFiles],
      stats: { totalFiles, skippedFiles: 0, skippedByField },
    };
  }

  const files: DataFile[] = [];
  let skippedFiles = 0;

  for (const dataFile of dataFiles) {
    let included = true;
    let skipField: string | null = null;

    for (const ff of fieldFilters) {
      if (!dataFile['lower-bounds'] && !dataFile['upper-bounds']) {
        continue;
      }

      const matches = evaluateFieldFilter(
        dataFile,
        ff.fieldId,
        ff.predicates,
        ff.type,
        ff.hasUnknownOperators && ff.neValue !== undefined,
        ff.neValue
      );

      if (!matches) {
        included = false;
        skipField = ff.path;
        break;
      }
    }

    if (included) {
      files.push(dataFile);
    } else {
      skippedFiles++;
      if (skipField) {
        skippedByField.set(skipField, (skippedByField.get(skipField) || 0) + 1);
      }
    }
  }

  return {
    files,
    stats: { totalFiles, skippedFiles, skippedByField },
  };
}
