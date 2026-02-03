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
export declare function createRangePredicate(operator: string, value: unknown): RangePredicate;
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
export declare function evaluateRangePredicate(predicate: RangePredicate, fileLower: unknown, fileUpper: unknown, _type: IcebergPrimitiveType): boolean;
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
export declare function combinePredicatesAnd(predicates: readonly RangePredicate[]): RangePredicate | null;
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
export declare function combinePredicatesOr(predicates: readonly RangePredicate[]): RangePredicate[];
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
export declare function filterDataFiles(dataFiles: readonly DataFile[], filter: Record<string, unknown>, configs: readonly VariantShredPropertyConfig[], fieldIdMap: Map<string, number>): DataFile[];
/**
 * Filter data files and return statistics about the filtering.
 *
 * @param dataFiles - Array of data files to filter
 * @param filter - Query filter object
 * @param configs - Variant shred configurations
 * @param fieldIdMap - Map from statistics paths to field IDs
 * @returns Object with filtered files and filtering statistics
 */
export declare function filterDataFilesWithStats(dataFiles: readonly DataFile[], filter: Record<string, unknown>, configs: readonly VariantShredPropertyConfig[], fieldIdMap: Map<string, number>): {
    files: DataFile[];
    stats: FilterStats;
};
//# sourceMappingURL=row-group-filter.d.ts.map