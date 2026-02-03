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
/**
 * Result of evaluating a predicate against data file statistics.
 */
export interface PredicateResult {
    /** Whether to skip this data file (true = definitely no matches) */
    readonly skip: boolean;
    /** Human-readable reason for the skip decision (useful for debugging) */
    readonly reason?: string;
}
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
export declare function boundsOverlapValue(lower: unknown, upper: unknown, operator: string, value: unknown, type: IcebergPrimitiveType): boolean;
/**
 * Evaluate an $in predicate against bounds.
 *
 * @param lower - Lower bound of values in the data file
 * @param upper - Upper bound of values in the data file
 * @param values - Array of values in the $in set
 * @param type - The Iceberg primitive type for comparison
 * @returns True if any value overlaps bounds (don't skip), false if none overlap (skip)
 */
export declare function evaluateInPredicate(lower: unknown, upper: unknown, values: unknown[], type: IcebergPrimitiveType): boolean;
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
export declare function shouldSkipDataFile(dataFile: DataFile, filter: Record<string, unknown>, configs: readonly VariantShredPropertyConfig[], fieldIdMap: Map<string, number>): PredicateResult;
//# sourceMappingURL=predicate-pushdown.d.ts.map