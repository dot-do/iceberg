/**
 * Variant Statistics Collection
 *
 * This module provides functions for collecting min/max bounds and statistics
 * on shredded variant columns (typed_value columns).
 *
 * Statistics are essential for predicate pushdown and file pruning in query
 * engines. By collecting min/max bounds on shredded variant fields, queries
 * can skip files that don't contain matching values.
 *
 * @see https://iceberg.apache.org/spec/#variant-shredding
 * @see https://iceberg.apache.org/spec/#manifests
 */
import type { IcebergPrimitiveType, DataFile } from '../metadata/types.js';
import type { VariantShredPropertyConfig } from './config.js';
/**
 * Column values for a single shredded field.
 */
export interface ColumnValues {
    /** The field path/name within the variant (e.g., "title") */
    readonly path: string;
    /** The array of values for this column (may include nulls) */
    readonly values: readonly unknown[];
}
/**
 * Collected statistics for a single shredded column during the collection phase.
 *
 * This type represents shredded column statistics with raw (unserialized) bounds,
 * as collected during data file processing. The bounds contain the actual typed
 * values (strings, numbers, etc.) before binary serialization.
 *
 * Use this type when:
 * - Collecting statistics from data during write operations
 * - Processing column values before manifest serialization
 * - Working with statistics in memory before final encoding
 *
 * @see {@link SerializedShreddedColumnStats} for the serialized form with binary-encoded bounds
 * @see https://iceberg.apache.org/spec/#manifests
 */
export interface CollectedShreddedColumnStats {
    /** The field path/name within the variant */
    readonly path: string;
    /** The Iceberg primitive type of the field */
    readonly type: IcebergPrimitiveType;
    /** The assigned field ID for this column */
    readonly fieldId: number;
    /** Total number of values (including nulls) */
    readonly valueCount: number;
    /** Number of null values */
    readonly nullCount: number;
    /** Minimum value (undefined if all values are null) */
    readonly lowerBound?: unknown;
    /** Maximum value (undefined if all values are null) */
    readonly upperBound?: unknown;
}
/**
 * @deprecated Use {@link CollectedShreddedColumnStats} instead.
 * This alias is provided for backward compatibility.
 */
export type ShreddedColumnStats = CollectedShreddedColumnStats;
/**
 * Result of collecting statistics for shredded columns.
 */
export interface CollectedStats {
    /** Statistics for each shredded column */
    readonly stats: readonly CollectedShreddedColumnStats[];
    /** Map from field path to assigned field ID */
    readonly fieldIdMap: Map<string, number>;
}
/**
 * Options for collecting shredded column statistics.
 */
export interface CollectStatsOptions {
    /** Maximum length for string bounds (default: 16) */
    maxStringLength?: number;
}
/**
 * Collect statistics for shredded variant columns.
 *
 * For each configured shredded field, computes:
 * - Value count (total including nulls)
 * - Null count
 * - Lower bound (min value)
 * - Upper bound (max value)
 *
 * @param columns - Array of column values to collect stats from
 * @param configs - Variant shred configurations specifying fields and types
 * @param startingFieldId - First field ID to assign to shredded columns
 * @param options - Optional collection options
 * @returns Collected statistics and field ID mapping
 *
 * @example
 * ```ts
 * const columns = [
 *   { path: 'title', values: ['Movie A', 'Movie B'] },
 *   { path: 'year', values: [2020, 2021] },
 * ];
 *
 * const configs = [{
 *   columnName: '$data',
 *   fields: ['title', 'year'],
 *   fieldTypes: { title: 'string', year: 'int' },
 * }];
 *
 * const result = collectShreddedColumnStats(columns, configs, 100);
 * // result.stats contains statistics for each field
 * // result.fieldIdMap maps 'title' -> 100, 'year' -> 101
 * ```
 */
export declare function collectShreddedColumnStats(columns: readonly ColumnValues[], configs: readonly VariantShredPropertyConfig[], startingFieldId: number, options?: CollectStatsOptions): CollectedStats;
/**
 * Compute lexicographic min/max bounds for string values.
 *
 * @param values - Array of string values (may include nulls)
 * @param maxLength - Maximum length for bounds (default: 16)
 * @returns Object with lower and upper bounds (null if no non-null values)
 */
export declare function computeStringBounds(values: readonly (string | null)[], maxLength?: number): {
    lower: string | null;
    upper: string | null;
};
/**
 * Compute numeric min/max bounds.
 *
 * @param values - Array of numeric values (may include nulls)
 * @returns Object with lower and upper bounds (null if no non-null values)
 */
export declare function computeNumericBounds<T extends number | bigint>(values: readonly (T | null)[]): {
    lower: T | null;
    upper: T | null;
};
/**
 * Compute timestamp min/max bounds in microseconds.
 *
 * @param values - Array of Date objects or numeric timestamps (may include nulls)
 * @returns Object with lower and upper bounds in microseconds (null if no non-null values)
 */
export declare function computeTimestampBounds(values: readonly (Date | number | null)[]): {
    lower: number | null;
    upper: number | null;
};
/**
 * Compute boolean min/max bounds.
 * false < true in Iceberg semantics.
 *
 * @param values - Array of boolean values (may include nulls)
 * @returns Object with lower and upper bounds (null if no non-null values)
 */
export declare function computeBooleanBounds(values: readonly (boolean | null)[]): {
    lower: boolean | null;
    upper: boolean | null;
};
/**
 * Add shredded column statistics to a DataFile.
 *
 * Merges the collected statistics into the DataFile's stats maps,
 * encoding bounds as Uint8Array per Iceberg spec.
 *
 * @param dataFile - The DataFile to add stats to
 * @param stats - Collected statistics from collectShreddedColumnStats
 * @returns A new DataFile with merged statistics
 *
 * @example
 * ```ts
 * const collected = collectShreddedColumnStats(columns, configs, 100);
 * const dataFileWithStats = addShreddedStatsToDataFile(dataFile, collected);
 * manifest.addDataFile(dataFileWithStats);
 * ```
 */
export declare function addShreddedStatsToDataFile(dataFile: DataFile, stats: CollectedStats): DataFile;
//# sourceMappingURL=stats-collector.d.ts.map