/**
 * Shredded Column Statistics for Manifests
 *
 * This module provides utilities for tracking column statistics on shredded
 * variant paths in Iceberg manifest files. This enables efficient predicate
 * pushdown on variant columns by maintaining min/max bounds and counts for
 * each shredded field.
 *
 * @see https://iceberg.apache.org/spec/#variant-shredding
 */
import type { IcebergPrimitiveType, DataFile } from '../metadata/types.js';
import type { VariantShredPropertyConfig } from './config.js';
/**
 * Serialized statistics for a single shredded column.
 *
 * This type represents shredded column statistics with binary-encoded bounds,
 * ready for storage in Iceberg manifest files. The bounds are serialized using
 * Iceberg's standard binary encoding per type (e.g., little-endian for integers,
 * UTF-8 for strings).
 *
 * Use this type when:
 * - Writing statistics to manifest files
 * - Reading statistics from manifest files
 * - Merging statistics during manifest compaction
 *
 * @see {@link CollectedShreddedColumnStats} for the pre-serialization form with raw values
 * @see https://iceberg.apache.org/spec/#manifests
 */
export interface SerializedShreddedColumnStats {
    /** The statistics path (e.g., "$data.typed_value.title.typed_value") */
    readonly path: string;
    /** The field ID assigned to this shredded column */
    readonly fieldId: number;
    /** Lower bound of values (binary encoded per Iceberg spec) */
    readonly lowerBound?: Uint8Array;
    /** Upper bound of values (binary encoded per Iceberg spec) */
    readonly upperBound?: Uint8Array;
    /** Count of null values */
    readonly nullCount?: number;
    /** Count of non-null values */
    readonly valueCount?: number;
}
/**
 * @deprecated Use {@link SerializedShreddedColumnStats} instead.
 * This alias is provided for backward compatibility.
 */
export type ShreddedColumnStats = SerializedShreddedColumnStats;
/**
 * Options for creating shredded column statistics.
 */
export interface CreateShreddedStatsOptions {
    /** The statistics path */
    readonly path: string;
    /** The field ID */
    readonly fieldId: number;
    /** The Iceberg primitive type for this field */
    readonly type: IcebergPrimitiveType;
    /** Lower bound value (will be serialized) */
    readonly lowerBound?: unknown;
    /** Upper bound value (will be serialized) */
    readonly upperBound?: unknown;
    /** Count of null values */
    readonly nullCount?: number;
    /** Count of non-null values */
    readonly valueCount?: number;
}
/**
 * Get all shredded statistics paths from variant shred configurations.
 *
 * Returns the typed_value paths for each shredded field, which are used
 * as keys in column statistics maps.
 *
 * @param configs - Array of variant shred configurations
 * @returns Array of statistics paths (e.g., "$data.typed_value.title.typed_value")
 *
 * @example
 * ```ts
 * const configs = [{ columnName: '$data', fields: ['title', 'year'], fieldTypes: {} }];
 * const paths = getShreddedStatisticsPaths(configs);
 * // ['$data.typed_value.title.typed_value', '$data.typed_value.year.typed_value']
 * ```
 */
export declare function getShreddedStatisticsPaths(configs: readonly VariantShredPropertyConfig[]): string[];
/**
 * Assign unique field IDs to each shredded statistics path.
 *
 * Field IDs for shredded columns start from the specified starting ID
 * and increment sequentially. This ensures they don't conflict with
 * regular schema field IDs.
 *
 * @param configs - Array of variant shred configurations
 * @param startingId - The first field ID to assign
 * @returns Map from statistics path to field ID
 *
 * @example
 * ```ts
 * const configs = [{ columnName: '$data', fields: ['title'], fieldTypes: {} }];
 * const map = assignShreddedFieldIds(configs, 1000);
 * // Map { '$data.typed_value.title.typed_value' => 1000 }
 * ```
 */
export declare function assignShreddedFieldIds(configs: readonly VariantShredPropertyConfig[], startingId: number): Map<string, number>;
/**
 * Serialize a bound value based on its Iceberg primitive type.
 *
 * Uses Iceberg's standard binary encoding for each type:
 * - boolean: 1 byte (0 or 1)
 * - int: 4 bytes little-endian
 * - long: 8 bytes little-endian
 * - float: 4 bytes IEEE 754
 * - double: 8 bytes IEEE 754
 * - date: 4 bytes little-endian (days since epoch)
 * - timestamp/timestamptz: 8 bytes little-endian (microseconds since epoch)
 * - string: UTF-8 encoded bytes
 *
 * @param value - The value to serialize
 * @param type - The Iceberg primitive type
 * @returns Binary encoded value
 */
export declare function serializeShreddedBound(value: unknown, type: IcebergPrimitiveType): Uint8Array;
/**
 * Deserialize a bound value based on its Iceberg primitive type.
 *
 * @param data - Binary encoded value
 * @param type - The Iceberg primitive type
 * @returns Deserialized value
 */
export declare function deserializeShreddedBound(data: Uint8Array, type: IcebergPrimitiveType): unknown;
/**
 * Create a ShreddedColumnStats object with serialized bounds.
 *
 * @param options - Options for creating the stats
 * @returns ShreddedColumnStats with serialized bounds
 */
export declare function createShreddedColumnStats(options: CreateShreddedStatsOptions): ShreddedColumnStats;
/**
 * Apply shredded column statistics to a DataFile.
 *
 * Merges shredded stats into the existing DataFile statistics maps,
 * preserving any existing column statistics.
 *
 * @param dataFile - The DataFile to update
 * @param shreddedStats - Array of shredded column statistics
 * @returns Updated DataFile with shredded stats included
 */
export declare function applyShreddedStatsToDataFile(dataFile: DataFile, shreddedStats: readonly ShreddedColumnStats[]): DataFile;
/**
 * Merge two shredded column statistics.
 *
 * Used during manifest compaction to combine statistics from multiple
 * data files. Takes the minimum of lower bounds, maximum of upper bounds,
 * and sums the counts.
 *
 * @param stats1 - First statistics
 * @param stats2 - Second statistics
 * @param type - The Iceberg primitive type for comparison
 * @returns Merged statistics
 */
export declare function mergeShreddedStats(stats1: ShreddedColumnStats, stats2: ShreddedColumnStats, type: IcebergPrimitiveType): ShreddedColumnStats;
//# sourceMappingURL=manifest-stats.d.ts.map