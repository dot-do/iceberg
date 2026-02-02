/**
 * Iceberg Column Statistics
 *
 * Provides column-level statistics for manifest entries including:
 * - Min/max values (lower_bounds, upper_bounds)
 * - Null value counts
 * - NaN value counts (for floating point columns)
 * - Value counts
 * - Column sizes
 *
 * These statistics enable zone map pruning for efficient query execution.
 *
 * @see https://iceberg.apache.org/spec/#manifests
 */
import type { IcebergSchema, IcebergPrimitiveType, IcebergType, DataFile } from './types.js';
/**
 * Raw column statistics before serialization.
 * Used for computing stats from data before encoding to binary format.
 */
export interface ColumnStatistics {
    /** Schema field ID */
    fieldId: number;
    /** Total number of values (including nulls) */
    valueCount?: number;
    /** Number of null values */
    nullCount?: number;
    /** Number of NaN values (for float/double columns) */
    nanCount?: number;
    /** Column size in bytes */
    columnSize?: number;
    /** Minimum value (before serialization) */
    lowerBound?: unknown;
    /** Maximum value (before serialization) */
    upperBound?: unknown;
}
/**
 * Per-column statistics collector for tracking min/max/null/nan/size
 */
export interface ColumnStatsCollector {
    /** Add a value to the statistics */
    add(value: unknown): void;
    /** Get the computed statistics */
    getStats(): ColumnStatistics;
    /** Reset the collector */
    reset(): void;
}
/**
 * Options for computing file-level statistics
 */
export interface ComputeStatsOptions {
    /** Schema for type information */
    schema: IcebergSchema;
    /** Maximum string length for bounds (default: 16) */
    maxStringLength?: number;
    /** Fields to collect statistics for (default: all) */
    includeFieldIds?: number[];
    /** Fields to exclude from statistics */
    excludeFieldIds?: number[];
}
/**
 * Result of computing file-level statistics
 */
export interface ComputedFileStats {
    /** Value counts by field ID */
    valueCounts: Record<number, number>;
    /** Null value counts by field ID */
    nullValueCounts: Record<number, number>;
    /** NaN value counts by field ID (only for float/double) */
    nanValueCounts: Record<number, number>;
    /** Column sizes by field ID */
    columnSizes: Record<number, number>;
    /** Lower bounds by field ID (binary encoded) */
    lowerBounds: Record<number, Uint8Array>;
    /** Upper bounds by field ID (binary encoded) */
    upperBounds: Record<number, Uint8Array>;
}
/**
 * Creates a statistics collector for a specific column type.
 */
export declare function createColumnStatsCollector(fieldId: number, type: IcebergPrimitiveType, maxStringLength?: number): ColumnStatsCollector;
/**
 * Collects statistics for multiple columns simultaneously.
 */
export declare class FileStatsCollector {
    private collectors;
    private schema;
    private maxStringLength;
    private includeFieldIds;
    private excludeFieldIds;
    constructor(options: ComputeStatsOptions);
    private shouldCollectStats;
    /**
     * Add a row of data to the statistics.
     * The row should be a record mapping field names to values.
     */
    addRow(row: Record<string, unknown>): void;
    /**
     * Add a value for a specific field.
     */
    addValue(fieldId: number, value: unknown): void;
    /**
     * Get the computed statistics for all columns.
     */
    getStats(): ColumnStatistics[];
    /**
     * Get encoded file statistics ready for use in a DataFile.
     */
    getEncodedStats(): ComputedFileStats;
    /**
     * Reset all collectors.
     */
    reset(): void;
}
/**
 * Encode column statistics to binary format for storage in manifest entries.
 */
export declare function encodeFileStats(stats: ColumnStatistics[], schema: IcebergSchema): ComputedFileStats;
/**
 * Apply computed statistics to a DataFile object.
 */
export declare function applyStatsToDataFile(dataFile: DataFile, stats: ComputedFileStats): DataFile;
/**
 * Aggregate statistics across multiple data files.
 * Used for computing manifest-level partition summaries.
 */
export declare function aggregateColumnStats(statsPerFile: ColumnStatistics[][], schema: IcebergSchema): ColumnStatistics[];
/**
 * Partition field summary for manifest list entries.
 */
export interface PartitionFieldSummary {
    /** Whether any value is null */
    'contains-null': boolean;
    /** Whether any value is NaN (for floating point) */
    'contains-nan'?: boolean;
    /** Lower bound of partition values */
    'lower-bound'?: Uint8Array;
    /** Upper bound of partition values */
    'upper-bound'?: Uint8Array;
}
/**
 * Compute partition field summaries from manifest entries.
 */
export declare function computePartitionSummaries(partitionValues: Record<string, unknown>[], partitionFieldTypes: Record<string, IcebergPrimitiveType>): PartitionFieldSummary[];
/**
 * Get the primitive type from an Iceberg type.
 * Returns undefined for complex types.
 */
export declare function getPrimitiveType(type: IcebergType): IcebergPrimitiveType | undefined;
/**
 * Get a comparator function for a primitive type.
 */
export declare function getComparator(type: IcebergPrimitiveType): (a: unknown, b: unknown) => number;
/**
 * Estimate the serialized size of a value.
 */
export declare function estimateValueSize(value: unknown, type: IcebergPrimitiveType): number;
/**
 * Truncate a string for upper bound.
 * For upper bounds, we need to find the smallest string that is >= all truncated values.
 */
export declare function truncateUpperBound(value: string, maxLength: number): string;
/**
 * Zone map metadata for quick file pruning.
 * Contains min/max bounds for key columns that enable skipping files
 * that cannot contain matching rows.
 */
export interface ZoneMap {
    /** Record count in the file */
    recordCount: number;
    /** Column bounds by field ID */
    bounds: Map<number, {
        min: unknown;
        max: unknown;
    }>;
    /** Null counts by field ID */
    nullCounts: Map<number, number>;
}
/**
 * Create a zone map from file statistics.
 */
export declare function createZoneMapFromStats(stats: ColumnStatistics[]): ZoneMap;
/**
 * Check if a zone map can be pruned for a given predicate.
 * Returns true if the file can be skipped (no matching rows possible).
 */
export declare function canPruneZoneMap(zoneMap: ZoneMap, fieldId: number, operator: '=' | '!=' | '<' | '<=' | '>' | '>=', value: unknown, type: IcebergPrimitiveType): boolean;
//# sourceMappingURL=column-stats.d.ts.map