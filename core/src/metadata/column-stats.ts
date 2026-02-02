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

import type {
  IcebergSchema,
  IcebergPrimitiveType,
  IcebergType,
  DataFile,
} from './types.js';
import { encodeStatValue, truncateString } from '../avro/index.js';

// ============================================================================
// Types
// ============================================================================

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

// ============================================================================
// Column Statistics Collector Implementation
// ============================================================================

/**
 * Creates a statistics collector for a specific column type.
 */
export function createColumnStatsCollector(
  fieldId: number,
  type: IcebergPrimitiveType,
  maxStringLength: number = 16
): ColumnStatsCollector {
  let valueCount = 0;
  let nullCount = 0;
  let nanCount = 0;
  let columnSize = 0;
  let min: unknown = undefined;
  let max: unknown = undefined;

  const isFloatingPoint = type === 'float' || type === 'double';
  const isString = type === 'string';

  const compare = getComparator(type);

  return {
    add(value: unknown): void {
      valueCount++;

      if (value === null || value === undefined) {
        nullCount++;
        return;
      }

      // Track NaN for floating point types
      if (isFloatingPoint && typeof value === 'number' && Number.isNaN(value)) {
        nanCount++;
        return; // NaN values don't contribute to min/max
      }

      // Update column size estimate
      columnSize += estimateValueSize(value, type);

      // Update min/max bounds
      if (min === undefined || compare(value, min) < 0) {
        min = isString ? truncateString(value as string, maxStringLength) : value;
      }
      if (max === undefined || compare(value, max) > 0) {
        max = isString ? truncateUpperBound(value as string, maxStringLength) : value;
      }
    },

    getStats(): ColumnStatistics {
      return {
        fieldId,
        valueCount,
        nullCount,
        nanCount: isFloatingPoint ? nanCount : undefined,
        columnSize,
        lowerBound: min,
        upperBound: max,
      };
    },

    reset(): void {
      valueCount = 0;
      nullCount = 0;
      nanCount = 0;
      columnSize = 0;
      min = undefined;
      max = undefined;
    },
  };
}

// ============================================================================
// Multi-Column Statistics Collector
// ============================================================================

/**
 * Collects statistics for multiple columns simultaneously.
 */
export class FileStatsCollector {
  private collectors: Map<number, ColumnStatsCollector> = new Map();
  private schema: IcebergSchema;
  private maxStringLength: number;
  private includeFieldIds: Set<number> | null;
  private excludeFieldIds: Set<number>;

  constructor(options: ComputeStatsOptions) {
    this.schema = options.schema;
    this.maxStringLength = options.maxStringLength ?? 16;
    this.includeFieldIds = options.includeFieldIds
      ? new Set(options.includeFieldIds)
      : null;
    this.excludeFieldIds = new Set(options.excludeFieldIds ?? []);

    // Initialize collectors for each field
    for (const field of this.schema.fields) {
      if (this.shouldCollectStats(field.id)) {
        const primitiveType = getPrimitiveType(field.type);
        if (primitiveType) {
          this.collectors.set(
            field.id,
            createColumnStatsCollector(field.id, primitiveType, this.maxStringLength)
          );
        }
      }
    }
  }

  private shouldCollectStats(fieldId: number): boolean {
    if (this.excludeFieldIds.has(fieldId)) {
      return false;
    }
    if (this.includeFieldIds && !this.includeFieldIds.has(fieldId)) {
      return false;
    }
    return true;
  }

  /**
   * Add a row of data to the statistics.
   * The row should be a record mapping field names to values.
   */
  addRow(row: Record<string, unknown>): void {
    const fieldNameToId = new Map<string, number>();
    for (const field of this.schema.fields) {
      fieldNameToId.set(field.name, field.id);
    }

    for (const [name, value] of Object.entries(row)) {
      const fieldId = fieldNameToId.get(name);
      if (fieldId !== undefined) {
        const collector = this.collectors.get(fieldId);
        if (collector) {
          collector.add(value);
        }
      }
    }
  }

  /**
   * Add a value for a specific field.
   */
  addValue(fieldId: number, value: unknown): void {
    const collector = this.collectors.get(fieldId);
    if (collector) {
      collector.add(value);
    }
  }

  /**
   * Get the computed statistics for all columns.
   */
  getStats(): ColumnStatistics[] {
    const stats: ColumnStatistics[] = [];
    for (const collector of this.collectors.values()) {
      stats.push(collector.getStats());
    }
    return stats;
  }

  /**
   * Get encoded file statistics ready for use in a DataFile.
   */
  getEncodedStats(): ComputedFileStats {
    const stats = this.getStats();
    return encodeFileStats(stats, this.schema);
  }

  /**
   * Reset all collectors.
   */
  reset(): void {
    for (const collector of this.collectors.values()) {
      collector.reset();
    }
  }
}

// ============================================================================
// Statistics Encoding
// ============================================================================

/**
 * Encode column statistics to binary format for storage in manifest entries.
 */
export function encodeFileStats(
  stats: ColumnStatistics[],
  schema: IcebergSchema
): ComputedFileStats {
  const result: ComputedFileStats = {
    valueCounts: {},
    nullValueCounts: {},
    nanValueCounts: {},
    columnSizes: {},
    lowerBounds: {},
    upperBounds: {},
  };

  const fieldTypes = new Map<number, IcebergPrimitiveType>();
  for (const field of schema.fields) {
    const primitiveType = getPrimitiveType(field.type);
    if (primitiveType) {
      fieldTypes.set(field.id, primitiveType);
    }
  }

  for (const stat of stats) {
    const type = fieldTypes.get(stat.fieldId);
    if (!type) continue;

    if (stat.valueCount !== undefined && stat.valueCount > 0) {
      result.valueCounts[stat.fieldId] = stat.valueCount;
    }

    if (stat.nullCount !== undefined) {
      result.nullValueCounts[stat.fieldId] = stat.nullCount;
    }

    if (stat.nanCount !== undefined && stat.nanCount > 0) {
      result.nanValueCounts[stat.fieldId] = stat.nanCount;
    }

    if (stat.columnSize !== undefined && stat.columnSize > 0) {
      result.columnSizes[stat.fieldId] = stat.columnSize;
    }

    if (stat.lowerBound !== undefined && stat.lowerBound !== null) {
      result.lowerBounds[stat.fieldId] = encodeStatValue(stat.lowerBound, type);
    }

    if (stat.upperBound !== undefined && stat.upperBound !== null) {
      result.upperBounds[stat.fieldId] = encodeStatValue(stat.upperBound, type);
    }
  }

  return result;
}

/**
 * Apply computed statistics to a DataFile object.
 */
export function applyStatsToDataFile(
  dataFile: DataFile,
  stats: ComputedFileStats
): DataFile {
  const result = { ...dataFile };

  if (Object.keys(stats.valueCounts).length > 0) {
    result['value-counts'] = stats.valueCounts;
  }

  if (Object.keys(stats.nullValueCounts).length > 0) {
    result['null-value-counts'] = stats.nullValueCounts;
  }

  if (Object.keys(stats.nanValueCounts).length > 0) {
    result['nan-value-counts'] = stats.nanValueCounts;
  }

  if (Object.keys(stats.columnSizes).length > 0) {
    result['column-sizes'] = stats.columnSizes;
  }

  if (Object.keys(stats.lowerBounds).length > 0) {
    result['lower-bounds'] = stats.lowerBounds;
  }

  if (Object.keys(stats.upperBounds).length > 0) {
    result['upper-bounds'] = stats.upperBounds;
  }

  return result;
}

// ============================================================================
// Statistics Aggregation
// ============================================================================

/**
 * Aggregate statistics across multiple data files.
 * Used for computing manifest-level partition summaries.
 */
export function aggregateColumnStats(
  statsPerFile: ColumnStatistics[][],
  schema: IcebergSchema
): ColumnStatistics[] {
  const aggregated = new Map<number, ColumnStatistics>();
  const fieldTypes = new Map<number, IcebergPrimitiveType>();

  for (const field of schema.fields) {
    const primitiveType = getPrimitiveType(field.type);
    if (primitiveType) {
      fieldTypes.set(field.id, primitiveType);
    }
  }

  for (const fileStats of statsPerFile) {
    for (const stat of fileStats) {
      const type = fieldTypes.get(stat.fieldId);
      if (!type) continue;

      const compare = getComparator(type);
      const existing = aggregated.get(stat.fieldId);

      if (!existing) {
        aggregated.set(stat.fieldId, { ...stat });
        continue;
      }

      // Aggregate value count
      if (stat.valueCount !== undefined) {
        existing.valueCount = (existing.valueCount ?? 0) + stat.valueCount;
      }

      // Aggregate null count
      if (stat.nullCount !== undefined) {
        existing.nullCount = (existing.nullCount ?? 0) + stat.nullCount;
      }

      // Aggregate NaN count
      if (stat.nanCount !== undefined) {
        existing.nanCount = (existing.nanCount ?? 0) + stat.nanCount;
      }

      // Aggregate column size
      if (stat.columnSize !== undefined) {
        existing.columnSize = (existing.columnSize ?? 0) + stat.columnSize;
      }

      // Update min bound
      if (stat.lowerBound !== undefined && stat.lowerBound !== null) {
        if (existing.lowerBound === undefined || existing.lowerBound === null) {
          existing.lowerBound = stat.lowerBound;
        } else if (compare(stat.lowerBound, existing.lowerBound) < 0) {
          existing.lowerBound = stat.lowerBound;
        }
      }

      // Update max bound
      if (stat.upperBound !== undefined && stat.upperBound !== null) {
        if (existing.upperBound === undefined || existing.upperBound === null) {
          existing.upperBound = stat.upperBound;
        } else if (compare(stat.upperBound, existing.upperBound) > 0) {
          existing.upperBound = stat.upperBound;
        }
      }
    }
  }

  return Array.from(aggregated.values());
}

// ============================================================================
// Partition Field Summary
// ============================================================================

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
export function computePartitionSummaries(
  partitionValues: Record<string, unknown>[],
  partitionFieldTypes: Record<string, IcebergPrimitiveType>
): PartitionFieldSummary[] {
  const summaries: PartitionFieldSummary[] = [];

  for (const [fieldName, type] of Object.entries(partitionFieldTypes)) {
    let containsNull = false;
    let containsNan = false;
    let min: unknown = undefined;
    let max: unknown = undefined;

    const compare = getComparator(type);
    const isFloatingPoint = type === 'float' || type === 'double';

    for (const partition of partitionValues) {
      const value = partition[fieldName];

      if (value === null || value === undefined) {
        containsNull = true;
        continue;
      }

      if (isFloatingPoint && typeof value === 'number' && Number.isNaN(value)) {
        containsNan = true;
        continue;
      }

      if (min === undefined || compare(value, min) < 0) {
        min = value;
      }
      if (max === undefined || compare(value, max) > 0) {
        max = value;
      }
    }

    const summary: PartitionFieldSummary = {
      'contains-null': containsNull,
    };

    if (isFloatingPoint) {
      summary['contains-nan'] = containsNan;
    }

    if (min !== undefined) {
      summary['lower-bound'] = encodeStatValue(min, type);
    }

    if (max !== undefined) {
      summary['upper-bound'] = encodeStatValue(max, type);
    }

    summaries.push(summary);
  }

  return summaries;
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Get the primitive type from an Iceberg type.
 * Returns undefined for complex types.
 */
export function getPrimitiveType(type: IcebergType): IcebergPrimitiveType | undefined {
  if (typeof type === 'string') {
    return type as IcebergPrimitiveType;
  }
  return undefined;
}

/**
 * Get a comparator function for a primitive type.
 */
export function getComparator(
  type: IcebergPrimitiveType
): (a: unknown, b: unknown) => number {
  switch (type) {
    case 'boolean':
      return (a, b) => (a === b ? 0 : a ? 1 : -1);

    case 'int':
    case 'long':
    case 'float':
    case 'double':
    case 'date':
    case 'time':
    case 'timestamp':
    case 'timestamptz':
      return (a, b) => {
        const numA = typeof a === 'bigint' ? Number(a) : (a as number);
        const numB = typeof b === 'bigint' ? Number(b) : (b as number);
        return numA - numB;
      };

    case 'string':
    case 'uuid':
      return (a, b) => (a as string).localeCompare(b as string);

    case 'binary':
    case 'fixed':
      return compareBinary;

    case 'decimal':
      return (a, b) => {
        // Handle decimal as number or string
        const numA =
          typeof a === 'number' ? a : parseFloat(a as string);
        const numB =
          typeof b === 'number' ? b : parseFloat(b as string);
        return numA - numB;
      };

    default:
      return (a, b) => String(a).localeCompare(String(b));
  }
}

/**
 * Compare two binary values lexicographically.
 */
function compareBinary(a: unknown, b: unknown): number {
  const bytesA = a instanceof Uint8Array ? a : new Uint8Array(a as ArrayBuffer);
  const bytesB = b instanceof Uint8Array ? b : new Uint8Array(b as ArrayBuffer);

  const minLen = Math.min(bytesA.length, bytesB.length);
  for (let i = 0; i < minLen; i++) {
    if (bytesA[i] !== bytesB[i]) {
      return bytesA[i] - bytesB[i];
    }
  }

  return bytesA.length - bytesB.length;
}

/**
 * Estimate the serialized size of a value.
 */
export function estimateValueSize(value: unknown, type: IcebergPrimitiveType): number {
  switch (type) {
    case 'boolean':
      return 1;
    case 'int':
    case 'float':
    case 'date':
      return 4;
    case 'long':
    case 'double':
    case 'time':
    case 'timestamp':
    case 'timestamptz':
      return 8;
    case 'string':
    case 'uuid':
      return typeof value === 'string' ? value.length : 0;
    case 'binary':
      return value instanceof Uint8Array ? value.length : 0;
    case 'fixed':
      return value instanceof Uint8Array ? value.length : 0;
    case 'decimal':
      // Decimal is typically stored as a byte array
      return 16; // Estimate for decimal128
    default:
      return 0;
  }
}

/**
 * Truncate a string for upper bound.
 * For upper bounds, we need to find the smallest string that is >= all truncated values.
 */
export function truncateUpperBound(value: string, maxLength: number): string {
  if (value.length <= maxLength) {
    return value;
  }

  // Truncate and increment the last character to get a valid upper bound
  const truncated = value.slice(0, maxLength);

  // Find the rightmost character that can be incremented
  const chars = truncated.split('');
  for (let i = chars.length - 1; i >= 0; i--) {
    const code = chars[i].charCodeAt(0);
    if (code < 0x10ffff) {
      // Not at max Unicode code point
      chars[i] = String.fromCodePoint(code + 1);
      return chars.slice(0, i + 1).join('');
    }
  }

  // If all characters are max, just return the truncated string
  return truncated;
}

// ============================================================================
// Zone Map Helper
// ============================================================================

/**
 * Zone map metadata for quick file pruning.
 * Contains min/max bounds for key columns that enable skipping files
 * that cannot contain matching rows.
 */
export interface ZoneMap {
  /** Record count in the file */
  recordCount: number;
  /** Column bounds by field ID */
  bounds: Map<number, { min: unknown; max: unknown }>;
  /** Null counts by field ID */
  nullCounts: Map<number, number>;
}

/**
 * Create a zone map from file statistics.
 */
export function createZoneMapFromStats(stats: ColumnStatistics[]): ZoneMap {
  const bounds = new Map<number, { min: unknown; max: unknown }>();
  const nullCounts = new Map<number, number>();
  let recordCount = 0;

  for (const stat of stats) {
    if (stat.valueCount !== undefined) {
      recordCount = Math.max(recordCount, stat.valueCount);
    }

    if (stat.lowerBound !== undefined || stat.upperBound !== undefined) {
      bounds.set(stat.fieldId, {
        min: stat.lowerBound,
        max: stat.upperBound,
      });
    }

    if (stat.nullCount !== undefined) {
      nullCounts.set(stat.fieldId, stat.nullCount);
    }
  }

  return {
    recordCount,
    bounds,
    nullCounts,
  };
}

/**
 * Check if a zone map can be pruned for a given predicate.
 * Returns true if the file can be skipped (no matching rows possible).
 */
export function canPruneZoneMap(
  zoneMap: ZoneMap,
  fieldId: number,
  operator: '=' | '!=' | '<' | '<=' | '>' | '>=',
  value: unknown,
  type: IcebergPrimitiveType
): boolean {
  const bounds = zoneMap.bounds.get(fieldId);
  if (!bounds) {
    // No bounds available, cannot prune
    return false;
  }

  const { min, max } = bounds;
  if (min === undefined || max === undefined) {
    return false;
  }

  const compare = getComparator(type);

  switch (operator) {
    case '=':
      // Can prune if value is outside [min, max]
      return compare(value, min) < 0 || compare(value, max) > 0;

    case '!=':
      // Can prune only if all values equal the predicate value
      return compare(min, max) === 0 && compare(min, value) === 0;

    case '<':
      // Can prune if all values >= predicate
      return compare(min, value) >= 0;

    case '<=':
      // Can prune if all values > predicate
      return compare(min, value) > 0;

    case '>':
      // Can prune if all values <= predicate
      return compare(max, value) <= 0;

    case '>=':
      // Can prune if all values < predicate
      return compare(max, value) < 0;

    default:
      return false;
  }
}
