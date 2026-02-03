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
import { compareValues } from './utils.js';

// ============================================================================
// Types
// ============================================================================

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

// ============================================================================
// Statistics Path Extraction
// ============================================================================

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
export function getShreddedStatisticsPaths(
  configs: readonly VariantShredPropertyConfig[]
): string[] {
  const paths: string[] = [];

  for (const config of configs) {
    for (const field of config.fields) {
      paths.push(`${config.columnName}.typed_value.${field}.typed_value`);
    }
  }

  return paths;
}

// ============================================================================
// Field ID Assignment
// ============================================================================

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
export function assignShreddedFieldIds(
  configs: readonly VariantShredPropertyConfig[],
  startingId: number
): Map<string, number> {
  const map = new Map<string, number>();
  let currentId = startingId;

  for (const config of configs) {
    for (const field of config.fields) {
      const path = `${config.columnName}.typed_value.${field}.typed_value`;
      map.set(path, currentId);
      currentId++;
    }
  }

  return map;
}

// ============================================================================
// Bounds Serialization
// ============================================================================

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
export function serializeShreddedBound(
  value: unknown,
  type: IcebergPrimitiveType
): Uint8Array {
  switch (type) {
    case 'boolean': {
      if (typeof value !== 'boolean') {
        throw new Error(`Expected boolean for type '${type}', got ${typeof value}`);
      }
      const result = new Uint8Array(1);
      result[0] = value ? 1 : 0;
      return result;
    }

    case 'int':
    case 'date': {
      if (typeof value !== 'number') {
        throw new Error(`Expected number for type '${type}', got ${typeof value}`);
      }
      const buffer = new ArrayBuffer(4);
      const view = new DataView(buffer);
      view.setInt32(0, value, true); // little-endian
      return new Uint8Array(buffer);
    }

    case 'long':
    case 'timestamp':
    case 'timestamptz':
    case 'timestamp_ns':
    case 'timestamptz_ns':
    case 'time': {
      if (typeof value !== 'number' && typeof value !== 'bigint') {
        throw new Error(`Expected number or bigint for type '${type}', got ${typeof value}`);
      }
      const buffer = new ArrayBuffer(8);
      const view = new DataView(buffer);
      const bigValue = typeof value === 'bigint' ? value : BigInt(value);
      view.setBigInt64(0, bigValue, true); // little-endian
      return new Uint8Array(buffer);
    }

    case 'float': {
      if (typeof value !== 'number') {
        throw new Error(`Expected number for type '${type}', got ${typeof value}`);
      }
      const buffer = new ArrayBuffer(4);
      const view = new DataView(buffer);
      view.setFloat32(0, value, true); // little-endian
      return new Uint8Array(buffer);
    }

    case 'double': {
      if (typeof value !== 'number') {
        throw new Error(`Expected number for type '${type}', got ${typeof value}`);
      }
      const buffer = new ArrayBuffer(8);
      const view = new DataView(buffer);
      view.setFloat64(0, value, true); // little-endian
      return new Uint8Array(buffer);
    }

    case 'string':
    case 'uuid': {
      if (typeof value !== 'string') {
        throw new Error(`Expected string for type '${type}', got ${typeof value}`);
      }
      return new TextEncoder().encode(value);
    }

    case 'binary':
    case 'fixed': {
      if (value instanceof Uint8Array) {
        return value;
      }
      if (Array.isArray(value) && value.every((v) => typeof v === 'number')) {
        return new Uint8Array(value);
      }
      throw new Error(`Expected Uint8Array or number[] for type '${type}', got ${typeof value}`);
    }

    default: {
      // For decimal, variant, unknown, or geospatial types
      // Just encode as string representation
      return new TextEncoder().encode(String(value));
    }
  }
}

// ============================================================================
// Bounds Deserialization
// ============================================================================

/**
 * Deserialize a bound value based on its Iceberg primitive type.
 *
 * @param data - Binary encoded value
 * @param type - The Iceberg primitive type
 * @returns Deserialized value
 */
export function deserializeShreddedBound(
  data: Uint8Array,
  type: IcebergPrimitiveType
): unknown {
  switch (type) {
    case 'boolean': {
      return data[0] !== 0;
    }

    case 'int':
    case 'date': {
      const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
      return view.getInt32(0, true); // little-endian
    }

    case 'long':
    case 'timestamp':
    case 'timestamptz':
    case 'timestamp_ns':
    case 'timestamptz_ns':
    case 'time': {
      const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
      return view.getBigInt64(0, true); // little-endian
    }

    case 'float': {
      const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
      return view.getFloat32(0, true); // little-endian
    }

    case 'double': {
      const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
      return view.getFloat64(0, true); // little-endian
    }

    case 'string':
    case 'uuid': {
      return new TextDecoder().decode(data);
    }

    case 'binary':
    case 'fixed': {
      return data;
    }

    default: {
      // For decimal, variant, unknown, or geospatial types
      // Decode as string
      return new TextDecoder().decode(data);
    }
  }
}

// ============================================================================
// Create Shredded Column Stats
// ============================================================================

/**
 * Create a ShreddedColumnStats object with serialized bounds.
 *
 * @param options - Options for creating the stats
 * @returns ShreddedColumnStats with serialized bounds
 */
export function createShreddedColumnStats(
  options: CreateShreddedStatsOptions
): ShreddedColumnStats {
  const { path, fieldId, type, lowerBound, upperBound, nullCount, valueCount } = options;

  const stats: ShreddedColumnStats = {
    path,
    fieldId,
    lowerBound: lowerBound !== undefined ? serializeShreddedBound(lowerBound, type) : undefined,
    upperBound: upperBound !== undefined ? serializeShreddedBound(upperBound, type) : undefined,
    nullCount,
    valueCount,
  };

  return stats;
}

// ============================================================================
// Apply Stats to DataFile
// ============================================================================

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
export function applyShreddedStatsToDataFile(
  dataFile: DataFile,
  shreddedStats: readonly ShreddedColumnStats[]
): DataFile {
  // Create mutable copies of the stats maps
  const lowerBounds: Record<number, Uint8Array | string> = {
    ...(dataFile['lower-bounds'] ?? {}),
  };
  const upperBounds: Record<number, Uint8Array | string> = {
    ...(dataFile['upper-bounds'] ?? {}),
  };
  const nullValueCounts: Record<number, number> = {
    ...(dataFile['null-value-counts'] ?? {}),
  };
  const valueCounts: Record<number, number> = {
    ...(dataFile['value-counts'] ?? {}),
  };

  // Add shredded stats
  for (const stats of shreddedStats) {
    if (stats.lowerBound !== undefined) {
      lowerBounds[stats.fieldId] = stats.lowerBound;
    }
    if (stats.upperBound !== undefined) {
      upperBounds[stats.fieldId] = stats.upperBound;
    }
    if (stats.nullCount !== undefined) {
      nullValueCounts[stats.fieldId] = stats.nullCount;
    }
    if (stats.valueCount !== undefined) {
      valueCounts[stats.fieldId] = stats.valueCount;
    }
  }

  // Return updated DataFile
  return {
    ...dataFile,
    'lower-bounds': Object.keys(lowerBounds).length > 0 ? lowerBounds : undefined,
    'upper-bounds': Object.keys(upperBounds).length > 0 ? upperBounds : undefined,
    'null-value-counts': Object.keys(nullValueCounts).length > 0 ? nullValueCounts : undefined,
    'value-counts': Object.keys(valueCounts).length > 0 ? valueCounts : undefined,
  };
}

// ============================================================================
// Merge Stats (for manifest compaction)
// ============================================================================

/**
 * Compare two serialized bounds based on type.
 *
 * @param a - First bound
 * @param b - Second bound
 * @param type - The Iceberg primitive type
 * @returns Negative if a < b, positive if a > b, zero if equal
 */
function compareBounds(a: Uint8Array, b: Uint8Array, type: IcebergPrimitiveType): number {
  const valueA = deserializeShreddedBound(a, type);
  const valueB = deserializeShreddedBound(b, type);
  return compareValues(valueA, valueB, type);
}

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
export function mergeShreddedStats(
  stats1: ShreddedColumnStats,
  stats2: ShreddedColumnStats,
  type: IcebergPrimitiveType
): ShreddedColumnStats {
  // Determine lower bound (minimum)
  let lowerBound: Uint8Array | undefined;
  if (stats1.lowerBound !== undefined && stats2.lowerBound !== undefined) {
    lowerBound =
      compareBounds(stats1.lowerBound, stats2.lowerBound, type) <= 0
        ? stats1.lowerBound
        : stats2.lowerBound;
  } else {
    lowerBound = stats1.lowerBound ?? stats2.lowerBound;
  }

  // Determine upper bound (maximum)
  let upperBound: Uint8Array | undefined;
  if (stats1.upperBound !== undefined && stats2.upperBound !== undefined) {
    upperBound =
      compareBounds(stats1.upperBound, stats2.upperBound, type) >= 0
        ? stats1.upperBound
        : stats2.upperBound;
  } else {
    upperBound = stats1.upperBound ?? stats2.upperBound;
  }

  // Sum counts
  let nullCount: number | undefined;
  if (stats1.nullCount !== undefined || stats2.nullCount !== undefined) {
    nullCount = (stats1.nullCount ?? 0) + (stats2.nullCount ?? 0);
  }

  let valueCount: number | undefined;
  if (stats1.valueCount !== undefined || stats2.valueCount !== undefined) {
    valueCount = (stats1.valueCount ?? 0) + (stats2.valueCount ?? 0);
  }

  return {
    path: stats1.path,
    fieldId: stats1.fieldId,
    lowerBound,
    upperBound,
    nullCount,
    valueCount,
  };
}
