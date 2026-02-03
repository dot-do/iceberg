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
import { compareValues } from './utils.js';
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
export function getShreddedStatisticsPaths(configs) {
    const paths = [];
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
export function assignShreddedFieldIds(configs, startingId) {
    const map = new Map();
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
export function serializeShreddedBound(value, type) {
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
export function deserializeShreddedBound(data, type) {
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
export function createShreddedColumnStats(options) {
    const { path, fieldId, type, lowerBound, upperBound, nullCount, valueCount } = options;
    const stats = {
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
export function applyShreddedStatsToDataFile(dataFile, shreddedStats) {
    // Create mutable copies of the stats maps
    const lowerBounds = {
        ...(dataFile['lower-bounds'] ?? {}),
    };
    const upperBounds = {
        ...(dataFile['upper-bounds'] ?? {}),
    };
    const nullValueCounts = {
        ...(dataFile['null-value-counts'] ?? {}),
    };
    const valueCounts = {
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
function compareBounds(a, b, type) {
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
export function mergeShreddedStats(stats1, stats2, type) {
    // Determine lower bound (minimum)
    let lowerBound;
    if (stats1.lowerBound !== undefined && stats2.lowerBound !== undefined) {
        lowerBound =
            compareBounds(stats1.lowerBound, stats2.lowerBound, type) <= 0
                ? stats1.lowerBound
                : stats2.lowerBound;
    }
    else {
        lowerBound = stats1.lowerBound ?? stats2.lowerBound;
    }
    // Determine upper bound (maximum)
    let upperBound;
    if (stats1.upperBound !== undefined && stats2.upperBound !== undefined) {
        upperBound =
            compareBounds(stats1.upperBound, stats2.upperBound, type) >= 0
                ? stats1.upperBound
                : stats2.upperBound;
    }
    else {
        upperBound = stats1.upperBound ?? stats2.upperBound;
    }
    // Sum counts
    let nullCount;
    if (stats1.nullCount !== undefined || stats2.nullCount !== undefined) {
        nullCount = (stats1.nullCount ?? 0) + (stats2.nullCount ?? 0);
    }
    let valueCount;
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
//# sourceMappingURL=manifest-stats.js.map