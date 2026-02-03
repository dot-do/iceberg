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
import { encodeStatValue, truncateString } from '../avro/index.js';
import { truncateUpperBound } from '../metadata/column-stats.js';
// ============================================================================
// Main Collection Function
// ============================================================================
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
export function collectShreddedColumnStats(columns, configs, startingFieldId, options) {
    const maxStringLength = options?.maxStringLength ?? 16;
    const stats = [];
    const fieldIdMap = new Map();
    // Build a set of configured field paths and their types
    const configuredFields = new Map();
    for (const config of configs) {
        for (const field of config.fields) {
            const type = config.fieldTypes[field] ?? 'string'; // Default to string if no type specified
            configuredFields.set(field, type);
        }
    }
    // Build a map from path to column values
    const columnMap = new Map();
    for (const col of columns) {
        columnMap.set(col.path, col.values);
    }
    // Assign field IDs and collect stats for configured fields in order
    let currentFieldId = startingFieldId;
    for (const config of configs) {
        for (const fieldPath of config.fields) {
            const type = config.fieldTypes[fieldPath] ?? 'string';
            const values = columnMap.get(fieldPath) ?? [];
            // Assign field ID
            const fieldId = currentFieldId++;
            fieldIdMap.set(fieldPath, fieldId);
            // Compute statistics
            const columnStats = computeColumnStats(fieldPath, type, fieldId, values, maxStringLength);
            stats.push(columnStats);
        }
    }
    return {
        stats,
        fieldIdMap,
    };
}
/**
 * Compute statistics for a single column.
 */
function computeColumnStats(path, type, fieldId, values, maxStringLength) {
    let valueCount = 0;
    let nullCount = 0;
    let lowerBound = undefined;
    let upperBound = undefined;
    const compare = getTypeComparator(type);
    for (const value of values) {
        valueCount++;
        if (value === null || value === undefined) {
            nullCount++;
            continue;
        }
        // For floating point, skip NaN values for bounds
        if ((type === 'float' || type === 'double') && typeof value === 'number' && Number.isNaN(value)) {
            continue;
        }
        // Update bounds
        if (lowerBound === undefined || compare(value, lowerBound) < 0) {
            lowerBound = type === 'string' ? truncateString(value, maxStringLength) : value;
        }
        if (upperBound === undefined || compare(value, upperBound) > 0) {
            upperBound = type === 'string' ? truncateUpperBound(value, maxStringLength) : value;
        }
    }
    // Convert timestamps to microseconds
    if ((type === 'timestamp' || type === 'timestamptz') && lowerBound !== undefined) {
        lowerBound = toMicroseconds(lowerBound);
        upperBound = toMicroseconds(upperBound);
    }
    return {
        path,
        type,
        fieldId,
        valueCount,
        nullCount,
        lowerBound,
        upperBound,
    };
}
/**
 * Convert a timestamp value to microseconds since epoch.
 */
function toMicroseconds(value) {
    if (value instanceof Date) {
        return value.getTime() * 1000;
    }
    if (typeof value === 'number') {
        // Assume milliseconds, convert to microseconds
        return value * 1000;
    }
    return value;
}
// ============================================================================
// Individual Bound Computation Functions
// ============================================================================
/**
 * Compute lexicographic min/max bounds for string values.
 *
 * @param values - Array of string values (may include nulls)
 * @param maxLength - Maximum length for bounds (default: 16)
 * @returns Object with lower and upper bounds (null if no non-null values)
 */
export function computeStringBounds(values, maxLength = 16) {
    let lower = null;
    let upper = null;
    for (const value of values) {
        if (value === null || value === undefined) {
            continue;
        }
        if (lower === null || value.localeCompare(lower) < 0) {
            lower = truncateString(value, maxLength);
        }
        if (upper === null || value.localeCompare(upper) > 0) {
            upper = truncateUpperBound(value, maxLength);
        }
    }
    return { lower, upper };
}
/**
 * Compute numeric min/max bounds.
 *
 * @param values - Array of numeric values (may include nulls)
 * @returns Object with lower and upper bounds (null if no non-null values)
 */
export function computeNumericBounds(values) {
    let lower = null;
    let upper = null;
    for (const value of values) {
        if (value === null || value === undefined) {
            continue;
        }
        // Skip NaN for floating point numbers
        if (typeof value === 'number' && Number.isNaN(value)) {
            continue;
        }
        if (lower === null || value < lower) {
            lower = value;
        }
        if (upper === null || value > upper) {
            upper = value;
        }
    }
    return { lower, upper };
}
/**
 * Compute timestamp min/max bounds in microseconds.
 *
 * @param values - Array of Date objects or numeric timestamps (may include nulls)
 * @returns Object with lower and upper bounds in microseconds (null if no non-null values)
 */
export function computeTimestampBounds(values) {
    let lower = null;
    let upper = null;
    for (const value of values) {
        if (value === null || value === undefined) {
            continue;
        }
        // Convert to microseconds
        const micros = value instanceof Date ? value.getTime() * 1000 : value * 1000;
        if (lower === null || micros < lower) {
            lower = micros;
        }
        if (upper === null || micros > upper) {
            upper = micros;
        }
    }
    return { lower, upper };
}
/**
 * Compute boolean min/max bounds.
 * false < true in Iceberg semantics.
 *
 * @param values - Array of boolean values (may include nulls)
 * @returns Object with lower and upper bounds (null if no non-null values)
 */
export function computeBooleanBounds(values) {
    let lower = null;
    let upper = null;
    for (const value of values) {
        if (value === null || value === undefined) {
            continue;
        }
        // false < true
        if (lower === null || (value === false && lower === true)) {
            lower = value;
        }
        if (upper === null || (value === true && upper === false)) {
            upper = value;
        }
    }
    return { lower, upper };
}
// ============================================================================
// Integration with DataFile
// ============================================================================
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
export function addShreddedStatsToDataFile(dataFile, stats) {
    // Start with existing stats or empty objects
    const valueCounts = { ...(dataFile['value-counts'] ?? {}) };
    const nullValueCounts = { ...(dataFile['null-value-counts'] ?? {}) };
    const lowerBounds = { ...(dataFile['lower-bounds'] ?? {}) };
    const upperBounds = { ...(dataFile['upper-bounds'] ?? {}) };
    // Add shredded column stats
    for (const stat of stats.stats) {
        valueCounts[stat.fieldId] = stat.valueCount;
        nullValueCounts[stat.fieldId] = stat.nullCount;
        if (stat.lowerBound !== undefined && stat.lowerBound !== null) {
            lowerBounds[stat.fieldId] = encodeStatValue(stat.lowerBound, stat.type);
        }
        if (stat.upperBound !== undefined && stat.upperBound !== null) {
            upperBounds[stat.fieldId] = encodeStatValue(stat.upperBound, stat.type);
        }
    }
    return {
        ...dataFile,
        'value-counts': valueCounts,
        'null-value-counts': nullValueCounts,
        'lower-bounds': lowerBounds,
        'upper-bounds': upperBounds,
    };
}
// ============================================================================
// Helper Functions
// ============================================================================
/**
 * Get a comparator function for a given Iceberg type.
 */
function getTypeComparator(type) {
    switch (type) {
        case 'boolean':
            // false < true
            return (a, b) => {
                if (a === b)
                    return 0;
                return a === false ? -1 : 1;
            };
        case 'int':
        case 'float':
        case 'double':
        case 'date':
        case 'time':
        case 'timestamp':
        case 'timestamptz':
        case 'timestamp_ns':
        case 'timestamptz_ns':
            return (a, b) => {
                // Handle Date objects for timestamps
                const aNum = a instanceof Date ? a.getTime() : a;
                const bNum = b instanceof Date ? b.getTime() : b;
                return aNum - bNum;
            };
        case 'long':
            return (a, b) => {
                // Handle bigint comparison without losing precision
                if (typeof a === 'bigint' && typeof b === 'bigint') {
                    return a < b ? -1 : a > b ? 1 : 0;
                }
                // Fall back to number comparison
                const aNum = typeof a === 'bigint' ? Number(a) : a;
                const bNum = typeof b === 'bigint' ? Number(b) : b;
                return aNum - bNum;
            };
        case 'string':
        case 'uuid':
            return (a, b) => a.localeCompare(b);
        case 'binary':
        case 'fixed':
            return compareBinary;
        case 'decimal':
            return (a, b) => {
                const aNum = typeof a === 'number' ? a : parseFloat(a);
                const bNum = typeof b === 'number' ? b : parseFloat(b);
                return aNum - bNum;
            };
        default:
            return (a, b) => String(a).localeCompare(String(b));
    }
}
/**
 * Compare two binary values lexicographically.
 */
function compareBinary(a, b) {
    const bytesA = a instanceof Uint8Array ? a : new Uint8Array(a);
    const bytesB = b instanceof Uint8Array ? b : new Uint8Array(b);
    const minLen = Math.min(bytesA.length, bytesB.length);
    for (let i = 0; i < minLen; i++) {
        if (bytesA[i] !== bytesB[i]) {
            return bytesA[i] - bytesB[i];
        }
    }
    return bytesA.length - bytesB.length;
}
//# sourceMappingURL=stats-collector.js.map