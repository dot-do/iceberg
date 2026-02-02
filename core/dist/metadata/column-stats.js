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
import { encodeStatValue, truncateString } from '../avro/index.js';
// ============================================================================
// Column Statistics Collector Implementation
// ============================================================================
/**
 * Creates a statistics collector for a specific column type.
 */
export function createColumnStatsCollector(fieldId, type, maxStringLength = 16) {
    let valueCount = 0;
    let nullCount = 0;
    let nanCount = 0;
    let columnSize = 0;
    let min = undefined;
    let max = undefined;
    const isFloatingPoint = type === 'float' || type === 'double';
    const isString = type === 'string';
    const compare = getComparator(type);
    return {
        add(value) {
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
                min = isString ? truncateString(value, maxStringLength) : value;
            }
            if (max === undefined || compare(value, max) > 0) {
                max = isString ? truncateUpperBound(value, maxStringLength) : value;
            }
        },
        getStats() {
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
        reset() {
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
    collectors = new Map();
    schema;
    maxStringLength;
    includeFieldIds;
    excludeFieldIds;
    constructor(options) {
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
                    this.collectors.set(field.id, createColumnStatsCollector(field.id, primitiveType, this.maxStringLength));
                }
            }
        }
    }
    shouldCollectStats(fieldId) {
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
    addRow(row) {
        const fieldNameToId = new Map();
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
    addValue(fieldId, value) {
        const collector = this.collectors.get(fieldId);
        if (collector) {
            collector.add(value);
        }
    }
    /**
     * Get the computed statistics for all columns.
     */
    getStats() {
        const stats = [];
        for (const collector of this.collectors.values()) {
            stats.push(collector.getStats());
        }
        return stats;
    }
    /**
     * Get encoded file statistics ready for use in a DataFile.
     */
    getEncodedStats() {
        const stats = this.getStats();
        return encodeFileStats(stats, this.schema);
    }
    /**
     * Reset all collectors.
     */
    reset() {
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
export function encodeFileStats(stats, schema) {
    const result = {
        valueCounts: {},
        nullValueCounts: {},
        nanValueCounts: {},
        columnSizes: {},
        lowerBounds: {},
        upperBounds: {},
    };
    const fieldTypes = new Map();
    for (const field of schema.fields) {
        const primitiveType = getPrimitiveType(field.type);
        if (primitiveType) {
            fieldTypes.set(field.id, primitiveType);
        }
    }
    for (const stat of stats) {
        const type = fieldTypes.get(stat.fieldId);
        if (!type)
            continue;
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
export function applyStatsToDataFile(dataFile, stats) {
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
export function aggregateColumnStats(statsPerFile, schema) {
    const aggregated = new Map();
    const fieldTypes = new Map();
    for (const field of schema.fields) {
        const primitiveType = getPrimitiveType(field.type);
        if (primitiveType) {
            fieldTypes.set(field.id, primitiveType);
        }
    }
    for (const fileStats of statsPerFile) {
        for (const stat of fileStats) {
            const type = fieldTypes.get(stat.fieldId);
            if (!type)
                continue;
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
                }
                else if (compare(stat.lowerBound, existing.lowerBound) < 0) {
                    existing.lowerBound = stat.lowerBound;
                }
            }
            // Update max bound
            if (stat.upperBound !== undefined && stat.upperBound !== null) {
                if (existing.upperBound === undefined || existing.upperBound === null) {
                    existing.upperBound = stat.upperBound;
                }
                else if (compare(stat.upperBound, existing.upperBound) > 0) {
                    existing.upperBound = stat.upperBound;
                }
            }
        }
    }
    return Array.from(aggregated.values());
}
/**
 * Compute partition field summaries from manifest entries.
 */
export function computePartitionSummaries(partitionValues, partitionFieldTypes) {
    const summaries = [];
    for (const [fieldName, type] of Object.entries(partitionFieldTypes)) {
        let containsNull = false;
        let containsNan = false;
        let min = undefined;
        let max = undefined;
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
        const summary = {
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
export function getPrimitiveType(type) {
    if (typeof type === 'string') {
        return type;
    }
    return undefined;
}
/**
 * Get a comparator function for a primitive type.
 */
export function getComparator(type) {
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
                const numA = typeof a === 'bigint' ? Number(a) : a;
                const numB = typeof b === 'bigint' ? Number(b) : b;
                return numA - numB;
            };
        case 'string':
        case 'uuid':
            return (a, b) => a.localeCompare(b);
        case 'binary':
        case 'fixed':
            return compareBinary;
        case 'decimal':
            return (a, b) => {
                // Handle decimal as number or string
                const numA = typeof a === 'number' ? a : parseFloat(a);
                const numB = typeof b === 'number' ? b : parseFloat(b);
                return numA - numB;
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
/**
 * Estimate the serialized size of a value.
 */
export function estimateValueSize(value, type) {
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
export function truncateUpperBound(value, maxLength) {
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
/**
 * Create a zone map from file statistics.
 */
export function createZoneMapFromStats(stats) {
    const bounds = new Map();
    const nullCounts = new Map();
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
export function canPruneZoneMap(zoneMap, fieldId, operator, value, type) {
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
//# sourceMappingURL=column-stats.js.map