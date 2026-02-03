/**
 * Variant Module Utilities
 *
 * Shared utility functions used across the variant module for filtering,
 * statistics, and predicate pushdown operations.
 *
 * @module variant/utils
 */
// ============================================================================
// Type Guards
// ============================================================================
/**
 * Check if a value is a plain object (not array, null, etc.).
 *
 * @param value - The value to check
 * @returns True if value is a plain object
 *
 * @example
 * ```ts
 * isPlainObject({}) // true
 * isPlainObject({ a: 1 }) // true
 * isPlainObject([]) // false
 * isPlainObject(null) // false
 * isPlainObject('string') // false
 * ```
 */
export function isPlainObject(value) {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}
/**
 * Parse a variant field path to extract column name and field name.
 *
 * Variant paths start with $ followed by the column name and a dot-separated
 * field path. For example:
 * - "$data.title" -> { columnName: "$data", fieldName: "title" }
 * - "$data.user.name" -> { columnName: "$data", fieldName: "user.name" }
 *
 * @param path - The full path (e.g., "$data.title" or "$data.user.name")
 * @returns Object with columnName and fieldName, or null if not a valid variant path
 *
 * @example
 * ```ts
 * parseVariantPath('$data.title')
 * // { columnName: '$data', fieldName: 'title' }
 *
 * parseVariantPath('$data.user.name')
 * // { columnName: '$data', fieldName: 'user.name' }
 *
 * parseVariantPath('regular_column')
 * // null (doesn't start with $)
 *
 * parseVariantPath('$data')
 * // null (no field name)
 * ```
 */
export function parseVariantPath(path) {
    // Must start with $ to be a variant column
    if (!path.startsWith('$')) {
        return null;
    }
    const dotIndex = path.indexOf('.');
    if (dotIndex === -1) {
        return null;
    }
    const columnName = path.substring(0, dotIndex);
    const fieldName = path.substring(dotIndex + 1);
    if (!columnName || !fieldName) {
        return null;
    }
    return { columnName, fieldName };
}
// ============================================================================
// Value Comparison
// ============================================================================
/**
 * Compare two values based on their types.
 *
 * Supports comparison of:
 * - BigInt values
 * - Numbers
 * - Strings (using localeCompare)
 * - Booleans (false < true)
 * - Uint8Array (byte-by-byte comparison)
 * - Mixed BigInt/number comparisons
 * - null/undefined handling
 *
 * @param a - First value
 * @param b - Second value
 * @param _type - Optional Iceberg primitive type (for future type-specific behavior)
 * @returns Negative if a < b, positive if a > b, zero if equal
 *
 * @example
 * ```ts
 * compareValues(1, 2, 'int') // -1 (1 < 2)
 * compareValues(2, 1, 'int') // 1 (2 > 1)
 * compareValues(1, 1, 'int') // 0 (equal)
 *
 * compareValues('a', 'b', 'string') // < 0
 * compareValues(BigInt(100), BigInt(50), 'long') // > 0
 * compareValues(false, true, 'boolean') // -1
 * ```
 */
export function compareValues(a, b, _type) {
    // Handle undefined/null
    if (a === undefined || a === null) {
        return b === undefined || b === null ? 0 : -1;
    }
    if (b === undefined || b === null) {
        return 1;
    }
    // Handle BigInt comparison
    if (typeof a === 'bigint' && typeof b === 'bigint') {
        if (a < b)
            return -1;
        if (a > b)
            return 1;
        return 0;
    }
    // Handle number comparison
    if (typeof a === 'number' && typeof b === 'number') {
        return a - b;
    }
    // Handle string comparison
    if (typeof a === 'string' && typeof b === 'string') {
        return a.localeCompare(b);
    }
    // Handle boolean comparison (false < true)
    if (typeof a === 'boolean' && typeof b === 'boolean') {
        return (a ? 1 : 0) - (b ? 1 : 0);
    }
    // Handle binary comparison (byte-by-byte)
    if (a instanceof Uint8Array && b instanceof Uint8Array) {
        const minLen = Math.min(a.length, b.length);
        for (let i = 0; i < minLen; i++) {
            if (a[i] !== b[i]) {
                return a[i] - b[i];
            }
        }
        return a.length - b.length;
    }
    // Handle BigInt with number (cross-type comparison)
    if (typeof a === 'bigint' && typeof b === 'number') {
        const bigB = BigInt(b);
        if (a < bigB)
            return -1;
        if (a > bigB)
            return 1;
        return 0;
    }
    if (typeof a === 'number' && typeof b === 'bigint') {
        const bigA = BigInt(a);
        if (bigA < b)
            return -1;
        if (bigA > b)
            return 1;
        return 0;
    }
    // Final fallback: string comparison
    return String(a).localeCompare(String(b));
}
//# sourceMappingURL=utils.js.map