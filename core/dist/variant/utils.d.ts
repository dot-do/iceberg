/**
 * Variant Module Utilities
 *
 * Shared utility functions used across the variant module for filtering,
 * statistics, and predicate pushdown operations.
 *
 * @module variant/utils
 */
import type { IcebergPrimitiveType } from '../metadata/types.js';
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
export declare function isPlainObject(value: unknown): value is Record<string, unknown>;
/**
 * Result of parsing a variant field path.
 */
export interface ParsedVariantPath {
    /** The variant column name (e.g., "$data") */
    readonly columnName: string;
    /** The field name within the variant (e.g., "title" or "user.name") */
    readonly fieldName: string;
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
export declare function parseVariantPath(path: string): ParsedVariantPath | null;
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
export declare function compareValues(a: unknown, b: unknown, _type?: IcebergPrimitiveType): number;
//# sourceMappingURL=utils.d.ts.map