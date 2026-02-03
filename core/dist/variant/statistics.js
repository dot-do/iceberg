/**
 * Variant Statistics Path Mapping Functions
 *
 * These functions map variant field paths to statistics paths for predicate pushdown.
 * When querying shredded variant columns, filter predicates reference fields using
 * paths like "$data.title", which need to be mapped to their corresponding
 * statistics paths for efficient file pruning.
 *
 * @see https://iceberg.apache.org/spec/#variant-shredding
 */
/**
 * Get statistics paths for shredded fields in a variant column.
 *
 * Given a variant column name and list of field names, returns the full
 * statistics paths for each field. Statistics paths follow the pattern:
 * `{columnName}.typed_value.{fieldName}.typed_value`
 *
 * @param columnName - The variant column name (e.g., "$data")
 * @param fields - Array of field names to get statistics paths for
 * @returns Array of statistics paths for each field
 *
 * @example
 * ```ts
 * getStatisticsPaths("$data", ["title", "year"])
 * // Returns: ["$data.typed_value.title.typed_value", "$data.typed_value.year.typed_value"]
 * ```
 */
export function getStatisticsPaths(columnName, fields) {
    return fields.map((field) => `${columnName}.typed_value.${field}.typed_value`);
}
/**
 * Extract the column name from a filter path.
 *
 * Given a filter path like "$data.title" or "$data.user.name", extracts the
 * column name (the first segment before the first dot).
 *
 * @param filterPath - The filter path (e.g., "$data.title")
 * @returns The column name (e.g., "$data")
 *
 * @example
 * ```ts
 * getColumnForFilterPath("$data.title") // Returns "$data"
 * getColumnForFilterPath("$data.user.name") // Returns "$data"
 * getColumnForFilterPath("column") // Returns "column"
 * ```
 */
export function getColumnForFilterPath(filterPath) {
    const dotIndex = filterPath.indexOf('.');
    if (dotIndex === -1) {
        return filterPath;
    }
    return filterPath.substring(0, dotIndex);
}
/**
 * Extract the field path from a filter path.
 *
 * Given a filter path like "$data.title" or "$data.user.name", extracts the
 * field path (everything after the first dot).
 *
 * @param filterPath - The filter path (e.g., "$data.title")
 * @returns The field path (e.g., "title") or empty string if no dot
 */
function getFieldFromFilterPath(filterPath) {
    const dotIndex = filterPath.indexOf('.');
    if (dotIndex === -1) {
        return '';
    }
    return filterPath.substring(dotIndex + 1);
}
/**
 * Map a filter path to its corresponding statistics path.
 *
 * Given a filter path like "$data.title" and variant shred configurations,
 * returns the statistics path for predicate pushdown. Returns null if the
 * path does not match any shredded field.
 *
 * @param filterPath - The filter path (e.g., "$data.title")
 * @param configs - Array of variant shred configurations
 * @returns The statistics path, or null if not a shredded field
 *
 * @example
 * ```ts
 * const configs = [{ columnName: '$data', fields: ['title'], fieldTypes: {} }];
 * mapFilterPathToStats("$data.title", configs)
 * // Returns "$data.typed_value.title.typed_value"
 *
 * mapFilterPathToStats("$data.unknown", configs)
 * // Returns null
 * ```
 */
export function mapFilterPathToStats(filterPath, configs) {
    const columnName = getColumnForFilterPath(filterPath);
    const fieldPath = getFieldFromFilterPath(filterPath);
    // No field means it's just a column reference, not a field path
    if (!fieldPath) {
        return null;
    }
    // Find matching config for this column
    const config = configs.find((c) => c.columnName === columnName);
    if (!config) {
        return null;
    }
    // Check if the field is in the shredded fields
    if (!config.fields.includes(fieldPath)) {
        return null;
    }
    // Return the statistics path
    return `${columnName}.typed_value.${fieldPath}.typed_value`;
}
/**
 * Check if a filter path corresponds to a shredded variant field.
 *
 * Returns true if the path matches a column and field in the variant
 * shred configurations.
 *
 * @param path - The filter path to check (e.g., "$data.title")
 * @param configs - Array of variant shred configurations
 * @returns True if the path is a shredded variant field
 *
 * @example
 * ```ts
 * const configs = [{ columnName: '$data', fields: ['title'], fieldTypes: {} }];
 *
 * isVariantFilterPath("$data.title", configs) // true
 * isVariantFilterPath("$data.unknown", configs) // false
 * isVariantFilterPath("regular_column", configs) // false
 * ```
 */
export function isVariantFilterPath(path, configs) {
    return mapFilterPathToStats(path, configs) !== null;
}
/**
 * Extract variant column information from a filter object.
 *
 * Given a filter object with keys that may reference variant fields, extracts
 * the variant columns that need to be read and the statistics paths for
 * predicate pushdown.
 *
 * @param filter - Filter object with field paths as keys
 * @param configs - Array of variant shred configurations
 * @returns Object with readColumns and statsColumns arrays
 *
 * @example
 * ```ts
 * const configs = [
 *   { columnName: '$data', fields: ['title', 'year'], fieldTypes: {} }
 * ];
 * const filter = { '$data.title': 'The Matrix', id: 123 };
 *
 * extractVariantFilterColumns(filter, configs)
 * // Returns {
 * //   readColumns: ['$data'],
 * //   statsColumns: ['$data.typed_value.title.typed_value']
 * // }
 * ```
 */
export function extractVariantFilterColumns(filter, configs) {
    const readColumnsSet = new Set();
    const statsColumns = [];
    for (const key of Object.keys(filter)) {
        const statsPath = mapFilterPathToStats(key, configs);
        if (statsPath !== null) {
            const columnName = getColumnForFilterPath(key);
            readColumnsSet.add(columnName);
            statsColumns.push(statsPath);
        }
    }
    return {
        readColumns: Array.from(readColumnsSet),
        statsColumns,
    };
}
//# sourceMappingURL=statistics.js.map