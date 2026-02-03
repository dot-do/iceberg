/**
 * Iceberg Type Definitions
 *
 * Core type definitions for Apache Iceberg format.
 *
 * @see https://iceberg.apache.org/spec/
 */
/** Array of valid edge-interpolation algorithms */
export const VALID_EDGE_INTERPOLATION_ALGORITHMS = [
    'spherical',
    'vincenty',
    'thomas',
    'andoyer',
    'karney',
];
/** Default CRS for geospatial types */
export const GEOSPATIAL_DEFAULT_CRS = 'OGC:CRS84';
/** Default edge-interpolation algorithm for geography types */
export const GEOSPATIAL_DEFAULT_ALGORITHM = 'spherical';
/**
 * Check if a type string represents a geospatial type (geometry or geography).
 */
export function isGeospatialType(type) {
    return type.startsWith('geometry') || type.startsWith('geography');
}
/**
 * Check if an algorithm name is a valid edge-interpolation algorithm.
 */
export function isValidEdgeInterpolationAlgorithm(algorithm) {
    return VALID_EDGE_INTERPOLATION_ALGORITHMS.includes(algorithm);
}
/**
 * Parse a geometry type string and extract its CRS parameter.
 * Returns null if the type is not a geometry type.
 *
 * @example
 * parseGeometryType('geometry') // { crs: 'OGC:CRS84' }
 * parseGeometryType('geometry(EPSG:4326)') // { crs: 'EPSG:4326' }
 */
export function parseGeometryType(type) {
    if (!type.startsWith('geometry')) {
        return null;
    }
    // Handle parameterized form: geometry(CRS)
    const match = type.match(/^geometry\(([^)]+)\)$/);
    if (match) {
        return { crs: match[1] };
    }
    // Handle bare form: geometry (uses default CRS)
    if (type === 'geometry') {
        return { crs: GEOSPATIAL_DEFAULT_CRS };
    }
    return null;
}
/**
 * Parse a geography type string and extract its CRS and algorithm parameters.
 * Returns null if the type is not a geography type.
 *
 * @example
 * parseGeographyType('geography') // { crs: 'OGC:CRS84', algorithm: 'spherical' }
 * parseGeographyType('geography(EPSG:4326, vincenty)') // { crs: 'EPSG:4326', algorithm: 'vincenty' }
 */
export function parseGeographyType(type) {
    if (!type.startsWith('geography')) {
        return null;
    }
    // Handle parameterized form: geography(CRS, algorithm)
    const match = type.match(/^geography\(([^,]+),\s*([^)]+)\)$/);
    if (match) {
        return {
            crs: match[1],
            algorithm: match[2],
        };
    }
    // Handle bare form: geography (uses defaults)
    if (type === 'geography') {
        return {
            crs: GEOSPATIAL_DEFAULT_CRS,
            algorithm: GEOSPATIAL_DEFAULT_ALGORITHM,
        };
    }
    return null;
}
/**
 * Serialize a GeometryTypeInfo back to its string representation.
 * Uses the compact form if using default CRS.
 *
 * @example
 * serializeGeometryType({ crs: 'OGC:CRS84' }) // 'geometry'
 * serializeGeometryType({ crs: 'EPSG:4326' }) // 'geometry(EPSG:4326)'
 */
export function serializeGeometryType(info) {
    if (info.crs === GEOSPATIAL_DEFAULT_CRS) {
        return 'geometry';
    }
    return `geometry(${info.crs})`;
}
/**
 * Serialize a GeographyTypeInfo back to its string representation.
 * Uses the compact form if using default CRS and algorithm.
 *
 * @example
 * serializeGeographyType({ crs: 'OGC:CRS84', algorithm: 'spherical' }) // 'geography'
 * serializeGeographyType({ crs: 'EPSG:4326', algorithm: 'karney' }) // 'geography(EPSG:4326, karney)'
 */
export function serializeGeographyType(info) {
    if (info.crs === GEOSPATIAL_DEFAULT_CRS && info.algorithm === GEOSPATIAL_DEFAULT_ALGORITHM) {
        return 'geography';
    }
    return `geography(${info.crs}, ${info.algorithm})`;
}
/**
 * Check if a DataFile represents a deletion vector.
 *
 * A deletion vector is a position delete file (content=1) that has all three
 * deletion vector fields: content-offset, content-size-in-bytes, and referenced-data-file.
 *
 * @param dataFile - The DataFile to check
 * @returns true if the DataFile is a deletion vector
 */
export function isDeletionVector(dataFile) {
    return (dataFile.content === 1 && // position deletes
        typeof dataFile['content-offset'] === 'number' &&
        typeof dataFile['content-size-in-bytes'] === 'number' &&
        typeof dataFile['referenced-data-file'] === 'string');
}
/**
 * Validate deletion vector fields on a DataFile.
 *
 * Validates that:
 * - content-offset and content-size-in-bytes are provided together
 * - referenced-data-file is required when DV fields are present
 *
 * @param dataFile - The DataFile to validate
 * @returns Validation result with any errors
 */
export function validateDeletionVectorFields(dataFile) {
    const errors = [];
    const hasContentOffset = typeof dataFile['content-offset'] === 'number';
    const hasContentSize = typeof dataFile['content-size-in-bytes'] === 'number';
    const hasReferencedFile = typeof dataFile['referenced-data-file'] === 'string';
    // content-offset requires content-size-in-bytes
    if (hasContentOffset && !hasContentSize) {
        errors.push('content-offset requires content-size-in-bytes');
    }
    // content-size-in-bytes requires content-offset
    if (hasContentSize && !hasContentOffset) {
        errors.push('content-size-in-bytes requires content-offset');
    }
    // If both content fields are present, referenced-data-file is required
    if (hasContentOffset && hasContentSize && !hasReferencedFile) {
        errors.push('referenced-data-file is required for deletion vectors');
    }
    return {
        valid: errors.length === 0,
        errors,
    };
}
// ============================================================================
// Row Lineage Helpers
// ============================================================================
/**
 * Calculate the row ID for a specific row in a data file.
 *
 * The row ID is calculated as: first_row_id + row_position
 * where row_position is the 0-based position of the row within the file.
 *
 * @param firstRowId - The first row ID assigned to this data file (or null/undefined)
 * @param rowPosition - The 0-based position of the row within the data file
 * @returns The unique row ID, or null if first_row_id is null or undefined
 *
 * @example
 * ```ts
 * // Data file with first-row-id of 5000
 * const rowId = calculateRowId(5000, 42);
 * console.log(rowId); // 5042
 *
 * // Data file with null first-row-id (inherits from manifest)
 * const rowId = calculateRowId(null, 42);
 * console.log(rowId); // null
 * ```
 *
 * @see https://iceberg.apache.org/spec/#row-lineage
 */
export function calculateRowId(firstRowId, rowPosition) {
    if (firstRowId === null || firstRowId === undefined) {
        return null;
    }
    return firstRowId + rowPosition;
}
//# sourceMappingURL=types.js.map