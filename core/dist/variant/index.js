/**
 * Variant Shredding Module
 *
 * Provides types and utilities for Iceberg variant shredding, which allows
 * semi-structured variant data to be decomposed into separate columns for
 * efficient querying and storage.
 *
 * @module variant
 * @see https://iceberg.apache.org/spec/#variant-shredding
 */
// Helper functions
export { createShreddedFieldInfo, createVariantColumnSchema, validateVariantShredConfig, } from './types.js';
// Utility functions
export { isPlainObject, parseVariantPath, compareValues } from './utils.js';
// Path generation functions
export { getMetadataPath, getValuePath, getTypedValuePath } from './types.js';
// Property key constants
export { VARIANT_SHRED_COLUMNS_KEY, VARIANT_SHRED_FIELDS_KEY_PREFIX, VARIANT_SHRED_FIELDS_KEY_SUFFIX, VARIANT_FIELD_TYPES_KEY_SUFFIX, } from './config.js';
// Key generation functions
export { getShredFieldsKey, getFieldTypesKey } from './config.js';
// Parsing functions
export { parseShredColumnsProperty, parseShredFieldsProperty, parseFieldTypesProperty, extractVariantShredConfig, } from './config.js';
// Serialization functions
export { formatShredColumnsProperty, formatShredFieldsProperty, formatFieldTypesProperty, toTableProperties, } from './config.js';
// Validation functions
export { validateShredConfig } from './config.js';
// Statistics path functions
export { getStatisticsPaths, getColumnForFilterPath, mapFilterPathToStats, isVariantFilterPath, extractVariantFilterColumns, } from './statistics.js';
// Filter transformation functions
export { transformVariantFilter, isComparisonOperator, isLogicalOperator } from './filter.js';
// Manifest stats functions
export { getShreddedStatisticsPaths, assignShreddedFieldIds, serializeShreddedBound, deserializeShreddedBound, createShreddedColumnStats, applyShreddedStatsToDataFile, mergeShreddedStats, } from './manifest-stats.js';
// Stats collector functions
export { collectShreddedColumnStats, computeStringBounds, computeNumericBounds, computeTimestampBounds, computeBooleanBounds, addShreddedStatsToDataFile, } from './stats-collector.js';
// Row group filter functions
export { createRangePredicate, evaluateRangePredicate, combinePredicatesAnd, combinePredicatesOr, filterDataFiles, filterDataFilesWithStats, } from './row-group-filter.js';
// Predicate pushdown functions
export { shouldSkipDataFile, boundsOverlapValue, evaluateInPredicate, } from './predicate-pushdown.js';
// Integration functions
export { parseShredConfig, formatShredConfig, createVariantSchemaFields, getFieldIdForShreddedPath, validateConfigWithSchema, setupVariantShredding, } from './integration.js';
//# sourceMappingURL=index.js.map