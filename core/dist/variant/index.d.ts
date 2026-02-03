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
export type { ShreddedFieldInfo, VariantColumnSchema, VariantShredConfig, VariantShredConfigValidationResult, } from './types.js';
export { createShreddedFieldInfo, createVariantColumnSchema, validateVariantShredConfig, } from './types.js';
export type { ParsedVariantPath } from './utils.js';
export { isPlainObject, parseVariantPath, compareValues } from './utils.js';
export { getMetadataPath, getValuePath, getTypedValuePath } from './types.js';
export type { VariantShredPropertyConfig, ShredConfigValidationResult } from './config.js';
export { VARIANT_SHRED_COLUMNS_KEY, VARIANT_SHRED_FIELDS_KEY_PREFIX, VARIANT_SHRED_FIELDS_KEY_SUFFIX, VARIANT_FIELD_TYPES_KEY_SUFFIX, } from './config.js';
export { getShredFieldsKey, getFieldTypesKey } from './config.js';
export { parseShredColumnsProperty, parseShredFieldsProperty, parseFieldTypesProperty, extractVariantShredConfig, } from './config.js';
export { formatShredColumnsProperty, formatShredFieldsProperty, formatFieldTypesProperty, toTableProperties, } from './config.js';
export { validateShredConfig } from './config.js';
export type { VariantFilterColumnsResult } from './statistics.js';
export { getStatisticsPaths, getColumnForFilterPath, mapFilterPathToStats, isVariantFilterPath, extractVariantFilterColumns, } from './statistics.js';
export type { TransformResult } from './filter.js';
export { transformVariantFilter, isComparisonOperator, isLogicalOperator } from './filter.js';
export type { SerializedShreddedColumnStats, ShreddedColumnStats, // Deprecated alias for backward compatibility
CreateShreddedStatsOptions, } from './manifest-stats.js';
export { getShreddedStatisticsPaths, assignShreddedFieldIds, serializeShreddedBound, deserializeShreddedBound, createShreddedColumnStats, applyShreddedStatsToDataFile, mergeShreddedStats, } from './manifest-stats.js';
export type { ColumnValues, CollectedStats, CollectedShreddedColumnStats, ShreddedColumnStats as ShreddedColumnStatsCollector, // Deprecated alias
CollectStatsOptions, } from './stats-collector.js';
export { collectShreddedColumnStats, computeStringBounds, computeNumericBounds, computeTimestampBounds, computeBooleanBounds, addShreddedStatsToDataFile, } from './stats-collector.js';
export type { RangePredicate, FilterStats } from './row-group-filter.js';
export { createRangePredicate, evaluateRangePredicate, combinePredicatesAnd, combinePredicatesOr, filterDataFiles, filterDataFilesWithStats, } from './row-group-filter.js';
export type { PredicateResult } from './predicate-pushdown.js';
export { shouldSkipDataFile, boundsOverlapValue, evaluateInPredicate, } from './predicate-pushdown.js';
export type { VariantShredConfigSimple, VariantSchemaField, SetupVariantShreddingOptions, SetupVariantShreddingResult, ConfigSchemaValidationResult, } from './integration.js';
export { parseShredConfig, formatShredConfig, createVariantSchemaFields, getFieldIdForShreddedPath, validateConfigWithSchema, setupVariantShredding, } from './integration.js';
//# sourceMappingURL=index.d.ts.map