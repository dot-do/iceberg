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

// Types
export type {
  ShreddedFieldInfo,
  VariantColumnSchema,
  VariantShredConfig,
  VariantShredConfigValidationResult,
} from './types.js';

// Helper functions
export {
  createShreddedFieldInfo,
  createVariantColumnSchema,
  validateVariantShredConfig,
} from './types.js';

// Path generation functions
export { getMetadataPath, getValuePath, getTypedValuePath } from './types.js';

// ============================================================================
// Configuration Property Types and Utilities
// ============================================================================

// Property config types
export type { VariantShredPropertyConfig } from './config.js';

// Property key constants
export {
  VARIANT_SHRED_COLUMNS_KEY,
  VARIANT_SHRED_FIELDS_KEY_PREFIX,
  VARIANT_SHRED_FIELDS_KEY_SUFFIX,
  VARIANT_FIELD_TYPES_KEY_SUFFIX,
} from './config.js';

// Key generation functions
export { getShredFieldsKey, getFieldTypesKey } from './config.js';

// Parsing functions
export {
  parseShredColumnsProperty,
  parseShredFieldsProperty,
  parseFieldTypesProperty,
  extractVariantShredConfig,
} from './config.js';

// Serialization functions
export {
  formatShredColumnsProperty,
  formatShredFieldsProperty,
  formatFieldTypesProperty,
  toTableProperties,
} from './config.js';

// Validation functions
export { validateShredConfig } from './config.js';

// ============================================================================
// Statistics Path Mapping
// ============================================================================

// Statistics path types
export type { VariantFilterColumnsResult } from './statistics.js';

// Statistics path functions
export {
  getStatisticsPaths,
  getColumnForFilterPath,
  mapFilterPathToStats,
  isVariantFilterPath,
  extractVariantFilterColumns,
} from './statistics.js';

// ============================================================================
// Filter Transformation
// ============================================================================

// Filter transformation types
export type { TransformResult } from './filter.js';

// Filter transformation functions
export { transformVariantFilter, isComparisonOperator, isLogicalOperator } from './filter.js';

// ============================================================================
// Manifest Statistics for Shredded Columns
// ============================================================================

// Manifest stats types
export type { ShreddedColumnStats, CreateShreddedStatsOptions } from './manifest-stats.js';

// Manifest stats functions
export {
  getShreddedStatisticsPaths,
  assignShreddedFieldIds,
  serializeShreddedBound,
  deserializeShreddedBound,
  createShreddedColumnStats,
  applyShreddedStatsToDataFile,
  mergeShreddedStats,
} from './manifest-stats.js';

// ============================================================================
// Statistics Collection for Shredded Columns
// ============================================================================

// Stats collector types
export type {
  ColumnValues,
  CollectedStats,
  ShreddedColumnStats as CollectedShreddedColumnStats,
  CollectStatsOptions,
} from './stats-collector.js';

// Stats collector functions
export {
  collectShreddedColumnStats,
  computeStringBounds,
  computeNumericBounds,
  computeTimestampBounds,
  computeBooleanBounds,
  addShreddedStatsToDataFile,
} from './stats-collector.js';

// ============================================================================
// Row Group Filtering with Variant Statistics
// ============================================================================

// Row group filter types
export type { RangePredicate, FilterStats } from './row-group-filter.js';

// Row group filter functions
export {
  createRangePredicate,
  evaluateRangePredicate,
  combinePredicatesAnd,
  combinePredicatesOr,
  filterDataFiles,
  filterDataFilesWithStats,
} from './row-group-filter.js';

// ============================================================================
// Predicate Pushdown for Variant Filters
// ============================================================================

// Predicate pushdown types
export type { PredicateResult } from './predicate-pushdown.js';

// Predicate pushdown functions
export {
  shouldSkipDataFile,
  boundsOverlapValue,
  evaluateInPredicate,
} from './predicate-pushdown.js';

// ============================================================================
// ParqueDB/Hyparquet Integration Helpers
// ============================================================================

// Integration types
export type {
  VariantShredConfigSimple,
  VariantSchemaField,
  SetupVariantShreddingOptions,
  SetupVariantShreddingResult,
  ConfigSchemaValidationResult,
} from './integration.js';

// Integration functions
export {
  parseShredConfig,
  formatShredConfig,
  createVariantSchemaFields,
  getFieldIdForShreddedPath,
  validateConfigWithSchema,
  setupVariantShredding,
} from './integration.js';
