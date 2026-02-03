/**
 * Variant Shredding Examples
 *
 * This file demonstrates how to use variant shredding features in @dotdo/iceberg.
 * Variant shredding allows decomposing semi-structured JSON/variant data into
 * separate typed columns for efficient querying and storage.
 *
 * @see https://iceberg.apache.org/spec/#variant-shredding
 *
 * Run this example:
 *   npx tsx examples/variant-shredding.ts
 */

import type { DataFile, IcebergSchema, VariantShredPropertyConfig } from '../src/index.js';
import {
  // Configuration
  toTableProperties,
  extractVariantShredConfig,
  validateShredConfig,
  // Types
  createShreddedFieldInfo,
  createVariantColumnSchema,
  // Statistics paths
  getStatisticsPaths,
  assignShreddedFieldIds,
  getShreddedStatisticsPaths,
  // Filter transformation
  transformVariantFilter,
  mapFilterPathToStats,
  // Stats collection
  collectShreddedColumnStats,
  addShreddedStatsToDataFile,
  // File filtering
  filterDataFiles,
  filterDataFilesWithStats,
  // Predicate pushdown
  shouldSkipDataFile,
} from '../src/index.js';

// ============================================================================
// Example 1: Configuring Variant Shredding
// ============================================================================

console.log('='.repeat(60));
console.log('Example 1: Configuring Variant Shredding');
console.log('='.repeat(60));

/**
 * Define which fields to shred from a variant column.
 *
 * In this example, we have a movies table where the 'data' column contains
 * semi-structured JSON like:
 * {
 *   "titleType": "movie",
 *   "releaseYear": 2020,
 *   "rating": 8.5,
 *   "genres": ["action", "sci-fi"],
 *   "director": { "name": "Christopher Nolan", ... }
 * }
 *
 * We want to shred titleType, releaseYear, and rating for efficient queries.
 */
const moviesShredConfig: VariantShredPropertyConfig = {
  columnName: '$data',
  fields: ['titleType', 'releaseYear', 'rating'],
  fieldTypes: {
    titleType: 'string',
    releaseYear: 'int',
    rating: 'double',
  },
};

// Validate the configuration
try {
  validateShredConfig(moviesShredConfig);
  console.log('Configuration is valid');
} catch (error) {
  console.error('Invalid configuration:', error);
}

// Convert to Iceberg table properties
const tableProperties = toTableProperties([moviesShredConfig]);
console.log('\nTable properties:');
console.log(JSON.stringify(tableProperties, null, 2));

// Read configuration back from properties
const configs = extractVariantShredConfig(tableProperties);
console.log('\nExtracted configs:');
console.log(JSON.stringify(configs, null, 2));

// ============================================================================
// Example 2: Working with Statistics Paths
// ============================================================================

console.log('\n' + '='.repeat(60));
console.log('Example 2: Working with Statistics Paths');
console.log('='.repeat(60));

/**
 * Statistics paths are how shredded fields are referenced in Iceberg manifests.
 * The format is: {column}.typed_value.{field}.typed_value
 */

// Get statistics paths for shredded fields
const statsPaths = getStatisticsPaths('$data', ['titleType', 'releaseYear', 'rating']);
console.log('\nStatistics paths:');
statsPaths.forEach((path) => console.log(`  - ${path}`));

// Get all statistics paths from configs
const allStatsPaths = getShreddedStatisticsPaths(configs);
console.log('\nAll statistics paths from config:');
allStatsPaths.forEach((path) => console.log(`  - ${path}`));

// Assign unique field IDs to shredded columns
// Start from 1000 to avoid conflicts with regular schema fields
const fieldIdMap = assignShreddedFieldIds(configs, 1000);
console.log('\nField ID assignments:');
for (const [path, id] of fieldIdMap) {
  console.log(`  ${path} -> ${id}`);
}

// ============================================================================
// Example 3: Filter Transformation
// ============================================================================

console.log('\n' + '='.repeat(60));
console.log('Example 3: Filter Transformation');
console.log('='.repeat(60));

/**
 * When querying variant columns, user filters reference fields like "$data.releaseYear".
 * These need to be transformed to statistics paths for predicate pushdown.
 */

const userFilter = {
  '$data.releaseYear': { $gte: 2010, $lte: 2020 },
  '$data.rating': { $gt: 7.0 },
  id: 123, // Non-variant field, preserved as-is
};

console.log('\nOriginal user filter:');
console.log(JSON.stringify(userFilter, null, 2));

const transformResult = transformVariantFilter(userFilter, configs);

console.log('\nTransformed filter:');
console.log(JSON.stringify(transformResult.filter, null, 2));

console.log('\nTransformed paths:', transformResult.transformedPaths);
console.log('Untransformed paths:', transformResult.untransformedPaths);

// Check individual paths
const yearStatsPath = mapFilterPathToStats('$data.releaseYear', configs);
console.log(`\nFilter path '$data.releaseYear' maps to: ${yearStatsPath}`);

// ============================================================================
// Example 4: Collecting Statistics from Data
// ============================================================================

console.log('\n' + '='.repeat(60));
console.log('Example 4: Collecting Statistics from Data');
console.log('='.repeat(60));

/**
 * When writing parquet files, collect min/max bounds for shredded fields.
 * This enables predicate pushdown during query planning.
 */

// Simulated data from a parquet row group
const rowGroupData = [
  { path: 'titleType', values: ['movie', 'movie', 'series', 'movie', 'series'] },
  { path: 'releaseYear', values: [2018, 2019, 2020, 2021, null] },
  { path: 'rating', values: [7.5, 8.0, 8.5, null, 9.0] },
];

const collectedStats = collectShreddedColumnStats(rowGroupData, configs, 1000);

console.log('\nCollected statistics:');
for (const stat of collectedStats.stats) {
  console.log(`\n  ${stat.path} (field ID: ${stat.fieldId}, type: ${stat.type}):`);
  console.log(`    lower bound: ${stat.lowerBound}`);
  console.log(`    upper bound: ${stat.upperBound}`);
  console.log(`    null count: ${stat.nullCount}`);
  console.log(`    value count: ${stat.valueCount}`);
}

// Create a data file and add shredded stats
const baseDataFile: DataFile = {
  content: 0,
  'file-path': 's3://my-bucket/movies/data/file1.parquet',
  'file-format': 'parquet',
  partition: {},
  'record-count': 5,
  'file-size-in-bytes': 1024,
};

const dataFileWithStats = addShreddedStatsToDataFile(baseDataFile, collectedStats);
console.log('\nData file with stats:');
console.log(JSON.stringify(dataFileWithStats, null, 2));

// ============================================================================
// Example 5: File Filtering with Statistics
// ============================================================================

console.log('\n' + '='.repeat(60));
console.log('Example 5: File Filtering with Statistics');
console.log('='.repeat(60));

/**
 * During query planning, use column statistics to skip files that
 * definitely don't contain matching rows.
 */

// Helper to encode int32 as Uint8Array (little-endian per Iceberg spec)
function encodeInt(value: number): Uint8Array {
  const buffer = new ArrayBuffer(4);
  new DataView(buffer).setInt32(0, value, true);
  return new Uint8Array(buffer);
}

// Create sample data files with different year ranges
const dataFiles: DataFile[] = [
  {
    content: 0,
    'file-path': 'file1.parquet', // Years 2000-2009
    'file-format': 'parquet',
    partition: {},
    'record-count': 100,
    'file-size-in-bytes': 1024,
    'lower-bounds': { 1001: encodeInt(2000) },
    'upper-bounds': { 1001: encodeInt(2009) },
  },
  {
    content: 0,
    'file-path': 'file2.parquet', // Years 2010-2015
    'file-format': 'parquet',
    partition: {},
    'record-count': 100,
    'file-size-in-bytes': 1024,
    'lower-bounds': { 1001: encodeInt(2010) },
    'upper-bounds': { 1001: encodeInt(2015) },
  },
  {
    content: 0,
    'file-path': 'file3.parquet', // Years 2016-2020
    'file-format': 'parquet',
    partition: {},
    'record-count': 100,
    'file-size-in-bytes': 1024,
    'lower-bounds': { 1001: encodeInt(2016) },
    'upper-bounds': { 1001: encodeInt(2020) },
  },
  {
    content: 0,
    'file-path': 'file4.parquet', // Years 2021-2024
    'file-format': 'parquet',
    partition: {},
    'record-count': 100,
    'file-size-in-bytes': 1024,
    'lower-bounds': { 1001: encodeInt(2021) },
    'upper-bounds': { 1001: encodeInt(2024) },
  },
];

// Query: Find movies from 2015-2018
const rangeFilter = { '$data.releaseYear': { $gte: 2015, $lte: 2018 } };

console.log('\nQuery filter:', JSON.stringify(rangeFilter));
console.log('\nAll data files:');
dataFiles.forEach((f) => console.log(`  - ${f['file-path']}`));

const { files, stats } = filterDataFilesWithStats(dataFiles, rangeFilter, configs, fieldIdMap);

console.log('\nFiltered files (might contain matching rows):');
files.forEach((f) => console.log(`  - ${f['file-path']}`));

console.log('\nFilter statistics:');
console.log(`  Total files: ${stats.totalFiles}`);
console.log(`  Skipped files: ${stats.skippedFiles}`);
console.log(`  Files to scan: ${stats.totalFiles - stats.skippedFiles}`);

// ============================================================================
// Example 6: Predicate Pushdown with shouldSkipDataFile
// ============================================================================

console.log('\n' + '='.repeat(60));
console.log('Example 6: Predicate Pushdown');
console.log('='.repeat(60));

/**
 * The shouldSkipDataFile function is the core predicate pushdown API.
 * It evaluates a filter against file statistics and determines if the
 * file can be safely skipped.
 */

// Test each file against a specific filter
const specificFilter = { '$data.releaseYear': { $eq: 2023 } };

console.log(`\nFilter: ${JSON.stringify(specificFilter)}\n`);

for (const file of dataFiles) {
  const result = shouldSkipDataFile(file, specificFilter, configs, fieldIdMap);
  const status = result.skip ? 'SKIP' : 'SCAN';
  const reason = result.reason ? ` (${result.reason})` : '';
  console.log(`  ${file['file-path']}: ${status}${reason}`);
}

// ============================================================================
// Example 7: Complete Table Setup
// ============================================================================

console.log('\n' + '='.repeat(60));
console.log('Example 7: Complete Table Setup');
console.log('='.repeat(60));

/**
 * Complete example showing how to set up a table with variant shredding.
 */

// Define table schema
const tableSchema: IcebergSchema = {
  'schema-id': 0,
  type: 'struct',
  fields: [
    { id: 1, name: 'id', required: true, type: 'long' },
    { id: 2, name: 'timestamp', required: true, type: 'timestamptz' },
    { id: 3, name: '$data', required: false, type: 'variant' },
  ],
};

// Define shredding configuration
const shreddingConfig: VariantShredPropertyConfig = {
  columnName: '$data',
  fields: ['event_type', 'user_id', 'amount'],
  fieldTypes: {
    event_type: 'string',
    user_id: 'long',
    amount: 'double',
  },
};

// Create complete table properties
const fullTableProperties = {
  ...toTableProperties([shreddingConfig]),
  'format-version': '2',
  'write.parquet.compression-codec': 'zstd',
};

console.log('\nTable schema:');
console.log(JSON.stringify(tableSchema, null, 2));

console.log('\nShredding configuration:');
console.log(JSON.stringify(shreddingConfig, null, 2));

console.log('\nComplete table properties:');
console.log(JSON.stringify(fullTableProperties, null, 2));

// Create detailed field info for each shredded field
const shreddedFieldInfos = shreddingConfig.fields.map((field) =>
  createShreddedFieldInfo(
    shreddingConfig.columnName,
    field,
    shreddingConfig.fieldTypes[field] ?? 'string'
  )
);

console.log('\nShredded field infos:');
shreddedFieldInfos.forEach((info) => {
  console.log(`  ${info.path}:`);
  console.log(`    type: ${info.type}`);
  console.log(`    statisticsPath: ${info.statisticsPath}`);
});

// Create the complete variant column schema
const variantSchema = createVariantColumnSchema(shreddingConfig.columnName, shreddedFieldInfos);

console.log('\nVariant column schema:');
console.log(JSON.stringify(variantSchema, null, 2));

console.log('\n' + '='.repeat(60));
console.log('Examples complete!');
console.log('='.repeat(60));
