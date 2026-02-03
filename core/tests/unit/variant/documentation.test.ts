/**
 * Variant Shredding Documentation Tests
 *
 * These tests serve as executable documentation for variant shredding features.
 * Each test demonstrates a specific use case and validates that the documented
 * API works as expected.
 *
 * @see https://iceberg.apache.org/spec/#variant-shredding
 */

import { describe, it, expect } from 'vitest';
import type {
  IcebergSchema,
  IcebergPrimitiveType,
  DataFile,
  VariantShredPropertyConfig,
} from '../../../src/index.js';

// Core variant shredding exports
import {
  // Types
  createShreddedFieldInfo,
  createVariantColumnSchema,
  validateVariantShredConfig,
  // Config functions
  toTableProperties,
  extractVariantShredConfig,
  validateShredConfig,
  // Statistics paths
  getStatisticsPaths,
  mapFilterPathToStats,
  extractVariantFilterColumns,
  // Filter transformation
  transformVariantFilter,
  // Field ID assignment
  assignShreddedFieldIds,
  getShreddedStatisticsPaths,
  // Stats collection
  collectShreddedColumnStats,
  addShreddedStatsToDataFile,
  // Row group filtering
  filterDataFiles,
  filterDataFilesWithStats,
  createRangePredicate,
  evaluateRangePredicate,
  // Predicate pushdown
  shouldSkipDataFile,
  boundsOverlapValue,
} from '../../../src/index.js';

// ============================================================================
// Example 1: Creating Table with Shredding Config
// ============================================================================

describe('Example 1: Creating table with shredding config', () => {
  /**
   * This example shows how to create an Iceberg table with a variant column
   * that has shredding configured for specific fields.
   *
   * Use case: A movies table where the 'data' column contains semi-structured
   * JSON data, but we want to shred 'titleType', 'releaseYear', and 'rating'
   * fields for efficient querying.
   */

  it('should create table properties for variant column with shredding config', () => {
    // Define which fields to shred from the variant column
    const shreddingConfig: VariantShredPropertyConfig = {
      columnName: '$data',
      fields: ['titleType', 'releaseYear', 'rating'],
      fieldTypes: {
        titleType: 'string',
        releaseYear: 'int',
        rating: 'double',
      },
    };

    // Convert to table properties for Iceberg metadata
    const tableProperties = toTableProperties([shreddingConfig]);

    // Verify the properties are set correctly
    expect(tableProperties['write.variant.shred-columns']).toBe('$data');
    expect(tableProperties['write.variant.$data.shred-fields']).toBe(
      'titleType,releaseYear,rating'
    );
    expect(tableProperties['write.variant.$data.field-types']).toMatch(/titleType:string/);
    expect(tableProperties['write.variant.$data.field-types']).toMatch(/releaseYear:int/);
    expect(tableProperties['write.variant.$data.field-types']).toMatch(/rating:double/);
  });

  it('should validate shredding configuration', () => {
    // Valid configuration
    const validConfig: VariantShredPropertyConfig = {
      columnName: '$data',
      fields: ['title', 'year'],
      fieldTypes: { title: 'string', year: 'int' },
    };

    // Should not throw
    expect(() => validateShredConfig(validConfig)).not.toThrow();

    // Invalid: empty fields array
    const invalidConfig: VariantShredPropertyConfig = {
      columnName: '$data',
      fields: [],
      fieldTypes: {},
    };

    expect(() => validateShredConfig(invalidConfig)).toThrow(/fields/i);
  });

  it('should assign unique field IDs to shredded columns', () => {
    const configs: VariantShredPropertyConfig[] = [
      {
        columnName: '$data',
        fields: ['titleType', 'releaseYear', 'rating'],
        fieldTypes: {
          titleType: 'string',
          releaseYear: 'int',
          rating: 'double',
        },
      },
    ];

    // Assign field IDs starting from 1000 (to avoid conflicts with schema fields)
    const fieldIdMap = assignShreddedFieldIds(configs, 1000);

    // Each shredded field gets a unique ID
    expect(fieldIdMap.get('$data.typed_value.titleType.typed_value')).toBe(1000);
    expect(fieldIdMap.get('$data.typed_value.releaseYear.typed_value')).toBe(1001);
    expect(fieldIdMap.get('$data.typed_value.rating.typed_value')).toBe(1002);
  });

  it('should create shredded field info with computed statistics path', () => {
    // Create field info for a shredded field
    const fieldInfo = createShreddedFieldInfo('$data', 'titleType', 'string');

    expect(fieldInfo).toEqual({
      path: 'titleType',
      type: 'string',
      statisticsPath: '$data.typed_value.titleType.typed_value',
    });
  });

  it('should create complete variant column schema', () => {
    const fields = [
      createShreddedFieldInfo('$data', 'titleType', 'string'),
      createShreddedFieldInfo('$data', 'releaseYear', 'int'),
    ];

    const schema = createVariantColumnSchema('$data', fields);

    expect(schema).toEqual({
      columnName: '$data',
      metadataPath: '$data.metadata',
      valuePath: '$data.value',
      typedValuePath: '$data.typed_value',
      shreddedFields: fields,
    });
  });
});

// ============================================================================
// Example 2: Querying with Variant Filters
// ============================================================================

describe('Example 2: Querying with variant filters', () => {
  /**
   * This example shows how to transform user queries on variant fields
   * into optimized queries that use shredded column statistics.
   *
   * Use case: User wants to find all movies from 2020 with rating > 8.0
   */

  const shreddingConfigs: VariantShredPropertyConfig[] = [
    {
      columnName: '$data',
      fields: ['titleType', 'releaseYear', 'rating'],
      fieldTypes: {
        titleType: 'string',
        releaseYear: 'int',
        rating: 'double',
      },
    },
  ];

  it('should transform variant filter to statistics paths', () => {
    // User's filter using variant field paths
    const userFilter = {
      '$data.releaseYear': 2020,
      '$data.rating': { $gt: 8.0 },
    };

    // Transform to use statistics paths
    const result = transformVariantFilter(userFilter, shreddingConfigs);

    // Filter paths are rewritten to use typed_value paths
    expect(result.filter).toEqual({
      '$data.typed_value.releaseYear.typed_value': 2020,
      '$data.typed_value.rating.typed_value': { $gt: 8.0 },
    });

    // Track which paths were transformed
    expect(result.transformedPaths).toContain('$data.releaseYear');
    expect(result.transformedPaths).toContain('$data.rating');
    expect(result.untransformedPaths).toHaveLength(0);
  });

  it('should handle filters with non-shredded fields', () => {
    const filter = {
      '$data.releaseYear': 2020,
      '$data.director': 'Christopher Nolan', // Not a shredded field
    };

    const result = transformVariantFilter(filter, shreddingConfigs);

    // Shredded field is transformed
    expect(result.filter).toHaveProperty('$data.typed_value.releaseYear.typed_value');

    // Non-shredded field is preserved as-is
    expect(result.filter).toHaveProperty('$data.director');

    // Track untransformed paths
    expect(result.untransformedPaths).toContain('$data.director');
  });

  it('should map filter path to statistics path', () => {
    // Get the statistics path for a filter path
    const statsPath = mapFilterPathToStats('$data.releaseYear', shreddingConfigs);

    expect(statsPath).toBe('$data.typed_value.releaseYear.typed_value');

    // Non-shredded field returns null
    const unknownPath = mapFilterPathToStats('$data.unknown', shreddingConfigs);
    expect(unknownPath).toBeNull();
  });

  it('should extract variant columns needed for filter', () => {
    const filter = {
      '$data.releaseYear': 2020,
      '$data.rating': { $gt: 8.0 },
      id: 123, // Non-variant field
    };

    const result = extractVariantFilterColumns(filter, shreddingConfigs);

    // Identifies which variant columns need to be read
    expect(result.readColumns).toEqual(['$data']);

    // Identifies statistics paths for predicate pushdown
    expect(result.statsColumns).toContain('$data.typed_value.releaseYear.typed_value');
    expect(result.statsColumns).toContain('$data.typed_value.rating.typed_value');
  });

  it('should support complex filter operators', () => {
    const complexFilter = {
      '$data.releaseYear': { $gte: 2010, $lte: 2020 },
      '$data.rating': { $in: [8.0, 8.5, 9.0] },
    };

    const result = transformVariantFilter(complexFilter, shreddingConfigs);

    // Operators are preserved
    const yearFilter = result.filter['$data.typed_value.releaseYear.typed_value'] as Record<
      string,
      unknown
    >;
    expect(yearFilter.$gte).toBe(2010);
    expect(yearFilter.$lte).toBe(2020);

    const ratingFilter = result.filter['$data.typed_value.rating.typed_value'] as Record<
      string,
      unknown
    >;
    expect(ratingFilter.$in).toEqual([8.0, 8.5, 9.0]);
  });
});

// ============================================================================
// Example 3: Statistics-based Filtering
// ============================================================================

describe('Example 3: Statistics-based filtering', () => {
  /**
   * This example shows how to collect statistics from data and use them
   * to filter files during query planning.
   *
   * Use case: After writing parquet files, we collect min/max bounds for
   * shredded fields and use them to skip files that don't match query predicates.
   */

  const configs: VariantShredPropertyConfig[] = [
    {
      columnName: '$data',
      fields: ['releaseYear', 'rating'],
      fieldTypes: {
        releaseYear: 'int',
        rating: 'double',
      },
    },
  ];

  it('should collect stats from column values', () => {
    // Simulated data from a parquet row group
    const columns = [
      { path: 'releaseYear', values: [2018, 2019, 2020, 2021, 2022] },
      { path: 'rating', values: [7.5, 8.0, 8.5, null, 9.0] },
    ];

    // Collect statistics
    const result = collectShreddedColumnStats(columns, configs, 1000);

    // Check releaseYear stats
    const yearStats = result.stats.find((s) => s.path === 'releaseYear');
    expect(yearStats).toBeDefined();
    expect(yearStats!.lowerBound).toBe(2018);
    expect(yearStats!.upperBound).toBe(2022);
    expect(yearStats!.nullCount).toBe(0);
    expect(yearStats!.valueCount).toBe(5);

    // Check rating stats
    const ratingStats = result.stats.find((s) => s.path === 'rating');
    expect(ratingStats).toBeDefined();
    expect(ratingStats!.lowerBound).toBe(7.5);
    expect(ratingStats!.upperBound).toBe(9.0);
    expect(ratingStats!.nullCount).toBe(1);
    expect(ratingStats!.valueCount).toBe(5);

    // Check field ID mapping
    expect(result.fieldIdMap.get('releaseYear')).toBe(1000);
    expect(result.fieldIdMap.get('rating')).toBe(1001);
  });

  it('should add shredded stats to a DataFile', () => {
    // Create a base data file
    const baseDataFile: DataFile = {
      content: 0,
      'file-path': 's3://bucket/table/data/file1.parquet',
      'file-format': 'parquet',
      'record-count': 100,
      'file-size-in-bytes': 1024,
    };

    // Collect stats
    const columns = [
      { path: 'releaseYear', values: [2018, 2019, 2020] },
      { path: 'rating', values: [7.5, 8.0, 8.5] },
    ];
    const stats = collectShreddedColumnStats(columns, configs, 1000);

    // Add stats to data file
    const dataFileWithStats = addShreddedStatsToDataFile(baseDataFile, stats);

    // Verify stats were added
    expect(dataFileWithStats['lower-bounds']).toBeDefined();
    expect(dataFileWithStats['upper-bounds']).toBeDefined();
    expect(dataFileWithStats['null-value-counts']).toBeDefined();
    expect(dataFileWithStats['value-counts']).toBeDefined();

    // Field IDs are used as keys
    expect(dataFileWithStats['lower-bounds']![1000]).toBeDefined();
    expect(dataFileWithStats['upper-bounds']![1000]).toBeDefined();
  });

  it('should filter files using stats', () => {
    const fieldIdMap = assignShreddedFieldIds(configs, 1000);

    // Helper to properly encode an int32 as Uint8Array (little-endian)
    const encodeInt = (value: number): Uint8Array => {
      const buffer = new ArrayBuffer(4);
      new DataView(buffer).setInt32(0, value, true);
      return new Uint8Array(buffer);
    };

    // Create data files with different year ranges
    const createDataFile = (
      path: string,
      yearLower: number,
      yearUpper: number
    ): DataFile => ({
      content: 0,
      'file-path': path,
      'file-format': 'parquet',
      'record-count': 100,
      'file-size-in-bytes': 1024,
      'lower-bounds': {
        1000: encodeInt(yearLower),
      },
      'upper-bounds': {
        1000: encodeInt(yearUpper),
      },
    });

    const dataFiles = [
      createDataFile('file1.parquet', 2010, 2015), // 2010-2015
      createDataFile('file2.parquet', 2016, 2018), // 2016-2018
      createDataFile('file3.parquet', 2019, 2022), // 2019-2022
    ];

    // Query for movies from 2020 or later
    const filter = { '$data.releaseYear': { $gte: 2020 } };

    const result = filterDataFiles(dataFiles, filter, configs, fieldIdMap);

    // Only file3 has years >= 2020
    expect(result).toHaveLength(1);
    expect(result[0]['file-path']).toBe('file3.parquet');
  });

  it('should return filter statistics', () => {
    const fieldIdMap = assignShreddedFieldIds(configs, 1000);

    const encodeInt = (value: number): Uint8Array => {
      const buffer = new ArrayBuffer(4);
      new DataView(buffer).setInt32(0, value, true);
      return new Uint8Array(buffer);
    };

    const dataFiles: DataFile[] = [
      {
        content: 0,
        'file-path': 'file1.parquet',
        'file-format': 'parquet',
        'record-count': 100,
        'file-size-in-bytes': 1024,
        'lower-bounds': { 1000: encodeInt(2010) },
        'upper-bounds': { 1000: encodeInt(2015) },
      },
      {
        content: 0,
        'file-path': 'file2.parquet',
        'file-format': 'parquet',
        'record-count': 100,
        'file-size-in-bytes': 1024,
        'lower-bounds': { 1000: encodeInt(2020) },
        'upper-bounds': { 1000: encodeInt(2025) },
      },
    ];

    const filter = { '$data.releaseYear': { $gte: 2020 } };

    const { files, stats } = filterDataFilesWithStats(dataFiles, filter, configs, fieldIdMap);

    expect(files).toHaveLength(1);
    expect(stats.totalFiles).toBe(2);
    expect(stats.skippedFiles).toBe(1);
    expect(stats.skippedByField.get('$data.releaseYear')).toBe(1);
  });
});

// ============================================================================
// Example 4: End-to-end Workflow
// ============================================================================

describe('Example 4: End-to-end workflow', () => {
  /**
   * This example demonstrates a complete workflow:
   * 1. Configure variant shredding for a table
   * 2. Write data with statistics
   * 3. Query with filter and get predicate pushdown
   */

  it('should complete full workflow: configure -> write -> query', () => {
    // Step 1: Configure shredding
    const shreddingConfig: VariantShredPropertyConfig = {
      columnName: '$data',
      fields: ['category', 'price', 'inStock'],
      fieldTypes: {
        category: 'string',
        price: 'double',
        inStock: 'boolean',
      },
    };

    // Create table properties
    const tableProperties = toTableProperties([shreddingConfig]);

    // Step 2: Write data - simulate collecting stats from parquet files
    const fieldIdMap = assignShreddedFieldIds([shreddingConfig], 1000);

    // Helper to encode stats
    const encodeDouble = (value: number): Uint8Array => {
      const buffer = new ArrayBuffer(8);
      new DataView(buffer).setFloat64(0, value, true);
      return new Uint8Array(buffer);
    };

    const encodeString = (value: string): Uint8Array => {
      return new TextEncoder().encode(value);
    };

    // Create data files with stats
    const dataFiles: DataFile[] = [
      {
        content: 0,
        'file-path': 'electronics.parquet',
        'file-format': 'parquet',
        'record-count': 1000,
        'file-size-in-bytes': 10240,
        'lower-bounds': {
          1000: encodeString('electronics'),
          1001: encodeDouble(10.0),
        },
        'upper-bounds': {
          1000: encodeString('electronics'),
          1001: encodeDouble(999.99),
        },
      },
      {
        content: 0,
        'file-path': 'clothing.parquet',
        'file-format': 'parquet',
        'record-count': 500,
        'file-size-in-bytes': 5120,
        'lower-bounds': {
          1000: encodeString('clothing'),
          1001: encodeDouble(5.0),
        },
        'upper-bounds': {
          1000: encodeString('clothing'),
          1001: encodeDouble(200.0),
        },
      },
      {
        content: 0,
        'file-path': 'furniture.parquet',
        'file-format': 'parquet',
        'record-count': 200,
        'file-size-in-bytes': 2048,
        'lower-bounds': {
          1000: encodeString('furniture'),
          1001: encodeDouble(50.0),
        },
        'upper-bounds': {
          1000: encodeString('furniture'),
          1001: encodeDouble(5000.0),
        },
      },
    ];

    // Step 3: Query with filter
    // Find products with price > 500
    const configs = extractVariantShredConfig(tableProperties);
    const filter = { '$data.price': { $gt: 500.0 } };

    const result = filterDataFilesWithStats(dataFiles, filter, configs, fieldIdMap);

    // Only electronics and furniture can have prices > 500
    expect(result.files).toHaveLength(2);
    expect(result.files.map((f) => f['file-path'])).toContain('electronics.parquet');
    expect(result.files.map((f) => f['file-path'])).toContain('furniture.parquet');

    // Clothing file was skipped (max price 200 < 500)
    expect(result.stats.skippedFiles).toBe(1);
    expect(result.stats.skippedByField.get('$data.price')).toBe(1);
  });

  it('should enable predicate pushdown with shouldSkipDataFile', () => {
    const configs: VariantShredPropertyConfig[] = [
      {
        columnName: '$data',
        fields: ['year', 'genre'],
        fieldTypes: { year: 'int', genre: 'string' },
      },
    ];

    const fieldIdMap = assignShreddedFieldIds(configs, 1000);

    const encodeInt = (value: number): Uint8Array => {
      const buffer = new ArrayBuffer(4);
      new DataView(buffer).setInt32(0, value, true);
      return new Uint8Array(buffer);
    };

    // Data file with years 2010-2015
    const dataFile: DataFile = {
      content: 0,
      'file-path': 'old_movies.parquet',
      'file-format': 'parquet',
      'record-count': 100,
      'file-size-in-bytes': 1024,
      'lower-bounds': { 1000: encodeInt(2010) },
      'upper-bounds': { 1000: encodeInt(2015) },
    };

    // Query for movies from 2020
    const filter = { '$data.year': { $gte: 2020 } };

    const result = shouldSkipDataFile(dataFile, filter, configs, fieldIdMap);

    // File can be skipped because max year (2015) < filter year (2020)
    expect(result.skip).toBe(true);
    expect(result.reason).toMatch(/bounds.*do not overlap/i);
  });
});

// ============================================================================
// Example 5: API Surface Validation
// ============================================================================

describe('Example 5: API surface validation', () => {
  /**
   * These tests validate that all documented exports are available
   * and function signatures match expectations.
   */

  it('should export all documented type helpers', () => {
    // Type creation helpers
    expect(typeof createShreddedFieldInfo).toBe('function');
    expect(typeof createVariantColumnSchema).toBe('function');
    expect(typeof validateVariantShredConfig).toBe('function');
  });

  it('should export all documented config functions', () => {
    // Config serialization/deserialization
    expect(typeof toTableProperties).toBe('function');
    expect(typeof extractVariantShredConfig).toBe('function');
    expect(typeof validateShredConfig).toBe('function');
  });

  it('should export all documented statistics path functions', () => {
    // Statistics path utilities
    expect(typeof getStatisticsPaths).toBe('function');
    expect(typeof mapFilterPathToStats).toBe('function');
    expect(typeof extractVariantFilterColumns).toBe('function');
    expect(typeof getShreddedStatisticsPaths).toBe('function');
  });

  it('should export all documented filter functions', () => {
    // Filter transformation
    expect(typeof transformVariantFilter).toBe('function');
  });

  it('should export all documented field ID functions', () => {
    // Field ID assignment
    expect(typeof assignShreddedFieldIds).toBe('function');
  });

  it('should export all documented stats collection functions', () => {
    // Stats collection
    expect(typeof collectShreddedColumnStats).toBe('function');
    expect(typeof addShreddedStatsToDataFile).toBe('function');
  });

  it('should export all documented filtering functions', () => {
    // Row group filtering
    expect(typeof filterDataFiles).toBe('function');
    expect(typeof filterDataFilesWithStats).toBe('function');
    expect(typeof createRangePredicate).toBe('function');
    expect(typeof evaluateRangePredicate).toBe('function');
  });

  it('should export all documented predicate pushdown functions', () => {
    // Predicate pushdown
    expect(typeof shouldSkipDataFile).toBe('function');
    expect(typeof boundsOverlapValue).toBe('function');
  });

  it('should have correct function signatures for key APIs', () => {
    // Test createShreddedFieldInfo signature
    const fieldInfo = createShreddedFieldInfo('$data', 'field', 'string');
    expect(fieldInfo).toHaveProperty('path');
    expect(fieldInfo).toHaveProperty('type');
    expect(fieldInfo).toHaveProperty('statisticsPath');

    // Test getStatisticsPaths signature
    const paths = getStatisticsPaths('$data', ['a', 'b']);
    expect(Array.isArray(paths)).toBe(true);
    expect(paths).toHaveLength(2);

    // Test transformVariantFilter signature
    const result = transformVariantFilter({}, []);
    expect(result).toHaveProperty('filter');
    expect(result).toHaveProperty('transformedPaths');
    expect(result).toHaveProperty('untransformedPaths');

    // Test assignShreddedFieldIds signature
    const map = assignShreddedFieldIds([], 1000);
    expect(map instanceof Map).toBe(true);
  });

  it('should validate types are correctly typed', () => {
    // VariantShredPropertyConfig type check
    const config: VariantShredPropertyConfig = {
      columnName: '$data',
      fields: ['test'],
      fieldTypes: { test: 'string' },
    };

    expect(config.columnName).toBe('$data');
    expect(config.fields).toEqual(['test']);
    expect(config.fieldTypes.test).toBe('string');
  });
});
