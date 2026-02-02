/**
 * Iceberg Metadata.json Generation Tests
 *
 * This module contains comprehensive tests for Iceberg metadata.json file generation
 * following the Apache Iceberg specification.
 *
 * Test Categories:
 * 1. Metadata File Structure - Required fields per Iceberg v2 spec
 * 2. Table UUID Generation - Unique identifier generation and persistence
 * 3. Schema Serialization - Schema and field type serialization
 * 4. Partition Spec Serialization - Partition transforms and fields
 * 5. Snapshot References - Branches and tags (refs)
 * 6. Current Snapshot Tracking - current-snapshot-id and snapshot-log
 * 7. Format Version Handling - v1 vs v2 format differences
 *
 * @see https://iceberg.apache.org/spec/
 */

// Re-export test files for documentation
export * from './metadata-structure.test.js';
export * from './table-uuid.test.js';
export * from './schema-serialization.test.js';
export * from './partition-spec-serialization.test.js';
export * from './snapshot-refs.test.js';
export * from './current-snapshot.test.js';
export * from './format-version.test.js';
