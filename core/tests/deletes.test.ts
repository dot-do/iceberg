import { describe, it, expect, beforeEach } from 'vitest';
import {
  // Constants
  CONTENT_DATA,
  CONTENT_POSITION_DELETES,
  CONTENT_EQUALITY_DELETES,
  MANIFEST_CONTENT_DATA,
  MANIFEST_CONTENT_DELETES,
  // Schema
  POSITION_DELETE_SCHEMA,
  // Builders
  PositionDeleteBuilder,
  EqualityDeleteBuilder,
  DeleteManifestGenerator,
  // Lookups
  PositionDeleteLookup,
  EqualityDeleteLookup,
  // Merger
  DeleteMerger,
  // Parsers
  parsePositionDeleteFile,
  parseEqualityDeleteFile,
  // Application
  applyDeletes,
  // Type guards
  isDeleteFile,
  isPositionDeleteFile,
  isEqualityDeleteFile,
  // Utilities
  getDeleteContentTypeName,
  createEqualityDeleteSchema,
  // Types
  type DataFile,
  type IcebergSchema,
  createDefaultSchema,
} from '../src/index.js';

// ============================================================================
// Test Schema
// ============================================================================

const testSchema: IcebergSchema = {
  'schema-id': 0,
  type: 'struct',
  fields: [
    { id: 1, name: 'id', required: true, type: 'long' },
    { id: 2, name: 'name', required: true, type: 'string' },
    { id: 3, name: 'email', required: false, type: 'string' },
    { id: 4, name: 'age', required: false, type: 'int' },
  ],
};

// ============================================================================
// Constants Tests
// ============================================================================

describe('Delete Constants', () => {
  it('should have correct content type values', () => {
    expect(CONTENT_DATA).toBe(0);
    expect(CONTENT_POSITION_DELETES).toBe(1);
    expect(CONTENT_EQUALITY_DELETES).toBe(2);
  });

  it('should have correct manifest content type values', () => {
    expect(MANIFEST_CONTENT_DATA).toBe(0);
    expect(MANIFEST_CONTENT_DELETES).toBe(1);
  });
});

describe('Position Delete Schema', () => {
  it('should have correct structure', () => {
    expect(POSITION_DELETE_SCHEMA.type).toBe('struct');
    expect(POSITION_DELETE_SCHEMA.fields).toHaveLength(2);

    const filePathField = POSITION_DELETE_SCHEMA.fields[0];
    expect(filePathField.name).toBe('file_path');
    expect(filePathField.type).toBe('string');
    expect(filePathField.required).toBe(true);

    const posField = POSITION_DELETE_SCHEMA.fields[1];
    expect(posField.name).toBe('pos');
    expect(posField.type).toBe('long');
    expect(posField.required).toBe(true);
  });
});

// ============================================================================
// PositionDeleteBuilder Tests
// ============================================================================

describe('PositionDeleteBuilder', () => {
  it('should create a position delete builder', () => {
    const builder = new PositionDeleteBuilder({
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    expect(builder.getEntryCount()).toBe(0);
    expect(builder.hasEntries()).toBe(false);
  });

  it('should add position deletes', () => {
    const builder = new PositionDeleteBuilder({
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    builder.addDelete('data/file1.parquet', 42);
    builder.addDelete('data/file1.parquet', 100);
    builder.addDelete('data/file2.parquet', 5);

    expect(builder.getEntryCount()).toBe(3);
    expect(builder.hasEntries()).toBe(true);
  });

  it('should add multiple deletes for a file', () => {
    const builder = new PositionDeleteBuilder({
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    builder.addDeletesForFile('data/file1.parquet', [10, 20, 30, 40, 50]);

    expect(builder.getEntryCount()).toBe(5);
  });

  it('should reject negative positions', () => {
    const builder = new PositionDeleteBuilder({
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    expect(() => builder.addDelete('data/file.parquet', -1)).toThrow(
      'Position must be non-negative'
    );
  });

  it('should build a position delete file', () => {
    const builder = new PositionDeleteBuilder({
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    builder.addDelete('data/file1.parquet', 42);
    builder.addDelete('data/file1.parquet', 100);
    builder.addDelete('data/file2.parquet', 5);

    const result = builder.build();

    expect(result.deleteFile.content).toBe(CONTENT_POSITION_DELETES);
    expect(result.deleteFile['record-count']).toBe(3);
    expect(result.deleteFile['file-format']).toBe('parquet');
    expect(result.deleteFile['file-path']).toContain('position-delete');
    expect(result.data.byteLength).toBeGreaterThan(0);
    expect(result.statistics.recordCount).toBe(3);
    expect(result.statistics.fileSizeBytes).toBeGreaterThan(0);
    expect(result.statistics.uniqueFilesCount).toBe(2);
    expect(result.statistics.lowerBoundFilePath).toBe('data/file1.parquet');
    expect(result.statistics.upperBoundFilePath).toBe('data/file2.parquet');
  });

  it('should sort entries by file path and position', () => {
    const builder = new PositionDeleteBuilder({
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    // Add out of order
    builder.addDelete('data/file2.parquet', 100);
    builder.addDelete('data/file1.parquet', 50);
    builder.addDelete('data/file2.parquet', 10);
    builder.addDelete('data/file1.parquet', 5);

    const result = builder.build();
    const parsed = parsePositionDeleteFile(result.data);

    // Should be sorted
    expect(parsed[0]).toEqual({ filePath: 'data/file1.parquet', pos: 5 });
    expect(parsed[1]).toEqual({ filePath: 'data/file1.parquet', pos: 50 });
    expect(parsed[2]).toEqual({ filePath: 'data/file2.parquet', pos: 10 });
    expect(parsed[3]).toEqual({ filePath: 'data/file2.parquet', pos: 100 });
  });

  it('should not allow building twice', () => {
    const builder = new PositionDeleteBuilder({
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    builder.addDelete('data/file.parquet', 1);
    builder.build();

    expect(() => builder.addDelete('data/file.parquet', 2)).toThrow('already been built');
  });

  it('should support custom output prefix', () => {
    const builder = new PositionDeleteBuilder({
      sequenceNumber: 5,
      snapshotId: 1234567890,
      outputPrefix: 'custom/path/',
    });

    builder.addDelete('data/file.parquet', 1);
    const result = builder.build();

    expect(result.deleteFile['file-path']).toContain('custom/path/');
  });

  it('should include partition data', () => {
    const builder = new PositionDeleteBuilder({
      sequenceNumber: 5,
      snapshotId: 1234567890,
      partition: { date: '2024-01-15' },
    });

    builder.addDelete('data/file.parquet', 1);
    const result = builder.build();

    expect(result.deleteFile.partition).toEqual({ date: '2024-01-15' });
  });
});

// ============================================================================
// EqualityDeleteBuilder Tests
// ============================================================================

describe('EqualityDeleteBuilder', () => {
  it('should create an equality delete builder', () => {
    const builder = new EqualityDeleteBuilder({
      schema: testSchema,
      equalityFieldIds: [1, 2],
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    expect(builder.getEntryCount()).toBe(0);
    expect(builder.hasEntries()).toBe(false);
    expect(builder.getEqualityFieldIds()).toEqual([1, 2]);
    expect(builder.getEqualityFieldNames()).toEqual(['id', 'name']);
  });

  it('should add equality deletes', () => {
    const builder = new EqualityDeleteBuilder({
      schema: testSchema,
      equalityFieldIds: [1, 2],
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    builder.addDelete({ id: 123, name: 'Alice' });
    builder.addDelete({ id: 456, name: 'Bob' });

    expect(builder.getEntryCount()).toBe(2);
    expect(builder.hasEntries()).toBe(true);
  });

  it('should reject missing equality fields', () => {
    const builder = new EqualityDeleteBuilder({
      schema: testSchema,
      equalityFieldIds: [1, 2],
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    expect(() => builder.addDelete({ id: 123 })).toThrow('Missing required equality field: name');
  });

  it('should reject invalid field IDs', () => {
    expect(
      () =>
        new EqualityDeleteBuilder({
          schema: testSchema,
          equalityFieldIds: [1, 999],
          sequenceNumber: 5,
          snapshotId: 1234567890,
        })
    ).toThrow('Field ID 999 not found in schema');
  });

  it('should build an equality delete file', () => {
    const builder = new EqualityDeleteBuilder({
      schema: testSchema,
      equalityFieldIds: [1, 2],
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    builder.addDelete({ id: 123, name: 'Alice' });
    builder.addDelete({ id: 456, name: 'Bob' });

    const result = builder.build();

    expect(result.deleteFile.content).toBe(CONTENT_EQUALITY_DELETES);
    expect(result.deleteFile['record-count']).toBe(2);
    expect(result.deleteFile['equality-ids']).toEqual([1, 2]);
    expect(result.deleteFile['file-format']).toBe('parquet');
    expect(result.deleteFile['file-path']).toContain('equality-delete');
    expect(result.data.byteLength).toBeGreaterThan(0);
    expect(result.statistics.recordCount).toBe(2);
  });

  it('should compute statistics for numeric fields', () => {
    const builder = new EqualityDeleteBuilder({
      schema: testSchema,
      equalityFieldIds: [1],
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    builder.addDelete({ id: 100 });
    builder.addDelete({ id: 500 });
    builder.addDelete({ id: 200 });

    const result = builder.build();

    expect(result.statistics.lowerBounds[1]).toBe(100);
    expect(result.statistics.upperBounds[1]).toBe(500);
    expect(result.statistics.nullCounts[1]).toBe(0);
  });

  it('should compute statistics for string fields', () => {
    const builder = new EqualityDeleteBuilder({
      schema: testSchema,
      equalityFieldIds: [2],
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    builder.addDelete({ name: 'Charlie' });
    builder.addDelete({ name: 'Alice' });
    builder.addDelete({ name: 'Bob' });

    const result = builder.build();

    expect(result.statistics.lowerBounds[2]).toBe('Alice');
    expect(result.statistics.upperBounds[2]).toBe('Charlie');
  });

  it('should only store equality field values', () => {
    const builder = new EqualityDeleteBuilder({
      schema: testSchema,
      equalityFieldIds: [1],
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    // Pass extra fields - they should be ignored
    builder.addDelete({ id: 123, name: 'Alice', email: 'alice@example.com' });

    const result = builder.build();
    const parsed = parseEqualityDeleteFile(result.data);

    expect(parsed.entries[0]).toEqual({ id: 123 });
    expect(parsed.entries[0]).not.toHaveProperty('name');
  });
});

// ============================================================================
// DeleteManifestGenerator Tests
// ============================================================================

describe('DeleteManifestGenerator', () => {
  it('should create an empty delete manifest', () => {
    const generator = new DeleteManifestGenerator({
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    expect(generator.entryCount).toBe(0);

    const result = generator.generate();
    expect(result.entries).toHaveLength(0);
    expect(result.summary.addedDeleteFiles).toBe(0);
  });

  it('should add position delete files', () => {
    const generator = new DeleteManifestGenerator({
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    generator.addPositionDeleteFile({
      'file-path': 'data/delete/pos-delete-1.parquet',
      'file-format': 'parquet',
      partition: {},
      'record-count': 100,
      'file-size-in-bytes': 4096,
    });

    expect(generator.entryCount).toBe(1);

    const result = generator.generate();
    expect(result.entries[0]['data-file'].content).toBe(CONTENT_POSITION_DELETES);
    expect(result.summary.positionDeleteFiles).toBe(1);
    expect(result.summary.equalityDeleteFiles).toBe(0);
  });

  it('should add equality delete files', () => {
    const generator = new DeleteManifestGenerator({
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    generator.addEqualityDeleteFile({
      'file-path': 'data/delete/eq-delete-1.parquet',
      'file-format': 'parquet',
      partition: {},
      'record-count': 50,
      'file-size-in-bytes': 2048,
      'equality-ids': [1, 2],
    });

    expect(generator.entryCount).toBe(1);

    const result = generator.generate();
    expect(result.entries[0]['data-file'].content).toBe(CONTENT_EQUALITY_DELETES);
    expect(result.entries[0]['data-file']['equality-ids']).toEqual([1, 2]);
    expect(result.summary.positionDeleteFiles).toBe(0);
    expect(result.summary.equalityDeleteFiles).toBe(1);
  });

  it('should compute summary statistics', () => {
    const generator = new DeleteManifestGenerator({
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    generator.addPositionDeleteFile({
      'file-path': 'data/delete/pos-1.parquet',
      'file-format': 'parquet',
      partition: {},
      'record-count': 100,
      'file-size-in-bytes': 4096,
    });

    generator.addPositionDeleteFile({
      'file-path': 'data/delete/pos-2.parquet',
      'file-format': 'parquet',
      partition: {},
      'record-count': 200,
      'file-size-in-bytes': 8192,
    });

    generator.addEqualityDeleteFile({
      'file-path': 'data/delete/eq-1.parquet',
      'file-format': 'parquet',
      partition: {},
      'record-count': 50,
      'file-size-in-bytes': 2048,
      'equality-ids': [1],
    });

    const result = generator.generate();

    expect(result.summary.addedDeleteFiles).toBe(3);
    expect(result.summary.addedDeleteRows).toBe(350);
    expect(result.summary.positionDeleteFiles).toBe(2);
    expect(result.summary.equalityDeleteFiles).toBe(1);
  });
});

// ============================================================================
// PositionDeleteLookup Tests
// ============================================================================

describe('PositionDeleteLookup', () => {
  it('should create an empty lookup', () => {
    const lookup = new PositionDeleteLookup(5);

    expect(lookup.getTotalDeleteCount()).toBe(0);
    expect(lookup.getSequenceNumber()).toBe(5);
  });

  it('should add and check deletes', () => {
    const lookup = new PositionDeleteLookup(5);

    lookup.addDeletes('data/file1.parquet', [10, 20, 30]);
    lookup.addDeletes('data/file2.parquet', [5, 15]);

    expect(lookup.getTotalDeleteCount()).toBe(5);
    expect(lookup.isDeleted('data/file1.parquet', 10)).toBe(true);
    expect(lookup.isDeleted('data/file1.parquet', 20)).toBe(true);
    expect(lookup.isDeleted('data/file1.parquet', 15)).toBe(false);
    expect(lookup.isDeleted('data/file2.parquet', 5)).toBe(true);
    expect(lookup.isDeleted('data/file3.parquet', 10)).toBe(false);
  });

  it('should get deleted positions for a file', () => {
    const lookup = new PositionDeleteLookup(5);

    lookup.addDeletes('data/file1.parquet', [10, 20, 30]);

    const positions = lookup.getDeletedPositions('data/file1.parquet');
    expect(positions).toBeDefined();
    expect(positions!.has(10)).toBe(true);
    expect(positions!.has(20)).toBe(true);
    expect(positions!.has(30)).toBe(true);

    expect(lookup.getDeletedPositions('data/file2.parquet')).toBeUndefined();
  });

  it('should get files with deletes', () => {
    const lookup = new PositionDeleteLookup(5);

    lookup.addDeletes('data/file1.parquet', [10, 20]);
    lookup.addDeletes('data/file2.parquet', [5]);

    const files = lookup.getFilesWithDeletes();
    expect(files).toHaveLength(2);
    expect(files).toContain('data/file1.parquet');
    expect(files).toContain('data/file2.parquet');
  });
});

// ============================================================================
// EqualityDeleteLookup Tests
// ============================================================================

describe('EqualityDeleteLookup', () => {
  it('should create an empty lookup', () => {
    const lookup = new EqualityDeleteLookup(['id', 'name'], 5);

    expect(lookup.getDeleteCount()).toBe(0);
    expect(lookup.getSequenceNumber()).toBe(5);
    expect(lookup.getFieldNames()).toEqual(['id', 'name']);
  });

  it('should add and check deletes', () => {
    const lookup = new EqualityDeleteLookup(['id', 'name'], 5);

    lookup.addDelete({ id: 123, name: 'Alice' });
    lookup.addDelete({ id: 456, name: 'Bob' });

    expect(lookup.getDeleteCount()).toBe(2);
    expect(lookup.isDeleted({ id: 123, name: 'Alice' })).toBe(true);
    expect(lookup.isDeleted({ id: 456, name: 'Bob' })).toBe(true);
    expect(lookup.isDeleted({ id: 123, name: 'Bob' })).toBe(false);
    expect(lookup.isDeleted({ id: 789, name: 'Charlie' })).toBe(false);
  });

  it('should deduplicate entries', () => {
    const lookup = new EqualityDeleteLookup(['id'], 5);

    lookup.addDelete({ id: 123 });
    lookup.addDelete({ id: 123 });
    lookup.addDelete({ id: 123 });

    expect(lookup.getDeleteCount()).toBe(1);
  });

  it('should ignore extra fields in row values', () => {
    const lookup = new EqualityDeleteLookup(['id'], 5);

    lookup.addDelete({ id: 123 });

    // Extra fields in row should be ignored when checking
    expect(lookup.isDeleted({ id: 123, name: 'Alice', email: 'alice@example.com' })).toBe(true);
  });
});

// ============================================================================
// DeleteMerger Tests
// ============================================================================

describe('DeleteMerger', () => {
  it('should merge position deletes', () => {
    const merger = new DeleteMerger({ schema: testSchema });

    merger.addPositionDeletes([
      { filePath: 'file1.parquet', pos: 10 },
      { filePath: 'file1.parquet', pos: 20 },
      { filePath: 'file2.parquet', pos: 5 },
    ]);

    merger.addPositionDeletes([
      { filePath: 'file1.parquet', pos: 10 }, // Duplicate
      { filePath: 'file1.parquet', pos: 30 },
    ]);

    expect(merger.getPositionDeleteCount()).toBe(4); // Deduped

    const result = merger.merge(123456, 5);

    expect(result.positionDeleteFiles).toHaveLength(1);
    expect(result.positionDeleteFiles[0].statistics.recordCount).toBe(4);
    expect(result.statistics.inputPositionDeleteCount).toBe(4);
    expect(result.statistics.outputPositionDeleteFiles).toBe(1);
  });

  it('should merge equality deletes', () => {
    const merger = new DeleteMerger({ schema: testSchema });

    merger.addEqualityDeletes(
      [{ id: 100, name: 'Alice' }, { id: 200, name: 'Bob' }],
      [1, 2]
    );

    merger.addEqualityDeletes(
      [{ id: 100, name: 'Alice' }, { id: 300, name: 'Charlie' }], // One duplicate
      [1, 2]
    );

    expect(merger.getEqualityDeleteCount()).toBe(3); // Deduped

    const result = merger.merge(123456, 5);

    expect(result.equalityDeleteFiles).toHaveLength(1);
    expect(result.equalityDeleteFiles[0].statistics.recordCount).toBe(3);
  });

  it('should chunk large delete sets', () => {
    const merger = new DeleteMerger({
      schema: testSchema,
      maxEntriesPerFile: 100,
    });

    // Add 250 position deletes
    for (let i = 0; i < 250; i++) {
      merger.addPositionDeletes([{ filePath: 'file.parquet', pos: i }]);
    }

    const result = merger.merge(123456, 5);

    expect(result.positionDeleteFiles).toHaveLength(3);
    expect(result.positionDeleteFiles[0].statistics.recordCount).toBe(100);
    expect(result.positionDeleteFiles[1].statistics.recordCount).toBe(100);
    expect(result.positionDeleteFiles[2].statistics.recordCount).toBe(50);
  });

  it('should clear accumulated deletes', () => {
    const merger = new DeleteMerger({ schema: testSchema });

    merger.addPositionDeletes([{ filePath: 'file.parquet', pos: 10 }]);
    expect(merger.getPositionDeleteCount()).toBe(1);

    merger.clear();
    expect(merger.getPositionDeleteCount()).toBe(0);
  });
});

// ============================================================================
// Parse Functions Tests
// ============================================================================

describe('parsePositionDeleteFile', () => {
  it('should parse position delete file', () => {
    const builder = new PositionDeleteBuilder({
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    builder.addDelete('file1.parquet', 10);
    builder.addDelete('file1.parquet', 20);
    builder.addDelete('file2.parquet', 5);

    const result = builder.build();
    const parsed = parsePositionDeleteFile(result.data);

    expect(parsed).toHaveLength(3);
    expect(parsed[0]).toEqual({ filePath: 'file1.parquet', pos: 10 });
    expect(parsed[1]).toEqual({ filePath: 'file1.parquet', pos: 20 });
    expect(parsed[2]).toEqual({ filePath: 'file2.parquet', pos: 5 });
  });
});

describe('parseEqualityDeleteFile', () => {
  it('should parse equality delete file', () => {
    const builder = new EqualityDeleteBuilder({
      schema: testSchema,
      equalityFieldIds: [1, 2],
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    builder.addDelete({ id: 100, name: 'Alice' });
    builder.addDelete({ id: 200, name: 'Bob' });

    const result = builder.build();
    const parsed = parseEqualityDeleteFile(result.data);

    expect(parsed.equalityFieldIds).toEqual([1, 2]);
    expect(parsed.fieldNames).toEqual(['id', 'name']);
    expect(parsed.entries).toHaveLength(2);
    expect(parsed.entries[0]).toEqual({ id: 100, name: 'Alice' });
    expect(parsed.entries[1]).toEqual({ id: 200, name: 'Bob' });
  });
});

// ============================================================================
// applyDeletes Tests
// ============================================================================

describe('applyDeletes', () => {
  it('should apply position deletes', () => {
    const rows = [
      { row: { id: 1, name: 'Alice' }, position: 0 },
      { row: { id: 2, name: 'Bob' }, position: 1 },
      { row: { id: 3, name: 'Charlie' }, position: 2 },
      { row: { id: 4, name: 'Diana' }, position: 3 },
    ];

    const posLookup = new PositionDeleteLookup(5);
    posLookup.addDeletes('data/file.parquet', [1, 3]); // Delete Bob and Diana

    const { rows: passed, result } = applyDeletes(
      rows,
      'data/file.parquet',
      3, // Data file seq < delete seq
      [posLookup],
      []
    );

    expect(passed).toHaveLength(2);
    expect(passed[0].name).toBe('Alice');
    expect(passed[1].name).toBe('Charlie');
    expect(result.passedRows).toBe(2);
    expect(result.positionDeletedRows).toBe(2);
    expect(result.equalityDeletedRows).toBe(0);
  });

  it('should apply equality deletes', () => {
    const rows = [
      { row: { id: 1, name: 'Alice' }, position: 0 },
      { row: { id: 2, name: 'Bob' }, position: 1 },
      { row: { id: 3, name: 'Charlie' }, position: 2 },
    ];

    const eqLookup = new EqualityDeleteLookup(['name'], 5);
    eqLookup.addDelete({ name: 'Bob' });

    const { rows: passed, result } = applyDeletes(
      rows,
      'data/file.parquet',
      3,
      [],
      [eqLookup]
    );

    expect(passed).toHaveLength(2);
    expect(passed[0].name).toBe('Alice');
    expect(passed[1].name).toBe('Charlie');
    expect(result.equalityDeletedRows).toBe(1);
  });

  it('should respect sequence number ordering', () => {
    const rows = [
      { row: { id: 1, name: 'Alice' }, position: 0 },
      { row: { id: 2, name: 'Bob' }, position: 1 },
    ];

    // Delete with seq 3 should NOT apply to data file with seq 5
    const posLookup = new PositionDeleteLookup(3);
    posLookup.addDeletes('data/file.parquet', [0, 1]);

    const { rows: passed } = applyDeletes(
      rows,
      'data/file.parquet',
      5, // Data file seq > delete seq
      [posLookup],
      []
    );

    // No rows deleted because delete is older than data
    expect(passed).toHaveLength(2);
  });

  it('should combine position and equality deletes', () => {
    const rows = [
      { row: { id: 1, name: 'Alice' }, position: 0 },
      { row: { id: 2, name: 'Bob' }, position: 1 },
      { row: { id: 3, name: 'Charlie' }, position: 2 },
      { row: { id: 4, name: 'Diana' }, position: 3 },
    ];

    const posLookup = new PositionDeleteLookup(5);
    posLookup.addDeletes('data/file.parquet', [0]); // Delete Alice by position

    const eqLookup = new EqualityDeleteLookup(['name'], 5);
    eqLookup.addDelete({ name: 'Charlie' }); // Delete Charlie by equality

    const { rows: passed, result } = applyDeletes(
      rows,
      'data/file.parquet',
      3,
      [posLookup],
      [eqLookup]
    );

    expect(passed).toHaveLength(2);
    expect(passed[0].name).toBe('Bob');
    expect(passed[1].name).toBe('Diana');
    expect(result.positionDeletedRows).toBe(1);
    expect(result.equalityDeletedRows).toBe(1);
  });
});

// ============================================================================
// Type Guard Tests
// ============================================================================

describe('Type Guards', () => {
  const dataFile: DataFile = {
    content: 0,
    'file-path': 'data/file.parquet',
    'file-format': 'parquet',
    partition: {},
    'record-count': 100,
    'file-size-in-bytes': 4096,
  };

  const posDeleteFile: DataFile = {
    content: 1,
    'file-path': 'data/delete/pos.parquet',
    'file-format': 'parquet',
    partition: {},
    'record-count': 10,
    'file-size-in-bytes': 1024,
  };

  const eqDeleteFile: DataFile = {
    content: 2,
    'file-path': 'data/delete/eq.parquet',
    'file-format': 'parquet',
    partition: {},
    'record-count': 5,
    'file-size-in-bytes': 512,
    'equality-ids': [1, 2],
  };

  describe('isDeleteFile', () => {
    it('should return false for data files', () => {
      expect(isDeleteFile(dataFile)).toBe(false);
    });

    it('should return true for position delete files', () => {
      expect(isDeleteFile(posDeleteFile)).toBe(true);
    });

    it('should return true for equality delete files', () => {
      expect(isDeleteFile(eqDeleteFile)).toBe(true);
    });
  });

  describe('isPositionDeleteFile', () => {
    it('should return false for data files', () => {
      expect(isPositionDeleteFile(dataFile)).toBe(false);
    });

    it('should return true for position delete files', () => {
      expect(isPositionDeleteFile(posDeleteFile)).toBe(true);
    });

    it('should return false for equality delete files', () => {
      expect(isPositionDeleteFile(eqDeleteFile)).toBe(false);
    });
  });

  describe('isEqualityDeleteFile', () => {
    it('should return false for data files', () => {
      expect(isEqualityDeleteFile(dataFile)).toBe(false);
    });

    it('should return false for position delete files', () => {
      expect(isEqualityDeleteFile(posDeleteFile)).toBe(false);
    });

    it('should return true for equality delete files', () => {
      expect(isEqualityDeleteFile(eqDeleteFile)).toBe(true);
    });
  });
});

// ============================================================================
// Utility Function Tests
// ============================================================================

describe('getDeleteContentTypeName', () => {
  it('should return correct names', () => {
    expect(getDeleteContentTypeName(0)).toBe('data');
    expect(getDeleteContentTypeName(1)).toBe('position-deletes');
    expect(getDeleteContentTypeName(2)).toBe('equality-deletes');
    expect(getDeleteContentTypeName(99)).toBe('unknown(99)');
  });
});

describe('createEqualityDeleteSchema', () => {
  it('should create schema from field IDs', () => {
    const schema = createEqualityDeleteSchema(testSchema, [1, 2]);

    expect(schema.type).toBe('struct');
    expect(schema.fields).toHaveLength(2);
    expect(schema.fields[0].id).toBe(1);
    expect(schema.fields[0].name).toBe('id');
    expect(schema.fields[0].required).toBe(true);
    expect(schema.fields[1].id).toBe(2);
    expect(schema.fields[1].name).toBe('name');
    expect(schema.fields[1].required).toBe(true);
  });

  it('should throw for missing field IDs', () => {
    expect(() => createEqualityDeleteSchema(testSchema, [1, 999])).toThrow(
      'Field ID 999 not found in schema'
    );
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('End-to-End Delete Flow', () => {
  it('should support complete position delete workflow', () => {
    // 1. Create position deletes
    const builder = new PositionDeleteBuilder({
      sequenceNumber: 5,
      snapshotId: 1234567890,
      partition: { date: '2024-01-15' },
    });

    builder.addDeletesForFile('data/2024-01-15/part-001.parquet', [10, 50, 100]);
    builder.addDeletesForFile('data/2024-01-15/part-002.parquet', [25]);

    const { deleteFile, data, statistics } = builder.build();

    // 2. Parse the delete file
    const parsed = parsePositionDeleteFile(data);
    expect(parsed).toHaveLength(4);

    // 3. Build lookup for read-side
    const lookup = new PositionDeleteLookup(5);
    for (const entry of parsed) {
      lookup.addDeletes(entry.filePath, [entry.pos]);
    }

    // 4. Apply to data
    const rows = [
      { row: { id: 1 }, position: 10 },
      { row: { id: 2 }, position: 11 },
      { row: { id: 3 }, position: 50 },
    ];

    const { rows: passed } = applyDeletes(
      rows,
      'data/2024-01-15/part-001.parquet',
      3,
      [lookup],
      []
    );

    expect(passed).toHaveLength(1);
    expect(passed[0].id).toBe(2);

    // 5. Add to delete manifest
    const manifest = new DeleteManifestGenerator({
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    manifest.addPositionDeleteFile({
      'file-path': deleteFile['file-path'],
      'file-format': deleteFile['file-format'],
      partition: deleteFile.partition,
      'record-count': deleteFile['record-count'],
      'file-size-in-bytes': deleteFile['file-size-in-bytes'],
    });

    const manifestResult = manifest.generate();
    expect(manifestResult.summary.positionDeleteFiles).toBe(1);
  });

  it('should support complete equality delete workflow', () => {
    // 1. Create equality deletes
    const builder = new EqualityDeleteBuilder({
      schema: testSchema,
      equalityFieldIds: [1],
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    builder.addDelete({ id: 100 });
    builder.addDelete({ id: 200 });
    builder.addDelete({ id: 300 });

    const { deleteFile, data } = builder.build();

    // 2. Parse the delete file
    const parsed = parseEqualityDeleteFile(data);
    expect(parsed.entries).toHaveLength(3);

    // 3. Build lookup for read-side
    const lookup = new EqualityDeleteLookup(parsed.fieldNames, 5);
    lookup.addDeletes(parsed.entries);

    // 4. Apply to data
    const rows = [
      { row: { id: 100, name: 'Alice' }, position: 0 },
      { row: { id: 150, name: 'Bob' }, position: 1 },
      { row: { id: 200, name: 'Charlie' }, position: 2 },
      { row: { id: 250, name: 'Diana' }, position: 3 },
    ];

    const { rows: passed } = applyDeletes(
      rows,
      'data/file.parquet',
      3,
      [],
      [lookup]
    );

    expect(passed).toHaveLength(2);
    expect(passed[0].name).toBe('Bob');
    expect(passed[1].name).toBe('Diana');

    // 5. Add to delete manifest
    const manifest = new DeleteManifestGenerator({
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    manifest.addEqualityDeleteFile({
      'file-path': deleteFile['file-path'],
      'file-format': deleteFile['file-format'],
      partition: deleteFile.partition,
      'record-count': deleteFile['record-count'],
      'file-size-in-bytes': deleteFile['file-size-in-bytes'],
      'equality-ids': deleteFile['equality-ids'],
    });

    const manifestResult = manifest.generate();
    expect(manifestResult.summary.equalityDeleteFiles).toBe(1);
  });
});
