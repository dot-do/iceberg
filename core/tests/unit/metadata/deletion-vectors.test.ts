/**
 * Deletion Vector Data File Fields Tests
 *
 * Tests for content-offset, content-size-in-bytes, and referenced-data-file
 * fields in DataFile for deletion vector support.
 *
 * @see https://iceberg.apache.org/spec/#deletion-vectors
 */
import { describe, it, expect } from 'vitest';
import type { DataFile, ManifestEntry, ManifestFile } from '../../../src/metadata/types.js';
import {
  isDeletionVector,
  validateDeletionVectorFields,
  type DeletionVectorValidationResult,
} from '../../../src/metadata/types.js';
import {
  CONTENT_POSITION_DELETES,
  MANIFEST_CONTENT_DELETES,
} from '../../../src/metadata/constants.js';
import { ManifestGenerator, ManifestListGenerator } from '../../../src/metadata/manifest.js';
import { DeleteManifestGenerator } from '../../../src/metadata/deletes.js';
import {
  findDeletionVectorsForFile,
  shouldIgnorePositionDeletes,
  createDeletionVectorEntry,
  validateV3DeletionVectorRules,
  countDeletionVectorsPerDataFile,
} from '../../../src/metadata/deletion-vectors.js';

// ============================================================================
// Type Tests - DataFile optional fields
// ============================================================================

describe('DataFile Deletion Vector Fields - Type Tests', () => {
  it('should allow DataFile to have optional content-offset (number)', () => {
    const dataFile: DataFile = {
      content: 0,
      'file-path': 's3://bucket/data/file.parquet',
      'file-format': 'parquet',
      partition: {},
      'record-count': 100,
      'file-size-in-bytes': 4096,
      'content-offset': 1024, // Byte offset in Puffin file
    };

    expect(dataFile['content-offset']).toBe(1024);
  });

  it('should allow DataFile to have optional content-size-in-bytes (number)', () => {
    const dataFile: DataFile = {
      content: 0,
      'file-path': 's3://bucket/data/file.parquet',
      'file-format': 'parquet',
      partition: {},
      'record-count': 100,
      'file-size-in-bytes': 4096,
      'content-size-in-bytes': 512, // Blob size in Puffin file
    };

    expect(dataFile['content-size-in-bytes']).toBe(512);
  });

  it('should allow DataFile to have optional referenced-data-file (string)', () => {
    const dataFile: DataFile = {
      content: 1, // position deletes
      'file-path': 's3://bucket/data/dv.puffin',
      'file-format': 'parquet',
      partition: {},
      'record-count': 10,
      'file-size-in-bytes': 1024,
      'referenced-data-file': 's3://bucket/data/original.parquet',
    };

    expect(dataFile['referenced-data-file']).toBe('s3://bucket/data/original.parquet');
  });

  it('should allow DataFile without any deletion vector fields', () => {
    const dataFile: DataFile = {
      content: 0,
      'file-path': 's3://bucket/data/file.parquet',
      'file-format': 'parquet',
      partition: {},
      'record-count': 100,
      'file-size-in-bytes': 4096,
    };

    expect(dataFile['content-offset']).toBeUndefined();
    expect(dataFile['content-size-in-bytes']).toBeUndefined();
    expect(dataFile['referenced-data-file']).toBeUndefined();
  });
});

// ============================================================================
// Deletion Vector Metadata Tests
// ============================================================================

describe('Deletion Vector Metadata', () => {
  it('should identify DV file with content=1 (position deletes)', () => {
    const dvFile: DataFile = {
      content: 1, // position deletes - deletion vectors use this content type
      'file-path': 's3://bucket/data/dv-0001.puffin',
      'file-format': 'parquet',
      partition: {},
      'record-count': 50,
      'file-size-in-bytes': 2048,
      'content-offset': 100,
      'content-size-in-bytes': 1024,
      'referenced-data-file': 's3://bucket/data/00001.parquet',
    };

    expect(isDeletionVector(dvFile)).toBe(true);
    expect(dvFile.content).toBe(CONTENT_POSITION_DELETES);
  });

  it('should require referenced-data-file for deletion vectors', () => {
    const dvFileWithRef: DataFile = {
      content: 1,
      'file-path': 's3://bucket/data/dv-0001.puffin',
      'file-format': 'parquet',
      partition: {},
      'record-count': 50,
      'file-size-in-bytes': 2048,
      'content-offset': 100,
      'content-size-in-bytes': 1024,
      'referenced-data-file': 's3://bucket/data/00001.parquet',
    };

    const dvFileWithoutRef: DataFile = {
      content: 1,
      'file-path': 's3://bucket/data/dv-0001.puffin',
      'file-format': 'parquet',
      partition: {},
      'record-count': 50,
      'file-size-in-bytes': 2048,
      'content-offset': 100,
      'content-size-in-bytes': 1024,
      // Missing referenced-data-file
    };

    expect(isDeletionVector(dvFileWithRef)).toBe(true);
    expect(isDeletionVector(dvFileWithoutRef)).toBe(false);
  });

  it('should require content-offset for deletion vectors', () => {
    const dvWithOffset: DataFile = {
      content: 1,
      'file-path': 's3://bucket/data/dv.puffin',
      'file-format': 'parquet',
      partition: {},
      'record-count': 50,
      'file-size-in-bytes': 2048,
      'content-offset': 100,
      'content-size-in-bytes': 1024,
      'referenced-data-file': 's3://bucket/data/00001.parquet',
    };

    const dvWithoutOffset: DataFile = {
      content: 1,
      'file-path': 's3://bucket/data/dv.puffin',
      'file-format': 'parquet',
      partition: {},
      'record-count': 50,
      'file-size-in-bytes': 2048,
      // Missing content-offset
      'content-size-in-bytes': 1024,
      'referenced-data-file': 's3://bucket/data/00001.parquet',
    };

    expect(isDeletionVector(dvWithOffset)).toBe(true);
    expect(isDeletionVector(dvWithoutOffset)).toBe(false);
  });

  it('should require content-size-in-bytes for deletion vectors', () => {
    const dvWithSize: DataFile = {
      content: 1,
      'file-path': 's3://bucket/data/dv.puffin',
      'file-format': 'parquet',
      partition: {},
      'record-count': 50,
      'file-size-in-bytes': 2048,
      'content-offset': 100,
      'content-size-in-bytes': 1024,
      'referenced-data-file': 's3://bucket/data/00001.parquet',
    };

    const dvWithoutSize: DataFile = {
      content: 1,
      'file-path': 's3://bucket/data/dv.puffin',
      'file-format': 'parquet',
      partition: {},
      'record-count': 50,
      'file-size-in-bytes': 2048,
      'content-offset': 100,
      // Missing content-size-in-bytes
      'referenced-data-file': 's3://bucket/data/00001.parquet',
    };

    expect(isDeletionVector(dvWithSize)).toBe(true);
    expect(isDeletionVector(dvWithoutSize)).toBe(false);
  });
});

// ============================================================================
// Validation Tests
// ============================================================================

describe('Deletion Vector Validation', () => {
  it('should validate that content-offset requires content-size-in-bytes', () => {
    const invalidDV: DataFile = {
      content: 1,
      'file-path': 's3://bucket/data/dv.puffin',
      'file-format': 'parquet',
      partition: {},
      'record-count': 50,
      'file-size-in-bytes': 2048,
      'content-offset': 100,
      // Missing content-size-in-bytes
      'referenced-data-file': 's3://bucket/data/00001.parquet',
    };

    const result = validateDeletionVectorFields(invalidDV);
    expect(result.valid).toBe(false);
    expect(result.errors).toContain('content-offset requires content-size-in-bytes');
  });

  it('should validate that content-size-in-bytes requires content-offset', () => {
    const invalidDV: DataFile = {
      content: 1,
      'file-path': 's3://bucket/data/dv.puffin',
      'file-format': 'parquet',
      partition: {},
      'record-count': 50,
      'file-size-in-bytes': 2048,
      // Missing content-offset
      'content-size-in-bytes': 1024,
      'referenced-data-file': 's3://bucket/data/00001.parquet',
    };

    const result = validateDeletionVectorFields(invalidDV);
    expect(result.valid).toBe(false);
    expect(result.errors).toContain('content-size-in-bytes requires content-offset');
  });

  it('should validate that referenced-data-file is required for deletion vectors with content fields', () => {
    const invalidDV: DataFile = {
      content: 1,
      'file-path': 's3://bucket/data/dv.puffin',
      'file-format': 'parquet',
      partition: {},
      'record-count': 50,
      'file-size-in-bytes': 2048,
      'content-offset': 100,
      'content-size-in-bytes': 1024,
      // Missing referenced-data-file
    };

    const result = validateDeletionVectorFields(invalidDV);
    expect(result.valid).toBe(false);
    expect(result.errors).toContain('referenced-data-file is required for deletion vectors');
  });

  it('should validate regular data files without deletion vector fields', () => {
    const regularDataFile: DataFile = {
      content: 0,
      'file-path': 's3://bucket/data/file.parquet',
      'file-format': 'parquet',
      partition: {},
      'record-count': 100,
      'file-size-in-bytes': 4096,
    };

    const result = validateDeletionVectorFields(regularDataFile);
    expect(result.valid).toBe(true);
    expect(result.errors).toHaveLength(0);
  });

  it('should validate a complete deletion vector entry', () => {
    const validDV: DataFile = {
      content: 1,
      'file-path': 's3://bucket/data/dv.puffin',
      'file-format': 'parquet',
      partition: {},
      'record-count': 50,
      'file-size-in-bytes': 2048,
      'content-offset': 100,
      'content-size-in-bytes': 1024,
      'referenced-data-file': 's3://bucket/data/00001.parquet',
    };

    const result = validateDeletionVectorFields(validDV);
    expect(result.valid).toBe(true);
    expect(result.errors).toHaveLength(0);
  });

  it('should validate position delete file without DV fields (legacy format)', () => {
    const positionDeleteFile: DataFile = {
      content: 1, // position deletes
      'file-path': 's3://bucket/data/delete/pos-delete.parquet',
      'file-format': 'parquet',
      partition: {},
      'record-count': 10,
      'file-size-in-bytes': 1024,
      // No DV fields - this is a legacy position delete file
    };

    const result = validateDeletionVectorFields(positionDeleteFile);
    expect(result.valid).toBe(true);
    expect(result.errors).toHaveLength(0);
  });
});

// ============================================================================
// JSON Serialization Tests
// ============================================================================

describe('DataFile JSON Serialization with Deletion Vector Fields', () => {
  it('should serialize content-offset in DataFile JSON', () => {
    const dataFile: DataFile = {
      content: 1,
      'file-path': 's3://bucket/data/dv.puffin',
      'file-format': 'parquet',
      partition: {},
      'record-count': 50,
      'file-size-in-bytes': 2048,
      'content-offset': 100,
      'content-size-in-bytes': 1024,
      'referenced-data-file': 's3://bucket/data/00001.parquet',
    };

    const json = JSON.stringify(dataFile);
    const parsed = JSON.parse(json);

    expect(parsed['content-offset']).toBe(100);
  });

  it('should serialize content-size-in-bytes in DataFile JSON', () => {
    const dataFile: DataFile = {
      content: 1,
      'file-path': 's3://bucket/data/dv.puffin',
      'file-format': 'parquet',
      partition: {},
      'record-count': 50,
      'file-size-in-bytes': 2048,
      'content-offset': 100,
      'content-size-in-bytes': 1024,
      'referenced-data-file': 's3://bucket/data/00001.parquet',
    };

    const json = JSON.stringify(dataFile);
    const parsed = JSON.parse(json);

    expect(parsed['content-size-in-bytes']).toBe(1024);
  });

  it('should serialize referenced-data-file in DataFile JSON', () => {
    const dataFile: DataFile = {
      content: 1,
      'file-path': 's3://bucket/data/dv.puffin',
      'file-format': 'parquet',
      partition: {},
      'record-count': 50,
      'file-size-in-bytes': 2048,
      'content-offset': 100,
      'content-size-in-bytes': 1024,
      'referenced-data-file': 's3://bucket/data/00001.parquet',
    };

    const json = JSON.stringify(dataFile);
    const parsed = JSON.parse(json);

    expect(parsed['referenced-data-file']).toBe('s3://bucket/data/00001.parquet');
  });

  it('should round-trip serialize DataFile with all deletion vector fields', () => {
    const original: DataFile = {
      content: 1,
      'file-path': 's3://bucket/data/dv.puffin',
      'file-format': 'parquet',
      partition: { date: '2024-01-15' },
      'record-count': 50,
      'file-size-in-bytes': 2048,
      'content-offset': 100,
      'content-size-in-bytes': 1024,
      'referenced-data-file': 's3://bucket/data/00001.parquet',
    };

    const json = JSON.stringify(original);
    const parsed: DataFile = JSON.parse(json);

    expect(parsed.content).toBe(original.content);
    expect(parsed['file-path']).toBe(original['file-path']);
    expect(parsed['file-format']).toBe(original['file-format']);
    expect(parsed.partition).toEqual(original.partition);
    expect(parsed['record-count']).toBe(original['record-count']);
    expect(parsed['file-size-in-bytes']).toBe(original['file-size-in-bytes']);
    expect(parsed['content-offset']).toBe(original['content-offset']);
    expect(parsed['content-size-in-bytes']).toBe(original['content-size-in-bytes']);
    expect(parsed['referenced-data-file']).toBe(original['referenced-data-file']);
  });

  it('should not include undefined deletion vector fields in serialized JSON', () => {
    const dataFile: DataFile = {
      content: 0,
      'file-path': 's3://bucket/data/file.parquet',
      'file-format': 'parquet',
      partition: {},
      'record-count': 100,
      'file-size-in-bytes': 4096,
    };

    const json = JSON.stringify(dataFile);

    expect(json).not.toContain('content-offset');
    expect(json).not.toContain('content-size-in-bytes');
    expect(json).not.toContain('referenced-data-file');
  });
});

// ============================================================================
// ManifestEntry Tests
// ============================================================================

describe('ManifestEntry with Deletion Vector Metadata', () => {
  it('should track deletion vector metadata in manifest entry', () => {
    const dvDataFile: DataFile = {
      content: 1,
      'file-path': 's3://bucket/data/dv-0001.puffin',
      'file-format': 'parquet',
      partition: {},
      'record-count': 50,
      'file-size-in-bytes': 2048,
      'content-offset': 100,
      'content-size-in-bytes': 1024,
      'referenced-data-file': 's3://bucket/data/00001.parquet',
    };

    const manifestEntry = {
      status: 1 as const, // ADDED
      'snapshot-id': 1234567890,
      'sequence-number': 5,
      'file-sequence-number': 5,
      'data-file': dvDataFile,
    };

    expect(manifestEntry['data-file']['content-offset']).toBe(100);
    expect(manifestEntry['data-file']['content-size-in-bytes']).toBe(1024);
    expect(manifestEntry['data-file']['referenced-data-file']).toBe(
      's3://bucket/data/00001.parquet'
    );
    expect(isDeletionVector(manifestEntry['data-file'])).toBe(true);
  });

  it('should serialize and deserialize manifest entry with DV fields', () => {
    const dvDataFile: DataFile = {
      content: 1,
      'file-path': 's3://bucket/data/dv-0001.puffin',
      'file-format': 'parquet',
      partition: {},
      'record-count': 50,
      'file-size-in-bytes': 2048,
      'content-offset': 256,
      'content-size-in-bytes': 512,
      'referenced-data-file': 's3://bucket/data/original.parquet',
    };

    const manifestEntry = {
      status: 1 as const,
      'snapshot-id': 1234567890,
      'sequence-number': 5,
      'file-sequence-number': 5,
      'data-file': dvDataFile,
    };

    const json = JSON.stringify(manifestEntry);
    const parsed = JSON.parse(json);

    expect(parsed['data-file']['content-offset']).toBe(256);
    expect(parsed['data-file']['content-size-in-bytes']).toBe(512);
    expect(parsed['data-file']['referenced-data-file']).toBe('s3://bucket/data/original.parquet');
  });
});

// ============================================================================
// Delete Manifest Generation Tests (content=1 for deletes)
// ============================================================================

describe('Delete Manifest Generation with Deletion Vectors', () => {
  it('should create delete manifest with content=1', () => {
    const generator = new DeleteManifestGenerator({
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    const dvEntry: Omit<DataFile, 'content'> = {
      'file-path': 's3://bucket/data/dv-0001.puffin',
      'file-format': 'parquet',
      partition: {},
      'record-count': 50,
      'file-size-in-bytes': 2048,
      'content-offset': 100,
      'content-size-in-bytes': 1024,
      'referenced-data-file': 's3://bucket/data/00001.parquet',
    };

    generator.addPositionDeleteFile(dvEntry);
    const result = generator.generate();

    expect(result.entries).toHaveLength(1);
    expect(result.entries[0]['data-file'].content).toBe(CONTENT_POSITION_DELETES);
  });

  it('should include deletion vector fields in manifest entries', () => {
    const generator = new DeleteManifestGenerator({
      sequenceNumber: 5,
      snapshotId: 1234567890,
    });

    const dvEntry: Omit<DataFile, 'content'> = {
      'file-path': 's3://bucket/data/dv-0001.puffin',
      'file-format': 'parquet',
      partition: {},
      'record-count': 50,
      'file-size-in-bytes': 2048,
      'content-offset': 100,
      'content-size-in-bytes': 1024,
      'referenced-data-file': 's3://bucket/data/00001.parquet',
    };

    generator.addPositionDeleteFile(dvEntry);
    const result = generator.generate();

    const dataFile = result.entries[0]['data-file'];
    expect(dataFile['content-offset']).toBe(100);
    expect(dataFile['content-size-in-bytes']).toBe(1024);
    expect(dataFile['referenced-data-file']).toBe('s3://bucket/data/00001.parquet');
  });

  it('should create deletion vector entry using helper function', () => {
    const dvEntry = createDeletionVectorEntry({
      filePath: 's3://bucket/data/dv-0001.puffin',
      fileFormat: 'parquet',
      partition: {},
      recordCount: 50,
      fileSizeInBytes: 2048,
      contentOffset: 100,
      contentSizeInBytes: 1024,
      referencedDataFile: 's3://bucket/data/00001.parquet',
    });

    expect(dvEntry.content).toBe(CONTENT_POSITION_DELETES);
    expect(dvEntry['file-path']).toBe('s3://bucket/data/dv-0001.puffin');
    expect(dvEntry['content-offset']).toBe(100);
    expect(dvEntry['content-size-in-bytes']).toBe(1024);
    expect(dvEntry['referenced-data-file']).toBe('s3://bucket/data/00001.parquet');
    expect(isDeletionVector(dvEntry)).toBe(true);
  });

  it('should track deletion vector entries have all required fields', () => {
    const dvEntry = createDeletionVectorEntry({
      filePath: 's3://bucket/data/dv-0001.puffin',
      fileFormat: 'parquet',
      partition: { date: '2024-01-15' },
      recordCount: 50,
      fileSizeInBytes: 2048,
      contentOffset: 100,
      contentSizeInBytes: 1024,
      referencedDataFile: 's3://bucket/data/00001.parquet',
    });

    // Verify all required fields are present
    expect(dvEntry.content).toBeDefined();
    expect(dvEntry['file-path']).toBeDefined();
    expect(dvEntry['file-format']).toBeDefined();
    expect(dvEntry.partition).toBeDefined();
    expect(dvEntry['record-count']).toBeDefined();
    expect(dvEntry['file-size-in-bytes']).toBeDefined();
    // DV-specific required fields
    expect(dvEntry['content-offset']).toBeDefined();
    expect(dvEntry['content-size-in-bytes']).toBeDefined();
    expect(dvEntry['referenced-data-file']).toBeDefined();
  });
});

// ============================================================================
// Scan Planning Tests - Finding DVs for Data Files
// ============================================================================

describe('Scan Planning - Finding Deletion Vectors for Data Files', () => {
  it('should find DVs that apply to a data file by referenced_data_file path', () => {
    const manifestEntries: ManifestEntry[] = [
      {
        status: 1,
        'snapshot-id': 1234567890,
        'sequence-number': 5,
        'file-sequence-number': 5,
        'data-file': {
          content: 1,
          'file-path': 's3://bucket/data/dv-0001.puffin',
          'file-format': 'parquet',
          partition: {},
          'record-count': 50,
          'file-size-in-bytes': 2048,
          'content-offset': 100,
          'content-size-in-bytes': 1024,
          'referenced-data-file': 's3://bucket/data/00001.parquet',
        },
      },
      {
        status: 1,
        'snapshot-id': 1234567890,
        'sequence-number': 5,
        'file-sequence-number': 5,
        'data-file': {
          content: 1,
          'file-path': 's3://bucket/data/dv-0002.puffin',
          'file-format': 'parquet',
          partition: {},
          'record-count': 30,
          'file-size-in-bytes': 1024,
          'content-offset': 200,
          'content-size-in-bytes': 512,
          'referenced-data-file': 's3://bucket/data/00002.parquet',
        },
      },
    ];

    const dvsForFile1 = findDeletionVectorsForFile(manifestEntries, 's3://bucket/data/00001.parquet');
    expect(dvsForFile1).toHaveLength(1);
    expect(dvsForFile1[0]['file-path']).toBe('s3://bucket/data/dv-0001.puffin');

    const dvsForFile2 = findDeletionVectorsForFile(manifestEntries, 's3://bucket/data/00002.parquet');
    expect(dvsForFile2).toHaveLength(1);
    expect(dvsForFile2[0]['file-path']).toBe('s3://bucket/data/dv-0002.puffin');

    const dvsForUnknownFile = findDeletionVectorsForFile(manifestEntries, 's3://bucket/data/00003.parquet');
    expect(dvsForUnknownFile).toHaveLength(0);
  });

  it('should match DVs by referenced_data_file path (exact match)', () => {
    const manifestEntries: ManifestEntry[] = [
      {
        status: 1,
        'snapshot-id': 1234567890,
        'sequence-number': 5,
        'file-sequence-number': 5,
        'data-file': {
          content: 1,
          'file-path': 's3://bucket/data/dv-0001.puffin',
          'file-format': 'parquet',
          partition: {},
          'record-count': 50,
          'file-size-in-bytes': 2048,
          'content-offset': 100,
          'content-size-in-bytes': 1024,
          'referenced-data-file': 's3://bucket/data/00001.parquet',
        },
      },
    ];

    // Exact match should work
    const dvs = findDeletionVectorsForFile(manifestEntries, 's3://bucket/data/00001.parquet');
    expect(dvs).toHaveLength(1);

    // Similar but not exact path should not match
    const dvsNoMatch = findDeletionVectorsForFile(manifestEntries, 's3://bucket/data/00001.parquet.bak');
    expect(dvsNoMatch).toHaveLength(0);

    // Partial path should not match
    const dvsPartial = findDeletionVectorsForFile(manifestEntries, '00001.parquet');
    expect(dvsPartial).toHaveLength(0);
  });

  it('should ignore position delete files when DV exists for same data file', () => {
    const manifestEntries: ManifestEntry[] = [
      // Legacy position delete file
      {
        status: 1,
        'snapshot-id': 1234567890,
        'sequence-number': 4,
        'file-sequence-number': 4,
        'data-file': {
          content: 1,
          'file-path': 's3://bucket/data/pos-delete-0001.parquet',
          'file-format': 'parquet',
          partition: {},
          'record-count': 10,
          'file-size-in-bytes': 512,
          // No DV fields - legacy format
        },
      },
      // Deletion vector for the same data file
      {
        status: 1,
        'snapshot-id': 1234567890,
        'sequence-number': 5,
        'file-sequence-number': 5,
        'data-file': {
          content: 1,
          'file-path': 's3://bucket/data/dv-0001.puffin',
          'file-format': 'parquet',
          partition: {},
          'record-count': 50,
          'file-size-in-bytes': 2048,
          'content-offset': 100,
          'content-size-in-bytes': 1024,
          'referenced-data-file': 's3://bucket/data/00001.parquet',
        },
      },
    ];

    // When a DV exists for the data file, position deletes should be ignored
    const shouldIgnore = shouldIgnorePositionDeletes(manifestEntries, 's3://bucket/data/00001.parquet');
    expect(shouldIgnore).toBe(true);

    // When no DV exists, position deletes should not be ignored
    const shouldNotIgnore = shouldIgnorePositionDeletes(manifestEntries, 's3://bucket/data/other.parquet');
    expect(shouldNotIgnore).toBe(false);
  });

  it('should not find DVs in data manifests (content=0)', () => {
    // Regular data file (not a DV, even if it has some similar fields)
    const manifestEntries: ManifestEntry[] = [
      {
        status: 1,
        'snapshot-id': 1234567890,
        'sequence-number': 5,
        'file-sequence-number': 5,
        'data-file': {
          content: 0, // data file, not delete
          'file-path': 's3://bucket/data/00001.parquet',
          'file-format': 'parquet',
          partition: {},
          'record-count': 1000,
          'file-size-in-bytes': 10240,
        },
      },
    ];

    const dvs = findDeletionVectorsForFile(manifestEntries, 's3://bucket/data/00001.parquet');
    expect(dvs).toHaveLength(0);
  });
});

// ============================================================================
// Manifest List Tests - Delete Manifests with DVs
// ============================================================================

describe('Manifest List - Delete Manifests with Deletion Vectors', () => {
  it('should track delete manifests with content=1 in manifest list', () => {
    const generator = new ManifestListGenerator({
      snapshotId: 1234567890,
      sequenceNumber: 5,
    });

    generator.addManifestWithStats(
      's3://bucket/metadata/dv-manifest-0001.avro',
      4096,
      0, // partition spec id
      {
        addedFiles: 3,
        existingFiles: 0,
        deletedFiles: 0,
        addedRows: 150,
        existingRows: 0,
        deletedRows: 0,
      },
      true // isDeleteManifest
    );

    const manifests = generator.generate();
    expect(manifests).toHaveLength(1);
    expect(manifests[0].content).toBe(MANIFEST_CONTENT_DELETES);
    expect(manifests[0]['added-files-count']).toBe(3);
    expect(manifests[0]['added-rows-count']).toBe(150);
  });

  it('should track separate data and delete manifests in manifest list', () => {
    const generator = new ManifestListGenerator({
      snapshotId: 1234567890,
      sequenceNumber: 5,
    });

    // Add data manifest
    generator.addManifestWithStats(
      's3://bucket/metadata/data-manifest-0001.avro',
      8192,
      0,
      {
        addedFiles: 10,
        existingFiles: 0,
        deletedFiles: 0,
        addedRows: 10000,
        existingRows: 0,
        deletedRows: 0,
      },
      false // data manifest
    );

    // Add delete manifest with DVs
    generator.addManifestWithStats(
      's3://bucket/metadata/dv-manifest-0001.avro',
      4096,
      0,
      {
        addedFiles: 3,
        existingFiles: 0,
        deletedFiles: 0,
        addedRows: 150,
        existingRows: 0,
        deletedRows: 0,
      },
      true // delete manifest
    );

    const manifests = generator.generate();
    expect(manifests).toHaveLength(2);

    const dataManifest = manifests.find(m => m.content === 0);
    const deleteManifest = manifests.find(m => m.content === MANIFEST_CONTENT_DELETES);

    expect(dataManifest).toBeDefined();
    expect(deleteManifest).toBeDefined();
    expect(dataManifest!['added-files-count']).toBe(10);
    expect(deleteManifest!['added-files-count']).toBe(3);
  });

  it('should include content counts for DV entries in manifest list', () => {
    const generator = new ManifestListGenerator({
      snapshotId: 1234567890,
      sequenceNumber: 5,
    });

    // Delete manifest with 5 DV entries
    generator.addManifestWithStats(
      's3://bucket/metadata/dv-manifest-0001.avro',
      4096,
      0,
      {
        addedFiles: 5,
        existingFiles: 2,
        deletedFiles: 1,
        addedRows: 250,
        existingRows: 100,
        deletedRows: 50,
      },
      true
    );

    const manifests = generator.generate();
    const deleteManifest = manifests[0];

    expect(deleteManifest['added-files-count']).toBe(5);
    expect(deleteManifest['existing-files-count']).toBe(2);
    expect(deleteManifest['deleted-files-count']).toBe(1);
    expect(deleteManifest['added-rows-count']).toBe(250);
    expect(deleteManifest['existing-rows-count']).toBe(100);
    expect(deleteManifest['deleted-rows-count']).toBe(50);
  });
});

// ============================================================================
// V3 Writer Rules Tests
// ============================================================================

describe('V3 Writer Rules for Deletion Vectors', () => {
  it('should validate v3 writers cannot add new position delete files', () => {
    // v3 table with existing DVs
    const existingEntries: ManifestEntry[] = [
      {
        status: 1,
        'snapshot-id': 1234567890,
        'sequence-number': 5,
        'file-sequence-number': 5,
        'data-file': {
          content: 1,
          'file-path': 's3://bucket/data/dv-0001.puffin',
          'file-format': 'parquet',
          partition: {},
          'record-count': 50,
          'file-size-in-bytes': 2048,
          'content-offset': 100,
          'content-size-in-bytes': 1024,
          'referenced-data-file': 's3://bucket/data/00001.parquet',
        },
      },
    ];

    // New position delete file (legacy format, no DV fields)
    const newPositionDelete: DataFile = {
      content: 1,
      'file-path': 's3://bucket/data/pos-delete-new.parquet',
      'file-format': 'parquet',
      partition: {},
      'record-count': 10,
      'file-size-in-bytes': 512,
      // No DV fields - legacy position delete
    };

    const result = validateV3DeletionVectorRules(
      3, // format version
      existingEntries,
      [newPositionDelete]
    );

    expect(result.valid).toBe(false);
    expect(result.errors).toContain('v3 writers cannot add new position delete files without deletion vector fields');
  });

  it('should allow v3 writers to add new deletion vectors', () => {
    const existingEntries: ManifestEntry[] = [];

    const newDV: DataFile = {
      content: 1,
      'file-path': 's3://bucket/data/dv-0001.puffin',
      'file-format': 'parquet',
      partition: {},
      'record-count': 50,
      'file-size-in-bytes': 2048,
      'content-offset': 100,
      'content-size-in-bytes': 1024,
      'referenced-data-file': 's3://bucket/data/00001.parquet',
    };

    const result = validateV3DeletionVectorRules(3, existingEntries, [newDV]);

    expect(result.valid).toBe(true);
    expect(result.errors).toHaveLength(0);
  });

  it('should allow v2 writers to add position delete files', () => {
    const existingEntries: ManifestEntry[] = [];

    const newPositionDelete: DataFile = {
      content: 1,
      'file-path': 's3://bucket/data/pos-delete-new.parquet',
      'file-format': 'parquet',
      partition: {},
      'record-count': 10,
      'file-size-in-bytes': 512,
    };

    const result = validateV3DeletionVectorRules(2, existingEntries, [newPositionDelete]);

    expect(result.valid).toBe(true);
    expect(result.errors).toHaveLength(0);
  });

  it('should detect existing position delete files that must be merged into DVs in v3', () => {
    // Existing position delete files (legacy format)
    const existingEntries: ManifestEntry[] = [
      {
        status: 0, // EXISTING
        'snapshot-id': 1234567880,
        'sequence-number': 4,
        'file-sequence-number': 4,
        'data-file': {
          content: 1,
          'file-path': 's3://bucket/data/pos-delete-old.parquet',
          'file-format': 'parquet',
          partition: {},
          'record-count': 10,
          'file-size-in-bytes': 512,
          // No DV fields - legacy position delete
        },
      },
    ];

    const result = validateV3DeletionVectorRules(3, existingEntries, []);

    // Should warn about existing position delete files
    expect(result.warnings).toBeDefined();
    expect(result.warnings).toContain(
      'existing position delete files should be merged into deletion vectors'
    );
  });

  it('should enforce at most one DV per data file per snapshot', () => {
    const manifestEntries: ManifestEntry[] = [
      {
        status: 1,
        'snapshot-id': 1234567890,
        'sequence-number': 5,
        'file-sequence-number': 5,
        'data-file': {
          content: 1,
          'file-path': 's3://bucket/data/dv-0001.puffin',
          'file-format': 'parquet',
          partition: {},
          'record-count': 50,
          'file-size-in-bytes': 2048,
          'content-offset': 100,
          'content-size-in-bytes': 1024,
          'referenced-data-file': 's3://bucket/data/00001.parquet',
        },
      },
      {
        status: 1,
        'snapshot-id': 1234567890,
        'sequence-number': 5,
        'file-sequence-number': 5,
        'data-file': {
          content: 1,
          'file-path': 's3://bucket/data/dv-0002.puffin',
          'file-format': 'parquet',
          partition: {},
          'record-count': 30,
          'file-size-in-bytes': 1024,
          'content-offset': 200,
          'content-size-in-bytes': 512,
          'referenced-data-file': 's3://bucket/data/00001.parquet', // Same data file!
        },
      },
    ];

    const dvCounts = countDeletionVectorsPerDataFile(manifestEntries, 1234567890);

    // Should detect multiple DVs for the same data file
    expect(dvCounts.get('s3://bucket/data/00001.parquet')).toBe(2);

    // Validation should fail
    const result = validateV3DeletionVectorRules(3, manifestEntries, []);
    expect(result.valid).toBe(false);
    expect(result.errors).toContain(
      'at most one deletion vector per data file per snapshot is allowed'
    );
  });

  it('should allow multiple DVs for same data file in different snapshots', () => {
    const manifestEntries: ManifestEntry[] = [
      {
        status: 0, // EXISTING from previous snapshot
        'snapshot-id': 1234567880,
        'sequence-number': 4,
        'file-sequence-number': 4,
        'data-file': {
          content: 1,
          'file-path': 's3://bucket/data/dv-0001.puffin',
          'file-format': 'parquet',
          partition: {},
          'record-count': 50,
          'file-size-in-bytes': 2048,
          'content-offset': 100,
          'content-size-in-bytes': 1024,
          'referenced-data-file': 's3://bucket/data/00001.parquet',
        },
      },
      {
        status: 1, // ADDED in current snapshot
        'snapshot-id': 1234567890,
        'sequence-number': 5,
        'file-sequence-number': 5,
        'data-file': {
          content: 1,
          'file-path': 's3://bucket/data/dv-0002.puffin',
          'file-format': 'parquet',
          partition: {},
          'record-count': 30,
          'file-size-in-bytes': 1024,
          'content-offset': 200,
          'content-size-in-bytes': 512,
          'referenced-data-file': 's3://bucket/data/00001.parquet',
        },
      },
    ];

    // Count DVs for the new snapshot only (ADDED entries)
    const dvCountsNewSnapshot = countDeletionVectorsPerDataFile(manifestEntries, 1234567890);
    expect(dvCountsNewSnapshot.get('s3://bucket/data/00001.parquet')).toBe(1);

    // This should be valid since only one DV per data file per snapshot
    const result = validateV3DeletionVectorRules(3, manifestEntries, []);
    expect(result.valid).toBe(true);
  });
});
