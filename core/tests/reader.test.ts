import { describe, it, expect, beforeEach } from 'vitest';
import {
  readTableMetadata,
  readMetadataFromPath,
  parseTableMetadata,
  getCurrentVersion,
  getSnapshotAtTimestamp,
  getSnapshotByRef,
  getSnapshotById,
  getCurrentSnapshot,
  listMetadataFiles,
  TableMetadataBuilder,
  SnapshotBuilder,
  METADATA_DIR,
  VERSION_HINT_FILENAME,
  type StorageBackend,
  type TableMetadata,
  type Snapshot,
} from '../src/index.js';

// ============================================================================
// Mock Storage Backend
// ============================================================================

/**
 * Create an in-memory storage backend for testing.
 */
function createMockStorage(): StorageBackend & {
  data: Map<string, Uint8Array>;
  clear: () => void;
} {
  const data = new Map<string, Uint8Array>();

  return {
    data,

    async get(key: string): Promise<Uint8Array | null> {
      return data.get(key) ?? null;
    },

    async put(key: string, value: Uint8Array): Promise<void> {
      data.set(key, value);
    },

    async delete(key: string): Promise<void> {
      data.delete(key);
    },

    async list(prefix: string): Promise<string[]> {
      const results: string[] = [];
      for (const key of data.keys()) {
        if (key.startsWith(prefix)) {
          results.push(key);
        }
      }
      return results.sort();
    },

    async exists(key: string): Promise<boolean> {
      return data.has(key);
    },

    clear(): void {
      data.clear();
    },
  };
}

/**
 * Create test table metadata.
 */
function createTestMetadata(location: string, options?: {
  snapshots?: Snapshot[];
  currentSnapshotId?: number | null;
  refs?: Record<string, { 'snapshot-id': number; type: 'branch' | 'tag' }>;
}): TableMetadata {
  const builder = new TableMetadataBuilder({ location });

  if (options?.snapshots) {
    for (const snapshot of options.snapshots) {
      builder.addSnapshot(snapshot);
    }
  }

  const metadata = builder.build();

  // If we need custom refs or currentSnapshotId, override them
  if (options?.refs || options?.currentSnapshotId !== undefined) {
    return {
      ...metadata,
      'current-snapshot-id': options?.currentSnapshotId ?? metadata['current-snapshot-id'],
      refs: options?.refs ?? metadata.refs,
    };
  }

  return metadata;
}

/**
 * Create a test snapshot.
 */
function createTestSnapshot(
  sequenceNumber: number,
  timestampMs: number,
  snapshotId?: number,
  parentSnapshotId?: number
): Snapshot {
  const builder = new SnapshotBuilder({
    sequenceNumber,
    parentSnapshotId,
    snapshotId,
    manifestListPath: `s3://bucket/metadata/snap-${snapshotId ?? Date.now()}.avro`,
    operation: 'append',
    timestampMs,
  });
  builder.setSummary(1, 0, 100, 0, 1024, 0, 100, 1024, 1);
  return builder.build();
}

/**
 * Store metadata in mock storage.
 */
async function storeMetadata(
  storage: ReturnType<typeof createMockStorage>,
  location: string,
  metadata: TableMetadata,
  version: number
): Promise<void> {
  const metadataPath = `${location}/${METADATA_DIR}/v${version}.metadata.json`;
  const versionHintPath = `${location}/${METADATA_DIR}/${VERSION_HINT_FILENAME}`;

  await storage.put(metadataPath, new TextEncoder().encode(JSON.stringify(metadata)));
  await storage.put(versionHintPath, new TextEncoder().encode(String(version)));
}

// ============================================================================
// readTableMetadata Tests
// ============================================================================

describe('readTableMetadata', () => {
  let storage: ReturnType<typeof createMockStorage>;
  const tableLocation = 's3://bucket/warehouse/db/table';

  beforeEach(() => {
    storage = createMockStorage();
  });

  describe('with version hint', () => {
    it('should read metadata using version number from version hint', async () => {
      const metadata = createTestMetadata(tableLocation);
      await storeMetadata(storage, tableLocation, metadata, 1);

      const result = await readTableMetadata(storage, tableLocation);

      expect(result).not.toBeNull();
      expect(result!.location).toBe(tableLocation);
      expect(result!['format-version']).toBe(2);
    });

    it('should read metadata using full path from version hint', async () => {
      const metadata = createTestMetadata(tableLocation);
      const metadataPath = `${tableLocation}/${METADATA_DIR}/1-abc123.metadata.json`;
      const versionHintPath = `${tableLocation}/${METADATA_DIR}/${VERSION_HINT_FILENAME}`;

      await storage.put(metadataPath, new TextEncoder().encode(JSON.stringify(metadata)));
      await storage.put(versionHintPath, new TextEncoder().encode(metadataPath));

      const result = await readTableMetadata(storage, tableLocation);

      expect(result).not.toBeNull();
      expect(result!.location).toBe(tableLocation);
    });

    it('should return null when metadata file does not exist', async () => {
      const versionHintPath = `${tableLocation}/${METADATA_DIR}/${VERSION_HINT_FILENAME}`;
      await storage.put(versionHintPath, new TextEncoder().encode('999'));

      const result = await readTableMetadata(storage, tableLocation);

      expect(result).toBeNull();
    });
  });

  describe('without version hint', () => {
    it('should find latest metadata file by scanning directory', async () => {
      const metadata = createTestMetadata(tableLocation);
      const metadataDir = `${tableLocation}/${METADATA_DIR}`;

      // Store multiple metadata files (without version hint)
      await storage.put(
        `${metadataDir}/v1.metadata.json`,
        new TextEncoder().encode(JSON.stringify({ ...metadata, 'table-uuid': 'old-uuid' }))
      );
      await storage.put(
        `${metadataDir}/v2.metadata.json`,
        new TextEncoder().encode(JSON.stringify(metadata))
      );

      const result = await readTableMetadata(storage, tableLocation);

      expect(result).not.toBeNull();
      // Should get the latest (v2) based on reverse sort
      expect(result!['table-uuid']).toBe(metadata['table-uuid']);
    });

    it('should return null when no metadata files exist', async () => {
      const result = await readTableMetadata(storage, tableLocation);

      expect(result).toBeNull();
    });

    it('should ignore non-metadata files when scanning', async () => {
      const metadata = createTestMetadata(tableLocation);
      const metadataDir = `${tableLocation}/${METADATA_DIR}`;

      await storage.put(
        `${metadataDir}/v1.metadata.json`,
        new TextEncoder().encode(JSON.stringify(metadata))
      );
      await storage.put(
        `${metadataDir}/snap-123.avro`,
        new TextEncoder().encode('binary data')
      );
      await storage.put(
        `${metadataDir}/manifest-list.avro`,
        new TextEncoder().encode('binary data')
      );

      const result = await readTableMetadata(storage, tableLocation);

      expect(result).not.toBeNull();
      expect(result!['format-version']).toBe(2);
    });
  });

  describe('edge cases', () => {
    it('should throw for empty location', async () => {
      await expect(readTableMetadata(storage, '')).rejects.toThrow(
        'Location must be a non-empty string'
      );
    });

    it('should throw for whitespace-only location', async () => {
      await expect(readTableMetadata(storage, '   ')).rejects.toThrow(
        'Location must be a non-empty string'
      );
    });

    it('should throw for empty version hint file', async () => {
      const versionHintPath = `${tableLocation}/${METADATA_DIR}/${VERSION_HINT_FILENAME}`;
      await storage.put(versionHintPath, new TextEncoder().encode(''));

      await expect(readTableMetadata(storage, tableLocation)).rejects.toThrow(
        'Version hint file is empty'
      );
    });

    it('should throw for invalid version hint (NaN)', async () => {
      const versionHintPath = `${tableLocation}/${METADATA_DIR}/${VERSION_HINT_FILENAME}`;
      await storage.put(versionHintPath, new TextEncoder().encode('not-a-number'));

      await expect(readTableMetadata(storage, tableLocation)).rejects.toThrow(
        'Invalid version hint: "not-a-number" is not a valid version number'
      );
    });

    it('should throw for path traversal attempt in version hint', async () => {
      const versionHintPath = `${tableLocation}/${METADATA_DIR}/${VERSION_HINT_FILENAME}`;
      await storage.put(
        versionHintPath,
        new TextEncoder().encode('../../../etc/passwd')
      );

      await expect(readTableMetadata(storage, tableLocation)).rejects.toThrow(
        /path traversal not allowed/
      );
    });

    it('should handle version hint with whitespace', async () => {
      const metadata = createTestMetadata(tableLocation);
      const metadataPath = `${tableLocation}/${METADATA_DIR}/v5.metadata.json`;
      const versionHintPath = `${tableLocation}/${METADATA_DIR}/${VERSION_HINT_FILENAME}`;

      await storage.put(metadataPath, new TextEncoder().encode(JSON.stringify(metadata)));
      await storage.put(versionHintPath, new TextEncoder().encode('  5  \n'));

      const result = await readTableMetadata(storage, tableLocation);

      expect(result).not.toBeNull();
    });
  });
});

// ============================================================================
// readMetadataFromPath Tests
// ============================================================================

describe('readMetadataFromPath', () => {
  let storage: ReturnType<typeof createMockStorage>;
  const tableLocation = 's3://bucket/warehouse/db/table';

  beforeEach(() => {
    storage = createMockStorage();
  });

  it('should read metadata from a specific path', async () => {
    const metadata = createTestMetadata(tableLocation);
    const metadataPath = `${tableLocation}/${METADATA_DIR}/v1.metadata.json`;

    await storage.put(metadataPath, new TextEncoder().encode(JSON.stringify(metadata)));

    const result = await readMetadataFromPath(storage, metadataPath);

    expect(result.location).toBe(tableLocation);
    expect(result['format-version']).toBe(2);
  });

  it('should throw when metadata file does not exist', async () => {
    const metadataPath = `${tableLocation}/${METADATA_DIR}/v999.metadata.json`;

    await expect(readMetadataFromPath(storage, metadataPath)).rejects.toThrow(
      `Metadata file not found: ${metadataPath}`
    );
  });

  it('should throw for empty path', async () => {
    await expect(readMetadataFromPath(storage, '')).rejects.toThrow(
      'Metadata path must be a non-empty string'
    );
  });

  it('should throw for whitespace-only path', async () => {
    await expect(readMetadataFromPath(storage, '   ')).rejects.toThrow(
      'Metadata path must be a non-empty string'
    );
  });
});

// ============================================================================
// parseTableMetadata Tests
// ============================================================================

describe('parseTableMetadata', () => {
  const tableLocation = 's3://bucket/warehouse/db/table';

  it('should parse valid JSON metadata', () => {
    const metadata = createTestMetadata(tableLocation);
    const json = JSON.stringify(metadata);

    const result = parseTableMetadata(json);

    expect(result['format-version']).toBe(2);
    expect(result.location).toBe(tableLocation);
    expect(result['table-uuid']).toBeTruthy();
  });

  describe('malformed JSON', () => {
    it('should throw for invalid JSON syntax', () => {
      expect(() => parseTableMetadata('{ invalid json }')).toThrow(
        /Failed to parse table metadata JSON/
      );
    });

    it('should throw for empty string', () => {
      expect(() => parseTableMetadata('')).toThrow(
        /Failed to parse table metadata JSON/
      );
    });

    it('should throw for JSON array', () => {
      expect(() => parseTableMetadata('[]')).toThrow(
        'Table metadata must be a JSON object'
      );
    });

    it('should throw for JSON primitive', () => {
      expect(() => parseTableMetadata('"string"')).toThrow(
        'Table metadata must be a JSON object'
      );
    });

    it('should throw for null', () => {
      expect(() => parseTableMetadata('null')).toThrow(
        'Table metadata must be a JSON object'
      );
    });
  });

  describe('missing required fields', () => {
    it('should throw for missing format-version', () => {
      const json = JSON.stringify({
        'table-uuid': 'test-uuid',
        location: tableLocation,
      });

      expect(() => parseTableMetadata(json)).toThrow(
        /Unsupported format version/
      );
    });

    it('should throw for missing table-uuid', () => {
      const json = JSON.stringify({
        'format-version': 2,
        location: tableLocation,
      });

      expect(() => parseTableMetadata(json)).toThrow(
        'Missing required field: table-uuid'
      );
    });

    it('should throw for missing location', () => {
      const json = JSON.stringify({
        'format-version': 2,
        'table-uuid': 'test-uuid',
      });

      expect(() => parseTableMetadata(json)).toThrow(
        'Missing required field: location'
      );
    });
  });

  describe('version mismatch', () => {
    it('should throw for format version 1', () => {
      const json = JSON.stringify({
        'format-version': 1,
        'table-uuid': 'test-uuid',
        location: tableLocation,
      });

      expect(() => parseTableMetadata(json)).toThrow(
        'Unsupported format version: 1'
      );
    });

    it('should accept format version 3', () => {
      const json = JSON.stringify({
        'format-version': 3,
        'table-uuid': 'test-uuid',
        location: tableLocation,
      });

      // Format version 3 is now supported
      const metadata = parseTableMetadata(json);
      expect(metadata['format-version']).toBe(3);
    });

    it('should throw for format version 4', () => {
      const json = JSON.stringify({
        'format-version': 4,
        'table-uuid': 'test-uuid',
        location: tableLocation,
      });

      expect(() => parseTableMetadata(json)).toThrow(
        'Unsupported format version: 4'
      );
    });

    it('should throw for undefined format version', () => {
      const json = JSON.stringify({
        'table-uuid': 'test-uuid',
        location: tableLocation,
      });

      expect(() => parseTableMetadata(json)).toThrow(
        'Unsupported format version: undefined'
      );
    });
  });
});

// ============================================================================
// getCurrentVersion Tests
// ============================================================================

describe('getCurrentVersion', () => {
  let storage: ReturnType<typeof createMockStorage>;
  const tableLocation = 's3://bucket/warehouse/db/table';

  beforeEach(() => {
    storage = createMockStorage();
  });

  it('should return version number from version hint', async () => {
    const versionHintPath = `${tableLocation}/${METADATA_DIR}/${VERSION_HINT_FILENAME}`;
    await storage.put(versionHintPath, new TextEncoder().encode('42'));

    const version = await getCurrentVersion(storage, tableLocation);

    expect(version).toBe(42);
  });

  it('should return null when version hint does not exist', async () => {
    const version = await getCurrentVersion(storage, tableLocation);

    expect(version).toBeNull();
  });

  it('should return null for invalid version hint (NaN)', async () => {
    const versionHintPath = `${tableLocation}/${METADATA_DIR}/${VERSION_HINT_FILENAME}`;
    await storage.put(versionHintPath, new TextEncoder().encode('invalid'));

    const version = await getCurrentVersion(storage, tableLocation);

    expect(version).toBeNull();
  });

  it('should handle version hint with whitespace', async () => {
    const versionHintPath = `${tableLocation}/${METADATA_DIR}/${VERSION_HINT_FILENAME}`;
    await storage.put(versionHintPath, new TextEncoder().encode('  123  \n'));

    const version = await getCurrentVersion(storage, tableLocation);

    expect(version).toBe(123);
  });

  it('should throw for empty location', async () => {
    await expect(getCurrentVersion(storage, '')).rejects.toThrow(
      'Location must be a non-empty string'
    );
  });

  it('should throw for whitespace-only location', async () => {
    await expect(getCurrentVersion(storage, '   ')).rejects.toThrow(
      'Location must be a non-empty string'
    );
  });
});

// ============================================================================
// getSnapshotAtTimestamp Tests
// ============================================================================

describe('getSnapshotAtTimestamp', () => {
  const tableLocation = 's3://bucket/warehouse/db/table';
  const now = Date.now();

  // Create snapshots at different times
  const snapshot1 = createTestSnapshot(1, now - 3000, 1001); // 3 seconds ago
  const snapshot2 = createTestSnapshot(2, now - 2000, 1002, 1001); // 2 seconds ago
  const snapshot3 = createTestSnapshot(3, now - 1000, 1003, 1002); // 1 second ago

  const metadata = createTestMetadata(tableLocation, {
    snapshots: [snapshot1, snapshot2, snapshot3],
    currentSnapshotId: 1003,
  });

  it('should return snapshot at exact timestamp', () => {
    const result = getSnapshotAtTimestamp(metadata, now - 2000);

    expect(result).not.toBeUndefined();
    expect(result!['snapshot-id']).toBe(1002);
  });

  it('should return most recent snapshot before timestamp', () => {
    const result = getSnapshotAtTimestamp(metadata, now - 1500); // Between snapshot2 and snapshot3

    expect(result).not.toBeUndefined();
    expect(result!['snapshot-id']).toBe(1002);
  });

  it('should return latest snapshot when timestamp is in future', () => {
    const result = getSnapshotAtTimestamp(metadata, now + 10000);

    expect(result).not.toBeUndefined();
    expect(result!['snapshot-id']).toBe(1003);
  });

  it('should return undefined when timestamp is before all snapshots', () => {
    const result = getSnapshotAtTimestamp(metadata, now - 10000);

    expect(result).toBeUndefined();
  });

  it('should return undefined for empty snapshots array', () => {
    const emptyMetadata = createTestMetadata(tableLocation);

    const result = getSnapshotAtTimestamp(emptyMetadata, now);

    expect(result).toBeUndefined();
  });

  it('should throw for NaN timestamp', () => {
    expect(() => getSnapshotAtTimestamp(metadata, NaN)).toThrow(
      'Timestamp must be a valid number'
    );
  });

  it('should throw for non-number timestamp', () => {
    expect(() => getSnapshotAtTimestamp(metadata, 'invalid' as unknown as number)).toThrow(
      'Timestamp must be a valid number'
    );
  });
});

// ============================================================================
// getSnapshotByRef Tests
// ============================================================================

describe('getSnapshotByRef', () => {
  const tableLocation = 's3://bucket/warehouse/db/table';
  const now = Date.now();

  const snapshot1 = createTestSnapshot(1, now - 2000, 1001);
  const snapshot2 = createTestSnapshot(2, now - 1000, 1002, 1001);

  const metadata = createTestMetadata(tableLocation, {
    snapshots: [snapshot1, snapshot2],
    currentSnapshotId: 1002,
    refs: {
      main: { 'snapshot-id': 1002, type: 'branch' },
      'release-v1': { 'snapshot-id': 1001, type: 'tag' },
    },
  });

  it('should return snapshot for branch reference', () => {
    const result = getSnapshotByRef(metadata, 'main');

    expect(result).not.toBeUndefined();
    expect(result!['snapshot-id']).toBe(1002);
  });

  it('should return snapshot for tag reference', () => {
    const result = getSnapshotByRef(metadata, 'release-v1');

    expect(result).not.toBeUndefined();
    expect(result!['snapshot-id']).toBe(1001);
  });

  it('should return undefined for non-existent reference', () => {
    const result = getSnapshotByRef(metadata, 'non-existent');

    expect(result).toBeUndefined();
  });

  it('should return undefined when ref points to non-existent snapshot', () => {
    const brokenMetadata = createTestMetadata(tableLocation, {
      snapshots: [snapshot1],
      currentSnapshotId: 1001,
      refs: {
        broken: { 'snapshot-id': 9999, type: 'branch' },
      },
    });

    const result = getSnapshotByRef(brokenMetadata, 'broken');

    expect(result).toBeUndefined();
  });

  it('should throw for empty reference name', () => {
    expect(() => getSnapshotByRef(metadata, '')).toThrow(
      'Reference name must be a non-empty string'
    );
  });

  it('should throw for whitespace-only reference name', () => {
    expect(() => getSnapshotByRef(metadata, '   ')).toThrow(
      'Reference name must be a non-empty string'
    );
  });
});

// ============================================================================
// getSnapshotById Tests
// ============================================================================

describe('getSnapshotById', () => {
  const tableLocation = 's3://bucket/warehouse/db/table';
  const now = Date.now();

  const snapshot1 = createTestSnapshot(1, now - 2000, 1001);
  const snapshot2 = createTestSnapshot(2, now - 1000, 1002, 1001);

  const metadata = createTestMetadata(tableLocation, {
    snapshots: [snapshot1, snapshot2],
    currentSnapshotId: 1002,
  });

  it('should return snapshot by ID', () => {
    const result = getSnapshotById(metadata, 1001);

    expect(result).not.toBeUndefined();
    expect(result!['snapshot-id']).toBe(1001);
    expect(result!['sequence-number']).toBe(1);
  });

  it('should return undefined for non-existent snapshot ID', () => {
    const result = getSnapshotById(metadata, 9999);

    expect(result).toBeUndefined();
  });

  it('should return undefined for empty snapshots array', () => {
    const emptyMetadata = createTestMetadata(tableLocation);

    const result = getSnapshotById(emptyMetadata, 1001);

    expect(result).toBeUndefined();
  });

  it('should throw for NaN snapshot ID', () => {
    expect(() => getSnapshotById(metadata, NaN)).toThrow(
      'Snapshot ID must be a valid number'
    );
  });

  it('should throw for non-number snapshot ID', () => {
    expect(() => getSnapshotById(metadata, 'invalid' as unknown as number)).toThrow(
      'Snapshot ID must be a valid number'
    );
  });
});

// ============================================================================
// getCurrentSnapshot Tests
// ============================================================================

describe('getCurrentSnapshot', () => {
  const tableLocation = 's3://bucket/warehouse/db/table';
  const now = Date.now();

  it('should return current snapshot', () => {
    const snapshot1 = createTestSnapshot(1, now - 2000, 1001);
    const snapshot2 = createTestSnapshot(2, now - 1000, 1002, 1001);

    const metadata = createTestMetadata(tableLocation, {
      snapshots: [snapshot1, snapshot2],
      currentSnapshotId: 1002,
    });

    const result = getCurrentSnapshot(metadata);

    expect(result).not.toBeUndefined();
    expect(result!['snapshot-id']).toBe(1002);
  });

  it('should return undefined when current-snapshot-id is null', () => {
    const metadata = createTestMetadata(tableLocation);

    const result = getCurrentSnapshot(metadata);

    expect(result).toBeUndefined();
  });

  it('should return undefined when current snapshot ID does not match any snapshot', () => {
    const snapshot1 = createTestSnapshot(1, now - 2000, 1001);

    const metadata = createTestMetadata(tableLocation, {
      snapshots: [snapshot1],
      currentSnapshotId: 9999, // Non-existent
    });

    const result = getCurrentSnapshot(metadata);

    expect(result).toBeUndefined();
  });
});

// ============================================================================
// listMetadataFiles Tests
// ============================================================================

describe('listMetadataFiles', () => {
  let storage: ReturnType<typeof createMockStorage>;
  const tableLocation = 's3://bucket/warehouse/db/table';

  beforeEach(() => {
    storage = createMockStorage();
  });

  it('should list all metadata files', async () => {
    const metadataDir = `${tableLocation}/${METADATA_DIR}`;

    await storage.put(`${metadataDir}/v1.metadata.json`, new TextEncoder().encode('{}'));
    await storage.put(`${metadataDir}/v2.metadata.json`, new TextEncoder().encode('{}'));
    await storage.put(`${metadataDir}/v3.metadata.json`, new TextEncoder().encode('{}'));

    const result = await listMetadataFiles(storage, tableLocation);

    expect(result).toHaveLength(3);
    expect(result).toContain(`${metadataDir}/v1.metadata.json`);
    expect(result).toContain(`${metadataDir}/v2.metadata.json`);
    expect(result).toContain(`${metadataDir}/v3.metadata.json`);
  });

  it('should return files sorted alphabetically', async () => {
    const metadataDir = `${tableLocation}/${METADATA_DIR}`;

    await storage.put(`${metadataDir}/v10.metadata.json`, new TextEncoder().encode('{}'));
    await storage.put(`${metadataDir}/v2.metadata.json`, new TextEncoder().encode('{}'));
    await storage.put(`${metadataDir}/v1.metadata.json`, new TextEncoder().encode('{}'));

    const result = await listMetadataFiles(storage, tableLocation);

    expect(result).toEqual([
      `${metadataDir}/v1.metadata.json`,
      `${metadataDir}/v10.metadata.json`,
      `${metadataDir}/v2.metadata.json`,
    ]);
  });

  it('should only include .metadata.json files', async () => {
    const metadataDir = `${tableLocation}/${METADATA_DIR}`;

    await storage.put(`${metadataDir}/v1.metadata.json`, new TextEncoder().encode('{}'));
    await storage.put(`${metadataDir}/snap-123.avro`, new TextEncoder().encode('binary'));
    await storage.put(`${metadataDir}/manifest-list.avro`, new TextEncoder().encode('binary'));
    await storage.put(`${metadataDir}/version-hint.text`, new TextEncoder().encode('1'));

    const result = await listMetadataFiles(storage, tableLocation);

    expect(result).toHaveLength(1);
    expect(result[0]).toBe(`${metadataDir}/v1.metadata.json`);
  });

  it('should return empty array when no metadata files exist', async () => {
    const result = await listMetadataFiles(storage, tableLocation);

    expect(result).toEqual([]);
  });

  it('should return empty array when metadata directory has only non-metadata files', async () => {
    const metadataDir = `${tableLocation}/${METADATA_DIR}`;

    await storage.put(`${metadataDir}/snap-123.avro`, new TextEncoder().encode('binary'));
    await storage.put(`${metadataDir}/version-hint.text`, new TextEncoder().encode('1'));

    const result = await listMetadataFiles(storage, tableLocation);

    expect(result).toEqual([]);
  });

  it('should throw for empty location', async () => {
    await expect(listMetadataFiles(storage, '')).rejects.toThrow(
      'Location must be a non-empty string'
    );
  });

  it('should throw for whitespace-only location', async () => {
    await expect(listMetadataFiles(storage, '   ')).rejects.toThrow(
      'Location must be a non-empty string'
    );
  });

  it('should handle versioned metadata file names with UUIDs', async () => {
    const metadataDir = `${tableLocation}/${METADATA_DIR}`;

    await storage.put(
      `${metadataDir}/1-abc123-def456.metadata.json`,
      new TextEncoder().encode('{}')
    );
    await storage.put(
      `${metadataDir}/2-xyz789.metadata.json`,
      new TextEncoder().encode('{}')
    );

    const result = await listMetadataFiles(storage, tableLocation);

    expect(result).toHaveLength(2);
  });
});
