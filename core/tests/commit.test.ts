import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  AtomicCommitter,
  createAtomicCommitter,
  commitWithCleanup,
  CommitConflictError,
  CommitRetryExhaustedError,
  CommitTransactionError,
  generateVersionedMetadataPath,
  parseMetadataVersion,
  getVersionHintPath,
  getMetadataVersion,
  COMMIT_MAX_RETRIES,
  TableMetadataBuilder,
  SnapshotBuilder,
  type StorageBackend,
  type TableMetadata,
  type PendingCommit,
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

  /**
   * Helper to compare Uint8Array values.
   */
  function areEqual(a: Uint8Array, b: Uint8Array): boolean {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
      if (a[i] !== b[i]) return false;
    }
    return true;
  }

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

    async putIfAbsent(key: string, value: Uint8Array): Promise<boolean> {
      if (data.has(key)) {
        return false;
      }
      data.set(key, value);
      return true;
    },

    async compareAndSwap(key: string, expected: Uint8Array | null, value: Uint8Array): Promise<boolean> {
      const current = data.get(key) ?? null;
      if (expected === null) {
        // Key must not exist
        if (current !== null) {
          return false;
        }
      } else {
        // Key must exist and match expected value
        if (current === null || !areEqual(current, expected)) {
          return false;
        }
      }
      data.set(key, value);
      return true;
    },

    clear(): void {
      data.clear();
    },
  };
}

/**
 * Create initial table metadata for testing.
 */
function createTestMetadata(location: string): TableMetadata {
  const builder = new TableMetadataBuilder({ location });
  return builder.build();
}

/**
 * Create a test snapshot.
 */
function createTestSnapshot(
  sequenceNumber: number,
  parentSnapshotId?: number
): ReturnType<SnapshotBuilder['build']> {
  const builder = new SnapshotBuilder({
    sequenceNumber,
    parentSnapshotId,
    manifestListPath: `s3://bucket/metadata/snap-${Date.now()}.avro`,
    operation: 'append',
  });
  builder.setSummary(1, 0, 100, 0, 1024, 0, 100, 1024, 1);
  return builder.build();
}

// ============================================================================
// Utility Function Tests
// ============================================================================

describe('Utility Functions', () => {
  describe('generateVersionedMetadataPath', () => {
    it('should generate valid metadata paths', () => {
      const path = generateVersionedMetadataPath('s3://bucket/table', 1);
      expect(path).toMatch(/^s3:\/\/bucket\/table\/metadata\/1-[a-f0-9-]+\.metadata\.json$/);
    });

    it('should generate unique paths', () => {
      const paths = new Set(
        Array.from({ length: 10 }, () =>
          generateVersionedMetadataPath('s3://bucket/table', 1)
        )
      );
      expect(paths.size).toBe(10);
    });
  });

  describe('parseMetadataVersion', () => {
    it('should parse versioned metadata paths with UUID', () => {
      const path = 's3://bucket/table/metadata/5-abc123.metadata.json';
      expect(parseMetadataVersion(path)).toBe(5);
    });

    it('should parse simple version paths', () => {
      const path = 's3://bucket/table/metadata/v10.metadata.json';
      expect(parseMetadataVersion(path)).toBe(10);
    });

    it('should return null for invalid paths', () => {
      expect(parseMetadataVersion('invalid')).toBeNull();
      expect(parseMetadataVersion('s3://bucket/data/file.parquet')).toBeNull();
    });
  });

  describe('getVersionHintPath', () => {
    it('should return correct path', () => {
      expect(getVersionHintPath('s3://bucket/table')).toBe(
        's3://bucket/table/metadata/version-hint.text'
      );
    });
  });

  describe('getMetadataVersion', () => {
    it('should return sequence number', () => {
      const metadata = createTestMetadata('s3://bucket/table');
      expect(getMetadataVersion(metadata)).toBe(0);
    });

    it('should return updated sequence number after snapshot', () => {
      const builder = new TableMetadataBuilder({ location: 's3://bucket/table' });
      builder.addSnapshot(createTestSnapshot(1));
      const metadata = builder.build();
      expect(getMetadataVersion(metadata)).toBe(1);
    });
  });
});

// ============================================================================
// Error Type Tests
// ============================================================================

describe('Error Types', () => {
  describe('CommitConflictError', () => {
    it('should contain version information', () => {
      const error = new CommitConflictError('conflict', 1, 2);
      expect(error.name).toBe('CommitConflictError');
      expect(error.expectedVersion).toBe(1);
      expect(error.actualVersion).toBe(2);
      expect(error.message).toBe('conflict');
    });
  });

  describe('CommitRetryExhaustedError', () => {
    it('should contain attempt count and last error', () => {
      const lastError = new Error('last');
      const error = new CommitRetryExhaustedError('exhausted', 5, lastError);
      expect(error.name).toBe('CommitRetryExhaustedError');
      expect(error.attempts).toBe(5);
      expect(error.lastError).toBe(lastError);
    });
  });

  describe('CommitTransactionError', () => {
    it('should contain cleanup information', () => {
      const error = new CommitTransactionError('failed', ['file1', 'file2'], true);
      expect(error.name).toBe('CommitTransactionError');
      expect(error.writtenFiles).toEqual(['file1', 'file2']);
      expect(error.cleanupSuccessful).toBe(true);
    });
  });
});

// ============================================================================
// AtomicCommitter Tests
// ============================================================================

describe('AtomicCommitter', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let committer: AtomicCommitter;
  const tableLocation = 's3://bucket/warehouse/db/table';

  beforeEach(() => {
    storage = createMockStorage();
    committer = new AtomicCommitter(storage, tableLocation);
  });

  describe('constructor', () => {
    it('should store table location', () => {
      expect(committer.getTableLocation()).toBe(tableLocation);
    });
  });

  describe('getCurrentVersion', () => {
    it('should return null when no version hint exists', async () => {
      const version = await committer.getCurrentVersion();
      expect(version).toBeNull();
    });

    it('should return version number from hint', async () => {
      const hintPath = getVersionHintPath(tableLocation);
      await storage.put(hintPath, new TextEncoder().encode('3'));

      const version = await committer.getCurrentVersion();
      expect(version).toBe(3);
    });

    it('should parse version from full path hint', async () => {
      const hintPath = getVersionHintPath(tableLocation);
      const metadataPath = `${tableLocation}/metadata/5-abc123.metadata.json`;
      await storage.put(hintPath, new TextEncoder().encode(metadataPath));

      const version = await committer.getCurrentVersion();
      expect(version).toBe(5);
    });
  });

  describe('loadMetadata', () => {
    it('should return null when no metadata exists', async () => {
      const metadata = await committer.loadMetadata();
      expect(metadata).toBeNull();
    });

    it('should load metadata from version hint', async () => {
      // Setup initial metadata
      const metadata = createTestMetadata(tableLocation);
      const metadataPath = `${tableLocation}/metadata/v1.metadata.json`;
      await storage.put(metadataPath, new TextEncoder().encode(JSON.stringify(metadata)));
      await storage.put(getVersionHintPath(tableLocation), new TextEncoder().encode('1'));

      const loaded = await committer.loadMetadata();
      expect(loaded).not.toBeNull();
      expect(loaded!.location).toBe(tableLocation);
    });
  });

  describe('commit', () => {
    beforeEach(async () => {
      // Setup initial table - version hint must match metadata's last-sequence-number (0)
      const metadata = createTestMetadata(tableLocation);
      const metadataPath = `${tableLocation}/metadata/v0.metadata.json`;
      await storage.put(metadataPath, new TextEncoder().encode(JSON.stringify(metadata)));
      await storage.put(getVersionHintPath(tableLocation), new TextEncoder().encode('0'));
    });

    it('should commit a new snapshot successfully', async () => {
      const result = await committer.commit(async (metadata) => {
        if (!metadata) throw new Error('No metadata');
        const snapshot = createTestSnapshot(metadata['last-sequence-number'] + 1);
        return {
          baseMetadata: metadata,
          snapshot,
        };
      });

      expect(result.metadataVersion).toBe(1);
      expect(result.attempts).toBe(1);
      expect(result.conflictResolved).toBe(false);
      expect(result.metadataPath).toContain('/metadata/');
    });

    it('should update version hint after commit', async () => {
      await committer.commit(async (metadata) => {
        if (!metadata) throw new Error('No metadata');
        const snapshot = createTestSnapshot(metadata['last-sequence-number'] + 1);
        return { baseMetadata: metadata, snapshot };
      });

      const version = await committer.getCurrentVersion();
      expect(version).toBe(1);
    });

    it('should write new metadata file', async () => {
      const result = await committer.commit(async (metadata) => {
        if (!metadata) throw new Error('No metadata');
        const snapshot = createTestSnapshot(metadata['last-sequence-number'] + 1);
        return { baseMetadata: metadata, snapshot };
      });

      const exists = await storage.exists(result.metadataPath);
      expect(exists).toBe(true);
    });

    it('should throw when table does not exist and required', async () => {
      storage.clear();

      await expect(
        committer.commit(async (metadata) => {
          if (!metadata) throw new Error('Table does not exist');
          return { baseMetadata: metadata, snapshot: createTestSnapshot(1) };
        })
      ).rejects.toThrow('Table does not exist');
    });
  });

  describe('commit with conflicts', () => {
    let callCount: number;

    beforeEach(async () => {
      callCount = 0;
      // Setup initial table - version hint must match metadata's last-sequence-number (0)
      const metadata = createTestMetadata(tableLocation);
      const metadataPath = `${tableLocation}/metadata/v0.metadata.json`;
      await storage.put(metadataPath, new TextEncoder().encode(JSON.stringify(metadata)));
      await storage.put(getVersionHintPath(tableLocation), new TextEncoder().encode('0'));
    });

    it('should retry on conflict and succeed', async () => {
      // Simulate a conflict on first attempt by modifying version hint
      // during the commit verification phase
      const originalGet = storage.get.bind(storage);
      let conflictTriggered = false;

      storage.get = async (key: string) => {
        callCount++;
        // On third call (2nd version hint read during attemptCommit), simulate conflict once
        if (key.includes('version-hint') && callCount === 3 && !conflictTriggered) {
          conflictTriggered = true;
          // Create new metadata at version 1 to simulate concurrent write
          const builder = new TableMetadataBuilder({ location: tableLocation });
          builder.addSnapshot(createTestSnapshot(1));
          const newMeta = builder.build();
          const newMetadataPath = `${tableLocation}/metadata/v1.metadata.json`;
          await storage.put(
            newMetadataPath,
            new TextEncoder().encode(JSON.stringify(newMeta))
          );
          // Update version hint to point to new metadata (use version number format)
          await storage.put(
            getVersionHintPath(tableLocation),
            new TextEncoder().encode('1')
          );
        }
        return originalGet(key);
      };

      const result = await committer.commit(async (metadata) => {
        if (!metadata) throw new Error('No metadata');
        const snapshot = createTestSnapshot(metadata['last-sequence-number'] + 1);
        return { baseMetadata: metadata, snapshot };
      });

      expect(result.attempts).toBeGreaterThan(1);
    });

    it('should exhaust retries and throw', async () => {
      // Always simulate conflict by advancing version on every commit attempt
      const originalGet = storage.get.bind(storage);
      let attemptVersions: number[] = [];

      storage.get = async (key: string) => {
        const result = await originalGet(key);
        // After loading metadata successfully, advance the version before commit verification
        if (key.includes('version-hint') && result) {
          const currentHint = new TextDecoder().decode(result).trim();
          const currentVersion = currentHint.includes('/')
            ? parseInt(currentHint.match(/\/v?(\d+)/)?.[1] || '0', 10)
            : parseInt(currentHint, 10);

          // Only advance if we haven't already advanced for this attempt
          if (!attemptVersions.includes(currentVersion + 1)) {
            attemptVersions.push(currentVersion + 1);
            const newVersion = currentVersion + 1;
            // Create metadata for the new version
            const builder = new TableMetadataBuilder({ location: tableLocation });
            for (let i = 1; i <= newVersion; i++) {
              builder.addSnapshot(createTestSnapshot(i));
            }
            const newMeta = builder.build();
            const newMetadataPath = `${tableLocation}/metadata/v${newVersion}.metadata.json`;
            await storage.put(
              newMetadataPath,
              new TextEncoder().encode(JSON.stringify(newMeta))
            );
            await storage.put(
              getVersionHintPath(tableLocation),
              new TextEncoder().encode(String(newVersion))
            );
          }
        }
        return result;
      };

      await expect(
        committer.commit(
          async (metadata) => {
            if (!metadata) throw new Error('No metadata');
            const snapshot = createTestSnapshot(metadata['last-sequence-number'] + 1);
            return { baseMetadata: metadata, snapshot };
          },
          { maxRetries: 2 }
        )
      ).rejects.toThrow(CommitRetryExhaustedError);
    });
  });

  describe('commitSnapshot', () => {
    beforeEach(async () => {
      // Setup initial table - version hint must match metadata's last-sequence-number (0)
      const metadata = createTestMetadata(tableLocation);
      const metadataPath = `${tableLocation}/metadata/v0.metadata.json`;
      await storage.put(metadataPath, new TextEncoder().encode(JSON.stringify(metadata)));
      await storage.put(getVersionHintPath(tableLocation), new TextEncoder().encode('0'));
    });

    it('should commit using simplified interface', async () => {
      const result = await committer.commitSnapshot(
        's3://bucket/metadata/manifest-list.avro',
        {
          operation: 'append',
          'added-data-files': '5',
          'added-records': '500',
          'total-records': '500',
          'total-data-files': '5',
        }
      );

      expect(result.metadataVersion).toBe(1);
      expect(result.snapshot.summary.operation).toBe('append');
    });

    it('should throw when table does not exist', async () => {
      storage.clear();

      await expect(
        committer.commitSnapshot(
          's3://bucket/metadata/manifest-list.avro',
          { operation: 'append' }
        )
      ).rejects.toThrow('Table does not exist');
    });
  });

  describe('cleanupOldMetadata', () => {
    beforeEach(async () => {
      // Setup table with multiple versions in metadata-log
      const builder = new TableMetadataBuilder({ location: tableLocation });

      // Add some snapshots to increase version
      builder.addSnapshot(createTestSnapshot(1));
      builder.addSnapshot(createTestSnapshot(2));

      // Add old metadata log entries
      const now = Date.now();
      const oldTime = now - 30 * 24 * 60 * 60 * 1000; // 30 days ago
      builder.addMetadataLogEntry(`${tableLocation}/metadata/v1.metadata.json`, oldTime);
      builder.addMetadataLogEntry(`${tableLocation}/metadata/v2.metadata.json`, oldTime);

      const metadata = builder.build();
      const currentPath = `${tableLocation}/metadata/v3.metadata.json`;
      await storage.put(currentPath, new TextEncoder().encode(JSON.stringify(metadata)));
      await storage.put(getVersionHintPath(tableLocation), new TextEncoder().encode('3'));

      // Create old metadata files
      await storage.put(
        `${tableLocation}/metadata/v1.metadata.json`,
        new TextEncoder().encode('{}')
      );
      await storage.put(
        `${tableLocation}/metadata/v2.metadata.json`,
        new TextEncoder().encode('{}')
      );
    });

    it('should delete old metadata files', async () => {
      const deleted = await committer.cleanupOldMetadata({
        retainVersions: 1,
        maxAgeMs: 7 * 24 * 60 * 60 * 1000,
      });

      expect(deleted.length).toBeGreaterThan(0);
    });

    it('should respect enabled option', async () => {
      const deleted = await committer.cleanupOldMetadata({ enabled: false });
      expect(deleted).toEqual([]);
    });

    it('should return empty when no metadata exists', async () => {
      storage.clear();
      const deleted = await committer.cleanupOldMetadata();
      expect(deleted).toEqual([]);
    });
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('Integration', () => {
  let storage: ReturnType<typeof createMockStorage>;
  const tableLocation = 's3://bucket/warehouse/db/table';

  beforeEach(() => {
    storage = createMockStorage();
  });

  describe('createAtomicCommitter', () => {
    it('should create committer instance', () => {
      const committer = createAtomicCommitter(storage, tableLocation);
      expect(committer).toBeInstanceOf(AtomicCommitter);
      expect(committer.getTableLocation()).toBe(tableLocation);
    });
  });

  describe('commitWithCleanup', () => {
    beforeEach(async () => {
      // Setup initial table - version hint must match metadata's last-sequence-number (0)
      const metadata = createTestMetadata(tableLocation);
      const metadataPath = `${tableLocation}/metadata/v0.metadata.json`;
      await storage.put(metadataPath, new TextEncoder().encode(JSON.stringify(metadata)));
      await storage.put(getVersionHintPath(tableLocation), new TextEncoder().encode('0'));
    });

    it('should commit and trigger cleanup', async () => {
      const committer = new AtomicCommitter(storage, tableLocation);

      const result = await commitWithCleanup(
        committer,
        async (metadata) => {
          if (!metadata) throw new Error('No metadata');
          const snapshot = createTestSnapshot(metadata['last-sequence-number'] + 1);
          return { baseMetadata: metadata, snapshot };
        },
        {},
        { enabled: true }
      );

      expect(result.metadataVersion).toBe(1);
    });

    it('should handle cleanup failure gracefully', async () => {
      const committer = new AtomicCommitter(storage, tableLocation);
      const cleanupFailureSpy = vi.fn();

      // Make delete throw
      storage.delete = async () => {
        throw new Error('Delete failed');
      };

      const result = await commitWithCleanup(
        committer,
        async (metadata) => {
          if (!metadata) throw new Error('No metadata');
          const snapshot = createTestSnapshot(metadata['last-sequence-number'] + 1);
          return { baseMetadata: metadata, snapshot };
        },
        {},
        {
          enabled: true,
          onCleanupFailure: cleanupFailureSpy,
        }
      );

      // Commit should still succeed
      expect(result.metadataVersion).toBe(1);
    });
  });

  describe('Multiple sequential commits', () => {
    it('should handle multiple commits correctly', async () => {
      // Setup initial table - version hint must match metadata's last-sequence-number (0)
      const metadata = createTestMetadata(tableLocation);
      const metadataPath = `${tableLocation}/metadata/v0.metadata.json`;
      await storage.put(metadataPath, new TextEncoder().encode(JSON.stringify(metadata)));
      await storage.put(getVersionHintPath(tableLocation), new TextEncoder().encode('0'));

      const committer = new AtomicCommitter(storage, tableLocation);

      // First commit
      const result1 = await committer.commit(async (metadata) => {
        if (!metadata) throw new Error('No metadata');
        const snapshot = createTestSnapshot(metadata['last-sequence-number'] + 1);
        return { baseMetadata: metadata, snapshot };
      });

      // Second commit
      const result2 = await committer.commit(async (metadata) => {
        if (!metadata) throw new Error('No metadata');
        const snapshot = createTestSnapshot(
          metadata['last-sequence-number'] + 1,
          metadata['current-snapshot-id'] ?? undefined
        );
        return { baseMetadata: metadata, snapshot };
      });

      // Third commit
      const result3 = await committer.commit(async (metadata) => {
        if (!metadata) throw new Error('No metadata');
        const snapshot = createTestSnapshot(
          metadata['last-sequence-number'] + 1,
          metadata['current-snapshot-id'] ?? undefined
        );
        return { baseMetadata: metadata, snapshot };
      });

      expect(result1.metadataVersion).toBe(1);
      expect(result2.metadataVersion).toBe(2);
      expect(result3.metadataVersion).toBe(3);

      // Verify final state
      const finalMetadata = await committer.loadMetadata();
      expect(finalMetadata).not.toBeNull();
      expect(finalMetadata!.snapshots.length).toBe(3);
      expect(finalMetadata!['last-sequence-number']).toBe(3);
    });
  });

  describe('Concurrent commit simulation', () => {
    it('should handle sequential commits from different committers', async () => {
      // Setup initial table - version hint must match metadata's last-sequence-number (0)
      const metadata = createTestMetadata(tableLocation);
      const metadataPath = `${tableLocation}/metadata/v0.metadata.json`;
      await storage.put(metadataPath, new TextEncoder().encode(JSON.stringify(metadata)));
      await storage.put(getVersionHintPath(tableLocation), new TextEncoder().encode('0'));

      const committer1 = new AtomicCommitter(storage, tableLocation);
      const committer2 = new AtomicCommitter(storage, tableLocation);

      // First committer commits
      const result1 = await committer1.commit(async (currentMetadata) => {
        if (!currentMetadata) throw new Error('No metadata');
        const snapshot = createTestSnapshot(currentMetadata['last-sequence-number'] + 1);
        return { baseMetadata: currentMetadata, snapshot };
      });

      expect(result1.metadataVersion).toBe(1);

      // Second committer commits after first (no conflict expected)
      const result2 = await committer2.commit(async (currentMetadata) => {
        if (!currentMetadata) throw new Error('No metadata');
        const version = currentMetadata['last-sequence-number'] + 1;
        const snapshot = createTestSnapshot(version);
        return { baseMetadata: currentMetadata, snapshot };
      });

      expect(result2.metadataVersion).toBe(2);
      expect(result2.attempts).toBe(1);

      // Verify final state
      const finalMetadata = await committer1.loadMetadata();
      expect(finalMetadata).not.toBeNull();
      expect(finalMetadata!.snapshots.length).toBe(2);
    });
  });
});

// ============================================================================
// Edge Case Tests
// ============================================================================

describe('Edge Cases', () => {
  let storage: ReturnType<typeof createMockStorage>;
  const tableLocation = 's3://bucket/warehouse/db/table';

  beforeEach(() => {
    storage = createMockStorage();
  });

  describe('Empty table', () => {
    it('should handle first commit to new table', async () => {
      // Just write version hint pointing to non-existent metadata
      // This simulates a corrupted or empty state
      await storage.put(
        getVersionHintPath(tableLocation),
        new TextEncoder().encode('0')
      );

      const committer = new AtomicCommitter(storage, tableLocation);
      const metadata = await committer.loadMetadata();

      // Should be null since metadata file doesn't exist
      expect(metadata).toBeNull();
    });
  });

  describe('Malformed version hint', () => {
    it('should handle non-numeric version hint', async () => {
      await storage.put(
        getVersionHintPath(tableLocation),
        new TextEncoder().encode('invalid')
      );

      const committer = new AtomicCommitter(storage, tableLocation);
      const version = await committer.getCurrentVersion();

      // parseInt of 'invalid' returns NaN
      expect(Number.isNaN(version)).toBe(true);
    });
  });

  describe('Commit options', () => {
    beforeEach(async () => {
      // Setup initial table - version hint must match metadata's last-sequence-number (0)
      const metadata = createTestMetadata(tableLocation);
      const metadataPath = `${tableLocation}/metadata/v0.metadata.json`;
      await storage.put(metadataPath, new TextEncoder().encode(JSON.stringify(metadata)));
      await storage.put(getVersionHintPath(tableLocation), new TextEncoder().encode('0'));
    });

    it('should respect maxRetries option', async () => {
      const committer = new AtomicCommitter(storage, tableLocation);

      // Keep track of what version to simulate
      let externalVersion = 0;

      // Force conflicts by simulating external writes before each commit attempt
      let attemptCount = 0;
      await expect(
        committer.commit(
          async (currentMetadata) => {
            attemptCount++;
            if (!currentMetadata) throw new Error('No metadata');

            // Before each commit attempt, simulate another writer committing
            // This ensures version mismatch on every attemptCommit verification
            externalVersion++;
            const builder = new TableMetadataBuilder({ location: tableLocation });
            for (let i = 1; i <= externalVersion; i++) {
              builder.addSnapshot(createTestSnapshot(i));
            }
            const newMeta = builder.build();
            await storage.put(
              `${tableLocation}/metadata/v${externalVersion}.metadata.json`,
              new TextEncoder().encode(JSON.stringify(newMeta))
            );
            await storage.put(
              getVersionHintPath(tableLocation),
              new TextEncoder().encode(String(externalVersion))
            );

            // Return commit based on currentMetadata (now stale)
            return {
              baseMetadata: currentMetadata,
              snapshot: createTestSnapshot(currentMetadata['last-sequence-number'] + 1),
            };
          },
          { maxRetries: 1 }
        )
      ).rejects.toThrow(CommitRetryExhaustedError);

      // Should have tried initial + 1 retry = 2 total
      expect(attemptCount).toBe(2);
    });

    it('should respect cleanupOnFailure option', async () => {
      const committer = new AtomicCommitter(storage, tableLocation);

      // Remove atomic operations to test the put-based fallback
      delete (storage as Partial<typeof storage>).putIfAbsent;
      delete (storage as Partial<typeof storage>).compareAndSwap;

      // Make metadata write succeed but version hint write fail
      const originalPut = storage.put.bind(storage);
      let putCalls = 0;
      storage.put = async (key: string, value: Uint8Array) => {
        putCalls++;
        if (key.includes('.metadata.json')) {
          // Allow metadata file writes
          await originalPut(key, value);
        } else if (key.includes('version-hint')) {
          // Fail on version hint writes
          throw new Error('Write failed');
        } else {
          await originalPut(key, value);
        }
      };

      await expect(
        committer.commit(
          async (metadata) => {
            if (!metadata) throw new Error('No metadata');
            return { baseMetadata: metadata, snapshot: createTestSnapshot(1) };
          },
          { cleanupOnFailure: true }
        )
      ).rejects.toThrow('Write failed');
    });
  });
});

// ============================================================================
// Atomic Operations Tests
// ============================================================================

describe('Atomic Storage Operations', () => {
  let storage: ReturnType<typeof createMockStorage>;

  beforeEach(() => {
    storage = createMockStorage();
  });

  describe('putIfAbsent', () => {
    it('should return true when key does not exist', async () => {
      const result = await storage.putIfAbsent!(
        'test-key',
        new TextEncoder().encode('test-value')
      );

      expect(result).toBe(true);
      const stored = await storage.get('test-key');
      expect(new TextDecoder().decode(stored!)).toBe('test-value');
    });

    it('should return false when key already exists', async () => {
      await storage.put('test-key', new TextEncoder().encode('existing'));

      const result = await storage.putIfAbsent!(
        'test-key',
        new TextEncoder().encode('new-value')
      );

      expect(result).toBe(false);
      const stored = await storage.get('test-key');
      expect(new TextDecoder().decode(stored!)).toBe('existing');
    });

    it('should be atomic across concurrent calls', async () => {
      // Simulate concurrent putIfAbsent calls
      const results = await Promise.all([
        storage.putIfAbsent!('concurrent-key', new TextEncoder().encode('value1')),
        storage.putIfAbsent!('concurrent-key', new TextEncoder().encode('value2')),
        storage.putIfAbsent!('concurrent-key', new TextEncoder().encode('value3')),
      ]);

      // Exactly one should succeed
      const successCount = results.filter(r => r).length;
      expect(successCount).toBe(1);
    });
  });

  describe('compareAndSwap', () => {
    it('should return true and update when expected matches current value', async () => {
      const originalValue = new TextEncoder().encode('original');
      await storage.put('cas-key', originalValue);

      const newValue = new TextEncoder().encode('updated');
      const result = await storage.compareAndSwap!('cas-key', originalValue, newValue);

      expect(result).toBe(true);
      const stored = await storage.get('cas-key');
      expect(new TextDecoder().decode(stored!)).toBe('updated');
    });

    it('should return false when expected does not match current value', async () => {
      await storage.put('cas-key', new TextEncoder().encode('original'));

      const wrongExpected = new TextEncoder().encode('wrong');
      const newValue = new TextEncoder().encode('updated');
      const result = await storage.compareAndSwap!('cas-key', wrongExpected, newValue);

      expect(result).toBe(false);
      const stored = await storage.get('cas-key');
      expect(new TextDecoder().decode(stored!)).toBe('original');
    });

    it('should return true when expected is null and key does not exist', async () => {
      const newValue = new TextEncoder().encode('new');
      const result = await storage.compareAndSwap!('new-key', null, newValue);

      expect(result).toBe(true);
      const stored = await storage.get('new-key');
      expect(new TextDecoder().decode(stored!)).toBe('new');
    });

    it('should return false when expected is null but key exists', async () => {
      await storage.put('existing-key', new TextEncoder().encode('existing'));

      const newValue = new TextEncoder().encode('new');
      const result = await storage.compareAndSwap!('existing-key', null, newValue);

      expect(result).toBe(false);
      const stored = await storage.get('existing-key');
      expect(new TextDecoder().decode(stored!)).toBe('existing');
    });

    it('should return false when expected is non-null but key does not exist', async () => {
      const expected = new TextEncoder().encode('expected');
      const newValue = new TextEncoder().encode('new');
      const result = await storage.compareAndSwap!('nonexistent', expected, newValue);

      expect(result).toBe(false);
      const stored = await storage.get('nonexistent');
      expect(stored).toBeNull();
    });

    it('should handle byte-level comparison correctly', async () => {
      const value1 = new Uint8Array([1, 2, 3, 4]);
      const value2 = new Uint8Array([1, 2, 3, 4]); // Same content, different instance
      const value3 = new Uint8Array([1, 2, 3, 5]); // Different content

      await storage.put('byte-key', value1);

      // Same content should match
      const result1 = await storage.compareAndSwap!('byte-key', value2, new Uint8Array([5, 6]));
      expect(result1).toBe(true);

      // Different content should not match
      const result2 = await storage.compareAndSwap!('byte-key', value3, new Uint8Array([7, 8]));
      expect(result2).toBe(false);
    });
  });
});

// ============================================================================
// Storage Error Handling Tests
// ============================================================================

describe('Error handling', () => {
  let storage: ReturnType<typeof createMockStorage>;
  const tableLocation = 's3://bucket/warehouse/db/table';

  beforeEach(async () => {
    storage = createMockStorage();
    // Setup initial table
    const metadata = createTestMetadata(tableLocation);
    const metadataPath = `${tableLocation}/metadata/v0.metadata.json`;
    await storage.put(metadataPath, new TextEncoder().encode(JSON.stringify(metadata)));
    await storage.put(getVersionHintPath(tableLocation), new TextEncoder().encode('0'));
  });

  it('should handle storage write failure gracefully', async () => {
    const committer = new AtomicCommitter(storage, tableLocation);

    // Make metadata write fail with a storage error
    const originalPut = storage.put.bind(storage);
    storage.put = async (key: string, value: Uint8Array) => {
      if (key.includes('.metadata.json') && key !== `${tableLocation}/metadata/v0.metadata.json`) {
        throw new Error('Disk full');
      }
      return originalPut(key, value);
    };

    // Also disable putIfAbsent to test put-based fallback
    delete (storage as Partial<typeof storage>).putIfAbsent;

    await expect(
      committer.commit(async (metadata) => {
        if (!metadata) throw new Error('No metadata');
        const snapshot = createTestSnapshot(metadata['last-sequence-number'] + 1);
        return { baseMetadata: metadata, snapshot };
      })
    ).rejects.toThrow('Disk full');
  });

  it('should handle storage read failure during commit', async () => {
    const committer = new AtomicCommitter(storage, tableLocation);

    // Make version hint read fail with network error
    const originalGet = storage.get.bind(storage);
    let callCount = 0;
    storage.get = async (key: string) => {
      callCount++;
      // Fail on second call (during attemptCommit version check)
      if (key.includes('version-hint') && callCount > 1) {
        throw new Error('Network timeout');
      }
      return originalGet(key);
    };

    await expect(
      committer.commit(async (metadata) => {
        if (!metadata) throw new Error('No metadata');
        const snapshot = createTestSnapshot(metadata['last-sequence-number'] + 1);
        return { baseMetadata: metadata, snapshot };
      })
    ).rejects.toThrow('Network timeout');
  });

  it('should handle storage list failure during cleanup', async () => {
    const committer = new AtomicCommitter(storage, tableLocation);

    // Make list fail
    storage.list = async () => {
      throw new Error('Permission denied');
    };

    // Cleanup should handle this gracefully
    const deleted = await committer.cleanupOldMetadata();
    expect(deleted).toEqual([]);
  });

  it('should wrap non-conflict storage errors in CommitTransactionError', async () => {
    const committer = new AtomicCommitter(storage, tableLocation);

    // Remove atomic operations and make version hint write fail
    delete (storage as Partial<typeof storage>).putIfAbsent;
    delete (storage as Partial<typeof storage>).compareAndSwap;

    const originalPut = storage.put.bind(storage);
    storage.put = async (key: string, value: Uint8Array) => {
      if (key.includes('version-hint')) {
        throw new Error('Storage quota exceeded');
      }
      return originalPut(key, value);
    };

    await expect(
      committer.commit(
        async (metadata) => {
          if (!metadata) throw new Error('No metadata');
          const snapshot = createTestSnapshot(metadata['last-sequence-number'] + 1);
          return { baseMetadata: metadata, snapshot };
        },
        { cleanupOnFailure: true }
      )
    ).rejects.toThrow(CommitTransactionError);
  });

  it('should handle intermittent storage failures with retry', async () => {
    const committer = new AtomicCommitter(storage, tableLocation);

    // Make first get call fail, then succeed
    const originalGet = storage.get.bind(storage);
    let callCount = 0;
    storage.get = async (key: string) => {
      callCount++;
      if (key.includes('version-hint') && callCount === 1) {
        throw new Error('Temporary network error');
      }
      return originalGet(key);
    };

    // This should fail because the first loadMetadata call fails
    await expect(
      committer.commit(async (metadata) => {
        if (!metadata) throw new Error('No metadata');
        const snapshot = createTestSnapshot(metadata['last-sequence-number'] + 1);
        return { baseMetadata: metadata, snapshot };
      })
    ).rejects.toThrow('Temporary network error');
  });

  it('should handle metadata file corruption after successful write', async () => {
    const committer = new AtomicCommitter(storage, tableLocation);

    // Make exists check fail
    storage.exists = async () => {
      throw new Error('Connection reset');
    };

    // Commit should still work as exists is not critical
    const result = await committer.commit(async (metadata) => {
      if (!metadata) throw new Error('No metadata');
      const snapshot = createTestSnapshot(metadata['last-sequence-number'] + 1);
      return { baseMetadata: metadata, snapshot };
    });

    expect(result.metadataVersion).toBe(1);
  });
});

// ============================================================================
// Atomic Commit with Storage Operations Tests
// ============================================================================

describe('Atomic Commit with Storage Operations', () => {
  let storage: ReturnType<typeof createMockStorage>;
  const tableLocation = 's3://bucket/warehouse/db/table';

  beforeEach(async () => {
    storage = createMockStorage();
    // Setup initial table
    const metadata = createTestMetadata(tableLocation);
    const metadataPath = `${tableLocation}/metadata/v0.metadata.json`;
    await storage.put(metadataPath, new TextEncoder().encode(JSON.stringify(metadata)));
    await storage.put(getVersionHintPath(tableLocation), new TextEncoder().encode('0'));
  });

  describe('commit uses putIfAbsent', () => {
    it('should use putIfAbsent for metadata file when available', async () => {
      const committer = new AtomicCommitter(storage, tableLocation);
      let putIfAbsentCalled = false;
      const originalPutIfAbsent = storage.putIfAbsent!.bind(storage);

      storage.putIfAbsent = async (key: string, value: Uint8Array) => {
        if (key.includes('.metadata.json')) {
          putIfAbsentCalled = true;
        }
        return originalPutIfAbsent(key, value);
      };

      await committer.commit(async (metadata) => {
        if (!metadata) throw new Error('No metadata');
        const snapshot = createTestSnapshot(metadata['last-sequence-number'] + 1);
        return { baseMetadata: metadata, snapshot };
      });

      expect(putIfAbsentCalled).toBe(true);
    });

    it('should detect conflict when metadata file already exists', async () => {
      const committer = new AtomicCommitter(storage, tableLocation);

      // Pre-create the metadata file that would be written
      // Since the file uses UUID, we need to make putIfAbsent return false
      storage.putIfAbsent = async () => false;

      // With maxRetries 0, CommitConflictError is caught and wrapped in CommitRetryExhaustedError
      // We verify the lastError is a CommitConflictError
      try {
        await committer.commit(
          async (metadata) => {
            if (!metadata) throw new Error('No metadata');
            const snapshot = createTestSnapshot(metadata['last-sequence-number'] + 1);
            return { baseMetadata: metadata, snapshot };
          },
          { maxRetries: 0 }
        );
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(CommitRetryExhaustedError);
        const exhaustedError = error as typeof CommitRetryExhaustedError.prototype;
        expect(exhaustedError.lastError).toBeInstanceOf(CommitConflictError);
        expect(exhaustedError.lastError.message).toContain('metadata file already exists');
      }
    });
  });

  describe('commit uses compareAndSwap for version hint', () => {
    it('should use compareAndSwap for version hint when available', async () => {
      const committer = new AtomicCommitter(storage, tableLocation);
      let compareAndSwapCalled = false;
      const originalCas = storage.compareAndSwap!.bind(storage);

      storage.compareAndSwap = async (key: string, expected: Uint8Array | null, value: Uint8Array) => {
        if (key.includes('version-hint')) {
          compareAndSwapCalled = true;
        }
        return originalCas(key, expected, value);
      };

      await committer.commit(async (metadata) => {
        if (!metadata) throw new Error('No metadata');
        const snapshot = createTestSnapshot(metadata['last-sequence-number'] + 1);
        return { baseMetadata: metadata, snapshot };
      });

      expect(compareAndSwapCalled).toBe(true);
    });
  });

  describe('fallback when atomic operations not available', () => {
    it('should work without putIfAbsent', async () => {
      // Remove putIfAbsent to test fallback
      delete (storage as Partial<typeof storage>).putIfAbsent;

      const committer = new AtomicCommitter(storage, tableLocation);

      const result = await committer.commit(async (metadata) => {
        if (!metadata) throw new Error('No metadata');
        const snapshot = createTestSnapshot(metadata['last-sequence-number'] + 1);
        return { baseMetadata: metadata, snapshot };
      });

      expect(result.metadataVersion).toBe(1);
    });

    it('should work without compareAndSwap', async () => {
      // Remove compareAndSwap to test fallback
      delete (storage as Partial<typeof storage>).compareAndSwap;

      const committer = new AtomicCommitter(storage, tableLocation);

      const result = await committer.commit(async (metadata) => {
        if (!metadata) throw new Error('No metadata');
        const snapshot = createTestSnapshot(metadata['last-sequence-number'] + 1);
        return { baseMetadata: metadata, snapshot };
      });

      expect(result.metadataVersion).toBe(1);
    });

    it('should work without any atomic operations', async () => {
      // Remove both atomic operations
      delete (storage as Partial<typeof storage>).putIfAbsent;
      delete (storage as Partial<typeof storage>).compareAndSwap;

      const committer = new AtomicCommitter(storage, tableLocation);

      const result = await committer.commit(async (metadata) => {
        if (!metadata) throw new Error('No metadata');
        const snapshot = createTestSnapshot(metadata['last-sequence-number'] + 1);
        return { baseMetadata: metadata, snapshot };
      });

      expect(result.metadataVersion).toBe(1);
    });
  });
});
