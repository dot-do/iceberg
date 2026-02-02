/**
 * Tests for Current Snapshot Tracking in Iceberg metadata.json
 *
 * @see https://iceberg.apache.org/spec/#table-metadata
 *
 * The current-snapshot-id field tracks which snapshot represents the
 * current state of the table. It is null for empty tables.
 */

import { describe, it, expect } from 'vitest';
import {
  TableMetadataBuilder,
  SnapshotBuilder,
  SnapshotManager,
  MetadataWriter,
  type Snapshot,
  type SnapshotLogEntry,
  type StorageBackend,
} from '../../../src/index.js';

// Mock storage backend for testing
function createMockStorage(): StorageBackend & { files: Map<string, Uint8Array> } {
  const files = new Map<string, Uint8Array>();
  return {
    files,
    async get(key: string) {
      return files.get(key) ?? null;
    },
    async put(key: string, data: Uint8Array) {
      files.set(key, data);
    },
    async delete(key: string) {
      files.delete(key);
    },
    async list(prefix: string) {
      return Array.from(files.keys()).filter((k) => k.startsWith(prefix));
    },
    async exists(key: string) {
      return files.has(key);
    },
  };
}

describe('Current Snapshot Tracking', () => {
  describe('current-snapshot-id Field', () => {
    it('should be null for tables with no snapshots', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      expect(metadata['current-snapshot-id']).toBeNull();
    });

    it('should reference first snapshot after adding it', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 100,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      const metadata = builder.build();

      expect(metadata['current-snapshot-id']).toBe(100);
    });

    it('should update to latest snapshot after adding multiple', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot1 = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 100,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      const snapshot2 = new SnapshotBuilder({
        sequenceNumber: 2,
        snapshotId: 200,
        parentSnapshotId: 100,
        manifestListPath: 's3://bucket/table/metadata/snap-2.avro',
      }).build();

      builder.addSnapshot(snapshot1);
      builder.addSnapshot(snapshot2);
      const metadata = builder.build();

      expect(metadata['current-snapshot-id']).toBe(200);
    });

    it('should reference snapshot that exists in snapshots array', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 12345,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      const metadata = builder.build();

      const snapshotIds = metadata.snapshots.map((s) => s['snapshot-id']);
      expect(snapshotIds).toContain(metadata['current-snapshot-id']);
    });
  });

  describe('last-sequence-number Field', () => {
    it('should be 0 for tables with no snapshots', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      expect(metadata['last-sequence-number']).toBe(0);
    });

    it('should update when adding snapshot', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      const metadata = builder.build();

      expect(metadata['last-sequence-number']).toBe(1);
    });

    it('should track highest sequence number', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot1 = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 100,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      const snapshot2 = new SnapshotBuilder({
        sequenceNumber: 5,
        snapshotId: 200,
        parentSnapshotId: 100,
        manifestListPath: 's3://bucket/table/metadata/snap-2.avro',
      }).build();

      builder.addSnapshot(snapshot1);
      builder.addSnapshot(snapshot2);
      const metadata = builder.build();

      expect(metadata['last-sequence-number']).toBe(5);
    });
  });

  describe('last-updated-ms Field', () => {
    it('should be set on table creation', () => {
      const before = Date.now();
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();
      const after = Date.now();

      expect(metadata['last-updated-ms']).toBeGreaterThanOrEqual(before);
      expect(metadata['last-updated-ms']).toBeLessThanOrEqual(after);
    });

    it('should update when adding snapshot', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        timestampMs: Date.now() + 1000, // Future timestamp
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      const metadata = builder.build();

      expect(metadata['last-updated-ms']).toBe(snapshot['timestamp-ms']);
    });
  });

  describe('snapshot-log Array', () => {
    it('should be empty for new tables', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      expect(metadata['snapshot-log']).toEqual([]);
    });

    it('should add entry when adding snapshot', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 100,
        timestampMs: 1700000000000,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      const metadata = builder.build();

      expect(metadata['snapshot-log'].length).toBe(1);
      expect(metadata['snapshot-log'][0]['snapshot-id']).toBe(100);
      expect(metadata['snapshot-log'][0]['timestamp-ms']).toBe(1700000000000);
    });

    it('should maintain chronological order', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot1 = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 100,
        timestampMs: 1700000000000,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      const snapshot2 = new SnapshotBuilder({
        sequenceNumber: 2,
        snapshotId: 200,
        timestampMs: 1700000001000,
        parentSnapshotId: 100,
        manifestListPath: 's3://bucket/table/metadata/snap-2.avro',
      }).build();

      builder.addSnapshot(snapshot1);
      builder.addSnapshot(snapshot2);
      const metadata = builder.build();

      expect(metadata['snapshot-log'].length).toBe(2);
      expect(metadata['snapshot-log'][0]['timestamp-ms']).toBeLessThan(
        metadata['snapshot-log'][1]['timestamp-ms']
      );
    });

    it('should include snapshot-id and timestamp-ms fields', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 999,
        timestampMs: 1700000000000,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      const metadata = builder.build();

      const logEntry = metadata['snapshot-log'][0];
      expect(logEntry).toHaveProperty('snapshot-id');
      expect(logEntry).toHaveProperty('timestamp-ms');
    });
  });

  describe('TableMetadataBuilder Snapshot Methods', () => {
    it('should get current snapshot ID via getCurrentSnapshotId', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      expect(builder.getCurrentSnapshotId()).toBeNull();

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 100,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      expect(builder.getCurrentSnapshotId()).toBe(100);
    });

    it('should get next sequence number via getNextSequenceNumber', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      expect(builder.getNextSequenceNumber()).toBe(1);

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      expect(builder.getNextSequenceNumber()).toBe(2);
    });

    it('should get snapshot by ID', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 100,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);

      const retrieved = builder.getSnapshot(100);
      expect(retrieved).toBeDefined();
      expect(retrieved!['snapshot-id']).toBe(100);
    });

    it('should return undefined for non-existent snapshot ID', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const retrieved = builder.getSnapshot(999);
      expect(retrieved).toBeUndefined();
    });

    it('should get current snapshot', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      expect(builder.getCurrentSnapshot()).toBeUndefined();

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 100,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      expect(builder.getCurrentSnapshot()!['snapshot-id']).toBe(100);
    });

    it('should get all snapshots', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      expect(builder.getSnapshots()).toEqual([]);

      const snapshot1 = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 100,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      const snapshot2 = new SnapshotBuilder({
        sequenceNumber: 2,
        snapshotId: 200,
        manifestListPath: 's3://bucket/table/metadata/snap-2.avro',
      }).build();

      builder.addSnapshot(snapshot1);
      builder.addSnapshot(snapshot2);

      expect(builder.getSnapshots().length).toBe(2);
    });

    it('should get snapshot history', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 100,
        timestampMs: 1700000000000,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      const history = builder.getSnapshotHistory();

      expect(history.length).toBe(1);
      expect(history[0]['snapshot-id']).toBe(100);
    });
  });

  describe('SnapshotManager Current Snapshot', () => {
    it('should get current snapshot via SnapshotManager', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 100,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      const metadata = builder.build();

      const manager = SnapshotManager.fromMetadata(metadata);
      const current = manager.getCurrentSnapshot();

      expect(current).toBeDefined();
      expect(current!['snapshot-id']).toBe(100);
    });

    it('should return undefined when no current snapshot', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      const manager = SnapshotManager.fromMetadata(metadata);
      expect(manager.getCurrentSnapshot()).toBeUndefined();
    });

    it('should get snapshot at timestamp (time travel)', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot1 = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 100,
        timestampMs: 1700000000000,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      const snapshot2 = new SnapshotBuilder({
        sequenceNumber: 2,
        snapshotId: 200,
        timestampMs: 1700000002000,
        parentSnapshotId: 100,
        manifestListPath: 's3://bucket/table/metadata/snap-2.avro',
      }).build();

      builder.addSnapshot(snapshot1);
      builder.addSnapshot(snapshot2);
      const metadata = builder.build();

      const manager = SnapshotManager.fromMetadata(metadata);

      // Query at time between snapshots should return first snapshot
      const atTime1 = manager.getSnapshotAtTimestamp(1700000001000);
      expect(atTime1!['snapshot-id']).toBe(100);

      // Query at time after second snapshot should return second snapshot
      const atTime2 = manager.getSnapshotAtTimestamp(1700000003000);
      expect(atTime2!['snapshot-id']).toBe(200);
    });

    it('should return undefined for timestamp before any snapshot', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 100,
        timestampMs: 1700000000000,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      const metadata = builder.build();

      const manager = SnapshotManager.fromMetadata(metadata);
      const result = manager.getSnapshotAtTimestamp(1600000000000);

      expect(result).toBeUndefined();
    });
  });

  describe('MetadataWriter Snapshot Updates', () => {
    it('should update current-snapshot-id via writeWithSnapshot', async () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      const result1 = await writer.writeNewTable({
        location: 's3://bucket/warehouse/db/table',
      });

      expect(result1.metadata['current-snapshot-id']).toBeNull();

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 100,
        manifestListPath: 's3://bucket/warehouse/db/table/metadata/snap-1.avro',
      }).build();

      const result2 = await writer.writeWithSnapshot(
        result1.metadata,
        snapshot,
        result1.metadataLocation
      );

      expect(result2.metadata['current-snapshot-id']).toBe(100);
    });

    it('should increment metadata version on snapshot update', async () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      const result1 = await writer.writeNewTable({
        location: 's3://bucket/warehouse/db/table',
      });

      expect(result1.version).toBe(1);

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 100,
        manifestListPath: 's3://bucket/warehouse/db/table/metadata/snap-1.avro',
      }).build();

      const result2 = await writer.writeWithSnapshot(
        result1.metadata,
        snapshot,
        result1.metadataLocation
      );

      expect(result2.version).toBe(2);
    });

    it('should add previous metadata to metadata-log', async () => {
      const storage = createMockStorage();
      const writer = new MetadataWriter(storage);

      const result1 = await writer.writeNewTable({
        location: 's3://bucket/warehouse/db/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 100,
        manifestListPath: 's3://bucket/warehouse/db/table/metadata/snap-1.avro',
      }).build();

      const result2 = await writer.writeWithSnapshot(
        result1.metadata,
        snapshot,
        result1.metadataLocation
      );

      expect(result2.metadata['metadata-log'].length).toBe(1);
      expect(result2.metadata['metadata-log'][0]['metadata-file']).toBe(
        result1.metadataLocation
      );
    });
  });

  describe('JSON Serialization', () => {
    it('should serialize current-snapshot-id correctly when null', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const json = builder.toJSON();

      expect(json).toContain('"current-snapshot-id": null');
    });

    it('should serialize current-snapshot-id correctly when set', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 123456789,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      const json = builder.toJSON();

      expect(json).toContain('"current-snapshot-id": 123456789');
    });

    it('should serialize snapshot-log correctly', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 100,
        timestampMs: 1700000000000,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      const json = builder.toJSON();

      expect(json).toContain('"snapshot-log"');
      expect(json).toContain('"snapshot-id": 100');
      expect(json).toContain('"timestamp-ms": 1700000000000');
    });
  });
});
