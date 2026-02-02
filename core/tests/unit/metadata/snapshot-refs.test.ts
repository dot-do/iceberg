/**
 * Tests for Snapshot References in Iceberg metadata.json
 *
 * @see https://iceberg.apache.org/spec/#snapshot-references
 *
 * Snapshot references (refs) provide named pointers to snapshots, supporting
 * branches and tags for version control semantics in Iceberg tables.
 */

import { describe, it, expect } from 'vitest';
import {
  TableMetadataBuilder,
  SnapshotBuilder,
  SnapshotManager,
  type SnapshotRef,
  type Snapshot,
} from '../../../src/index.js';

describe('Snapshot References', () => {
  describe('Refs Object Structure', () => {
    it('should include refs object in table metadata', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      expect(metadata).toHaveProperty('refs');
      expect(typeof metadata.refs).toBe('object');
      expect(metadata.refs).not.toBeNull();
    });

    it('should have empty refs object for new tables without snapshots', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      expect(Object.keys(metadata.refs).length).toBe(0);
    });
  });

  describe('Branch References', () => {
    it('should create main branch when adding first snapshot', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      const metadata = builder.build();

      expect(metadata.refs).toHaveProperty('main');
      expect(metadata.refs.main.type).toBe('branch');
      expect(metadata.refs.main['snapshot-id']).toBe(snapshot['snapshot-id']);
    });

    it('should serialize branch ref with type "branch"', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      const metadata = builder.build();

      const ref = metadata.refs.main;
      expect(ref.type).toBe('branch');
    });

    it('should create custom branch via createBranch', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      builder.createBranch('feature-branch', snapshot['snapshot-id']);
      const metadata = builder.build();

      expect(metadata.refs).toHaveProperty('feature-branch');
      expect(metadata.refs['feature-branch'].type).toBe('branch');
    });

    it('should support optional max-ref-age-ms for branches', () => {
      const ref: SnapshotRef = {
        'snapshot-id': 12345,
        type: 'branch',
        'max-ref-age-ms': 86400000, // 24 hours
      };

      expect(ref['max-ref-age-ms']).toBe(86400000);
    });

    it('should support optional max-snapshot-age-ms for branches', () => {
      const ref: SnapshotRef = {
        'snapshot-id': 12345,
        type: 'branch',
        'max-snapshot-age-ms': 604800000, // 7 days
      };

      expect(ref['max-snapshot-age-ms']).toBe(604800000);
    });

    it('should support optional min-snapshots-to-keep for branches', () => {
      const ref: SnapshotRef = {
        'snapshot-id': 12345,
        type: 'branch',
        'min-snapshots-to-keep': 5,
      };

      expect(ref['min-snapshots-to-keep']).toBe(5);
    });
  });

  describe('Tag References', () => {
    it('should create tag via createTag', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      builder.createTag('v1.0.0', snapshot['snapshot-id']);
      const metadata = builder.build();

      expect(metadata.refs).toHaveProperty('v1.0.0');
      expect(metadata.refs['v1.0.0'].type).toBe('tag');
    });

    it('should serialize tag ref with type "tag"', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      builder.createTag('release-2024', snapshot['snapshot-id']);
      const metadata = builder.build();

      expect(metadata.refs['release-2024'].type).toBe('tag');
    });

    it('should support optional max-ref-age-ms for tags', () => {
      const ref: SnapshotRef = {
        'snapshot-id': 12345,
        type: 'tag',
        'max-ref-age-ms': 31536000000, // 1 year
      };

      expect(ref['max-ref-age-ms']).toBe(31536000000);
    });
  });

  describe('Reference Validation', () => {
    it('should reference existing snapshot-id', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
        snapshotId: 123456789,
      }).build();

      builder.addSnapshot(snapshot);
      const metadata = builder.build();

      const snapshotIds = metadata.snapshots.map((s) => s['snapshot-id']);
      expect(snapshotIds).toContain(metadata.refs.main['snapshot-id']);
    });

    it('should update main branch when adding new snapshot', () => {
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

      expect(metadata.refs.main['snapshot-id']).toBe(200);
    });
  });

  describe('Multiple References', () => {
    it('should support multiple branches', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 100,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      builder.createBranch('develop', 100);
      builder.createBranch('staging', 100);
      const metadata = builder.build();

      expect(metadata.refs).toHaveProperty('main');
      expect(metadata.refs).toHaveProperty('develop');
      expect(metadata.refs).toHaveProperty('staging');
    });

    it('should support multiple tags', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 100,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      builder.createTag('v1.0.0', 100);
      builder.createTag('release-jan-2024', 100);
      const metadata = builder.build();

      expect(metadata.refs).toHaveProperty('v1.0.0');
      expect(metadata.refs).toHaveProperty('release-jan-2024');
    });

    it('should support mix of branches and tags', () => {
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
      builder.createTag('v1.0.0', 100);
      builder.createBranch('feature-x', 100);
      const metadata = builder.build();

      expect(metadata.refs.main.type).toBe('branch');
      expect(metadata.refs['v1.0.0'].type).toBe('tag');
      expect(metadata.refs['feature-x'].type).toBe('branch');
    });
  });

  describe('addRef Method', () => {
    it('should add reference via addRef with full SnapshotRef', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 100,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      builder.addRef('custom-ref', {
        'snapshot-id': 100,
        type: 'branch',
        'max-ref-age-ms': 3600000,
        'min-snapshots-to-keep': 3,
      });
      const metadata = builder.build();

      expect(metadata.refs['custom-ref']['max-ref-age-ms']).toBe(3600000);
      expect(metadata.refs['custom-ref']['min-snapshots-to-keep']).toBe(3);
    });
  });

  describe('SnapshotManager Refs', () => {
    it('should set ref via SnapshotManager.setRef', () => {
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
      manager.setRef('test-branch', 100, 'branch');

      const updatedMetadata = manager.getMetadata();
      expect(updatedMetadata.refs['test-branch']['snapshot-id']).toBe(100);
      expect(updatedMetadata.refs['test-branch'].type).toBe('branch');
    });

    it('should remove ref via SnapshotManager.removeRef', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 100,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      builder.createTag('to-remove', 100);
      const metadata = builder.build();

      const manager = SnapshotManager.fromMetadata(metadata);
      const removed = manager.removeRef('to-remove');

      expect(removed).toBe(true);
      const updatedMetadata = manager.getMetadata();
      expect(updatedMetadata.refs).not.toHaveProperty('to-remove');
    });

    it('should return false when removing non-existent ref', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      const manager = SnapshotManager.fromMetadata(metadata);
      const removed = manager.removeRef('non-existent');

      expect(removed).toBe(false);
    });

    it('should throw when setting ref to non-existent snapshot', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      const manager = SnapshotManager.fromMetadata(metadata);
      expect(() => manager.setRef('bad-ref', 99999, 'branch')).toThrow();
    });

    it('should get snapshot by ref name', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 100,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      builder.createTag('v1', 100);
      const metadata = builder.build();

      const manager = SnapshotManager.fromMetadata(metadata);
      const refSnapshot = manager.getSnapshotByRef('v1');

      expect(refSnapshot).toBeDefined();
      expect(refSnapshot!['snapshot-id']).toBe(100);
    });

    it('should return undefined for non-existent ref', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });
      const metadata = builder.build();

      const manager = SnapshotManager.fromMetadata(metadata);
      const refSnapshot = manager.getSnapshotByRef('non-existent');

      expect(refSnapshot).toBeUndefined();
    });
  });

  describe('JSON Serialization', () => {
    it('should serialize refs to JSON correctly', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 123456789,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      builder.createTag('v1.0.0', 123456789);
      const metadata = builder.build();

      const json = JSON.stringify(metadata.refs, null, 2);

      expect(json).toContain('"main"');
      expect(json).toContain('"v1.0.0"');
      expect(json).toContain('"snapshot-id"');
      expect(json).toContain('"type"');
    });

    it('should roundtrip refs through JSON parse/stringify', () => {
      const refs: Record<string, SnapshotRef> = {
        main: {
          'snapshot-id': 100,
          type: 'branch',
          'max-ref-age-ms': 86400000,
        },
        'v1.0.0': {
          'snapshot-id': 100,
          type: 'tag',
        },
      };

      const json = JSON.stringify(refs);
      const parsed = JSON.parse(json) as Record<string, SnapshotRef>;

      expect(parsed.main['snapshot-id']).toBe(100);
      expect(parsed.main.type).toBe('branch');
      expect(parsed.main['max-ref-age-ms']).toBe(86400000);
      expect(parsed['v1.0.0'].type).toBe('tag');
    });
  });

  describe('Stats from SnapshotManager', () => {
    it('should count branches and tags', () => {
      const builder = new TableMetadataBuilder({
        location: 's3://bucket/table',
      });

      const snapshot = new SnapshotBuilder({
        sequenceNumber: 1,
        snapshotId: 100,
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      }).build();

      builder.addSnapshot(snapshot);
      builder.createBranch('develop', 100);
      builder.createTag('v1.0.0', 100);
      builder.createTag('v1.0.1', 100);
      const metadata = builder.build();

      const manager = SnapshotManager.fromMetadata(metadata);
      const stats = manager.getStats();

      expect(stats.branchCount).toBe(2); // main + develop
      expect(stats.tagCount).toBe(2); // v1.0.0 + v1.0.1
    });
  });
});
