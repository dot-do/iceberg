/**
 * Deletion Vector Encoding Tests
 *
 * Tests for reading/writing deletion vectors using Roaring bitmap-like encoding
 * in Puffin files per the deletion-vector-v1 spec.
 *
 * @see https://iceberg.apache.org/spec/#deletion-vectors
 * @see https://iceberg.apache.org/puffin-spec/
 */
import { describe, it, expect, beforeEach } from 'vitest';
import {
  DeletionVector,
  serializeDeletionVector,
  deserializeDeletionVector,
  createDeletionVectorBlob,
  mergeDeletionVectors,
  DELETION_VECTOR_V1_BLOB_TYPE,
  type PuffinBlob,
} from '../../../src/deletes/deletion-vector.js';

// ============================================================================
// Roaring Bitmap Tests
// ============================================================================

describe('DeletionVector - Basic Operations', () => {
  describe('Empty deletion vector', () => {
    it('should create an empty deletion vector', () => {
      const dv = new DeletionVector();

      expect(dv.cardinality()).toBe(0);
      expect(dv.isEmpty()).toBe(true);
    });

    it('should report isEmpty correctly', () => {
      const dv = new DeletionVector();
      expect(dv.isEmpty()).toBe(true);

      dv.add(0n);
      expect(dv.isEmpty()).toBe(false);
    });
  });

  describe('Adding single position', () => {
    it('should add a single position to deletion vector', () => {
      const dv = new DeletionVector();
      dv.add(42n);

      expect(dv.cardinality()).toBe(1);
      expect(dv.has(42n)).toBe(true);
      expect(dv.has(41n)).toBe(false);
      expect(dv.has(43n)).toBe(false);
    });

    it('should handle position 0', () => {
      const dv = new DeletionVector();
      dv.add(0n);

      expect(dv.cardinality()).toBe(1);
      expect(dv.has(0n)).toBe(true);
    });

    it('should not duplicate positions when adding same position twice', () => {
      const dv = new DeletionVector();
      dv.add(100n);
      dv.add(100n);

      expect(dv.cardinality()).toBe(1);
      expect(dv.has(100n)).toBe(true);
    });
  });

  describe('Adding multiple positions', () => {
    it('should add multiple positions to deletion vector', () => {
      const dv = new DeletionVector();
      dv.add(10n);
      dv.add(20n);
      dv.add(30n);
      dv.add(40n);
      dv.add(50n);

      expect(dv.cardinality()).toBe(5);
      expect(dv.has(10n)).toBe(true);
      expect(dv.has(20n)).toBe(true);
      expect(dv.has(30n)).toBe(true);
      expect(dv.has(40n)).toBe(true);
      expect(dv.has(50n)).toBe(true);
      expect(dv.has(15n)).toBe(false);
    });

    it('should handle addAll for bulk insertion', () => {
      const dv = new DeletionVector();
      dv.addAll([100n, 200n, 300n, 400n, 500n]);

      expect(dv.cardinality()).toBe(5);
      expect(dv.has(100n)).toBe(true);
      expect(dv.has(500n)).toBe(true);
    });
  });

  describe('Checking if position is deleted', () => {
    it('should correctly check if position is deleted', () => {
      const dv = new DeletionVector();
      dv.add(5n);
      dv.add(10n);
      dv.add(15n);

      expect(dv.has(5n)).toBe(true);
      expect(dv.has(10n)).toBe(true);
      expect(dv.has(15n)).toBe(true);
      expect(dv.has(7n)).toBe(false);
      expect(dv.has(0n)).toBe(false);
      expect(dv.has(1000n)).toBe(false);
    });
  });

  describe('64-bit position support', () => {
    it('should support 64-bit positions (key = high 32 bits, sub-position = low 32 bits)', () => {
      const dv = new DeletionVector();

      // Position that requires more than 32 bits
      const largePosition = BigInt(2 ** 33) + 100n; // High 32 bits = 2, low 32 bits = 100
      dv.add(largePosition);

      expect(dv.cardinality()).toBe(1);
      expect(dv.has(largePosition)).toBe(true);
      expect(dv.has(100n)).toBe(false); // Different key
    });

    it('should handle positions near 32-bit boundary', () => {
      const dv = new DeletionVector();

      const position32BitMax = BigInt(2 ** 32 - 1);
      const position32BitMaxPlus1 = BigInt(2 ** 32);

      dv.add(position32BitMax);
      dv.add(position32BitMaxPlus1);

      expect(dv.cardinality()).toBe(2);
      expect(dv.has(position32BitMax)).toBe(true);
      expect(dv.has(position32BitMaxPlus1)).toBe(true);
    });

    it('should correctly separate positions into keys and sub-positions', () => {
      const dv = new DeletionVector();

      // Key 0, various sub-positions
      dv.add(0n);
      dv.add(100n);
      dv.add(BigInt(2 ** 32 - 1)); // Max sub-position in key 0

      // Key 1, various sub-positions
      dv.add(BigInt(2 ** 32)); // Key 1, sub-position 0
      dv.add(BigInt(2 ** 32) + 50n); // Key 1, sub-position 50

      expect(dv.cardinality()).toBe(5);
    });

    it('should handle very large 64-bit positions', () => {
      const dv = new DeletionVector();

      // Large position near 64-bit max (staying within safe range)
      const largePosition = BigInt('9007199254740991'); // Number.MAX_SAFE_INTEGER
      dv.add(largePosition);

      expect(dv.cardinality()).toBe(1);
      expect(dv.has(largePosition)).toBe(true);
    });
  });

  describe('Remove operation', () => {
    it('should remove a position', () => {
      const dv = new DeletionVector();
      dv.add(10n);
      dv.add(20n);

      expect(dv.has(10n)).toBe(true);
      dv.remove(10n);
      expect(dv.has(10n)).toBe(false);
      expect(dv.cardinality()).toBe(1);
    });

    it('should handle removing non-existent position', () => {
      const dv = new DeletionVector();
      dv.add(10n);

      // Should not throw
      dv.remove(999n);
      expect(dv.cardinality()).toBe(1);
    });
  });

  describe('Iteration', () => {
    it('should iterate over all positions', () => {
      const dv = new DeletionVector();
      dv.add(30n);
      dv.add(10n);
      dv.add(20n);

      const positions = Array.from(dv.positions());
      expect(positions.sort()).toEqual([10n, 20n, 30n]);
    });

    it('should iterate in sorted order when using sortedPositions', () => {
      const dv = new DeletionVector();
      dv.add(30n);
      dv.add(10n);
      dv.add(20n);

      const positions = dv.sortedPositions();
      expect(positions).toEqual([10n, 20n, 30n]);
    });
  });

  describe('Clear operation', () => {
    it('should clear all positions', () => {
      const dv = new DeletionVector();
      dv.add(10n);
      dv.add(20n);
      dv.add(30n);

      expect(dv.cardinality()).toBe(3);
      dv.clear();
      expect(dv.cardinality()).toBe(0);
      expect(dv.isEmpty()).toBe(true);
    });
  });
});

// ============================================================================
// Binary Encoding Tests
// ============================================================================

describe('DeletionVector - Binary Encoding', () => {
  describe('Serialization', () => {
    it('should serialize deletion vector to binary (deletion-vector-v1 format)', () => {
      const dv = new DeletionVector();
      dv.add(10n);
      dv.add(20n);
      dv.add(30n);

      const binary = serializeDeletionVector(dv);

      expect(binary).toBeInstanceOf(Uint8Array);
      expect(binary.byteLength).toBeGreaterThan(0);
    });

    it('should serialize empty deletion vector', () => {
      const dv = new DeletionVector();

      const binary = serializeDeletionVector(dv);

      expect(binary).toBeInstanceOf(Uint8Array);
      // Empty DV should still have a minimal header
      expect(binary.byteLength).toBeGreaterThan(0);
    });
  });

  describe('Deserialization', () => {
    it('should deserialize binary back to deletion vector', () => {
      const dv = new DeletionVector();
      dv.add(10n);
      dv.add(20n);
      dv.add(30n);

      const binary = serializeDeletionVector(dv);
      const restored = deserializeDeletionVector(binary);

      expect(restored.cardinality()).toBe(3);
      expect(restored.has(10n)).toBe(true);
      expect(restored.has(20n)).toBe(true);
      expect(restored.has(30n)).toBe(true);
    });

    it('should deserialize empty deletion vector', () => {
      const dv = new DeletionVector();

      const binary = serializeDeletionVector(dv);
      const restored = deserializeDeletionVector(binary);

      expect(restored.cardinality()).toBe(0);
      expect(restored.isEmpty()).toBe(true);
    });
  });

  describe('Round-trip encode/decode', () => {
    it('should round-trip encode/decode without data loss', () => {
      const dv = new DeletionVector();
      dv.addAll([1n, 5n, 10n, 50n, 100n, 500n, 1000n]);

      const binary = serializeDeletionVector(dv);
      const restored = deserializeDeletionVector(binary);

      expect(restored.cardinality()).toBe(dv.cardinality());

      // Check all original positions are present
      for (const pos of dv.positions()) {
        expect(restored.has(pos)).toBe(true);
      }
    });

    it('should round-trip with 64-bit positions', () => {
      const dv = new DeletionVector();
      const largePosition = BigInt(2 ** 40) + 12345n;
      dv.add(0n);
      dv.add(largePosition);

      const binary = serializeDeletionVector(dv);
      const restored = deserializeDeletionVector(binary);

      expect(restored.cardinality()).toBe(2);
      expect(restored.has(0n)).toBe(true);
      expect(restored.has(largePosition)).toBe(true);
    });

    it('should round-trip with sparse positions', () => {
      const dv = new DeletionVector();
      // Very sparse - positions far apart
      dv.add(0n);
      dv.add(1000000n);
      dv.add(2000000000n);

      const binary = serializeDeletionVector(dv);
      const restored = deserializeDeletionVector(binary);

      expect(restored.cardinality()).toBe(3);
      expect(restored.has(0n)).toBe(true);
      expect(restored.has(1000000n)).toBe(true);
      expect(restored.has(2000000000n)).toBe(true);
    });

    it('should round-trip with dense positions', () => {
      const dv = new DeletionVector();
      // Dense - consecutive positions
      for (let i = 0n; i < 100n; i++) {
        dv.add(i);
      }

      const binary = serializeDeletionVector(dv);
      const restored = deserializeDeletionVector(binary);

      expect(restored.cardinality()).toBe(100);
      for (let i = 0n; i < 100n; i++) {
        expect(restored.has(i)).toBe(true);
      }
    });
  });

  describe('Format validation', () => {
    it('should throw on invalid binary data', () => {
      const invalidData = new Uint8Array([0, 0, 0, 0]);

      expect(() => deserializeDeletionVector(invalidData)).toThrow();
    });

    it('should throw on truncated data', () => {
      const dv = new DeletionVector();
      dv.add(10n);

      const binary = serializeDeletionVector(dv);
      const truncated = binary.slice(0, binary.length / 2);

      expect(() => deserializeDeletionVector(truncated)).toThrow();
    });
  });
});

// ============================================================================
// Puffin Blob Tests
// ============================================================================

describe('DeletionVector - Puffin Blob', () => {
  describe('Blob type', () => {
    it('should have blob type deletion-vector-v1', () => {
      expect(DELETION_VECTOR_V1_BLOB_TYPE).toBe('deletion-vector-v1');
    });

    it('should create deletion vector blob with correct type', () => {
      const dv = new DeletionVector();
      dv.add(10n);

      const blob = createDeletionVectorBlob(dv, 's3://bucket/data/file.parquet');

      expect(blob.type).toBe('deletion-vector-v1');
    });
  });

  describe('Referenced data file', () => {
    it('should include referenced data file path in blob', () => {
      const dv = new DeletionVector();
      dv.add(10n);

      const referencedFile = 's3://bucket/data/00001.parquet';
      const blob = createDeletionVectorBlob(dv, referencedFile);

      expect(blob.referencedDataFile).toBe(referencedFile);
    });

    it('should handle different path formats', () => {
      const dv = new DeletionVector();
      dv.add(10n);

      const paths = [
        's3://bucket/data/file.parquet',
        's3a://bucket/warehouse/db/table/data/file.parquet',
        'hdfs://namenode:8020/warehouse/file.parquet',
        '/local/path/file.parquet',
      ];

      for (const path of paths) {
        const blob = createDeletionVectorBlob(dv, path);
        expect(blob.referencedDataFile).toBe(path);
      }
    });
  });

  describe('Blob structure', () => {
    it('should include blob data', () => {
      const dv = new DeletionVector();
      dv.add(10n);
      dv.add(20n);

      const blob = createDeletionVectorBlob(dv, 's3://bucket/data/file.parquet');

      expect(blob.data).toBeInstanceOf(Uint8Array);
      expect(blob.data.byteLength).toBeGreaterThan(0);
    });

    it('should set size matching data length', () => {
      const dv = new DeletionVector();
      dv.add(10n);

      const blob = createDeletionVectorBlob(dv, 's3://bucket/data/file.parquet');

      expect(blob.size).toBe(blob.data.byteLength);
    });

    it('should have properties field with referenced-data-file', () => {
      const dv = new DeletionVector();
      dv.add(10n);

      const referencedFile = 's3://bucket/data/file.parquet';
      const blob = createDeletionVectorBlob(dv, referencedFile);

      expect(blob.properties).toBeDefined();
      expect(blob.properties['referenced-data-file']).toBe(referencedFile);
    });
  });

  describe('DataFile field alignment', () => {
    it('should produce blob offset and size for DataFile fields', () => {
      const dv = new DeletionVector();
      dv.add(10n);
      dv.add(20n);

      const blob = createDeletionVectorBlob(dv, 's3://bucket/data/file.parquet');

      // The blob size should match what would be stored in content-size-in-bytes
      expect(typeof blob.size).toBe('number');
      expect(blob.size).toBeGreaterThan(0);

      // Offset will be determined when written to Puffin file
      // Here we just verify the size is correct
    });
  });
});

// ============================================================================
// Merge Tests
// ============================================================================

describe('DeletionVector - Merge', () => {
  describe('Merging two deletion vectors', () => {
    it('should merge two deletion vectors', () => {
      const dv1 = new DeletionVector();
      dv1.add(10n);
      dv1.add(20n);

      const dv2 = new DeletionVector();
      dv2.add(30n);
      dv2.add(40n);

      dv1.merge(dv2);

      expect(dv1.cardinality()).toBe(4);
      expect(dv1.has(10n)).toBe(true);
      expect(dv1.has(20n)).toBe(true);
      expect(dv1.has(30n)).toBe(true);
      expect(dv1.has(40n)).toBe(true);
    });

    it('should handle overlapping positions during merge', () => {
      const dv1 = new DeletionVector();
      dv1.add(10n);
      dv1.add(20n);
      dv1.add(30n);

      const dv2 = new DeletionVector();
      dv2.add(20n); // Overlap
      dv2.add(30n); // Overlap
      dv2.add(40n);

      dv1.merge(dv2);

      expect(dv1.cardinality()).toBe(4); // Deduplicated
      expect(dv1.has(10n)).toBe(true);
      expect(dv1.has(20n)).toBe(true);
      expect(dv1.has(30n)).toBe(true);
      expect(dv1.has(40n)).toBe(true);
    });

    it('should merge with empty deletion vector', () => {
      const dv1 = new DeletionVector();
      dv1.add(10n);
      dv1.add(20n);

      const dv2 = new DeletionVector();

      dv1.merge(dv2);

      expect(dv1.cardinality()).toBe(2);
    });

    it('should merge into empty deletion vector', () => {
      const dv1 = new DeletionVector();

      const dv2 = new DeletionVector();
      dv2.add(10n);
      dv2.add(20n);

      dv1.merge(dv2);

      expect(dv1.cardinality()).toBe(2);
      expect(dv1.has(10n)).toBe(true);
      expect(dv1.has(20n)).toBe(true);
    });
  });

  describe('mergeDeletionVectors function', () => {
    it('should merge multiple deletion vectors', () => {
      const dv1 = new DeletionVector();
      dv1.add(10n);

      const dv2 = new DeletionVector();
      dv2.add(20n);

      const dv3 = new DeletionVector();
      dv3.add(30n);

      const merged = mergeDeletionVectors([dv1, dv2, dv3]);

      expect(merged.cardinality()).toBe(3);
      expect(merged.has(10n)).toBe(true);
      expect(merged.has(20n)).toBe(true);
      expect(merged.has(30n)).toBe(true);
    });

    it('should return empty DV when merging empty array', () => {
      const merged = mergeDeletionVectors([]);

      expect(merged.cardinality()).toBe(0);
      expect(merged.isEmpty()).toBe(true);
    });

    it('should return copy when merging single DV', () => {
      const dv = new DeletionVector();
      dv.add(10n);

      const merged = mergeDeletionVectors([dv]);

      expect(merged.cardinality()).toBe(1);
      expect(merged.has(10n)).toBe(true);

      // Should be a copy, not the same instance
      merged.add(20n);
      expect(dv.has(20n)).toBe(false);
    });
  });

  describe('Merging DV with position delete file entries', () => {
    it('should merge DV with position delete entries', () => {
      const dv = new DeletionVector();
      dv.add(10n);
      dv.add(20n);

      // Simulate position delete entries from a file
      const positionDeleteEntries: bigint[] = [30n, 40n, 50n];

      dv.addAll(positionDeleteEntries);

      expect(dv.cardinality()).toBe(5);
      expect(dv.has(10n)).toBe(true);
      expect(dv.has(30n)).toBe(true);
      expect(dv.has(50n)).toBe(true);
    });
  });
});

// ============================================================================
// Performance Tests (Optional)
// ============================================================================

describe('DeletionVector - Performance', () => {
  describe('Large deletion vectors', () => {
    it('should handle thousands of positions', () => {
      const dv = new DeletionVector();
      const count = 10000;

      // Add 10000 positions
      for (let i = 0n; i < BigInt(count); i++) {
        dv.add(i * 10n); // Sparse: 0, 10, 20, ...
      }

      expect(dv.cardinality()).toBe(count);

      // Verify some random positions
      expect(dv.has(0n)).toBe(true);
      expect(dv.has(5000n * 10n)).toBe(true);
      expect(dv.has(9999n * 10n)).toBe(true);
      expect(dv.has(5n)).toBe(false); // Not a multiple of 10
    });

    it('should serialize and deserialize large deletion vectors', () => {
      const dv = new DeletionVector();
      const count = 5000;

      for (let i = 0n; i < BigInt(count); i++) {
        dv.add(i);
      }

      const binary = serializeDeletionVector(dv);
      const restored = deserializeDeletionVector(binary);

      expect(restored.cardinality()).toBe(count);
    });
  });

  describe('Sparse vs dense deletion patterns', () => {
    it('should handle dense deletion pattern efficiently', () => {
      const dv = new DeletionVector();

      // Dense: consecutive positions 0-999
      for (let i = 0n; i < 1000n; i++) {
        dv.add(i);
      }

      expect(dv.cardinality()).toBe(1000);

      // Serialize should be efficient for dense data
      const binary = serializeDeletionVector(dv);
      const restored = deserializeDeletionVector(binary);

      expect(restored.cardinality()).toBe(1000);
    });

    it('should handle sparse deletion pattern', () => {
      const dv = new DeletionVector();

      // Sparse: positions 0, 1000000, 2000000, ...
      for (let i = 0n; i < 100n; i++) {
        dv.add(i * 1000000n);
      }

      expect(dv.cardinality()).toBe(100);

      const binary = serializeDeletionVector(dv);
      const restored = deserializeDeletionVector(binary);

      expect(restored.cardinality()).toBe(100);
      expect(restored.has(0n)).toBe(true);
      expect(restored.has(99n * 1000000n)).toBe(true);
    });

    it('should handle mixed deletion pattern', () => {
      const dv = new DeletionVector();

      // Dense cluster at start
      for (let i = 0n; i < 100n; i++) {
        dv.add(i);
      }

      // Sparse in middle
      dv.add(1000000n);
      dv.add(2000000n);

      // Dense cluster at end
      for (let i = 5000000n; i < 5000100n; i++) {
        dv.add(i);
      }

      expect(dv.cardinality()).toBe(202);

      const binary = serializeDeletionVector(dv);
      const restored = deserializeDeletionVector(binary);

      expect(restored.cardinality()).toBe(202);
    });
  });
});

// ============================================================================
// Edge Cases
// ============================================================================

describe('DeletionVector - Edge Cases', () => {
  it('should handle maximum 32-bit value', () => {
    const dv = new DeletionVector();
    const maxUint32 = BigInt(2 ** 32 - 1);
    dv.add(maxUint32);

    expect(dv.has(maxUint32)).toBe(true);

    const binary = serializeDeletionVector(dv);
    const restored = deserializeDeletionVector(binary);

    expect(restored.has(maxUint32)).toBe(true);
  });

  it('should reject negative positions', () => {
    const dv = new DeletionVector();

    expect(() => dv.add(-1n)).toThrow();
  });

  it('should create independent copies via clone', () => {
    const dv1 = new DeletionVector();
    dv1.add(10n);
    dv1.add(20n);

    const dv2 = dv1.clone();

    // Modify original
    dv1.add(30n);

    // Clone should not be affected
    expect(dv2.cardinality()).toBe(2);
    expect(dv2.has(30n)).toBe(false);
  });
});
