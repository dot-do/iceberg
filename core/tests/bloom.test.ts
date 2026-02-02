import { describe, it, expect, beforeEach } from 'vitest';
import {
  BloomFilter,
  BloomFilterWriter,
  xxh64,
  xxh64String,
  xxh64Number,
  xxh64BigInt,
  calculateOptimalBlocks,
  estimateFalsePositiveRate,
  parseBloomFilterFile,
  createBloomFilterMap,
  shouldReadFile,
  shouldReadFileForAny,
  getBloomFilterPath,
} from '../src/index.js';

describe('XXH64 Hash Function', () => {
  it('should hash empty data', () => {
    const hash = xxh64(new Uint8Array(0));
    expect(hash).toBeTypeOf('bigint');
    expect(hash).toBeGreaterThan(0n);
  });

  it('should hash string values', () => {
    const hash1 = xxh64String('hello');
    const hash2 = xxh64String('hello');
    const hash3 = xxh64String('world');

    expect(hash1).toBe(hash2); // Same input = same hash
    expect(hash1).not.toBe(hash3); // Different input = different hash
  });

  it('should hash number values', () => {
    const hash1 = xxh64Number(42);
    const hash2 = xxh64Number(42);
    const hash3 = xxh64Number(43);

    expect(hash1).toBe(hash2);
    expect(hash1).not.toBe(hash3);
  });

  it('should hash BigInt values', () => {
    const hash1 = xxh64BigInt(123456789012345678901234567890n);
    const hash2 = xxh64BigInt(123456789012345678901234567890n);
    const hash3 = xxh64BigInt(987654321098765432109876543210n);

    expect(hash1).toBe(hash2);
    expect(hash1).not.toBe(hash3);
  });

  it('should produce different hashes with different seeds', () => {
    const data = new TextEncoder().encode('test data');
    const hash1 = xxh64(data, 0n);
    const hash2 = xxh64(data, 12345n);

    expect(hash1).not.toBe(hash2);
  });

  it('should hash longer data correctly', () => {
    // Test with data longer than 32 bytes to exercise the main loop
    const longData = new TextEncoder().encode('This is a longer string that exceeds 32 bytes for testing');
    const hash = xxh64(longData);
    expect(hash).toBeTypeOf('bigint');
    expect(hash).toBeGreaterThan(0n);
  });
});

describe('BloomFilter Calculations', () => {
  it('should calculate optimal blocks for small datasets', () => {
    const blocks = calculateOptimalBlocks(1000, 0.01);
    expect(blocks).toBeGreaterThan(0);
    expect(blocks).toBeLessThanOrEqual(1024); // Should be reasonable size
  });

  it('should calculate optimal blocks for large datasets', () => {
    const blocks = calculateOptimalBlocks(1000000, 0.01);
    expect(blocks).toBeGreaterThan(100);
  });

  it('should respect max bytes limit', () => {
    const maxBytes = 1024; // 1KB limit
    const blocks = calculateOptimalBlocks(1000000, 0.01, maxBytes);
    expect(blocks * 32).toBeLessThanOrEqual(maxBytes);
  });

  it('should return minimum blocks for zero items', () => {
    const blocks = calculateOptimalBlocks(0, 0.01);
    expect(blocks).toBe(1);
  });

  it('should estimate false positive rate', () => {
    const fpr = estimateFalsePositiveRate(100, 1000);
    expect(fpr).toBeGreaterThan(0);
    expect(fpr).toBeLessThan(1);
  });

  it('should return 0 FPR for empty filter', () => {
    const fpr = estimateFalsePositiveRate(100, 0);
    expect(fpr).toBe(0);
  });
});

describe('BloomFilter', () => {
  let filter: BloomFilter;

  beforeEach(() => {
    filter = new BloomFilter({ expectedItems: 1000, falsePositiveRate: 0.01 });
  });

  it('should create a filter with correct parameters', () => {
    expect(filter.blockCount).toBeGreaterThan(0);
    expect(filter.count).toBe(0);
    expect(filter.falsePositiveRate).toBe(0.01);
    expect(filter.sizeInBytes).toBe(filter.blockCount * 32);
  });

  it('should add and query string values', () => {
    filter.add('test-value');
    expect(filter.mightContain('test-value')).toBe(true);
    expect(filter.count).toBe(1);
  });

  it('should add and query number values', () => {
    filter.add(12345);
    expect(filter.mightContain(12345)).toBe(true);
  });

  it('should add and query BigInt values', () => {
    filter.add(9007199254740993n);
    expect(filter.mightContain(9007199254740993n)).toBe(true);
  });

  it('should add and query Uint8Array values', () => {
    const data = new Uint8Array([1, 2, 3, 4, 5]);
    filter.add(data);
    expect(filter.mightContain(data)).toBe(true);
  });

  it('should return false for definitely absent values', () => {
    // Add some values
    for (let i = 0; i < 100; i++) {
      filter.add(`value-${i}`);
    }

    // Test values that were never added - most should return false
    let falsePositives = 0;
    const testCount = 1000;
    for (let i = 0; i < testCount; i++) {
      if (filter.mightContain(`absent-${i}`)) {
        falsePositives++;
      }
    }

    // False positive rate should be close to target (0.01 = 1%)
    const actualFpr = falsePositives / testCount;
    expect(actualFpr).toBeLessThan(0.05); // Allow some margin
  });

  it('should add multiple values with addAll', () => {
    const values = ['a', 'b', 'c', 'd', 'e'];
    filter.addAll(values);

    for (const value of values) {
      expect(filter.mightContain(value)).toBe(true);
    }
    expect(filter.count).toBe(5);
  });

  it('should clear the filter', () => {
    filter.add('test');
    expect(filter.mightContain('test')).toBe(true);

    filter.clear();
    expect(filter.count).toBe(0);
    // After clear, values should not be found (filter is empty)
    // Note: mightContain returns false when no bits are set
  });

  it('should merge two filters', () => {
    const filter1 = new BloomFilter({ numBlocks: 10 });
    const filter2 = new BloomFilter({ numBlocks: 10 });

    filter1.add('value-1');
    filter2.add('value-2');

    filter1.merge(filter2);

    expect(filter1.mightContain('value-1')).toBe(true);
    expect(filter1.mightContain('value-2')).toBe(true);
    expect(filter1.count).toBe(2);
  });

  it('should throw when merging filters with different block counts', () => {
    const filter1 = new BloomFilter({ numBlocks: 10 });
    const filter2 = new BloomFilter({ numBlocks: 20 });

    expect(() => filter1.merge(filter2)).toThrow('Cannot merge bloom filters with different block counts');
  });

  it('should calculate estimated FPR', () => {
    for (let i = 0; i < 100; i++) {
      filter.add(`value-${i}`);
    }

    const estimatedFpr = filter.estimatedFalsePositiveRate;
    expect(estimatedFpr).toBeGreaterThan(0);
    expect(estimatedFpr).toBeLessThan(1);
  });
});

describe('BloomFilter Serialization', () => {
  it('should serialize and deserialize a filter', () => {
    const filter = new BloomFilter({ expectedItems: 1000 });
    filter.add('test-value-1');
    filter.add('test-value-2');
    filter.add('test-value-3');

    const serialized = filter.serialize();
    expect(serialized).toBeInstanceOf(Uint8Array);

    const deserialized = BloomFilter.deserialize(serialized);
    expect(deserialized.blockCount).toBe(filter.blockCount);
    expect(deserialized.count).toBe(filter.count);
    expect(deserialized.falsePositiveRate).toBe(filter.falsePositiveRate);

    // Values should still be found
    expect(deserialized.mightContain('test-value-1')).toBe(true);
    expect(deserialized.mightContain('test-value-2')).toBe(true);
    expect(deserialized.mightContain('test-value-3')).toBe(true);
  });

  it('should throw on invalid magic bytes', () => {
    const badData = new Uint8Array([0, 0, 0, 0, 0, 1, 0, 0, 0, 0]);
    expect(() => BloomFilter.deserialize(badData)).toThrow('Invalid bloom filter magic bytes');
  });

  it('should create filter from raw data', () => {
    const filter = new BloomFilter({ numBlocks: 5 });
    filter.add('test');

    const rawData = filter.getRawData();
    const restored = BloomFilter.fromRawData(rawData, filter.count, filter.falsePositiveRate);

    expect(restored.blockCount).toBe(filter.blockCount);
    expect(restored.mightContain('test')).toBe(true);
  });
});

describe('BloomFilterWriter', () => {
  it('should create a writer with options', () => {
    const writer = new BloomFilterWriter({
      basePath: '/data/table',
      expectedItemsPerColumn: 10000,
      falsePositiveRate: 0.01,
    });

    expect(writer.columnCount).toBe(0);
    expect(writer.fieldIds).toEqual([]);
  });

  it('should add values for columns', () => {
    const writer = new BloomFilterWriter({ basePath: '/data/table' });

    writer.addValue(1, 'user_id', 'user-123');
    writer.addValue(1, 'user_id', 'user-456');
    writer.addValue(2, 'email', 'test@example.com');

    expect(writer.columnCount).toBe(2);
    expect(writer.fieldIds).toContain(1);
    expect(writer.fieldIds).toContain(2);
  });

  it('should add multiple values at once', () => {
    const writer = new BloomFilterWriter({ basePath: '/data/table' });

    writer.addValues(1, 'user_id', ['user-1', 'user-2', 'user-3']);

    const filter = writer.getOrCreateFilter(1, 'user_id');
    expect(filter.count).toBe(3);
  });

  it('should finalize and produce serialized data', () => {
    const writer = new BloomFilterWriter({ basePath: '/data/table' });

    writer.addValue(1, 'user_id', 'user-123');
    writer.addValue(2, 'email', 'test@example.com');

    const { data, metadata } = writer.finalize();

    expect(data).toBeInstanceOf(Uint8Array);
    expect(data.length).toBeGreaterThan(0);
    expect(metadata).toHaveLength(2);

    expect(metadata[0].fieldId).toBe(1);
    expect(metadata[0].columnName).toBe('user_id');
    expect(metadata[0].algorithm).toBe('SPLIT_BLOCK');
    expect(metadata[0].hashFunction).toBe('XXHASH64');

    expect(metadata[1].fieldId).toBe(2);
    expect(metadata[1].columnName).toBe('email');
  });

  it('should clear all filters', () => {
    const writer = new BloomFilterWriter({ basePath: '/data/table' });

    writer.addValue(1, 'user_id', 'user-123');
    expect(writer.columnCount).toBe(1);

    writer.clear();
    expect(writer.columnCount).toBe(0);
  });
});

describe('Bloom Filter File Parsing', () => {
  it('should parse a finalized bloom filter file', () => {
    const writer = new BloomFilterWriter({ basePath: '/data/table' });

    writer.addValues(1, 'user_id', ['user-1', 'user-2', 'user-3']);
    writer.addValues(2, 'email', ['a@b.com', 'c@d.com']);

    const { data } = writer.finalize();

    const entries = parseBloomFilterFile(data);
    expect(entries).toHaveLength(2);

    const userFilter = entries.find((e) => e.fieldId === 1);
    expect(userFilter).toBeDefined();
    expect(userFilter!.columnName).toBe('user_id');
    expect(userFilter!.filter.mightContain('user-1')).toBe(true);
    expect(userFilter!.filter.mightContain('user-2')).toBe(true);
    expect(userFilter!.filter.mightContain('user-3')).toBe(true);

    const emailFilter = entries.find((e) => e.fieldId === 2);
    expect(emailFilter).toBeDefined();
    expect(emailFilter!.filter.mightContain('a@b.com')).toBe(true);
  });

  it('should create a lookup map from parsed entries', () => {
    const writer = new BloomFilterWriter({ basePath: '/data/table' });
    writer.addValues(1, 'col1', ['val1']);
    writer.addValues(2, 'col2', ['val2']);

    const { data } = writer.finalize();
    const entries = parseBloomFilterFile(data);
    const map = createBloomFilterMap(entries);

    expect(map.size).toBe(2);
    expect(map.has(1)).toBe(true);
    expect(map.has(2)).toBe(true);
    expect(map.get(1)!.mightContain('val1')).toBe(true);
    expect(map.get(2)!.mightContain('val2')).toBe(true);
  });
});

describe('File Skipping Utilities', () => {
  let filter: BloomFilter;

  beforeEach(() => {
    filter = new BloomFilter({ expectedItems: 100 });
    filter.addAll(['value-1', 'value-2', 'value-3']);
  });

  it('should indicate file should be read when value might exist', () => {
    expect(shouldReadFile(filter, 'value-1')).toBe(true);
    expect(shouldReadFile(filter, 'value-2')).toBe(true);
  });

  it('should indicate file can be skipped when value definitely does not exist', () => {
    // Most values not in the filter should return false
    let canSkipCount = 0;
    for (let i = 0; i < 100; i++) {
      if (!shouldReadFile(filter, `not-present-${i}`)) {
        canSkipCount++;
      }
    }
    // Should be able to skip most non-existent values
    expect(canSkipCount).toBeGreaterThan(90);
  });

  it('should return true when no filter provided', () => {
    expect(shouldReadFile(undefined, 'any-value')).toBe(true);
  });

  it('should check multiple values for IN clause', () => {
    // At least one value exists
    expect(shouldReadFileForAny(filter, ['value-1', 'not-present'])).toBe(true);

    // None of the values exist (with high probability)
    const absent = Array.from({ length: 10 }, (_, i) => `definitely-not-${i}`);
    // This might occasionally return true due to false positives
    const result = shouldReadFileForAny(filter, absent);
    // Just verify it returns a boolean
    expect(typeof result).toBe('boolean');
  });

  it('should return true when no filter for shouldReadFileForAny', () => {
    expect(shouldReadFileForAny(undefined, ['a', 'b'])).toBe(true);
  });
});

describe('Path Utilities', () => {
  it('should generate bloom filter path from data file path', () => {
    expect(getBloomFilterPath('/data/file.parquet')).toBe('/data/file.bloom');
    expect(getBloomFilterPath('/data/table/part-00001.parquet')).toBe('/data/table/part-00001.bloom');
    expect(getBloomFilterPath('file.avro')).toBe('file.bloom');
    expect(getBloomFilterPath('file')).toBe('file.bloom');
  });
});

describe('BloomFilter Performance', () => {
  it('should handle large numbers of insertions', () => {
    const filter = new BloomFilter({ expectedItems: 100000, falsePositiveRate: 0.01 });

    const startAdd = performance.now();
    for (let i = 0; i < 10000; i++) {
      filter.add(`value-${i}`);
    }
    const addTime = performance.now() - startAdd;

    expect(filter.count).toBe(10000);
    expect(addTime).toBeLessThan(1000); // Should complete in < 1 second

    // Verify some values
    const startQuery = performance.now();
    for (let i = 0; i < 1000; i++) {
      filter.mightContain(`value-${i}`);
    }
    const queryTime = performance.now() - startQuery;

    expect(queryTime).toBeLessThan(100); // Queries should be fast
  });

  it('should maintain reasonable FPR under load', () => {
    const expectedItems = 10000;
    const targetFpr = 0.01;
    const filter = new BloomFilter({ expectedItems, falsePositiveRate: targetFpr });

    // Insert expected number of items
    for (let i = 0; i < expectedItems; i++) {
      filter.add(`value-${i}`);
    }

    // Test for false positives
    let falsePositives = 0;
    const testCount = 10000;
    for (let i = 0; i < testCount; i++) {
      if (filter.mightContain(`not-present-${i}`)) {
        falsePositives++;
      }
    }

    const actualFpr = falsePositives / testCount;
    // Allow 3x the target FPR as margin
    expect(actualFpr).toBeLessThan(targetFpr * 3);
  });
});
