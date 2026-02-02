import { describe, it, expect } from 'vitest';
import {
  AvroEncoder,
  AvroFileWriter,
  createManifestEntrySchema,
  createManifestListSchema,
  encodeStatValue,
  truncateString,
} from '../src/avro/index.js';

describe('AvroEncoder', () => {
  describe('primitive encoding', () => {
    it('should encode boolean values', () => {
      const encoder = new AvroEncoder();
      encoder.writeBoolean(true);
      encoder.writeBoolean(false);

      const buffer = encoder.toBuffer();
      expect(buffer[0]).toBe(1);
      expect(buffer[1]).toBe(0);
    });

    it('should encode int values with zig-zag encoding', () => {
      const encoder = new AvroEncoder();
      encoder.writeInt(0);
      expect(encoder.toBuffer()).toEqual(new Uint8Array([0]));
    });

    it('should encode long values', () => {
      const encoder = new AvroEncoder();
      encoder.writeLong(0);
      expect(encoder.toBuffer()).toEqual(new Uint8Array([0]));
    });

    it('should encode float values', () => {
      const encoder = new AvroEncoder();
      encoder.writeFloat(1.5);

      const buffer = encoder.toBuffer();
      expect(buffer.length).toBe(4);

      // Verify the float encoding by checking little-endian bytes
      const view = new DataView(new ArrayBuffer(4));
      view.setFloat32(0, 1.5, true);
      expect(buffer).toEqual(new Uint8Array(view.buffer));
    });

    it('should encode double values', () => {
      const encoder = new AvroEncoder();
      encoder.writeDouble(3.14159);

      const buffer = encoder.toBuffer();
      expect(buffer.length).toBe(8);
    });

    it('should encode string values', () => {
      const encoder = new AvroEncoder();
      encoder.writeString('hello');

      const buffer = encoder.toBuffer();
      // Length (5 as varint) + "hello" (5 bytes)
      expect(buffer.length).toBe(6);
    });

    it('should encode bytes values', () => {
      const encoder = new AvroEncoder();
      encoder.writeBytes(new Uint8Array([1, 2, 3, 4]));

      const buffer = encoder.toBuffer();
      // Length (4 as varint) + 4 bytes
      expect(buffer.length).toBe(5);
    });
  });

  describe('complex encoding', () => {
    it('should encode arrays', () => {
      const encoder = new AvroEncoder();
      encoder.writeArray([1, 2, 3], (v) => encoder.writeInt(v));

      const buffer = encoder.toBuffer();
      // Count (3) + 3 ints + terminating 0
      expect(buffer.length).toBeGreaterThan(0);
    });

    it('should encode empty arrays', () => {
      const encoder = new AvroEncoder();
      encoder.writeArray([], (v: number) => encoder.writeInt(v));

      const buffer = encoder.toBuffer();
      // Just terminating 0
      expect(buffer.length).toBe(1);
      expect(buffer[0]).toBe(0);
    });

    it('should encode maps', () => {
      const encoder = new AvroEncoder();
      encoder.writeMap({ a: 1, b: 2 }, (v) => encoder.writeInt(v));

      const buffer = encoder.toBuffer();
      expect(buffer.length).toBeGreaterThan(0);
    });

    it('should encode union indices', () => {
      const encoder = new AvroEncoder();
      encoder.writeUnionIndex(0);
      encoder.writeUnionIndex(1);

      const buffer = encoder.toBuffer();
      expect(buffer[0]).toBe(0);
      expect(buffer[1]).toBe(2); // 1 encoded as zig-zag varint
    });
  });
});

describe('AvroFileWriter', () => {
  it('should create a valid Avro container file', () => {
    const schema = createManifestListSchema();
    const writer = new AvroFileWriter(schema, new Map([['test-key', 'test-value']]));

    // Add a block with some data
    const encoder = new AvroEncoder();
    encoder.writeString('/path/to/manifest.avro');
    encoder.writeLong(1024);
    encoder.writeInt(0);
    encoder.writeInt(0);
    encoder.writeLong(1);
    encoder.writeLong(1);
    encoder.writeLong(123456789);
    encoder.writeInt(5);
    encoder.writeInt(0);
    encoder.writeInt(0);
    encoder.writeLong(1000);
    encoder.writeLong(0);
    encoder.writeLong(0);
    encoder.writeUnionIndex(0); // null partitions

    writer.addBlock(1, encoder.toBuffer());

    const buffer = writer.toBuffer();

    // Check magic bytes "Obj" + version 1
    expect(buffer[0]).toBe(0x4f); // 'O'
    expect(buffer[1]).toBe(0x62); // 'b'
    expect(buffer[2]).toBe(0x6a); // 'j'
    expect(buffer[3]).toBe(0x01); // version 1
  });
});

describe('Iceberg Avro Schemas', () => {
  it('should create manifest entry schema', () => {
    const schema = createManifestEntrySchema([], 0);

    expect(schema.type).toBe('record');
    expect(schema.name).toBe('manifest_entry');
    expect(schema.fields).toHaveLength(5);

    const fieldNames = schema.fields.map((f) => f.name);
    expect(fieldNames).toContain('status');
    expect(fieldNames).toContain('snapshot_id');
    expect(fieldNames).toContain('data_file');
  });

  it('should create manifest list schema', () => {
    const schema = createManifestListSchema();

    expect(schema.type).toBe('record');
    expect(schema.name).toBe('manifest_file');
    expect(schema.fields.length).toBeGreaterThan(0);

    const fieldNames = schema.fields.map((f) => f.name);
    expect(fieldNames).toContain('manifest_path');
    expect(fieldNames).toContain('manifest_length');
    expect(fieldNames).toContain('partition_spec_id');
  });
});

describe('Statistics Encoding', () => {
  it('should encode boolean stats', () => {
    const bytes = encodeStatValue(true, 'boolean');
    expect(bytes).toEqual(new Uint8Array([1]));

    const bytesF = encodeStatValue(false, 'boolean');
    expect(bytesF).toEqual(new Uint8Array([0]));
  });

  it('should encode int stats', () => {
    const bytes = encodeStatValue(42, 'int');
    expect(bytes.length).toBe(4);

    const view = new DataView(bytes.buffer);
    expect(view.getInt32(0, true)).toBe(42);
  });

  it('should encode long stats', () => {
    const bytes = encodeStatValue(1234567890, 'long');
    expect(bytes.length).toBe(8);

    const view = new DataView(bytes.buffer);
    expect(view.getBigInt64(0, true)).toBe(1234567890n);
  });

  it('should encode float stats', () => {
    const bytes = encodeStatValue(3.14, 'float');
    expect(bytes.length).toBe(4);
  });

  it('should encode double stats', () => {
    const bytes = encodeStatValue(3.14159265359, 'double');
    expect(bytes.length).toBe(8);
  });

  it('should encode string stats', () => {
    const bytes = encodeStatValue('hello', 'string');
    expect(bytes).toEqual(new TextEncoder().encode('hello'));
  });

  it('should encode null values as empty array', () => {
    const bytes = encodeStatValue(null, 'string');
    expect(bytes.length).toBe(0);

    const bytes2 = encodeStatValue(undefined, 'int');
    expect(bytes2.length).toBe(0);
  });

  it('should truncate strings', () => {
    expect(truncateString('hello', 10)).toBe('hello');
    expect(truncateString('hello world', 5)).toBe('hello');
    expect(truncateString('', 10)).toBe('');
  });
});
