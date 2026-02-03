import { describe, it, expect } from 'vitest';
import {
  AvroEncoder,
  AvroDecoder,
  AvroFileWriter,
  createManifestEntrySchema,
  createManifestListSchema,
  encodeStatValue,
  truncateString,
  encodeDataFile,
  decodeDataFile,
  encodeManifestEntry,
  decodeManifestEntry,
  encodeManifestListEntry,
  decodeManifestListEntry,
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

describe('Manifest Entry Schema v3 Fields', () => {
  describe('schema structure', () => {
    it('should include field 142 (first_row_id) as optional long in data_file', () => {
      const schema = createManifestEntrySchema([], 0);
      const dataFileField = schema.fields.find((f) => f.name === 'data_file');
      expect(dataFileField).toBeDefined();

      const dataFileSchema = dataFileField!.type as { fields: Array<{ name: string; type: unknown; 'field-id': number }> };
      const firstRowIdField = dataFileSchema.fields.find((f) => f['field-id'] === 142);

      expect(firstRowIdField).toBeDefined();
      expect(firstRowIdField!.name).toBe('first_row_id');
      expect(firstRowIdField!.type).toEqual(['null', 'long']); // optional long
    });

    it('should include field 143 (referenced_data_file) as optional string in data_file', () => {
      const schema = createManifestEntrySchema([], 0);
      const dataFileField = schema.fields.find((f) => f.name === 'data_file');
      expect(dataFileField).toBeDefined();

      const dataFileSchema = dataFileField!.type as { fields: Array<{ name: string; type: unknown; 'field-id': number }> };
      const referencedDataFileField = dataFileSchema.fields.find((f) => f['field-id'] === 143);

      expect(referencedDataFileField).toBeDefined();
      expect(referencedDataFileField!.name).toBe('referenced_data_file');
      expect(referencedDataFileField!.type).toEqual(['null', 'string']); // optional string
    });

    it('should include field 144 (content_offset) as optional long in data_file', () => {
      const schema = createManifestEntrySchema([], 0);
      const dataFileField = schema.fields.find((f) => f.name === 'data_file');
      expect(dataFileField).toBeDefined();

      const dataFileSchema = dataFileField!.type as { fields: Array<{ name: string; type: unknown; 'field-id': number }> };
      const contentOffsetField = dataFileSchema.fields.find((f) => f['field-id'] === 144);

      expect(contentOffsetField).toBeDefined();
      expect(contentOffsetField!.name).toBe('content_offset');
      expect(contentOffsetField!.type).toEqual(['null', 'long']); // optional long
    });

    it('should include field 145 (content_size_in_bytes) as optional long in data_file', () => {
      const schema = createManifestEntrySchema([], 0);
      const dataFileField = schema.fields.find((f) => f.name === 'data_file');
      expect(dataFileField).toBeDefined();

      const dataFileSchema = dataFileField!.type as { fields: Array<{ name: string; type: unknown; 'field-id': number }> };
      const contentSizeField = dataFileSchema.fields.find((f) => f['field-id'] === 145);

      expect(contentSizeField).toBeDefined();
      expect(contentSizeField!.name).toBe('content_size_in_bytes');
      expect(contentSizeField!.type).toEqual(['null', 'long']); // optional long
    });
  });

  describe('encoding data file with v3 fields', () => {
    it('should encode data file with first_row_id', () => {
      // Test that encodeDataFile handles first_row_id
      const dataFile = {
        content: 0,
        file_path: 's3://bucket/data.parquet',
        file_format: 'parquet',
        partition: {},
        record_count: 1000,
        file_size_in_bytes: 5000,
        first_row_id: 42,
      };

      const encoder = new AvroEncoder();
      encodeDataFile(encoder, dataFile, []);
      const buffer = encoder.toBuffer();
      expect(buffer.length).toBeGreaterThan(0);
    });

    it('should encode data file with null first_row_id', () => {
      const dataFile = {
        content: 0,
        file_path: 's3://bucket/data.parquet',
        file_format: 'parquet',
        partition: {},
        record_count: 1000,
        file_size_in_bytes: 5000,
        first_row_id: null,
      };

      const encoder = new AvroEncoder();
      encodeDataFile(encoder, dataFile, []);
      const buffer = encoder.toBuffer();
      expect(buffer.length).toBeGreaterThan(0);
    });

    it('should encode data file with deletion vector fields', () => {
      const dataFile = {
        content: 1, // position deletes
        file_path: 's3://bucket/dv.puffin',
        file_format: 'parquet',
        partition: {},
        record_count: 100,
        file_size_in_bytes: 1000,
        referenced_data_file: 's3://bucket/data.parquet',
        content_offset: 256,
        content_size_in_bytes: 512,
      };

      const encoder = new AvroEncoder();
      encodeDataFile(encoder, dataFile, []);
      const buffer = encoder.toBuffer();
      expect(buffer.length).toBeGreaterThan(0);
    });

    it('should encode manifest entry with all v3 fields', () => {
      const dataFile = {
        content: 1,
        file_path: 's3://bucket/dv.puffin',
        file_format: 'parquet',
        partition: {},
        record_count: 100,
        file_size_in_bytes: 1000,
        first_row_id: 5000,
        referenced_data_file: 's3://bucket/data.parquet',
        content_offset: 256,
        content_size_in_bytes: 512,
      };

      const entry = {
        status: 1,
        snapshot_id: 123456789,
        sequence_number: 1,
        file_sequence_number: 1,
        data_file: dataFile,
      };

      const encoder = new AvroEncoder();
      encodeManifestEntry(encoder, entry, []);
      const buffer = encoder.toBuffer();
      expect(buffer.length).toBeGreaterThan(0);
    });
  });

  describe('decoding manifest entry with v3 fields', () => {
    it('should decode manifest entry with v3 fields', () => {
      const dataFile = {
        content: 1,
        file_path: 's3://bucket/dv.puffin',
        file_format: 'parquet',
        partition: {},
        record_count: 100,
        file_size_in_bytes: 1000,
        first_row_id: 5000,
        referenced_data_file: 's3://bucket/data.parquet',
        content_offset: 256,
        content_size_in_bytes: 512,
      };

      const entry = {
        status: 1,
        snapshot_id: 123456789,
        sequence_number: 1,
        file_sequence_number: 1,
        data_file: dataFile,
      };

      // Encode
      const encoder = new AvroEncoder();
      encodeManifestEntry(encoder, entry, []);
      const buffer = encoder.toBuffer();

      // Decode
      const decoder = new AvroDecoder(buffer);
      const decoded = decodeManifestEntry(decoder, []);

      expect(decoded.status).toBe(1);
      expect(decoded.data_file.first_row_id).toBe(5000);
      expect(decoded.data_file.referenced_data_file).toBe('s3://bucket/data.parquet');
      expect(decoded.data_file.content_offset).toBe(256);
      expect(decoded.data_file.content_size_in_bytes).toBe(512);
    });

    it('should decode manifest entry without v3 fields (v2 compatibility)', () => {
      const dataFile = {
        content: 0,
        file_path: 's3://bucket/data.parquet',
        file_format: 'parquet',
        partition: {},
        record_count: 1000,
        file_size_in_bytes: 5000,
        // No v3 fields
      };

      const entry = {
        status: 1,
        snapshot_id: 123456789,
        sequence_number: 1,
        file_sequence_number: 1,
        data_file: dataFile,
      };

      // Encode
      const encoder = new AvroEncoder();
      encodeManifestEntry(encoder, entry, []);
      const buffer = encoder.toBuffer();

      // Decode
      const decoder = new AvroDecoder(buffer);
      const decoded = decodeManifestEntry(decoder, []);

      expect(decoded.status).toBe(1);
      expect(decoded.data_file.first_row_id).toBeNull();
      expect(decoded.data_file.referenced_data_file).toBeNull();
      expect(decoded.data_file.content_offset).toBeNull();
      expect(decoded.data_file.content_size_in_bytes).toBeNull();
    });

    it('should round-trip encode/decode with v3 fields', () => {
      const originalDataFile = {
        content: 1,
        file_path: 's3://bucket/dv.puffin',
        file_format: 'parquet',
        partition: {},
        record_count: 100,
        file_size_in_bytes: 1000,
        first_row_id: 9999,
        referenced_data_file: 's3://bucket/original-data.parquet',
        content_offset: 1024,
        content_size_in_bytes: 2048,
      };

      const originalEntry = {
        status: 1,
        snapshot_id: 987654321,
        sequence_number: 5,
        file_sequence_number: 3,
        data_file: originalDataFile,
      };

      // Encode
      const encoder = new AvroEncoder();
      encodeManifestEntry(encoder, originalEntry, []);
      const buffer = encoder.toBuffer();

      // Decode
      const decoder = new AvroDecoder(buffer);
      const decoded = decodeManifestEntry(decoder, []);

      // Verify all fields match
      expect(decoded.status).toBe(originalEntry.status);
      expect(decoded.snapshot_id).toBe(originalEntry.snapshot_id);
      expect(decoded.sequence_number).toBe(originalEntry.sequence_number);
      expect(decoded.file_sequence_number).toBe(originalEntry.file_sequence_number);
      expect(decoded.data_file.content).toBe(originalDataFile.content);
      expect(decoded.data_file.file_path).toBe(originalDataFile.file_path);
      expect(decoded.data_file.file_format).toBe(originalDataFile.file_format);
      expect(decoded.data_file.record_count).toBe(originalDataFile.record_count);
      expect(decoded.data_file.file_size_in_bytes).toBe(originalDataFile.file_size_in_bytes);
      expect(decoded.data_file.first_row_id).toBe(originalDataFile.first_row_id);
      expect(decoded.data_file.referenced_data_file).toBe(originalDataFile.referenced_data_file);
      expect(decoded.data_file.content_offset).toBe(originalDataFile.content_offset);
      expect(decoded.data_file.content_size_in_bytes).toBe(originalDataFile.content_size_in_bytes);
    });
  });
});

describe('Manifest List Schema v3 Fields', () => {
  it('should include first_row_id field in manifest list schema', () => {
    const schema = createManifestListSchema();
    const firstRowIdField = schema.fields.find((f) => f.name === 'first_row_id');

    expect(firstRowIdField).toBeDefined();
    expect(firstRowIdField!['field-id']).toBe(519);
    expect(firstRowIdField!.type).toEqual(['null', 'long']); // optional long
  });

  it('should encode manifest list entry with first_row_id', () => {
    const manifestFile = {
      manifest_path: 's3://bucket/manifest.avro',
      manifest_length: 1024,
      partition_spec_id: 0,
      content: 0,
      sequence_number: 1,
      min_sequence_number: 1,
      added_snapshot_id: 123456789,
      added_files_count: 5,
      existing_files_count: 0,
      deleted_files_count: 0,
      added_rows_count: 1000,
      existing_rows_count: 0,
      deleted_rows_count: 0,
      first_row_id: 5000,
    };

    const encoder = new AvroEncoder();
    encodeManifestListEntry(encoder, manifestFile);
    const buffer = encoder.toBuffer();
    expect(buffer.length).toBeGreaterThan(0);
  });

  it('should decode manifest list entry with first_row_id', () => {
    const manifestFile = {
      manifest_path: 's3://bucket/manifest.avro',
      manifest_length: 1024,
      partition_spec_id: 0,
      content: 0,
      sequence_number: 1,
      min_sequence_number: 1,
      added_snapshot_id: 123456789,
      added_files_count: 5,
      existing_files_count: 0,
      deleted_files_count: 0,
      added_rows_count: 1000,
      existing_rows_count: 0,
      deleted_rows_count: 0,
      first_row_id: 5000,
    };

    // Encode
    const encoder = new AvroEncoder();
    encodeManifestListEntry(encoder, manifestFile);
    const buffer = encoder.toBuffer();

    // Decode
    const decoder = new AvroDecoder(buffer);
    const decoded = decodeManifestListEntry(decoder);

    expect(decoded.manifest_path).toBe(manifestFile.manifest_path);
    expect(decoded.first_row_id).toBe(5000);
  });

  it('should handle null first_row_id in manifest list entry', () => {
    const manifestFile = {
      manifest_path: 's3://bucket/manifest.avro',
      manifest_length: 1024,
      partition_spec_id: 0,
      content: 0,
      sequence_number: 1,
      min_sequence_number: 1,
      added_snapshot_id: 123456789,
      added_files_count: 5,
      existing_files_count: 0,
      deleted_files_count: 0,
      added_rows_count: 1000,
      existing_rows_count: 0,
      deleted_rows_count: 0,
      first_row_id: null,
    };

    // Encode
    const encoder = new AvroEncoder();
    encodeManifestListEntry(encoder, manifestFile);
    const buffer = encoder.toBuffer();

    // Decode
    const decoder = new AvroDecoder(buffer);
    const decoded = decodeManifestListEntry(decoder);

    expect(decoded.first_row_id).toBeNull();
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
