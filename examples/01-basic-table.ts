/**
 * Example 01: Basic Table Creation
 *
 * This example demonstrates how to create a new Iceberg table using @dotdo/iceberg.
 * It shows the fundamental concepts of:
 * - Creating a storage backend
 * - Building table metadata
 * - Writing metadata to storage
 *
 * Run with: npx tsx examples/01-basic-table.ts
 */

import {
  TableMetadataBuilder,
  MetadataWriter,
  type StorageBackend,
  type IcebergSchema,
} from '../core/src/index.js';

// ============================================================================
// In-Memory Storage Backend
// ============================================================================

/**
 * Simple in-memory storage backend for demonstration purposes.
 * In production, you would use FileSystemCatalog with a real storage backend.
 */
class MemoryStorage implements StorageBackend {
  private data: Map<string, Uint8Array> = new Map();

  async get(key: string): Promise<Uint8Array | null> {
    return this.data.get(key) ?? null;
  }

  async put(key: string, data: Uint8Array): Promise<void> {
    this.data.set(key, data);
  }

  async delete(key: string): Promise<void> {
    this.data.delete(key);
  }

  async list(prefix: string): Promise<string[]> {
    return Array.from(this.data.keys()).filter((k) => k.startsWith(prefix));
  }

  async exists(key: string): Promise<boolean> {
    return this.data.has(key);
  }
}

// ============================================================================
// Main Example
// ============================================================================

async function main() {
  console.log('='.repeat(60));
  console.log('Example 01: Basic Table Creation');
  console.log('='.repeat(60));
  console.log();

  // Step 1: Create a storage backend
  const storage = new MemoryStorage();
  console.log('1. Created in-memory storage backend');

  // Step 2: Define a custom schema
  const schema: IcebergSchema = {
    'schema-id': 0,
    type: 'struct',
    fields: [
      { id: 1, name: 'id', required: true, type: 'long' },
      { id: 2, name: 'name', required: true, type: 'string' },
      { id: 3, name: 'email', required: false, type: 'string' },
      { id: 4, name: 'created_at', required: true, type: 'timestamptz' },
      { id: 5, name: 'is_active', required: true, type: 'boolean' },
    ],
  };
  console.log('2. Defined schema with 5 fields:');
  for (const field of schema.fields) {
    console.log(`   - ${field.name}: ${field.type}${field.required ? ' (required)' : ''}`);
  }
  console.log();

  // Step 3: Create table using TableMetadataBuilder
  const tableLocation = 'warehouse/db/users';

  const builder = new TableMetadataBuilder({
    location: tableLocation,
    schema: schema,
    properties: {
      'description': 'User accounts table',
      'owner': 'data-team',
    },
  });

  const metadata = builder.build();
  console.log('3. Built table metadata using TableMetadataBuilder');
  console.log(`   - Table UUID: ${metadata['table-uuid']}`);
  console.log(`   - Location: ${metadata.location}`);
  console.log(`   - Format version: ${metadata['format-version']}`);
  console.log();

  // Step 4: Write metadata using MetadataWriter
  const writer = new MetadataWriter(storage);
  const result = await writer.writeNewTable({
    location: tableLocation,
    schema: schema,
    properties: {
      'description': 'User accounts table',
      'owner': 'data-team',
    },
  });

  console.log('4. Wrote table metadata to storage');
  console.log(`   - Metadata location: ${result.metadataLocation}`);
  console.log(`   - Version: ${result.version}`);
  console.log();

  // Step 5: Read back and display the result
  const storedMetadata = await storage.get(result.metadataLocation);
  if (storedMetadata) {
    const parsed = JSON.parse(new TextDecoder().decode(storedMetadata));
    console.log('5. Read back stored metadata:');
    console.log(`   - Table UUID: ${parsed['table-uuid']}`);
    console.log(`   - Current schema ID: ${parsed['current-schema-id']}`);
    console.log(`   - Current snapshot ID: ${parsed['current-snapshot-id']}`);
    console.log(`   - Properties:`);
    for (const [key, value] of Object.entries(parsed.properties)) {
      console.log(`     - ${key}: ${value}`);
    }
  }

  console.log();
  console.log('='.repeat(60));
  console.log('Table created successfully!');
  console.log('='.repeat(60));
}

main().catch(console.error);
