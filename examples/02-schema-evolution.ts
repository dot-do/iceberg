/**
 * Example 02: Schema Evolution
 *
 * This example demonstrates how to evolve an Iceberg table schema using @dotdo/iceberg.
 * Schema evolution allows you to:
 * - Add new columns
 * - Drop columns
 * - Rename columns
 * - Change column types (compatible promotions only)
 * - Update documentation
 *
 * Run with: npx tsx examples/02-schema-evolution.ts
 */

import {
  TableMetadataBuilder,
  SchemaEvolutionBuilder,
  compareSchemas,
  getSchemaHistory,
  isBackwardCompatible,
  type IcebergSchema,
  type TableMetadata,
} from '../core/src/index.js';

// ============================================================================
// Main Example
// ============================================================================

async function main() {
  console.log('='.repeat(60));
  console.log('Example 02: Schema Evolution');
  console.log('='.repeat(60));
  console.log();

  // Step 1: Create initial table with a basic schema
  const initialSchema: IcebergSchema = {
    'schema-id': 0,
    type: 'struct',
    fields: [
      { id: 1, name: 'id', required: true, type: 'long' },
      { id: 2, name: 'user_name', required: true, type: 'string' },
      { id: 3, name: 'age', required: false, type: 'int' },
      { id: 4, name: 'deprecated_field', required: false, type: 'string' },
    ],
  };

  const builder = new TableMetadataBuilder({
    location: 'warehouse/db/users',
    schema: initialSchema,
  });

  let metadata = builder.build();

  console.log('1. Initial schema (schema-id: 0):');
  printSchema(getCurrentSchema(metadata));
  console.log();

  // Step 2: Evolve the schema - add columns
  console.log('2. Schema evolution - Adding columns...');

  const evolutionBuilder1 = new SchemaEvolutionBuilder(getCurrentSchema(metadata), metadata);
  evolutionBuilder1
    .addColumn('email', 'string', { required: false, doc: 'User email address' })
    .addColumn('phone', 'string', { required: false });

  // Validate before applying
  const validation1 = evolutionBuilder1.validate();
  if (validation1.valid) {
    const result1 = evolutionBuilder1.buildWithMetadata();
    metadata = result1.metadata;
    console.log('   Added columns: email, phone');
    console.log(`   New schema-id: ${result1.schema['schema-id']}`);
  } else {
    console.log('   Validation failed:', validation1.errors);
  }
  console.log();

  // Step 3: Rename a column
  console.log('3. Schema evolution - Renaming column...');

  const evolutionBuilder2 = new SchemaEvolutionBuilder(getCurrentSchema(metadata), metadata);
  evolutionBuilder2.renameColumn('user_name', 'username');

  const validation2 = evolutionBuilder2.validate();
  if (validation2.valid) {
    const result2 = evolutionBuilder2.buildWithMetadata();
    metadata = result2.metadata;
    console.log('   Renamed: user_name -> username');
    console.log(`   New schema-id: ${result2.schema['schema-id']}`);
  }
  console.log();

  // Step 4: Drop a column
  console.log('4. Schema evolution - Dropping column...');

  const evolutionBuilder3 = new SchemaEvolutionBuilder(getCurrentSchema(metadata), metadata);
  evolutionBuilder3.dropColumn('deprecated_field');

  const validation3 = evolutionBuilder3.validate();
  if (validation3.valid) {
    const result3 = evolutionBuilder3.buildWithMetadata();
    metadata = result3.metadata;
    console.log('   Dropped: deprecated_field');
    console.log(`   New schema-id: ${result3.schema['schema-id']}`);
  }
  console.log();

  // Step 5: Widen a type (int -> long)
  console.log('5. Schema evolution - Type promotion...');

  const evolutionBuilder4 = new SchemaEvolutionBuilder(getCurrentSchema(metadata), metadata);
  evolutionBuilder4.updateColumnType('age', 'long');

  const validation4 = evolutionBuilder4.validate();
  if (validation4.valid) {
    const result4 = evolutionBuilder4.buildWithMetadata();
    metadata = result4.metadata;
    console.log('   Promoted: age (int -> long)');
    console.log(`   New schema-id: ${result4.schema['schema-id']}`);
  }
  console.log();

  // Step 6: Show final schema
  console.log('6. Final schema:');
  printSchema(getCurrentSchema(metadata));
  console.log();

  // Step 7: Show schema history
  console.log('7. Schema history:');
  const history = getSchemaHistory(metadata);
  for (const entry of history) {
    console.log(`   Schema ID ${entry.schemaId}:`);
    const schema = metadata.schemas.find((s) => s['schema-id'] === entry.schemaId);
    if (schema) {
      for (const field of schema.fields) {
        console.log(`     - ${field.name}: ${typeof field.type === 'string' ? field.type : 'complex'}`);
      }
    }
  }
  console.log();

  // Step 8: Compare first and last schemas
  console.log('8. Schema comparison (initial vs final):');
  const changes = compareSchemas(metadata.schemas[0], getCurrentSchema(metadata));
  for (const change of changes) {
    console.log(`   - ${change.type}: ${change.fieldName} (field ID: ${change.fieldId})`);
    if (change.oldValue !== undefined) {
      console.log(`     old: ${JSON.stringify(change.oldValue)}`);
    }
    if (change.newValue !== undefined) {
      console.log(`     new: ${JSON.stringify(change.newValue)}`);
    }
  }
  console.log();

  // Step 9: Check backward compatibility
  console.log('9. Backward compatibility check:');
  const compatibility = isBackwardCompatible(changes);
  console.log(`   Compatible: ${compatibility.compatible}`);
  if (compatibility.incompatibleChanges.length > 0) {
    console.log('   Incompatible changes:');
    for (const change of compatibility.incompatibleChanges) {
      console.log(`     - ${change.type}: ${change.fieldName}`);
    }
  }

  console.log();
  console.log('='.repeat(60));
  console.log('Schema evolution completed!');
  console.log('='.repeat(60));
}

// Helper function to get the current schema from metadata
function getCurrentSchema(metadata: TableMetadata): IcebergSchema {
  const schema = metadata.schemas.find((s) => s['schema-id'] === metadata['current-schema-id']);
  if (!schema) {
    throw new Error('Current schema not found');
  }
  return schema;
}

// Helper function to print schema
function printSchema(schema: IcebergSchema): void {
  console.log(`   Schema ID: ${schema['schema-id']}`);
  for (const field of schema.fields) {
    const typeStr = typeof field.type === 'string' ? field.type : JSON.stringify(field.type);
    const docStr = field.doc ? ` -- ${field.doc}` : '';
    console.log(`   - ${field.name}: ${typeStr}${field.required ? ' (required)' : ''}${docStr}`);
  }
}

main().catch(console.error);
