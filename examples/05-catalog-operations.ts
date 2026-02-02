/**
 * Example 05: Catalog Operations
 *
 * This example demonstrates how to use the Iceberg catalog for managing
 * tables and namespaces. The catalog provides:
 * - Namespace management (create, list, drop)
 * - Table management (create, load, list, drop, rename)
 * - Atomic commits for table updates
 *
 * Run with: npx tsx examples/05-catalog-operations.ts
 */

import {
  MemoryCatalog,
  type TableIdentifier,
} from '../core/src/catalog/index.js';
import type {
  IcebergSchema,
  PartitionSpec,
} from '../core/src/metadata/types.js';

// ============================================================================
// Main Example
// ============================================================================

async function main() {
  console.log('='.repeat(60));
  console.log('Example 05: Catalog Operations');
  console.log('='.repeat(60));
  console.log();

  // Step 1: Create a catalog
  console.log('1. Creating MemoryCatalog:');
  const catalog = new MemoryCatalog({ name: 'demo-catalog' });
  console.log(`   Catalog name: ${catalog.name()}`);
  console.log();

  // Step 2: Create namespaces
  console.log('2. Creating namespaces:');

  await catalog.createNamespace(['production'], {
    description: 'Production data warehouse',
    owner: 'data-team',
  });
  console.log('   Created: production');

  await catalog.createNamespace(['production', 'analytics'], {
    description: 'Analytics datasets',
  });
  console.log('   Created: production.analytics');

  await catalog.createNamespace(['staging'], {
    description: 'Staging environment',
  });
  console.log('   Created: staging');
  console.log();

  // Step 3: List namespaces
  console.log('3. Listing namespaces:');

  const topLevel = await catalog.listNamespaces();
  console.log('   Top-level namespaces:');
  for (const ns of topLevel) {
    console.log(`     - ${ns.join('.')}`);
  }

  const productionChildren = await catalog.listNamespaces(['production']);
  console.log('   Under production:');
  for (const ns of productionChildren) {
    console.log(`     - ${ns.join('.')}`);
  }
  console.log();

  // Step 4: Get namespace properties
  console.log('4. Namespace properties:');

  const props = await catalog.getNamespaceProperties(['production']);
  console.log('   production namespace:');
  for (const [key, value] of Object.entries(props)) {
    console.log(`     ${key}: ${value}`);
  }
  console.log();

  // Step 5: Create tables
  console.log('5. Creating tables:');

  // Users table schema
  const usersSchema: IcebergSchema = {
    'schema-id': 0,
    type: 'struct',
    fields: [
      { id: 1, name: 'id', required: true, type: 'long' },
      { id: 2, name: 'name', required: true, type: 'string' },
      { id: 3, name: 'email', required: false, type: 'string' },
      { id: 4, name: 'created_at', required: true, type: 'timestamptz' },
    ],
  };

  // Events table schema
  const eventsSchema: IcebergSchema = {
    'schema-id': 0,
    type: 'struct',
    fields: [
      { id: 1, name: 'event_id', required: true, type: 'string' },
      { id: 2, name: 'user_id', required: true, type: 'long' },
      { id: 3, name: 'event_type', required: true, type: 'string' },
      { id: 4, name: 'event_time', required: true, type: 'timestamptz' },
      { id: 5, name: 'properties', required: false, type: 'binary' },
    ],
  };

  // Events partition spec
  const eventsPartitionSpec: PartitionSpec = {
    'spec-id': 0,
    fields: [
      {
        'source-id': 4,
        'field-id': 1000,
        name: 'event_day',
        transform: 'day',
      },
    ],
  };

  // Create users table
  const usersMetadata = await catalog.createTable(['production'], {
    name: 'users',
    schema: usersSchema,
    properties: { 'description': 'User accounts' },
  });
  console.log(`   Created: production.users`);
  console.log(`     UUID: ${usersMetadata['table-uuid']}`);

  // Create events table with partitioning
  const eventsMetadata = await catalog.createTable(['production', 'analytics'], {
    name: 'events',
    schema: eventsSchema,
    partitionSpec: eventsPartitionSpec,
    properties: { 'description': 'User events log' },
  });
  console.log(`   Created: production.analytics.events`);
  console.log(`     UUID: ${eventsMetadata['table-uuid']}`);
  console.log(`     Partition spec: ${eventsMetadata['partition-specs'][0].fields[0]?.name || 'unpartitioned'}`);

  // Create a staging table
  await catalog.createTable(['staging'], {
    name: 'users_staging',
    schema: usersSchema,
  });
  console.log(`   Created: staging.users_staging`);
  console.log();

  // Step 6: List tables
  console.log('6. Listing tables:');

  const productionTables = await catalog.listTables(['production']);
  console.log('   Tables in production:');
  for (const table of productionTables) {
    console.log(`     - ${formatTableIdentifier(table)}`);
  }

  const analyticsTables = await catalog.listTables(['production', 'analytics']);
  console.log('   Tables in production.analytics:');
  for (const table of analyticsTables) {
    console.log(`     - ${formatTableIdentifier(table)}`);
  }

  const stagingTables = await catalog.listTables(['staging']);
  console.log('   Tables in staging:');
  for (const table of stagingTables) {
    console.log(`     - ${formatTableIdentifier(table)}`);
  }
  console.log();

  // Step 7: Load table metadata
  console.log('7. Loading table metadata:');

  const identifier: TableIdentifier = {
    namespace: ['production'],
    name: 'users',
  };

  const loadedMetadata = await catalog.loadTable(identifier);
  console.log(`   Table: ${formatTableIdentifier(identifier)}`);
  console.log(`   Format version: ${loadedMetadata['format-version']}`);
  console.log(`   Schema fields: ${loadedMetadata.schemas[0].fields.length}`);
  console.log(`   Current snapshot: ${loadedMetadata['current-snapshot-id']}`);
  console.log();

  // Step 8: Check table existence
  console.log('8. Checking table existence:');

  const existingTable = { namespace: ['production'], name: 'users' };
  const nonExistingTable = { namespace: ['production'], name: 'orders' };

  console.log(`   ${formatTableIdentifier(existingTable)}: ${await catalog.tableExists(existingTable)}`);
  console.log(`   ${formatTableIdentifier(nonExistingTable)}: ${await catalog.tableExists(nonExistingTable)}`);
  console.log();

  // Step 9: Rename a table
  console.log('9. Renaming a table:');

  const fromId: TableIdentifier = { namespace: ['staging'], name: 'users_staging' };
  const toId: TableIdentifier = { namespace: ['staging'], name: 'users_backup' };

  console.log(`   Renaming ${formatTableIdentifier(fromId)} to ${formatTableIdentifier(toId)}`);
  await catalog.renameTable(fromId, toId);

  const renamedTables = await catalog.listTables(['staging']);
  console.log('   Tables in staging after rename:');
  for (const table of renamedTables) {
    console.log(`     - ${formatTableIdentifier(table)}`);
  }
  console.log();

  // Step 10: Update table properties via commit
  console.log('10. Updating table properties:');

  const commitResult = await catalog.commitTable({
    identifier: { namespace: ['production'], name: 'users' },
    requirements: [],
    updates: [
      {
        action: 'set-properties',
        updates: {
          'retention.days': '90',
          'last-modified-by': 'admin',
        },
      },
    ],
  });

  console.log(`   Updated metadata location: ${commitResult['metadata-location']}`);
  console.log(`   New properties:`);
  for (const [key, value] of Object.entries(commitResult.metadata.properties)) {
    console.log(`     ${key}: ${value}`);
  }
  console.log();

  // Step 11: Drop a table
  console.log('11. Dropping a table:');

  const dropResult = await catalog.dropTable(toId);
  console.log(`   Dropped ${formatTableIdentifier(toId)}: ${dropResult}`);

  const afterDropTables = await catalog.listTables(['staging']);
  console.log(`   Tables in staging after drop: ${afterDropTables.length}`);
  console.log();

  // Step 12: Drop empty namespace
  console.log('12. Dropping empty namespace:');

  const nsDropResult = await catalog.dropNamespace(['staging']);
  console.log(`   Dropped staging namespace: ${nsDropResult}`);

  const finalNamespaces = await catalog.listNamespaces();
  console.log('   Remaining top-level namespaces:');
  for (const ns of finalNamespaces) {
    console.log(`     - ${ns.join('.')}`);
  }

  console.log();
  console.log('='.repeat(60));
  console.log('Catalog operations example completed!');
  console.log('='.repeat(60));
}

// Helper function to format table identifier
function formatTableIdentifier(id: TableIdentifier): string {
  return [...id.namespace, id.name].join('.');
}

main().catch(console.error);
