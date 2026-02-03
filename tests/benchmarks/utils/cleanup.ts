/**
 * Test Data Cleanup Utilities
 *
 * Ensures benchmark test data is properly cleaned up after runs.
 */

import type { CatalogClient } from './clients.js';

// ============================================================================
// Cleanup Functions
// ============================================================================

/**
 * Clean up a namespace and all its tables.
 */
export async function cleanupNamespace(
  client: CatalogClient,
  namespace: string[]
): Promise<{ tablesDropped: number; success: boolean; error?: string }> {
  let tablesDropped = 0;

  try {
    // First, list and drop all tables
    try {
      const tables = await client.listTables(namespace);
      for (const table of tables) {
        try {
          await client.dropTable(namespace, table.name);
          tablesDropped++;
        } catch {
          // Ignore individual table drop errors
        }
      }
    } catch {
      // Namespace might not exist or be empty
    }

    // Then drop the namespace
    await client.dropNamespace(namespace);

    return { tablesDropped, success: true };
  } catch (error) {
    return {
      tablesDropped,
      success: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

/**
 * Clean up multiple namespaces matching a prefix.
 */
export async function cleanupNamespacesWithPrefix(
  client: CatalogClient,
  prefix: string
): Promise<{
  namespacesDropped: number;
  tablesDropped: number;
  errors: string[];
}> {
  let namespacesDropped = 0;
  let tablesDropped = 0;
  const errors: string[] = [];

  try {
    const namespaces = await client.listNamespaces();

    for (const namespace of namespaces) {
      if (namespace[0].startsWith(prefix)) {
        const result = await cleanupNamespace(client, namespace);
        if (result.success) {
          namespacesDropped++;
          tablesDropped += result.tablesDropped;
        } else if (result.error) {
          errors.push(`${namespace.join('.')}: ${result.error}`);
        }
      }
    }
  } catch (error) {
    errors.push(error instanceof Error ? error.message : String(error));
  }

  return { namespacesDropped, tablesDropped, errors };
}

/**
 * Cleanup context for managing test resources.
 */
export class CleanupContext {
  private resources: Array<{ client: CatalogClient; namespace: string[] }> = [];

  /**
   * Register a namespace for cleanup.
   */
  register(client: CatalogClient, namespace: string[]): void {
    this.resources.push({ client, namespace });
  }

  /**
   * Clean up all registered resources.
   */
  async cleanup(): Promise<{
    namespacesDropped: number;
    tablesDropped: number;
    errors: string[];
  }> {
    let namespacesDropped = 0;
    let tablesDropped = 0;
    const errors: string[] = [];

    for (const { client, namespace } of this.resources) {
      const result = await cleanupNamespace(client, namespace);
      if (result.success) {
        namespacesDropped++;
        tablesDropped += result.tablesDropped;
      } else if (result.error) {
        errors.push(`${client.name}/${namespace.join('.')}: ${result.error}`);
      }
    }

    this.resources = [];
    return { namespacesDropped, tablesDropped, errors };
  }
}

/**
 * Generate a unique namespace name for benchmarks.
 */
export function generateBenchmarkNamespace(prefix: string = 'bench'): string[] {
  return [`${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`];
}

/**
 * Generate multiple unique table names.
 */
export function generateTableNames(count: number, prefix: string = 'table'): string[] {
  return Array.from({ length: count }, (_, i) => `${prefix}_${i}`);
}
