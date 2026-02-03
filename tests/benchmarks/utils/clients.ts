/**
 * Catalog Clients for Benchmarks
 *
 * Provides unified interfaces for both iceberg.do and R2 Data Catalog.
 */

import {
  R2DataCatalogClient,
  type R2DataCatalogConfig,
  type RegisterTableRequest,
  type CatalogTable,
  type ListTablesResponse,
  type CatalogNamespace,
} from '../../../core/src/catalog/r2-client.js';
import type { IcebergSchema, PartitionSpec } from '../../../core/src/metadata/types.js';

// ============================================================================
// Configuration
// ============================================================================

export const ICEBERG_DO_URL =
  process.env.ICEBERG_DO_URL || 'https://iceberg-do.dotdo.workers.dev';

export const ICEBERG_DO_BASE_URL = `${ICEBERG_DO_URL}/v1`;

// ============================================================================
// Common Types
// ============================================================================

export interface CreateTableRequest {
  name: string;
  namespace: string[];
  schema: IcebergSchema;
  partitionSpec?: PartitionSpec;
  properties?: Record<string, string>;
}

export interface TableIdentifier {
  namespace: string[];
  name: string;
}

export interface CatalogClient {
  name: string;

  // Namespace operations
  createNamespace(
    namespace: string[],
    properties?: Record<string, string>
  ): Promise<{ namespace: string[]; properties?: Record<string, string> }>;

  listNamespaces(parent?: string[]): Promise<string[][]>;

  getNamespace(
    namespace: string[]
  ): Promise<{ namespace: string[]; properties?: Record<string, string> }>;

  dropNamespace(namespace: string[]): Promise<boolean>;

  // Table operations
  createTable(request: CreateTableRequest): Promise<TableMetadata>;

  loadTable(namespace: string[], name: string): Promise<TableMetadata>;

  listTables(namespace: string[]): Promise<TableIdentifier[]>;

  dropTable(namespace: string[], name: string): Promise<boolean>;

  // Commits
  commitPropertyUpdate(
    namespace: string[],
    name: string,
    properties: Record<string, string>
  ): Promise<TableMetadata>;
}

export interface TableMetadata {
  'format-version': number;
  'table-uuid': string;
  location: string;
  'last-updated-ms': number;
  schemas: Array<{
    type: 'struct';
    'schema-id': number;
    fields: Array<{ id: number; name: string; required: boolean; type: string }>;
  }>;
  properties: Record<string, string>;
}

// ============================================================================
// iceberg.do Client
// ============================================================================

export class IcebergDoClient implements CatalogClient {
  name = 'iceberg.do';
  private baseUrl: string;

  constructor(baseUrl: string = ICEBERG_DO_BASE_URL) {
    this.baseUrl = baseUrl;
  }

  private async request<T>(
    method: 'GET' | 'POST' | 'DELETE',
    path: string,
    body?: unknown
  ): Promise<T> {
    const url = `${this.baseUrl}${path}`;

    const response = await fetch(url, {
      method,
      headers: { 'Content-Type': 'application/json' },
      body: body ? JSON.stringify(body) : undefined,
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`HTTP ${response.status}: ${text}`);
    }

    // Handle 204 No Content
    if (response.status === 204) {
      return undefined as T;
    }

    return response.json() as Promise<T>;
  }

  async createNamespace(
    namespace: string[],
    properties?: Record<string, string>
  ): Promise<{ namespace: string[]; properties?: Record<string, string> }> {
    return this.request('POST', '/namespaces', {
      namespace,
      properties: properties ?? {},
    });
  }

  async listNamespaces(parent?: string[]): Promise<string[][]> {
    const params = parent ? `?parent=${encodeURIComponent(parent.join('\x1f'))}` : '';
    const result = await this.request<{ namespaces: string[][] }>('GET', `/namespaces${params}`);
    return result.namespaces;
  }

  async getNamespace(
    namespace: string[]
  ): Promise<{ namespace: string[]; properties?: Record<string, string> }> {
    return this.request('GET', `/namespaces/${namespace.join('%1F')}`);
  }

  async dropNamespace(namespace: string[]): Promise<boolean> {
    try {
      await this.request('DELETE', `/namespaces/${namespace.join('%1F')}`);
      return true;
    } catch (error) {
      if (error instanceof Error && error.message.includes('404')) {
        return false;
      }
      throw error;
    }
  }

  async createTable(request: CreateTableRequest): Promise<TableMetadata> {
    const result = await this.request<{ metadata: TableMetadata }>(
      'POST',
      `/namespaces/${request.namespace.join('%1F')}/tables`,
      {
        name: request.name,
        schema: request.schema,
        'partition-spec': request.partitionSpec,
        properties: request.properties,
      }
    );
    return result.metadata;
  }

  async loadTable(namespace: string[], name: string): Promise<TableMetadata> {
    const result = await this.request<{ metadata: TableMetadata }>(
      'GET',
      `/namespaces/${namespace.join('%1F')}/tables/${encodeURIComponent(name)}`
    );
    return result.metadata;
  }

  async listTables(namespace: string[]): Promise<TableIdentifier[]> {
    const result = await this.request<{ identifiers: TableIdentifier[] }>(
      'GET',
      `/namespaces/${namespace.join('%1F')}/tables`
    );
    return result.identifiers || [];
  }

  async dropTable(namespace: string[], name: string): Promise<boolean> {
    try {
      await this.request(
        'DELETE',
        `/namespaces/${namespace.join('%1F')}/tables/${encodeURIComponent(name)}`
      );
      return true;
    } catch (error) {
      if (error instanceof Error && error.message.includes('404')) {
        return false;
      }
      throw error;
    }
  }

  async commitPropertyUpdate(
    namespace: string[],
    name: string,
    properties: Record<string, string>
  ): Promise<TableMetadata> {
    // First load table to get UUID
    const current = await this.loadTable(namespace, name);

    const result = await this.request<{ metadata: TableMetadata }>(
      'POST',
      `/namespaces/${namespace.join('%1F')}/tables/${encodeURIComponent(name)}`,
      {
        requirements: [{ type: 'assert-table-uuid', uuid: current['table-uuid'] }],
        updates: [{ action: 'set-properties', updates: properties }],
      }
    );
    return result.metadata;
  }
}

// ============================================================================
// R2 Data Catalog Client Wrapper
// ============================================================================

export class R2CatalogClientWrapper implements CatalogClient {
  name = 'r2-data-catalog';
  private client: R2DataCatalogClient;

  constructor(config?: R2DataCatalogConfig) {
    if (config) {
      this.client = new R2DataCatalogClient(config);
    } else {
      // Try to create from env vars
      const accountId = process.env.CF_ACCOUNT_ID;
      const token = process.env.R2_DATA_CATALOG_TOKEN;

      if (!accountId || !token) {
        throw new Error(
          'R2 Data Catalog requires CF_ACCOUNT_ID and R2_DATA_CATALOG_TOKEN environment variables'
        );
      }

      this.client = new R2DataCatalogClient({ accountId, token });
    }
  }

  async createNamespace(
    namespace: string[],
    properties?: Record<string, string>
  ): Promise<{ namespace: string[]; properties?: Record<string, string> }> {
    const result = await this.client.createNamespace(namespace, properties);
    return { namespace: result.namespace, properties: result.properties };
  }

  async listNamespaces(parent?: string[]): Promise<string[][]> {
    const result = await this.client.listNamespaces(parent);
    return result.namespaces;
  }

  async getNamespace(
    namespace: string[]
  ): Promise<{ namespace: string[]; properties?: Record<string, string> }> {
    const result = await this.client.getNamespace(namespace);
    return { namespace: result.namespace, properties: result.properties };
  }

  async dropNamespace(namespace: string[]): Promise<boolean> {
    return this.client.dropNamespace(namespace);
  }

  async createTable(request: CreateTableRequest): Promise<TableMetadata> {
    const result = await this.client.createTable({
      name: request.name,
      namespace: request.namespace,
      location: `s3://benchmark-bucket/warehouse/${request.namespace.join('/')}/${request.name}`,
      schema: request.schema,
      partitionSpec: request.partitionSpec,
      properties: request.properties,
    });
    return result.metadata as TableMetadata;
  }

  async loadTable(namespace: string[], name: string): Promise<TableMetadata> {
    const result = await this.client.loadTable(namespace, name);
    return result.metadata as TableMetadata;
  }

  async listTables(namespace: string[]): Promise<TableIdentifier[]> {
    const result = await this.client.listTables(namespace);
    return result.identifiers;
  }

  async dropTable(namespace: string[], name: string): Promise<boolean> {
    return this.client.dropTable(namespace, name);
  }

  async commitPropertyUpdate(
    namespace: string[],
    name: string,
    properties: Record<string, string>
  ): Promise<TableMetadata> {
    const result = await this.client.updateTableLocation({
      name,
      namespace,
      properties,
    });
    return result.metadata as TableMetadata;
  }
}

// ============================================================================
// Factory Functions
// ============================================================================

export function createIcebergDoClient(): IcebergDoClient {
  return new IcebergDoClient();
}

export function createR2CatalogClient(): R2CatalogClientWrapper | null {
  const accountId = process.env.CF_ACCOUNT_ID;
  const token = process.env.R2_DATA_CATALOG_TOKEN;

  if (!accountId || !token) {
    return null;
  }

  return new R2CatalogClientWrapper({ accountId, token });
}

export function getAvailableClients(): CatalogClient[] {
  const clients: CatalogClient[] = [createIcebergDoClient()];

  const r2Client = createR2CatalogClient();
  if (r2Client) {
    clients.push(r2Client);
  }

  return clients;
}

/**
 * Check if a specific catalog should be benchmarked based on BENCHMARK_CATALOG env var.
 */
export function shouldBenchmarkCatalog(catalogName: string): boolean {
  const envCatalog = process.env.BENCHMARK_CATALOG;
  if (!envCatalog) return true; // Run all if not specified

  const normalizedEnv = envCatalog.toLowerCase();
  const normalizedName = catalogName.toLowerCase();

  if (normalizedEnv === 'iceberg.do' || normalizedEnv === 'iceberg-do') {
    return normalizedName === 'iceberg.do';
  }
  if (normalizedEnv === 'r2' || normalizedEnv === 'r2-data-catalog') {
    return normalizedName === 'r2-data-catalog';
  }

  return true;
}
