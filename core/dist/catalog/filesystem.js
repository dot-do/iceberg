/**
 * Filesystem Catalog Implementation
 *
 * Provides catalog management for Iceberg tables using a filesystem backend.
 *
 * @see https://iceberg.apache.org/spec/
 */
import { createUnpartitionedSpec, } from '../metadata/schema.js';
import { TableMetadataBuilder, } from '../metadata/snapshot.js';
import { parseTableMetadata } from '../metadata/reader.js';
// ============================================================================
// Filesystem Catalog Implementation
// ============================================================================
/**
 * File system based Iceberg catalog.
 *
 * Stores table metadata in JSON files on the underlying storage backend.
 * Uses atomic writes for catalog operations.
 *
 * Directory structure:
 * ```
 * warehouse/
 *   namespace1/
 *     table1/
 *       metadata/
 *         v1.metadata.json
 *         v2.metadata.json
 *         ...
 *       data/
 *         00000-0-<uuid>.parquet
 *         ...
 * ```
 */
export class FileSystemCatalog {
    catalogName;
    warehouse;
    storage;
    defaultProperties;
    constructor(config) {
        this.catalogName = config.name;
        this.warehouse = config.warehouse.replace(/\/$/, ''); // Remove trailing slash
        this.storage = config.storage;
        this.defaultProperties = config.defaultProperties ?? {};
    }
    name() {
        return this.catalogName;
    }
    namespacePath(namespace) {
        return `${this.warehouse}/${namespace.join('/')}`;
    }
    tablePath(identifier) {
        return `${this.namespacePath(identifier.namespace)}/${identifier.name}`;
    }
    metadataPath(identifier) {
        return `${this.tablePath(identifier)}/metadata`;
    }
    async listNamespaces(parent) {
        const basePath = parent ? this.namespacePath(parent) : this.warehouse;
        const files = await this.storage.list(basePath);
        // Extract unique directory names
        const dirs = new Set();
        for (const file of files) {
            const relativePath = file.slice(basePath.length + 1);
            const firstDir = relativePath.split('/')[0];
            if (firstDir && !firstDir.includes('.')) {
                dirs.add(firstDir);
            }
        }
        return Array.from(dirs).map((dir) => (parent ? [...parent, dir] : [dir]));
    }
    async createNamespace(namespace, properties) {
        const path = this.namespacePath(namespace);
        const propsPath = `${path}/.namespace-properties.json`;
        // Check if already exists
        if (await this.storage.exists(propsPath)) {
            throw new Error(`Namespace ${namespace.join('.')} already exists`);
        }
        // Store namespace properties
        const props = properties ?? {};
        await this.storage.put(propsPath, new TextEncoder().encode(JSON.stringify(props, null, 2)));
    }
    async dropNamespace(namespace) {
        const path = this.namespacePath(namespace);
        const propsPath = `${path}/.namespace-properties.json`;
        // Check if namespace exists
        if (!(await this.storage.exists(propsPath))) {
            return false;
        }
        // Check if empty (only has properties file)
        const files = await this.storage.list(path);
        if (files.length > 1) {
            throw new Error(`Namespace ${namespace.join('.')} is not empty`);
        }
        await this.storage.delete(propsPath);
        return true;
    }
    async namespaceExists(namespace) {
        const propsPath = `${this.namespacePath(namespace)}/.namespace-properties.json`;
        return this.storage.exists(propsPath);
    }
    async getNamespaceProperties(namespace) {
        const propsPath = `${this.namespacePath(namespace)}/.namespace-properties.json`;
        const data = await this.storage.get(propsPath);
        if (!data) {
            throw new Error(`Namespace ${namespace.join('.')} does not exist`);
        }
        return JSON.parse(new TextDecoder().decode(data));
    }
    async updateNamespaceProperties(namespace, updates, removals) {
        const propsPath = `${this.namespacePath(namespace)}/.namespace-properties.json`;
        const data = await this.storage.get(propsPath);
        if (!data) {
            throw new Error(`Namespace ${namespace.join('.')} does not exist`);
        }
        const props = JSON.parse(new TextDecoder().decode(data));
        // Apply updates
        for (const [key, value] of Object.entries(updates)) {
            props[key] = value;
        }
        // Apply removals
        for (const key of removals) {
            delete props[key];
        }
        await this.storage.put(propsPath, new TextEncoder().encode(JSON.stringify(props, null, 2)));
    }
    async listTables(namespace) {
        const basePath = this.namespacePath(namespace);
        const files = await this.storage.list(basePath);
        // Find tables by looking for metadata directories
        const tables = new Set();
        for (const file of files) {
            const relativePath = file.slice(basePath.length + 1);
            const parts = relativePath.split('/');
            if (parts.length >= 2 && parts[1] === 'metadata') {
                tables.add(parts[0]);
            }
        }
        return Array.from(tables).map((name) => ({
            namespace,
            name,
        }));
    }
    async createTable(namespace, request) {
        const identifier = { namespace, name: request.name };
        const tablePath = this.tablePath(identifier);
        const metadataDir = this.metadataPath(identifier);
        // Build table metadata
        const builder = new TableMetadataBuilder({
            location: request.location ?? tablePath,
            schema: request.schema,
            partitionSpec: request.partitionSpec ?? createUnpartitionedSpec(),
            properties: {
                ...this.defaultProperties,
                ...request.properties,
            },
        });
        const metadata = builder.build();
        // Write metadata file
        const metadataPath = `${metadataDir}/v1.metadata.json`;
        const metadataContent = new TextEncoder().encode(JSON.stringify(metadata, null, 2));
        // Use putIfAbsent for atomic table creation if available
        if (this.storage.putIfAbsent) {
            const created = await this.storage.putIfAbsent(metadataPath, metadataContent);
            if (!created) {
                throw new Error(`Table ${namespace.join('.')}.${request.name} already exists`);
            }
        }
        else {
            // Fallback: Check if table already exists
            const existingFiles = await this.storage.list(metadataDir);
            if (existingFiles.length > 0) {
                throw new Error(`Table ${namespace.join('.')}.${request.name} already exists`);
            }
            await this.storage.put(metadataPath, metadataContent);
        }
        // Write version hint file
        await this.storage.put(`${metadataDir}/version-hint.text`, new TextEncoder().encode('1'));
        return metadata;
    }
    async loadTable(identifier) {
        const metadataDir = this.metadataPath(identifier);
        // Try to read version hint
        const versionHintPath = `${metadataDir}/version-hint.text`;
        const versionHintData = await this.storage.get(versionHintPath);
        let metadataPath;
        if (versionHintData) {
            const version = new TextDecoder().decode(versionHintData).trim();
            metadataPath = `${metadataDir}/v${version}.metadata.json`;
        }
        else {
            // Fall back to finding the latest metadata file
            const files = await this.storage.list(metadataDir);
            const metadataFiles = files
                .filter((f) => f.endsWith('.metadata.json'))
                .sort()
                .reverse();
            if (metadataFiles.length === 0) {
                throw new Error(`Table ${identifier.namespace.join('.')}.${identifier.name} does not exist`);
            }
            metadataPath = metadataFiles[0];
        }
        const data = await this.storage.get(metadataPath);
        if (!data) {
            throw new Error(`Table ${identifier.namespace.join('.')}.${identifier.name} does not exist`);
        }
        return parseTableMetadata(new TextDecoder().decode(data));
    }
    async tableExists(identifier) {
        try {
            await this.loadTable(identifier);
            return true;
        }
        catch {
            return false;
        }
    }
    async dropTable(identifier, purge = false) {
        const tablePath = this.tablePath(identifier);
        // Check if table exists
        if (!(await this.tableExists(identifier))) {
            return false;
        }
        if (purge) {
            // Delete all files including data
            const files = await this.storage.list(tablePath);
            for (const file of files) {
                await this.storage.delete(file);
            }
        }
        else {
            // Only delete metadata
            const metadataFiles = await this.storage.list(this.metadataPath(identifier));
            for (const file of metadataFiles) {
                await this.storage.delete(file);
            }
        }
        return true;
    }
    async renameTable(from, to) {
        // Load existing metadata
        const metadata = await this.loadTable(from);
        // Update location if it contained the old table name
        const oldPath = this.tablePath(from);
        const newPath = this.tablePath(to);
        const newLocation = metadata.location.replace(oldPath, newPath);
        await this.createTable(to.namespace, {
            name: to.name,
            schema: metadata.schemas[metadata['current-schema-id']] ?? metadata.schemas[0],
            partitionSpec: metadata['partition-specs'].find((s) => s['spec-id'] === metadata['default-spec-id']),
            location: newLocation,
            properties: metadata.properties,
        });
        await this.dropTable(from, false);
    }
    async commitTable(request) {
        const { identifier, requirements, updates } = request;
        // Load current metadata
        const currentMetadata = await this.loadTable(identifier);
        const builder = TableMetadataBuilder.fromMetadata(currentMetadata);
        // Validate requirements
        for (const req of requirements) {
            switch (req.type) {
                case 'assert-table-uuid':
                    if (currentMetadata['table-uuid'] !== req.uuid) {
                        throw new Error(`Table UUID mismatch: expected ${req.uuid}, got ${currentMetadata['table-uuid']}`);
                    }
                    break;
                case 'assert-ref-snapshot-id': {
                    const ref = currentMetadata.refs[req.ref];
                    const currentSnapshotId = ref ? ref['snapshot-id'] : null;
                    if (currentSnapshotId !== req['snapshot-id']) {
                        throw new Error(`Ref ${req.ref} snapshot ID mismatch: expected ${req['snapshot-id']}, got ${currentSnapshotId}`);
                    }
                    break;
                }
                case 'assert-current-schema-id':
                    if (currentMetadata['current-schema-id'] !== req['current-schema-id']) {
                        throw new Error(`Current schema ID mismatch: expected ${req['current-schema-id']}, got ${currentMetadata['current-schema-id']}`);
                    }
                    break;
                case 'assert-default-spec-id':
                    if (currentMetadata['default-spec-id'] !== req['default-spec-id']) {
                        throw new Error(`Default spec ID mismatch: expected ${req['default-spec-id']}, got ${currentMetadata['default-spec-id']}`);
                    }
                    break;
            }
        }
        // Apply updates
        for (const update of updates) {
            switch (update.action) {
                case 'add-schema':
                    builder.addSchema(update.schema);
                    break;
                case 'set-current-schema':
                    builder.setCurrentSchema(update['schema-id']);
                    break;
                case 'add-partition-spec':
                    builder.addPartitionSpec(update.spec);
                    break;
                case 'set-default-spec':
                    builder.setDefaultPartitionSpec(update['spec-id']);
                    break;
                case 'add-snapshot':
                    builder.addSnapshot(update.snapshot);
                    break;
                case 'set-snapshot-ref':
                    builder.addRef(update['ref-name'], {
                        'snapshot-id': update['snapshot-id'],
                        type: update.type,
                    });
                    break;
                case 'set-properties':
                    for (const [key, value] of Object.entries(update.updates)) {
                        builder.setProperty(key, value);
                    }
                    break;
                case 'remove-properties':
                    for (const key of update.removals) {
                        builder.removeProperty(key);
                    }
                    break;
            }
        }
        const newMetadata = builder.build();
        // Determine next version number
        const metadataDir = this.metadataPath(identifier);
        const files = await this.storage.list(metadataDir);
        const versions = files
            .filter((f) => f.match(/v\d+\.metadata\.json$/))
            .map((f) => {
            const match = f.match(/v(\d+)\.metadata\.json$/);
            return match ? parseInt(match[1], 10) : 0;
        });
        const nextVersion = Math.max(0, ...versions) + 1;
        // Write new metadata file using putIfAbsent for atomicity if available
        const metadataPath = `${metadataDir}/v${nextVersion}.metadata.json`;
        const metadataContent = new TextEncoder().encode(JSON.stringify(newMetadata, null, 2));
        if (this.storage.putIfAbsent) {
            const created = await this.storage.putIfAbsent(metadataPath, metadataContent);
            if (!created) {
                throw new Error(`Commit conflict: metadata version ${nextVersion} already exists`);
            }
        }
        else {
            await this.storage.put(metadataPath, metadataContent);
        }
        // Update version hint using compareAndSwap for atomicity if available
        const versionHintPath = `${metadataDir}/version-hint.text`;
        const newVersionHint = new TextEncoder().encode(String(nextVersion));
        if (this.storage.compareAndSwap) {
            const currentVersionHint = await this.storage.get(versionHintPath);
            const success = await this.storage.compareAndSwap(versionHintPath, currentVersionHint, newVersionHint);
            if (!success) {
                // Another commit happened concurrently - we still wrote our metadata file successfully
                // so just update the version hint with put (our metadata is valid)
                await this.storage.put(versionHintPath, newVersionHint);
            }
        }
        else {
            await this.storage.put(versionHintPath, newVersionHint);
        }
        return {
            'metadata-location': metadataPath,
            metadata: newMetadata,
        };
    }
}
// ============================================================================
// Memory Catalog Implementation
// ============================================================================
/**
 * In-memory Iceberg catalog for testing.
 */
export class MemoryCatalog {
    catalogName;
    namespaces = new Map();
    tables = new Map();
    constructor(config) {
        this.catalogName = config.name;
    }
    namespaceKey(namespace) {
        return namespace.join('.');
    }
    tableKey(identifier) {
        return `${this.namespaceKey(identifier.namespace)}.${identifier.name}`;
    }
    name() {
        return this.catalogName;
    }
    async listNamespaces(parent) {
        const parentKey = parent ? this.namespaceKey(parent) + '.' : '';
        const result = [];
        for (const key of this.namespaces.keys()) {
            if (key.startsWith(parentKey)) {
                const rest = key.slice(parentKey.length);
                const parts = rest.split('.');
                if (parts.length === 1) {
                    result.push(parent ? [...parent, parts[0]] : [parts[0]]);
                }
            }
        }
        return result;
    }
    async createNamespace(namespace, properties) {
        const key = this.namespaceKey(namespace);
        if (this.namespaces.has(key)) {
            throw new Error(`Namespace ${key} already exists`);
        }
        this.namespaces.set(key, properties ?? {});
    }
    async dropNamespace(namespace) {
        const key = this.namespaceKey(namespace);
        if (!this.namespaces.has(key)) {
            return false;
        }
        // Check if empty
        for (const tableKey of this.tables.keys()) {
            if (tableKey.startsWith(key + '.')) {
                throw new Error(`Namespace ${key} is not empty`);
            }
        }
        this.namespaces.delete(key);
        return true;
    }
    async namespaceExists(namespace) {
        return this.namespaces.has(this.namespaceKey(namespace));
    }
    async getNamespaceProperties(namespace) {
        const key = this.namespaceKey(namespace);
        const props = this.namespaces.get(key);
        if (!props) {
            throw new Error(`Namespace ${key} does not exist`);
        }
        return { ...props };
    }
    async updateNamespaceProperties(namespace, updates, removals) {
        const key = this.namespaceKey(namespace);
        const props = this.namespaces.get(key);
        if (!props) {
            throw new Error(`Namespace ${key} does not exist`);
        }
        for (const [k, v] of Object.entries(updates)) {
            props[k] = v;
        }
        for (const k of removals) {
            delete props[k];
        }
    }
    async listTables(namespace) {
        const prefix = this.namespaceKey(namespace) + '.';
        const result = [];
        for (const key of this.tables.keys()) {
            if (key.startsWith(prefix)) {
                const name = key.slice(prefix.length);
                if (!name.includes('.')) {
                    result.push({ namespace, name });
                }
            }
        }
        return result;
    }
    async createTable(namespace, request) {
        const identifier = { namespace, name: request.name };
        const key = this.tableKey(identifier);
        if (this.tables.has(key)) {
            throw new Error(`Table ${key} already exists`);
        }
        const builder = new TableMetadataBuilder({
            location: request.location ?? `memory://${key}`,
            schema: request.schema,
            partitionSpec: request.partitionSpec ?? createUnpartitionedSpec(),
            properties: request.properties,
        });
        const metadata = builder.build();
        this.tables.set(key, metadata);
        return metadata;
    }
    async loadTable(identifier) {
        const key = this.tableKey(identifier);
        const metadata = this.tables.get(key);
        if (!metadata) {
            throw new Error(`Table ${key} does not exist`);
        }
        return { ...metadata };
    }
    async tableExists(identifier) {
        return this.tables.has(this.tableKey(identifier));
    }
    async dropTable(identifier, _purge) {
        const key = this.tableKey(identifier);
        if (!this.tables.has(key)) {
            return false;
        }
        this.tables.delete(key);
        return true;
    }
    async renameTable(from, to) {
        const fromKey = this.tableKey(from);
        const toKey = this.tableKey(to);
        const metadata = this.tables.get(fromKey);
        if (!metadata) {
            throw new Error(`Table ${fromKey} does not exist`);
        }
        if (this.tables.has(toKey)) {
            throw new Error(`Table ${toKey} already exists`);
        }
        this.tables.delete(fromKey);
        this.tables.set(toKey, metadata);
    }
    async commitTable(request) {
        const key = this.tableKey(request.identifier);
        const currentMetadata = this.tables.get(key);
        if (!currentMetadata) {
            throw new Error(`Table ${key} does not exist`);
        }
        const builder = TableMetadataBuilder.fromMetadata(currentMetadata);
        // Apply updates (simplified version)
        for (const update of request.updates) {
            switch (update.action) {
                case 'add-snapshot':
                    builder.addSnapshot(update.snapshot);
                    break;
                case 'set-properties':
                    for (const [k, v] of Object.entries(update.updates)) {
                        builder.setProperty(k, v);
                    }
                    break;
            }
        }
        const newMetadata = builder.build();
        this.tables.set(key, newMetadata);
        return {
            'metadata-location': `memory://${key}/metadata`,
            metadata: newMetadata,
        };
    }
    /**
     * Clear all data (for testing).
     */
    clear() {
        this.namespaces.clear();
        this.tables.clear();
    }
}
// ============================================================================
// Factory Function
// ============================================================================
/**
 * Create an Iceberg catalog from configuration.
 */
export function createCatalog(config) {
    switch (config.type) {
        case 'filesystem':
            if (!config.warehouse || !config.storage) {
                throw new Error('Filesystem catalog requires warehouse and storage');
            }
            return new FileSystemCatalog({
                name: config.name,
                warehouse: config.warehouse,
                storage: config.storage,
                defaultProperties: config.defaultProperties,
            });
        case 'memory':
            return new MemoryCatalog({ name: config.name });
        case 'rest':
            throw new Error('REST catalog not yet implemented');
        default:
            throw new Error(`Unknown catalog type: ${config.type}`);
    }
}
//# sourceMappingURL=filesystem.js.map