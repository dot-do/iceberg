/**
 * Tests for RBAC and FGA
 *
 * Tests role-based access control and fine-grained authorization.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import type { AuthContext } from '../src/auth/middleware.js';
import { PermissionLevel, type PermissionGrant } from '../src/auth/permissions.js';
import {
  // RBAC types and classes
  type Action,
  type ResourceType,
  type RoleName,
  type Resource,
  ROLE_DEFINITIONS,
  RBACManager,
  InMemoryPermissionStore,
  // Resource helpers
  namespaceResource,
  tableResource,
  resourceToId,
  parseResourceId,
  // Conversion helpers
  actionToPermissionLevel,
  roleToPermissionLevel,
  permissionLevelToRole,
} from '../src/auth/rbac.js';
import {
  // FGA types and classes
  type PolicyRule,
  type PolicyContext,
  FGAEngine,
  createFGAEngine,
  // Authorization helpers
  canAccessNamespace,
  canAccessTable,
  requireNamespaceAccess,
  requireTableAccess,
} from '../src/auth/fga.js';

// ============================================================================
// Test Helpers
// ============================================================================

function createAuthContext(overrides: Partial<AuthContext> = {}): AuthContext {
  return {
    authenticated: true,
    userId: 'user-123',
    email: 'test@example.com',
    roles: [],
    ...overrides,
  };
}

function createOwnerContext(): AuthContext {
  return createAuthContext({ roles: ['owner'] });
}

function createAdminContext(): AuthContext {
  return createAuthContext({ roles: ['admin'] });
}

function createWriteContext(): AuthContext {
  return createAuthContext({ roles: ['write'] });
}

function createReadContext(): AuthContext {
  return createAuthContext({ roles: ['read'] });
}

function createUnauthenticatedContext(): AuthContext {
  return { authenticated: false, roles: [] };
}

// ============================================================================
// Role Definitions Tests
// ============================================================================

describe('Role Definitions', () => {
  it('should define all standard roles', () => {
    expect(ROLE_DEFINITIONS.viewer).toBeDefined();
    expect(ROLE_DEFINITIONS.editor).toBeDefined();
    expect(ROLE_DEFINITIONS.admin).toBeDefined();
    expect(ROLE_DEFINITIONS.owner).toBeDefined();
  });

  it('should have correct permission levels for roles', () => {
    expect(ROLE_DEFINITIONS.viewer.level).toBe(PermissionLevel.READ);
    expect(ROLE_DEFINITIONS.editor.level).toBe(PermissionLevel.WRITE);
    expect(ROLE_DEFINITIONS.admin.level).toBe(PermissionLevel.ADMIN);
    expect(ROLE_DEFINITIONS.owner.level).toBe(PermissionLevel.OWNER);
  });

  it('should have names matching keys', () => {
    for (const [key, definition] of Object.entries(ROLE_DEFINITIONS)) {
      expect(definition.name).toBe(key);
    }
  });
});

// ============================================================================
// Resource Helpers Tests
// ============================================================================

describe('Resource Helpers', () => {
  describe('namespaceResource', () => {
    it('should create a namespace resource', () => {
      const resource = namespaceResource(['db', 'schema']);
      expect(resource.type).toBe('namespace');
      expect(resource.namespace).toEqual(['db', 'schema']);
      expect(resource.tableName).toBeUndefined();
    });
  });

  describe('tableResource', () => {
    it('should create a table resource', () => {
      const resource = tableResource(['db', 'schema'], 'users');
      expect(resource.type).toBe('table');
      expect(resource.namespace).toEqual(['db', 'schema']);
      expect(resource.tableName).toBe('users');
    });
  });

  describe('resourceToId', () => {
    it('should convert namespace resource to ID', () => {
      const resource = namespaceResource(['db', 'schema']);
      const id = resourceToId(resource);
      expect(id).toBe('db\x1fschema');
    });

    it('should convert table resource to ID', () => {
      const resource = tableResource(['db'], 'users');
      const id = resourceToId(resource);
      expect(id).toBe('db\x00users');
    });

    it('should handle nested namespaces for tables', () => {
      const resource = tableResource(['db', 'schema'], 'users');
      const id = resourceToId(resource);
      expect(id).toBe('db\x1fschema\x00users');
    });
  });

  describe('parseResourceId', () => {
    it('should parse namespace resource ID', () => {
      const resource = parseResourceId('namespace', 'db\x1fschema');
      expect(resource.type).toBe('namespace');
      expect(resource.namespace).toEqual(['db', 'schema']);
    });

    it('should parse table resource ID', () => {
      const resource = parseResourceId('table', 'db\x00users');
      expect(resource.type).toBe('table');
      expect(resource.namespace).toEqual(['db']);
      expect(resource.tableName).toBe('users');
    });
  });
});

// ============================================================================
// Conversion Helpers Tests
// ============================================================================

describe('Conversion Helpers', () => {
  describe('actionToPermissionLevel', () => {
    it('should convert actions to permission levels', () => {
      expect(actionToPermissionLevel('read')).toBe(PermissionLevel.READ);
      expect(actionToPermissionLevel('write')).toBe(PermissionLevel.WRITE);
      expect(actionToPermissionLevel('admin')).toBe(PermissionLevel.ADMIN);
      expect(actionToPermissionLevel('owner')).toBe(PermissionLevel.OWNER);
    });
  });

  describe('roleToPermissionLevel', () => {
    it('should convert roles to permission levels', () => {
      expect(roleToPermissionLevel('viewer')).toBe(PermissionLevel.READ);
      expect(roleToPermissionLevel('editor')).toBe(PermissionLevel.WRITE);
      expect(roleToPermissionLevel('admin')).toBe(PermissionLevel.ADMIN);
      expect(roleToPermissionLevel('owner')).toBe(PermissionLevel.OWNER);
    });
  });

  describe('permissionLevelToRole', () => {
    it('should convert permission levels to roles', () => {
      expect(permissionLevelToRole(PermissionLevel.READ)).toBe('viewer');
      expect(permissionLevelToRole(PermissionLevel.WRITE)).toBe('editor');
      expect(permissionLevelToRole(PermissionLevel.ADMIN)).toBe('admin');
      expect(permissionLevelToRole(PermissionLevel.OWNER)).toBe('owner');
    });

    it('should default to viewer for NONE', () => {
      expect(permissionLevelToRole(PermissionLevel.NONE)).toBe('viewer');
    });
  });
});

// ============================================================================
// InMemoryPermissionStore Tests
// ============================================================================

describe('InMemoryPermissionStore', () => {
  let store: InMemoryPermissionStore;

  beforeEach(() => {
    store = new InMemoryPermissionStore();
  });

  it('should create and retrieve grants', async () => {
    const grant: PermissionGrant = {
      id: 'grant-1',
      resourceType: 'namespace',
      resourceId: 'db',
      principalType: 'user',
      principalId: 'user-123',
      level: PermissionLevel.READ,
      createdAt: Date.now(),
      createdBy: 'admin',
    };

    await store.createGrant(grant);
    const retrieved = await store.getGrant('grant-1');

    expect(retrieved).toEqual(grant);
  });

  it('should get grants for principal', async () => {
    const grant1: PermissionGrant = {
      id: 'grant-1',
      resourceType: 'namespace',
      resourceId: 'db1',
      principalType: 'user',
      principalId: 'user-123',
      level: PermissionLevel.READ,
      createdAt: Date.now(),
      createdBy: 'admin',
    };
    const grant2: PermissionGrant = {
      id: 'grant-2',
      resourceType: 'namespace',
      resourceId: 'db2',
      principalType: 'user',
      principalId: 'user-123',
      level: PermissionLevel.WRITE,
      createdAt: Date.now(),
      createdBy: 'admin',
    };
    const grant3: PermissionGrant = {
      id: 'grant-3',
      resourceType: 'namespace',
      resourceId: 'db3',
      principalType: 'user',
      principalId: 'user-456',
      level: PermissionLevel.READ,
      createdAt: Date.now(),
      createdBy: 'admin',
    };

    await store.createGrant(grant1);
    await store.createGrant(grant2);
    await store.createGrant(grant3);

    const grants = await store.getGrantsForPrincipal('user', 'user-123');
    expect(grants).toHaveLength(2);
    expect(grants.map(g => g.id).sort()).toEqual(['grant-1', 'grant-2']);
  });

  it('should get grants for resource', async () => {
    const grant1: PermissionGrant = {
      id: 'grant-1',
      resourceType: 'namespace',
      resourceId: 'db',
      principalType: 'user',
      principalId: 'user-123',
      level: PermissionLevel.READ,
      createdAt: Date.now(),
      createdBy: 'admin',
    };
    const grant2: PermissionGrant = {
      id: 'grant-2',
      resourceType: 'namespace',
      resourceId: 'db',
      principalType: 'role',
      principalId: 'developers',
      level: PermissionLevel.WRITE,
      createdAt: Date.now(),
      createdBy: 'admin',
    };

    await store.createGrant(grant1);
    await store.createGrant(grant2);

    const grants = await store.getGrantsForResource('namespace', 'db');
    expect(grants).toHaveLength(2);
  });

  it('should delete grants', async () => {
    const grant: PermissionGrant = {
      id: 'grant-1',
      resourceType: 'namespace',
      resourceId: 'db',
      principalType: 'user',
      principalId: 'user-123',
      level: PermissionLevel.READ,
      createdAt: Date.now(),
      createdBy: 'admin',
    };

    await store.createGrant(grant);
    await store.deleteGrant('grant-1');

    const retrieved = await store.getGrant('grant-1');
    expect(retrieved).toBeNull();
  });

  it('should delete all grants for a resource', async () => {
    const grant1: PermissionGrant = {
      id: 'grant-1',
      resourceType: 'namespace',
      resourceId: 'db',
      principalType: 'user',
      principalId: 'user-123',
      level: PermissionLevel.READ,
      createdAt: Date.now(),
      createdBy: 'admin',
    };
    const grant2: PermissionGrant = {
      id: 'grant-2',
      resourceType: 'namespace',
      resourceId: 'db',
      principalType: 'user',
      principalId: 'user-456',
      level: PermissionLevel.WRITE,
      createdAt: Date.now(),
      createdBy: 'admin',
    };
    const grant3: PermissionGrant = {
      id: 'grant-3',
      resourceType: 'namespace',
      resourceId: 'other',
      principalType: 'user',
      principalId: 'user-123',
      level: PermissionLevel.READ,
      createdAt: Date.now(),
      createdBy: 'admin',
    };

    await store.createGrant(grant1);
    await store.createGrant(grant2);
    await store.createGrant(grant3);

    await store.deleteGrantsForResource('namespace', 'db');

    const allGrants = store.getAllGrants();
    expect(allGrants).toHaveLength(1);
    expect(allGrants[0].id).toBe('grant-3');
  });
});

// ============================================================================
// RBACManager Tests
// ============================================================================

describe('RBACManager', () => {
  let store: InMemoryPermissionStore;
  let rbac: RBACManager;

  beforeEach(() => {
    store = new InMemoryPermissionStore();
    rbac = new RBACManager(store);
  });

  describe('checkPermission', () => {
    it('should deny unauthenticated users', async () => {
      const user = createUnauthenticatedContext();
      const resource = namespaceResource(['db']);

      const result = await rbac.checkPermission(user, resource, 'read');

      expect(result.allowed).toBe(false);
      expect(result.reason).toContain('not authenticated');
    });

    it('should allow owner role for all actions', async () => {
      const user = createOwnerContext();
      const resource = namespaceResource(['db']);

      const readResult = await rbac.checkPermission(user, resource, 'read');
      const ownerResult = await rbac.checkPermission(user, resource, 'owner');

      expect(readResult.allowed).toBe(true);
      expect(readResult.source).toBe('role');
      expect(ownerResult.allowed).toBe(true);
    });

    it('should allow admin role for admin actions', async () => {
      const user = createAdminContext();
      const resource = namespaceResource(['db']);

      const adminResult = await rbac.checkPermission(user, resource, 'admin');
      expect(adminResult.allowed).toBe(true);
      expect(adminResult.source).toBe('role');
    });

    it('should check catalog-level roles', async () => {
      const user = createWriteContext();
      const resource = namespaceResource(['db']);

      const writeResult = await rbac.checkPermission(user, resource, 'write');
      const adminResult = await rbac.checkPermission(user, resource, 'admin');

      expect(writeResult.allowed).toBe(true);
      expect(writeResult.source).toBe('catalog');
      expect(adminResult.allowed).toBe(false);
    });

    it('should check direct resource grants', async () => {
      const user = createAuthContext({ userId: 'user-123', roles: [] });
      const resource = namespaceResource(['db']);

      // Grant write access
      const grant: PermissionGrant = {
        id: 'grant-1',
        resourceType: 'namespace',
        resourceId: 'db',
        principalType: 'user',
        principalId: 'user-123',
        level: PermissionLevel.WRITE,
        createdAt: Date.now(),
        createdBy: 'admin',
      };
      await store.createGrant(grant);

      const writeResult = await rbac.checkPermission(user, resource, 'write');
      const adminResult = await rbac.checkPermission(user, resource, 'admin');

      expect(writeResult.allowed).toBe(true);
      expect(writeResult.source).toBe('direct');
      expect(adminResult.allowed).toBe(false);
    });

    it('should inherit permissions from namespace to table', async () => {
      const user = createAuthContext({ userId: 'user-123', roles: [] });
      const resource = tableResource(['db'], 'users');

      // Grant admin access to namespace
      const grant: PermissionGrant = {
        id: 'grant-1',
        resourceType: 'namespace',
        resourceId: 'db',
        principalType: 'user',
        principalId: 'user-123',
        level: PermissionLevel.ADMIN,
        createdAt: Date.now(),
        createdBy: 'admin',
      };
      await store.createGrant(grant);

      const result = await rbac.checkPermission(user, resource, 'write');

      expect(result.allowed).toBe(true);
      expect(result.source).toBe('inherited');
    });

    it('should inherit permissions from parent namespace', async () => {
      const user = createAuthContext({ userId: 'user-123', roles: [] });
      const resource = namespaceResource(['parent', 'child']);

      // Grant admin access to parent namespace
      const grant: PermissionGrant = {
        id: 'grant-1',
        resourceType: 'namespace',
        resourceId: 'parent',
        principalType: 'user',
        principalId: 'user-123',
        level: PermissionLevel.WRITE,
        createdAt: Date.now(),
        createdBy: 'admin',
      };
      await store.createGrant(grant);

      const result = await rbac.checkPermission(user, resource, 'write');

      expect(result.allowed).toBe(true);
      expect(result.source).toBe('inherited');
    });

    it('should check role grants', async () => {
      const user = createAuthContext({ userId: 'user-123', roles: ['developers'] });
      const resource = namespaceResource(['db']);

      // Grant write access to developers role
      const grant: PermissionGrant = {
        id: 'grant-1',
        resourceType: 'namespace',
        resourceId: 'db',
        principalType: 'role',
        principalId: 'developers',
        level: PermissionLevel.WRITE,
        createdAt: Date.now(),
        createdBy: 'admin',
      };
      await store.createGrant(grant);

      const result = await rbac.checkPermission(user, resource, 'write');

      expect(result.allowed).toBe(true);
      expect(result.source).toBe('direct');
    });
  });

  describe('grantPermission', () => {
    it('should allow owner to grant permissions', async () => {
      const granter = createOwnerContext();
      const resource = namespaceResource(['db']);

      const result = await rbac.grantPermission(
        granter,
        { type: 'user', id: 'user-456' },
        resource,
        'editor'
      );

      expect(result.success).toBe(true);
      expect(result.grant).toBeDefined();
      expect(result.grant?.level).toBe(PermissionLevel.WRITE);
    });

    it('should deny non-owner from granting permissions', async () => {
      const granter = createWriteContext();
      const resource = namespaceResource(['db']);

      const result = await rbac.grantPermission(
        granter,
        { type: 'user', id: 'user-456' },
        resource,
        'editor'
      );

      expect(result.success).toBe(false);
      expect(result.error).toContain('owner permission');
    });

    it('should reject invalid role names', async () => {
      const granter = createOwnerContext();
      const resource = namespaceResource(['db']);

      const result = await rbac.grantPermission(
        granter,
        { type: 'user', id: 'user-456' },
        resource,
        'invalid-role' as RoleName
      );

      expect(result.success).toBe(false);
      expect(result.error).toContain('Invalid role');
    });

    it('should allow granting to roles', async () => {
      const granter = createOwnerContext();
      const resource = namespaceResource(['db']);

      const result = await rbac.grantPermission(
        granter,
        { type: 'role', id: 'developers' },
        resource,
        'editor'
      );

      expect(result.success).toBe(true);
      expect(result.grant?.principalType).toBe('role');
      expect(result.grant?.principalId).toBe('developers');
    });
  });

  describe('revokePermission', () => {
    it('should allow owner to revoke permissions', async () => {
      const granter = createOwnerContext();
      const resource = namespaceResource(['db']);

      // First grant permission
      await rbac.grantPermission(
        granter,
        { type: 'user', id: 'user-456' },
        resource,
        'editor'
      );

      // Then revoke
      const result = await rbac.revokePermission(
        granter,
        { type: 'user', id: 'user-456' },
        resource
      );

      expect(result.success).toBe(true);
      expect(result.revokedCount).toBe(1);
    });

    it('should deny non-owner from revoking permissions', async () => {
      const granter = createOwnerContext();
      const revoker = createWriteContext();
      const resource = namespaceResource(['db']);

      // First grant permission as owner
      await rbac.grantPermission(
        granter,
        { type: 'user', id: 'user-456' },
        resource,
        'editor'
      );

      // Try to revoke as non-owner
      const result = await rbac.revokePermission(
        revoker,
        { type: 'user', id: 'user-456' },
        resource
      );

      expect(result.success).toBe(false);
      expect(result.error).toContain('owner permission');
    });

    it('should report zero revoked when no grants exist', async () => {
      const revoker = createOwnerContext();
      const resource = namespaceResource(['db']);

      const result = await rbac.revokePermission(
        revoker,
        { type: 'user', id: 'user-456' },
        resource
      );

      expect(result.success).toBe(true);
      expect(result.revokedCount).toBe(0);
    });
  });

  describe('getResourceGrants', () => {
    it('should return all grants for a resource', async () => {
      const granter = createOwnerContext();
      const resource = namespaceResource(['db']);

      await rbac.grantPermission(granter, { type: 'user', id: 'user-1' }, resource, 'viewer');
      await rbac.grantPermission(granter, { type: 'user', id: 'user-2' }, resource, 'editor');

      const grants = await rbac.getResourceGrants(resource);

      expect(grants).toHaveLength(2);
    });
  });

  describe('getUserGrants', () => {
    it('should return all grants for a user', async () => {
      const granter = createOwnerContext();

      await rbac.grantPermission(
        granter,
        { type: 'user', id: 'user-123' },
        namespaceResource(['db1']),
        'viewer'
      );
      await rbac.grantPermission(
        granter,
        { type: 'user', id: 'user-123' },
        namespaceResource(['db2']),
        'editor'
      );

      const grants = await rbac.getUserGrants('user-123');

      expect(grants).toHaveLength(2);
    });
  });
});

// ============================================================================
// FGAEngine Tests
// ============================================================================

describe('FGAEngine', () => {
  let store: InMemoryPermissionStore;
  let engine: FGAEngine;

  beforeEach(() => {
    store = new InMemoryPermissionStore();
    engine = createFGAEngine(store);
  });

  describe('evaluate', () => {
    it('should deny unauthenticated users', async () => {
      const context: PolicyContext = {
        user: createUnauthenticatedContext(),
        resource: namespaceResource(['db']),
        action: 'read',
      };

      const result = await engine.evaluate(context);

      expect(result.allowed).toBe(false);
      expect(result.source).toBe('default');
    });

    it('should allow based on RBAC grants', async () => {
      const user = createAuthContext({ userId: 'user-123', roles: [] });

      // Grant access
      const grant: PermissionGrant = {
        id: 'grant-1',
        resourceType: 'namespace',
        resourceId: 'db',
        principalType: 'user',
        principalId: 'user-123',
        level: PermissionLevel.READ,
        createdAt: Date.now(),
        createdBy: 'admin',
      };
      await store.createGrant(grant);

      const context: PolicyContext = {
        user,
        resource: namespaceResource(['db']),
        action: 'read',
      };

      const result = await engine.evaluate(context);

      expect(result.allowed).toBe(true);
      expect(result.source).toBe('grant');
    });

    it('should deny based on deny policies', async () => {
      const user = createOwnerContext();

      // Add a deny policy
      engine.addPolicy({
        id: 'deny-all',
        name: 'Deny All',
        resourceType: '*',
        requiredLevel: PermissionLevel.NONE,
        deny: true,
        priority: 100,
      });

      const context: PolicyContext = {
        user,
        resource: namespaceResource(['db']),
        action: 'read',
      };

      const result = await engine.evaluate(context);

      expect(result.allowed).toBe(false);
      expect(result.source).toBe('policy');
    });

    it('should allow based on allow policies', async () => {
      const user = createAuthContext({ userId: 'user-123', roles: [] });

      // Add an allow policy
      engine.addPolicy({
        id: 'allow-all-read',
        name: 'Allow All Read',
        resourceType: 'namespace',
        requiredLevel: PermissionLevel.READ,
        priority: 50,
      });

      const context: PolicyContext = {
        user,
        resource: namespaceResource(['db']),
        action: 'read',
      };

      const result = await engine.evaluate(context);

      expect(result.allowed).toBe(true);
      expect(result.source).toBe('policy');
    });

    it('should evaluate time-based conditions', async () => {
      const user = createAuthContext({ userId: 'user-123', roles: [] });
      const now = Date.now();

      // Add a policy with time condition
      engine.addPolicy({
        id: 'time-limited',
        name: 'Time Limited Access',
        resourceType: 'namespace',
        requiredLevel: PermissionLevel.READ,
        conditions: [
          {
            type: 'time-range',
            params: {
              start: now - 3600000, // 1 hour ago
              end: now + 3600000,   // 1 hour from now
            },
          },
        ],
      });

      const context: PolicyContext = {
        user,
        resource: namespaceResource(['db']),
        action: 'read',
        timestamp: now,
      };

      const result = await engine.evaluate(context);

      expect(result.allowed).toBe(true);
    });

    it('should deny when time condition is not met', async () => {
      const user = createAuthContext({ userId: 'user-123', roles: [] });
      const now = Date.now();

      // Add a policy with expired time condition
      engine.addPolicy({
        id: 'time-limited',
        name: 'Time Limited Access',
        resourceType: 'namespace',
        requiredLevel: PermissionLevel.READ,
        conditions: [
          {
            type: 'time-range',
            params: {
              start: now - 7200000, // 2 hours ago
              end: now - 3600000,   // 1 hour ago (expired)
            },
          },
        ],
      });

      const context: PolicyContext = {
        user,
        resource: namespaceResource(['db']),
        action: 'read',
        timestamp: now,
      };

      const result = await engine.evaluate(context);

      expect(result.allowed).toBe(false);
    });
  });

  describe('checkNamespaceAccess', () => {
    it('should check namespace read access', async () => {
      const user = createReadContext();

      const result = await engine.checkNamespaceAccess(user, ['db'], 'namespace:read');

      expect(result.allowed).toBe(true);
    });

    it('should deny namespace create for read-only user', async () => {
      const user = createReadContext();

      const result = await engine.checkNamespaceAccess(user, ['db'], 'namespace:create');

      expect(result.allowed).toBe(false);
    });

    it('should allow namespace drop for owner', async () => {
      const user = createOwnerContext();

      const result = await engine.checkNamespaceAccess(user, ['db'], 'namespace:drop');

      expect(result.allowed).toBe(true);
    });
  });

  describe('checkTableAccess', () => {
    it('should check table read access', async () => {
      const user = createReadContext();

      const result = await engine.checkTableAccess(user, ['db'], 'users', 'table:read');

      expect(result.allowed).toBe(true);
    });

    it('should check table write access', async () => {
      const user = createWriteContext();

      const result = await engine.checkTableAccess(user, ['db'], 'users', 'table:write');

      expect(result.allowed).toBe(true);
    });

    it('should deny table drop for write-only user', async () => {
      const user = createWriteContext();

      const result = await engine.checkTableAccess(user, ['db'], 'users', 'table:drop');

      expect(result.allowed).toBe(false);
    });

    it('should allow table drop for admin', async () => {
      const user = createAdminContext();

      const result = await engine.checkTableAccess(user, ['db'], 'users', 'table:drop');

      // Admin has full access in catalog-level roles
      expect(result.allowed).toBe(true);
    });

    it('should inherit table access from namespace', async () => {
      const user = createAuthContext({ userId: 'user-123', roles: [] });

      // Grant write access to namespace
      const grant: PermissionGrant = {
        id: 'grant-1',
        resourceType: 'namespace',
        resourceId: 'db',
        principalType: 'user',
        principalId: 'user-123',
        level: PermissionLevel.WRITE,
        createdAt: Date.now(),
        createdBy: 'admin',
      };
      await store.createGrant(grant);

      const result = await engine.checkTableAccess(user, ['db'], 'users', 'table:write');

      expect(result.allowed).toBe(true);
    });
  });

  describe('getEffectivePermissions', () => {
    it('should return effective permissions for a user', async () => {
      const user = createAuthContext({ userId: 'user-123', roles: ['write'] });
      const resource = namespaceResource(['db']);

      const result = await engine.getEffectivePermissions(user, resource);

      expect(result.level).toBe(PermissionLevel.WRITE);
      expect(result.source).toContain('role');
    });

    it('should prefer direct grants over role', async () => {
      const user = createAuthContext({ userId: 'user-123', roles: ['read'] });
      const resource = namespaceResource(['db']);

      // Grant admin access
      const grant: PermissionGrant = {
        id: 'grant-1',
        resourceType: 'namespace',
        resourceId: 'db',
        principalType: 'user',
        principalId: 'user-123',
        level: PermissionLevel.ADMIN,
        createdAt: Date.now(),
        createdBy: 'admin',
      };
      await store.createGrant(grant);

      const result = await engine.getEffectivePermissions(user, resource);

      expect(result.level).toBe(PermissionLevel.ADMIN);
      expect(result.source).toBe('grant:direct');
    });
  });

  describe('listAccessibleResources', () => {
    it('should list accessible namespaces', async () => {
      const user = createAuthContext({ userId: 'user-123', roles: [] });

      // Grant access to two namespaces
      await store.createGrant({
        id: 'grant-1',
        resourceType: 'namespace',
        resourceId: 'db1',
        principalType: 'user',
        principalId: 'user-123',
        level: PermissionLevel.READ,
        createdAt: Date.now(),
        createdBy: 'admin',
      });
      await store.createGrant({
        id: 'grant-2',
        resourceType: 'namespace',
        resourceId: 'db2',
        principalType: 'user',
        principalId: 'user-123',
        level: PermissionLevel.READ,
        createdAt: Date.now(),
        createdBy: 'admin',
      });

      const resources = await engine.listAccessibleResources(user, 'namespace', 'read');

      expect(resources).toHaveLength(2);
      expect(resources.map(r => r.namespace[0]).sort()).toEqual(['db1', 'db2']);
    });
  });

  describe('policy management', () => {
    it('should add and remove policies', () => {
      const policy: PolicyRule = {
        id: 'test-policy',
        name: 'Test Policy',
        resourceType: 'namespace',
        requiredLevel: PermissionLevel.READ,
      };

      engine.addPolicy(policy);
      expect(engine.getPolicies()).toHaveLength(1);

      engine.removePolicy('test-policy');
      expect(engine.getPolicies()).toHaveLength(0);
    });
  });
});

// ============================================================================
// Authorization Helper Tests
// ============================================================================

describe('Authorization Helpers', () => {
  let store: InMemoryPermissionStore;
  let engine: FGAEngine;

  beforeEach(() => {
    store = new InMemoryPermissionStore();
    engine = createFGAEngine(store);
  });

  describe('canAccessNamespace', () => {
    it('should return true for allowed access', async () => {
      const user = createReadContext();

      const result = await canAccessNamespace(engine, user, ['db'], 'namespace:read');

      expect(result).toBe(true);
    });

    it('should return false for denied access', async () => {
      const user = createReadContext();

      const result = await canAccessNamespace(engine, user, ['db'], 'namespace:drop');

      expect(result).toBe(false);
    });
  });

  describe('canAccessTable', () => {
    it('should return true for allowed access', async () => {
      const user = createWriteContext();

      const result = await canAccessTable(engine, user, ['db'], 'users', 'table:write');

      expect(result).toBe(true);
    });

    it('should return false for denied access', async () => {
      const user = createReadContext();

      const result = await canAccessTable(engine, user, ['db'], 'users', 'table:write');

      expect(result).toBe(false);
    });
  });

  describe('requireNamespaceAccess', () => {
    it('should not throw for allowed access', async () => {
      const user = createReadContext();

      await expect(
        requireNamespaceAccess(engine, user, ['db'], 'namespace:read')
      ).resolves.toBeUndefined();
    });

    it('should throw for denied access', async () => {
      const user = createReadContext();

      await expect(
        requireNamespaceAccess(engine, user, ['db'], 'namespace:drop')
      ).rejects.toThrow('Access denied');
    });
  });

  describe('requireTableAccess', () => {
    it('should not throw for allowed access', async () => {
      const user = createWriteContext();

      await expect(
        requireTableAccess(engine, user, ['db'], 'users', 'table:write')
      ).resolves.toBeUndefined();
    });

    it('should throw for denied access', async () => {
      const user = createReadContext();

      await expect(
        requireTableAccess(engine, user, ['db'], 'users', 'table:write')
      ).rejects.toThrow('Access denied');
    });
  });
});
