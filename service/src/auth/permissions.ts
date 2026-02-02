/**
 * Fine-Grained Authorization (FGA) and Role-Based Access Control (RBAC)
 *
 * Implements permission checking for Iceberg catalog operations with:
 * - Role-based access control (read, write, admin, owner)
 * - Namespace-level permissions
 * - Table-level permissions
 * - Permission inheritance (namespace -> table)
 * - Integration with oauth.do user context
 */

import type { AuthContext } from './middleware.js';

// ============================================================================
// Permission Types
// ============================================================================

/**
 * Base permission levels in order of increasing privilege.
 * Higher levels inherit all permissions from lower levels.
 */
export enum PermissionLevel {
  /** No access */
  NONE = 0,
  /** Read-only access: can view metadata and read data */
  READ = 1,
  /** Write access: can modify data (includes READ) */
  WRITE = 2,
  /** Admin access: can modify schema and settings (includes WRITE) */
  ADMIN = 3,
  /** Owner access: full control including delete and transfer ownership (includes ADMIN) */
  OWNER = 4,
}

/** String representation of permission levels */
export type PermissionLevelString = 'none' | 'read' | 'write' | 'admin' | 'owner';

/** Namespace-level operation permissions */
export type NamespacePermission =
  | 'namespace:list'
  | 'namespace:read'
  | 'namespace:create'
  | 'namespace:update'
  | 'namespace:drop'
  | 'namespace:grant'
  | 'namespace:revoke';

/** Table-level operation permissions */
export type TablePermission =
  | 'table:list'
  | 'table:read'
  | 'table:create'
  | 'table:write'
  | 'table:commit'
  | 'table:drop'
  | 'table:rename'
  | 'table:grant'
  | 'table:revoke';

/** All permission types */
export type Permission = NamespacePermission | TablePermission;

// ============================================================================
// Permission Grant Types
// ============================================================================

/**
 * A permission grant associates a principal (user/role) with a permission level
 * on a specific resource (namespace or table).
 */
export interface PermissionGrant {
  /** Unique grant ID */
  id: string;
  /** Resource type */
  resourceType: 'namespace' | 'table';
  /** Resource identifier (namespace path or namespace.table) */
  resourceId: string;
  /** Principal type */
  principalType: 'user' | 'role' | 'group';
  /** Principal identifier */
  principalId: string;
  /** Permission level granted */
  level: PermissionLevel;
  /** Grant created timestamp */
  createdAt: number;
  /** Grant created by */
  createdBy: string;
  /** Grant expiration (optional) */
  expiresAt?: number;
}

/**
 * Effective permission for a principal on a resource.
 * Considers inheritance and multiple grants.
 */
export interface EffectivePermission {
  /** Resource type */
  resourceType: 'namespace' | 'table';
  /** Resource identifier */
  resourceId: string;
  /** Effective permission level */
  level: PermissionLevel;
  /** Source of the permission (direct, inherited, role) */
  source: 'direct' | 'inherited' | 'role' | 'catalog';
  /** The grant that provided this permission (if applicable) */
  grantId?: string;
}

// ============================================================================
// Permission Mapping
// ============================================================================

/**
 * Map permission level strings to enum values.
 */
export function parsePermissionLevel(level: string): PermissionLevel {
  switch (level.toLowerCase()) {
    case 'none':
      return PermissionLevel.NONE;
    case 'read':
      return PermissionLevel.READ;
    case 'write':
      return PermissionLevel.WRITE;
    case 'admin':
      return PermissionLevel.ADMIN;
    case 'owner':
      return PermissionLevel.OWNER;
    default:
      return PermissionLevel.NONE;
  }
}

/**
 * Convert permission level enum to string.
 */
export function permissionLevelToString(level: PermissionLevel): PermissionLevelString {
  switch (level) {
    case PermissionLevel.NONE:
      return 'none';
    case PermissionLevel.READ:
      return 'read';
    case PermissionLevel.WRITE:
      return 'write';
    case PermissionLevel.ADMIN:
      return 'admin';
    case PermissionLevel.OWNER:
      return 'owner';
  }
}

/**
 * Get minimum permission level required for an operation.
 */
export function getRequiredPermissionLevel(permission: Permission): PermissionLevel {
  switch (permission) {
    // Read operations
    case 'namespace:list':
    case 'namespace:read':
    case 'table:list':
    case 'table:read':
      return PermissionLevel.READ;

    // Write operations
    case 'table:create':
    case 'table:write':
    case 'table:commit':
      return PermissionLevel.WRITE;

    // Admin operations
    case 'namespace:create':
    case 'namespace:update':
    case 'table:rename':
      return PermissionLevel.ADMIN;

    // Owner operations
    case 'namespace:drop':
    case 'namespace:grant':
    case 'namespace:revoke':
    case 'table:drop':
    case 'table:grant':
    case 'table:revoke':
      return PermissionLevel.OWNER;

    default:
      return PermissionLevel.OWNER;
  }
}

// ============================================================================
// Permission Checking
// ============================================================================

/**
 * Check if a permission level satisfies a required level.
 * Higher levels include all permissions of lower levels.
 */
export function satisfiesPermissionLevel(
  actual: PermissionLevel,
  required: PermissionLevel
): boolean {
  return actual >= required;
}

/**
 * Compute effective permission level from multiple grants.
 * Returns the highest permission level found.
 */
export function computeEffectiveLevel(grants: PermissionGrant[]): PermissionLevel {
  const now = Date.now();

  // Filter out expired grants
  const validGrants = grants.filter(g => !g.expiresAt || g.expiresAt > now);

  if (validGrants.length === 0) {
    return PermissionLevel.NONE;
  }

  // Return highest permission level
  return Math.max(...validGrants.map(g => g.level)) as PermissionLevel;
}

/**
 * Check namespace-level permission with FGA.
 */
export function checkNamespacePermission(
  context: AuthContext,
  namespace: string[],
  permission: NamespacePermission,
  grants?: PermissionGrant[]
): boolean {
  // Unauthenticated users have no permissions
  if (!context.authenticated) {
    return false;
  }

  // Get required permission level
  const requiredLevel = getRequiredPermissionLevel(permission);

  // Check catalog-level admin role (from oauth.do)
  if (context.roles.includes('admin') || context.roles.includes('owner')) {
    return true;
  }

  // Check catalog-level roles
  const catalogLevel = getCatalogPermissionLevel(context);
  if (satisfiesPermissionLevel(catalogLevel, requiredLevel)) {
    return true;
  }

  // Check namespace-specific grants
  if (grants && grants.length > 0) {
    const namespaceId = namespace.join('\x1f');
    const relevantGrants = grants.filter(g =>
      g.resourceType === 'namespace' &&
      g.resourceId === namespaceId &&
      matchesPrincipal(context, g)
    );

    const effectiveLevel = computeEffectiveLevel(relevantGrants);
    if (satisfiesPermissionLevel(effectiveLevel, requiredLevel)) {
      return true;
    }
  }

  // Check parent namespace inheritance
  if (namespace.length > 1 && grants && grants.length > 0) {
    const parentNamespace = namespace.slice(0, -1);
    const parentGrants = grants.filter(g =>
      g.resourceType === 'namespace' &&
      g.resourceId === parentNamespace.join('\x1f') &&
      matchesPrincipal(context, g)
    );

    const parentLevel = computeEffectiveLevel(parentGrants);
    if (satisfiesPermissionLevel(parentLevel, requiredLevel)) {
      return true;
    }
  }

  return false;
}

/**
 * Check table-level permission with FGA.
 * Implements permission inheritance from namespace.
 */
export function checkTablePermission(
  context: AuthContext,
  namespace: string[],
  tableName: string,
  permission: TablePermission,
  grants?: PermissionGrant[]
): boolean {
  // Unauthenticated users have no permissions
  if (!context.authenticated) {
    return false;
  }

  // Get required permission level
  const requiredLevel = getRequiredPermissionLevel(permission);

  // Check catalog-level admin role (from oauth.do)
  if (context.roles.includes('admin') || context.roles.includes('owner')) {
    return true;
  }

  // Check catalog-level roles
  const catalogLevel = getCatalogPermissionLevel(context);
  if (satisfiesPermissionLevel(catalogLevel, requiredLevel)) {
    return true;
  }

  if (grants && grants.length > 0) {
    const tableId = `${namespace.join('\x1f')}\x00${tableName}`;
    const namespaceId = namespace.join('\x1f');

    // Check table-specific grants (highest priority)
    const tableGrants = grants.filter(g =>
      g.resourceType === 'table' &&
      g.resourceId === tableId &&
      matchesPrincipal(context, g)
    );

    const tableLevel = computeEffectiveLevel(tableGrants);
    if (satisfiesPermissionLevel(tableLevel, requiredLevel)) {
      return true;
    }

    // Check namespace-level grants (inheritance)
    const namespaceGrants = grants.filter(g =>
      g.resourceType === 'namespace' &&
      g.resourceId === namespaceId &&
      matchesPrincipal(context, g)
    );

    const namespaceLevel = computeEffectiveLevel(namespaceGrants);
    if (satisfiesPermissionLevel(namespaceLevel, requiredLevel)) {
      return true;
    }

    // Check parent namespace inheritance
    if (namespace.length > 1) {
      const parentNamespace = namespace.slice(0, -1);
      const parentGrants = grants.filter(g =>
        g.resourceType === 'namespace' &&
        g.resourceId === parentNamespace.join('\x1f') &&
        matchesPrincipal(context, g)
      );

      const parentLevel = computeEffectiveLevel(parentGrants);
      if (satisfiesPermissionLevel(parentLevel, requiredLevel)) {
        return true;
      }
    }
  }

  return false;
}

/**
 * Check catalog-level permission.
 */
export function checkCatalogPermission(
  context: AuthContext,
  permission: Permission
): boolean {
  if (!context.authenticated) {
    return false;
  }

  // Check admin role
  if (context.roles.includes('admin') || context.roles.includes('owner')) {
    return true;
  }

  const requiredLevel = getRequiredPermissionLevel(permission);
  const catalogLevel = getCatalogPermissionLevel(context);

  return satisfiesPermissionLevel(catalogLevel, requiredLevel);
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Get catalog-level permission from user roles (from oauth.do).
 */
export function getCatalogPermissionLevel(context: AuthContext): PermissionLevel {
  if (context.roles.includes('owner')) {
    return PermissionLevel.OWNER;
  }
  if (context.roles.includes('admin')) {
    return PermissionLevel.ADMIN;
  }
  if (context.roles.includes('write')) {
    return PermissionLevel.WRITE;
  }
  if (context.roles.includes('read')) {
    return PermissionLevel.READ;
  }
  return PermissionLevel.NONE;
}

/**
 * Check if a grant matches the current user/principal.
 */
export function matchesPrincipal(context: AuthContext, grant: PermissionGrant): boolean {
  switch (grant.principalType) {
    case 'user':
      return context.userId === grant.principalId;
    case 'role':
      return context.roles.includes(grant.principalId);
    case 'group':
      // Groups would be checked via context.groups if available
      return (context as AuthContext & { groups?: string[] }).groups?.includes(grant.principalId) ?? false;
    default:
      return false;
  }
}

/**
 * Get effective permission for a user on a resource.
 */
export function getEffectivePermission(
  context: AuthContext,
  resourceType: 'namespace' | 'table',
  resourceId: string,
  grants: PermissionGrant[]
): EffectivePermission {
  // Start with catalog-level permission
  const catalogLevel = getCatalogPermissionLevel(context);

  let effectiveLevel = catalogLevel;
  let source: EffectivePermission['source'] = 'catalog';
  let grantId: string | undefined;

  // Check for admin/owner roles
  if (context.roles.includes('owner')) {
    return {
      resourceType,
      resourceId,
      level: PermissionLevel.OWNER,
      source: 'role',
    };
  }
  if (context.roles.includes('admin')) {
    return {
      resourceType,
      resourceId,
      level: PermissionLevel.ADMIN,
      source: 'role',
    };
  }

  // Check direct grants
  const directGrants = grants.filter(g =>
    g.resourceType === resourceType &&
    g.resourceId === resourceId &&
    matchesPrincipal(context, g)
  );

  if (directGrants.length > 0) {
    const directLevel = computeEffectiveLevel(directGrants);
    if (directLevel > effectiveLevel) {
      effectiveLevel = directLevel;
      source = 'direct';
      grantId = directGrants.find(g => g.level === directLevel)?.id;
    }
  }

  // Check inheritance for tables
  if (resourceType === 'table') {
    const parts = resourceId.split('\x00');
    if (parts.length === 2) {
      const namespaceId = parts[0];
      const namespaceGrants = grants.filter(g =>
        g.resourceType === 'namespace' &&
        g.resourceId === namespaceId &&
        matchesPrincipal(context, g)
      );

      const inheritedLevel = computeEffectiveLevel(namespaceGrants);
      if (inheritedLevel > effectiveLevel) {
        effectiveLevel = inheritedLevel;
        source = 'inherited';
        grantId = namespaceGrants.find(g => g.level === inheritedLevel)?.id;
      }
    }
  }

  // Check parent namespace inheritance for nested namespaces
  if (resourceType === 'namespace') {
    const parts = resourceId.split('\x1f');
    if (parts.length > 1) {
      const parentId = parts.slice(0, -1).join('\x1f');
      const parentGrants = grants.filter(g =>
        g.resourceType === 'namespace' &&
        g.resourceId === parentId &&
        matchesPrincipal(context, g)
      );

      const parentLevel = computeEffectiveLevel(parentGrants);
      if (parentLevel > effectiveLevel) {
        effectiveLevel = parentLevel;
        source = 'inherited';
        grantId = parentGrants.find(g => g.level === parentLevel)?.id;
      }
    }
  }

  return {
    resourceType,
    resourceId,
    level: effectiveLevel,
    source,
    grantId,
  };
}

// ============================================================================
// Legacy Exports (for backward compatibility)
// ============================================================================

/** @deprecated Use TablePermission instead */
export type CatalogPermission = TablePermission;
