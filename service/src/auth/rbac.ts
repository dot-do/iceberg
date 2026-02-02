/**
 * Role-Based Access Control (RBAC) for Iceberg Catalog
 *
 * Implements role-based access control with:
 * - Permission types (read, write, admin, owner)
 * - Resource types (namespace, table)
 * - Role definitions (viewer, editor, admin, owner)
 * - Permission checking, granting, and revoking
 *
 * @see https://iceberg.do
 */

import type { AuthContext } from './middleware.js';
import {
  type PermissionGrant,
  PermissionLevel,
  permissionLevelToString,
  computeEffectiveLevel,
} from './permissions.js';

// ============================================================================
// Permission Types
// ============================================================================

/** Actions that can be performed on resources */
export type Action = 'read' | 'write' | 'admin' | 'owner';

/** Resource types that can have permissions */
export type ResourceType = 'namespace' | 'table';

/** Predefined role definitions */
export type RoleName = 'viewer' | 'editor' | 'admin' | 'owner';

// ============================================================================
// Role Definitions
// ============================================================================

/**
 * Role definition with associated permission level.
 */
export interface RoleDefinition {
  /** Role name */
  name: RoleName;
  /** Display name */
  displayName: string;
  /** Description */
  description: string;
  /** Permission level granted by this role */
  level: PermissionLevel;
}

/**
 * Predefined roles and their permission levels.
 */
export const ROLE_DEFINITIONS: Record<RoleName, RoleDefinition> = {
  viewer: {
    name: 'viewer',
    displayName: 'Viewer',
    description: 'Can view namespaces and tables, read table data',
    level: PermissionLevel.READ,
  },
  editor: {
    name: 'editor',
    displayName: 'Editor',
    description: 'Can view and modify table data, create tables',
    level: PermissionLevel.WRITE,
  },
  admin: {
    name: 'admin',
    displayName: 'Administrator',
    description: 'Can manage namespaces, schemas, and settings',
    level: PermissionLevel.ADMIN,
  },
  owner: {
    name: 'owner',
    displayName: 'Owner',
    description: 'Full control including delete and permission management',
    level: PermissionLevel.OWNER,
  },
};

// ============================================================================
// Resource Identifier
// ============================================================================

/**
 * Represents a resource (namespace or table) for permission checks.
 */
export interface Resource {
  /** Resource type */
  type: ResourceType;
  /** Namespace path (e.g., ['db', 'schema']) */
  namespace: string[];
  /** Table name (only for table resources) */
  tableName?: string;
}

/**
 * Create a resource identifier for a namespace.
 */
export function namespaceResource(namespace: string[]): Resource {
  return { type: 'namespace', namespace };
}

/**
 * Create a resource identifier for a table.
 */
export function tableResource(namespace: string[], tableName: string): Resource {
  return { type: 'table', namespace, tableName };
}

/**
 * Convert a resource to its string identifier.
 * Uses unit separator (\x1f) between namespace parts and null (\x00) before table name.
 */
export function resourceToId(resource: Resource): string {
  const namespaceId = resource.namespace.join('\x1f');
  if (resource.type === 'table' && resource.tableName) {
    return `${namespaceId}\x00${resource.tableName}`;
  }
  return namespaceId;
}

/**
 * Parse a resource identifier string back to a Resource.
 */
export function parseResourceId(type: ResourceType, id: string): Resource {
  if (type === 'table') {
    const parts = id.split('\x00');
    const namespace = parts[0].split('\x1f');
    const tableName = parts[1];
    return { type: 'table', namespace, tableName };
  }
  return { type: 'namespace', namespace: id.split('\x1f') };
}

// ============================================================================
// Permission Store Interface
// ============================================================================

/**
 * Interface for storing and retrieving permission grants.
 * Implementations can use D1, Durable Objects, or in-memory storage.
 */
export interface PermissionStore {
  /** Get all grants for a principal (user or role) */
  getGrantsForPrincipal(principalType: 'user' | 'role' | 'group', principalId: string): Promise<PermissionGrant[]>;

  /** Get all grants for a resource */
  getGrantsForResource(resourceType: ResourceType, resourceId: string): Promise<PermissionGrant[]>;

  /** Get a specific grant by ID */
  getGrant(grantId: string): Promise<PermissionGrant | null>;

  /** Create a new grant */
  createGrant(grant: PermissionGrant): Promise<void>;

  /** Delete a grant by ID */
  deleteGrant(grantId: string): Promise<void>;

  /** Delete all grants for a resource */
  deleteGrantsForResource(resourceType: ResourceType, resourceId: string): Promise<void>;
}

// ============================================================================
// In-Memory Permission Store
// ============================================================================

/**
 * In-memory implementation of PermissionStore for testing and development.
 */
export class InMemoryPermissionStore implements PermissionStore {
  private grants: Map<string, PermissionGrant> = new Map();

  async getGrantsForPrincipal(principalType: 'user' | 'role' | 'group', principalId: string): Promise<PermissionGrant[]> {
    return Array.from(this.grants.values()).filter(
      g => g.principalType === principalType && g.principalId === principalId
    );
  }

  async getGrantsForResource(resourceType: ResourceType, resourceId: string): Promise<PermissionGrant[]> {
    return Array.from(this.grants.values()).filter(
      g => g.resourceType === resourceType && g.resourceId === resourceId
    );
  }

  async getGrant(grantId: string): Promise<PermissionGrant | null> {
    return this.grants.get(grantId) ?? null;
  }

  async createGrant(grant: PermissionGrant): Promise<void> {
    this.grants.set(grant.id, grant);
  }

  async deleteGrant(grantId: string): Promise<void> {
    this.grants.delete(grantId);
  }

  async deleteGrantsForResource(resourceType: ResourceType, resourceId: string): Promise<void> {
    for (const [id, grant] of this.grants) {
      if (grant.resourceType === resourceType && grant.resourceId === resourceId) {
        this.grants.delete(id);
      }
    }
  }

  /** Clear all grants (for testing) */
  clear(): void {
    this.grants.clear();
  }

  /** Get all grants (for testing) */
  getAllGrants(): PermissionGrant[] {
    return Array.from(this.grants.values());
  }
}

// ============================================================================
// RBAC Manager
// ============================================================================

/**
 * Result of a permission check.
 */
export interface PermissionCheckResult {
  /** Whether the action is allowed */
  allowed: boolean;
  /** Reason for the decision */
  reason: string;
  /** Source of the permission (if allowed) */
  source?: 'role' | 'catalog' | 'direct' | 'inherited';
  /** Effective permission level */
  level?: PermissionLevel;
}

/**
 * Result of a grant operation.
 */
export interface GrantResult {
  /** Whether the grant was successful */
  success: boolean;
  /** Error message if failed */
  error?: string;
  /** The created grant (if successful) */
  grant?: PermissionGrant;
}

/**
 * Result of a revoke operation.
 */
export interface RevokeResult {
  /** Whether the revoke was successful */
  success: boolean;
  /** Error message if failed */
  error?: string;
  /** Number of grants revoked */
  revokedCount?: number;
}

/**
 * RBAC Manager handles permission checking, granting, and revoking.
 */
export class RBACManager {
  constructor(private store: PermissionStore) {}

  /**
   * Check if a user can perform an action on a resource.
   *
   * @param user - The user's auth context
   * @param resource - The resource to check
   * @param action - The action to perform
   * @returns Permission check result
   */
  async checkPermission(
    user: AuthContext,
    resource: Resource,
    action: Action
  ): Promise<PermissionCheckResult> {
    // Unauthenticated users have no permissions
    if (!user.authenticated) {
      return {
        allowed: false,
        reason: 'User is not authenticated',
      };
    }

    // Convert action to permission level
    const requiredLevel = actionToPermissionLevel(action);

    // Check catalog-level roles (admin/owner get full access)
    if (user.roles.includes('owner')) {
      return {
        allowed: true,
        reason: 'User has owner role',
        source: 'role',
        level: PermissionLevel.OWNER,
      };
    }

    if (user.roles.includes('admin')) {
      return {
        allowed: true,
        reason: 'User has admin role',
        source: 'role',
        level: PermissionLevel.ADMIN,
      };
    }

    // Check catalog-level permission from roles
    const catalogLevel = getCatalogLevelFromRoles(user.roles);
    if (catalogLevel >= requiredLevel) {
      return {
        allowed: true,
        reason: `User has catalog-level ${permissionLevelToString(catalogLevel)} access`,
        source: 'catalog',
        level: catalogLevel,
      };
    }

    // Get all grants for the user
    const userGrants = user.userId
      ? await this.store.getGrantsForPrincipal('user', user.userId)
      : [];

    // Get role grants
    const roleGrants: PermissionGrant[] = [];
    for (const role of user.roles) {
      const grants = await this.store.getGrantsForPrincipal('role', role);
      roleGrants.push(...grants);
    }

    const allGrants = [...userGrants, ...roleGrants];

    // Check direct resource grants
    const resourceId = resourceToId(resource);
    const directGrants = allGrants.filter(
      g => g.resourceType === resource.type && g.resourceId === resourceId
    );

    const directLevel = computeEffectiveLevel(directGrants);
    if (directLevel >= requiredLevel) {
      return {
        allowed: true,
        reason: `User has direct ${permissionLevelToString(directLevel)} access to ${resource.type}`,
        source: 'direct',
        level: directLevel,
      };
    }

    // Check namespace inheritance for tables
    if (resource.type === 'table') {
      const namespaceId = resource.namespace.join('\x1f');
      const namespaceGrants = allGrants.filter(
        g => g.resourceType === 'namespace' && g.resourceId === namespaceId
      );

      const namespaceLevel = computeEffectiveLevel(namespaceGrants);
      if (namespaceLevel >= requiredLevel) {
        return {
          allowed: true,
          reason: `User has inherited ${permissionLevelToString(namespaceLevel)} access from namespace`,
          source: 'inherited',
          level: namespaceLevel,
        };
      }
    }

    // Check parent namespace inheritance
    if (resource.namespace.length > 1) {
      const parentId = resource.namespace.slice(0, -1).join('\x1f');
      const parentGrants = allGrants.filter(
        g => g.resourceType === 'namespace' && g.resourceId === parentId
      );

      const parentLevel = computeEffectiveLevel(parentGrants);
      if (parentLevel >= requiredLevel) {
        return {
          allowed: true,
          reason: `User has inherited ${permissionLevelToString(parentLevel)} access from parent namespace`,
          source: 'inherited',
          level: parentLevel,
        };
      }
    }

    return {
      allowed: false,
      reason: `User lacks ${action} permission on ${resource.type}`,
      level: Math.max(catalogLevel, directLevel) as PermissionLevel,
    };
  }

  /**
   * Grant a permission to a user or role on a resource.
   *
   * @param granter - The user granting the permission
   * @param grantee - The user or role receiving the permission
   * @param resource - The resource to grant access to
   * @param role - The role (permission level) to grant
   * @returns Grant result
   */
  async grantPermission(
    granter: AuthContext,
    grantee: { type: 'user' | 'role' | 'group'; id: string },
    resource: Resource,
    role: RoleName
  ): Promise<GrantResult> {
    // Check if granter has permission to grant
    const canGrant = await this.checkPermission(granter, resource, 'owner');
    if (!canGrant.allowed) {
      return {
        success: false,
        error: 'Granter does not have owner permission on this resource',
      };
    }

    // Get the permission level for the role
    const roleDefinition = ROLE_DEFINITIONS[role];
    if (!roleDefinition) {
      return {
        success: false,
        error: `Invalid role: ${role}`,
      };
    }

    // Create the grant
    const grant: PermissionGrant = {
      id: crypto.randomUUID(),
      resourceType: resource.type,
      resourceId: resourceToId(resource),
      principalType: grantee.type,
      principalId: grantee.id,
      level: roleDefinition.level,
      createdAt: Date.now(),
      createdBy: granter.userId ?? 'unknown',
    };

    await this.store.createGrant(grant);

    return {
      success: true,
      grant,
    };
  }

  /**
   * Revoke permissions from a user or role on a resource.
   *
   * @param revoker - The user revoking the permission
   * @param target - The user or role to revoke from
   * @param resource - The resource to revoke access from
   * @returns Revoke result
   */
  async revokePermission(
    revoker: AuthContext,
    target: { type: 'user' | 'role' | 'group'; id: string },
    resource: Resource
  ): Promise<RevokeResult> {
    // Check if revoker has permission to revoke
    const canRevoke = await this.checkPermission(revoker, resource, 'owner');
    if (!canRevoke.allowed) {
      return {
        success: false,
        error: 'Revoker does not have owner permission on this resource',
      };
    }

    // Get grants for the target on this resource
    const resourceId = resourceToId(resource);
    const grants = await this.store.getGrantsForResource(resource.type, resourceId);
    const targetGrants = grants.filter(
      g => g.principalType === target.type && g.principalId === target.id
    );

    // Delete matching grants
    let revokedCount = 0;
    for (const grant of targetGrants) {
      await this.store.deleteGrant(grant.id);
      revokedCount++;
    }

    return {
      success: true,
      revokedCount,
    };
  }

  /**
   * Get all grants for a resource.
   */
  async getResourceGrants(resource: Resource): Promise<PermissionGrant[]> {
    const resourceId = resourceToId(resource);
    return this.store.getGrantsForResource(resource.type, resourceId);
  }

  /**
   * Get all grants for a user.
   */
  async getUserGrants(userId: string): Promise<PermissionGrant[]> {
    return this.store.getGrantsForPrincipal('user', userId);
  }

  /**
   * Get all grants for a role.
   */
  async getRoleGrants(roleName: string): Promise<PermissionGrant[]> {
    return this.store.getGrantsForPrincipal('role', roleName);
  }
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Convert an action to the required permission level.
 */
export function actionToPermissionLevel(action: Action): PermissionLevel {
  switch (action) {
    case 'read':
      return PermissionLevel.READ;
    case 'write':
      return PermissionLevel.WRITE;
    case 'admin':
      return PermissionLevel.ADMIN;
    case 'owner':
      return PermissionLevel.OWNER;
  }
}

/**
 * Get catalog-level permission from user roles.
 */
function getCatalogLevelFromRoles(roles: string[]): PermissionLevel {
  if (roles.includes('owner')) return PermissionLevel.OWNER;
  if (roles.includes('admin')) return PermissionLevel.ADMIN;
  if (roles.includes('write')) return PermissionLevel.WRITE;
  if (roles.includes('read')) return PermissionLevel.READ;
  return PermissionLevel.NONE;
}

/**
 * Convert a role name to permission level.
 */
export function roleToPermissionLevel(role: RoleName): PermissionLevel {
  return ROLE_DEFINITIONS[role].level;
}

/**
 * Convert a permission level to the closest role name.
 */
export function permissionLevelToRole(level: PermissionLevel): RoleName {
  switch (level) {
    case PermissionLevel.OWNER:
      return 'owner';
    case PermissionLevel.ADMIN:
      return 'admin';
    case PermissionLevel.WRITE:
      return 'editor';
    case PermissionLevel.READ:
    case PermissionLevel.NONE:
    default:
      return 'viewer';
  }
}
