/**
 * Fine-Grained Authorization (FGA) for Iceberg Catalog
 *
 * Implements fine-grained authorization with:
 * - Policy evaluation (who can access what)
 * - Namespace-level and table-level permissions
 * - Permission inheritance (namespace -> table)
 * - Context-aware access decisions
 *
 * @see https://iceberg.do
 */

import type { AuthContext } from './middleware.js';
import {
  type PermissionGrant,
  PermissionLevel,
  computeEffectiveLevel,
  type NamespacePermission,
  type TablePermission,
} from './permissions.js';
import {
  type Resource,
  type ResourceType,
  type Action,
  type RoleName,
  type PermissionStore,
  RBACManager,
  resourceToId,
  namespaceResource,
  tableResource,
  actionToPermissionLevel,
} from './rbac.js';

// ============================================================================
// FGA Policy Types
// ============================================================================

/**
 * Policy condition that can be evaluated at runtime.
 */
export interface PolicyCondition {
  /** Condition type */
  type: 'time-range' | 'ip-range' | 'organization' | 'custom';
  /** Condition parameters */
  params: Record<string, unknown>;
}

/**
 * A policy rule that determines access.
 */
export interface PolicyRule {
  /** Rule ID */
  id: string;
  /** Rule name */
  name: string;
  /** Description */
  description?: string;
  /** Resource type this rule applies to */
  resourceType: ResourceType | '*';
  /** Resource pattern (glob-like matching) */
  resourcePattern?: string;
  /** Required permission level */
  requiredLevel: PermissionLevel;
  /** Additional conditions */
  conditions?: PolicyCondition[];
  /** Whether this is a deny rule (takes precedence) */
  deny?: boolean;
  /** Priority (higher = evaluated first) */
  priority?: number;
}

/**
 * Result of a policy evaluation.
 */
export interface PolicyEvaluationResult {
  /** Whether access is allowed */
  allowed: boolean;
  /** Reason for the decision */
  reason: string;
  /** Matched rules (if any) */
  matchedRules?: PolicyRule[];
  /** Effective permission level */
  effectiveLevel?: PermissionLevel;
  /** Source of the permission */
  source?: 'policy' | 'grant' | 'role' | 'inherited' | 'default';
}

/**
 * Request context for policy evaluation.
 */
export interface PolicyContext {
  /** User auth context */
  user: AuthContext;
  /** Resource being accessed */
  resource: Resource;
  /** Action being performed */
  action: Action;
  /** Request timestamp */
  timestamp?: number;
  /** Request IP address */
  ipAddress?: string;
  /** Additional context attributes */
  attributes?: Record<string, unknown>;
}

// ============================================================================
// FGA Authorization Engine
// ============================================================================

/**
 * Fine-Grained Authorization engine for evaluating access policies.
 */
export class FGAEngine {
  private policies: Map<string, PolicyRule> = new Map();
  private rbac: RBACManager;

  constructor(store: PermissionStore) {
    this.rbac = new RBACManager(store);
  }

  /**
   * Add a policy rule.
   */
  addPolicy(rule: PolicyRule): void {
    this.policies.set(rule.id, rule);
  }

  /**
   * Remove a policy rule.
   */
  removePolicy(ruleId: string): void {
    this.policies.delete(ruleId);
  }

  /**
   * Get all policy rules.
   */
  getPolicies(): PolicyRule[] {
    return Array.from(this.policies.values());
  }

  /**
   * Evaluate policies and RBAC grants to determine access.
   */
  async evaluate(context: PolicyContext): Promise<PolicyEvaluationResult> {
    const { user, resource, action } = context;

    // Unauthenticated users have no permissions
    if (!user.authenticated) {
      return {
        allowed: false,
        reason: 'User is not authenticated',
        source: 'default',
      };
    }

    // Check for explicit deny policies first
    const denyResult = await this.checkDenyPolicies(context);
    if (denyResult) {
      return denyResult;
    }

    // Check RBAC grants
    const rbacResult = await this.rbac.checkPermission(user, resource, action);
    if (rbacResult.allowed) {
      return {
        allowed: true,
        reason: rbacResult.reason,
        effectiveLevel: rbacResult.level,
        source: rbacResult.source === 'role' ? 'role' : rbacResult.source === 'inherited' ? 'inherited' : 'grant',
      };
    }

    // Check allow policies
    const allowResult = await this.checkAllowPolicies(context);
    if (allowResult) {
      return allowResult;
    }

    // Default deny
    return {
      allowed: false,
      reason: `No policy or grant allows ${action} on ${resource.type}`,
      source: 'default',
    };
  }

  /**
   * Check namespace-level permission using FGA.
   */
  async checkNamespaceAccess(
    user: AuthContext,
    namespace: string[],
    permission: NamespacePermission
  ): Promise<PolicyEvaluationResult> {
    const resource = namespaceResource(namespace);
    const action = namespacePermissionToAction(permission);

    return this.evaluate({
      user,
      resource,
      action,
      timestamp: Date.now(),
    });
  }

  /**
   * Check table-level permission using FGA.
   */
  async checkTableAccess(
    user: AuthContext,
    namespace: string[],
    tableName: string,
    permission: TablePermission
  ): Promise<PolicyEvaluationResult> {
    const resource = tableResource(namespace, tableName);
    const action = tablePermissionToAction(permission);

    return this.evaluate({
      user,
      resource,
      action,
      timestamp: Date.now(),
    });
  }

  /**
   * Get effective permissions for a user on a resource.
   */
  async getEffectivePermissions(
    user: AuthContext,
    resource: Resource
  ): Promise<{
    level: PermissionLevel;
    source: string;
    grants: PermissionGrant[];
  }> {
    // Get all grants for this resource
    const grants = await this.rbac.getResourceGrants(resource);

    // Filter to grants that match this user
    const userGrants = grants.filter(g => {
      if (g.principalType === 'user') {
        return g.principalId === user.userId;
      }
      if (g.principalType === 'role') {
        return user.roles.includes(g.principalId);
      }
      return false;
    });

    // Compute effective level
    let effectiveLevel = PermissionLevel.NONE;
    let source = 'none';

    // Check role-based permissions
    if (user.roles.includes('owner')) {
      effectiveLevel = PermissionLevel.OWNER;
      source = 'role:owner';
    } else if (user.roles.includes('admin')) {
      effectiveLevel = PermissionLevel.ADMIN;
      source = 'role:admin';
    } else if (user.roles.includes('write')) {
      effectiveLevel = PermissionLevel.WRITE;
      source = 'role:write';
    } else if (user.roles.includes('read')) {
      effectiveLevel = PermissionLevel.READ;
      source = 'role:read';
    }

    // Check direct grants
    const directLevel = computeEffectiveLevel(userGrants);
    if (directLevel > effectiveLevel) {
      effectiveLevel = directLevel;
      source = 'grant:direct';
    }

    // Check namespace inheritance for tables
    if (resource.type === 'table') {
      const namespaceGrants = await this.rbac.getResourceGrants(
        namespaceResource(resource.namespace)
      );
      const namespaceUserGrants = namespaceGrants.filter(g => {
        if (g.principalType === 'user') {
          return g.principalId === user.userId;
        }
        if (g.principalType === 'role') {
          return user.roles.includes(g.principalId);
        }
        return false;
      });
      const namespaceLevel = computeEffectiveLevel(namespaceUserGrants);
      if (namespaceLevel > effectiveLevel) {
        effectiveLevel = namespaceLevel;
        source = 'grant:inherited';
      }
    }

    return {
      level: effectiveLevel,
      source,
      grants: userGrants,
    };
  }

  /**
   * List all resources a user has access to.
   */
  async listAccessibleResources(
    user: AuthContext,
    resourceType: ResourceType,
    action: Action
  ): Promise<Resource[]> {
    // Get all grants for this user
    const userGrants = user.userId
      ? await this.rbac.getUserGrants(user.userId)
      : [];

    // Get role grants
    const roleGrants: PermissionGrant[] = [];
    for (const role of user.roles) {
      const grants = await this.rbac.getRoleGrants(role);
      roleGrants.push(...grants);
    }

    const allGrants = [...userGrants, ...roleGrants];

    // Filter grants by resource type and action level
    const requiredLevel = actionToPermissionLevel(action);
    const matchingGrants = allGrants.filter(
      g => g.resourceType === resourceType && g.level >= requiredLevel
    );

    // Convert to resources (deduplicate by resource ID)
    const resourceMap = new Map<string, Resource>();
    for (const grant of matchingGrants) {
      if (!resourceMap.has(grant.resourceId)) {
        if (resourceType === 'namespace') {
          const namespace = grant.resourceId.split('\x1f');
          resourceMap.set(grant.resourceId, namespaceResource(namespace));
        } else {
          const parts = grant.resourceId.split('\x00');
          const namespace = parts[0].split('\x1f');
          const tableName = parts[1];
          resourceMap.set(grant.resourceId, tableResource(namespace, tableName));
        }
      }
    }

    return Array.from(resourceMap.values());
  }

  /**
   * Grant permission using the underlying RBAC manager.
   */
  async grantPermission(
    granter: AuthContext,
    grantee: { type: 'user' | 'role' | 'group'; id: string },
    resource: Resource,
    role: RoleName
  ) {
    return this.rbac.grantPermission(granter, grantee, resource, role);
  }

  /**
   * Revoke permission using the underlying RBAC manager.
   */
  async revokePermission(
    revoker: AuthContext,
    target: { type: 'user' | 'role' | 'group'; id: string },
    resource: Resource
  ) {
    return this.rbac.revokePermission(revoker, target, resource);
  }

  // -------------------------------------------------------------------------
  // Private Methods
  // -------------------------------------------------------------------------

  private async checkDenyPolicies(context: PolicyContext): Promise<PolicyEvaluationResult | null> {
    const denyPolicies = Array.from(this.policies.values())
      .filter(p => p.deny === true)
      .sort((a, b) => (b.priority ?? 0) - (a.priority ?? 0));

    for (const policy of denyPolicies) {
      if (this.matchesPolicy(policy, context)) {
        if (this.evaluateConditions(policy.conditions, context)) {
          return {
            allowed: false,
            reason: `Denied by policy: ${policy.name}`,
            matchedRules: [policy],
            source: 'policy',
          };
        }
      }
    }

    return null;
  }

  private async checkAllowPolicies(context: PolicyContext): Promise<PolicyEvaluationResult | null> {
    const allowPolicies = Array.from(this.policies.values())
      .filter(p => p.deny !== true)
      .sort((a, b) => (b.priority ?? 0) - (a.priority ?? 0));

    for (const policy of allowPolicies) {
      if (this.matchesPolicy(policy, context)) {
        if (this.evaluateConditions(policy.conditions, context)) {
          const requiredLevel = actionToPermissionLevel(context.action);
          if (policy.requiredLevel >= requiredLevel) {
            return {
              allowed: true,
              reason: `Allowed by policy: ${policy.name}`,
              matchedRules: [policy],
              effectiveLevel: policy.requiredLevel,
              source: 'policy',
            };
          }
        }
      }
    }

    return null;
  }

  private matchesPolicy(policy: PolicyRule, context: PolicyContext): boolean {
    // Check resource type
    if (policy.resourceType !== '*' && policy.resourceType !== context.resource.type) {
      return false;
    }

    // Check resource pattern if specified
    if (policy.resourcePattern) {
      const resourceId = resourceToId(context.resource);
      if (!this.matchesPattern(policy.resourcePattern, resourceId)) {
        return false;
      }
    }

    return true;
  }

  private matchesPattern(pattern: string, value: string): boolean {
    // Simple glob-like pattern matching
    // * matches any characters within a segment
    // ** matches any characters including separators
    const regexPattern = pattern
      .replace(/\*\*/g, '.*')
      .replace(/\*/g, '[^\x1f\x00]*')
      .replace(/\?/g, '.');

    const regex = new RegExp(`^${regexPattern}$`);
    return regex.test(value);
  }

  private evaluateConditions(
    conditions: PolicyCondition[] | undefined,
    context: PolicyContext
  ): boolean {
    if (!conditions || conditions.length === 0) {
      return true;
    }

    for (const condition of conditions) {
      if (!this.evaluateCondition(condition, context)) {
        return false;
      }
    }

    return true;
  }

  private evaluateCondition(condition: PolicyCondition, context: PolicyContext): boolean {
    switch (condition.type) {
      case 'time-range': {
        const now = context.timestamp ?? Date.now();
        const start = condition.params.start as number | undefined;
        const end = condition.params.end as number | undefined;
        if (start && now < start) return false;
        if (end && now > end) return false;
        return true;
      }

      case 'ip-range': {
        // Simplified IP range check (production would need proper CIDR matching)
        const allowed = condition.params.allowed as string[] | undefined;
        if (!allowed || !context.ipAddress) return false;
        return allowed.includes(context.ipAddress);
      }

      case 'organization': {
        const orgId = condition.params.organizationId as string | undefined;
        return context.user.organizationId === orgId;
      }

      case 'custom': {
        // Custom conditions can be extended
        return true;
      }

      default:
        return true;
    }
  }
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Convert a namespace permission to an action.
 */
function namespacePermissionToAction(permission: NamespacePermission): Action {
  switch (permission) {
    case 'namespace:list':
    case 'namespace:read':
      return 'read';
    case 'namespace:create':
    case 'namespace:update':
      return 'admin';
    case 'namespace:drop':
    case 'namespace:grant':
    case 'namespace:revoke':
      return 'owner';
  }
}

/**
 * Convert a table permission to an action.
 */
function tablePermissionToAction(permission: TablePermission): Action {
  switch (permission) {
    case 'table:list':
    case 'table:read':
      return 'read';
    case 'table:create':
    case 'table:write':
    case 'table:commit':
      return 'write';
    case 'table:rename':
      return 'admin';
    case 'table:drop':
    case 'table:grant':
    case 'table:revoke':
      return 'owner';
  }
}

/**
 * Create a default FGA engine with no policies.
 */
export function createFGAEngine(store: PermissionStore): FGAEngine {
  return new FGAEngine(store);
}

// ============================================================================
// Authorization Middleware Helpers
// ============================================================================

/**
 * Check if a user can access a namespace with a specific permission.
 */
export async function canAccessNamespace(
  engine: FGAEngine,
  user: AuthContext,
  namespace: string[],
  permission: NamespacePermission
): Promise<boolean> {
  const result = await engine.checkNamespaceAccess(user, namespace, permission);
  return result.allowed;
}

/**
 * Check if a user can access a table with a specific permission.
 */
export async function canAccessTable(
  engine: FGAEngine,
  user: AuthContext,
  namespace: string[],
  tableName: string,
  permission: TablePermission
): Promise<boolean> {
  const result = await engine.checkTableAccess(user, namespace, tableName, permission);
  return result.allowed;
}

/**
 * Require access to a namespace, throwing if denied.
 */
export async function requireNamespaceAccess(
  engine: FGAEngine,
  user: AuthContext,
  namespace: string[],
  permission: NamespacePermission
): Promise<void> {
  const result = await engine.checkNamespaceAccess(user, namespace, permission);
  if (!result.allowed) {
    const error = new Error(`Access denied: ${result.reason}`) as Error & { code: number };
    error.code = 403;
    throw error;
  }
}

/**
 * Require access to a table, throwing if denied.
 */
export async function requireTableAccess(
  engine: FGAEngine,
  user: AuthContext,
  namespace: string[],
  tableName: string,
  permission: TablePermission
): Promise<void> {
  const result = await engine.checkTableAccess(user, namespace, tableName, permission);
  if (!result.allowed) {
    const error = new Error(`Access denied: ${result.reason}`) as Error & { code: number };
    error.code = 403;
    throw error;
  }
}
