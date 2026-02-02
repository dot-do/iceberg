/**
 * Authorization Middleware for Iceberg Catalog
 *
 * Middleware to enforce FGA/RBAC permissions on namespace and table operations.
 *
 * @see https://iceberg.do
 */

import type { Context, Next, MiddlewareHandler } from 'hono';
import type { AuthContext } from './middleware.js';
import type { NamespacePermission, TablePermission } from './permissions.js';
import {
  type PermissionStore,
  type Resource,
  InMemoryPermissionStore,
} from './rbac.js';
import { FGAEngine, createFGAEngine } from './fga.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Authorization configuration.
 */
export interface AuthorizationConfig {
  /** Permission store (defaults to in-memory) */
  store?: PermissionStore;
  /** Enable/disable authorization (defaults to true) */
  enabled?: boolean;
  /** Skip authorization for these paths */
  skipPaths?: string[];
}

/**
 * Authorization context stored in Hono variables.
 */
export interface AuthorizationContext {
  /** FGA engine instance */
  fga: FGAEngine;
  /** Permission store instance */
  store: PermissionStore;
}

/**
 * Variables type for authorization context.
 */
export interface AuthorizationVariables {
  auth: AuthContext;
  authorization: AuthorizationContext;
}

// ============================================================================
// Middleware
// ============================================================================

/**
 * Create authorization middleware.
 *
 * This middleware:
 * 1. Initializes the FGA engine and stores it in context
 * 2. Provides helper functions for permission checking
 *
 * @example
 * ```ts
 * app.use('/*', createAuthorizationMiddleware({
 *   store: new D1PermissionStore(db),
 * }));
 * ```
 */
export function createAuthorizationMiddleware(
  config: AuthorizationConfig = {}
): MiddlewareHandler<{ Variables: AuthorizationVariables }> {
  const { store = new InMemoryPermissionStore(), enabled = true, skipPaths = [] } = config;
  const engine = createFGAEngine(store);

  return async (c: Context<{ Variables: AuthorizationVariables }>, next: Next) => {
    // Store authorization context
    c.set('authorization', { fga: engine, store });

    // Skip if disabled
    if (!enabled) {
      return next();
    }

    // Check skip paths
    const pathname = new URL(c.req.url).pathname;
    for (const path of skipPaths) {
      if (path.endsWith('*')) {
        if (pathname.startsWith(path.slice(0, -1))) {
          return next();
        }
      } else if (pathname === path) {
        return next();
      }
    }

    return next();
  };
}

/**
 * Get the FGA engine from context.
 */
export function getFGAEngine(c: Context<{ Variables: AuthorizationVariables }>): FGAEngine {
  const auth = c.get('authorization');
  if (!auth) {
    throw new Error('Authorization middleware not initialized');
  }
  return auth.fga;
}

/**
 * Get the permission store from context.
 */
export function getPermissionStore(c: Context<{ Variables: AuthorizationVariables }>): PermissionStore {
  const auth = c.get('authorization');
  if (!auth) {
    throw new Error('Authorization middleware not initialized');
  }
  return auth.store;
}

// ============================================================================
// Route Protection Middleware
// ============================================================================

/**
 * Create middleware that requires a specific namespace permission.
 *
 * When namespaceParam is not in the URL (e.g., for list operations), the middleware
 * falls back to checking catalog-level permissions based on user roles.
 *
 * @example
 * ```ts
 * app.get('/namespaces/:namespace', requireNamespacePermission('namespace:read'), handler);
 * app.get('/namespaces', requireNamespacePermission('namespace:list'), handler);
 * ```
 */
export function requireNamespacePermission(
  permission: NamespacePermission,
  options: { namespaceParam?: string } = {}
): MiddlewareHandler<{ Variables: AuthorizationVariables }> {
  const { namespaceParam = 'namespace' } = options;

  return async (c, next) => {
    const auth = c.get('auth') as AuthContext;
    if (!auth?.authenticated) {
      return c.json(
        {
          error: {
            message: 'Authentication required',
            type: 'NotAuthorizedException',
            code: 401,
          },
        },
        401
      );
    }

    // Skip permission checks for admin/owner roles (they have full access)
    if (auth.roles?.includes('admin') || auth.roles?.includes('owner')) {
      return next();
    }

    const namespaceValue = c.req.param(namespaceParam);

    // For routes without namespace parameter (e.g., list namespaces, create namespace),
    // check catalog-level permissions based on roles
    if (!namespaceValue) {
      const engine = getFGAEngine(c);
      // Use a placeholder namespace for catalog-level permission checks
      // The FGA engine will check role-based permissions
      const result = await engine.checkNamespaceAccess(auth, ['*'], permission);

      if (!result.allowed) {
        return c.json(
          {
            error: {
              message: `Access denied: ${result.reason}`,
              type: 'ForbiddenException',
              code: 403,
            },
          },
          403
        );
      }

      return next();
    }

    const namespace = parseNamespace(namespaceValue);
    const engine = getFGAEngine(c);
    const result = await engine.checkNamespaceAccess(auth, namespace, permission);

    if (!result.allowed) {
      return c.json(
        {
          error: {
            message: `Access denied: ${result.reason}`,
            type: 'ForbiddenException',
            code: 403,
          },
        },
        403
      );
    }

    return next();
  };
}

/**
 * Create middleware that requires a specific table permission.
 *
 * For routes without namespace/table parameters (e.g., rename), the middleware
 * falls back to checking catalog-level permissions based on user roles.
 *
 * @example
 * ```ts
 * app.get('/namespaces/:namespace/tables/:table', requireTablePermission('table:read'), handler);
 * app.get('/namespaces/:namespace/tables', requireTablePermission('table:list'), handler);
 * app.post('/tables/rename', requireTablePermission('table:rename'), handler);
 * ```
 */
export function requireTablePermission(
  permission: TablePermission,
  options: { namespaceParam?: string; tableParam?: string } = {}
): MiddlewareHandler<{ Variables: AuthorizationVariables }> {
  const { namespaceParam = 'namespace', tableParam = 'table' } = options;

  return async (c, next) => {
    const auth = c.get('auth') as AuthContext;
    if (!auth?.authenticated) {
      return c.json(
        {
          error: {
            message: 'Authentication required',
            type: 'NotAuthorizedException',
            code: 401,
          },
        },
        401
      );
    }

    // Skip permission checks for admin/owner roles (they have full access)
    if (auth.roles?.includes('admin') || auth.roles?.includes('owner')) {
      return next();
    }

    const namespaceValue = c.req.param(namespaceParam);
    const tableName = c.req.param(tableParam);

    // For routes without namespace/table parameters (e.g., rename table),
    // check catalog-level permissions based on roles
    if (!namespaceValue) {
      const engine = getFGAEngine(c);
      // Use a placeholder for catalog-level permission checks
      const result = await engine.checkTableAccess(auth, ['*'], '*', permission);

      if (!result.allowed) {
        return c.json(
          {
            error: {
              message: `Access denied: ${result.reason}`,
              type: 'ForbiddenException',
              code: 403,
            },
          },
          403
        );
      }

      return next();
    }

    // For routes with namespace but without table (e.g., list tables, create table),
    // check namespace-level permissions
    if (!tableName) {
      const namespace = parseNamespace(namespaceValue);
      const engine = getFGAEngine(c);
      // Use placeholder table name for namespace-level table operations
      const result = await engine.checkTableAccess(auth, namespace, '*', permission);

      if (!result.allowed) {
        return c.json(
          {
            error: {
              message: `Access denied: ${result.reason}`,
              type: 'ForbiddenException',
              code: 403,
            },
          },
          403
        );
      }

      return next();
    }

    const namespace = parseNamespace(namespaceValue);
    const engine = getFGAEngine(c);
    const result = await engine.checkTableAccess(auth, namespace, tableName, permission);

    if (!result.allowed) {
      return c.json(
        {
          error: {
            message: `Access denied: ${result.reason}`,
            type: 'ForbiddenException',
            code: 403,
          },
        },
        403
      );
    }

    return next();
  };
}

/**
 * Create middleware that requires owner permission for granting access.
 */
export function requireGrantPermission(
  resourceType: 'namespace' | 'table',
  options: { namespaceParam?: string; tableParam?: string } = {}
): MiddlewareHandler<{ Variables: AuthorizationVariables }> {
  const { namespaceParam = 'namespace', tableParam = 'table' } = options;

  return async (c, next) => {
    const auth = c.get('auth') as AuthContext;
    if (!auth?.authenticated) {
      return c.json(
        {
          error: {
            message: 'Authentication required',
            type: 'NotAuthorizedException',
            code: 401,
          },
        },
        401
      );
    }

    const namespaceValue = c.req.param(namespaceParam);
    if (!namespaceValue) {
      return c.json(
        {
          error: {
            message: 'Namespace parameter is required',
            type: 'BadRequestException',
            code: 400,
          },
        },
        400
      );
    }

    const namespace = parseNamespace(namespaceValue);
    const engine = getFGAEngine(c);

    let result;
    if (resourceType === 'table') {
      const tableName = c.req.param(tableParam);
      if (!tableName) {
        return c.json(
          {
            error: {
              message: 'Table parameter is required',
              type: 'BadRequestException',
              code: 400,
            },
          },
          400
        );
      }
      result = await engine.checkTableAccess(auth, namespace, tableName, 'table:grant');
    } else {
      result = await engine.checkNamespaceAccess(auth, namespace, 'namespace:grant');
    }

    if (!result.allowed) {
      return c.json(
        {
          error: {
            message: `Access denied: ${result.reason}`,
            type: 'ForbiddenException',
            code: 403,
          },
        },
        403
      );
    }

    return next();
  };
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Parse namespace from URL parameter.
 */
function parseNamespace(namespaceParam: string): string[] {
  const decoded = decodeURIComponent(namespaceParam);
  if (decoded.includes('\x1f')) {
    return decoded.split('\x1f');
  }
  if (decoded.includes('.')) {
    return decoded.split('.');
  }
  return [decoded];
}

/**
 * Check authorization inline (for use within handlers).
 */
export async function checkAuthorization(
  c: Context<{ Variables: AuthorizationVariables }>,
  resource: Resource,
  action: 'read' | 'write' | 'admin' | 'owner'
): Promise<{ allowed: boolean; reason: string }> {
  const auth = c.get('auth') as AuthContext;
  if (!auth?.authenticated) {
    return { allowed: false, reason: 'User is not authenticated' };
  }

  const engine = getFGAEngine(c);
  const result = await engine.evaluate({
    user: auth,
    resource,
    action,
    timestamp: Date.now(),
  });

  return { allowed: result.allowed, reason: result.reason };
}

/**
 * Require authorization inline (throws on failure).
 */
export async function requireAuthorization(
  c: Context<{ Variables: AuthorizationVariables }>,
  resource: Resource,
  action: 'read' | 'write' | 'admin' | 'owner'
): Promise<void> {
  const result = await checkAuthorization(c, resource, action);
  if (!result.allowed) {
    const error = new Error(`Access denied: ${result.reason}`) as Error & { code: number };
    error.code = 403;
    throw error;
  }
}
