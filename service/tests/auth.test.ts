/**
 * Tests for Auth Middleware and Permissions
 *
 * Tests authentication middleware and fine-grained authorization.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { Hono } from 'hono';
import type { Context, Next } from 'hono';
import {
  createAuthMiddleware,
  extractUserContext,
  checkPermission,
  hasTableAccess,
  getAuthContext,
  type AuthConfig,
  type AuthContext,
  type EnvWithOAuth,
  type OAuthService,
} from '../src/auth/middleware.js';
import {
  PermissionLevel,
  parsePermissionLevel,
  permissionLevelToString,
  getRequiredPermissionLevel,
  satisfiesPermissionLevel,
  computeEffectiveLevel,
  checkNamespacePermission,
  checkTablePermission,
  checkCatalogPermission,
  getCatalogPermissionLevel,
  matchesPrincipal,
  getEffectivePermission,
  type PermissionGrant,
} from '../src/auth/permissions.js';
import {
  createOAuthDoMiddleware,
  oauthUserToAuthContext,
  authContextToOAuthUser,
  validateTokenViaServiceBinding,
  type OAuthDoUser,
  type OAuthDoEnv,
} from '../src/auth/oauth-do.js';

// ============================================================================
// JWT Test Helpers
// ============================================================================

/**
 * Create a mock JWT token for testing.
 * Note: This creates a token without cryptographic signature verification.
 */
function createMockJwt(claims: Record<string, unknown>): string {
  const header = { alg: 'HS256', typ: 'JWT' };
  const encodeBase64Url = (obj: unknown): string => {
    const json = JSON.stringify(obj);
    const base64 = btoa(json);
    return base64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  };

  const headerEncoded = encodeBase64Url(header);
  const payloadEncoded = encodeBase64Url(claims);
  const signature = 'mock_signature';

  return `${headerEncoded}.${payloadEncoded}.${signature}`;
}

/**
 * Create a valid test token with standard claims.
 */
function createValidToken(overrides: Partial<{
  sub: string;
  email: string;
  roles: string[];
  permissions: string[];
  org_id: string;
  exp: number;
  custom: string;
}>): string {
  const now = Math.floor(Date.now() / 1000);
  return createMockJwt({
    sub: 'user-123',
    email: 'test@example.com',
    roles: ['read'],
    iat: now,
    exp: now + 3600, // 1 hour from now
    ...overrides,
  });
}

/**
 * Create an expired test token.
 */
function createExpiredToken(): string {
  const now = Math.floor(Date.now() / 1000);
  return createMockJwt({
    sub: 'user-123',
    email: 'test@example.com',
    roles: ['read'],
    iat: now - 7200,
    exp: now - 3600, // Expired 1 hour ago
  });
}

// ============================================================================
// Mock OAuth Service
// ============================================================================

function createMockOAuthService(
  validateResponse: { valid: boolean; user?: object; error?: { code: string; message: string } }
): OAuthService {
  return {
    fetch: async (_request: Request): Promise<Response> => {
      return Response.json(validateResponse);
    },
  };
}

// ============================================================================
// Mock Environment
// ============================================================================

function createMockEnv(oauth?: OAuthService): EnvWithOAuth {
  return {
    OAUTH: oauth,
    ENVIRONMENT: 'test',
  };
}

// ============================================================================
// Auth Middleware Tests
// ============================================================================

describe('Auth Middleware', () => {
  describe('extractUserContext', () => {
    it('should extract user context from valid JWT', () => {
      const token = createValidToken({
        sub: 'user-456',
        email: 'user@example.com',
        roles: ['admin', 'write'],
        org_id: 'org-789',
      });

      const context = extractUserContext(token);

      expect(context).not.toBeNull();
      expect(context?.userId).toBe('user-456');
      expect(context?.email).toBe('user@example.com');
      expect(context?.roles).toEqual(['admin', 'write']);
      expect(context?.organizationId).toBe('org-789');
    });

    it('should return null for invalid JWT format', () => {
      expect(extractUserContext('not-a-jwt')).toBeNull();
      expect(extractUserContext('only.two.parts')).toBeNull();
      expect(extractUserContext('')).toBeNull();
    });

    it('should return null for JWT without sub claim', () => {
      const token = createMockJwt({ email: 'test@example.com' });
      expect(extractUserContext(token)).toBeNull();
    });

    it('should handle missing optional claims', () => {
      const token = createMockJwt({ sub: 'user-123' });
      const context = extractUserContext(token);

      expect(context).not.toBeNull();
      expect(context?.userId).toBe('user-123');
      expect(context?.email).toBeUndefined();
      expect(context?.roles).toEqual([]);
      expect(context?.organizationId).toBeUndefined();
    });

    it('should extract custom metadata from non-standard claims', () => {
      const token = createMockJwt({
        sub: 'user-123',
        custom_field: 'custom_value',
        another_field: 123,
      });

      const context = extractUserContext(token);
      expect(context?.metadata).toEqual({
        custom_field: 'custom_value',
        another_field: 123,
      });
    });

    it('should not include reserved claims in metadata', () => {
      const token = createMockJwt({
        sub: 'user-123',
        iss: 'https://oauth.do',
        aud: 'iceberg-api',
        exp: 1234567890,
        iat: 1234567890,
        custom: 'value',
      });

      const context = extractUserContext(token);
      expect(context?.metadata).toEqual({ custom: 'value' });
      expect(context?.metadata?.iss).toBeUndefined();
      expect(context?.metadata?.aud).toBeUndefined();
    });
  });

  describe('createAuthMiddleware', () => {
    let app: Hono<{ Bindings: EnvWithOAuth }>;

    beforeEach(() => {
      app = new Hono<{ Bindings: EnvWithOAuth }>();
    });

    it('should grant full admin access when authentication is disabled', async () => {
      const middleware = createAuthMiddleware({ enabled: false });
      app.use('*', middleware);
      app.get('/test', (c) => {
        const auth = getAuthContext(c);
        return c.json({ authenticated: auth.authenticated, roles: auth.roles });
      });

      const res = await app.fetch(new Request('http://test/test'));
      expect(res.status).toBe(200);
      const data = await res.json();
      expect(data.authenticated).toBe(true);
      expect(data.roles).toContain('admin');
    });

    it('should allow access to public paths without authentication', async () => {
      const middleware = createAuthMiddleware({
        enabled: true,
        publicPaths: ['/health', '/v1/config'],
      });
      app.use('*', middleware);
      app.get('/health', (c) => c.json({ status: 'ok' }));
      app.get('/v1/config', (c) => c.json({ config: {} }));

      const healthRes = await app.fetch(new Request('http://test/health'));
      expect(healthRes.status).toBe(200);

      const configRes = await app.fetch(new Request('http://test/v1/config'));
      expect(configRes.status).toBe(200);
    });

    it('should support wildcard public paths', async () => {
      const middleware = createAuthMiddleware({
        enabled: true,
        publicPaths: ['/public/*'],
      });
      app.use('*', middleware);
      app.get('/public/docs', (c) => c.json({ docs: [] }));
      app.get('/public/status/health', (c) => c.json({ status: 'ok' }));

      const docsRes = await app.fetch(new Request('http://test/public/docs'));
      expect(docsRes.status).toBe(200);

      const statusRes = await app.fetch(new Request('http://test/public/status/health'));
      expect(statusRes.status).toBe(200);
    });

    it('should return 401 when Authorization header is missing', async () => {
      const middleware = createAuthMiddleware({ enabled: true });
      app.use('*', middleware);
      app.get('/protected', (c) => c.json({ data: 'secret' }));

      const res = await app.fetch(new Request('http://test/protected'));
      expect(res.status).toBe(401);

      const data = await res.json();
      expect(data.error.message).toBe('Authorization header is required');
      expect(res.headers.get('WWW-Authenticate')).toContain('Bearer');
    });

    it('should return 401 for non-Bearer authorization scheme', async () => {
      const middleware = createAuthMiddleware({ enabled: true });
      app.use('*', middleware);
      app.get('/protected', (c) => c.json({ data: 'secret' }));

      const res = await app.fetch(
        new Request('http://test/protected', {
          headers: { Authorization: 'Basic dXNlcjpwYXNz' },
        })
      );
      expect(res.status).toBe(401);

      const data = await res.json();
      expect(data.error.message).toBe('Authorization header must use Bearer scheme');
    });

    it('should return 401 for invalid token format', async () => {
      const middleware = createAuthMiddleware({ enabled: true });
      app.use('*', middleware);
      app.get('/protected', (c) => c.json({ data: 'secret' }));

      // Invalid JWT format - not a valid base64 encoded JWT
      const res = await app.fetch(
        new Request('http://test/protected', {
          headers: { Authorization: 'Bearer invalid-token-format' },
        }),
        createMockEnv()
      );
      expect(res.status).toBe(401);

      const data = await res.json();
      expect(data.error.message).toBe('Failed to parse token');
    });

    it('should return 401 for expired tokens (local validation)', async () => {
      const middleware = createAuthMiddleware({ enabled: true });
      app.use('*', middleware);
      app.get('/protected', (c) => c.json({ data: 'secret' }));

      const res = await app.fetch(
        new Request('http://test/protected', {
          headers: { Authorization: `Bearer ${createExpiredToken()}` },
        }),
        createMockEnv()
      );
      expect(res.status).toBe(401);

      const data = await res.json();
      expect(data.error.message).toBe('Token has expired');
    });

    it('should authenticate valid tokens (local validation)', async () => {
      const middleware = createAuthMiddleware({ enabled: true });
      app.use('*', middleware);
      app.get('/protected', (c) => {
        const auth = getAuthContext(c);
        return c.json({
          authenticated: auth.authenticated,
          userId: auth.userId,
          roles: auth.roles,
        });
      });

      const token = createValidToken({
        sub: 'user-123',
        roles: ['admin'],
      });

      const res = await app.fetch(
        new Request('http://test/protected', {
          headers: { Authorization: `Bearer ${token}` },
        }),
        createMockEnv()
      );
      expect(res.status).toBe(200);

      const data = await res.json();
      expect(data.authenticated).toBe(true);
      expect(data.userId).toBe('user-123');
      expect(data.roles).toEqual(['admin']);
    });

    it('should use OAuth service when available', async () => {
      const mockOAuth = createMockOAuthService({
        valid: true,
        user: {
          userId: 'oauth-user-456',
          email: 'oauth@example.com',
          roles: ['write'],
        },
      });

      const middleware = createAuthMiddleware({ enabled: true });
      app.use('*', middleware);
      app.get('/protected', (c) => {
        const auth = getAuthContext(c);
        return c.json({
          authenticated: auth.authenticated,
          userId: auth.userId,
        });
      });

      const res = await app.fetch(
        new Request('http://test/protected', {
          headers: { Authorization: `Bearer valid-token` },
        }),
        createMockEnv(mockOAuth)
      );
      expect(res.status).toBe(200);

      const data = await res.json();
      expect(data.authenticated).toBe(true);
      expect(data.userId).toBe('oauth-user-456');
    });

    it('should return 401 when OAuth service rejects token', async () => {
      const mockOAuth = createMockOAuthService({
        valid: false,
        error: { code: 'INVALID_TOKEN', message: 'Token is invalid' },
      });

      const middleware = createAuthMiddleware({ enabled: true });
      app.use('*', middleware);
      app.get('/protected', (c) => c.json({ data: 'secret' }));

      const res = await app.fetch(
        new Request('http://test/protected', {
          headers: { Authorization: `Bearer invalid-token` },
        }),
        createMockEnv(mockOAuth)
      );
      expect(res.status).toBe(401);

      const data = await res.json();
      expect(data.error.message).toBe('Token is invalid');
    });

    it('should allow anonymous read access when configured', async () => {
      const middleware = createAuthMiddleware({
        enabled: true,
        allowAnonymousRead: true,
      });
      app.use('*', middleware);
      app.get('/data', (c) => {
        const auth = getAuthContext(c);
        return c.json({
          authenticated: auth.authenticated,
          roles: auth.roles,
        });
      });

      const res = await app.fetch(new Request('http://test/data'));
      expect(res.status).toBe(200);

      const data = await res.json();
      expect(data.authenticated).toBe(false);
      expect(data.roles).toEqual(['read']);
    });

    it('should require auth for write operations even with allowAnonymousRead', async () => {
      const middleware = createAuthMiddleware({
        enabled: true,
        allowAnonymousRead: true,
      });
      app.use('*', middleware);
      app.post('/data', (c) => c.json({ created: true }));

      const res = await app.fetch(
        new Request('http://test/data', { method: 'POST' })
      );
      expect(res.status).toBe(401);
    });

    it('should add auth headers to response for authenticated users', async () => {
      const middleware = createAuthMiddleware({ enabled: true });
      app.use('*', middleware);
      app.get('/protected', (c) => c.json({ data: 'success' }));

      const token = createValidToken({
        sub: 'user-123',
        email: 'test@example.com',
        roles: ['admin', 'write'],
        org_id: 'org-456',
      });

      const res = await app.fetch(
        new Request('http://test/protected', {
          headers: { Authorization: `Bearer ${token}` },
        }),
        createMockEnv()
      );
      expect(res.status).toBe(200);
      expect(res.headers.get('X-User-Id')).toBe('user-123');
      expect(res.headers.get('X-User-Email')).toBe('test@example.com');
      expect(res.headers.get('X-Organization-Id')).toBe('org-456');
      expect(res.headers.get('X-User-Roles')).toBe('admin,write');
    });

    // =========================================================================
    // Security: Local Validation Protection Tests
    // =========================================================================

    it('should return 503 in production when OAuth service is unavailable', async () => {
      const middleware = createAuthMiddleware({ enabled: true });
      app.use('*', middleware);
      app.get('/protected', (c) => c.json({ data: 'secret' }));

      const token = createValidToken({ sub: 'user-123' });

      // Production environment without OAuth service
      const productionEnv: EnvWithOAuth = {
        OAUTH: undefined,
        ENVIRONMENT: 'production',
      };

      const res = await app.fetch(
        new Request('http://test/protected', {
          headers: { Authorization: `Bearer ${token}` },
        }),
        productionEnv
      );
      expect(res.status).toBe(503);

      const data = await res.json();
      expect(data.error.message).toBe('Authentication service unavailable');
      expect(data.error.type).toBe('ServiceUnavailable');
    });

    it('should return 503 when disableLocalValidation is true and no OAuth service', async () => {
      const middleware = createAuthMiddleware({
        enabled: true,
        disableLocalValidation: true,
      });
      app.use('*', middleware);
      app.get('/protected', (c) => c.json({ data: 'secret' }));

      const token = createValidToken({ sub: 'user-123' });

      // Non-production environment but local validation disabled
      const envWithoutOAuth: EnvWithOAuth = {
        OAUTH: undefined,
        ENVIRONMENT: 'development',
      };

      const res = await app.fetch(
        new Request('http://test/protected', {
          headers: { Authorization: `Bearer ${token}` },
        }),
        envWithoutOAuth
      );
      expect(res.status).toBe(503);

      const data = await res.json();
      expect(data.error.message).toBe('Authentication service unavailable and local validation is disabled');
    });

    it('should mark locally validated tokens as unverified', async () => {
      const middleware = createAuthMiddleware({ enabled: true });
      app.use('*', middleware);
      app.get('/protected', (c) => {
        const auth = getAuthContext(c);
        return c.json({
          authenticated: auth.authenticated,
          unverified: auth.unverified,
        });
      });

      const token = createValidToken({ sub: 'user-123' });

      // Non-production without OAuth service triggers local validation
      const res = await app.fetch(
        new Request('http://test/protected', {
          headers: { Authorization: `Bearer ${token}` },
        }),
        createMockEnv()  // ENVIRONMENT: 'test', no OAUTH
      );
      expect(res.status).toBe(200);

      const data = await res.json();
      expect(data.authenticated).toBe(true);
      expect(data.unverified).toBe(true);
    });

    it('should add X-Token-Unverified header for locally validated tokens', async () => {
      const middleware = createAuthMiddleware({ enabled: true });
      app.use('*', middleware);
      app.get('/protected', (c) => c.json({ data: 'success' }));

      const token = createValidToken({ sub: 'user-123' });

      const res = await app.fetch(
        new Request('http://test/protected', {
          headers: { Authorization: `Bearer ${token}` },
        }),
        createMockEnv()  // Non-production without OAuth
      );
      expect(res.status).toBe(200);
      expect(res.headers.get('X-Token-Unverified')).toBe('true');
    });

    it('should NOT mark OAuth-validated tokens as unverified', async () => {
      const mockOAuth = createMockOAuthService({
        valid: true,
        user: {
          userId: 'oauth-user-123',
          email: 'oauth@example.com',
          roles: ['read'],
        },
      });

      const middleware = createAuthMiddleware({ enabled: true });
      app.use('*', middleware);
      app.get('/protected', (c) => {
        const auth = getAuthContext(c);
        return c.json({
          authenticated: auth.authenticated,
          unverified: auth.unverified,
        });
      });

      const res = await app.fetch(
        new Request('http://test/protected', {
          headers: { Authorization: 'Bearer valid-token' },
        }),
        createMockEnv(mockOAuth)
      );
      expect(res.status).toBe(200);

      const data = await res.json();
      expect(data.authenticated).toBe(true);
      expect(data.unverified).toBeUndefined();
    });

    it('should NOT add X-Token-Unverified header for OAuth-validated tokens', async () => {
      const mockOAuth = createMockOAuthService({
        valid: true,
        user: {
          userId: 'oauth-user-123',
          email: 'oauth@example.com',
          roles: ['read'],
        },
      });

      const middleware = createAuthMiddleware({ enabled: true });
      app.use('*', middleware);
      app.get('/protected', (c) => c.json({ data: 'success' }));

      const res = await app.fetch(
        new Request('http://test/protected', {
          headers: { Authorization: 'Bearer valid-token' },
        }),
        createMockEnv(mockOAuth)
      );
      expect(res.status).toBe(200);
      expect(res.headers.get('X-Token-Unverified')).toBeNull();
    });

    it('should allow local validation in non-production when disableLocalValidation is false', async () => {
      const middleware = createAuthMiddleware({
        enabled: true,
        disableLocalValidation: false,
      });
      app.use('*', middleware);
      app.get('/protected', (c) => {
        const auth = getAuthContext(c);
        return c.json({ authenticated: auth.authenticated });
      });

      const token = createValidToken({ sub: 'user-123' });

      // Development environment without OAuth service
      const devEnv: EnvWithOAuth = {
        OAUTH: undefined,
        ENVIRONMENT: 'development',
      };

      const res = await app.fetch(
        new Request('http://test/protected', {
          headers: { Authorization: `Bearer ${token}` },
        }),
        devEnv
      );
      expect(res.status).toBe(200);

      const data = await res.json();
      expect(data.authenticated).toBe(true);
    });

    it('should prefer OAuth service even when environment is not production', async () => {
      const mockOAuth = createMockOAuthService({
        valid: true,
        user: {
          userId: 'oauth-user-456',
          email: 'oauth@example.com',
          roles: ['admin'],
        },
      });

      const middleware = createAuthMiddleware({ enabled: true });
      app.use('*', middleware);
      app.get('/protected', (c) => {
        const auth = getAuthContext(c);
        return c.json({
          userId: auth.userId,
          unverified: auth.unverified,
        });
      });

      // Development environment WITH OAuth service
      const devEnvWithOAuth: EnvWithOAuth = {
        OAUTH: mockOAuth,
        ENVIRONMENT: 'development',
      };

      const res = await app.fetch(
        new Request('http://test/protected', {
          headers: { Authorization: 'Bearer any-token' },
        }),
        devEnvWithOAuth
      );
      expect(res.status).toBe(200);

      const data = await res.json();
      expect(data.userId).toBe('oauth-user-456');
      expect(data.unverified).toBeUndefined();
    });
  });

  describe('checkPermission', () => {
    it('should return false for unauthenticated users', () => {
      const context: AuthContext = { authenticated: false, roles: [] };
      expect(checkPermission(context, 'read')).toBe(false);
      expect(checkPermission(context, 'write')).toBe(false);
      expect(checkPermission(context, 'admin')).toBe(false);
    });

    it('should grant all permissions to admin role', () => {
      const context: AuthContext = { authenticated: true, roles: ['admin'] };
      expect(checkPermission(context, 'read')).toBe(true);
      expect(checkPermission(context, 'write')).toBe(true);
      expect(checkPermission(context, 'admin')).toBe(true);
    });

    it('should grant read and write permissions to write role', () => {
      const context: AuthContext = { authenticated: true, roles: ['write'] };
      expect(checkPermission(context, 'read')).toBe(true);
      expect(checkPermission(context, 'write')).toBe(true);
      expect(checkPermission(context, 'admin')).toBe(false);
    });

    it('should grant only read permission to read role', () => {
      const context: AuthContext = { authenticated: true, roles: ['read'] };
      expect(checkPermission(context, 'read')).toBe(true);
      expect(checkPermission(context, 'write')).toBe(false);
      expect(checkPermission(context, 'admin')).toBe(false);
    });
  });

  describe('hasTableAccess', () => {
    it('should return false for unauthenticated users', () => {
      const context: AuthContext = { authenticated: false, roles: [] };
      expect(hasTableAccess(context, ['db'], 'users', 'read')).toBe(false);
    });

    it('should check basic permission for table access', () => {
      const readContext: AuthContext = { authenticated: true, roles: ['read'] };
      expect(hasTableAccess(readContext, ['db'], 'users', 'read')).toBe(true);
      expect(hasTableAccess(readContext, ['db'], 'users', 'write')).toBe(false);

      const writeContext: AuthContext = { authenticated: true, roles: ['write'] };
      expect(hasTableAccess(writeContext, ['db'], 'users', 'write')).toBe(true);
    });
  });
});

// ============================================================================
// Permissions Tests
// ============================================================================

describe('Permissions', () => {
  describe('PermissionLevel', () => {
    it('should have correct ordering', () => {
      expect(PermissionLevel.NONE).toBeLessThan(PermissionLevel.READ);
      expect(PermissionLevel.READ).toBeLessThan(PermissionLevel.WRITE);
      expect(PermissionLevel.WRITE).toBeLessThan(PermissionLevel.ADMIN);
      expect(PermissionLevel.ADMIN).toBeLessThan(PermissionLevel.OWNER);
    });
  });

  describe('parsePermissionLevel', () => {
    it('should parse permission level strings', () => {
      expect(parsePermissionLevel('none')).toBe(PermissionLevel.NONE);
      expect(parsePermissionLevel('read')).toBe(PermissionLevel.READ);
      expect(parsePermissionLevel('write')).toBe(PermissionLevel.WRITE);
      expect(parsePermissionLevel('admin')).toBe(PermissionLevel.ADMIN);
      expect(parsePermissionLevel('owner')).toBe(PermissionLevel.OWNER);
    });

    it('should be case insensitive', () => {
      expect(parsePermissionLevel('READ')).toBe(PermissionLevel.READ);
      expect(parsePermissionLevel('Write')).toBe(PermissionLevel.WRITE);
      expect(parsePermissionLevel('ADMIN')).toBe(PermissionLevel.ADMIN);
    });

    it('should return NONE for invalid strings', () => {
      expect(parsePermissionLevel('invalid')).toBe(PermissionLevel.NONE);
      expect(parsePermissionLevel('')).toBe(PermissionLevel.NONE);
    });
  });

  describe('permissionLevelToString', () => {
    it('should convert permission levels to strings', () => {
      expect(permissionLevelToString(PermissionLevel.NONE)).toBe('none');
      expect(permissionLevelToString(PermissionLevel.READ)).toBe('read');
      expect(permissionLevelToString(PermissionLevel.WRITE)).toBe('write');
      expect(permissionLevelToString(PermissionLevel.ADMIN)).toBe('admin');
      expect(permissionLevelToString(PermissionLevel.OWNER)).toBe('owner');
    });
  });

  describe('getRequiredPermissionLevel', () => {
    it('should return READ for read operations', () => {
      expect(getRequiredPermissionLevel('namespace:list')).toBe(PermissionLevel.READ);
      expect(getRequiredPermissionLevel('namespace:read')).toBe(PermissionLevel.READ);
      expect(getRequiredPermissionLevel('table:list')).toBe(PermissionLevel.READ);
      expect(getRequiredPermissionLevel('table:read')).toBe(PermissionLevel.READ);
    });

    it('should return WRITE for write operations', () => {
      expect(getRequiredPermissionLevel('table:create')).toBe(PermissionLevel.WRITE);
      expect(getRequiredPermissionLevel('table:write')).toBe(PermissionLevel.WRITE);
      expect(getRequiredPermissionLevel('table:commit')).toBe(PermissionLevel.WRITE);
    });

    it('should return ADMIN for admin operations', () => {
      expect(getRequiredPermissionLevel('namespace:create')).toBe(PermissionLevel.ADMIN);
      expect(getRequiredPermissionLevel('namespace:update')).toBe(PermissionLevel.ADMIN);
      expect(getRequiredPermissionLevel('table:rename')).toBe(PermissionLevel.ADMIN);
    });

    it('should return OWNER for owner operations', () => {
      expect(getRequiredPermissionLevel('namespace:drop')).toBe(PermissionLevel.OWNER);
      expect(getRequiredPermissionLevel('namespace:grant')).toBe(PermissionLevel.OWNER);
      expect(getRequiredPermissionLevel('namespace:revoke')).toBe(PermissionLevel.OWNER);
      expect(getRequiredPermissionLevel('table:drop')).toBe(PermissionLevel.OWNER);
      expect(getRequiredPermissionLevel('table:grant')).toBe(PermissionLevel.OWNER);
      expect(getRequiredPermissionLevel('table:revoke')).toBe(PermissionLevel.OWNER);
    });
  });

  describe('satisfiesPermissionLevel', () => {
    it('should return true when actual level meets or exceeds required', () => {
      expect(satisfiesPermissionLevel(PermissionLevel.READ, PermissionLevel.READ)).toBe(true);
      expect(satisfiesPermissionLevel(PermissionLevel.WRITE, PermissionLevel.READ)).toBe(true);
      expect(satisfiesPermissionLevel(PermissionLevel.ADMIN, PermissionLevel.WRITE)).toBe(true);
      expect(satisfiesPermissionLevel(PermissionLevel.OWNER, PermissionLevel.ADMIN)).toBe(true);
    });

    it('should return false when actual level is below required', () => {
      expect(satisfiesPermissionLevel(PermissionLevel.NONE, PermissionLevel.READ)).toBe(false);
      expect(satisfiesPermissionLevel(PermissionLevel.READ, PermissionLevel.WRITE)).toBe(false);
      expect(satisfiesPermissionLevel(PermissionLevel.WRITE, PermissionLevel.ADMIN)).toBe(false);
      expect(satisfiesPermissionLevel(PermissionLevel.ADMIN, PermissionLevel.OWNER)).toBe(false);
    });
  });

  describe('computeEffectiveLevel', () => {
    it('should return NONE for empty grants', () => {
      expect(computeEffectiveLevel([])).toBe(PermissionLevel.NONE);
    });

    it('should return highest level from grants', () => {
      const grants: PermissionGrant[] = [
        {
          id: '1',
          resourceType: 'namespace',
          resourceId: 'db',
          principalType: 'user',
          principalId: 'user-1',
          level: PermissionLevel.READ,
          createdAt: Date.now(),
          createdBy: 'admin',
        },
        {
          id: '2',
          resourceType: 'namespace',
          resourceId: 'db',
          principalType: 'user',
          principalId: 'user-1',
          level: PermissionLevel.WRITE,
          createdAt: Date.now(),
          createdBy: 'admin',
        },
      ];
      expect(computeEffectiveLevel(grants)).toBe(PermissionLevel.WRITE);
    });

    it('should filter out expired grants', () => {
      const now = Date.now();
      const grants: PermissionGrant[] = [
        {
          id: '1',
          resourceType: 'namespace',
          resourceId: 'db',
          principalType: 'user',
          principalId: 'user-1',
          level: PermissionLevel.ADMIN,
          createdAt: now - 7200000,
          createdBy: 'admin',
          expiresAt: now - 3600000, // Expired 1 hour ago
        },
        {
          id: '2',
          resourceType: 'namespace',
          resourceId: 'db',
          principalType: 'user',
          principalId: 'user-1',
          level: PermissionLevel.READ,
          createdAt: now,
          createdBy: 'admin',
        },
      ];
      expect(computeEffectiveLevel(grants)).toBe(PermissionLevel.READ);
    });

    it('should include non-expired grants', () => {
      const now = Date.now();
      const grants: PermissionGrant[] = [
        {
          id: '1',
          resourceType: 'namespace',
          resourceId: 'db',
          principalType: 'user',
          principalId: 'user-1',
          level: PermissionLevel.ADMIN,
          createdAt: now,
          createdBy: 'admin',
          expiresAt: now + 3600000, // Expires in 1 hour
        },
      ];
      expect(computeEffectiveLevel(grants)).toBe(PermissionLevel.ADMIN);
    });
  });

  describe('getCatalogPermissionLevel', () => {
    it('should return OWNER for owner role', () => {
      const context: AuthContext = { authenticated: true, roles: ['owner'] };
      expect(getCatalogPermissionLevel(context)).toBe(PermissionLevel.OWNER);
    });

    it('should return ADMIN for admin role', () => {
      const context: AuthContext = { authenticated: true, roles: ['admin'] };
      expect(getCatalogPermissionLevel(context)).toBe(PermissionLevel.ADMIN);
    });

    it('should return WRITE for write role', () => {
      const context: AuthContext = { authenticated: true, roles: ['write'] };
      expect(getCatalogPermissionLevel(context)).toBe(PermissionLevel.WRITE);
    });

    it('should return READ for read role', () => {
      const context: AuthContext = { authenticated: true, roles: ['read'] };
      expect(getCatalogPermissionLevel(context)).toBe(PermissionLevel.READ);
    });

    it('should return NONE for no matching roles', () => {
      const context: AuthContext = { authenticated: true, roles: ['custom-role'] };
      expect(getCatalogPermissionLevel(context)).toBe(PermissionLevel.NONE);
    });

    it('should prioritize higher roles', () => {
      const context: AuthContext = { authenticated: true, roles: ['read', 'write', 'admin'] };
      expect(getCatalogPermissionLevel(context)).toBe(PermissionLevel.ADMIN);
    });
  });

  describe('matchesPrincipal', () => {
    it('should match user grants by userId', () => {
      const context: AuthContext = {
        authenticated: true,
        userId: 'user-123',
        roles: [],
      };
      const grant: PermissionGrant = {
        id: '1',
        resourceType: 'namespace',
        resourceId: 'db',
        principalType: 'user',
        principalId: 'user-123',
        level: PermissionLevel.READ,
        createdAt: Date.now(),
        createdBy: 'admin',
      };
      expect(matchesPrincipal(context, grant)).toBe(true);
    });

    it('should not match user grants with different userId', () => {
      const context: AuthContext = {
        authenticated: true,
        userId: 'user-123',
        roles: [],
      };
      const grant: PermissionGrant = {
        id: '1',
        resourceType: 'namespace',
        resourceId: 'db',
        principalType: 'user',
        principalId: 'user-456',
        level: PermissionLevel.READ,
        createdAt: Date.now(),
        createdBy: 'admin',
      };
      expect(matchesPrincipal(context, grant)).toBe(false);
    });

    it('should match role grants by role membership', () => {
      const context: AuthContext = {
        authenticated: true,
        roles: ['developers', 'readers'],
      };
      const grant: PermissionGrant = {
        id: '1',
        resourceType: 'namespace',
        resourceId: 'db',
        principalType: 'role',
        principalId: 'developers',
        level: PermissionLevel.READ,
        createdAt: Date.now(),
        createdBy: 'admin',
      };
      expect(matchesPrincipal(context, grant)).toBe(true);
    });

    it('should not match role grants without role membership', () => {
      const context: AuthContext = {
        authenticated: true,
        roles: ['readers'],
      };
      const grant: PermissionGrant = {
        id: '1',
        resourceType: 'namespace',
        resourceId: 'db',
        principalType: 'role',
        principalId: 'developers',
        level: PermissionLevel.READ,
        createdAt: Date.now(),
        createdBy: 'admin',
      };
      expect(matchesPrincipal(context, grant)).toBe(false);
    });
  });

  describe('checkNamespacePermission', () => {
    it('should return false for unauthenticated users', () => {
      const context: AuthContext = { authenticated: false, roles: [] };
      expect(checkNamespacePermission(context, ['db'], 'namespace:read')).toBe(false);
    });

    it('should allow admin role for all namespace operations', () => {
      const context: AuthContext = { authenticated: true, roles: ['admin'] };
      expect(checkNamespacePermission(context, ['db'], 'namespace:list')).toBe(true);
      expect(checkNamespacePermission(context, ['db'], 'namespace:create')).toBe(true);
      expect(checkNamespacePermission(context, ['db'], 'namespace:drop')).toBe(true);
    });

    it('should allow owner role for all namespace operations', () => {
      const context: AuthContext = { authenticated: true, roles: ['owner'] };
      expect(checkNamespacePermission(context, ['db'], 'namespace:drop')).toBe(true);
      expect(checkNamespacePermission(context, ['db'], 'namespace:grant')).toBe(true);
    });

    it('should check catalog-level roles', () => {
      const readContext: AuthContext = { authenticated: true, roles: ['read'] };
      expect(checkNamespacePermission(readContext, ['db'], 'namespace:read')).toBe(true);
      expect(checkNamespacePermission(readContext, ['db'], 'namespace:create')).toBe(false);

      const writeContext: AuthContext = { authenticated: true, roles: ['write'] };
      expect(checkNamespacePermission(writeContext, ['db'], 'namespace:read')).toBe(true);
      expect(checkNamespacePermission(writeContext, ['db'], 'namespace:create')).toBe(false);
    });

    it('should check namespace-specific grants', () => {
      const context: AuthContext = {
        authenticated: true,
        userId: 'user-123',
        roles: [],
      };
      const grants: PermissionGrant[] = [
        {
          id: '1',
          resourceType: 'namespace',
          resourceId: 'db',
          principalType: 'user',
          principalId: 'user-123',
          level: PermissionLevel.WRITE,
          createdAt: Date.now(),
          createdBy: 'admin',
        },
      ];

      expect(checkNamespacePermission(context, ['db'], 'namespace:read', grants)).toBe(true);
      expect(checkNamespacePermission(context, ['db'], 'namespace:create', grants)).toBe(false);
    });

    it('should support parent namespace inheritance', () => {
      const context: AuthContext = {
        authenticated: true,
        userId: 'user-123',
        roles: [],
      };
      const grants: PermissionGrant[] = [
        {
          id: '1',
          resourceType: 'namespace',
          resourceId: 'parent',
          principalType: 'user',
          principalId: 'user-123',
          level: PermissionLevel.ADMIN,
          createdAt: Date.now(),
          createdBy: 'admin',
        },
      ];

      // Child namespace should inherit from parent
      expect(checkNamespacePermission(context, ['parent', 'child'], 'namespace:create', grants)).toBe(true);
    });
  });

  describe('checkTablePermission', () => {
    it('should return false for unauthenticated users', () => {
      const context: AuthContext = { authenticated: false, roles: [] };
      expect(checkTablePermission(context, ['db'], 'users', 'table:read')).toBe(false);
    });

    it('should allow admin role for all table operations', () => {
      const context: AuthContext = { authenticated: true, roles: ['admin'] };
      expect(checkTablePermission(context, ['db'], 'users', 'table:read')).toBe(true);
      expect(checkTablePermission(context, ['db'], 'users', 'table:write')).toBe(true);
      expect(checkTablePermission(context, ['db'], 'users', 'table:drop')).toBe(true);
    });

    it('should check catalog-level roles', () => {
      const readContext: AuthContext = { authenticated: true, roles: ['read'] };
      expect(checkTablePermission(readContext, ['db'], 'users', 'table:read')).toBe(true);
      expect(checkTablePermission(readContext, ['db'], 'users', 'table:write')).toBe(false);

      const writeContext: AuthContext = { authenticated: true, roles: ['write'] };
      expect(checkTablePermission(writeContext, ['db'], 'users', 'table:read')).toBe(true);
      expect(checkTablePermission(writeContext, ['db'], 'users', 'table:write')).toBe(true);
      expect(checkTablePermission(writeContext, ['db'], 'users', 'table:drop')).toBe(false);
    });

    it('should check table-specific grants', () => {
      const context: AuthContext = {
        authenticated: true,
        userId: 'user-123',
        roles: [],
      };
      const grants: PermissionGrant[] = [
        {
          id: '1',
          resourceType: 'table',
          resourceId: 'db\x00users',
          principalType: 'user',
          principalId: 'user-123',
          level: PermissionLevel.WRITE,
          createdAt: Date.now(),
          createdBy: 'admin',
        },
      ];

      expect(checkTablePermission(context, ['db'], 'users', 'table:read', grants)).toBe(true);
      expect(checkTablePermission(context, ['db'], 'users', 'table:write', grants)).toBe(true);
      expect(checkTablePermission(context, ['db'], 'users', 'table:drop', grants)).toBe(false);
    });

    it('should inherit permissions from namespace', () => {
      const context: AuthContext = {
        authenticated: true,
        userId: 'user-123',
        roles: [],
      };
      const grants: PermissionGrant[] = [
        {
          id: '1',
          resourceType: 'namespace',
          resourceId: 'db',
          principalType: 'user',
          principalId: 'user-123',
          level: PermissionLevel.ADMIN,
          createdAt: Date.now(),
          createdBy: 'admin',
        },
      ];

      // Table should inherit from namespace
      expect(checkTablePermission(context, ['db'], 'users', 'table:read', grants)).toBe(true);
      expect(checkTablePermission(context, ['db'], 'users', 'table:rename', grants)).toBe(true);
    });

    it('should inherit permissions from parent namespace', () => {
      const context: AuthContext = {
        authenticated: true,
        userId: 'user-123',
        roles: [],
      };
      const grants: PermissionGrant[] = [
        {
          id: '1',
          resourceType: 'namespace',
          resourceId: 'parent',
          principalType: 'user',
          principalId: 'user-123',
          level: PermissionLevel.WRITE,
          createdAt: Date.now(),
          createdBy: 'admin',
        },
      ];

      // Table in nested namespace should inherit from parent
      expect(checkTablePermission(context, ['parent', 'child'], 'users', 'table:write', grants)).toBe(true);
    });
  });

  describe('checkCatalogPermission', () => {
    it('should return false for unauthenticated users', () => {
      const context: AuthContext = { authenticated: false, roles: [] };
      expect(checkCatalogPermission(context, 'namespace:list')).toBe(false);
    });

    it('should allow admin for all operations', () => {
      const context: AuthContext = { authenticated: true, roles: ['admin'] };
      expect(checkCatalogPermission(context, 'namespace:list')).toBe(true);
      expect(checkCatalogPermission(context, 'namespace:drop')).toBe(true);
      expect(checkCatalogPermission(context, 'table:drop')).toBe(true);
    });

    it('should check catalog-level permission from roles', () => {
      const readContext: AuthContext = { authenticated: true, roles: ['read'] };
      expect(checkCatalogPermission(readContext, 'namespace:list')).toBe(true);
      expect(checkCatalogPermission(readContext, 'table:create')).toBe(false);

      const writeContext: AuthContext = { authenticated: true, roles: ['write'] };
      expect(checkCatalogPermission(writeContext, 'table:create')).toBe(true);
      expect(checkCatalogPermission(writeContext, 'namespace:create')).toBe(false);
    });
  });

  describe('getEffectivePermission', () => {
    it('should return catalog level for users without grants', () => {
      const context: AuthContext = {
        authenticated: true,
        userId: 'user-123',
        roles: ['write'],
      };

      const result = getEffectivePermission(context, 'namespace', 'db', []);
      expect(result.level).toBe(PermissionLevel.WRITE);
      expect(result.source).toBe('catalog');
    });

    it('should return owner level for owner role', () => {
      const context: AuthContext = {
        authenticated: true,
        userId: 'user-123',
        roles: ['owner'],
      };

      const result = getEffectivePermission(context, 'namespace', 'db', []);
      expect(result.level).toBe(PermissionLevel.OWNER);
      expect(result.source).toBe('role');
    });

    it('should return admin level for admin role', () => {
      const context: AuthContext = {
        authenticated: true,
        userId: 'user-123',
        roles: ['admin'],
      };

      const result = getEffectivePermission(context, 'namespace', 'db', []);
      expect(result.level).toBe(PermissionLevel.ADMIN);
      expect(result.source).toBe('role');
    });

    it('should use direct grant when higher than catalog level', () => {
      const context: AuthContext = {
        authenticated: true,
        userId: 'user-123',
        roles: ['read'],
      };
      const grants: PermissionGrant[] = [
        {
          id: 'grant-1',
          resourceType: 'namespace',
          resourceId: 'db',
          principalType: 'user',
          principalId: 'user-123',
          level: PermissionLevel.ADMIN,
          createdAt: Date.now(),
          createdBy: 'admin',
        },
      ];

      const result = getEffectivePermission(context, 'namespace', 'db', grants);
      expect(result.level).toBe(PermissionLevel.ADMIN);
      expect(result.source).toBe('direct');
      expect(result.grantId).toBe('grant-1');
    });

    it('should use inherited permission for tables', () => {
      const context: AuthContext = {
        authenticated: true,
        userId: 'user-123',
        roles: [],
      };
      const grants: PermissionGrant[] = [
        {
          id: 'grant-1',
          resourceType: 'namespace',
          resourceId: 'db',
          principalType: 'user',
          principalId: 'user-123',
          level: PermissionLevel.WRITE,
          createdAt: Date.now(),
          createdBy: 'admin',
        },
      ];

      const result = getEffectivePermission(context, 'table', 'db\x00users', grants);
      expect(result.level).toBe(PermissionLevel.WRITE);
      expect(result.source).toBe('inherited');
    });

    it('should prefer direct table grant over inherited namespace grant', () => {
      const context: AuthContext = {
        authenticated: true,
        userId: 'user-123',
        roles: [],
      };
      const grants: PermissionGrant[] = [
        {
          id: 'grant-1',
          resourceType: 'namespace',
          resourceId: 'db',
          principalType: 'user',
          principalId: 'user-123',
          level: PermissionLevel.READ,
          createdAt: Date.now(),
          createdBy: 'admin',
        },
        {
          id: 'grant-2',
          resourceType: 'table',
          resourceId: 'db\x00users',
          principalType: 'user',
          principalId: 'user-123',
          level: PermissionLevel.ADMIN,
          createdAt: Date.now(),
          createdBy: 'admin',
        },
      ];

      const result = getEffectivePermission(context, 'table', 'db\x00users', grants);
      expect(result.level).toBe(PermissionLevel.ADMIN);
      expect(result.source).toBe('direct');
      expect(result.grantId).toBe('grant-2');
    });

    it('should inherit from parent namespace for nested namespaces', () => {
      const context: AuthContext = {
        authenticated: true,
        userId: 'user-123',
        roles: [],
      };
      const grants: PermissionGrant[] = [
        {
          id: 'grant-1',
          resourceType: 'namespace',
          resourceId: 'parent',
          principalType: 'user',
          principalId: 'user-123',
          level: PermissionLevel.WRITE,
          createdAt: Date.now(),
          createdBy: 'admin',
        },
      ];

      const result = getEffectivePermission(context, 'namespace', 'parent\x1fchild', grants);
      expect(result.level).toBe(PermissionLevel.WRITE);
      expect(result.source).toBe('inherited');
    });
  });
});

// ============================================================================
// oauth.do Integration Tests
// ============================================================================

describe('oauth.do Integration', () => {
  describe('oauthUserToAuthContext', () => {
    it('should convert oauth.do user to AuthContext', () => {
      const user: OAuthDoUser = {
        id: 'user-123',
        email: 'test@example.com',
        name: 'Test User',
        organizationId: 'org-456',
        roles: ['admin', 'write'],
        permissions: ['namespace:create', 'table:write'],
      };

      const context = oauthUserToAuthContext(user, 'token-xyz');

      expect(context.authenticated).toBe(true);
      expect(context.userId).toBe('user-123');
      expect(context.email).toBe('test@example.com');
      expect(context.organizationId).toBe('org-456');
      expect(context.roles).toEqual(['admin', 'write']);
      expect(context.scopes).toEqual(['namespace:create', 'table:write']);
    });

    it('should return unauthenticated context for null user', () => {
      const context = oauthUserToAuthContext(null);

      expect(context.authenticated).toBe(false);
      expect(context.roles).toEqual([]);
      expect(context.userId).toBeUndefined();
    });

    it('should handle user with minimal fields', () => {
      const user: OAuthDoUser = {
        id: 'user-123',
      };

      const context = oauthUserToAuthContext(user, 'token');

      expect(context.authenticated).toBe(true);
      expect(context.userId).toBe('user-123');
      expect(context.email).toBeUndefined();
      expect(context.roles).toEqual([]);
    });
  });

  describe('authContextToOAuthUser', () => {
    it('should convert AuthContext to oauth.do user', () => {
      const context: AuthContext = {
        authenticated: true,
        userId: 'user-123',
        email: 'test@example.com',
        organizationId: 'org-456',
        roles: ['admin'],
        scopes: ['read', 'write'],
      };

      const user = authContextToOAuthUser(context);

      expect(user).not.toBeNull();
      expect(user?.id).toBe('user-123');
      expect(user?.email).toBe('test@example.com');
      expect(user?.organizationId).toBe('org-456');
      expect(user?.roles).toEqual(['admin']);
      expect(user?.permissions).toEqual(['read', 'write']);
    });

    it('should return null for unauthenticated context', () => {
      const context: AuthContext = {
        authenticated: false,
        roles: [],
      };

      const user = authContextToOAuthUser(context);

      expect(user).toBeNull();
    });

    it('should return null for context without userId', () => {
      const context: AuthContext = {
        authenticated: true,
        roles: ['read'],
      };

      const user = authContextToOAuthUser(context);

      expect(user).toBeNull();
    });
  });

  describe('validateTokenViaServiceBinding', () => {
    it('should return error when service binding is not configured', async () => {
      const result = await validateTokenViaServiceBinding(undefined, 'token');

      expect(result.valid).toBe(false);
      expect(result.error?.code).toBe('NO_SERVICE_BINDING');
    });

    it('should validate token using service binding', async () => {
      const mockOAuth = {
        fetch: async (_request: Request): Promise<Response> => {
          return Response.json({
            valid: true,
            user: {
              id: 'user-123',
              email: 'test@example.com',
              roles: ['write'],
            },
          });
        },
      };

      const result = await validateTokenViaServiceBinding(mockOAuth, 'valid-token');

      expect(result.valid).toBe(true);
      expect(result.user?.id).toBe('user-123');
      expect(result.user?.email).toBe('test@example.com');
    });

    it('should handle validation failure from service', async () => {
      const mockOAuth = {
        fetch: async (_request: Request): Promise<Response> => {
          return Response.json(
            {
              valid: false,
              error: { code: 'INVALID_TOKEN', message: 'Token expired' },
            },
            { status: 401 }
          );
        },
      };

      const result = await validateTokenViaServiceBinding(mockOAuth, 'expired-token');

      expect(result.valid).toBe(false);
      expect(result.error?.code).toBe('INVALID_TOKEN');
      expect(result.error?.message).toBe('Token expired');
    });

    it('should handle service errors gracefully', async () => {
      const mockOAuth = {
        fetch: async (_request: Request): Promise<Response> => {
          throw new Error('Network error');
        },
      };

      const result = await validateTokenViaServiceBinding(mockOAuth, 'token');

      expect(result.valid).toBe(false);
      expect(result.error?.code).toBe('OAUTH_ERROR');
      expect(result.error?.message).toBe('Network error');
    });
  });

  describe('createOAuthDoMiddleware', () => {
    let app: Hono<{ Bindings: OAuthDoEnv }>;

    beforeEach(() => {
      app = new Hono<{ Bindings: OAuthDoEnv }>();
    });

    it('should skip authentication when disabled', async () => {
      app.use('*', createOAuthDoMiddleware({ enabled: false }));
      app.get('/test', (c) => {
        const auth = getAuthContext(c);
        return c.json({ authenticated: auth.authenticated });
      });

      const res = await app.fetch(new Request('http://test/test'));
      expect(res.status).toBe(200);
      const data = await res.json();
      expect(data.authenticated).toBe(false);
    });

    it('should allow public paths without authentication', async () => {
      app.use('*', createOAuthDoMiddleware({
        publicPaths: ['/health', '/v1/config'],
      }));
      app.get('/health', (c) => c.json({ status: 'ok' }));

      const res = await app.fetch(new Request('http://test/health'));
      expect(res.status).toBe(200);
    });

    it('should support wildcard public paths', async () => {
      app.use('*', createOAuthDoMiddleware({
        publicPaths: ['/public/*'],
      }));
      app.get('/public/docs', (c) => c.json({ docs: [] }));

      const res = await app.fetch(new Request('http://test/public/docs'));
      expect(res.status).toBe(200);
    });

    it('should require authorization for protected paths', async () => {
      app.use('*', createOAuthDoMiddleware({}));
      app.get('/protected', (c) => c.json({ data: 'secret' }));

      const res = await app.fetch(new Request('http://test/protected'));
      expect(res.status).toBe(401);
      expect(res.headers.get('WWW-Authenticate')).toContain('Bearer');
    });

    it('should allow anonymous read when configured', async () => {
      app.use('*', createOAuthDoMiddleware({ allowAnonymousRead: true }));
      app.get('/data', (c) => {
        const auth = getAuthContext(c);
        return c.json({ roles: auth.roles });
      });

      const res = await app.fetch(new Request('http://test/data'));
      expect(res.status).toBe(200);
      const data = await res.json();
      expect(data.roles).toEqual(['read']);
    });

    it('should authenticate with valid JWT token', async () => {
      app.use('*', createOAuthDoMiddleware({}));
      app.get('/protected', (c) => {
        const auth = getAuthContext(c);
        return c.json({
          authenticated: auth.authenticated,
          userId: auth.userId,
        });
      });

      const token = createValidToken({ sub: 'user-123', roles: ['admin'] });
      const res = await app.fetch(
        new Request('http://test/protected', {
          headers: { Authorization: `Bearer ${token}` },
        })
      );

      expect(res.status).toBe(200);
      const data = await res.json();
      expect(data.authenticated).toBe(true);
      expect(data.userId).toBe('user-123');
    });

    it('should authenticate with API key when verifier provided', async () => {
      const verifyApiKey = async (key: string): Promise<OAuthDoUser | null> => {
        if (key === 'valid-api-key') {
          return {
            id: 'api-user-123',
            email: 'api@example.com',
            roles: ['write'],
          };
        }
        return null;
      };

      app.use('*', createOAuthDoMiddleware({ verifyApiKey }));
      app.get('/protected', (c) => {
        const auth = getAuthContext(c);
        return c.json({
          authenticated: auth.authenticated,
          userId: auth.userId,
        });
      });

      const res = await app.fetch(
        new Request('http://test/protected', {
          headers: { 'X-API-Key': 'valid-api-key' },
        })
      );

      expect(res.status).toBe(200);
      const data = await res.json();
      expect(data.authenticated).toBe(true);
      expect(data.userId).toBe('api-user-123');
    });

    it('should use oauth.do service binding when available', async () => {
      const mockEnv: OAuthDoEnv = {
        OAUTH: {
          fetch: async (_request: Request): Promise<Response> => {
            return Response.json({
              valid: true,
              user: {
                id: 'oauth-user-456',
                email: 'oauth@example.com',
                roles: ['admin'],
              },
            });
          },
        },
        ENVIRONMENT: 'test',
        AUTH_ENABLED: 'true',
      };

      app.use('*', createOAuthDoMiddleware({}));
      app.get('/protected', (c) => {
        const auth = getAuthContext(c);
        return c.json({
          authenticated: auth.authenticated,
          userId: auth.userId,
        });
      });

      const res = await app.fetch(
        new Request('http://test/protected', {
          headers: { Authorization: 'Bearer any-token' },
        }),
        mockEnv
      );

      expect(res.status).toBe(200);
      const data = await res.json();
      expect(data.authenticated).toBe(true);
      expect(data.userId).toBe('oauth-user-456');
    });

    it('should add auth headers to response', async () => {
      app.use('*', createOAuthDoMiddleware({}));
      app.get('/protected', (c) => c.json({ success: true }));

      const token = createValidToken({
        sub: 'user-123',
        email: 'test@example.com',
        roles: ['admin'],
        org_id: 'org-456',
      });

      const res = await app.fetch(
        new Request('http://test/protected', {
          headers: { Authorization: `Bearer ${token}` },
        })
      );

      expect(res.status).toBe(200);
      expect(res.headers.get('X-User-Id')).toBe('user-123');
      expect(res.headers.get('X-User-Email')).toBe('test@example.com');
      expect(res.headers.get('X-Organization-Id')).toBe('org-456');
      expect(res.headers.get('X-User-Roles')).toBe('admin');
    });
  });
});
