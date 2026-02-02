/**
 * oauth.do Integration Module
 *
 * Integrates the iceberg.do service with oauth.do for authentication.
 * Supports multiple authentication methods:
 * - Service binding (Cloudflare Workers service-to-service)
 * - Direct JWT validation via JWKS
 * - API key authentication
 *
 * @see https://oauth.do
 * @see https://github.com/dot-do/oauth.do
 */

import type { Context, MiddlewareHandler } from 'hono';
import type { AuthContext, AuthVariables } from './middleware.js';

// ============================================================================
// Types
// ============================================================================

/**
 * oauth.do user type (from oauth.do/hono)
 */
export interface OAuthDoUser {
  id: string;
  email?: string;
  name?: string;
  organizationId?: string;
  roles?: string[];
  permissions?: string[];
  metadata?: Record<string, unknown>;
}

/**
 * oauth.do Hono middleware variables
 */
export interface OAuthDoVariables {
  user: OAuthDoUser | null;
  userId: string | null;
  isAuth: boolean;
  token: string | null;
}

/**
 * Combined variables for iceberg + oauth.do
 */
export type IcebergOAuthVariables = AuthVariables & OAuthDoVariables;

/**
 * Configuration for oauth.do integration
 */
export interface OAuthDoConfig {
  /**
   * JWKS URI for JWT verification
   * Reserved for future use with direct JWKS validation
   * @example 'https://api.workos.com/sso/jwks/client_xxx'
   */
  jwksUri?: string;

  /**
   * WorkOS Client ID for audience verification
   * Reserved for future use with direct JWKS validation
   */
  clientId?: string;

  /**
   * Cookie name for JWT token
   * Reserved for future use with cookie-based auth
   * @default 'auth'
   */
  cookieName?: string;

  /**
   * API key verification function
   * When provided, enables X-API-Key authentication
   */
  verifyApiKey?: (key: string, c: Context) => Promise<OAuthDoUser | null>;

  /**
   * Paths to skip authentication
   */
  publicPaths?: string[];

  /**
   * Allow anonymous read access
   */
  allowAnonymousRead?: boolean;

  /**
   * Enable authentication
   * @default true
   */
  enabled?: boolean;

  /**
   * Cache TTL for JWKS in seconds
   * Reserved for future use with direct JWKS validation
   * @default 3600
   */
  jwksCacheTtl?: number;
}

/**
 * Environment with oauth.do bindings
 */
export interface OAuthDoEnv {
  /** Service binding to oauth.do worker */
  OAUTH?: {
    fetch(request: Request): Promise<Response>;
  };
  /** JWKS URI environment variable */
  OAUTH_JWKS_URI?: string;
  /** WorkOS Client ID */
  OAUTH_CLIENT_ID?: string;
  /** Environment name */
  ENVIRONMENT?: string;
  /** Auth enabled flag */
  AUTH_ENABLED?: string;
  /** Allow anonymous read */
  AUTH_ALLOW_ANONYMOUS_READ?: string;
}

// ============================================================================
// Converters
// ============================================================================

/**
 * Convert oauth.do user to iceberg AuthContext
 */
export function oauthUserToAuthContext(user: OAuthDoUser | null, _token?: string | null): AuthContext {
  if (!user) {
    return {
      authenticated: false,
      roles: [],
    };
  }

  return {
    authenticated: true,
    userId: user.id,
    email: user.email,
    roles: user.roles ?? [],
    organizationId: user.organizationId,
    scopes: user.permissions,
  };
}

/**
 * Convert iceberg AuthContext to oauth.do user
 */
export function authContextToOAuthUser(context: AuthContext): OAuthDoUser | null {
  if (!context.authenticated || !context.userId) {
    return null;
  }

  return {
    id: context.userId,
    email: context.email,
    roles: context.roles,
    organizationId: context.organizationId,
    permissions: context.scopes,
  };
}

// ============================================================================
// Service Binding Validation
// ============================================================================

/**
 * Response from oauth.do token validation endpoint
 */
interface OAuthDoValidationResponse {
  valid: boolean;
  user?: OAuthDoUser;
  error?: {
    code: string;
    message: string;
  };
}

/**
 * Validate token using oauth.do service binding
 */
export async function validateTokenViaServiceBinding(
  oauth: OAuthDoEnv['OAUTH'],
  token: string
): Promise<OAuthDoValidationResponse> {
  if (!oauth) {
    return {
      valid: false,
      error: {
        code: 'NO_SERVICE_BINDING',
        message: 'oauth.do service binding not configured',
      },
    };
  }

  try {
    const response = await oauth.fetch(
      new Request('https://oauth.do/api/validate', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`,
        },
        body: JSON.stringify({ token }),
      })
    );

    if (!response.ok) {
      try {
        const error = await response.json() as { error?: { code?: string; message?: string }; message?: string };
        return {
          valid: false,
          error: {
            code: error.error?.code ?? 'VALIDATION_FAILED',
            message: error.error?.message ?? error.message ?? 'Token validation failed',
          },
        };
      } catch {
        return {
          valid: false,
          error: {
            code: 'VALIDATION_FAILED',
            message: `Token validation failed with status ${response.status}`,
          },
        };
      }
    }

    return await response.json() as OAuthDoValidationResponse;
  } catch (err) {
    return {
      valid: false,
      error: {
        code: 'OAUTH_ERROR',
        message: err instanceof Error ? err.message : 'OAuth service error',
      },
    };
  }
}

// ============================================================================
// Middleware Factory
// ============================================================================

// Type for the middleware context
type OAuthMiddlewareContext = Context<{
  Bindings: OAuthDoEnv;
  Variables: IcebergOAuthVariables;
}>;

/**
 * Create oauth.do integrated authentication middleware
 *
 * This middleware integrates with oauth.do using either:
 * 1. Service binding (OAUTH env binding) - for Cloudflare Workers
 * 2. Direct JWKS validation - for standalone JWT verification (future)
 * 3. API key verification - for programmatic access
 *
 * @example
 * ```ts
 * import { createOAuthDoMiddleware } from './auth/oauth-do.js';
 *
 * app.use('/*', createOAuthDoMiddleware({
 *   publicPaths: ['/health', '/v1/config'],
 *   allowAnonymousRead: true,
 * }));
 * ```
 */
export function createOAuthDoMiddleware(config: OAuthDoConfig = {}): MiddlewareHandler<{
  Bindings: OAuthDoEnv;
  Variables: IcebergOAuthVariables;
}> {
  const {
    // Reserved for future JWKS validation
    jwksUri: _jwksUri,
    clientId: _clientId,
    cookieName: _cookieName = 'auth',
    jwksCacheTtl: _jwksCacheTtl = 3600,
    // Active config
    verifyApiKey,
    publicPaths = ['/health', '/v1/config'],
    allowAnonymousRead = false,
    enabled = true,
  } = config;

  return async (c: OAuthMiddlewareContext, next) => {
    // Initialize oauth.do variables
    c.set('user', null);
    c.set('userId', null);
    c.set('isAuth', false);
    c.set('token', null);

    // Check if auth is enabled
    const authEnabled = enabled && (c.env?.AUTH_ENABLED !== 'false');
    if (!authEnabled) {
      c.set('auth', { authenticated: false, roles: [] } as AuthContext);
      return next();
    }

    const pathname = new URL(c.req.url).pathname;

    // Check public paths
    for (const path of publicPaths) {
      if (path.endsWith('*')) {
        if (pathname.startsWith(path.slice(0, -1))) {
          c.set('auth', { authenticated: false, roles: [] } as AuthContext);
          return next();
        }
      } else if (pathname === path) {
        c.set('auth', { authenticated: false, roles: [] } as AuthContext);
        return next();
      }
    }

    // Try to extract token from Authorization header
    const authHeader = c.req.header('Authorization');
    let token: string | null = null;

    if (authHeader?.startsWith('Bearer ')) {
      token = authHeader.slice(7).trim();
    }

    // Try API key authentication
    const apiKey = c.req.header('X-API-Key');
    if (!token && apiKey && verifyApiKey) {
      const user = await verifyApiKey(apiKey, c);
      if (user) {
        c.set('auth', oauthUserToAuthContext(user, apiKey));
        c.set('user', user);
        c.set('userId', user.id);
        c.set('isAuth', true);
        c.set('token', apiKey);
        return next();
      }
    }

    // Allow anonymous read if configured
    if (!token && !apiKey) {
      const allowAnonRead = allowAnonymousRead || c.env?.AUTH_ALLOW_ANONYMOUS_READ === 'true';
      if (allowAnonRead && isReadRequest(c.req.method)) {
        c.set('auth', { authenticated: false, roles: ['read'] } as AuthContext);
        return next();
      }

      return c.json(
        {
          error: {
            message: 'Authorization required',
            type: 'Unauthorized',
            code: 401,
          },
        },
        401,
        {
          'WWW-Authenticate': 'Bearer realm="iceberg.do"',
        }
      );
    }

    if (!token) {
      return c.json(
        {
          error: {
            message: 'Bearer token or API key required',
            type: 'Unauthorized',
            code: 401,
          },
        },
        401,
        {
          'WWW-Authenticate': 'Bearer realm="iceberg.do"',
        }
      );
    }

    // Validate token using service binding if available
    if (c.env?.OAUTH) {
      const validation = await validateTokenViaServiceBinding(c.env.OAUTH, token);
      if (validation.valid && validation.user) {
        const authContext = oauthUserToAuthContext(validation.user, token);
        c.set('auth', authContext);
        c.set('user', validation.user);
        c.set('userId', validation.user.id);
        c.set('isAuth', true);
        c.set('token', token);
        await next();
        addAuthHeaders(authContext, c);
        return;
      } else if (!validation.valid) {
        return c.json(
          {
            error: {
              message: validation.error?.message ?? 'Token validation failed',
              type: 'Unauthorized',
              code: 401,
            },
          },
          401,
          {
            'WWW-Authenticate': `Bearer realm="iceberg.do", error="invalid_token"`,
          }
        );
      }
    }

    // Fallback: Try local JWT parsing (development mode)
    const user = parseJwtLocally(token);
    if (user) {
      const authContext = oauthUserToAuthContext(user, token);
      c.set('auth', authContext);
      c.set('user', user);
      c.set('userId', user.id);
      c.set('isAuth', true);
      c.set('token', token);
      await next();
      addAuthHeaders(authContext, c);
      return;
    }

    return c.json(
      {
        error: {
          message: 'Invalid token',
          type: 'Unauthorized',
          code: 401,
        },
      },
      401,
      {
        'WWW-Authenticate': `Bearer realm="iceberg.do", error="invalid_token"`,
      }
    );
  };
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Check if request is a read-only operation
 */
function isReadRequest(method: string): boolean {
  return method === 'GET' || method === 'HEAD' || method === 'OPTIONS';
}

/**
 * Add authentication headers to response
 */
function addAuthHeaders(auth: AuthContext, c: Context): void {
  if (auth.authenticated && auth.userId) {
    c.header('X-User-Id', auth.userId);
    if (auth.email) {
      c.header('X-User-Email', auth.email);
    }
    if (auth.organizationId) {
      c.header('X-Organization-Id', auth.organizationId);
    }
    if (auth.roles.length > 0) {
      c.header('X-User-Roles', auth.roles.join(','));
    }
  }
}

/**
 * Parse JWT locally without signature verification (development/fallback)
 */
function parseJwtLocally(token: string): OAuthDoUser | null {
  try {
    const parts = token.split('.');
    if (parts.length !== 3) {
      return null;
    }

    // Decode payload
    let base64 = parts[1].replace(/-/g, '+').replace(/_/g, '/');
    while (base64.length % 4) {
      base64 += '=';
    }
    const payload = JSON.parse(atob(base64));

    if (!payload.sub) {
      return null;
    }

    // Check expiration
    if (payload.exp) {
      const now = Math.floor(Date.now() / 1000);
      if (payload.exp < now) {
        return null;
      }
    }

    return {
      id: payload.sub,
      email: payload.email,
      name: payload.name,
      organizationId: payload.org_id,
      roles: payload.roles ?? [],
      permissions: payload.permissions,
      metadata: extractMetadata(payload),
    };
  } catch {
    return null;
  }
}

/**
 * Extract non-standard claims as metadata
 */
function extractMetadata(claims: Record<string, unknown>): Record<string, unknown> | undefined {
  const reservedClaims = new Set([
    'sub', 'email', 'name', 'roles', 'permissions', 'org_id',
    'iss', 'aud', 'exp', 'iat', 'nbf', 'jti',
  ]);

  const metadata: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(claims)) {
    if (!reservedClaims.has(key)) {
      metadata[key] = value;
    }
  }

  return Object.keys(metadata).length > 0 ? metadata : undefined;
}

// ============================================================================
// Exports
// ============================================================================

export {
  createOAuthDoMiddleware as default,
};
