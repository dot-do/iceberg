/**
 * Auth Middleware
 *
 * Authentication middleware for Iceberg REST server using oauth.do.
 * Provides Bearer token authentication with user context extraction.
 *
 * @see https://oauth.do
 */

import type { Context, Next } from 'hono';

// ============================================================================
// Types
// ============================================================================

export interface AuthConfig {
  /** OAuth provider URL */
  providerUrl?: string;
  /** Enable/disable authentication */
  enabled: boolean;
  /** Allow anonymous read access */
  allowAnonymousRead?: boolean;
  /** Public paths that skip authentication */
  publicPaths?: string[];
  /**
   * Disable local JWT validation entirely.
   * When true, authentication will fail if the OAuth service is unavailable.
   * Recommended for production environments to prevent token forgery attacks.
   * @default false
   */
  disableLocalValidation?: boolean;
}

export interface AuthContext {
  /** User ID */
  userId?: string;
  /** User email */
  email?: string;
  /** User roles */
  roles: string[];
  /** Is authenticated */
  authenticated: boolean;
  /** Organization ID */
  organizationId?: string;
  /** Token scopes */
  scopes?: string[];
  /**
   * Indicates if the token was validated without cryptographic signature verification.
   * When true, the token claims should not be fully trusted.
   * This is set when local validation is used in non-production environments.
   */
  unverified?: boolean;
}

export type Permission = 'read' | 'write' | 'admin';

export interface TablePermissions {
  namespace: string[];
  name: string;
  permissions: Permission[];
}

/**
 * User context extracted from OAuth tokens
 */
export interface UserContext {
  userId: string;
  email?: string;
  roles: string[];
  permissions?: string[];
  organizationId?: string;
  metadata?: Record<string, unknown>;
}

/**
 * Response from oauth.do token validation
 */
interface OAuthValidationResponse {
  valid: boolean;
  user?: UserContext;
  error?: {
    code: string;
    message: string;
  };
  /**
   * Indicates the token was parsed without cryptographic signature verification.
   * Set to true when using local validation fallback.
   */
  unverified?: boolean;
}

/**
 * OAuth service binding interface
 */
export interface OAuthService {
  fetch(request: Request): Promise<Response>;
}

/**
 * Environment with OAuth binding
 */
export interface EnvWithOAuth {
  OAUTH?: OAuthService;
  ENVIRONMENT?: string;
}

// ============================================================================
// JWT Helpers
// ============================================================================

/**
 * Decode a base64url-encoded string to UTF-8
 */
function decodeBase64Url(input: string): string {
  let base64 = input.replace(/-/g, '+').replace(/_/g, '/');
  while (base64.length % 4) {
    base64 += '=';
  }
  return atob(base64);
}

/**
 * Parse JWT claims without signature verification
 * (signature verification is handled by oauth.do)
 */
function parseJwtClaims(token: string): Record<string, unknown> | null {
  try {
    const parts = token.split('.');
    if (parts.length !== 3) {
      return null;
    }
    const payload = decodeBase64Url(parts[1]);
    return JSON.parse(payload);
  } catch {
    return null;
  }
}

/**
 * Extract user context from JWT claims
 */
export function extractUserContext(token: string): UserContext | null {
  const claims = parseJwtClaims(token);
  if (!claims || !claims.sub) {
    return null;
  }

  return {
    userId: claims.sub as string,
    email: claims.email as string | undefined,
    roles: (claims.roles as string[]) ?? [],
    permissions: claims.permissions as string[] | undefined,
    organizationId: claims.org_id as string | undefined,
    metadata: extractMetadata(claims),
  };
}

/**
 * Extract non-standard claims as metadata
 */
function extractMetadata(claims: Record<string, unknown>): Record<string, unknown> | undefined {
  const reservedClaims = new Set([
    'sub', 'email', 'roles', 'permissions', 'org_id',
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
// OAuth Token Validation
// ============================================================================

/**
 * Validate a Bearer token using oauth.do service
 */
async function validateTokenWithOAuth(
  oauth: OAuthService,
  token: string
): Promise<OAuthValidationResponse> {
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
      // Try to parse error response
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

    const data = await response.json() as OAuthValidationResponse;
    return data;
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

/**
 * Validate a Bearer token locally (fallback when oauth.do is not available)
 *
 * SECURITY WARNING: This performs basic JWT parsing WITHOUT cryptographic
 * signature verification. Tokens validated this way should be marked as
 * "unverified" and only used in development/testing environments.
 *
 * @param token - The JWT token to validate
 * @returns Validation response with unverified flag set to true
 */
function validateTokenLocally(token: string): OAuthValidationResponse {
  const user = extractUserContext(token);
  if (!user) {
    return {
      valid: false,
      error: {
        code: 'INVALID_TOKEN',
        message: 'Failed to parse token',
      },
    };
  }

  // Check token expiration
  const claims = parseJwtClaims(token);
  if (claims?.exp) {
    const now = Math.floor(Date.now() / 1000);
    if ((claims.exp as number) < now) {
      return {
        valid: false,
        error: {
          code: 'TOKEN_EXPIRED',
          message: 'Token has expired',
        },
      };
    }
  }

  // Mark as unverified since we didn't verify the cryptographic signature
  return { valid: true, user, unverified: true };
}

// ============================================================================
// Middleware
// ============================================================================

/** Variables type for auth context */
export type AuthVariables = {
  auth: AuthContext;
};

/**
 * Create authentication middleware.
 *
 * @param config - Auth configuration
 * @returns Hono middleware function
 */
export function createAuthMiddleware<E extends EnvWithOAuth, V extends AuthVariables = AuthVariables>(config: AuthConfig) {
  const publicPaths = config.publicPaths ?? ['/health', '/v1/config'];

  return async (c: Context<{ Bindings: E; Variables: V }>, next: Next): Promise<Response | void> => {
    // Skip auth if disabled - grant full admin access
    if (!config.enabled) {
      c.set('auth', {
        authenticated: true,
        userId: 'anonymous',
        roles: ['admin', 'owner'],
      } as AuthContext);
      await next();
      return;
    }

    const pathname = new URL(c.req.url).pathname;

    // Skip auth for public paths
    for (const path of publicPaths) {
      if (path.endsWith('*')) {
        if (pathname.startsWith(path.slice(0, -1))) {
          c.set('auth', { authenticated: false, roles: [] } as AuthContext);
          await next();
          return;
        }
      } else if (pathname === path) {
        c.set('auth', { authenticated: false, roles: [] } as AuthContext);
        await next();
        return;
      }
    }

    // Extract Bearer token
    const authHeader = c.req.header('Authorization');

    // Allow anonymous read access if configured
    if (!authHeader) {
      if (config.allowAnonymousRead && isReadRequest(c.req.method)) {
        c.set('auth', {
          authenticated: false,
          roles: ['read'],
        } as AuthContext);
        await next();
        return;
      }

      return c.json(
        {
          error: {
            message: 'Authorization header is required',
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

    // Validate Bearer token format
    if (!authHeader.startsWith('Bearer ')) {
      return c.json(
        {
          error: {
            message: 'Authorization header must use Bearer scheme',
            type: 'Unauthorized',
            code: 401,
          },
        },
        401,
        {
          'WWW-Authenticate': 'Bearer realm="iceberg.do", error="invalid_request"',
        }
      );
    }

    const token = authHeader.slice(7).trim();
    if (!token) {
      return c.json(
        {
          error: {
            message: 'Bearer token is required',
            type: 'Unauthorized',
            code: 401,
          },
        },
        401,
        {
          'WWW-Authenticate': 'Bearer realm="iceberg.do", error="invalid_token"',
        }
      );
    }

    // Validate token
    let validation: OAuthValidationResponse;
    const oauthService = c.env?.OAUTH;
    const isProduction = c.env?.ENVIRONMENT === 'production';

    if (oauthService) {
      // Use oauth.do for token validation (preferred, cryptographically secure)
      validation = await validateTokenWithOAuth(oauthService, token);
    } else if (isProduction) {
      // SECURITY: Never use local validation in production - it doesn't verify signatures
      return c.json(
        {
          error: {
            message: 'Authentication service unavailable',
            type: 'ServiceUnavailable',
            code: 503,
          },
        },
        503
      );
    } else if (config.disableLocalValidation) {
      // Local validation is explicitly disabled via config
      return c.json(
        {
          error: {
            message: 'Authentication service unavailable and local validation is disabled',
            type: 'ServiceUnavailable',
            code: 503,
          },
        },
        503
      );
    } else {
      // Fallback to local validation (development/testing mode only)
      // WARNING: This does NOT verify cryptographic signatures
      validation = validateTokenLocally(token);
    }

    if (!validation.valid) {
      const errorDescription = validation.error?.message ?? 'Token validation failed';
      return c.json(
        {
          error: {
            message: errorDescription,
            type: 'Unauthorized',
            code: 401,
          },
        },
        401,
        {
          'WWW-Authenticate': `Bearer realm="iceberg.do", error="invalid_token", error_description="${errorDescription}"`,
        }
      );
    }

    // Set auth context
    const authContext: AuthContext = {
      authenticated: true,
      userId: validation.user?.userId,
      email: validation.user?.email,
      roles: validation.user?.roles ?? [],
      organizationId: validation.user?.organizationId,
      scopes: validation.user?.permissions,
      // Mark as unverified if local validation was used (no cryptographic signature verification)
      unverified: validation.unverified,
    };

    c.set('auth', authContext);

    // Continue to handler
    await next();

    // Add auth headers to response
    addAuthHeaders(authContext, c);
  };
}

/**
 * Check if request is a read-only operation
 */
function isReadRequest(method: string): boolean {
  return method === 'GET' || method === 'HEAD' || method === 'OPTIONS';
}

/**
 * Add authentication headers to response
 */
function addAuthHeaders<E extends EnvWithOAuth, V extends AuthVariables = AuthVariables>(auth: AuthContext, c: Context<{ Bindings: E; Variables: V }>): void {
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
    // Warn downstream services that this token was not cryptographically verified
    if (auth.unverified) {
      c.header('X-Token-Unverified', 'true');
    }
  }
}

// ============================================================================
// Permission Checking
// ============================================================================

/**
 * Check if user has a specific permission.
 */
export function checkPermission(
  context: AuthContext,
  required: Permission
): boolean {
  if (!context.authenticated) {
    return false;
  }

  // Admin role has all permissions
  if (context.roles.includes('admin')) {
    return true;
  }

  // Check specific permission
  switch (required) {
    case 'read':
      return context.roles.includes('read') || context.roles.includes('write');
    case 'write':
      return context.roles.includes('write');
    case 'admin':
      return context.roles.includes('admin');
    default:
      return false;
  }
}

/**
 * Check if user has access to a specific table.
 */
export function hasTableAccess(
  context: AuthContext,
  _namespace: string[],
  _tableName: string,
  permission: Permission
): boolean {
  // Basic permission check
  if (!checkPermission(context, permission)) {
    return false;
  }

  // TODO: Implement fine-grained table-level permissions
  // Will use _namespace and _tableName for table-level ACLs

  return true;
}

/**
 * Get auth context from Hono context
 */
export function getAuthContext(c: Context): AuthContext {
  return c.get('auth') ?? { authenticated: false, roles: [] };
}
