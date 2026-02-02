/**
 * iceberg.do - Iceberg REST Catalog Service
 *
 * Cloudflare Worker implementing the Iceberg REST Catalog API.
 * Supports two backend options:
 * - Durable Objects with SQLite (default): Single-threaded, strongly consistent
 * - D1 Database: Global access, SQL queries across all data
 *
 * Integrates with oauth.do for user authentication.
 *
 * @see https://iceberg.apache.org/spec/#iceberg-rest-catalog
 * @see https://iceberg.do
 * @see https://oauth.do
 */

import { Hono } from 'hono';
import { cors } from 'hono/cors';
import { createIcebergRoutes } from './routes.js';
import { createD1CatalogHandler } from './catalog/d1.js';
import { createAuthMiddleware, createAuthorizationMiddleware, type OAuthService, type AuthVariables, type AuthorizationVariables } from './auth/index.js';

// Re-export the Durable Object classes for wrangler
export { CatalogDO, CatalogDOv2 } from './catalog/durable-object.js';

// Re-export catalog types
export * from './catalog/types.js';
export { D1CatalogBackend, D1_SCHEMA, createD1CatalogHandler } from './catalog/d1.js';

// Re-export auth types and middleware
export {
  createAuthMiddleware,
  createOAuthDoMiddleware,
  type OAuthService,
  type AuthVariables,
  type OAuthDoConfig,
  type OAuthDoEnv,
} from './auth/index.js';

// Environment bindings
export interface Env {
  // Catalog backends - at least one must be configured
  CATALOG?: DurableObjectNamespace;  // Durable Object backend
  DB?: D1Database;                    // D1 backend

  // Storage
  R2_BUCKET: R2Bucket;

  // R2 S3-compatible credentials (for external client data access)
  R2_ACCESS_KEY_ID?: string;
  R2_SECRET_ACCESS_KEY?: string;
  R2_URL?: string;  // e.g., https://<account>.r2.cloudflarestorage.com

  // Authentication
  OAUTH?: OAuthService;

  // Configuration
  ENVIRONMENT: string;
  AUTH_ENABLED?: string;
  AUTH_ALLOW_ANONYMOUS_READ?: string;

  /**
   * Catalog backend to use.
   * - "durable-object" (default): Use Durable Objects with SQLite
   * - "d1": Use D1 database
   */
  CATALOG_BACKEND?: 'durable-object' | 'd1';
}

// Hono variable types - use AuthVariables and AuthorizationVariables from the auth module
type Variables = AuthVariables & AuthorizationVariables & {
  // Catalog stub for backend-agnostic access
  catalogStub: { fetch: (request: Request) => Promise<Response> };
};

// Create the Hono app
const app = new Hono<{ Bindings: Env; Variables: Variables }>();

// Enable CORS for all routes
app.use('/*', cors());

// Catalog backend middleware - sets up the appropriate backend
// Uses warehouse parameter for catalog isolation (different warehouses = different catalogs)
app.use('/*', async (c, next) => {
  const backendType = c.env.CATALOG_BACKEND ?? 'durable-object';

  // Extract warehouse from query parameter or header for catalog isolation
  // Per Iceberg REST spec, warehouse is passed as query param to /v1/config
  const warehouse = c.req.query('warehouse') || c.req.header('X-Iceberg-Warehouse') || 'default';

  if (backendType === 'd1') {
    // Use D1 backend
    if (!c.env.DB) {
      return c.json(
        { error: 'D1 database not configured. Set DB binding in wrangler.toml.' },
        500
      );
    }
    c.set('catalogStub', createD1CatalogHandler(c.env.DB));
  } else {
    // Use Durable Object backend (default)
    if (!c.env.CATALOG) {
      return c.json(
        { error: 'Catalog Durable Object not configured. Set CATALOG binding in wrangler.toml.' },
        500
      );
    }
    // Use warehouse name to isolate catalogs - each warehouse gets its own Durable Object
    const id = c.env.CATALOG.idFromName(warehouse);
    c.set('catalogStub', c.env.CATALOG.get(id));
  }

  return next();
});

// Auth middleware - enabled based on environment config
app.use('/*', async (c, next) => {
  const authEnabled = c.env.AUTH_ENABLED === 'true' || c.env.ENVIRONMENT === 'production';
  const allowAnonymousRead = c.env.AUTH_ALLOW_ANONYMOUS_READ === 'true';

  const authMiddleware = createAuthMiddleware<Env, Variables>({
    enabled: authEnabled,
    allowAnonymousRead,
    publicPaths: ['/health', '/', '/v1/config'],
  });

  return authMiddleware(c, next);
});

// Authorization middleware - sets up FGA engine for permission checks
app.use('/*', createAuthorizationMiddleware({
  skipPaths: ['/health', '/', '/v1/config'],
}));

// Health check endpoint
app.get('/health', (c) => {
  const auth = c.get('auth');
  const backendType = c.env.CATALOG_BACKEND ?? 'durable-object';
  return c.json({
    status: 'healthy',
    service: 'iceberg.do',
    backend: backendType,
    authenticated: auth?.authenticated ?? false,
  });
});

// Mount Iceberg REST Catalog routes at /v1
app.route('/v1', createIcebergRoutes());

// Root endpoint
app.get('/', (c) => {
  const auth = c.get('auth');
  const backendType = c.env.CATALOG_BACKEND ?? 'durable-object';
  return c.json({
    service: 'iceberg.do',
    description: 'Iceberg REST Catalog as a Service',
    version: '0.1.0',
    spec: 'https://iceberg.apache.org/spec/#iceberg-rest-catalog',
    backend: backendType,
    authenticated: auth?.authenticated ?? false,
    endpoints: {
      health: '/health',
      config: '/v1/config',
      namespaces: '/v1/namespaces',
      tables: '/v1/namespaces/{namespace}/tables',
    },
  });
});

// Export the fetch handler
export default app;
