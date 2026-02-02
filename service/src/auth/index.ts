/**
 * Iceberg Auth Module
 *
 * Authentication and authorization for Iceberg catalog operations.
 * Integrates with oauth.do for OAuth-based authentication.
 *
 * @see https://oauth.do
 */

export {
  // Types
  type AuthConfig,
  type AuthContext,
  type AuthVariables,
  type Permission,
  type TablePermissions,
  type UserContext,
  type OAuthService,
  type EnvWithOAuth,
  // Middleware
  createAuthMiddleware,
  // Utilities
  checkPermission,
  hasTableAccess,
  getAuthContext,
  extractUserContext,
} from './middleware.js';

export {
  // Permission definitions
  type NamespacePermission,
  type CatalogPermission,
  // Permission checking
  checkNamespacePermission,
  checkTablePermission,
  checkCatalogPermission,
} from './permissions.js';

// oauth.do integration
export {
  // Types
  type OAuthDoUser,
  type OAuthDoVariables,
  type OAuthDoConfig,
  type OAuthDoEnv,
  type IcebergOAuthVariables,
  // Converters
  oauthUserToAuthContext,
  authContextToOAuthUser,
  // Service binding
  validateTokenViaServiceBinding,
  // Middleware
  createOAuthDoMiddleware,
} from './oauth-do.js';

// RBAC (Role-Based Access Control)
export {
  // Types
  type Action,
  type ResourceType,
  type RoleName,
  type RoleDefinition,
  type Resource,
  type PermissionStore,
  type PermissionCheckResult,
  type GrantResult,
  type RevokeResult,
  // Constants
  ROLE_DEFINITIONS,
  // Classes
  RBACManager,
  InMemoryPermissionStore,
  // Functions
  namespaceResource,
  tableResource,
  resourceToId,
  parseResourceId,
  actionToPermissionLevel,
  roleToPermissionLevel,
  permissionLevelToRole,
} from './rbac.js';

// FGA (Fine-Grained Authorization)
export {
  // Types
  type PolicyCondition,
  type PolicyRule,
  type PolicyEvaluationResult,
  type PolicyContext,
  // Classes
  FGAEngine,
  // Functions
  createFGAEngine,
  canAccessNamespace,
  canAccessTable,
  requireNamespaceAccess,
  requireTableAccess,
} from './fga.js';

// Authorization Middleware
export {
  // Types
  type AuthorizationConfig,
  type AuthorizationContext,
  type AuthorizationVariables,
  // Middleware
  createAuthorizationMiddleware,
  requireNamespacePermission,
  requireTablePermission,
  requireGrantPermission,
  // Helpers
  getFGAEngine,
  getPermissionStore,
  checkAuthorization,
  requireAuthorization,
} from './authorization-middleware.js';
