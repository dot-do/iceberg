/**
 * Path Validation Utilities
 *
 * Security helpers for validating and sanitizing file paths
 * to prevent path traversal attacks and ensure safe path handling.
 */

// ============================================================================
// Path Traversal Patterns
// ============================================================================

/**
 * Regex patterns for detecting path traversal attempts.
 * Covers both Unix-style (../) and Windows-style (..\) traversals,
 * as well as URL-encoded variants.
 */
const PATH_TRAVERSAL_PATTERNS = [
  /\.\.\//g,           // Unix-style: ../
  /\.\.\\/g,           // Windows-style: ..\
  /%2e%2e%2f/gi,       // URL-encoded: ../
  /%2e%2e%5c/gi,       // URL-encoded: ..\
  /%252e%252e%252f/gi, // Double URL-encoded: ../
  /%252e%252e%255c/gi, // Double URL-encoded: ..\
  /^\.\./,             // Leading ..
  /\.\.$/,             // Trailing ..
];

/**
 * Protocol pattern for detecting absolute paths with protocols.
 * Matches common storage protocols: s3://, gs://, hdfs://, file://, etc.
 */
const PROTOCOL_PATTERN = /^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//;

// ============================================================================
// Validation Functions
// ============================================================================

/**
 * Validates that a path does not contain path traversal attempts.
 *
 * @param path - The path to validate
 * @throws Error if path contains traversal attempts
 *
 * @example
 * ```ts
 * validatePath('/data/table/file.parquet'); // OK
 * validatePath('../etc/passwd'); // throws Error
 * ```
 */
export function validatePath(path: string): void {
  if (typeof path !== 'string') {
    throw new Error('Path must be a string');
  }

  for (const pattern of PATH_TRAVERSAL_PATTERNS) {
    if (pattern.test(path)) {
      throw new Error(`Invalid path: path traversal not allowed in "${path}"`);
    }
  }
}

/**
 * Sanitizes a path by normalizing separators and removing traversal attempts.
 *
 * This function:
 * - Normalizes backslashes to forward slashes
 * - Removes path traversal sequences (../, ..\)
 * - Removes URL-encoded traversal sequences
 * - Collapses multiple consecutive slashes
 * - Removes trailing slashes (except for root paths)
 *
 * @param path - The path to sanitize
 * @returns The sanitized path
 *
 * @example
 * ```ts
 * sanitizePath('data\\table\\file.parquet'); // 'data/table/file.parquet'
 * sanitizePath('/data/../table/./file.parquet'); // '/table/file.parquet'
 * sanitizePath('s3://bucket//data///file.parquet'); // 's3://bucket/data/file.parquet'
 * ```
 */
export function sanitizePath(path: string): string {
  if (typeof path !== 'string') {
    return '';
  }

  let sanitized = path;

  // Normalize backslashes to forward slashes
  sanitized = sanitized.replace(/\\/g, '/');

  // Decode URL-encoded traversal sequences and remove them
  sanitized = sanitized.replace(/%2e%2e%2f/gi, '');
  sanitized = sanitized.replace(/%2e%2e%5c/gi, '');
  sanitized = sanitized.replace(/%252e%252e%252f/gi, '');
  sanitized = sanitized.replace(/%252e%252e%255c/gi, '');

  // Remove path traversal sequences (../ and ./)
  // Keep doing this until no more changes occur
  let previous: string;
  do {
    previous = sanitized;
    // Remove ../ sequences
    sanitized = sanitized.replace(/\/\.\.\//g, '/');
    sanitized = sanitized.replace(/^\.\.\//g, '');
    // Remove ./ sequences (current directory references)
    sanitized = sanitized.replace(/\/\.\//g, '/');
    sanitized = sanitized.replace(/^\.\//g, '');
  } while (sanitized !== previous);

  // Handle trailing /.. or /.
  sanitized = sanitized.replace(/\/\.\.$/g, '');
  sanitized = sanitized.replace(/\/\.$/g, '');
  sanitized = sanitized.replace(/^\.\.$/g, '');
  sanitized = sanitized.replace(/^\.$/g, '');

  // Preserve protocol prefix during slash collapse
  const protocolMatch = sanitized.match(PROTOCOL_PATTERN);
  if (protocolMatch) {
    const protocol = protocolMatch[0];
    const rest = sanitized.slice(protocol.length);
    // Collapse multiple slashes in the path part (not the protocol)
    const collapsedRest = rest.replace(/\/+/g, '/');
    sanitized = protocol + collapsedRest;
  } else {
    // No protocol - collapse all multiple slashes but preserve leading slash
    const hasLeadingSlash = sanitized.startsWith('/');
    sanitized = sanitized.replace(/\/+/g, '/');
    if (hasLeadingSlash && !sanitized.startsWith('/')) {
      sanitized = '/' + sanitized;
    }
  }

  // Remove trailing slash (unless it's the root or protocol root)
  if (sanitized.length > 1 && sanitized.endsWith('/')) {
    // Don't remove trailing slash from protocol roots like "s3://bucket/"
    const afterProtocol = protocolMatch
      ? sanitized.slice(protocolMatch[0].length)
      : sanitized;

    // Only remove if there's more than just the host/bucket
    if (afterProtocol.includes('/') && afterProtocol !== '/') {
      sanitized = sanitized.slice(0, -1);
    }
  }

  return sanitized;
}

// ============================================================================
// Path Type Detection
// ============================================================================

/**
 * Checks if a path is an absolute path.
 *
 * A path is considered absolute if it:
 * - Starts with a protocol (e.g., s3://, gs://, hdfs://, file://)
 * - Starts with a forward slash (Unix absolute path)
 *
 * @param path - The path to check
 * @returns True if the path is absolute
 *
 * @example
 * ```ts
 * isAbsolutePath('s3://bucket/data'); // true
 * isAbsolutePath('/var/data/table'); // true
 * isAbsolutePath('relative/path'); // false
 * isAbsolutePath('./local'); // false
 * ```
 */
export function isAbsolutePath(path: string): boolean {
  if (typeof path !== 'string' || path === '') {
    return false;
  }

  // Check for protocol prefix (s3://, gs://, hdfs://, file://, etc.)
  if (PROTOCOL_PATTERN.test(path)) {
    return true;
  }

  // Check for Unix absolute path
  return path.startsWith('/');
}

// ============================================================================
// Path Joining
// ============================================================================

/**
 * Safely joins path segments together.
 *
 * This function:
 * - Validates each segment for path traversal attempts
 * - Handles absolute paths correctly (absolute path resets the base)
 * - Normalizes separators to forward slashes
 * - Removes empty segments
 * - Does not collapse '..' - use sanitizePath() if you need that
 *
 * @param paths - Path segments to join
 * @returns The joined path
 * @throws Error if any segment contains path traversal attempts
 *
 * @example
 * ```ts
 * joinPaths('s3://bucket', 'data', 'table'); // 's3://bucket/data/table'
 * joinPaths('/var', 'data', 'file.parquet'); // '/var/data/file.parquet'
 * joinPaths('base', 's3://other/path'); // 's3://other/path' (absolute resets)
 * ```
 */
export function joinPaths(...paths: string[]): string {
  if (paths.length === 0) {
    return '';
  }

  // Filter out empty strings
  const segments = paths.filter((p) => typeof p === 'string' && p !== '');

  if (segments.length === 0) {
    return '';
  }

  // Validate each segment
  for (const segment of segments) {
    validatePath(segment);
  }

  // Build the path, handling absolute paths
  let result = '';

  for (const segment of segments) {
    // Normalize backslashes
    const normalized = segment.replace(/\\/g, '/');

    if (isAbsolutePath(normalized)) {
      // Absolute path resets the result
      result = normalized;
    } else if (result === '') {
      result = normalized;
    } else {
      // Join with separator, avoiding double slashes
      const base = result.endsWith('/') ? result.slice(0, -1) : result;
      const part = normalized.startsWith('/') ? normalized.slice(1) : normalized;
      result = `${base}/${part}`;
    }
  }

  return result;
}

// ============================================================================
// Additional Utilities
// ============================================================================

/**
 * Extracts the parent directory from a path.
 *
 * @param path - The path to get the parent of
 * @returns The parent directory path
 *
 * @example
 * ```ts
 * getParentPath('s3://bucket/data/file.parquet'); // 's3://bucket/data'
 * getParentPath('/var/data/table'); // '/var/data'
 * ```
 */
export function getParentPath(path: string): string {
  if (typeof path !== 'string' || path === '') {
    return '';
  }

  // Normalize and remove trailing slash
  let normalized = path.replace(/\\/g, '/');
  if (normalized.endsWith('/') && normalized.length > 1) {
    normalized = normalized.slice(0, -1);
  }

  const lastSlash = normalized.lastIndexOf('/');

  if (lastSlash === -1) {
    return '';
  }

  // Handle protocol roots like s3://bucket
  const protocolMatch = normalized.match(PROTOCOL_PATTERN);
  if (protocolMatch) {
    const afterProtocol = normalized.slice(protocolMatch[0].length);
    if (!afterProtocol.includes('/')) {
      // Already at protocol root (e.g., s3://bucket)
      return normalized;
    }
  }

  // Handle Unix root
  if (lastSlash === 0) {
    return '/';
  }

  return normalized.slice(0, lastSlash);
}

/**
 * Extracts the filename (basename) from a path.
 *
 * @param path - The path to get the filename from
 * @returns The filename
 *
 * @example
 * ```ts
 * getBasename('s3://bucket/data/file.parquet'); // 'file.parquet'
 * getBasename('/var/data/table'); // 'table'
 * ```
 */
export function getBasename(path: string): string {
  if (typeof path !== 'string' || path === '') {
    return '';
  }

  // Normalize and remove trailing slash
  let normalized = path.replace(/\\/g, '/');
  if (normalized.endsWith('/') && normalized.length > 1) {
    normalized = normalized.slice(0, -1);
  }

  const lastSlash = normalized.lastIndexOf('/');

  if (lastSlash === -1) {
    return normalized;
  }

  return normalized.slice(lastSlash + 1);
}
