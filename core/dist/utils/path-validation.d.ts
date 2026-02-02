/**
 * Path Validation Utilities
 *
 * Security helpers for validating and sanitizing file paths
 * to prevent path traversal attacks and ensure safe path handling.
 */
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
export declare function validatePath(path: string): void;
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
export declare function sanitizePath(path: string): string;
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
export declare function isAbsolutePath(path: string): boolean;
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
export declare function joinPaths(...paths: string[]): string;
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
export declare function getParentPath(path: string): string;
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
export declare function getBasename(path: string): string;
//# sourceMappingURL=path-validation.d.ts.map