import { describe, it, expect } from 'vitest';
import {
  validatePath,
  sanitizePath,
  isAbsolutePath,
  joinPaths,
  getParentPath,
  getBasename,
} from '../../src/utils/path-validation.js';

describe('validatePath', () => {
  describe('valid paths', () => {
    it('should accept simple paths', () => {
      expect(() => validatePath('data/table/file.parquet')).not.toThrow();
      expect(() => validatePath('file.parquet')).not.toThrow();
      expect(() => validatePath('data')).not.toThrow();
    });

    it('should accept absolute paths', () => {
      expect(() => validatePath('/var/data/table')).not.toThrow();
      expect(() => validatePath('s3://bucket/data/table')).not.toThrow();
      expect(() => validatePath('gs://bucket/data/table')).not.toThrow();
      expect(() => validatePath('hdfs://cluster/data/table')).not.toThrow();
    });

    it('should accept paths with dots in filenames', () => {
      expect(() => validatePath('data/file.name.parquet')).not.toThrow();
      expect(() => validatePath('.hidden/file')).not.toThrow();
      expect(() => validatePath('data/.metadata')).not.toThrow();
    });

    it('should accept paths with special characters', () => {
      expect(() => validatePath('data/table-name/file_v1.parquet')).not.toThrow();
      expect(() => validatePath('data/2024/01/file.parquet')).not.toThrow();
    });
  });

  describe('path traversal detection', () => {
    it('should reject Unix-style path traversal (../)', () => {
      expect(() => validatePath('../etc/passwd')).toThrow('path traversal not allowed');
      expect(() => validatePath('data/../secret')).toThrow('path traversal not allowed');
      expect(() => validatePath('data/table/../../secret')).toThrow('path traversal not allowed');
    });

    it('should reject Windows-style path traversal (..\\)', () => {
      expect(() => validatePath('..\\etc\\passwd')).toThrow('path traversal not allowed');
      expect(() => validatePath('data\\..\\secret')).toThrow('path traversal not allowed');
    });

    it('should reject URL-encoded path traversal', () => {
      expect(() => validatePath('%2e%2e%2fetc/passwd')).toThrow('path traversal not allowed');
      expect(() => validatePath('%2e%2e%5cetc\\passwd')).toThrow('path traversal not allowed');
    });

    it('should reject double URL-encoded path traversal', () => {
      expect(() => validatePath('%252e%252e%252fetc/passwd')).toThrow('path traversal not allowed');
      expect(() => validatePath('%252e%252e%255cetc\\passwd')).toThrow('path traversal not allowed');
    });

    it('should reject trailing ..', () => {
      expect(() => validatePath('data/table/..')).toThrow('path traversal not allowed');
    });
  });

  describe('edge cases', () => {
    it('should throw for non-string input', () => {
      expect(() => validatePath(null as unknown as string)).toThrow('Path must be a string');
      expect(() => validatePath(undefined as unknown as string)).toThrow('Path must be a string');
      expect(() => validatePath(123 as unknown as string)).toThrow('Path must be a string');
    });

    it('should accept empty string', () => {
      expect(() => validatePath('')).not.toThrow();
    });
  });
});

describe('sanitizePath', () => {
  describe('backslash normalization', () => {
    it('should convert backslashes to forward slashes', () => {
      expect(sanitizePath('data\\table\\file.parquet')).toBe('data/table/file.parquet');
      expect(sanitizePath('C:\\Users\\data')).toBe('C:/Users/data');
    });
  });

  describe('path traversal removal', () => {
    it('should remove ../ sequences', () => {
      // sanitizePath removes /../ and ../ patterns without semantic path resolution
      expect(sanitizePath('data/../table')).toBe('data/table');
      expect(sanitizePath('/data/../table/file.parquet')).toBe('/data/table/file.parquet');
    });

    it('should remove ./ sequences', () => {
      expect(sanitizePath('./data/table')).toBe('data/table');
      expect(sanitizePath('data/./table')).toBe('data/table');
    });

    it('should handle multiple traversal sequences', () => {
      // Removes traversal patterns without semantic resolution
      expect(sanitizePath('a/b/../c/../d')).toBe('a/b/c/d');
      expect(sanitizePath('../../../data')).toBe('data');
    });

    it('should remove URL-encoded traversal sequences', () => {
      expect(sanitizePath('%2e%2e%2fdata')).toBe('data');
      expect(sanitizePath('%2e%2e%5cdata')).toBe('data');
    });

    it('should handle trailing /.. and /.', () => {
      expect(sanitizePath('data/table/..')).toBe('data/table');
      expect(sanitizePath('data/table/.')).toBe('data/table');
    });
  });

  describe('slash collapse', () => {
    it('should collapse multiple consecutive slashes', () => {
      expect(sanitizePath('data//table///file.parquet')).toBe('data/table/file.parquet');
      expect(sanitizePath('/data//table')).toBe('/data/table');
    });

    it('should preserve protocol double slash', () => {
      expect(sanitizePath('s3://bucket//data///file.parquet')).toBe('s3://bucket/data/file.parquet');
      expect(sanitizePath('gs://bucket/data')).toBe('gs://bucket/data');
    });
  });

  describe('trailing slash handling', () => {
    it('should remove trailing slashes from paths', () => {
      expect(sanitizePath('data/table/')).toBe('data/table');
      expect(sanitizePath('/data/table/')).toBe('/data/table');
    });

    it('should preserve root paths', () => {
      expect(sanitizePath('/')).toBe('/');
    });

    it('should preserve protocol roots', () => {
      // Protocol roots without trailing content have trailing slash removed
      expect(sanitizePath('s3://bucket/')).toBe('s3://bucket');
      expect(sanitizePath('s3://bucket/data/')).toBe('s3://bucket/data');
    });
  });

  describe('edge cases', () => {
    it('should return empty string for non-string input', () => {
      expect(sanitizePath(null as unknown as string)).toBe('');
      expect(sanitizePath(undefined as unknown as string)).toBe('');
      expect(sanitizePath(123 as unknown as string)).toBe('');
    });

    it('should handle empty string', () => {
      expect(sanitizePath('')).toBe('');
    });

    it('should handle just . or ..', () => {
      expect(sanitizePath('.')).toBe('');
      expect(sanitizePath('..')).toBe('');
    });
  });
});

describe('isAbsolutePath', () => {
  describe('protocol paths', () => {
    it('should return true for S3 paths', () => {
      expect(isAbsolutePath('s3://bucket/data')).toBe(true);
      expect(isAbsolutePath('s3://bucket')).toBe(true);
    });

    it('should return true for GCS paths', () => {
      expect(isAbsolutePath('gs://bucket/data')).toBe(true);
    });

    it('should return true for HDFS paths', () => {
      expect(isAbsolutePath('hdfs://cluster/data')).toBe(true);
    });

    it('should return true for file:// paths', () => {
      expect(isAbsolutePath('file:///var/data')).toBe(true);
    });

    it('should return true for other protocol paths', () => {
      expect(isAbsolutePath('abfs://container@account.dfs.core.windows.net/data')).toBe(true);
      expect(isAbsolutePath('wasb://container@account.blob.core.windows.net/data')).toBe(true);
    });
  });

  describe('Unix paths', () => {
    it('should return true for paths starting with /', () => {
      expect(isAbsolutePath('/var/data')).toBe(true);
      expect(isAbsolutePath('/')).toBe(true);
      expect(isAbsolutePath('/file.parquet')).toBe(true);
    });
  });

  describe('relative paths', () => {
    it('should return false for relative paths', () => {
      expect(isAbsolutePath('data/table')).toBe(false);
      expect(isAbsolutePath('file.parquet')).toBe(false);
      expect(isAbsolutePath('./local')).toBe(false);
      expect(isAbsolutePath('../parent')).toBe(false);
    });
  });

  describe('edge cases', () => {
    it('should return false for empty string', () => {
      expect(isAbsolutePath('')).toBe(false);
    });

    it('should return false for non-string input', () => {
      expect(isAbsolutePath(null as unknown as string)).toBe(false);
      expect(isAbsolutePath(undefined as unknown as string)).toBe(false);
      expect(isAbsolutePath(123 as unknown as string)).toBe(false);
    });
  });
});

describe('joinPaths', () => {
  describe('basic joining', () => {
    it('should join path segments with /', () => {
      expect(joinPaths('data', 'table', 'file.parquet')).toBe('data/table/file.parquet');
      expect(joinPaths('a', 'b', 'c')).toBe('a/b/c');
    });

    it('should handle protocol paths as base', () => {
      expect(joinPaths('s3://bucket', 'data', 'table')).toBe('s3://bucket/data/table');
      expect(joinPaths('gs://bucket', 'data')).toBe('gs://bucket/data');
    });

    it('should handle Unix absolute paths as base', () => {
      expect(joinPaths('/var', 'data', 'file.parquet')).toBe('/var/data/file.parquet');
    });
  });

  describe('absolute path handling', () => {
    it('should reset when encountering absolute path', () => {
      expect(joinPaths('base', 's3://other/path')).toBe('s3://other/path');
      expect(joinPaths('base', '/absolute/path')).toBe('/absolute/path');
      expect(joinPaths('s3://bucket', 'data', '/new/root')).toBe('/new/root');
    });
  });

  describe('slash handling', () => {
    it('should handle trailing slashes in segments', () => {
      expect(joinPaths('data/', 'table')).toBe('data/table');
      expect(joinPaths('s3://bucket/', 'data')).toBe('s3://bucket/data');
    });

    it('should handle leading slashes in non-absolute segments', () => {
      expect(joinPaths('data', '/table')).toBe('/table'); // Leading / makes it absolute
    });

    it('should normalize backslashes', () => {
      expect(joinPaths('data\\sub', 'table')).toBe('data/sub/table');
    });
  });

  describe('empty handling', () => {
    it('should return empty string for no arguments', () => {
      expect(joinPaths()).toBe('');
    });

    it('should filter out empty strings', () => {
      expect(joinPaths('data', '', 'table')).toBe('data/table');
      expect(joinPaths('', 'data', '')).toBe('data');
    });

    it('should return empty for all empty arguments', () => {
      expect(joinPaths('', '', '')).toBe('');
    });
  });

  describe('security', () => {
    it('should throw for path traversal in any segment', () => {
      expect(() => joinPaths('data', '../secret')).toThrow('path traversal not allowed');
      expect(() => joinPaths('../base', 'data')).toThrow('path traversal not allowed');
      expect(() => joinPaths('data', 'table', '../../secret')).toThrow('path traversal not allowed');
    });
  });
});

describe('getParentPath', () => {
  it('should return parent directory', () => {
    expect(getParentPath('/var/data/file.parquet')).toBe('/var/data');
    expect(getParentPath('data/table/file.parquet')).toBe('data/table');
  });

  it('should handle protocol paths', () => {
    expect(getParentPath('s3://bucket/data/file.parquet')).toBe('s3://bucket/data');
    expect(getParentPath('s3://bucket/file.parquet')).toBe('s3://bucket');
  });

  it('should return root for top-level paths', () => {
    expect(getParentPath('/file.parquet')).toBe('/');
    expect(getParentPath('s3://bucket')).toBe('s3://bucket');
  });

  it('should handle trailing slashes', () => {
    expect(getParentPath('/var/data/')).toBe('/var');
    expect(getParentPath('s3://bucket/data/')).toBe('s3://bucket');
  });

  it('should return empty for relative paths without parent', () => {
    expect(getParentPath('file.parquet')).toBe('');
  });

  it('should handle edge cases', () => {
    expect(getParentPath('')).toBe('');
    expect(getParentPath(null as unknown as string)).toBe('');
  });
});

describe('getBasename', () => {
  it('should return filename from path', () => {
    expect(getBasename('/var/data/file.parquet')).toBe('file.parquet');
    expect(getBasename('data/table')).toBe('table');
  });

  it('should handle protocol paths', () => {
    expect(getBasename('s3://bucket/data/file.parquet')).toBe('file.parquet');
  });

  it('should handle trailing slashes', () => {
    expect(getBasename('/var/data/')).toBe('data');
    expect(getBasename('s3://bucket/data/')).toBe('data');
  });

  it('should return path if no directory', () => {
    expect(getBasename('file.parquet')).toBe('file.parquet');
  });

  it('should handle edge cases', () => {
    expect(getBasename('')).toBe('');
    expect(getBasename(null as unknown as string)).toBe('');
  });
});
