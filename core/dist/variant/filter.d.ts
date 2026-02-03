/**
 * Variant Filter Transformation
 *
 * Provides utilities for rewriting variant field filters to use statistics paths.
 * When variant columns are shredded, queries on specific fields can be optimized
 * by directing them to the shredded column statistics paths.
 *
 * @see https://iceberg.apache.org/spec/#variant-shredding
 */
import type { VariantShredPropertyConfig } from './config.js';
/**
 * Result of transforming a variant filter.
 *
 * Contains the transformed filter and information about which paths were
 * transformed and which could not be transformed.
 */
export interface TransformResult {
    /** The transformed filter with variant paths rewritten to statistics paths */
    readonly filter: Record<string, unknown>;
    /** List of variant field paths that were transformed to statistics paths */
    readonly transformedPaths: readonly string[];
    /** List of variant field paths that could not be transformed (non-shredded fields) */
    readonly untransformedPaths: readonly string[];
}
/**
 * Check if a key is a comparison operator.
 *
 * @param key - The key to check
 * @returns True if the key is a comparison operator
 */
export declare function isComparisonOperator(key: string): boolean;
/**
 * Check if a key is a logical operator.
 *
 * @param key - The key to check
 * @returns True if the key is a logical operator
 */
export declare function isLogicalOperator(key: string): boolean;
/**
 * Transform a filter object, rewriting variant field paths to statistics paths.
 *
 * When variant columns are shredded, specific fields are extracted into separate
 * columns. This function rewrites filter paths to point to the shredded column
 * statistics paths, enabling efficient query pruning.
 *
 * @param filter - The filter object to transform
 * @param configs - Array of variant shred configurations
 * @returns TransformResult with transformed filter and path information
 *
 * @example Basic equality filter
 * ```ts
 * const config: VariantShredPropertyConfig = {
 *   columnName: '$data',
 *   fields: ['title', 'year'],
 *   fieldTypes: { title: 'string', year: 'int' },
 * };
 *
 * const result = transformVariantFilter(
 *   { '$data.title': 'The Matrix' },
 *   [config]
 * );
 *
 * // result.filter = { '$data.typed_value.title.typed_value': 'The Matrix' }
 * // result.transformedPaths = ['$data.title']
 * // result.untransformedPaths = []
 * ```
 *
 * @example Range filter with operators
 * ```ts
 * const result = transformVariantFilter(
 *   { '$data.year': { $gte: 2010, $lte: 2020 } },
 *   [config]
 * );
 *
 * // result.filter = {
 * //   '$data.typed_value.year.typed_value': { $gte: 2010, $lte: 2020 }
 * // }
 * ```
 *
 * @example Mixed filter with shredded and non-shredded fields
 * ```ts
 * const result = transformVariantFilter(
 *   { '$data.title': 'foo', '$data.director': 'Nolan', id: 123 },
 *   [config]
 * );
 *
 * // Shredded field transformed, others preserved as-is
 * // result.filter = {
 * //   '$data.typed_value.title.typed_value': 'foo',
 * //   '$data.director': 'Nolan',
 * //   id: 123
 * // }
 * // result.transformedPaths = ['$data.title']
 * // result.untransformedPaths = ['$data.director']
 * ```
 */
export declare function transformVariantFilter(filter: Record<string, unknown>, configs: readonly VariantShredPropertyConfig[]): TransformResult;
//# sourceMappingURL=filter.d.ts.map