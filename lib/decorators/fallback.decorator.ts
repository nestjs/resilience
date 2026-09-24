import { SetMetadata } from '@nestjs/common';
import type {
  EntrypointDecorator,
  EntrypointFallbackFn,
  FallbackDecoratorOptions,
  FallbackMetadata,
  FallbackMethodDecorator,
} from '../interfaces/entrypoint-options.interface.js';
import { FALLBACK_METADATA } from '../resilience.constants.js';

/**
 * Returns the fallback's result instead of failing: the name of a method on
 * the same class (TypeScript checks that it exists), or a function. Both are
 * called with `(error, executionContext)`.
 *
 * Not applied to client errors (`isClientError()`: 4xx `HttpException`s and
 * other errors with a 4xx `status`; override with `handleIf`), nor once the
 * response was committed.
 */
export function Fallback<K extends string>(method: K, options?: FallbackDecoratorOptions): FallbackMethodDecorator<K>;

export function Fallback(fn: EntrypointFallbackFn, options?: FallbackDecoratorOptions): EntrypointDecorator;

export function Fallback(target: string | EntrypointFallbackFn, options: FallbackDecoratorOptions = {}) {
  const metadata: FallbackMetadata =
    typeof target === 'function'
      ? { fn: target, handleIf: options.handleIf }
      : { method: target, handleIf: options.handleIf };
  return SetMetadata(FALLBACK_METADATA, metadata);
}
