import type { ExecutionContext } from '@nestjs/common';
import type { RetryOptions } from './retry-options.interface.js';

/** Decorators for entrypoints (controllers, resolvers, gateways, message handlers), on the class or a handler. */
export type EntrypointDecorator = MethodDecorator & ClassDecorator;

/** `@Retry()` options: the retry vocabulary, plus the handler author's `idempotent` statement. */
export interface EntrypointRetryOptions extends Omit<RetryOptions, 'retryOnResult'> {
  /**
   * Declares the handler safe to run more than once. Without it, retries only
   * happen where the protocol says repeating is safe: HTTP GET, HEAD and
   * OPTIONS, and GraphQL queries. Message, event and WebSocket handlers have
   * no such signal, so for them a `@Retry()` on the handler itself is the
   * opt-in; a retry inherited from a preset or the class does not apply.
   */
  idempotent?: boolean;
}

export type EntrypointFallbackFn = (error: unknown, context: ExecutionContext) => unknown;

export interface FallbackDecoratorOptions {
  /** Which errors the fallback replaces. Default: all but client errors (`isClientError()`). */
  handleIf?: (error: unknown) => boolean;
}

type Method = (...args: any[]) => unknown;

/** Applies where the class has a method named `K`: on that class, or on one of its handlers. */
export interface FallbackMethodDecorator<K extends string> {
  (target: abstract new (...args: any[]) => Record<K, Method>): void;
  (target: Record<K, Method>, key: string | symbol, descriptor: PropertyDescriptor): void;
}

export interface FallbackMetadata {
  method?: string;
  fn?: EntrypointFallbackFn;
  handleIf?: (error: unknown) => boolean;
}
