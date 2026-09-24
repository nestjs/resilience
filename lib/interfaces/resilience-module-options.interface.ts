import type { BulkheadOptions } from './bulkhead-options.interface.js';
import type { CircuitBreakerOptions } from './circuit-breaker-options.interface.js';
import type { Duration } from './duration.interface.js';
import type { FallbackHandler } from './fallback-options.interface.js';
import type { OutboundRateLimitOptions } from './outbound-rate-limit-options.interface.js';
import type { RetryOptions } from './retry-options.interface.js';

/**
 * The stages of a policy as options: a preset in `forRoot({ presets })`, or
 * inline options for `ResilienceService.create()`. They always compose in
 * the canonical order, whatever order they are written in.
 */
export interface ResiliencePreset {
  /** A number is `{ attempts }`; `false` is a single attempt. */
  retry?: number | false | RetryOptions;
  /** Budget per attempt. */
  timeout?: Duration;
  /** A string refers to a breaker configured elsewhere. A preset's breaker is shared under the preset's name. */
  circuitBreaker?: CircuitBreakerOptions | string;
  /** A string refers to a bulkhead configured elsewhere. A preset's bulkhead is shared under the preset's name. */
  bulkhead?: BulkheadOptions | string;
  /** Keeps calls to the dependency within its quota, shared under the preset's name. */
  outboundRateLimit?: OutboundRateLimitOptions | string;
  /**
   * Replaces failures (`execute()` callers). At entrypoints, prefer
   * `@Fallback()`, whose handler also gets the `ExecutionContext`.
   */
  fallback?: FallbackHandler | { handler: FallbackHandler; handleIf?: (error: unknown) => boolean };
}

/** `ResilienceModuleOptions.defaults`: field defaults, merged under what decorators and presets set. */
export interface ResilienceDefaults {
  /** A number is `{ attempts }`. */
  retry?: number | RetryOptions;
  /** Used by `@Timeout()` without an argument. */
  timeout?: Duration;
  circuitBreaker?: Omit<CircuitBreakerOptions, 'name'>;
  bulkhead?: Omit<BulkheadOptions, 'name'>;
}

export interface ResilienceModuleOptions {
  /** Field defaults for every stage that a decorator or preset turns on. They never turn a stage on. */
  defaults?: ResilienceDefaults;
  /**
   * Named presets, applied with `@Resilience(name)` and
   * `ResilienceService.preset(name)`. A preset's breaker, bulkhead and
   * outbound rate limit are shared under the preset's name unless they set
   * their own.
   */
  presets?: Record<string, ResiliencePreset>;
  /**
   * Turns resilience errors (`ResilienceTimeoutError`, `CircuitOpenError`, …)
   * thrown by any handler, including from policies used inside services, into
   * the transport's error (504/503 for HTTP, `RpcException`, `WsException`,
   * `GraphQLError`). Default true.
   */
  mapErrors?: boolean;
}
