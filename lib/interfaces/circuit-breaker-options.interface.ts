import type { Duration } from './duration.interface.js';

export interface CircuitBreakerOptions {
  /** Breakers with the same name share their state. Without one, a decorator's breaker belongs to its handler. */
  name?: string;
  /** Failure percentage (above 0, at most 100) at or above which the breaker opens. Default 50. */
  failureRateThreshold?: number;
  /** Calls the window needs before the rate is evaluated: at most `slidingWindow.size` for a count window. Default 10. */
  minimumCalls?: number;
  /** The last `size` calls, or the calls of the last `size` of time. Default `{ type: 'count', size: 20 }`. */
  slidingWindow?: { type: 'count'; size: number } | { type: 'time'; size: Duration };
  /** How long the breaker stays open before letting probe calls through. Default `30s`. */
  openDuration?: Duration;
  /** Probe calls let through while half-open. Default 1. */
  halfOpenMaxCalls?: number;
  /**
   * Which errors count as failures. Default: every error except client
   * errors (`isClientError()`: 4xx `HttpException`s, and other errors with a
   * 4xx `status` apart from 408 and 429). Rejections from inner policies and
   * caller aborts never count, whatever this returns. Errors that don't count
   * are ignored, not counted as successes.
   */
  recordIf?: (error: unknown) => boolean;
}
