import { ResilienceError } from './resilience.error.js';

/**
 * A policy refused to run the call at all (open breaker, full
 * bulkhead, outbound rate limit), so the call never started. Rejections are
 * never retried and never recorded as breaker failures, whatever `retryIf`
 * or `recordIf` return: they exist to shed load.
 */
export abstract class RejectionError extends ResilienceError {
  constructor(
    message: string,
    policy: string,
    /** Milliseconds after which a new call may be let through, if known. */
    readonly retryAfterMs?: number,
  ) {
    super(message, policy);
  }
}
