import { RejectionError } from '../errors/rejection.error.js';
import type { RetryOptions } from '../interfaces/retry-options.interface.js';
import { computeBackoff, resolveBackoff } from '../utils/backoff.util.js';
import { callPredicate, countOption, durationOption } from '../utils/options.util.js';
import { sleep } from '../utils/timers.util.js';
import { call, isClientError, ResiliencePolicy, type Execution, type Next } from './resilience.policy.js';

/** @internal `retry: 5` is `{ attempts: 5 }`, `retry: false` is a single attempt. */
export function normalizeRetry<R extends RetryOptions>(retry: number | false | R): R | RetryOptions {
  if (retry === false) {
    return { attempts: 1 };
  }
  if (typeof retry === 'number') {
    return { attempts: retry };
  }
  return retry;
}

const defaultRetryIf = (error: unknown): boolean => !isClientError(error);

export class RetryPolicy extends ResiliencePolicy {
  readonly attempts: number;
  private readonly delay: (attempt: number, error: unknown) => number;
  private readonly retryIf: (error: unknown, attempt: number) => boolean;
  private readonly retryOnResult?: (result: unknown, attempt: number) => boolean;

  /** `new RetryPolicy(5)` is `{ attempts: 5 }`; `name` labels its events. */
  constructor(options: number | false | (RetryOptions & { name?: string }) = {}) {
    const retry: RetryOptions & { name?: string } = normalizeRetry(options);
    super(retry.name ?? 'retry');
    this.attempts = countOption(retry.attempts ?? 3, 'attempts', 1, true);

    const backoff = retry.backoff;
    if (typeof backoff === 'function') {
      this.delay = (attempt, error) => {
        const wait = backoff(attempt, error);
        return typeof wait === 'number' && wait < 0 ? 0 : durationOption(wait, 'backoff');
      };
    } else {
      const resolved = resolveBackoff(backoff);
      this.delay = (attempt) => computeBackoff(resolved, attempt);
    }

    this.retryIf = retry.retryIf ?? defaultRetryIf;
    this.retryOnResult = retry.retryOnResult;
  }

  /** @internal */
  async run<T>(next: Next<T>, execution: Execution): Promise<T> {
    for (let attempt = 1; ; attempt++) {
      const last = attempt >= this.attempts;
      let outcome: { value: T } | { error: unknown };
      try {
        outcome = { value: await call(next, { ...execution, attempt }) };
      } catch (error) {
        outcome = { error };
      }

      // The predicates run outside the try: one that throws is a bug to
      // surface, not a failed attempt to retry.
      let error: unknown;
      if ('value' in outcome) {
        const { value } = outcome;
        if (last || !this.retryOnResult || execution.committed()) {
          return value;
        }
        if (!callPredicate('retryOnResult', this.retryOnResult, value, attempt)) {
          return value;
        }
      } else {
        error = outcome.error;
        const final = last || execution.signal.aborted || execution.committed() || error instanceof RejectionError;
        if (final || !callPredicate('retryIf', this.retryIf, error, attempt)) {
          throw error;
        }
      }

      const delay = this.delay(attempt, error);
      this.emit({ type: 'retry', policy: this.name, source: execution.source, attempt, delayMs: delay, error });
      await sleep(delay, execution.signal);
    }
  }
}
