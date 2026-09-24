import type { AttemptScope } from '../context/attempt-scope.context.js';

/** What a call run through a policy receives: its attempt's signal and number. */
export interface AttemptContext extends AttemptScope {
  /**
   * Aborted when this attempt times out (reason: `ResilienceTimeoutError`), when the
   * execution is cancelled, and, for Observables, when the subscriber
   * unsubscribes. Pass it on to `fetch`, database drivers, SDKs.
   */
  readonly signal: AbortSignal;
  /** 1-based attempt number. */
  readonly attempt: number;
}

export interface ExecuteOptions {
  /**
   * Cancels the execution: aborts in-flight attempts, backoff and queue
   * waits. Default: the signal of the attempt the caller runs in (inside a
   * resilient entrypoint, or `ResilienceContext.run()`), so a handler's
   * timeout also stops the calls it made through policies.
   */
  signal?: AbortSignal;
  /** Label for the events this execution emits (their `source`), e.g. the calling method. */
  source?: string;
}
