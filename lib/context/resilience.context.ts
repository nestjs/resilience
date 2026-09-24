import { Injectable } from '@nestjs/common';
import { currentAttempt, runInAttempt } from './attempt-scope.context.js';

/**
 * The attempt the calling code runs in: an attempt of a resilient
 * entrypoint, of a policy's `execute()`, or a scope opened with `run()`.
 * Backed by `AsyncLocalStorage`, so it reaches every service the handler
 * calls, on every transport.
 */
@Injectable()
export class ResilienceContext {
  /**
   * Aborted when the attempt times out (reason: `ResilienceTimeoutError`) or the call
   * is cancelled. `undefined` outside an attempt.
   */
  get signal(): AbortSignal | undefined {
    return currentAttempt()?.signal;
  }

  /** 1-based attempt number. `undefined` outside an attempt. */
  get attempt(): number | undefined {
    return currentAttempt()?.attempt;
  }

  /**
   * Runs `fn` as an attempt with `signal` (a scheduled job, a script, a
   * test). Policies executed inside it stop when `signal` aborts.
   */
  run<T>(scope: { signal: AbortSignal; attempt?: number }, fn: () => T): T {
    return runInAttempt({ signal: scope.signal, attempt: scope.attempt ?? 1 }, fn);
  }
}
