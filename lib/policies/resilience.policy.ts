import { HttpException } from '@nestjs/common';
import { isObservable, Observable, type Subscription } from 'rxjs';
import { currentAttempt, runInAttempt } from '../context/attempt-scope.context.js';
import type { ResilienceEvent } from '../events/resilience-events.interface.js';
import { publish } from '../events/resilience.channels.js';
import type { AttemptContext, ExecuteOptions } from '../interfaces/execute-options.interface.js';
import { linkSignal } from '../utils/timers.util.js';

/**
 * State threaded through the stages of one execution.
 * @internal
 */
export interface Execution extends AttemptContext {
  readonly source?: string;
  /**
   * True once the outcome can no longer be replaced: an Observable already
   * emitted, or the HTTP response was committed. Retry, Fallback and Timeout
   * stand down from then on.
   */
  readonly committed: () => boolean;
  /** Turns a fallback result into the execution's result. */
  readonly adopt: (value: unknown) => Promise<unknown>;
  /** What entrypoint fallbacks receive as their second argument (Nest's `ExecutionContext`). */
  readonly invocation?: unknown;
}

/** @internal */
export type Next<T> = (execution: Execution) => Promise<T>;

/**
 * A resilience policy. Every primitive (`RetryPolicy`, `TimeoutPolicy`,
 * `CircuitBreakerPolicy`, `BulkheadPolicy`, `OutboundRateLimitPolicy`,
 * `FallbackPolicy`), every composition (`ResiliencePolicy.wrap(...)`) and
 * every preset (`ResilienceService.preset(name)`) is one.
 */
export abstract class ResiliencePolicy {
  constructor(
    /** Labels this policy's events (`policy`) and errors. */
    readonly name: string,
  ) {}

  /**
   * Composes policies in the order given: the first is outermost.
   * `ResiliencePolicy.wrap(fallback, retry, breaker, timeout)` runs
   * `fallback(retry(breaker(timeout(call))))`.
   */
  static wrap(...policies: ResiliencePolicy[]): ResiliencePolicy {
    return new PolicyWrap(policies.flatMap((p) => (p instanceof PolicyWrap ? p.policies : [p])));
  }

  /** @internal Runs `next` under this policy. */
  abstract run<T>(next: Next<T>, execution: Execution): Promise<T>;

  /**
   * Runs `fn` under this policy. `fn` may be called several times (by a
   * retry) and receives its attempt's signal, which it should honor. While
   * `fn` runs, `ResilienceContext` reports that attempt.
   */
  execute<T>(fn: (context: AttemptContext) => T | PromiseLike<T>, options: ExecuteOptions = {}): Promise<T> {
    const signal = options.signal ?? currentAttempt()?.signal ?? new AbortController().signal;

    if (signal.aborted) {
      return Promise.reject(signal.reason);
    }

    const execution: Execution = {
      signal,
      attempt: 1,
      source: options.source,
      committed: () => false,
      adopt: async (value) => value,
    };

    return this.run(
      async ({ signal, attempt }) => runInAttempt({ signal, attempt }, () => fn({ signal, attempt }), true),
      execution,
    );
  }

  /**
   * Observable flavor: every attempt calls `factory` and subscribes to what it
   * returns, forwarding values as they arrive. The returned Observable is
   * cold; unsubscribing cancels the execution. Once a value has been emitted
   * the outcome is committed: later errors are neither retried nor replaced
   * by a fallback, and Timeout stops timing.
   */
  executeObservable<T>(factory: (context: AttemptContext) => Observable<T>, options: ExecuteOptions = {}): Observable<T> {
    return observe(this, factory, options);
  }

  protected emit(event: ResilienceEvent): void {
    publish(this, event);
  }
}

/** @internal Options only the entrypoint interceptor passes. */
export interface ObserveOptions extends ExecuteOptions {
  /** Extra "the outcome is already committed" check (the interceptor checks headers sent). */
  committed?: () => boolean;
  invocation?: unknown;
}

/** @internal `executeObservable()`, plus the interceptor's options. */
export function observe<T>(
  policy: ResiliencePolicy,
  factory: (context: AttemptContext) => Observable<T>,
  options: ObserveOptions = {},
): Observable<T> {
  return new Observable<T>((subscriber) => {
    // The execution's signal: aborted by the caller's signal (or attempt)
    // while the execution runs, and by the consumer unsubscribing.
    const controller = new AbortController();
    const outer = options.signal ?? currentAttempt()?.signal;
    const unlink = outer ? linkSignal(outer, controller) : () => undefined;
    const { signal } = controller;
    let emitted = false;
    let settled = false;

    const forward = (source: Observable<unknown>, attemptSignal: AbortSignal) =>
      new Promise<void>((resolve, reject) => {
        if (attemptSignal.aborted) {
          return reject(attemptSignal.reason);
        }

        let sub: Subscription | undefined;
        const onAbort = () => {
          sub?.unsubscribe();
          reject(attemptSignal.reason);
        };

        attemptSignal.addEventListener('abort', onAbort, { once: true });
        sub = source.subscribe({
          next: (value) => {
            emitted = true;
            subscriber.next(value as T);
          },
          error: (error) => {
            attemptSignal.removeEventListener('abort', onAbort);
            reject(error);
          },
          complete: () => {
            attemptSignal.removeEventListener('abort', onAbort);
            resolve();
          },
        });
        // A value emitted while subscribing can make the consumer unsubscribe
        // (`firstValueFrom()`, `take(1)`), which aborts the signal before
        // `sub` exists: onAbort couldn't unsubscribe the source then.
        if (attemptSignal.aborted) {
          sub.unsubscribe();
        }
      });

    const execution: Execution = {
      signal,
      attempt: 1,
      source: options.source,
      invocation: options.invocation,
      committed: () => emitted || (options.committed?.() ?? false),
      adopt: async (value) => {
        if (isObservable(value)) {
          return forward(value, signal);
        }

        const resolved = await value;
        emitted = true;
        subscriber.next(resolved as T);
      },
    };

    if (signal.aborted) {
      unlink();
      subscriber.error(signal.reason);
      return;
    }
    // Each attempt calls the factory and subscribes inside its own attempt
    // scope, so code running in it (Nest binds pipes and the handler at
    // `next.handle()`) sees that attempt in ResilienceContext and @Signal().
    policy
      .run(
        async ({ signal, attempt }) =>
          runInAttempt({ signal, attempt }, () => forward(factory({ signal, attempt }), signal), true),
        execution,
      )
      .then(
        () => {
          settled = true;
          unlink();
          subscriber.complete();
        },
        (error) => {
          settled = true;
          unlink();
          subscriber.error(error);
        },
      );

    return () => {
      if (!settled) {
        controller.abort();
      }
      unlink();
    };
  });
}

/** @internal Policies composed in explicit order: the first is outermost. */
export class PolicyWrap extends ResiliencePolicy {
  constructor(readonly policies: readonly ResiliencePolicy[]) {
    super(policies.map((p) => p.name).join(' > ') || 'noop');
  }

  /** @internal */
  run<T>(next: Next<T>, execution: Execution): Promise<T> {
    let chain = next;
    for (let i = this.policies.length - 1; i >= 0; i--) {
      const policy = this.policies[i];
      const inner = chain;
      chain = (e) => policy.run(inner, e);
    }

    return chain(execution);
  }
}

/** Dependency answers that mean "slow down" rather than "your request is wrong". */
const BACK_PRESSURE_STATUSES = new Set([408, 429]);

/**
 * Errors caused by the request rather than by a failing dependency. By default
 * they are not retried, not recorded by breakers and not replaced by fallbacks:
 *
 * - `HttpException`s below 500: the handler rejected its caller's request.
 * - Any other error with a numeric `status` or `statusCode` from 400 to 499,
 *   which is how HTTP and SDK clients report a dependency's 4xx answer (a 422
 *   says this request is invalid, not that the dependency is down). 408 and
 *   429 are the exception: they are the dependency pushing back, so they are
 *   retried and recorded like 5xx answers.
 */
export function isClientError(error: unknown): boolean {
  if (error instanceof HttpException) {
    return error.getStatus() < 500;
  }

  if (typeof error !== 'object' || error === null) {
    return false;
  }

  const { status, statusCode } = error as { status?: unknown; statusCode?: unknown };
  const code = typeof status === 'number' ? status : statusCode;
  return typeof code === 'number' && code >= 400 && code < 500 && !BACK_PRESSURE_STATUSES.has(code);
}

/** @internal Runs `next`, turning a synchronous throw into a rejection. */
export function call<T>(next: Next<T>, execution: Execution): Promise<T> {
  try {
    return next(execution);
  } catch (error) {
    return Promise.reject(error);
  }
}
