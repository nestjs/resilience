import { AsyncLocalStorage } from 'node:async_hooks';

/** The attempt a piece of code runs in: set per attempt by entrypoints and `execute()`. */
export interface AttemptScope {
  readonly signal: AbortSignal;
  readonly attempt: number;
}

interface StoredScope extends AttemptScope {
  /** The scope this one was opened in. */
  readonly parent: StoredScope | undefined;
  /** The attempt settled: code that still runs in its async context no longer belongs to it. */
  ended: boolean;
}

const storage = new AsyncLocalStorage<StoredScope>();

/**
 * @internal Runs `fn` with `scope` as the current attempt. With
 * `endsWhenSettled` (the attempts policies run), the scope stops being
 * current once `fn`'s result settles or the attempt's signal aborts (a
 * timeout, a cancelled execution), whichever comes first: a function that
 * ignores its signal keeps running, but no longer as the attempt. Async
 * resources created during an attempt keep its context for good: a pooled
 * connection opened by the first request (a microservice `ClientProxy`) runs
 * every later callback in it, and code on those callbacks would otherwise
 * inherit that request's signal, aborted long ago if it timed out.
 */
export function runInAttempt<T>(scope: AttemptScope, fn: () => T, endsWhenSettled = false): T {
  const stored: StoredScope = {
    signal: scope.signal,
    attempt: scope.attempt,
    parent: storage.getStore(),
    ended: false,
  };

  if (!endsWhenSettled) {
    return storage.run(stored, fn);
  }

  const end = () => {
    stored.ended = true;
    scope.signal.removeEventListener('abort', end);
  };
  if (scope.signal.aborted) {
    end();
  } else {
    scope.signal.addEventListener('abort', end, { once: true });
  }

  let result: T;
  try {
    result = storage.run(stored, fn);
  } catch (error) {
    end();
    throw error;
  }

  if (typeof (result as PromiseLike<unknown> | undefined)?.then === 'function') {
    (result as PromiseLike<unknown>).then(end, end);
  } else {
    end();
  }
  return result;
}

/** @internal The innermost attempt the calling code runs in that hasn't ended, if any. */
export function currentAttempt(): AttemptScope | undefined {
  let scope = storage.getStore();
  while (scope?.ended) {
    scope = scope.parent;
  }
  return scope;
}

/** @internal What `@Signal()` injects; bootstrap looks for it to find handlers that use `@Signal()`. */
export function signalOfAttempt(): AbortSignal | undefined {
  return currentAttempt()?.signal;
}
