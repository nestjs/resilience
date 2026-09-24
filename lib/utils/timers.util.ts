/**
 * The longest delay `setTimeout` supports: 2^31 - 1 ms, about 24.8 days.
 * Node runs a longer timer after 1 ms instead, so a `timeout: '30d'` would
 * time out every call at once.
 */
const MAX_TIMER_DELAY = 2 ** 31 - 1;

/** @internal `setTimeout`, with delays beyond what timers support treated as the longest one. */
export function setTimer(callback: () => void, ms: number): ReturnType<typeof setTimeout> {
  return setTimeout(callback, Math.min(ms, MAX_TIMER_DELAY));
}

/**
 * @internal Aborts `controller` with `source`'s reason when `source` aborts,
 * until the returned function is called. Not `AbortSignal.any()`: Node keeps
 * a composite signal reachable from its sources for as long as the composite
 * has listeners, and the attempt signals built here go to user code that adds
 * listeners it never removes. One composite per attempt on a long-lived
 * source (a job's signal, via `ResilienceContext.run()`) would never be freed.
 */
export function linkSignal(source: AbortSignal, controller: AbortController): () => void {
  if (source.aborted) {
    controller.abort(source.reason);
    return () => undefined;
  }

  const onAbort = () => controller.abort(source.reason);
  source.addEventListener('abort', onAbort, { once: true });
  return () => source.removeEventListener('abort', onAbort);
}

/** @internal `setTimeout` as a promise that rejects with `signal.reason` on abort. */
export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      return reject(signal.reason);
    }
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal!.reason);
    };

    const timer = setTimer(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}
