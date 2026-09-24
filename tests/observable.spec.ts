import {
  BehaviorSubject,
  concat,
  defer,
  firstValueFrom,
  lastValueFrom,
  NEVER,
  Observable,
  of,
  throwError,
  timer,
  toArray,
} from 'rxjs';
import { map } from 'rxjs/operators';
import { FallbackPolicy, ResiliencePolicy, RetryPolicy, ResilienceTimeoutError, TimeoutPolicy } from '../lib/index.js';

const noDelay = { delay: 0 };

describe('ResiliencePolicy.executeObservable', () => {
  afterEach(() => vi.useRealTimers());

  it('calls the factory again for every attempt and forwards values', async () => {
    let subscriptions = 0;
    const source = defer(() => {
      subscriptions++;
      return subscriptions < 3 ? throwError(() => new Error('flaky')) : of('a', 'b');
    });
    const attempts: number[] = [];
    const result = new RetryPolicy({ attempts: 3, backoff: noDelay }).executeObservable(({ attempt }) => {
      attempts.push(attempt);
      return source;
    });
    expect(await lastValueFrom(result.pipe(toArray()))).toEqual(['a', 'b']);
    expect(attempts).toEqual([1, 2, 3]);
  });

  it('is lazy: nothing runs until subscribed', () => {
    const factory = vi.fn(() => of(1));
    new RetryPolicy().executeObservable(factory);
    expect(factory).not.toHaveBeenCalled();
  });

  it('does not retry once a value was emitted (the outcome is committed)', async () => {
    const factory = vi.fn(() => concat(of('partial'), throwError(() => new Error('mid-stream'))));
    const values: unknown[] = [];
    const error = await new Promise((resolve) =>
      new RetryPolicy({ attempts: 3, backoff: noDelay })
        .executeObservable(factory)
        .subscribe({ next: (v) => values.push(v), error: resolve }),
    );
    expect((error as Error).message).toBe('mid-stream');
    expect(values).toEqual(['partial']);
    expect(factory).toHaveBeenCalledTimes(1);
  });

  it('times out on the first value, not on the stream lifetime', async () => {
    vi.useFakeTimers();
    const values: number[] = [];
    let done = false;
    new TimeoutPolicy(100)
      .executeObservable(() => timer(50, 80).pipe(map((i) => i)))
      .subscribe({ next: (v) => values.push(v), complete: () => (done = true) });
    await vi.advanceTimersByTimeAsync(400);
    expect(values).toEqual([0, 1, 2, 3, 4]);
    expect(done).toBe(false);
  });

  it('errors with ResilienceTimeoutError and unsubscribes the attempt when no value arrives in time', async () => {
    vi.useFakeTimers();
    let unsubscribed = false;
    const source = new Observable(() => () => (unsubscribed = true));
    const error = new Promise((resolve) =>
      new TimeoutPolicy(100).executeObservable(() => source).subscribe({ error: resolve }),
    );
    await vi.advanceTimersByTimeAsync(100);
    expect(await error).toBeInstanceOf(ResilienceTimeoutError);
    expect(unsubscribed).toBe(true);
  });

  it('aborts the signal when the subscriber unsubscribes', () => {
    let signal: AbortSignal | undefined;
    const subscription = new RetryPolicy()
      .executeObservable((ctx) => {
        signal = ctx.signal;
        return NEVER;
      })
      .subscribe();
    expect(signal!.aborted).toBe(false);
    subscription.unsubscribe();
    expect(signal!.aborted).toBe(true);
  });

  it('unsubscribes a source that emits while subscribing, when the consumer takes one value', async () => {
    const state = new BehaviorSubject('current');
    // The second attempt subscribes after the backoff; its value arrives
    // synchronously and firstValueFrom() unsubscribes right away.
    const value = await firstValueFrom(
      new RetryPolicy({ attempts: 2, backoff: { delay: 1 } }).executeObservable(({ attempt }) =>
        attempt === 1 ? throwError(() => new Error('flaky')) : state,
      ),
    );
    expect(value).toBe('current');
    expect(state.observed).toBe(false);
  });

  it('subscribes to an Observable returned by a fallback', async () => {
    const policy = ResiliencePolicy.wrap(
      new FallbackPolicy(() => of('cached-1', 'cached-2')),
      new RetryPolicy({ attempts: 2, backoff: noDelay }),
    );
    const values = await lastValueFrom(
      policy.executeObservable(() => throwError(() => new Error('down'))).pipe(toArray()),
    );
    expect(values).toEqual(['cached-1', 'cached-2']);
  });
});
