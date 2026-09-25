import { getEventListeners } from 'node:events';
import { lastValueFrom, NEVER, of } from 'rxjs';
import {
  BulkheadPolicy,
  OutboundRateLimitPolicy,
  ResiliencePolicy,
  ResilienceTimeoutError,
  RetryPolicy,
  TimeoutPolicy,
} from '../lib/index.js';
import { recordEvents } from './events.js';

/**
 * Signals that `AbortSignal.any()` derived from `signal` (Node keeps them in
 * an internal set on the source). 0 when Node exposes no such set.
 */
function dependantsOf(signal: AbortSignal): number {
  const key = Object.getOwnPropertySymbols(signal).find((s) => s.description === 'kDependantSignals');
  return key ? ((signal as unknown as Record<symbol, { size?: number } | undefined>)[key]?.size ?? 0) : 0;
}

describe('TimeoutPolicy', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('leaves nothing attached to a long-lived caller signal, however many attempts ran under it', async () => {
    // A job cancels everything it starts with one signal (ResilienceContext.run(),
    // or execute({ signal })). Node keeps an AbortSignal.any() composite alive
    // for as long as it has listeners, and handlers add listeners to their
    // attempt signal that they never remove: one composite per attempt on the
    // job's signal would grow with every call the job makes.
    const job = new AbortController();
    const policy = ResiliencePolicy.wrap(new RetryPolicy(2), new TimeoutPolicy('1s'));
    const keepsListening = ({ signal }: { signal: AbortSignal }) => signal.addEventListener('abort', () => undefined);
    for (let i = 0; i < 200; i++) {
      await policy.execute((attempt) => (keepsListening(attempt), i), { signal: job.signal });
      await lastValueFrom(policy.executeObservable((attempt) => (keepsListening(attempt), of(i)), { signal: job.signal }));
    }
    expect(getEventListeners(job.signal, 'abort')).toHaveLength(0);
    expect(dependantsOf(job.signal)).toBe(0);
  });

  it('rejects with ResilienceTimeoutError and aborts the signal the call received', async () => {
    const events = recordEvents();
    const policy = new TimeoutPolicy({ timeout: 100, name: 'catalog' });
    let seen: AbortSignal | undefined;
    const result = policy
      .execute(({ signal }) => {
        seen = signal;
        return new Promise(() => {}); // never settles, ignores the signal
      })
      .catch((e) => e);

    await vi.advanceTimersByTimeAsync(99);
    expect(seen!.aborted).toBe(false);
    await vi.advanceTimersByTimeAsync(1);

    const error = await result;
    expect(error).toBeInstanceOf(ResilienceTimeoutError);
    expect(error).toMatchObject({ timeoutMs: 100, policy: 'catalog', code: 'TIMEOUT', name: 'ResilienceTimeoutError' });
    expect(seen!.aborted).toBe(true);
    expect(seen!.reason).toBe(error);
    expect(events).toEqual([{ type: 'timeout', policy: 'catalog', source: undefined, timeoutMs: 100 }]);
  });

  it('takes a Duration: milliseconds or a string', () => {
    expect(new TimeoutPolicy(1_500).timeout).toBe(1_500);
    expect(new TimeoutPolicy('1.5s').timeout).toBe(1_500);
    expect(new TimeoutPolicy({ timeout: '2m' }).timeout).toBe(120_000);
    expect(() => new TimeoutPolicy('soon' as never)).toThrow('Invalid duration "soon"');
    expect(() => new TimeoutPolicy(0)).toThrow('Invalid duration 0. Use a duration longer than 0.');
  });

  it('lets a cooperative call stop early with the same error', async () => {
    const policy = new TimeoutPolicy(50);
    let cleanedUp = false;
    const result = policy
      .execute(
        ({ signal }) =>
          new Promise((_, reject) =>
            signal.addEventListener('abort', () => {
              cleanedUp = true;
              reject(signal.reason);
            }),
          ),
      )
      .catch((e) => e);
    await vi.advanceTimersByTimeAsync(50);
    expect(await result).toBeInstanceOf(ResilienceTimeoutError);
    expect(cleanedUp).toBe(true);
  });

  it('resolves fast calls and leaves no timer behind', async () => {
    const policy = new TimeoutPolicy('1s');
    const result = policy.execute(async () => 'fast');
    await vi.advanceTimersByTimeAsync(0);
    expect(await result).toBe('fast');
    expect(vi.getTimerCount()).toBe(0);
  });

  it('drops the late result of a call that ignored the signal, without an unhandled rejection', async () => {
    const policy = new TimeoutPolicy(10);
    const result = policy
      .execute(() => new Promise((_, reject) => setTimeout(() => reject(new Error('late')), 50)))
      .catch((e) => e);
    await vi.advanceTimersByTimeAsync(100);
    expect(await result).toBeInstanceOf(ResilienceTimeoutError);
  });

  it('is per attempt inside Retry, and a total budget outside it', async () => {
    let calls = 0;
    const slowThenFast = () => (++calls < 3 ? new Promise(() => {}) : Promise.resolve('ok'));

    const perAttempt = ResiliencePolicy.wrap(
      new RetryPolicy({ attempts: 3, backoff: { delay: 0 } }),
      new TimeoutPolicy(100),
    ).execute(slowThenFast);
    await vi.advanceTimersByTimeAsync(250);
    expect(await perAttempt).toBe('ok');

    calls = 0;
    const total = ResiliencePolicy.wrap(
      new TimeoutPolicy(150),
      new RetryPolicy({ attempts: 3, backoff: { delay: 0 } }),
      new TimeoutPolicy(100),
    )
      .execute(slowThenFast)
      .catch((e) => e);
    await vi.advanceTimersByTimeAsync(300);
    expect(await total).toMatchObject({ timeoutMs: 150 });
    expect(calls).toBe(2); // the outer budget ran out during the second attempt
  });
});

describe('Durations longer than timers support', () => {
  // Node runs a setTimeout() longer than 2^31 - 1 ms (about 24.8 days) after
  // 1 ms. These run on real timers: fake ones don't reproduce the overflow.
  const settlesIn = (ms: number) => new Promise<string>((resolve) => setTimeout(() => resolve('done'), ms));

  it('a 30-day timeout does not time out at once', async () => {
    expect(await new TimeoutPolicy('30d').execute(() => settlesIn(20))).toBe('done');
  });

  it('a 30-day queue timeout does not reject at once', async () => {
    const bulkhead = new BulkheadPolicy({ maxConcurrent: 1, maxQueue: 1, queueTimeout: '30d' });
    const running = bulkhead.execute(() => settlesIn(20));
    expect(await bulkhead.execute(() => 'queued')).toBe('queued');
    await running;
  });

  it('a 30-day backoff does not retry at once', async () => {
    const controller = new AbortController();
    let calls = 0;
    const result = new RetryPolicy({ attempts: 2, backoff: () => '30d' })
      .execute(
        () => {
          calls++;
          throw new Error('down');
        },
        { signal: controller.signal },
      )
      .catch((error: unknown) => error);
    await settlesIn(20);
    expect(calls).toBe(1);
    controller.abort(new Error('gave up'));
    expect(((await result) as Error).message).toBe('gave up');
  });

  it("a 30-day wait for a token doesn't end at once", async () => {
    const quota = new OutboundRateLimitPolicy({ limit: 1, interval: '30d', maxWait: '30d' });
    await quota.execute(() => 'first');
    const controller = new AbortController();
    let ran = false;
    const second = quota.execute(() => (ran = true), { signal: controller.signal }).catch((error: unknown) => error);
    await settlesIn(20);
    expect(ran).toBe(false);
    controller.abort(new Error('gave up'));
    expect(((await second) as Error).message).toBe('gave up');
  });
});

describe('TimeoutPolicy: cancellation and attempts', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  const untilAborted = ({ signal }: { signal: AbortSignal }) =>
    new Promise<never>((_, reject) => signal.addEventListener('abort', () => reject(signal.reason)));

  it("passes a caller's abort on to the attempt with the caller's reason, and emits no timeout", async () => {
    const events = recordEvents();
    const controller = new AbortController();
    let seen: AbortSignal | undefined;
    const result = new TimeoutPolicy(1_000)
      .execute(
        (attempt) => {
          seen = attempt.signal;
          return untilAborted(attempt);
        },
        { signal: controller.signal },
      )
      .catch((e: unknown) => e);
    await vi.advanceTimersByTimeAsync(10);
    const reason = new Error('client disconnected');
    controller.abort(reason);

    expect(await result).toBe(reason);
    expect(seen!.reason).toBe(reason);
    expect(seen).not.toBe(controller.signal); // the attempt has its own signal
    await vi.advanceTimersByTimeAsync(2_000);
    expect(events).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('does not start a call whose caller already aborted', async () => {
    const fn = vi.fn();
    await expect(new TimeoutPolicy(100).execute(fn, { signal: AbortSignal.abort('gone') })).rejects.toBe('gone');
    expect(fn).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('turns a synchronous throw into a rejection and clears its timer', async () => {
    await expect(
      new TimeoutPolicy(100).execute(() => {
        throw new Error('sync');
      }),
    ).rejects.toThrow('sync');
    expect(vi.getTimerCount()).toBe(0);
  });

  it('gives each attempt inside Retry a fresh signal: a timed-out first attempt leaves the second one running', async () => {
    const signals: AbortSignal[] = [];
    const policy = ResiliencePolicy.wrap(
      new RetryPolicy({ attempts: 2, backoff: { delay: 0 } }),
      new TimeoutPolicy(100),
    );
    const result = policy.execute((attempt) => {
      signals.push(attempt.signal);
      return attempt.attempt === 1 ? untilAborted(attempt) : new Promise((r) => setTimeout(() => r('second'), 50));
    });
    await vi.advanceTimersByTimeAsync(200);
    expect(await result).toBe('second');
    expect(signals[0].aborted).toBe(true);
    expect(signals[0].reason).toBeInstanceOf(ResilienceTimeoutError);
    expect(signals[1]).not.toBe(signals[0]);
    expect(signals[1].aborted).toBe(false);
  });

  it('labels its timeout event with the execution source, and with a retry both events tell the story', async () => {
    const events = recordEvents();
    const policy = ResiliencePolicy.wrap(
      new RetryPolicy({ attempts: 2, backoff: { delay: 0 }, name: 'partner' }),
      new TimeoutPolicy({ timeout: 30, name: 'partner' }),
    );
    const result = policy.execute(untilAborted, { source: 'Jobs.sync' }).catch((e: unknown) => e);
    await vi.advanceTimersByTimeAsync(100);
    expect(await result).toBeInstanceOf(ResilienceTimeoutError);
    expect(events.map((e) => [e.type, e.source])).toEqual([
      ['timeout', 'Jobs.sync'],
      ['retry', 'Jobs.sync'],
      ['timeout', 'Jobs.sync'],
    ]);
    expect(events[1]).toMatchObject({ attempt: 1, error: expect.any(ResilienceTimeoutError) });
  });

  it('retries an Observable attempt that produced no value within the budget', async () => {
    const policy = ResiliencePolicy.wrap(
      new RetryPolicy({ attempts: 2, backoff: { delay: 0 } }),
      new TimeoutPolicy(100),
    );
    let subscriptions = 0;
    const result = lastValueFrom(
      policy.executeObservable(({ attempt }) => {
        subscriptions++;
        return attempt === 1 ? NEVER : of('second');
      }),
    );
    await vi.advanceTimersByTimeAsync(200);
    expect(await result).toBe('second');
    expect(subscriptions).toBe(2);
  });
});

describe('Durations', () => {
  it('accept milliseconds and every unit from ms to weeks, with decimals', () => {
    expect(new TimeoutPolicy('250ms').timeout).toBe(250);
    expect(new TimeoutPolicy('0.5s').timeout).toBe(500);
    expect(new TimeoutPolicy('2m').timeout).toBe(120_000);
    expect(new TimeoutPolicy('1.5h').timeout).toBe(5_400_000);
    expect(new TimeoutPolicy('1d').timeout).toBe(86_400_000);
    expect(new TimeoutPolicy('1w').timeout).toBe(604_800_000);
  });

  it('reject negative, non-finite and malformed values', () => {
    expect(() => new TimeoutPolicy(-5)).toThrow('Invalid duration -5. Use a non-negative number of milliseconds.');
    expect(() => new TimeoutPolicy(Infinity)).toThrow('Invalid duration Infinity');
    expect(() => new TimeoutPolicy(Number.NaN)).toThrow('Invalid duration NaN');
    for (const text of ['-1s', '1S', ' 1s', '1 s', '1sec', 's', '1y']) {
      expect(() => new TimeoutPolicy(text as never)).toThrow(`Invalid duration "${text}"`);
    }
  });
});
