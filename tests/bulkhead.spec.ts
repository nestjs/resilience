import {
  BulkheadFullError,
  BulkheadPolicy,
  OutboundRateLimitError,
  OutboundRateLimitPolicy,
  ResiliencePolicy,
  ResilienceTimeoutError,
  RetryPolicy,
  TimeoutPolicy,
} from '../lib/index.js';
import { recordEvents } from './events.js';

function gate() {
  let open!: () => void;
  const promise = new Promise<void>((resolve) => (open = resolve));
  return { promise, open };
}

describe('BulkheadPolicy', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('runs at most maxConcurrent calls and rejects the rest when there is no queue', async () => {
    const events = recordEvents();
    const bulkhead = new BulkheadPolicy({ name: 'reports', maxConcurrent: 2 });
    const g = gate();
    const a = bulkhead.execute(() => g.promise);
    const b = bulkhead.execute(() => g.promise);
    const c = bulkhead.execute(() => 'never').catch((e) => e);

    expect(await c).toBeInstanceOf(BulkheadFullError);
    expect(await c).toMatchObject({ reason: 'full', policy: 'reports', code: 'BULKHEAD_FULL' });
    expect(bulkhead.active).toBe(2);
    expect(events).toEqual([
      { type: 'bulkhead-rejected', policy: 'reports', source: undefined, reason: 'full', active: 2, queued: 0 },
    ]);
    g.open();
    await Promise.all([a, b]);
    expect(bulkhead.active).toBe(0);
    expect(await bulkhead.execute(() => 'again')).toBe('again');
  });

  it('queues up to maxQueue calls and runs them in order as slots free up', async () => {
    const bulkhead = new BulkheadPolicy({ maxConcurrent: 1, maxQueue: 2 });
    const order: string[] = [];
    const g = gate();
    const first = bulkhead.execute(async () => {
      await g.promise;
      order.push('first');
    });
    const second = bulkhead.execute(() => void order.push('second'));
    const third = bulkhead.execute(() => void order.push('third'));
    const fourth = bulkhead.execute(() => void order.push('fourth')).catch((e) => e);

    expect(await fourth).toMatchObject({ reason: 'full' });
    expect(bulkhead.queued).toBe(2);
    g.open();
    await Promise.all([first, second, third]);
    expect(order).toEqual(['first', 'second', 'third']);
    expect(bulkhead.active).toBe(0);
  });

  it('rejects a queued call after queueTimeout', async () => {
    const events = recordEvents();
    const bulkhead = new BulkheadPolicy({ maxConcurrent: 1, maxQueue: 1, queueTimeout: '500ms' });
    const g = gate();
    const running = bulkhead.execute(() => g.promise);
    const queued = bulkhead.execute(() => 'late').catch((e) => e);
    await vi.advanceTimersByTimeAsync(499);
    expect(bulkhead.queued).toBe(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(await queued).toMatchObject({ reason: 'queue-timeout' });
    expect(bulkhead.queued).toBe(0);
    expect(events.map((e) => e.type === 'bulkhead-rejected' && e.reason)).toEqual(['queue-timeout']);
    g.open();
    await running;
  });

  it('removes a queued call whose caller aborts', async () => {
    const bulkhead = new BulkheadPolicy({ maxConcurrent: 1, maxQueue: 1 });
    const g = gate();
    const running = bulkhead.execute(() => g.promise);
    const controller = new AbortController();
    const fn = vi.fn();
    const queued = bulkhead.execute(fn, { signal: controller.signal }).catch((e) => e);
    controller.abort('gone');
    expect(await queued).toBe('gone');
    expect(bulkhead.queued).toBe(0);
    g.open();
    await running;
    expect(fn).not.toHaveBeenCalled();
  });

  it('inside a Timeout, queue time counts against the budget', async () => {
    const bulkhead = new BulkheadPolicy({ maxConcurrent: 1, maxQueue: 1 });
    const policy = ResiliencePolicy.wrap(new TimeoutPolicy(100), bulkhead);
    const g = gate();
    const running = bulkhead.execute(() => g.promise);
    const queued = policy.execute(() => 'late').catch((e) => e);
    await vi.advanceTimersByTimeAsync(100);
    expect(await queued).toBeInstanceOf(ResilienceTimeoutError);
    expect(bulkhead.queued).toBe(0);
    g.open();
    await running;
  });

  it('holds the slot until the call really settles, even after an outer timeout gave up', async () => {
    const bulkhead = new BulkheadPolicy({ maxConcurrent: 1 });
    const policy = ResiliencePolicy.wrap(new TimeoutPolicy(100), bulkhead);
    const g = gate();
    const timedOut = policy.execute(() => g.promise).catch((e) => e); // ignores its signal
    await vi.advanceTimersByTimeAsync(100);
    expect(await timedOut).toBeInstanceOf(ResilienceTimeoutError);
    expect(bulkhead.active).toBe(1);
    expect(await bulkhead.execute(() => 'x').catch((e) => e)).toBeInstanceOf(BulkheadFullError);
    g.open();
    await vi.advanceTimersByTimeAsync(0);
    expect(bulkhead.active).toBe(0);
  });

  it('rejects invalid options when it is created, naming them', () => {
    // A setting read from an unset environment variable: NaN used to make every call wait or fail.
    expect(() => new BulkheadPolicy({ maxConcurrent: Number(process.env.RESILIENCE_UNSET) })).toThrow(
      'maxConcurrent: Invalid value NaN. Use a whole number of at least 1, or Infinity.',
    );
    expect(() => new BulkheadPolicy({ maxQueue: -1 })).toThrow(
      'maxQueue: Invalid value -1. Use a whole number of at least 0, or Infinity.',
    );
    expect(() => new BulkheadPolicy({ queueTimeout: '10 s' as never })).toThrow(
      'queueTimeout: Invalid duration "10 s"',
    );
    expect(new BulkheadPolicy({ maxQueue: Infinity }).maxQueue).toBe(Infinity);
  });
});

describe('OutboundRateLimitPolicy', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('allows a burst of `limit`, rejects with retryAfterMs, and refills over time', async () => {
    const events = recordEvents();
    const limiter = new OutboundRateLimitPolicy({ name: 'github', limit: 2, interval: '1s' });
    expect(await limiter.execute(() => 1)).toBe(1);
    expect(await limiter.execute(() => 2)).toBe(2);
    const error = await limiter.execute(() => 3).catch((e) => e);
    expect(error).toBeInstanceOf(OutboundRateLimitError);
    expect(error).toMatchObject({ policy: 'github', retryAfterMs: 500, code: 'RATE_LIMITED' });
    expect(events).toEqual([{ type: 'rate-limited', policy: 'github', source: undefined, retryAfterMs: 500 }]);
    vi.advanceTimersByTime(500);
    expect(await limiter.execute(() => 4)).toBe(4);
    expect(limiter.available).toBe(0);
  });

  it('waits up to maxWait for a token, reserving tokens in order', async () => {
    const limiter = new OutboundRateLimitPolicy({ limit: 1, interval: 100, maxWait: '250ms' });
    const started: number[] = [];
    const calls = [1, 2, 3, 4].map((n) =>
      limiter.execute(() => started.push(n)).catch((e) => e),
    );
    await vi.advanceTimersByTimeAsync(0);
    expect(started).toEqual([1]);
    expect(await calls[3]).toBeInstanceOf(OutboundRateLimitError); // would need to wait 300 ms
    await vi.advanceTimersByTimeAsync(100);
    expect(started).toEqual([1, 2]);
    await vi.advanceTimersByTimeAsync(100);
    expect(started).toEqual([1, 2, 3]);
  });

  it('gives a reserved token back when its caller stops waiting', async () => {
    const limiter = new OutboundRateLimitPolicy({ limit: 1, interval: 100, maxWait: '1s' });
    const started: string[] = [];
    await limiter.execute(() => started.push('a')); // the only token
    const gone = new AbortController();
    const b = limiter.execute(() => started.push('b'), { signal: gone.signal }).catch((e) => e); // due at 100 ms
    const c = limiter.execute(() => started.push('c')); // due at 200 ms
    gone.abort('gone');
    expect(await b).toBe('gone');
    const d = limiter.execute(() => started.push('d')); // b's token is back: due at 200 ms, not 300
    await vi.advanceTimersByTimeAsync(199);
    expect(started).toEqual(['a']);
    await vi.advanceTimersByTimeAsync(1);
    await Promise.all([c, d]);
    expect(started).toEqual(['a', 'c', 'd']); // 3 calls in 200 ms: the initial token plus 2 refilled
  });

  it('rejects invalid options when it is created, naming them', () => {
    expect(
      () => new OutboundRateLimitPolicy({ limit: Number(process.env.RESILIENCE_UNSET), interval: '1s' }),
    ).toThrow('limit: Invalid value NaN. Use a whole number of at least 1.');
    expect(() => new OutboundRateLimitPolicy({ limit: 10, interval: 0 })).toThrow(
      'interval: Invalid duration 0. Use a duration longer than 0.',
    );
  });
});

describe('BulkheadPolicy: slots and queue', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('frees the slot when the call throws synchronously or rejects', async () => {
    const bulkhead = new BulkheadPolicy({ maxConcurrent: 1 });
    await expect(
      bulkhead.execute(() => {
        throw new Error('sync');
      }),
    ).rejects.toThrow('sync');
    await expect(bulkhead.execute(() => Promise.reject(new Error('async')))).rejects.toThrow('async');
    expect(bulkhead.active).toBe(0);
    expect(await bulkhead.execute(() => 'free')).toBe('free');
  });

  it('hands a freed slot to the next queued call and cancels its queue timer', async () => {
    const bulkhead = new BulkheadPolicy({ maxConcurrent: 1, maxQueue: 1, queueTimeout: '1s' });
    const g = gate();
    const running = bulkhead.execute(() => g.promise);
    const queued = bulkhead.execute(() => 'admitted');
    expect(vi.getTimerCount()).toBe(1);
    g.open();
    await running;
    expect(await queued).toBe('admitted');
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(2_000);
    expect(bulkhead.active).toBe(0);
    expect(bulkhead.queued).toBe(0);
  });

  it('keeps the order of the others when a caller in the middle of the queue gives up', async () => {
    const bulkhead = new BulkheadPolicy({ maxConcurrent: 1, maxQueue: 3 });
    const order: string[] = [];
    const g = gate();
    const running = bulkhead.execute(() => g.promise);
    const leaving = new AbortController();
    const a = bulkhead.execute(() => void order.push('a'));
    const b = bulkhead.execute(() => void order.push('b'), { signal: leaving.signal }).catch((e: unknown) => e);
    const c = bulkhead.execute(() => void order.push('c'));
    leaving.abort('gone');
    expect(await b).toBe('gone');
    expect(bulkhead.queued).toBe(2);
    g.open();
    await Promise.all([running, a, c]);
    expect(order).toEqual(['a', 'c']);
  });

  it('reports the queue length in its rejection event when the queue is full', async () => {
    const events = recordEvents();
    const bulkhead = new BulkheadPolicy({ name: 'exports', maxConcurrent: 1, maxQueue: 2 });
    const g = gate();
    const calls = [1, 2, 3].map(() => bulkhead.execute(() => g.promise));
    const rejected = await bulkhead.execute(() => 'x', { source: 'Exports.run' }).catch((e: unknown) => e);
    expect(rejected).toMatchObject({ reason: 'full', message: 'Bulkhead "exports" is full' });
    expect(events).toEqual([
      { type: 'bulkhead-rejected', policy: 'exports', source: 'Exports.run', reason: 'full', active: 1, queued: 2 },
    ]);
    g.open();
    await Promise.all(calls);
  });

  it('has no limit with maxConcurrent: Infinity', async () => {
    const bulkhead = new BulkheadPolicy({ maxConcurrent: Infinity });
    const g = gate();
    const calls = Array.from({ length: 500 }, () => bulkhead.execute(() => g.promise));
    expect(bulkhead.active).toBe(500);
    g.open();
    await Promise.all(calls);
    expect(bulkhead.active).toBe(0);
  });

  it('inside Retry, takes a fresh slot per attempt and holds none during the backoff', async () => {
    const bulkhead = new BulkheadPolicy({ maxConcurrent: 1 });
    const active: number[] = [];
    const policy = ResiliencePolicy.wrap(new RetryPolicy({ attempts: 2, backoff: { delay: 100, factor: 1 } }), bulkhead);
    const result = policy.execute(({ attempt }) => {
      active.push(bulkhead.active);
      return attempt === 1 ? Promise.reject(new Error('flaky')) : 'ok';
    });
    await vi.advanceTimersByTimeAsync(50);
    expect(bulkhead.active).toBe(0);
    expect(await bulkhead.execute(() => 'other caller')).toBe('other caller');
    await vi.advanceTimersByTimeAsync(50);
    expect(await result).toBe('ok');
    expect(active).toEqual([1, 1]);
  });

  it('describes a queue timeout in its error message', async () => {
    const bulkhead = new BulkheadPolicy({ name: 'reports', maxConcurrent: 1, maxQueue: 1, queueTimeout: 10 });
    const g = gate();
    const running = bulkhead.execute(() => g.promise);
    const queued = bulkhead.execute(() => 'late').catch((e: unknown) => e);
    await vi.advanceTimersByTimeAsync(10);
    expect(await queued).toMatchObject({ message: 'Timed out waiting for a slot in bulkhead "reports"' });
    g.open();
    await running;
  });
});

describe('OutboundRateLimitPolicy: refill', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('refills continuously at limit / interval and never holds more than limit tokens', async () => {
    const limiter = new OutboundRateLimitPolicy({ limit: 10, interval: '1s' });
    for (let i = 0; i < 10; i++) {
      await limiter.execute(() => i);
    }
    expect(limiter.available).toBe(0);
    vi.advanceTimersByTime(250);
    expect(limiter.available).toBe(2);
    vi.advanceTimersByTime(50);
    expect(limiter.available).toBe(3);
    vi.advanceTimersByTime(60_000);
    expect(limiter.available).toBe(10);
  });

  it('reports no tokens while callers wait for reserved ones', async () => {
    const limiter = new OutboundRateLimitPolicy({ limit: 1, interval: 100, maxWait: '1s' });
    await limiter.execute(() => 'first');
    const waiting = limiter.execute(() => 'second');
    expect(limiter.available).toBe(0);
    await vi.advanceTimersByTimeAsync(100);
    expect(await waiting).toBe('second');
    expect(limiter.available).toBe(0);
  });

  it('computes retryAfterMs from the queue of reserved tokens and labels the event with the source', async () => {
    const events = recordEvents();
    const limiter = new OutboundRateLimitPolicy({ name: 'maps', limit: 4, interval: '1s', maxWait: 600 });
    for (let i = 0; i < 4; i++) {
      await limiter.execute(() => i);
    }
    const waits = [limiter.execute(() => 'a'), limiter.execute(() => 'b')]; // due at 250 and 500 ms
    const rejected = await limiter.execute(() => 'c', { source: 'Geo.lookup' }).catch((e: unknown) => e);
    expect(rejected).toBeInstanceOf(OutboundRateLimitError);
    expect(rejected).toMatchObject({ retryAfterMs: 750, message: 'Outbound rate limit "maps" exceeded' });
    expect(events).toEqual([{ type: 'rate-limited', policy: 'maps', source: 'Geo.lookup', retryAfterMs: 750 }]);
    await vi.advanceTimersByTimeAsync(500);
    expect(await Promise.all(waits)).toEqual(['a', 'b']);
  });

  it('rejects an invalid maxWait', () => {
    expect(() => new OutboundRateLimitPolicy({ limit: 1, interval: '1s', maxWait: '1 minute' as never })).toThrow(
      'maxWait: Invalid duration "1 minute"',
    );
  });
});
