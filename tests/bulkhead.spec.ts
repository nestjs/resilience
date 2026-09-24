import {
  BulkheadFullError,
  BulkheadPolicy,
  OutboundRateLimitError,
  OutboundRateLimitPolicy,
  ResiliencePolicy,
  ResilienceTimeoutError,
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
