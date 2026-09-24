import { BadRequestException } from '@nestjs/common';
import { computeBackoff, resolveBackoff } from '../lib/utils/backoff.util.js';
import {
  BulkheadFullError,
  CircuitOpenError,
  OutboundRateLimitError,
  RetryPolicy,
} from '../lib/index.js';
import { recordEvents } from './events.js';

const boom = () => new Error('boom');

describe('RetryPolicy', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('makes up to `attempts` calls with exponential backoff, then throws the last error', async () => {
    const events = recordEvents();
    const policy = new RetryPolicy({ attempts: 4, backoff: { delay: 100, jitter: 'none' }, name: 'catalog' });
    const attempts: number[] = [];
    const result = policy.execute(({ attempt }) => {
      attempts.push(attempt);
      throw boom();
    });
    const settled = result.catch((e) => e);

    await vi.advanceTimersByTimeAsync(0);
    expect(attempts).toEqual([1]);
    await vi.advanceTimersByTimeAsync(99);
    expect(attempts).toEqual([1]);
    await vi.advanceTimersByTimeAsync(1); // 100
    expect(attempts).toEqual([1, 2]);
    await vi.advanceTimersByTimeAsync(200);
    expect(attempts).toEqual([1, 2, 3]);
    await vi.advanceTimersByTimeAsync(400);
    expect(attempts).toEqual([1, 2, 3, 4]);

    expect(await settled).toEqual(boom());
    expect(events.map((e) => e.type === 'retry' && [e.policy, e.attempt, e.delayMs])).toEqual([
      ['catalog', 1, 100],
      ['catalog', 2, 200],
      ['catalog', 3, 400],
    ]);
    expect(events[0]).toEqual({
      type: 'retry',
      policy: 'catalog',
      source: undefined,
      attempt: 1,
      delayMs: 100,
      error: boom(),
    });
  });

  it('returns as soon as an attempt succeeds', async () => {
    const policy = new RetryPolicy({ attempts: 5, backoff: { delay: '50ms', factor: 1 } });
    let calls = 0;
    const result = policy.execute(() => (++calls < 3 ? Promise.reject(boom()) : 'ok'));
    await vi.advanceTimersByTimeAsync(100);
    expect(await result).toBe('ok');
    expect(calls).toBe(3);
  });

  it('takes a number of attempts, or false for a single attempt', async () => {
    let calls = 0;
    const fail = () => {
      calls++;
      throw boom();
    };
    const five = new RetryPolicy(5).execute(fail).catch(() => undefined);
    await vi.advanceTimersByTimeAsync(60_000);
    await five;
    expect(calls).toBe(5);

    calls = 0;
    await new RetryPolicy(false).execute(fail).catch(() => undefined);
    expect(calls).toBe(1);
  });

  it('computes backoff from delay, factor and maxDelay, with full, equal or no jitter', () => {
    const waits = (options: Parameters<typeof resolveBackoff>[0], random = () => 0.5) =>
      [1, 2, 3, 4, 5].map((n) => computeBackoff(resolveBackoff(options), n, random));
    expect(waits({ delay: 100, maxDelay: 500, jitter: 'none' })).toEqual([100, 200, 400, 500, 500]);
    expect(waits({ delay: '1s', factor: 3, jitter: 'none' })).toEqual([1_000, 3_000, 9_000, 27_000, 30_000]);
    // factor 1 is constant, without jitter unless asked for.
    expect(waits({ delay: 300, factor: 1 })).toEqual([300, 300, 300, 300, 300]);
    expect(waits({ delay: 300, factor: 1, jitter: 'full' }, () => 0.25)).toEqual([75, 75, 75, 75, 75]);
    // Full jitter is the default otherwise: random in [0, wait].
    expect(waits({ delay: 100 }).slice(0, 3)).toEqual([50, 100, 200]);
    expect(waits({ delay: 100 }, () => 0).slice(0, 3)).toEqual([0, 0, 0]);
    // Equal jitter: half the wait plus a random half.
    expect(waits({ delay: 100, jitter: 'equal' }, () => 0).slice(0, 3)).toEqual([50, 100, 200]);
    expect(waits({ delay: 100, jitter: 'equal' }, () => 0.5).slice(0, 3)).toEqual([75, 150, 300]);
    // Defaults: 200 ms, doubling, capped at 30 s.
    expect(waits({}, () => 1)).toEqual([200, 400, 800, 1_600, 3_200]);
    expect(computeBackoff(resolveBackoff({}), 20, () => 1)).toBe(30_000);
  });

  it('uses Math.random for jitter by default', async () => {
    vi.spyOn(Math, 'random').mockReturnValue(0.5);
    const events = recordEvents();
    const policy = new RetryPolicy({ attempts: 3, backoff: { delay: '1s' } });
    const result = policy.execute(() => Promise.reject(boom())).catch(() => undefined);
    await vi.advanceTimersByTimeAsync(5_000);
    await result;
    expect(events.map((e) => e.type === 'retry' && e.delayMs)).toEqual([500, 1_000]);
  });

  it('accepts a backoff function returning a Duration (e.g. honoring Retry-After)', async () => {
    const events = recordEvents();
    const policy = new RetryPolicy({
      attempts: 3,
      backoff: (attempt, error) => `${(error as { retryAfter: number }).retryAfter * attempt}s`,
    });
    const result = policy
      .execute(() => Promise.reject(Object.assign(boom(), { retryAfter: 2 })))
      .catch(() => undefined);
    await vi.advanceTimersByTimeAsync(10_000);
    await result;
    expect(events.map((e) => e.type === 'retry' && e.delayMs)).toEqual([2_000, 4_000]);
  });

  it('rejects an invalid duration when the policy is created', () => {
    expect(() => new RetryPolicy({ backoff: { delay: '5 seconds' as never } })).toThrow(
      'backoff.delay: Invalid duration "5 seconds"',
    );
  });

  it('rejects invalid numbers when the policy is created, naming them', () => {
    // A setting read from an unset environment variable: without the check, NaN attempts retried forever.
    expect(() => new RetryPolicy({ attempts: Number(process.env.RESILIENCE_UNSET) })).toThrow(
      'attempts: Invalid value NaN. Use a whole number of at least 1, or Infinity.',
    );
    expect(() => new RetryPolicy(2.5)).toThrow('attempts: Invalid value 2.5.');
    expect(() => new RetryPolicy({ backoff: { factor: 0.5 } })).toThrow(
      'backoff.factor: Invalid value 0.5. Use a number of at least 1.',
    );
    expect(new RetryPolicy(Infinity).attempts).toBe(Infinity);
  });

  it('treats a negative wait from a backoff function as no wait', async () => {
    const events = recordEvents();
    const retryAt = Date.now() - 1_000; // a Retry-After date already in the past
    const policy = new RetryPolicy({ attempts: 2, backoff: () => retryAt - Date.now() });
    let calls = 0;
    const result = policy.execute(() => (++calls === 1 ? Promise.reject(boom()) : 'ok'));
    await vi.advanceTimersByTimeAsync(0);
    expect(await result).toBe('ok');
    expect(events.map((e) => e.type === 'retry' && e.delayMs)).toEqual([0]);
  });

  it('surfaces a retryIf that returns a Promise, which would count as true and could reject unhandled', async () => {
    // TypeScript rejects an async predicate; JavaScript and casts don't.
    const retryIf = (async () => {
      throw new Error('lookup failed');
    }) as unknown as (error: unknown) => boolean;
    let calls = 0;
    const result = new RetryPolicy({ attempts: 3, backoff: { delay: 0 }, retryIf }).execute(() => {
      calls++;
      throw boom();
    });
    await expect(result).rejects.toThrow('retryIf returned a Promise. It must return a boolean, synchronously.');
    expect(calls).toBe(1);
  });

  it('surfaces a retryOnResult that throws instead of retrying the call that succeeded', async () => {
    let calls = 0;
    const policy = new RetryPolicy({
      attempts: 3,
      backoff: { delay: 0 },
      retryOnResult: (result) => (result as { status: number }).status === 503,
    });
    await expect(policy.execute(() => (calls++, null))).rejects.toThrow(TypeError);
    expect(calls).toBe(1);
  });

  it('only retries errors accepted by retryIf', async () => {
    const policy = new RetryPolicy({ attempts: 5, retryIf: (e) => (e as Error).message === 'transient' });
    let calls = 0;
    const result = policy
      .execute(() => {
        calls++;
        throw new Error(calls === 1 ? 'transient' : 'fatal');
      })
      .catch((e) => e);
    await vi.advanceTimersByTimeAsync(10_000);
    expect(((await result) as Error).message).toBe('fatal');
    expect(calls).toBe(2);
  });

  it('does not retry rejections or 4xx HttpExceptions by default', async () => {
    for (const error of [
      new CircuitOpenError('b', 1_000),
      new BulkheadFullError('b', 'full'),
      new BadRequestException(),
    ]) {
      let calls = 0;
      await expect(
        new RetryPolicy({ attempts: 3 }).execute(() => {
          calls++;
          throw error;
        }),
      ).rejects.toBe(error);
      expect(calls).toBe(1);
    }
  });

  it('never retries rejections, even when retryIf accepts every error', async () => {
    for (const error of [
      new CircuitOpenError('b', 1_000),
      new BulkheadFullError('b', 'queue-timeout'),
      new OutboundRateLimitError('b', 500),
    ]) {
      let calls = 0;
      await expect(
        new RetryPolicy({ attempts: 3, retryIf: () => true }).execute(() => {
          calls++;
          throw error;
        }),
      ).rejects.toBe(error);
      expect(calls).toBe(1);
    }
  });

  it("does not retry a dependency's 4xx answer, but retries 408, 429 and 5xx", async () => {
    vi.useRealTimers();
    // How HTTP and SDK clients report a failed response: a plain error with a status.
    class ApiError extends Error {
      constructor(readonly status: number) {
        super(`API answered ${status}`);
      }
    }
    const callsFor = async (error: object) => {
      let calls = 0;
      await new RetryPolicy({ attempts: 3, backoff: { delay: 0 } })
        .execute(() => {
          calls++;
          throw error;
        })
        .catch(() => undefined);
      return calls;
    };
    expect(await callsFor(new ApiError(422))).toBe(1);
    expect(await callsFor(Object.assign(new Error('not found'), { statusCode: 404 }))).toBe(1);
    expect(await callsFor(new ApiError(408))).toBe(3);
    expect(await callsFor(new ApiError(429))).toBe(3);
    expect(await callsFor(new ApiError(503))).toBe(3);
  });

  it('retries on a result when retryOnResult says so, and returns the last result', async () => {
    const policy = new RetryPolicy({
      attempts: 3,
      backoff: { delay: 10, factor: 1 },
      retryOnResult: (r) => (r as { status: number }).status === 503,
    });
    let calls = 0;
    const result = policy.execute(() => ({ status: ++calls < 5 ? 503 : 200 }));
    await vi.advanceTimersByTimeAsync(100);
    expect(await result).toEqual({ status: 503 });
    expect(calls).toBe(3);
  });

  it('stops waiting when the caller aborts during backoff, and never retries a caller abort', async () => {
    const controller = new AbortController();
    const policy = new RetryPolicy({ attempts: 5, backoff: { delay: '1s', factor: 1 } });
    let calls = 0;
    const result = policy
      .execute(
        () => {
          calls++;
          throw boom();
        },
        { signal: controller.signal },
      )
      .catch((e) => e);
    await vi.advanceTimersByTimeAsync(10);
    controller.abort(new Error('user gave up'));
    expect(((await result) as Error).message).toBe('user gave up');
    await vi.advanceTimersByTimeAsync(10_000);
    expect(calls).toBe(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('rejects immediately with an already-aborted signal', async () => {
    const fn = vi.fn();
    await expect(new RetryPolicy().execute(fn, { signal: AbortSignal.abort('nope') })).rejects.toBe('nope');
    expect(fn).not.toHaveBeenCalled();
  });
});
