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

describe('RetryPolicy: backoff bounds and predicate arguments', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('keeps full jitter in [0, wait) and equal jitter in [wait / 2, wait), both capped by maxDelay', () => {
    const almostOne = () => 0.999_999;
    const full = resolveBackoff({ delay: 100, maxDelay: 1_000, jitter: 'full' });
    const equal = resolveBackoff({ delay: 100, maxDelay: 1_000, jitter: 'equal' });
    for (let attempt = 1; attempt <= 8; attempt++) {
      const wait = Math.min(1_000, 100 * 2 ** (attempt - 1));
      for (const random of [() => 0, () => 0.3, almostOne]) {
        const fullWait = computeBackoff(full, attempt, random);
        expect(fullWait).toBeGreaterThanOrEqual(0);
        expect(fullWait).toBeLessThan(wait);
        const equalWait = computeBackoff(equal, attempt, random);
        expect(equalWait).toBeGreaterThanOrEqual(wait / 2);
        expect(equalWait).toBeLessThan(wait);
      }
    }
    expect(computeBackoff(full, 30, almostOne)).toBe(999);
  });

  it('takes Duration strings for delay and maxDelay', () => {
    expect(resolveBackoff({ delay: '1.5s', maxDelay: '1m' })).toEqual({
      delay: 1_500,
      factor: 2,
      maxDelay: 60_000,
      jitter: 'full',
    });
  });

  it('calls retryIf with the error and the 1-based attempt that failed, never after the last attempt', async () => {
    const retryIf = vi.fn((_error: unknown, _attempt: number) => true);
    const result = new RetryPolicy({ attempts: 3, backoff: { delay: 0 }, retryIf })
      .execute(({ attempt }) => Promise.reject(new Error(`attempt ${attempt}`)))
      .catch((e: Error) => e.message);
    await vi.runAllTimersAsync();
    expect(await result).toBe('attempt 3');
    expect(retryIf.mock.calls.map(([error, attempt]) => [(error as Error).message, attempt])).toEqual([
      ['attempt 1', 1],
      ['attempt 2', 2],
    ]);
  });

  it('calls a backoff function with the attempt that failed and its error, only between attempts', async () => {
    const backoff = vi.fn((_attempt: number, _error: unknown) => 10);
    const result = new RetryPolicy({ attempts: 3, backoff })
      .execute(({ attempt }) => Promise.reject(new Error(`attempt ${attempt}`)))
      .catch(() => undefined);
    await vi.advanceTimersByTimeAsync(20);
    await result;
    expect(backoff.mock.calls.map(([attempt, error]) => [attempt, (error as Error).message])).toEqual([
      [1, 'attempt 1'],
      [2, 'attempt 2'],
    ]);
  });

  it('rejects with a TypeError naming backoff when the backoff function returns an invalid duration', async () => {
    let calls = 0;
    const policy = new RetryPolicy({ attempts: 3, backoff: () => 'soon' as never });
    await expect(
      policy.execute(() => {
        calls++;
        throw boom();
      }),
    ).rejects.toThrow('backoff: Invalid duration "soon"');
    expect(calls).toBe(1);
  });

  it('with attempts: 1, calls once, emits no retry event and schedules no timer', async () => {
    const events = recordEvents();
    const retryIf = vi.fn(() => true);
    await expect(new RetryPolicy({ attempts: 1, retryIf }).execute(() => Promise.reject(boom()))).rejects.toThrow(
      'boom',
    );
    expect(retryIf).not.toHaveBeenCalled();
    expect(events).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('with attempts: Infinity, keeps retrying until the signal aborts', async () => {
    const controller = new AbortController();
    let calls = 0;
    const result = new RetryPolicy({ attempts: Infinity, backoff: { delay: 10, factor: 1 } })
      .execute(
        () => {
          calls++;
          throw boom();
        },
        { signal: controller.signal },
      )
      .catch((e: Error) => e.message);
    await vi.advanceTimersByTimeAsync(995);
    expect(calls).toBe(100);
    controller.abort(new Error('enough'));
    expect(await result).toBe('enough');
    expect(vi.getTimerCount()).toBe(0);
  });

  it('calls retryOnResult with the result and the attempt, and returns the first result it accepts', async () => {
    const retryOnResult = vi.fn((result: unknown) => result === 'pending');
    let calls = 0;
    const result = new RetryPolicy({ attempts: 5, backoff: { delay: 0 }, retryOnResult }).execute(() =>
      ++calls < 3 ? 'pending' : 'done',
    );
    await vi.runAllTimersAsync();
    expect(await result).toBe('done');
    expect(calls).toBe(3);
    expect(retryOnResult.mock.calls).toEqual([
      ['pending', 1],
      ['pending', 2],
      ['done', 3],
    ]);
  });

  it('emits a retry event for a retried result, with no error', async () => {
    const events = recordEvents();
    const result = new RetryPolicy({
      attempts: 2,
      backoff: { delay: 5, factor: 1 },
      retryOnResult: () => true,
      name: 'poller',
    }).execute(() => 'pending', { source: 'Jobs.poll' });
    await vi.advanceTimersByTimeAsync(5);
    expect(await result).toBe('pending');
    expect(events).toEqual([
      { type: 'retry', policy: 'poller', source: 'Jobs.poll', attempt: 1, delayMs: 5, error: undefined },
    ]);
  });

  it('does not retry an attempt that failed because the caller aborted, whatever retryIf says', async () => {
    const controller = new AbortController();
    let calls = 0;
    const result = new RetryPolicy({ attempts: 5, backoff: { delay: 0 }, retryIf: () => true })
      .execute(
        ({ signal }) => {
          calls++;
          return new Promise((_, reject) => signal.addEventListener('abort', () => reject(new Error('aborted'))));
        },
        { signal: controller.signal },
      )
      .catch((e: Error) => e.message);
    await vi.advanceTimersByTimeAsync(0);
    controller.abort();
    expect(await result).toBe('aborted');
    await vi.advanceTimersByTimeAsync(1_000);
    expect(calls).toBe(1);
  });

  it('passes the same execution signal to every attempt when nothing inside it is per attempt', async () => {
    const signals: AbortSignal[] = [];
    const result = new RetryPolicy({ attempts: 3, backoff: { delay: 0 } }).execute(({ signal }) => {
      signals.push(signal);
      return signals.length < 3 ? Promise.reject(boom()) : 'ok';
    });
    await vi.runAllTimersAsync();
    expect(await result).toBe('ok');
    expect(signals).toHaveLength(3);
    expect(new Set(signals).size).toBe(1);
  });
});
