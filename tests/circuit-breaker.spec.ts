import { BadRequestException } from '@nestjs/common';
import { CircuitBreakerRegistry } from '../lib/services/circuit-breaker-registry.service.js';
import {
  BulkheadFullError,
  CircuitBreakerPolicy,
  CircuitOpenError,
  OutboundRateLimitError,
} from '../lib/index.js';
import { recordEvents } from './events.js';

const ok = () => Promise.resolve('ok');
const fail = () => Promise.reject(new Error('down'));

async function run(breaker: CircuitBreakerPolicy, fn: () => Promise<unknown>) {
  return breaker.execute(fn).then(
    () => 'ok',
    (e) => (e instanceof CircuitOpenError ? 'rejected' : 'failed'),
  );
}

function deferred() {
  let resolve!: () => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<void>((res, rej) => ((resolve = res), (reject = rej)));
  return { promise, resolve, reject };
}

describe('CircuitBreakerPolicy', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('stays closed until minimumCalls, then opens at the failure-rate threshold', async () => {
    const breaker = new CircuitBreakerPolicy({ failureRateThreshold: 50, minimumCalls: 4 });
    await run(breaker, fail);
    await run(breaker, fail);
    await run(breaker, fail);
    expect(breaker.state).toBe('closed'); // 3 of 3 failed, but below minimumCalls
    await run(breaker, ok);
    expect(breaker.state).toBe('open'); // 3 of 4 = 75% ≥ 50%
  });

  it('stays closed below the threshold', async () => {
    const breaker = new CircuitBreakerPolicy({ failureRateThreshold: 50, minimumCalls: 4 });
    for (const fn of [fail, ok, ok, ok, fail, ok]) {
      await run(breaker, fn);
    }
    expect(breaker.state).toBe('closed');
    expect(breaker.stats).toMatchObject({ total: 6, failures: 2 });
  });

  it('count window only remembers the last `size` calls', async () => {
    const breaker = new CircuitBreakerPolicy({
      failureRateThreshold: 50,
      minimumCalls: 4,
      slidingWindow: { type: 'count', size: 4 },
    });
    for (const fn of [fail, ok, ok, ok, ok, fail]) {
      await run(breaker, fn);
    }
    // Window is now [ok, ok, ok, fail]: 25%.
    expect(breaker.stats).toMatchObject({ total: 4, failures: 1 });
    await run(breaker, fail); // [ok, ok, fail, fail]: 50%
    expect(breaker.state).toBe('open');
  });

  it('time window forgets calls older than `size`', async () => {
    const breaker = new CircuitBreakerPolicy({
      failureRateThreshold: 50,
      minimumCalls: 3,
      slidingWindow: { type: 'time', size: '10s' },
    });
    await run(breaker, fail);
    await run(breaker, fail);
    vi.advanceTimersByTime(10_000);
    expect(breaker.stats.total).toBe(0);
    await run(breaker, fail);
    await run(breaker, ok);
    await run(breaker, ok);
    expect(breaker.state).toBe('closed'); // 1 of 3, the two old failures aged out
    await run(breaker, fail);
    expect(breaker.state).toBe('open'); // 2 of 4 = 50%, the threshold is inclusive
  });

  it('time window opens on failures within the window', async () => {
    const breaker = new CircuitBreakerPolicy({
      failureRateThreshold: 60,
      minimumCalls: 3,
      slidingWindow: { type: 'time', size: 1_000 },
    });
    await run(breaker, fail);
    vi.advanceTimersByTime(500);
    await run(breaker, fail);
    vi.advanceTimersByTime(400);
    await run(breaker, ok);
    expect(breaker.state).toBe('open'); // 2 of 3 within 1s = 66%
  });

  it('rejects while open with CircuitOpenError and the remaining open time', async () => {
    const events = recordEvents();
    const breaker = new CircuitBreakerPolicy({ name: 'inventory', minimumCalls: 1, openDuration: '5s' });
    await run(breaker, fail);
    vi.advanceTimersByTime(2_000);
    const fn = vi.fn(ok);
    const error = await breaker.execute(fn).catch((e) => e);
    expect(fn).not.toHaveBeenCalled();
    expect(error).toBeInstanceOf(CircuitOpenError);
    expect(error).toMatchObject({ policy: 'inventory', retryAfterMs: 3_000, code: 'CIRCUIT_OPEN' });
    expect(events).toEqual([
      { type: 'circuit-open', policy: 'inventory', from: 'closed', to: 'open' },
      { type: 'circuit-rejected', policy: 'inventory', source: undefined, retryAfterMs: 3_000 },
    ]);
  });

  it('half-open after openDuration: a successful probe closes it', async () => {
    const events = recordEvents();
    const breaker = new CircuitBreakerPolicy({ minimumCalls: 1, openDuration: 1_000 });
    await run(breaker, fail);
    vi.advanceTimersByTime(999);
    expect(breaker.state).toBe('open');
    vi.advanceTimersByTime(1);
    expect(breaker.state).toBe('half-open');
    expect(await run(breaker, ok)).toBe('ok');
    expect(breaker.state).toBe('closed');
    expect(breaker.stats.total).toBe(0); // fresh window after closing
    // One channel per state: nestjs:resilience:circuit-open, -half-open, -closed.
    expect(events.map((e) => e.type)).toEqual(['circuit-open', 'circuit-half-open', 'circuit-closed']);
    expect(events.map((e) => 'from' in e && `${e.from}->${e.to}`)).toEqual([
      'closed->open',
      'open->half-open',
      'half-open->closed',
    ]);
  });

  it('half-open: a failed probe opens it again for another openDuration', async () => {
    const breaker = new CircuitBreakerPolicy({ minimumCalls: 1, openDuration: 1_000 });
    await run(breaker, fail);
    vi.advanceTimersByTime(1_000);
    expect(await run(breaker, fail)).toBe('failed');
    expect(breaker.state).toBe('open');
    vi.advanceTimersByTime(999);
    expect(await run(breaker, ok)).toBe('rejected');
    vi.advanceTimersByTime(1);
    expect(await run(breaker, ok)).toBe('ok');
    expect(breaker.state).toBe('closed');
  });

  it('half-open lets only halfOpenMaxCalls probes through and decides when all finished', async () => {
    const breaker = new CircuitBreakerPolicy({
      minimumCalls: 1,
      openDuration: 1_000,
      halfOpenMaxCalls: 2,
      failureRateThreshold: 50,
    });
    await run(breaker, fail);
    vi.advanceTimersByTime(1_000);

    const first = deferred();
    const second = deferred();
    const p1 = run(breaker, () => first.promise);
    const p2 = run(breaker, () => second.promise);
    expect(await run(breaker, ok)).toBe('rejected'); // third concurrent probe
    first.resolve();
    await p1;
    expect(breaker.state).toBe('half-open'); // still waiting for the second probe
    second.reject(new Error('down'));
    await p2;
    expect(breaker.state).toBe('open'); // 1 of 2 failed = 50%
  });

  it('ignores errors rejected by recordIf, and 4xx HttpExceptions by default', async () => {
    const breaker = new CircuitBreakerPolicy({
      minimumCalls: 2,
      recordIf: (e) => !(e as Error).message.startsWith('ignore'),
    });
    await breaker.execute(() => Promise.reject(new Error('ignore me'))).catch(() => undefined);
    expect(breaker.stats.total).toBe(0);
    // A custom recordIf replaces the default entirely.
    await breaker.execute(() => Promise.reject(new BadRequestException())).catch(() => undefined);
    expect(breaker.stats.total).toBe(1);

    const defaults = new CircuitBreakerPolicy({ minimumCalls: 1 });
    await defaults.execute(() => Promise.reject(new BadRequestException())).catch(() => undefined);
    expect(defaults.state).toBe('closed');
  });

  it('never records rejections from inner policies, even when recordIf accepts every error', async () => {
    const breaker = new CircuitBreakerPolicy({ minimumCalls: 1, recordIf: () => true });
    await breaker.execute(() => Promise.reject(new BulkheadFullError('exports', 'full'))).catch(() => undefined);
    await breaker.execute(() => Promise.reject(new OutboundRateLimitError('partner', 100))).catch(() => undefined);
    expect(breaker.stats.total).toBe(0);
    expect(breaker.state).toBe('closed');
  });

  it("does not record a dependency's 4xx answer by default, but records 429 and 5xx", async () => {
    const withStatus = (status: number) => Object.assign(new Error(`answered ${status}`), { status });
    const breaker = new CircuitBreakerPolicy({ minimumCalls: 1 });
    await breaker.execute(() => Promise.reject(withStatus(422))).catch(() => undefined);
    await breaker.execute(() => Promise.reject(withStatus(404))).catch(() => undefined);
    expect(breaker.stats.total).toBe(0);
    expect(breaker.state).toBe('closed');

    await breaker.execute(() => Promise.reject(withStatus(429))).catch(() => undefined);
    expect(breaker.state).toBe('open');
    const other = new CircuitBreakerPolicy({ minimumCalls: 1 });
    await other.execute(() => Promise.reject(withStatus(502))).catch(() => undefined);
    expect(other.state).toBe('open');
  });

  it('drops outcomes of calls admitted before a state change', async () => {
    const breaker = new CircuitBreakerPolicy({ minimumCalls: 1, openDuration: 1_000 });
    const slow = deferred();
    const pending = run(breaker, () => slow.promise); // admitted while closed
    await run(breaker, fail); // opens
    vi.advanceTimersByTime(1_000);
    await run(breaker, ok); // probe closes it
    slow.reject(new Error('late failure'));
    await pending;
    expect(breaker.state).toBe('closed');
    expect(breaker.stats.total).toBe(0);
  });

  it('releases a probe whose recordIf throws: the error surfaces, and the breaker opens again', async () => {
    const breaker = new CircuitBreakerPolicy({
      minimumCalls: 1,
      openDuration: 1_000,
      recordIf: (error) => {
        if ((error as Error).message === 'unexpected') {
          throw new TypeError('recordIf bug');
        }
        return true;
      },
    });
    await run(breaker, fail);
    vi.advanceTimersByTime(1_000);
    await expect(breaker.execute(() => Promise.reject(new Error('unexpected')))).rejects.toThrow('recordIf bug');
    // The probe counted as a failure instead of staying in flight forever,
    // which would have kept the breaker half-open and rejecting every call.
    expect(breaker.state).toBe('open');
    vi.advanceTimersByTime(1_000);
    expect(await run(breaker, ok)).toBe('ok');
    expect(breaker.state).toBe('closed');
  });

  it('time window: a call drops out once its bucket (a tenth of the window) is `size` old', async () => {
    vi.setSystemTime(0);
    const breaker = new CircuitBreakerPolicy({ minimumCalls: 100, slidingWindow: { type: 'time', size: 1_000 } });
    await run(breaker, fail); // t = 0: bucket [0, 100)
    vi.advanceTimersByTime(950);
    await run(breaker, fail); // t = 950: bucket [900, 1000)
    vi.advanceTimersByTime(49); // t = 999
    expect(breaker.stats.total).toBe(2);
    vi.advanceTimersByTime(1); // t = 1000: the first bucket is 1 s old
    expect(breaker.stats).toMatchObject({ total: 1, failures: 1 });
    vi.advanceTimersByTime(899); // t = 1899
    expect(breaker.stats.total).toBe(1);
    vi.advanceTimersByTime(1); // t = 1900: so is the second
    expect(breaker.stats.total).toBe(0);
  });

  it('rejects invalid options when it is created, naming them', () => {
    expect(() => new CircuitBreakerPolicy({ failureRateThreshold: 0 })).toThrow(
      'failureRateThreshold: Invalid value 0. Use a number above 0 and at most 100.',
    );
    // A setting read from an unset environment variable.
    expect(() => new CircuitBreakerPolicy({ minimumCalls: Number(process.env.RESILIENCE_UNSET) })).toThrow(
      'minimumCalls: Invalid value NaN. Use a whole number of at least 1.',
    );
    expect(() => new CircuitBreakerPolicy({ halfOpenMaxCalls: 0 })).toThrow('halfOpenMaxCalls: Invalid value 0.');
    expect(() => new CircuitBreakerPolicy({ slidingWindow: { type: 'time', size: '0s' } })).toThrow(
      'slidingWindow.size: Invalid duration "0s". Use a duration longer than 0.',
    );
    expect(() => new CircuitBreakerPolicy({ slidingWindow: { type: 'count', size: 2.5 } })).toThrow(
      'slidingWindow.size: Invalid value 2.5.',
    );
    // A count window never fills beyond its size: evaluating at `size` calls would open earlier than asked.
    expect(() => new CircuitBreakerPolicy({ minimumCalls: 50 })).toThrow(
      'minimumCalls: Invalid value 50. A count window of 20 calls never holds that many: ' +
        'use at most 20, or a larger slidingWindow.size.',
    );
    expect(() => new CircuitBreakerPolicy({ minimumCalls: 50, slidingWindow: { type: 'count', size: 50 } })).not.toThrow();
    expect(() => new CircuitBreakerPolicy({ minimumCalls: 50, slidingWindow: { type: 'time', size: '1m' } })).not.toThrow();
  });

  it('can be tripped and reset by hand', async () => {
    const breaker = new CircuitBreakerPolicy();
    breaker.trip();
    expect(await run(breaker, ok)).toBe('rejected');
    breaker.reset();
    expect(await run(breaker, ok)).toBe('ok');
  });

  it('resolves defaults and durations into options', () => {
    expect(new CircuitBreakerPolicy({ openDuration: '1m', slidingWindow: { type: 'time', size: '30s' } }).options).toMatchObject({
      failureRateThreshold: 50,
      minimumCalls: 10,
      halfOpenMaxCalls: 1,
      openDuration: 60_000,
      slidingWindow: { type: 'time', size: 30_000 },
    });
    expect(new CircuitBreakerPolicy().options.slidingWindow).toEqual({ type: 'count', size: 20 });
  });
});

describe('CircuitBreakerRegistry', () => {
  it('shares one breaker per name and applies module defaults', () => {
    const registry = new CircuitBreakerRegistry({ defaults: { circuitBreaker: { minimumCalls: 3 } } });
    const a = registry.get('payments', { failureRateThreshold: 25 });
    expect(registry.get('payments')).toBe(a);
    expect(a.options).toMatchObject({ minimumCalls: 3, failureRateThreshold: 25 });
    expect(registry.list()).toEqual([a]);
  });

  it('rejects two different configurations for one name, comparing durations by value', () => {
    const registry = new CircuitBreakerRegistry();
    registry.declare('payments', { name: 'payments', minimumCalls: 5, openDuration: '30s' }, 'A.a');
    registry.declare('payments', { name: 'payments', minimumCalls: 5, openDuration: 30_000 }, 'B.b'); // same
    registry.declare('payments', { name: 'payments' }, 'C.c'); // reference only: fine
    expect(() => registry.declare('payments', { name: 'payments', minimumCalls: 6 }, 'D.d')).toThrow(
      /configured differently by A\.a and D\.d/,
    );
  });
});
