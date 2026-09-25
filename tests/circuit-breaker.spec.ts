import { BadRequestException } from '@nestjs/common';
import { BulkheadRegistry } from '../lib/services/bulkhead-registry.service.js';
import { CircuitBreakerRegistry } from '../lib/services/circuit-breaker-registry.service.js';
import { OutboundRateLimitRegistry } from '../lib/services/outbound-rate-limit-registry.service.js';
import {
  BulkheadFullError,
  CircuitBreakerPolicy,
  CircuitOpenError,
  OutboundRateLimitError,
  ResiliencePolicy,
  RetryPolicy,
  TimeoutPolicy,
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

describe('CircuitBreakerPolicy: transitions and probes', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('holds no timer while open, and moves to half-open on the next call once openDuration passed', async () => {
    const events = recordEvents();
    const breaker = new CircuitBreakerPolicy({ minimumCalls: 1, openDuration: 1_000 });
    await run(breaker, fail);
    expect(vi.getTimerCount()).toBe(0);
    vi.advanceTimersByTime(1_000);
    // No `state` read in between: execute() itself notices the elapsed openDuration.
    expect(await run(breaker, ok)).toBe('ok');
    expect(events.map((e) => e.type)).toEqual(['circuit-open', 'circuit-half-open', 'circuit-closed']);
  });

  it('rejects in half-open with retryAfterMs 0 while all probes are in flight', async () => {
    const events = recordEvents();
    const breaker = new CircuitBreakerPolicy({ name: 'ledger', minimumCalls: 1, openDuration: 1_000 });
    await run(breaker, fail);
    vi.advanceTimersByTime(1_000);
    const probe = deferred();
    const pending = run(breaker, () => probe.promise);
    const error = await breaker.execute(ok).catch((e: unknown) => e);
    expect(error).toMatchObject({ retryAfterMs: 0, policy: 'ledger' });
    expect(events.at(-1)).toEqual({ type: 'circuit-rejected', policy: 'ledger', source: undefined, retryAfterMs: 0 });
    probe.resolve();
    await pending;
    expect(breaker.state).toBe('closed');
  });

  it('frees the probe slot of a probe whose error is not recorded, and lets the next call probe', async () => {
    const breaker = new CircuitBreakerPolicy({ minimumCalls: 1, openDuration: 1_000 });
    await run(breaker, fail);
    vi.advanceTimersByTime(1_000);
    await expect(breaker.execute(() => Promise.reject(new BadRequestException()))).rejects.toBeInstanceOf(
      BadRequestException,
    );
    expect(breaker.state).toBe('half-open'); // a client error says nothing about the dependency
    expect(await run(breaker, ok)).toBe('ok');
    expect(breaker.state).toBe('closed');
  });

  it('closes after several probes when their failure rate stays below the threshold', async () => {
    const breaker = new CircuitBreakerPolicy({
      minimumCalls: 1,
      openDuration: 1_000,
      halfOpenMaxCalls: 3,
      failureRateThreshold: 50,
    });
    await run(breaker, fail);
    vi.advanceTimersByTime(1_000);
    await run(breaker, fail);
    expect(breaker.state).toBe('half-open');
    await run(breaker, ok);
    expect(breaker.state).toBe('half-open');
    await run(breaker, ok);
    expect(breaker.state).toBe('closed'); // 1 of 3 = 33 %
  });

  it('does not record a call the caller aborted', async () => {
    const breaker = new CircuitBreakerPolicy({ minimumCalls: 1 });
    const controller = new AbortController();
    const result = breaker
      .execute(
        ({ signal }) => new Promise((_, reject) => signal.addEventListener('abort', () => reject(new Error('aborted')))),
        { signal: controller.signal },
      )
      .catch((e: Error) => e.message);
    controller.abort();
    expect(await result).toBe('aborted');
    expect(breaker.stats.total).toBe(0);
    expect(breaker.state).toBe('closed');
  });

  it('counts successes and failures in stats, with the failure rate in percent', async () => {
    const breaker = new CircuitBreakerPolicy({ minimumCalls: 20 });
    expect(breaker.stats).toEqual({ total: 0, failures: 0, failureRate: 0 });
    for (const fn of [fail, ok, ok, ok]) {
      await run(breaker, fn);
    }
    expect(breaker.stats).toEqual({ total: 4, failures: 1, failureRate: 25 });
  });

  it('with a threshold of 100, opens only once every call in the window failed', async () => {
    const breaker = new CircuitBreakerPolicy({
      minimumCalls: 3,
      failureRateThreshold: 100,
      slidingWindow: { type: 'count', size: 3 },
    });
    for (const fn of [fail, fail, ok, fail, fail]) {
      await run(breaker, fn);
    }
    expect(breaker.state).toBe('closed'); // [ok, fail, fail]
    await run(breaker, fail);
    expect(breaker.state).toBe('open'); // [fail, fail, fail]
  });

  it('trip() and reset() emit the transitions, and trip() while open restarts openDuration silently', async () => {
    const events = recordEvents();
    const breaker = new CircuitBreakerPolicy({ name: 'search', openDuration: 1_000 });
    breaker.trip();
    vi.advanceTimersByTime(600);
    breaker.trip();
    vi.advanceTimersByTime(600);
    expect(breaker.state).toBe('open'); // 600 ms after the second trip, not 1 200 ms after the first
    vi.advanceTimersByTime(400);
    expect(breaker.state).toBe('half-open');
    breaker.reset();
    expect(breaker.state).toBe('closed');
    expect(events).toEqual([
      { type: 'circuit-open', policy: 'search', from: 'closed', to: 'open' },
      { type: 'circuit-half-open', policy: 'search', from: 'open', to: 'half-open' },
      { type: 'circuit-closed', policy: 'search', from: 'half-open', to: 'closed' },
    ]);
  });

  it('reset() empties the window, so earlier failures no longer count', async () => {
    const breaker = new CircuitBreakerPolicy({ minimumCalls: 3 });
    await run(breaker, fail);
    await run(breaker, fail);
    breaker.reset();
    expect(breaker.stats.total).toBe(0);
    await run(breaker, fail);
    await run(breaker, ok);
    await run(breaker, ok);
    expect(breaker.state).toBe('closed'); // 1 of 3, not 3 of 5
  });

  it('ignores the late outcome of a call admitted before trip()', async () => {
    const breaker = new CircuitBreakerPolicy({ minimumCalls: 1, openDuration: 1_000 });
    const slow = deferred();
    const pending = run(breaker, () => slow.promise);
    breaker.trip();
    breaker.reset();
    slow.reject(new Error('late'));
    expect(await pending).toBe('failed');
    expect(breaker.state).toBe('closed');
    expect(breaker.stats.total).toBe(0);
  });

  it('shares its state between every composition that includes the same instance', async () => {
    const breaker = new CircuitBreakerPolicy({ minimumCalls: 2 });
    const reads = ResiliencePolicy.wrap(new TimeoutPolicy('1s'), breaker);
    const writes = ResiliencePolicy.wrap(breaker);
    await reads.execute(fail).catch(() => undefined);
    await writes.execute(fail).catch(() => undefined);
    expect(breaker.state).toBe('open');
    await expect(reads.execute(ok)).rejects.toBeInstanceOf(CircuitOpenError);
    await expect(writes.execute(ok)).rejects.toBeInstanceOf(CircuitOpenError);
  });

  it('inside Retry, records every attempt; outside it, one outcome per execution', async () => {
    const inner = new CircuitBreakerPolicy({ minimumCalls: 20 });
    const retried = ResiliencePolicy.wrap(new RetryPolicy({ attempts: 3, backoff: { delay: 0 } }), inner)
      .execute(fail)
      .catch(() => undefined);
    await vi.runAllTimersAsync();
    await retried;
    expect(inner.stats).toMatchObject({ total: 3, failures: 3 });

    const outer = new CircuitBreakerPolicy({ minimumCalls: 20 });
    const result = ResiliencePolicy.wrap(outer, new RetryPolicy({ attempts: 3, backoff: { delay: 0 } }))
      .execute(fail)
      .catch(() => undefined);
    await vi.runAllTimersAsync();
    await result;
    expect(outer.stats).toMatchObject({ total: 1, failures: 1 });
  });

  it('rejects an unknown sliding window type', () => {
    expect(() => new CircuitBreakerPolicy({ slidingWindow: { type: 'sliding', size: 5 } as never })).toThrow(
      `slidingWindow.type: Invalid type "sliding". Use 'count' or 'time'.`,
    );
    expect(() => new CircuitBreakerPolicy({ failureRateThreshold: 101 })).toThrow(
      'failureRateThreshold: Invalid value 101. Use a number above 0 and at most 100.',
    );
  });
});

describe('Named registries', () => {
  it('refuse a configuration that arrives after the instance was created', () => {
    const registry = new CircuitBreakerRegistry();
    registry.get('payments');
    expect(() => registry.declare('payments', { name: 'payments', minimumCalls: 5 }, 'Late.handler')).toThrow(
      'Circuit breaker "payments" was already created before Late.handler configured it.',
    );
  });

  it('find() only returns names that exist or are configured, and names() lists both', () => {
    const registry = new CircuitBreakerRegistry();
    registry.declare('declared', { name: 'declared', minimumCalls: 2 }, 'A.a');
    registry.get('created');
    expect(registry.find('nobody')).toBeUndefined();
    expect(registry.names()).toEqual(['created', 'declared']);
    expect(registry.list().map((b) => b.name)).toEqual(['created']);
    expect(registry.find('declared')!.options.minimumCalls).toBe(2);
    expect(registry.list().map((b) => b.name)).toEqual(['created', 'declared']);
  });

  it('compare functions by identity: the same recordIf twice is one configuration, another one is not', () => {
    const recordIf = () => true;
    const registry = new CircuitBreakerRegistry();
    registry.declare('payments', { name: 'payments', recordIf }, 'A.a');
    registry.declare('payments', { name: 'payments', recordIf }, 'B.b');
    expect(() => registry.declare('payments', { name: 'payments', recordIf: () => true }, 'C.c')).toThrow(
      'configured differently by A.a and C.c',
    );
  });

  it('compare time windows by value, whatever unit the size is written in', () => {
    const registry = new CircuitBreakerRegistry();
    registry.declare('search', { name: 'search', slidingWindow: { type: 'time', size: '1m' } }, 'A.a');
    registry.declare('search', { name: 'search', slidingWindow: { type: 'time', size: 60_000 } }, 'B.b');
    expect(() =>
      registry.declare('search', { name: 'search', slidingWindow: { type: 'count', size: 60 } }, 'C.c'),
    ).toThrow('configured differently by A.a and C.c');
  });

  it('bulkheads compare queueTimeout by value and apply defaults.bulkhead', () => {
    const registry = new BulkheadRegistry({ defaults: { bulkhead: { maxConcurrent: 3 } } });
    registry.declare('exports', { name: 'exports', maxQueue: 5, queueTimeout: '2s' }, 'A.a');
    registry.declare('exports', { name: 'exports', maxQueue: 5, queueTimeout: 2_000 }, 'B.b');
    expect(() => registry.declare('exports', { name: 'exports', maxQueue: 6 }, 'C.c')).toThrow(
      'Bulkhead "exports" is configured differently by A.a and C.c',
    );
    expect(registry.get('exports')).toMatchObject({ maxConcurrent: 3, maxQueue: 5, queueTimeout: 2_000 });
  });

  it('an outbound rate limit needs a configuration before it can be created', () => {
    const registry = new OutboundRateLimitRegistry();
    expect(() => registry.get('partner')).toThrow(
      'Outbound rate limit "partner" has no configuration (limit and interval are required).',
    );
    registry.declare('github', { name: 'github', limit: 10, interval: '1s' }, 'preset "github"');
    expect(registry.get('github')).toMatchObject({ limit: 10, interval: 1_000, maxWait: 0 });
  });
});
