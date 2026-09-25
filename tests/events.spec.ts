import { subscribe, unsubscribe } from 'node:diagnostics_channel';
import { Module } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { attachEventSink } from '../lib/events/resilience.channels.js';
import {
  BulkheadPolicy,
  CircuitBreakerPolicy,
  FallbackPolicy,
  OutboundRateLimitPolicy,
  ResilienceEvents,
  ResilienceModule,
  ResilienceService,
  RetryPolicy,
  TimeoutPolicy,
  type ResilienceEvent,
  type ResilienceModuleOptions,
} from '../lib/index.js';
import { recordEvents } from './events.js';

async function boot(options: ResilienceModuleOptions) {
  @Module({ imports: [ResilienceModule.forRoot(options)] })
  class AppModule {}
  const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
  moduleRef.useLogger(false);
  await moduleRef.init();
  return moduleRef;
}

describe('Diagnostics channels', () => {
  afterEach(() => vi.useRealTimers());

  it('publish each event type on its own channel only', async () => {
    vi.useFakeTimers();
    const rejected: unknown[] = [];
    const listener = (message: unknown) => rejected.push(message);
    subscribe('nestjs:resilience:circuit-rejected', listener);
    try {
      const breaker = new CircuitBreakerPolicy({ name: 'search' });
      breaker.trip();
      await breaker.execute(() => 'x', { source: 'Search.find' }).catch(() => undefined);
    } finally {
      unsubscribe('nestjs:resilience:circuit-rejected', listener);
    }
    expect(rejected).toEqual([
      { type: 'circuit-rejected', policy: 'search', source: 'Search.find', retryAfterMs: 30_000 },
    ]);
  });

  it('carry every event type, each with its policy name', async () => {
    vi.useFakeTimers();
    const events = recordEvents();

    const retried = new RetryPolicy({ attempts: 2, backoff: { delay: 0 }, name: 'r' }).execute(({ attempt }) =>
      attempt === 1 ? Promise.reject(new Error('x')) : 'ok',
    );
    await vi.runAllTimersAsync();
    await retried;

    const timedOut = new TimeoutPolicy({ timeout: 10, name: 't' })
      .execute(() => new Promise(() => {}))
      .catch(() => undefined);
    await vi.advanceTimersByTimeAsync(10);
    await timedOut;

    const breaker = new CircuitBreakerPolicy({ name: 'cb', minimumCalls: 1, openDuration: 100 });
    await breaker.execute(() => Promise.reject(new Error('x'))).catch(() => undefined);
    await breaker.execute(() => 'x').catch(() => undefined);
    vi.advanceTimersByTime(100);
    await breaker.execute(() => 'x');

    const bulkhead = new BulkheadPolicy({ name: 'bh', maxConcurrent: 1 });
    let release!: () => void;
    const held = bulkhead.execute(() => new Promise<void>((r) => (release = r)));
    await bulkhead.execute(() => 'x').catch(() => undefined);
    release();
    await held;

    const limiter = new OutboundRateLimitPolicy({ name: 'rl', limit: 1, interval: '1s' });
    await limiter.execute(() => 'x');
    await limiter.execute(() => 'x').catch(() => undefined);

    await new FallbackPolicy(() => 'y', { name: 'fb' }).execute(() => Promise.reject(new Error('x')));

    expect(events.map((e) => `${e.type} ${e.policy}`)).toEqual([
      'retry r',
      'timeout t',
      'circuit-open cb',
      'circuit-rejected cb',
      'circuit-half-open cb',
      'circuit-closed cb',
      'bulkhead-rejected bh',
      'rate-limited rl',
      'fallback fb',
    ]);
  });
});

describe('Event sinks', () => {
  it("don't change the outcome of the call they observe when they throw", async () => {
    const sink = vi.fn(() => {
      throw new Error('listener bug');
    });
    const policy = attachEventSink(new FallbackPolicy(() => 'fallback'), sink);
    expect(await policy.execute(() => Promise.reject(new Error('down')))).toBe('fallback');
    expect(sink).toHaveBeenCalledWith(expect.objectContaining({ type: 'fallback' }));
  });
});

describe('ResilienceEvents', () => {
  it("carries the events of a preset's breaker and rate limit, named after the preset", async () => {
    const moduleRef = await boot({
      presets: {
        partner: {
          circuitBreaker: { minimumCalls: 1 },
          outboundRateLimit: { limit: 1, interval: '1m' },
        },
      },
    });
    const seen: ResilienceEvent[] = [];
    moduleRef.get(ResilienceEvents).events$.subscribe((event) => seen.push(event));
    const partner = moduleRef.get(ResilienceService).preset('partner');

    await partner.execute(() => Promise.reject(new Error('down')), { source: 'Partner.a' }).catch(() => undefined);
    await partner.execute(() => 'x', { source: 'Partner.b' }).catch(() => undefined);
    moduleRef.get(ResilienceService).circuitBreaker('partner').reset();
    await partner.execute(() => 'x', { source: 'Partner.c' }).catch(() => undefined);

    expect(seen.map((e) => [e.type, e.policy, e.source])).toEqual([
      ['circuit-open', 'partner', undefined],
      ['circuit-rejected', 'partner', 'Partner.b'],
      ['circuit-closed', 'partner', undefined],
      ['rate-limited', 'partner', 'Partner.c'],
    ]);
    await moduleRef.close();
  });

  it('keeps delivering to other subscribers after events$ completes for one app', async () => {
    const first = await boot({});
    const second = await boot({});
    const seen: string[] = [];
    second.get(ResilienceEvents).events$.subscribe((e) => seen.push(e.type));
    await first.close();

    const policy = second.get(ResilienceService).create({ fallback: () => 'fallback' }, 'jobs');
    expect(await policy.execute(() => Promise.reject(new Error('down')))).toBe('fallback');
    expect(seen).toEqual(['fallback']);
    await second.close();
  });
});
