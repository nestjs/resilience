import { Controller, Get, Logger, Module, Res, type Type } from '@nestjs/common';
import { Query, ResolveField, Resolver } from '@nestjs/graphql';
import { Test } from '@nestjs/testing';
import { ResilienceInterceptor } from '../lib/interceptors/resilience.interceptor.js';
import { PolicyWrap } from '../lib/policies/resilience.policy.js';
import { EntrypointPlanner, type EntrypointPlan } from '../lib/services/entrypoint-planner.service.js';
import {
  BulkheadPolicy,
  CircuitBreaker,
  CircuitBreakerPolicy,
  CircuitOpenError,
  Fallback,
  ResilienceModule,
  ResilienceService,
  ResilienceTimeoutError,
  Retry,
  RetryPolicy,
  Timeout,
  type ResilienceModuleOptions,
  type ResiliencePolicy,
} from '../lib/index.js';
import { recordEvents } from './events.js';

async function boot(options: ResilienceModuleOptions, parts: { controllers?: Type[]; providers?: Type[] } = {}) {
  @Module({ imports: [ResilienceModule.forRoot(options)], ...parts })
  class AppModule {}
  const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
  moduleRef.useLogger(false);
  await moduleRef.init();
  return moduleRef;
}

const stagesOf = (policy: ResiliencePolicy) => (policy as PolicyWrap).policies;
const stageNames = (policy: ResiliencePolicy) => stagesOf(policy).map((p) => p.constructor.name);
const fail = () => Promise.reject(new Error('down'));

describe('ResilienceService', () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('hands out one policy per preset, and fails loudly for an unknown one', async () => {
    const resilience = (await boot({ presets: { partner: { timeout: '1s' }, maps: { retry: 2 } } })).get(
      ResilienceService,
    );
    expect(resilience.preset('partner')).toBe(resilience.preset('partner'));
    expect(() => resilience.preset('parnter')).toThrow(
      'Unknown resilience preset "parnter". Known presets: partner, maps.',
    );
  });

  it('composes every stage of a preset in canonical order, the outbound rate limit between breaker and timeout', async () => {
    const resilience = (
      await boot({
        presets: {
          partner: {
            bulkhead: { maxConcurrent: 5 },
            timeout: '1s',
            outboundRateLimit: { limit: 10, interval: '1s' },
            circuitBreaker: {},
            retry: 2,
            fallback: () => null,
          },
        },
      })
    ).get(ResilienceService);
    expect(stageNames(resilience.preset('partner'))).toEqual([
      'FallbackPolicy',
      'RetryPolicy',
      'CircuitBreakerPolicy',
      'OutboundRateLimitPolicy',
      'TimeoutPolicy',
      'BulkheadPolicy',
    ]);
  });

  it("doesn't count the wait for an outbound token against the timeout", async () => {
    vi.useFakeTimers();
    const resilience = (
      await boot({
        presets: { partner: { outboundRateLimit: { limit: 1, interval: 100, maxWait: '1s' }, timeout: 50 } },
      })
    ).get(ResilienceService);
    const partner = resilience.preset('partner');
    expect(await partner.execute(() => 'first')).toBe('first');
    const second = partner.execute(() => 'second');
    await vi.advanceTimersByTimeAsync(100);
    expect(await second).toBe('second'); // waited 100 ms for its token, then ran well within 50 ms
  });

  it('shares a preset breaker with create() options that refer to it by name', async () => {
    const resilience = (await boot({ presets: { partner: { circuitBreaker: { minimumCalls: 1 } } } })).get(
      ResilienceService,
    );
    const nightly = resilience.create({ circuitBreaker: 'partner', timeout: '1s' }, 'nightly');
    await nightly.execute(fail).catch(() => undefined);
    await expect(resilience.preset('partner').execute(() => 'x')).rejects.toBeInstanceOf(CircuitOpenError);
    expect(stagesOf(nightly)[0]).toBe(resilience.circuitBreaker('partner'));
  });

  it("lets one preset use another preset's breaker by name", async () => {
    const resilience = (
      await boot({ presets: { payments: { circuitBreaker: { minimumCalls: 1 } }, refunds: { circuitBreaker: 'payments' } } })
    ).get(ResilienceService);
    await resilience.preset('refunds').execute(fail).catch(() => undefined);
    expect(resilience.circuitBreaker('payments').state).toBe('open');
    expect(resilience.circuitBreakers()).toHaveLength(1);
  });

  it('gives each create() call its own unnamed breaker, with defaults.circuitBreaker applied', async () => {
    const resilience = (await boot({ defaults: { circuitBreaker: { minimumCalls: 1 } } })).get(ResilienceService);
    const a = resilience.create({ circuitBreaker: {} }, 'a');
    const b = resilience.create({ circuitBreaker: {} }, 'b');
    await a.execute(fail).catch(() => undefined);
    await expect(a.execute(() => 'x')).rejects.toBeInstanceOf(CircuitOpenError);
    expect(await b.execute(() => 'x')).toBe('x');
    // Unnamed breakers are private to their policy: not in the registry.
    expect(resilience.circuitBreakers()).toEqual([]);
  });

  it('applies defaults.bulkhead to a bulkhead without settings', async () => {
    const resilience = (await boot({ defaults: { bulkhead: { maxConcurrent: 2, maxQueue: 1 } } })).get(
      ResilienceService,
    );
    const [bulkhead] = stagesOf(resilience.create({ bulkhead: {} }));
    expect(bulkhead).toBeInstanceOf(BulkheadPolicy);
    expect(bulkhead).toMatchObject({ maxConcurrent: 2, maxQueue: 1 });
  });

  it('finds a preset bulkhead nothing used yet, and lists the bulkheads created', async () => {
    const resilience = (await boot({ presets: { exports: { bulkhead: { maxConcurrent: 3 } } } })).get(
      ResilienceService,
    );
    expect(resilience.bulkheads()).toEqual([]);
    const bulkhead = resilience.bulkhead('exports');
    expect(bulkhead).toMatchObject({ name: 'exports', maxConcurrent: 3, active: 0 });
    expect(resilience.bulkheads()).toEqual([bulkhead]);
    expect(stagesOf(resilience.preset('exports'))).toEqual([bulkhead]);
  });

  it("keeps retryOnResult for create(), unlike at entrypoints", async () => {
    vi.useFakeTimers();
    const resilience = (await boot({})).get(ResilienceService);
    let calls = 0;
    const policy = resilience.create({
      retry: { attempts: 3, backoff: { delay: 0 }, retryOnResult: (r) => r === 'pending' },
    });
    const result = policy.execute(() => (++calls < 2 ? 'pending' : 'done'));
    await vi.runAllTimersAsync();
    expect(await result).toBe('done');
    expect(calls).toBe(2);
  });

  it('merges retry options over defaults.retry, backoff field by field', async () => {
    vi.useFakeTimers();
    const events = recordEvents();
    const resilience = (
      await boot({ defaults: { retry: { attempts: 3, backoff: { delay: 40, jitter: 'none' } } } })
    ).get(ResilienceService);
    const policy = resilience.create({ retry: { backoff: { factor: 3 } } }, 'merged');
    const result = policy.execute(fail).catch(() => undefined);
    await vi.runAllTimersAsync();
    await result;
    expect(events.map((e) => e.type === 'retry' && e.delayMs)).toEqual([40, 120]);
  });

  it('turns off the retry stage for retry: false or a single attempt, whatever defaults.retry says', async () => {
    const resilience = (await boot({ defaults: { retry: 5 } })).get(ResilienceService);
    expect(stageNames(resilience.create({ retry: false, timeout: '1s' }))).toEqual(['TimeoutPolicy']);
    expect(stageNames(resilience.create({ retry: 1, timeout: '1s' }))).toEqual(['TimeoutPolicy']);
    expect((stagesOf(resilience.create({ retry: {} }))[0] as RetryPolicy).attempts).toBe(5);
  });

  it('names the invalid option when create() gets one', async () => {
    const resilience = (await boot({})).get(ResilienceService);
    expect(() => resilience.create({ retry: { attempts: 0 } })).toThrow(
      'retry.attempts: Invalid value 0. Use a whole number of at least 1, or Infinity.',
    );
    expect(() => resilience.create({ bulkhead: { maxConcurrent: 0 } })).toThrow('bulkhead.maxConcurrent: Invalid value 0.');
    expect(() => resilience.create({ outboundRateLimit: 'nobody' })).toThrow(
      'Outbound rate limit "nobody" has no configuration',
    );
  });

  it('labels the events of create() policies with the given name', async () => {
    vi.useFakeTimers();
    const events = recordEvents();
    const resilience = (await boot({})).get(ResilienceService);
    const result = resilience.create({ timeout: 10 }, 'reports').execute(() => new Promise(() => {})).catch((e) => e);
    await vi.advanceTimersByTimeAsync(10);
    expect(await result).toBeInstanceOf(ResilienceTimeoutError);
    expect(events).toEqual([{ type: 'timeout', policy: 'reports', source: undefined, timeoutMs: 10 }]);
  });
});

describe('ResilienceModule options at startup', () => {
  it('fails on an invalid outbound rate limit in a preset, naming it', async () => {
    await expect(boot({ presets: { partner: { outboundRateLimit: { limit: 0, interval: '1s' } } } })).rejects.toThrow(
      'presets.partner.outboundRateLimit.limit: Invalid value 0. Use a whole number of at least 1.',
    );
    await expect(
      boot({ presets: { partner: { outboundRateLimit: { limit: 5, interval: '1 sec' as never } } } }),
    ).rejects.toThrow('presets.partner.outboundRateLimit.interval: Invalid duration "1 sec"');
  });

  it('fails on invalid defaults even when nothing uses them', async () => {
    await expect(boot({ defaults: { timeout: 0 } })).rejects.toThrow(
      'defaults.timeout: Invalid duration 0. Use a duration longer than 0.',
    );
    await expect(boot({ defaults: { circuitBreaker: { halfOpenMaxCalls: 0 } } })).rejects.toThrow(
      'defaults.circuitBreaker.halfOpenMaxCalls: Invalid value 0.',
    );
    await expect(boot({ defaults: { bulkhead: { queueTimeout: '5 secs' as never } } })).rejects.toThrow(
      'defaults.bulkhead.queueTimeout: Invalid duration "5 secs"',
    );
  });

  it('accepts references by name in presets without validating them there', async () => {
    const moduleRef = await boot({
      presets: { shared: { circuitBreaker: { minimumCalls: 2 } }, user: { circuitBreaker: 'shared', bulkhead: 'pool' } },
    });
    expect(moduleRef.get(ResilienceService).circuitBreaker('shared').options.minimumCalls).toBe(2);
  });

  it('fails when two presets configure one named breaker differently', async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [
        ResilienceModule.forRoot({
          presets: {
            a: { circuitBreaker: { name: 'db', minimumCalls: 2 } },
            b: { circuitBreaker: { name: 'db', minimumCalls: 3 } },
          },
        }),
      ],
    }).compile();
    moduleRef.useLogger(false);
    await expect(moduleRef.init()).rejects.toThrow(
      'Circuit breaker "db" is configured differently by preset "a" and preset "b"',
    );
  });

  it('shares one named breaker between presets that configure it the same way', async () => {
    const resilience = (
      await boot({
        presets: {
          reads: { circuitBreaker: { name: 'db', minimumCalls: 1 }, timeout: '1s' },
          writes: { circuitBreaker: { name: 'db', minimumCalls: 1 } },
        },
      })
    ).get(ResilienceService);
    await resilience.preset('writes').execute(fail).catch(() => undefined);
    await expect(resilience.preset('reads').execute(() => 'x')).rejects.toBeInstanceOf(CircuitOpenError);
    expect(resilience.circuitBreakers().map((b) => b.name)).toEqual(['db']);
  });
});

describe('Entrypoint plans', () => {
  beforeEach(() => vi.spyOn(Logger.prototype, 'warn').mockImplementation(() => undefined));
  afterEach(() => vi.restoreAllMocks());

  const canRepeat = (moduleRef: Awaited<ReturnType<typeof boot>>, plan: EntrypointPlan, context: object) =>
    (moduleRef.get(ResilienceInterceptor) as unknown as { canRepeat(plan: unknown, context: unknown): boolean }).canRepeat(
      plan,
      context,
    );

  it('repeats only safe HTTP methods, in any case, unless the handler is idempotent', async () => {
    @Controller()
    class C {
      @Get() @Retry(2) read() {}
      @Get('upsert') @Retry({ attempts: 2, idempotent: true }) upsert() {}
    }
    const moduleRef = await boot({}, { controllers: [C] });
    const planner = moduleRef.get(EntrypointPlanner);
    const http = (method: string) => ({
      getType: () => 'http',
      switchToHttp: () => ({ getRequest: () => ({ method }) }),
    });
    const read = planner.planFor(C, C.prototype.read)!;
    expect(['GET', 'HEAD', 'OPTIONS', 'get'].map((m) => canRepeat(moduleRef, read, http(m)))).toEqual([
      true,
      true,
      true,
      true,
    ]);
    expect(['POST', 'PUT', 'PATCH', 'DELETE'].map((m) => canRepeat(moduleRef, read, http(m)))).toEqual([
      false,
      false,
      false,
      false,
    ]);
    expect(canRepeat(moduleRef, planner.planFor(C, C.prototype.upsert)!, http('DELETE'))).toBe(true);
  });

  it('repeats GraphQL queries but not mutations', async () => {
    @Resolver()
    class R {
      @Query(() => String) @Retry(2) search() {}
    }
    const moduleRef = await boot({}, { providers: [R] });
    const plan = moduleRef.get(EntrypointPlanner).planFor(R, R.prototype.search)!;
    const graphql = (operation: string) => ({
      getType: () => 'graphql',
      getArgByIndex: () => ({ operation: { operation } }),
    });
    expect(canRepeat(moduleRef, plan, graphql('query'))).toBe(true);
    expect(canRepeat(moduleRef, plan, graphql('mutation'))).toBe(false);
  });

  it("gives field resolvers only their own decorators, not the class's", async () => {
    @Resolver()
    @Timeout('1s')
    @CircuitBreaker({ minimumCalls: 1 })
    class ProductResolver {
      @Query(() => String) product() {}
      @ResolveField(() => String) price() {}
      @ResolveField(() => String) @Retry(3) stock() {}
    }
    const moduleRef = await boot({}, { providers: [ProductResolver] });
    const planner = moduleRef.get(EntrypointPlanner);
    expect(stageNames(planner.planFor(ProductResolver, ProductResolver.prototype.product)!.withRetry)).toEqual([
      'CircuitBreakerPolicy',
      'TimeoutPolicy',
    ]);
    expect(planner.planFor(ProductResolver, ProductResolver.prototype.price)).toBeNull();
    expect(stageNames(planner.planFor(ProductResolver, ProductResolver.prototype.stock)!.withRetry)).toEqual([
      'RetryPolicy',
    ]);
  });

  it('keeps @Fallback on a @Res({ passthrough: true }) handler, where Nest still sends the result', async () => {
    const warn = vi.mocked(Logger.prototype.warn);
    @Controller()
    class C {
      @Get() @Fallback(() => 'cached') get(@Res({ passthrough: true }) _response: unknown) {}
    }
    const moduleRef = await boot({}, { controllers: [C] });
    expect(stageNames(moduleRef.get(EntrypointPlanner).planFor(C, C.prototype.get)!.withRetry)).toEqual([
      'FallbackPolicy',
    ]);
    expect(warn).not.toHaveBeenCalled();
  });

  it('keeps the same stages, and the same breaker, in the pipelines with and without retry', async () => {
    @Controller()
    class C {
      @Get() @Retry(3) @CircuitBreaker() @Timeout(100) get() {}
    }
    const moduleRef = await boot({}, { controllers: [C] });
    const plan = moduleRef.get(EntrypointPlanner).planFor(C, C.prototype.get)!;
    expect(stageNames(plan.withRetry)).toEqual(['RetryPolicy', 'CircuitBreakerPolicy', 'TimeoutPolicy']);
    expect(stageNames(plan.withoutRetry)).toEqual(['CircuitBreakerPolicy', 'TimeoutPolicy']);
    expect(stagesOf(plan.withoutRetry)[0]).toBe(stagesOf(plan.withRetry)[1]);
    expect(stagesOf(plan.withRetry)[1]).toBeInstanceOf(CircuitBreakerPolicy);
  });

  it('has no plan for handlers without resilience settings', async () => {
    @Controller()
    class C {
      @Get() plain() {}
    }
    const moduleRef = await boot({ defaults: { timeout: '1s', retry: 3 } }, { controllers: [C] });
    expect(moduleRef.get(EntrypointPlanner).planFor(C, C.prototype.plain)).toBeNull();
  });
});
