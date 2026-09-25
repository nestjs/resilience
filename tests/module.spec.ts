import {
  Controller,
  Get,
  Injectable,
  Logger,
  Module,
  Post,
  Scope,
  SetMetadata,
  type BeforeApplicationShutdown,
  type Type,
} from '@nestjs/common';
import { Query, Resolver, Subscription } from '@nestjs/graphql';
import { GrpcStreamMethod } from '@nestjs/microservices';
import { Test } from '@nestjs/testing';
import { firstValueFrom, take, toArray } from 'rxjs';
import type { MockInstance } from 'vitest';
import { EntrypointPlanner } from '../lib/services/entrypoint-planner.service.js';
import { PolicyWrap } from '../lib/policies/resilience.policy.js';
import { ResilienceInterceptor } from '../lib/interceptors/resilience.interceptor.js';
import {
  Bulkhead,
  BulkheadPolicy,
  CircuitBreaker,
  CircuitBreakerPolicy,
  CircuitOpenError,
  Fallback,
  Resilience,
  RESILIENCE_MODULE_OPTIONS,
  ResilienceEvents,
  ResilienceModule,
  ResilienceService,
  Retry,
  RetryPolicy,
  Signal,
  Timeout,
  TimeoutPolicy,
  type ResilienceModuleOptions,
  type ResilienceOptionsFactory,
  type ResiliencePolicy,
} from '../lib/index.js';
import { recordEvents } from './events.js';

const noDelay = { delay: 0 };

async function boot(options: ResilienceModuleOptions, parts: { controllers?: Type[]; providers?: Type[] }) {
  @Module({ imports: [ResilienceModule.forRoot(options)], ...parts })
  class AppModule {}
  const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
  moduleRef.useLogger(false);
  await moduleRef.init();
  return moduleRef;
}

function stagesOf(plan: { withRetry: unknown } | null) {
  return (plan!.withRetry as PolicyWrap).policies;
}

function stageNames(plan: { withRetry: unknown } | null) {
  return stagesOf(plan).map((p) => p.constructor.name);
}

const stage = <T>(plan: { withRetry: unknown } | null, type: abstract new (...args: any[]) => T) =>
  stagesOf(plan).find((p) => p instanceof type) as T | undefined;

describe('ResilienceModule', () => {
  let warn: MockInstance<Logger['warn']>;
  beforeEach(() => {
    warn = vi.spyOn(Logger.prototype, 'warn').mockImplementation(() => undefined);
  });
  afterEach(() => vi.restoreAllMocks());

  describe('decorators on providers', () => {
    @Injectable()
    class PaymentsService {
      calls = 0;
      @Retry(3)
      @CircuitBreaker({ minimumCalls: 1 })
      charge(): never {
        this.calls++;
        throw new Error('card network down');
      }
    }

    @Injectable()
    @Timeout(100)
    class ClassLevelService {}

    it('have no effect, and bootstrap warns about each one', async () => {
      const moduleRef = await boot({}, { providers: [PaymentsService, ClassLevelService] });
      const payments = moduleRef.get(PaymentsService);
      expect(() => payments.charge()).toThrow('card network down');
      expect(() => payments.charge()).toThrow('card network down'); // no breaker either
      expect(payments.calls).toBe(2);

      const messages = warn.mock.calls.map(([message]) => String(message));
      expect(messages).toEqual([
        expect.stringContaining('on PaymentsService.charge() have no effect'),
        expect.stringContaining('on ClassLevelService have no effect'),
      ]);
      expect(messages[0]).toContain('use a policy object (ResilienceService.preset() or create()');
    });
  });

  it('warns about decorated non-handler methods of a controller', async () => {
    @Controller()
    class HelperController {
      @Get() list() {
        return [];
      }
      @Retry() helper() {}
    }
    await boot({}, { controllers: [HelperController] });
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('HelperController.helper() have no effect'));
  });

  it('warns about @Signal() on a handler without resilience decorators, where it is always undefined', async () => {
    @Controller()
    class SearchController {
      @Get('plain') plain(@Signal() signal: AbortSignal) {
        return signal;
      }
      @Get('timed') @Timeout('1s') timed(@Signal() signal: AbortSignal) {
        return signal;
      }
    }
    await boot({}, { controllers: [SearchController] });
    expect(warn.mock.calls.map(([m]) => String(m))).toEqual([
      '@Signal() on SearchController.plain() is always undefined: the handler has no resilience decorators. ' +
        'Add @Timeout() or @Resilience() to give it an attempt signal.',
    ]);
  });

  it('warns about resilience decorators on a GraphQL subscription, which they do not apply to', async () => {
    @Resolver()
    class FeedResolver {
      @Subscription(() => String) @Timeout('1s') updates() {}
      @Query(() => String) @Timeout('1s') latest() {}
    }
    await boot({}, { providers: [FeedResolver] });
    expect(warn.mock.calls.map(([m]) => String(m))).toEqual([
      'Resilience decorators on FeedResolver.updates() have no effect: GraphQL subscriptions are not supported.',
    ]);
  });

  it("warns about @nestjs/schedule's @Timeout() on a route, which calls the handler once after startup", async () => {
    // What @nestjs/schedule's @Timeout(5000) sets on the method (@nestjs/schedule 12.0.1).
    const ScheduleTimeout = (timeout: number) => SetMetadata('SCHEDULE_TIMEOUT_OPTIONS', { timeout });
    @Controller('reports')
    class ReportsController {
      @Get() @ScheduleTimeout(5_000) latest() {}
    }
    await boot({}, { controllers: [ReportsController] });
    expect(warn.mock.calls.map(([m]) => String(m))).toEqual([
      "ReportsController.latest() has @nestjs/schedule's @Timeout(5000), which calls it once after startup. " +
        'For a time budget on each call, import Timeout from @nestjs/resilience.',
    ]);
  });

  it('never retries a gRPC handler that reads a request stream, and warns about its @Retry()', async () => {
    @Controller()
    class UploadsController {
      @GrpcStreamMethod('Uploads', 'Upload') @Retry() upload() {}
    }
    const moduleRef = await boot({}, { controllers: [UploadsController] });
    expect(warn.mock.calls.map(([m]) => String(m))).toEqual([
      '@Retry() on UploadsController.upload() is inactive: the handler reads a gRPC request stream, ' +
        "which a second attempt couldn't read again.",
    ]);
    const plan = moduleRef.get(EntrypointPlanner).planFor(UploadsController, UploadsController.prototype.upload)!;
    const interceptor = moduleRef.get(ResilienceInterceptor) as unknown as {
      canRepeat(plan: unknown, context: { getType(): string }): boolean;
    };
    expect(interceptor.canRepeat(plan, { getType: () => 'rpc' })).toBe(false);
  });

  it('warns that @Retry() on an unsafe HTTP method is inactive without idempotent: true', async () => {
    @Controller()
    class OrdersController {
      @Post() @Retry() create() {}
      @Post('safe') @Retry({ idempotent: true }) createSafe() {}
      @Post('off') @Retry(false) createOnce() {}
    }
    await boot({}, { controllers: [OrdersController] });
    const messages = warn.mock.calls.map(([m]) => String(m));
    expect(messages).toEqual([expect.stringContaining('@Retry() on OrdersController.create() is inactive')]);
  });

  describe('fails the boot on misconfiguration', () => {
    it('unknown preset', async () => {
      @Controller()
      class C {
        @Get() @Resilience('nope') get() {}
      }
      await expect(boot({ presets: { known: {} } }, { controllers: [C] })).rejects.toThrow(
        'Unknown resilience preset "nope". Known presets: known.',
      );
    });

    it('missing fallback method', async () => {
      @Controller()
      class C {
        // @ts-expect-error: C has no method "cached"
        @Get() @Fallback('cached') get() {}
      }
      await expect(boot({}, { controllers: [C] })).rejects.toThrow('C has no method "cached"');
    });

    it('method fallback on a request-scoped controller', async () => {
      @Controller({ scope: Scope.REQUEST })
      class C {
        @Get() @Fallback('cached') get() {}
        cached() {}
      }
      await expect(boot({}, { controllers: [C] })).rejects.toThrow('method fallbacks need a singleton C');
    });

    it('one named breaker configured two ways', async () => {
      @Controller()
      class C {
        @Get('a') @CircuitBreaker({ name: 'inventory', minimumCalls: 5 }) a() {}
        @Get('b') @CircuitBreaker({ name: 'inventory', minimumCalls: 50 }) b() {}
        @Get('c') @CircuitBreaker('inventory') c() {}
      }
      await expect(boot({}, { controllers: [C] })).rejects.toThrow(
        'Circuit breaker "inventory" is configured differently by C.a and C.b',
      );
    });

    it("a handler refining a preset's shared breaker", async () => {
      @Controller()
      @Resilience('carrier')
      class C {
        @Get('a') a() {}
        @Get('b') @CircuitBreaker({ minimumCalls: 3 }) b() {}
      }
      await expect(
        boot({ presets: { carrier: { circuitBreaker: { openDuration: '30s' } } } }, { controllers: [C] }),
      ).rejects.toThrow('Circuit breaker "carrier" is configured differently by preset "carrier" and C.b');
    });

    it('@Timeout() without a duration or default', async () => {
      @Controller()
      class C {
        @Get() @Timeout() get() {}
      }
      await expect(boot({}, { controllers: [C] })).rejects.toThrow('@Timeout() on C.get has no duration');
    });

    it('an invalid number, naming the option, or the handler for a decorator', async () => {
      // Settings read from an unset environment variable.
      const unset = Number(process.env.RESILIENCE_UNSET);
      await expect(boot({ presets: { exports: { bulkhead: { maxConcurrent: unset } } } }, {})).rejects.toThrow(
        'presets.exports.bulkhead.maxConcurrent: Invalid value NaN. Use a whole number of at least 1, or Infinity.',
      );
      await expect(boot({ defaults: { retry: unset } }, {})).rejects.toThrow(
        'defaults.retry.attempts: Invalid value NaN.',
      );
      @Controller()
      class C {
        @Get() @CircuitBreaker({ failureRateThreshold: 0 }) get() {}
      }
      await expect(boot({}, { controllers: [C] })).rejects.toThrow(
        'Resilience decorators on C.get: circuitBreaker.failureRateThreshold: Invalid value 0. ' +
          'Use a number above 0 and at most 100.',
      );
    });

    it('ResilienceModule registered twice', async () => {
      // A library module that registers its own copy: every handler would run under two pipelines.
      @Module({ imports: [ResilienceModule.forRoot({ defaults: { timeout: '1s' } })] })
      class PaymentsLibraryModule {}
      @Module({ imports: [ResilienceModule.forRoot(), PaymentsLibraryModule] })
      class AppModule {}
      const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
      await expect(moduleRef.init()).rejects.toThrow(
        'ResilienceModule is registered 2 times (forRoot() or forRootAsync() in more than one module). ' +
          'Each registration adds an app-wide interceptor, so every handler would run under 2 pipelines, ' +
          'with retries multiplied and breakers split. Register it once, in the root module: it is global.',
      );
    });

    it('two handlers with the same name, whose per-handler breakers would be shared', async () => {
      const version = (path: string) => {
        @Controller(path)
        class UsersController {
          @Get() @CircuitBreaker() find() {}
        }
        return UsersController;
      };
      await expect(boot({}, { controllers: [version('v1'), version('v2')] })).rejects.toThrow(
        'Two handlers are named UsersController.find (two classes are called UsersController), so they would ' +
          "share one per-handler circuit breaker. Give it a name: @CircuitBreaker({ name: '…' }).",
      );
    });

    it('an invalid duration, naming the option, even in a preset nothing uses yet', async () => {
      await expect(boot({ presets: { slow: { timeout: '2 seconds' as never } } }, {})).rejects.toThrow(
        'presets.slow.timeout: Invalid duration "2 seconds"',
      );
      await expect(
        boot({ presets: { partner: { circuitBreaker: { openDuration: '1 min' as never } } } }, {}),
      ).rejects.toThrow('presets.partner.circuitBreaker.openDuration: Invalid duration "1 min"');
      await expect(
        boot({ defaults: { retry: { backoff: { maxDelay: -1 } } } }, {}),
      ).rejects.toThrow('defaults.retry.backoff.maxDelay: Invalid duration -1');
    });

    it('a preset that cannot be built with the defaults under it, even if nothing uses it', async () => {
      // Valid on its own (minimumCalls defaults to 10), invalid once defaults.circuitBreaker applies.
      const defaults = { circuitBreaker: { minimumCalls: 30, slidingWindow: { type: 'count', size: 40 } as const } };
      const presets = { partner: { circuitBreaker: { slidingWindow: { type: 'count', size: 10 } as const } } };
      await expect(boot({ defaults, presets }, {})).rejects.toThrow(
        'presets.partner.circuitBreaker.minimumCalls: Invalid value 30. A count window of 10 calls never holds that many',
      );
    });
  });

  describe('composition order', () => {
    const CANONICAL = ['FallbackPolicy', 'RetryPolicy', 'CircuitBreakerPolicy', 'TimeoutPolicy', 'BulkheadPolicy'];

    it('is canonical whatever order the decorators are written in', async () => {
      @Controller()
      class C {
        @Get('a')
        @Fallback(() => 'fallback')
        @Retry()
        @CircuitBreaker()
        @Timeout(100)
        @Bulkhead()
        a() {}

        @Get('b')
        @Bulkhead()
        @Timeout(100)
        @CircuitBreaker()
        @Retry()
        @Fallback(() => 'fallback')
        b() {}
      }
      const moduleRef = await boot({}, { controllers: [C] });
      const planner = moduleRef.get(EntrypointPlanner);
      expect(stageNames(planner.planFor(C, C.prototype.a))).toEqual(CANONICAL);
      expect(stageNames(planner.planFor(C, C.prototype.b))).toEqual(CANONICAL);
    });

    it('means an open breaker stops retries and the fallback sees the rejection', async () => {
      const moduleRef = await boot({}, {});
      const policy = moduleRef.get(ResilienceService).create({
        fallback: (error) => ({ fallback: (error as Error).name }),
        retry: { attempts: 5, backoff: noDelay },
        circuitBreaker: { minimumCalls: 2, failureRateThreshold: 50 },
      });
      let calls = 0;
      const result = await policy.execute(() => {
        calls++;
        throw new Error('down');
      });
      expect(calls).toBe(2); // the third attempt was rejected by the breaker and not retried
      expect(result).toEqual({ fallback: 'CircuitOpenError' });
    });

    it('resolves each stage from handler decorator > handler preset > class decorator > class preset', async () => {
      @Controller()
      @Timeout(10)
      @Resilience('slow')
      class C {
        @Get('class') fromClass() {}
        @Get('handler-preset') @Resilience('fast') fromHandlerPreset() {}
        @Get('handler') @Resilience('fast') @Timeout(30) fromHandler() {}
        @Get('class-preset') @Retry(2) fromClassPreset() {}
      }
      const moduleRef = await boot(
        {
          defaults: { retry: { attempts: 7 } },
          presets: { slow: { timeout: '1s', bulkhead: { maxConcurrent: 3 } }, fast: { timeout: 20 } },
        },
        { controllers: [C] },
      );
      const planner = moduleRef.get(EntrypointPlanner);
      const timeoutOf = (handler: Function) => stage(planner.planFor(C, handler), TimeoutPolicy)!.timeout;
      expect(timeoutOf(C.prototype.fromClass)).toBe(10);
      expect(timeoutOf(C.prototype.fromHandlerPreset)).toBe(20);
      expect(timeoutOf(C.prototype.fromHandler)).toBe(30);
      // The class preset still contributes the stages nobody else set.
      expect(stageNames(planner.planFor(C, C.prototype.fromClassPreset))).toEqual([
        'RetryPolicy',
        'TimeoutPolicy',
        'BulkheadPolicy',
      ]);
      expect(stage(planner.planFor(C, C.prototype.fromClassPreset), RetryPolicy)).toMatchObject({ attempts: 2 });
    });
  });

  describe('combining levels', () => {
    it("merges a handler's @Retry() options into the preset's retry", async () => {
      @Controller()
      @Resilience('carrier')
      class C {
        @Post() @Retry({ idempotent: true }) create() {}
        @Get() @Retry(5) list() {}
        @Get('once') @Retry(false) once() {}
      }
      const moduleRef = await boot(
        { presets: { carrier: { retry: { attempts: 2, backoff: { delay: 50, factor: 1 } } } } },
        { controllers: [C] },
      );
      const planner = moduleRef.get(EntrypointPlanner);
      const create = planner.planFor(C, C.prototype.create);
      expect(create!.idempotent).toBe(true);
      expect(stage(create, RetryPolicy)!.attempts).toBe(2); // the preset's, not the default 3
      expect(stage(planner.planFor(C, C.prototype.list), RetryPolicy)!.attempts).toBe(5);
      // @Retry(false) turns off the retry the handler would inherit.
      expect(planner.planFor(C, C.prototype.once)!.hasRetry).toBe(false);
    });

    it('accepts the retry shorthand in presets: a number of attempts, or false', async () => {
      @Controller()
      class C {
        @Get('five') @Resilience('five') five() {}
        @Get('off') @Resilience('off') off() {}
      }
      const moduleRef = await boot(
        { presets: { five: { retry: 5 }, off: { retry: false, timeout: '1s' } } },
        { controllers: [C] },
      );
      const planner = moduleRef.get(EntrypointPlanner);
      expect(stage(planner.planFor(C, C.prototype.five), RetryPolicy)!.attempts).toBe(5);
      expect(stageNames(planner.planFor(C, C.prototype.off))).toEqual(['TimeoutPolicy']);
    });

    it('does not let a preset declare handlers idempotent', async () => {
      @Controller()
      class C {
        @Post() @Resilience('eager') create() {}
      }
      const moduleRef = await boot(
        { presets: { eager: { retry: { attempts: 3, idempotent: true } as never } } },
        { controllers: [C] },
      );
      expect(moduleRef.get(EntrypointPlanner).planFor(C, C.prototype.create)!.idempotent).toBe(false);
    });

    it('takes a preset whole: decorators on less specific levels do not contribute to its stages', async () => {
      @Controller()
      @Retry({ attempts: 9, backoff: { delay: '1s' } })
      class C {
        @Get() @Resilience('carrier') get() {}
      }
      const moduleRef = await boot(
        { defaults: { retry: { attempts: 4 } }, presets: { carrier: { retry: {} } } },
        { controllers: [C] },
      );
      // The preset's retry sets nothing, so defaults.retry applies, not the class decorator.
      expect(stage(moduleRef.get(EntrypointPlanner).planFor(C, C.prototype.get), RetryPolicy)!.attempts).toBe(4);
    });

    it('merges method breaker and bulkhead options over class options, field by field', async () => {
      @Controller()
      @CircuitBreaker({ openDuration: '1m', minimumCalls: 5 })
      @Bulkhead({ maxConcurrent: 4, queueTimeout: '2s' })
      class C {
        @Get('a') @CircuitBreaker({ minimumCalls: 3 }) @Bulkhead({ maxQueue: 8 }) a() {}
        @Get('b') b() {}
      }
      const moduleRef = await boot({}, { controllers: [C] });
      const planner = moduleRef.get(EntrypointPlanner);
      const a = planner.planFor(C, C.prototype.a);
      expect(stage(a, CircuitBreakerPolicy)!.options).toMatchObject({ openDuration: 60_000, minimumCalls: 3 });
      expect(stage(a, BulkheadPolicy)).toMatchObject({ maxConcurrent: 4, maxQueue: 8, queueTimeout: 2_000 });
      // Still one breaker per handler: nothing names it.
      const b = planner.planFor(C, C.prototype.b);
      expect(stage(b, CircuitBreakerPolicy)!.options).toMatchObject({ openDuration: 60_000, minimumCalls: 5 });
      expect(stage(a, CircuitBreakerPolicy)).not.toBe(stage(b, CircuitBreakerPolicy));
    });

    it("takes a bare @Timeout()'s duration from the preset or class before defaults.timeout", async () => {
      @Controller()
      @Resilience('carrier')
      class C {
        @Get('preset') @Timeout() fromPreset() {}
      }
      @Controller('other')
      @Timeout('10ms')
      class D {
        @Get() @Timeout() fromClass() {}
      }
      const moduleRef = await boot(
        { defaults: { timeout: '5s' }, presets: { carrier: { timeout: '2s' } } },
        { controllers: [C, D] },
      );
      const planner = moduleRef.get(EntrypointPlanner);
      expect(stage(planner.planFor(C, C.prototype.fromPreset), TimeoutPolicy)!.timeout).toBe(2_000);
      expect(stage(planner.planFor(D, D.prototype.fromClass), TimeoutPolicy)!.timeout).toBe(10);
    });
  });

  it("registers per-handler breakers and bulkheads under the handler's name", async () => {
    @Controller()
    @CircuitBreaker({ minimumCalls: 1 })
    class C {
      @Get('a') @Bulkhead({ maxConcurrent: 2 }) a() {}
      @Get('b') b() {}
    }
    const moduleRef = await boot({}, { controllers: [C] });
    const planner = moduleRef.get(EntrypointPlanner);
    const resilience = moduleRef.get(ResilienceService);
    const planA = planner.planFor(C, C.prototype.a);

    expect(resilience.circuitBreaker('C.a')).toBe(stage(planA, CircuitBreakerPolicy));
    expect(resilience.circuitBreaker('C.b')).toBe(stage(planner.planFor(C, C.prototype.b), CircuitBreakerPolicy));
    expect(resilience.circuitBreaker('C.a')).not.toBe(resilience.circuitBreaker('C.b')); // still one per handler
    expect(resilience.bulkhead('C.a')).toBeInstanceOf(BulkheadPolicy);
    expect(resilience.bulkhead('C.a').active).toBe(0);
    expect(stagesOf(planA)).toContain(resilience.bulkhead('C.a'));
    expect(resilience.circuitBreakers().map((b) => b.name)).toEqual(['C.a', 'C.b']);
    expect(resilience.bulkheads().map((b) => b.name)).toEqual(['C.a']);
  });

  describe('ResilienceService', () => {
    @Injectable()
    class PartnerClient {
      private readonly policy: ResiliencePolicy;
      calls = 0;
      constructor(resilience: ResilienceService) {
        this.policy = resilience.preset('partner-api');
      }
      fetch() {
        return this.policy.execute(() => {
          this.calls++;
          throw new Error('partner down');
        });
      }
    }

    @Controller()
    class PartnerController {
      @Get() @Resilience('partner-api') get() {}
    }

    it('shares the preset breaker between preset() and @Resilience()', async () => {
      const moduleRef = await boot(
        { presets: { 'partner-api': { circuitBreaker: { minimumCalls: 1, openDuration: '1m' } } } },
        { providers: [PartnerClient], controllers: [PartnerController] },
      );
      const client = moduleRef.get(PartnerClient);
      const resilience = moduleRef.get(ResilienceService);
      await expect(client.fetch()).rejects.toThrow('partner down');
      await expect(client.fetch()).rejects.toBeInstanceOf(CircuitOpenError);
      expect(client.calls).toBe(1);
      const plan = moduleRef.get(EntrypointPlanner).planFor(PartnerController, PartnerController.prototype.get)!;
      expect(stagesOf(plan)[0]).toBe(resilience.circuitBreaker('partner-api'));
      expect(resilience.circuitBreaker('partner-api').state).toBe('open');
    });

    it("finds a preset's breaker that nothing used yet, and rejects unknown names", async () => {
      const moduleRef = await boot(
        { presets: { quiet: { circuitBreaker: {} }, plain: { timeout: '1s' } } },
        {},
      );
      const resilience = moduleRef.get(ResilienceService);
      expect(resilience.circuitBreaker('quiet').state).toBe('closed');
      expect(() => resilience.circuitBreaker('qiuet')).toThrow('Unknown circuit breaker "qiuet". Known: quiet.');
      expect(() => resilience.bulkhead('plain')).toThrow('Unknown bulkhead "plain". Known: (none).');
    });

    it('create() applies module defaults and composes in canonical order', async () => {
      const moduleRef = await boot({ defaults: { retry: { attempts: 4, backoff: noDelay } } }, {});
      const policy = moduleRef.get(ResilienceService).create({ timeout: '1s', retry: {} }, 'nightly-sync');
      const stages = (policy as PolicyWrap).policies;
      expect(stages.map((p) => p.constructor.name)).toEqual(['RetryPolicy', 'TimeoutPolicy']);
      expect(stages[0]).toMatchObject({ attempts: 4, name: 'nightly-sync' });
      expect(stages[1]).toMatchObject({ timeout: 1_000 });
    });

    it('the options are injectable as RESILIENCE_MODULE_OPTIONS, and tests can override them', async () => {
      const options = { presets: { carrier: { circuitBreaker: { openDuration: '30s' as const } } } };
      expect((await boot(options, {})).get(RESILIENCE_MODULE_OPTIONS)).toEqual(options);

      @Module({ imports: [ResilienceModule.forRoot(options)] })
      class AppModule {}
      const moduleRef = await Test.createTestingModule({ imports: [AppModule] })
        .overrideProvider(RESILIENCE_MODULE_OPTIONS)
        .useValue({ presets: { carrier: { circuitBreaker: { openDuration: '1s' } } } })
        .compile();
      await moduleRef.init();
      expect(moduleRef.get(ResilienceService).circuitBreaker('carrier').options.openDuration).toBe(1_000);
    });
  });

  describe('events', () => {
    it("ResilienceEvents streams the app's events, which also go to the diagnostics channels", async () => {
      const published = recordEvents();
      const moduleRef = await boot({}, {});
      const events = firstValueFrom(moduleRef.get(ResilienceEvents).events$.pipe(take(2), toArray()));
      const policy = moduleRef
        .get(ResilienceService)
        .create({ retry: { attempts: 3, backoff: noDelay } }, 'nightly-sync');
      await policy.execute(({ attempt }) => (attempt < 3 ? Promise.reject(new Error('x')) : 'ok'), {
        source: 'Jobs.sync',
      });
      const expected = [
        expect.objectContaining({ type: 'retry', policy: 'nightly-sync', source: 'Jobs.sync', attempt: 1 }),
        expect.objectContaining({ type: 'retry', policy: 'nightly-sync', source: 'Jobs.sync', attempt: 2 }),
      ];
      expect(await events).toEqual(expected);
      expect(published).toEqual(expected);
    });

    it('does not stream events of policies created with new, nor of other apps', async () => {
      const first = await boot({}, {});
      const second = await boot({}, {});
      const seen: unknown[] = [];
      first.get(ResilienceEvents).events$.subscribe((e) => seen.push(e));
      await new RetryPolicy({ attempts: 2, backoff: noDelay }).execute(({ attempt }) =>
        attempt < 2 ? Promise.reject(new Error('x')) : 'ok',
      );
      await second
        .get(ResilienceService)
        .create({ retry: { attempts: 2, backoff: noDelay } })
        .execute(({ attempt }) => (attempt < 2 ? Promise.reject(new Error('x')) : 'ok'));
      expect(seen).toEqual([]);
    });
  });

  it('checks fallback method names against the class, on handlers and classes', () => {
    class OrdersController {
      @Fallback('cachedList') list() {}
      // @ts-expect-error: not a method of OrdersController
      @Fallback('cachedLsit') other() {}
      cachedList() {}
    }
    // @ts-expect-error: not a method of Unrelated
    @Fallback('cachedList')
    class Unrelated {
      list() {}
    }
    expect([OrdersController, Unrelated]).toHaveLength(2);
    expect(Fallback(() => [])).toBeTypeOf('function');
  });

  it('supports forRootAsync({ useClass }) with a ResilienceOptionsFactory', async () => {
    @Injectable()
    class ResilienceConfig implements ResilienceOptionsFactory {
      createResilienceOptions(): ResilienceModuleOptions {
        return { presets: { partner: { timeout: '750ms' } } };
      }
    }
    @Controller()
    class C {
      @Get() @Resilience('partner') get() {}
    }
    @Module({ imports: [ResilienceModule.forRootAsync({ useClass: ResilienceConfig })], controllers: [C] })
    class AppModule {}
    const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
    await moduleRef.init();
    expect(stage(moduleRef.get(EntrypointPlanner).planFor(C, C.prototype.get), TimeoutPolicy)!.timeout).toBe(750);
  });

  it('completes events$ at application shutdown, after work that ran while the app was closing', async () => {
    @Injectable()
    class NightlySync implements BeforeApplicationShutdown {
      constructor(private readonly resilience: ResilienceService) {}
      // A last run while the app shuts down (as in-flight requests do while the server drains), retried once.
      beforeApplicationShutdown() {
        return this.resilience
          .create({ retry: { attempts: 2, backoff: noDelay } }, 'nightly-sync')
          .execute(({ attempt }) => (attempt === 1 ? Promise.reject(new Error('flaky')) : 'ok'));
      }
    }
    const moduleRef = await boot({}, { providers: [NightlySync] });
    const seen: string[] = [];
    let completed = false;
    moduleRef.get(ResilienceEvents).events$.subscribe({
      next: (event) => seen.push(`${event.type} ${event.policy}`),
      complete: () => (completed = true),
    });
    await moduleRef.close();
    expect(seen).toEqual(['retry nightly-sync']);
    expect(completed).toBe(true);
  });

  it('supports forRootAsync', async () => {
    @Controller()
    class C {
      @Get() @Timeout() get() {}
    }
    @Module({
      imports: [ResilienceModule.forRootAsync({ useFactory: async () => ({ defaults: { timeout: '1234ms' } }) })],
      controllers: [C],
    })
    class AppModule {}
    const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
    await moduleRef.init();
    const plan = moduleRef.get(EntrypointPlanner).planFor(C, C.prototype.get);
    expect(stagesOf(plan)[0]).toMatchObject({ timeout: 1_234 });
  });
});
