/**
 * Every stage at HTTP entrypoints on both adapters, against a real HTTP
 * dependency on 127.0.0.1 that fails, hangs or throttles on demand. Timeouts
 * and backoffs run on short real timers (the sockets need real I/O);
 * `openDuration`, time windows and token refills read `Date`, which is the
 * only clock these tests fake.
 */
import {
  Controller,
  Get,
  Head,
  Injectable,
  Module,
  Post,
  type ExecutionContext,
  type OnModuleInit,
  type INestApplication,
} from '@nestjs/common';
import { from } from 'rxjs';
import request from 'supertest';
import { adapters, createApp } from './support/adapters.js';
import {
  Bulkhead,
  CircuitBreaker,
  CircuitOpenError,
  Fallback,
  FallbackPolicy,
  OutboundRateLimitError,
  OutboundRateLimitPolicy,
  Resilience,
  ResilienceContext,
  ResilienceEvents,
  ResilienceModule,
  ResiliencePolicy,
  ResilienceService,
  ResilienceTimeoutError,
  Retry,
  RetryPolicy,
  Signal,
  Timeout,
  TimeoutPolicy,
  type ResilienceEvent,
} from '../lib/index.js';
import { Downstream, DownstreamError, fast, send, until } from './downstream.js';
import { recordEvents } from './events.js';

const downstream = new Downstream();

/** Reads the attempt's signal from ResilienceContext instead of a handler parameter. */
@Injectable()
class CatalogApi {
  constructor(private readonly context: ResilienceContext) {}

  search() {
    return downstream.call('/search', this.context.signal);
  }
}

@Controller('retry')
class RetryController {
  @Get('flaky')
  @Retry({ attempts: 3, backoff: fast })
  flaky(@Signal() signal: AbortSignal) {
    return downstream.call('/flaky', signal);
  }

  @Head('head')
  @Retry({ attempts: 3, backoff: fast })
  async head(@Signal() signal: AbortSignal) {
    await downstream.call('/head', signal);
  }

  @Post('charge')
  @Retry({ attempts: 3, backoff: fast })
  charge() {
    return downstream.call('/charge');
  }

  @Post('upsert')
  @Retry({ attempts: 3, backoff: fast, idempotent: true })
  upsert() {
    return downstream.call('/upsert');
  }

  @Get('only-503')
  @Retry({ attempts: 3, backoff: fast, retryIf: (error) => (error as DownstreamError).status === 503 })
  only503() {
    return downstream.call('/only-503');
  }

  // Honors the dependency's own hint, as an SDK's Retry-After would be.
  @Get('throttled')
  @Retry({ attempts: 2, backoff: (_attempt, error) => (error as DownstreamError).retryAfterMs ?? 0 })
  throttled() {
    return downstream.call('/throttled');
  }

  @Get('guarded')
  @Retry({ attempts: 3, backoff: fast })
  @CircuitBreaker({ minimumCalls: 1, openDuration: '30s' })
  @Fallback(() => ({ fallback: true }))
  guarded() {
    return downstream.call('/guarded');
  }
}

@Controller('class-retry')
@Retry({ attempts: 3, backoff: fast })
class ClassRetryController {
  @Get('inherits')
  inherits() {
    return downstream.call('/inherits');
  }

  @Get('opted-out')
  @Retry(false)
  optedOut() {
    return downstream.call('/opted-out');
  }
}

@Controller('timeout')
class TimeoutController {
  constructor(private readonly catalog: CatalogApi) {}

  @Get('hang')
  @Timeout(250)
  hang(@Signal() signal: AbortSignal) {
    return downstream.call('/hang', signal);
  }

  @Get('per-attempt')
  @Timeout(250)
  @Retry({ attempts: 2, backoff: fast })
  perAttempt(@Signal() signal: AbortSignal) {
    return downstream.call('/per-attempt', signal);
  }

  @Get('context')
  @Timeout(250)
  context() {
    return this.catalog.search();
  }
}

// Refers to a breaker a decorator configures, so it builds its policy once bootstrap declared that breaker
// (ResilienceModule's onModuleInit runs first); a preset could be used from the constructor.
@Injectable()
class InventorySync implements OnModuleInit {
  private policy!: ResiliencePolicy;

  constructor(private readonly resilience: ResilienceService) {}

  onModuleInit() {
    this.policy = this.resilience.create({ circuitBreaker: 'inventory' }, 'inventory-sync');
  }

  run() {
    return this.policy.execute(({ signal }) => downstream.call('/sync', signal), { source: 'InventorySync.run' });
  }
}

@Controller('breaker')
class BreakerController {
  constructor(
    private readonly sync: InventorySync,
    private readonly resilience: ResilienceService,
  ) {}

  @Get('stock')
  @CircuitBreaker({ name: 'inventory', minimumCalls: 2, failureRateThreshold: 50, openDuration: '30s' })
  stock(@Signal() signal: AbortSignal) {
    return downstream.call('/stock', signal);
  }

  @Get('reserve')
  @CircuitBreaker('inventory')
  reserve(@Signal() signal: AbortSignal) {
    return downstream.call('/reserve', signal);
  }

  // No decorators: the service's policy shares the breaker, and its CircuitOpenError still becomes a 503.
  @Post('sync')
  runSync() {
    return this.sync.run();
  }

  @Get('search')
  @CircuitBreaker({
    slidingWindow: { type: 'time', size: '10s' },
    minimumCalls: 3,
    failureRateThreshold: 100,
    openDuration: '5s',
  })
  search() {
    return downstream.call('/search');
  }

  @Get('recorded')
  @CircuitBreaker({
    minimumCalls: 1,
    openDuration: '5s',
    recordIf: (error) => (error as DownstreamError).status >= 503,
  })
  recorded() {
    return downstream.call('/recorded');
  }

  @Get('probe')
  @CircuitBreaker({ minimumCalls: 1, openDuration: '5s', halfOpenMaxCalls: 1 })
  probe() {
    return downstream.call('/probe');
  }

  @Get('health')
  health() {
    return this.resilience.circuitBreakers().map((b) => ({ name: b.name, state: b.state }));
  }
}

@Controller('bulkhead')
class BulkheadController {
  constructor(private readonly resilience: ResilienceService) {}

  @Get('export')
  @Bulkhead({ name: 'exports', maxConcurrent: 1, maxQueue: 1, queueTimeout: '100ms' })
  export() {
    return downstream.call('/export');
  }

  @Get('report/long')
  @Timeout(2_000)
  @Bulkhead({ name: 'reports', maxConcurrent: 1, maxQueue: 5 })
  longReport(@Signal() signal: AbortSignal) {
    return downstream.call('/report/long', signal);
  }

  @Get('report')
  @Timeout(60)
  @Bulkhead('reports')
  report(@Signal() signal: AbortSignal) {
    return downstream.call('/report', signal);
  }

  @Get('load')
  load() {
    return this.resilience.bulkheads().map((b) => ({ name: b.name, active: b.active, queued: b.queued }));
  }
}

@Injectable()
class PartnerQuotes {
  private readonly policy: ResiliencePolicy;

  constructor(resilience: ResilienceService) {
    this.policy = resilience.preset('partner');
  }

  batch() {
    return this.policy.execute(({ signal }) => downstream.call('/batch', signal), { source: 'PartnerQuotes.batch' });
  }
}

@Controller('partner')
class PartnerController {
  constructor(private readonly quotes: PartnerQuotes) {}

  @Get('quote')
  @Resilience('partner')
  quote(@Signal() signal: AbortSignal) {
    return downstream.call('/quote', signal);
  }

  @Get('batch')
  batch() {
    return this.quotes.batch();
  }

  @Get('metered')
  @Resilience('metered')
  metered(@Signal() signal: AbortSignal) {
    return downstream.call('/metered', signal);
  }
}

@Controller('fallback')
class FallbackController {
  @Get('recommendations')
  @Fallback('cachedRecommendations')
  recommendations() {
    return downstream.call('/recommendations');
  }

  cachedRecommendations(error: DownstreamError, context: ExecutionContext) {
    return { items: ['bestseller'], status: error.status, path: context.switchToHttp().getRequest().url };
  }

  // The tutorial's recipe: live answers or an error while closed, flat rates while open.
  @Get('quotes')
  @CircuitBreaker({ minimumCalls: 1, openDuration: '30s' })
  @Fallback(() => ({ flatRate: 4.99 }), { handleIf: (error) => error instanceof CircuitOpenError })
  quotes() {
    return downstream.call('/quotes');
  }

  @Get('feed')
  @Resilience('cached-feed')
  feed(@Signal() signal: AbortSignal) {
    return downstream.call('/feed', signal);
  }

  @Get('busy')
  @Bulkhead({ maxConcurrent: 1 })
  @Fallback(() => ({ busy: true }))
  busy() {
    return downstream.call('/busy');
  }
}

const describeError = (error: unknown) => ({ fallback: (error as Error).constructor.name });

// The same stages, written in two orders: both compose Fallback(Retry(CircuitBreaker(Timeout(Bulkhead)))).
@Controller('order')
class OrderController {
  @Get('written-inside-out')
  @Bulkhead({ maxConcurrent: 5 })
  @Timeout(1_000)
  @CircuitBreaker({ minimumCalls: 2, failureRateThreshold: 100, openDuration: '30s' })
  @Retry({ attempts: 5, backoff: fast })
  @Fallback(describeError)
  insideOut() {
    return downstream.call('/inside-out');
  }

  @Get('written-outside-in')
  @Fallback(describeError)
  @Retry({ attempts: 5, backoff: fast })
  @CircuitBreaker({ minimumCalls: 2, failureRateThreshold: 100, openDuration: '30s' })
  @Timeout(1_000)
  @Bulkhead({ maxConcurrent: 5 })
  outsideIn() {
    return downstream.call('/outside-in');
  }
}

@Injectable()
class RepricingJob {
  private readonly policy: ResiliencePolicy;

  constructor(resilience: ResilienceService) {
    this.policy = resilience.create({ retry: { attempts: Infinity, backoff: { delay: 10, factor: 1 } } }, 'repricing');
  }

  run() {
    return this.policy.execute(({ signal }) => downstream.call('/reprice', signal));
  }
}

/**
 * Policy objects created with `new`, composed in a non-canonical order: a
 * total budget around the retries, each attempt with its own timeout. They
 * publish on the diagnostics channels, not on the app's events$.
 */
@Injectable()
class SearchClient {
  private readonly total = ResiliencePolicy.wrap(
    new TimeoutPolicy(1_000),
    new RetryPolicy({ attempts: 10, backoff: { delay: 1, factor: 1 } }),
    new TimeoutPolicy(250),
  );

  private readonly limited = ResiliencePolicy.wrap(
    new FallbackPolicy(() => ({ ok: false, n: 0 }), { handleIf: (error) => error instanceof OutboundRateLimitError }),
    new OutboundRateLimitPolicy({ name: 'search-quota', limit: 2, interval: '1m' }),
  );

  search() {
    return this.total.execute(({ signal }) => downstream.call('/search', signal), { source: 'SearchClient.search' });
  }

  stream() {
    return this.total.executeObservable(({ signal }) => from(downstream.call('/stream', signal)));
  }

  suggest() {
    return this.limited.execute(({ signal }) => downstream.call('/suggest', signal), {
      source: 'SearchClient.suggest',
    });
  }
}

@Controller('search')
class SearchController {
  constructor(private readonly client: SearchClient) {}

  @Get()
  search() {
    return this.client.search();
  }

  @Get('stream')
  stream() {
    return this.client.stream();
  }

  @Get('suggest')
  suggest() {
    return this.client.suggest();
  }
}

@Module({
  imports: [
    ResilienceModule.forRoot({
      presets: {
        partner: {
          circuitBreaker: { minimumCalls: 1, openDuration: '30s' },
          outboundRateLimit: { limit: 2, interval: '10s' },
        },
        metered: { outboundRateLimit: { limit: 1, interval: 600, maxWait: 1_200 }, timeout: 300 },
        'cached-feed': { timeout: 250, fallback: () => ({ items: [], stale: true }) },
      },
    }),
  ],
  controllers: [
    RetryController,
    ClassRetryController,
    TimeoutController,
    BreakerController,
    BulkheadController,
    PartnerController,
    FallbackController,
    OrderController,
    SearchController,
  ],
  providers: [CatalogApi, InventorySync, PartnerQuotes, RepricingJob, SearchClient],
})
class HttpIntegrationModule {}

describe.each(adapters.map((a) => a.name))(
  'Resilience at HTTP entrypoints, against a real dependency (%s)',
  (adapter) => {
    let app: INestApplication;
    let events: ResilienceEvent[];

    beforeAll(async () => {
      await downstream.start();
      app = await createApp(adapter, HttpIntegrationModule, { setup: (a) => a.useLogger(false) });
      app.get(ResilienceEvents).events$.subscribe((event) => events.push(event));
    });
    afterAll(async () => {
      await app.close();
      await downstream.stop();
    });
    beforeEach(() => {
      downstream.reset();
      for (const breaker of app.get(ResilienceService).circuitBreakers()) {
        breaker.reset();
      }
      events = [];
    });
    afterEach(() => {
      vi.useRealTimers();
      downstream.release();
    });

    const http = () => request(app.getHttpServer());
    const ofType = <T extends ResilienceEvent['type']>(type: T) =>
      events.filter((e): e is Extract<ResilienceEvent, { type: T }> => e.type === type);

    describe('Retry', () => {
      it('retries a GET until the dependency recovers, reporting each failed attempt', async () => {
        downstream.next('fail', 'fail');
        const res = await http().get('/retry/flaky').expect(200);
        expect(res.body).toEqual({ ok: true, n: 3 });
        expect(downstream.requests).toHaveLength(3);
        expect(ofType('retry')).toEqual([
          expect.objectContaining({
            policy: 'RetryController.flaky',
            source: 'RetryController.flaky',
            attempt: 1,
            delayMs: 1,
          }),
          expect.objectContaining({ attempt: 2, error: expect.any(DownstreamError) }),
        ]);
      });

      it('retries HEAD, a safe method, too', async () => {
        downstream.next('fail');
        await http().head('/retry/head').expect(200);
        expect(downstream.requests).toHaveLength(2);
      });

      it('calls the dependency once for a POST, and again only when the handler is declared idempotent', async () => {
        downstream.mode = 'fail';
        await http().post('/retry/charge').expect(500);
        expect(downstream.requests).toHaveLength(1);

        downstream.reset();
        downstream.next('fail');
        await http().post('/retry/upsert').expect(201);
        expect(downstream.requests).toHaveLength(2);
      });

      it('gives up after the last attempt with the error the dependency caused', async () => {
        downstream.mode = 'fail';
        await http().get('/retry/flaky').expect(500);
        expect(downstream.requests).toHaveLength(3);
      });

      it('retries only what retryIf accepts', async () => {
        downstream.mode = 'fail';
        downstream.failStatus = 500;
        await http().get('/retry/only-503').expect(500);
        expect(downstream.requests).toHaveLength(1);

        downstream.reset();
        downstream.next('fail', 'fail');
        await http().get('/retry/only-503').expect(200);
        expect(downstream.requests).toHaveLength(3);
      });

      it("waits for the delay a backoff function reads from the dependency's 429", async () => {
        downstream.failStatus = 429;
        downstream.retryAfterMs = 40;
        downstream.next('fail');
        const started = performance.now();
        await http().get('/retry/throttled').expect(200);
        expect(performance.now() - started).toBeGreaterThanOrEqual(35);
        expect(ofType('retry')).toEqual([expect.objectContaining({ attempt: 1, delayMs: 40 })]);
      });

      it("neither retries, records nor replaces a dependency's 4xx answer", async () => {
        downstream.mode = 'fail';
        downstream.failStatus = 422;
        await http().get('/retry/guarded').expect(500);
        await http().get('/retry/guarded').expect(500);
        expect(downstream.requests).toHaveLength(2);
        const breaker = app.get(ResilienceService).circuitBreaker('RetryController.guarded');
        expect(breaker.state).toBe('closed');
        expect(breaker.stats.failures).toBe(0);
        expect(ofType('fallback')).toEqual([]);
      });

      it("retries and records a dependency's 408 and 429 like a 5xx", async () => {
        downstream.mode = 'fail';
        downstream.failStatus = 429;
        // The first attempt fails and opens the breaker (minimumCalls: 1); the retry is rejected, and the fallback answers.
        await http().get('/retry/guarded').expect(200, { fallback: true });
        expect(downstream.requests).toHaveLength(1);
        expect(app.get(ResilienceService).circuitBreaker('RetryController.guarded').state).toBe('open');

        app.get(ResilienceService).circuitBreaker('RetryController.guarded').reset();
        downstream.reset();
        downstream.failStatus = 408;
        downstream.next('fail');
        await http().get('/retry/flaky').expect(200);
        expect(downstream.requests).toHaveLength(2);
      });

      it('applies a class-level @Retry() to every route, except one that turns it off with @Retry(false)', async () => {
        downstream.next('fail');
        await http().get('/class-retry/inherits').expect(200);
        expect(downstream.requests).toHaveLength(2);

        downstream.reset();
        downstream.next('fail');
        await http().get('/class-retry/opted-out').expect(500);
        expect(downstream.requests).toHaveLength(1);
      });
    });

    describe('Timeout', () => {
      it('answers 504 in time and cancels the request to a hanging dependency', async () => {
        downstream.mode = 'hang';
        const started = performance.now();
        const res = await http().get('/timeout/hang').expect(504);
        expect(performance.now() - started).toBeLessThan(1_000);
        expect(res.body).toEqual({
          statusCode: 504,
          error: 'Gateway Timeout',
          message: 'The operation timed out',
          code: 'TIMEOUT',
        });
        await until(() => downstream.requests[0]?.aborted, 'the dependency to see the request cancelled');
        expect(ofType('timeout')).toEqual([
          expect.objectContaining({
            policy: 'TimeoutController.hang',
            source: 'TimeoutController.hang',
            timeoutMs: 250,
          }),
        ]);
      });

      it('gives every attempt its own budget, and cancels each one', async () => {
        downstream.mode = 'hang';
        await http().get('/timeout/per-attempt').expect(504);
        await until(
          () => downstream.requests.length === 2 && downstream.requests.every((r) => r.aborted),
          'both attempts cancelled',
        );
        expect(ofType('timeout')).toHaveLength(2);
        expect(ofType('retry')).toEqual([
          expect.objectContaining({ attempt: 1, error: expect.any(ResilienceTimeoutError) }),
        ]);
      });

      it('answers from the second attempt when only the first one hung', async () => {
        downstream.next('hang');
        await http().get('/timeout/per-attempt').expect(200, { ok: true, n: 2 });
        await until(() => downstream.requests[0].aborted, 'the first attempt cancelled');
      });

      it('cancels the call a service makes with the signal from ResilienceContext', async () => {
        downstream.mode = 'hang';
        await http().get('/timeout/context').expect(504);
        await until(() => downstream.requests[0]?.aborted, 'the service call cancelled');
      });
    });

    describe('Circuit breaker', () => {
      it('opens a named breaker shared by two routes and a service, then recovers through a probe', async () => {
        vi.useFakeTimers({ toFake: ['Date'] });
        const channel = recordEvents();
        downstream.mode = 'fail';
        await http().get('/breaker/stock').expect(500);
        await http().get('/breaker/stock').expect(500);

        const open = await http().get('/breaker/stock').expect(503);
        expect(open.headers['retry-after']).toBe('30');
        expect(open.body).toEqual({
          statusCode: 503,
          error: 'Service Unavailable',
          message: 'Service temporarily unavailable',
          code: 'CIRCUIT_OPEN',
        });
        await http().get('/breaker/reserve').expect(503);
        const fromService = await http().post('/breaker/sync').expect(503);
        expect(fromService.body.code).toBe('CIRCUIT_OPEN');
        expect(downstream.requests).toHaveLength(2);

        vi.setSystemTime(Date.now() + 10_000);
        expect((await http().get('/breaker/reserve').expect(503)).headers['retry-after']).toBe('20');

        vi.setSystemTime(Date.now() + 20_000);
        expect(app.get(ResilienceService).circuitBreaker('inventory').state).toBe('half-open');
        downstream.mode = 'up';
        await http().post('/breaker/sync').expect(201);
        await http().get('/breaker/reserve').expect(200);
        expect(app.get(ResilienceService).circuitBreaker('inventory').state).toBe('closed');

        const transitions = events.filter((e) => e.type.startsWith('circuit-') && e.type !== 'circuit-rejected');
        expect(transitions).toEqual([
          expect.objectContaining({ type: 'circuit-open', policy: 'inventory', from: 'closed', to: 'open' }),
          expect.objectContaining({ type: 'circuit-half-open', policy: 'inventory', from: 'open', to: 'half-open' }),
          expect.objectContaining({ type: 'circuit-closed', policy: 'inventory', from: 'half-open', to: 'closed' }),
        ]);
        expect(ofType('circuit-rejected').map((e) => [e.source, e.retryAfterMs])).toEqual([
          ['BreakerController.stock', 30_000],
          ['BreakerController.reserve', 30_000],
          ['InventorySync.run', 30_000],
          ['BreakerController.reserve', 20_000],
        ]);
        // Diagnostics channels carry the same events (plus those of other apps in the process).
        expect(channel.filter((e) => e.policy === 'inventory').map((e) => e.type)).toEqual(
          events.filter((e) => e.policy === 'inventory').map((e) => e.type),
        );
      });

      it('opens again for another openDuration when the probe fails', async () => {
        vi.useFakeTimers({ toFake: ['Date'] });
        downstream.mode = 'fail';
        await http().get('/breaker/stock').expect(500);
        await http().get('/breaker/stock').expect(500);
        vi.setSystemTime(Date.now() + 30_000);

        await http().get('/breaker/stock').expect(500);
        const res = await http().get('/breaker/stock').expect(503);
        expect(res.headers['retry-after']).toBe('30');
        expect(downstream.requests).toHaveLength(3);
        expect(ofType('circuit-open').map((e) => e.from)).toEqual(['closed', 'half-open']);
      });

      it('forgets failures older than a time window', async () => {
        vi.useFakeTimers({ toFake: ['Date'] });
        downstream.mode = 'fail';
        await http().get('/breaker/search').expect(500);
        await http().get('/breaker/search').expect(500);
        vi.setSystemTime(Date.now() + 11_000);

        await http().get('/breaker/search').expect(500);
        expect(app.get(ResilienceService).circuitBreaker('BreakerController.search').stats).toMatchObject({ total: 1 });
        await http().get('/breaker/search').expect(500);
        await http().get('/breaker/search').expect(500);
        await http().get('/breaker/search').expect(503);
        expect(downstream.requests).toHaveLength(5);
      });

      it('records only the failures recordIf accepts', async () => {
        downstream.mode = 'fail';
        downstream.failStatus = 500;
        await http().get('/breaker/recorded').expect(500);
        await http().get('/breaker/recorded').expect(500);
        expect(app.get(ResilienceService).circuitBreaker('BreakerController.recorded').state).toBe('closed');

        downstream.failStatus = 503;
        await http().get('/breaker/recorded').expect(500);
        await http().get('/breaker/recorded').expect(503);
        expect(downstream.requests).toHaveLength(3);
      });

      it('lets halfOpenMaxCalls probes through, and answers the others 503 with Retry-After: 1', async () => {
        vi.useFakeTimers({ toFake: ['Date'] });
        downstream.mode = 'fail';
        await http().get('/breaker/probe').expect(500);
        vi.setSystemTime(Date.now() + 5_000);

        downstream.mode = 'hold';
        const probe = send(http().get('/breaker/probe'));
        await until(() => downstream.requests.length === 2, 'the probe to reach the dependency');
        const rejected = await http().get('/breaker/probe').expect(503);
        expect(rejected.headers['retry-after']).toBe('1');
        expect(ofType('circuit-rejected').at(-1)).toMatchObject({ retryAfterMs: 0 });

        downstream.release();
        expect((await probe).status).toBe(200);
        expect(app.get(ResilienceService).circuitBreaker('BreakerController.probe').state).toBe('closed');
      });

      it('can be tripped and reset by operators, and lists every breaker for a health check', async () => {
        const inventory = app.get(ResilienceService).circuitBreaker('inventory');
        inventory.trip();
        await http().get('/breaker/reserve').expect(503);
        const health = await http().get('/breaker/health').expect(200);
        expect(health.body).toContainEqual({ name: 'inventory', state: 'open' });
        expect(health.body).toContainEqual({ name: 'BreakerController.probe', state: 'closed' });

        inventory.reset();
        await http().get('/breaker/reserve').expect(200);
        expect(downstream.requests).toHaveLength(1);
      });
    });

    describe('Bulkhead', () => {
      it('runs maxConcurrent calls, queues maxQueue, and answers the rest 503 without Retry-After', async () => {
        downstream.mode = 'hold';
        const first = send(http().get('/bulkhead/export'));
        await until(() => downstream.requests.length === 1, 'the first export to start');
        const second = send(http().get('/bulkhead/export'));
        await until(() => app.get(ResilienceService).bulkhead('exports').queued === 1, 'the second export to queue');
        expect((await http().get('/bulkhead/load').expect(200)).body).toContainEqual({
          name: 'exports',
          active: 1,
          queued: 1,
        });

        const third = await http().get('/bulkhead/export').expect(503);
        expect(third.body).toEqual({
          statusCode: 503,
          error: 'Service Unavailable',
          message: 'Server is at capacity',
          code: 'BULKHEAD_FULL',
        });
        expect(third.headers['retry-after']).toBeUndefined();
        expect(ofType('bulkhead-rejected')).toEqual([
          expect.objectContaining({
            policy: 'exports',
            source: 'BulkheadController.export',
            reason: 'full',
            active: 1,
            queued: 1,
          }),
        ]);

        downstream.mode = 'up';
        downstream.release();
        expect((await first).status).toBe(200);
        expect((await second).status).toBe(200);
        expect(downstream.requests).toHaveLength(2);
        expect(app.get(ResilienceService).bulkhead('exports').active).toBe(0);
      });

      it('answers 503 to a call that waited queueTimeout in the queue', async () => {
        downstream.mode = 'hold';
        const first = send(http().get('/bulkhead/export'));
        await until(() => downstream.requests.length === 1, 'the first export to start');
        const started = performance.now();
        const queued = await http().get('/bulkhead/export').expect(503);
        expect(performance.now() - started).toBeGreaterThanOrEqual(90);
        expect(queued.body.code).toBe('BULKHEAD_FULL');
        expect(ofType('bulkhead-rejected')).toEqual([expect.objectContaining({ reason: 'queue-timeout' })]);

        downstream.release();
        expect((await first).status).toBe(200);
        expect(downstream.requests).toHaveLength(1);
      });

      it('counts time in the queue against the timeout: a queued call times out and leaves the queue', async () => {
        downstream.mode = 'hold';
        const first = send(http().get('/bulkhead/report/long'));
        await until(() => downstream.requests.length === 1, 'the long report to start');
        await http().get('/bulkhead/report').expect(504);
        const bulkhead = app.get(ResilienceService).bulkhead('reports');
        expect([bulkhead.active, bulkhead.queued]).toEqual([1, 0]);

        downstream.release();
        expect((await first).status).toBe(200);
        expect(downstream.requests).toHaveLength(1);
      });
    });

    describe('Outbound rate limit', () => {
      it("shares a preset's token bucket between a route and a service, answering 503 with Retry-After", async () => {
        vi.useFakeTimers({ toFake: ['Date'] });
        await http().get('/partner/quote').expect(200);
        await http().get('/partner/batch').expect(200);

        const fromService = await http().get('/partner/batch').expect(503);
        expect(fromService.headers['retry-after']).toBe('5');
        expect(fromService.body).toEqual({
          statusCode: 503,
          error: 'Service Unavailable',
          message: 'Rate limit of a dependency exceeded',
          code: 'RATE_LIMITED',
        });
        const fromRoute = await http().get('/partner/quote').expect(503);
        expect(fromRoute.headers['retry-after']).toBe('5');
        expect(downstream.requests).toHaveLength(2);
        expect(ofType('rate-limited')).toEqual([
          expect.objectContaining({ policy: 'partner', source: 'PartnerQuotes.batch', retryAfterMs: 5_000 }),
          expect.objectContaining({ policy: 'partner', source: 'PartnerController.quote', retryAfterMs: 5_000 }),
        ]);
        // Inside the breaker: its rejections are not failures of the dependency.
        expect(app.get(ResilienceService).circuitBreaker('partner').stats).toMatchObject({ total: 2, failures: 0 });

        vi.setSystemTime(Date.now() + 5_000);
        await http().get('/partner/quote').expect(200);
      });

      it("waits up to maxWait for a token, outside the timeout's budget", async () => {
        await http().get('/partner/metered').expect(200);
        const started = performance.now();
        await http().get('/partner/metered').expect(200); // waits up to 600 ms for a token; the timeout is 300 ms
        expect(performance.now() - started).toBeGreaterThanOrEqual(250);
        expect(ofType('timeout')).toEqual([]);
        expect(downstream.requests).toHaveLength(2);
      });
    });

    describe('Fallback', () => {
      it('answers with the fallback method, which gets the error and the ExecutionContext', async () => {
        downstream.mode = 'fail';
        await http()
          .get('/fallback/recommendations')
          .expect(200, { items: ['bestseller'], status: 503, path: '/fallback/recommendations' });
        expect(ofType('fallback')).toEqual([
          expect.objectContaining({ policy: 'FallbackController.recommendations', error: expect.any(DownstreamError) }),
        ]);
      });

      it("replaces only an open breaker's rejection when handleIf says so, without calling the dependency", async () => {
        downstream.mode = 'fail';
        await http().get('/fallback/quotes').expect(500);
        await http().get('/fallback/quotes').expect(200, { flatRate: 4.99 });
        expect(downstream.requests).toHaveLength(1);
      });

      it("applies a preset's fallback at the entrypoint, replacing its timeout", async () => {
        downstream.mode = 'hang';
        await http().get('/fallback/feed').expect(200, { items: [], stale: true });
        await until(() => downstream.requests[0]?.aborted, 'the dependency call cancelled');
      });

      it('replaces a full bulkhead', async () => {
        downstream.mode = 'hold';
        const first = send(http().get('/fallback/busy'));
        await until(() => downstream.requests.length === 1, 'the first call to start');
        await http().get('/fallback/busy').expect(200, { busy: true });
        downstream.release();
        expect((await first).body).toEqual({ ok: true, n: 1 });
      });
    });

    describe('Composition order', () => {
      it.each(['written-inside-out', 'written-outside-in'])(
        'is canonical (%s): the retry stops once the breaker opens, and the fallback sees the rejection',
        async (route) => {
          downstream.mode = 'fail';
          await http().get(`/order/${route}`).expect(200, { fallback: 'CircuitOpenError' });
          expect(downstream.requests).toHaveLength(2);
          expect(events.map((e) => e.type)).toEqual(['retry', 'circuit-open', 'retry', 'circuit-rejected', 'fallback']);
        },
      );
    });

    describe('Inside services', () => {
      it('bounds retries with a total timeout composed by ResiliencePolicy.wrap(), answering 504', async () => {
        const channel = recordEvents();
        downstream.mode = 'hang';
        const started = performance.now();
        await http().get('/search').expect(504);
        expect(performance.now() - started).toBeLessThan(3_000);

        await until(
          () => downstream.requests.length >= 2 && downstream.requests.every((r) => r.aborted),
          'every attempt cancelled',
        );
        expect(downstream.requests.length).toBeLessThan(10);
        const timeouts = channel.filter((e) => e.type === 'timeout' && e.source === 'SearchClient.search');
        expect(timeouts.at(-1)).toMatchObject({ timeoutMs: 1_000 });
        expect(events).toEqual([]); // policies created with new aren't the app's
      });

      it('retries an Observable call through executeObservable()', async () => {
        downstream.next('fail');
        await http().get('/search/stream').expect(200, { ok: true, n: 2 });
      });

      it("replaces a rejection of the policy object's rate limit with its fallback", async () => {
        vi.useFakeTimers({ toFake: ['Date'] });
        const channel = recordEvents();
        await http().get('/search/suggest').expect(200, { ok: true, n: 1 });
        await http().get('/search/suggest').expect(200, { ok: true, n: 2 });
        await http().get('/search/suggest').expect(200, { ok: false, n: 0 });
        expect(downstream.requests).toHaveLength(2);
        expect(channel.filter((e) => e.policy === 'search-quota')).toEqual([
          expect.objectContaining({ type: 'rate-limited', source: 'SearchClient.suggest', retryAfterMs: 30_000 }),
        ]);
      });

      it('cancels a job and every retry it started with the signal of ResilienceContext.run()', async () => {
        downstream.mode = 'fail';
        const controller = new AbortController();
        const job = app.get(ResilienceContext).run({ signal: controller.signal }, () => app.get(RepricingJob).run());
        await until(() => downstream.requests.length >= 3, 'a few attempts');
        controller.abort(new Error('shutting down'));
        await expect(job).rejects.toThrow('shutting down');

        const calls = downstream.requests.length;
        await new Promise((resolve) => setTimeout(resolve, 50));
        expect(downstream.requests).toHaveLength(calls);
      });
    });
  },
);
