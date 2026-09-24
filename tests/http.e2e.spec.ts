import {
  Controller,
  Get,
  Injectable,
  Logger,
  Module,
  NotFoundException,
  Post,
  Scope,
  Sse,
  UseGuards,
  type CanActivate,
  type ExecutionContext,
  type INestApplication,
} from '@nestjs/common';
import { EMPTY, interval, map, take } from 'rxjs';
import request from 'supertest';
import { adapters, createApp } from './support/adapters.js';
import {
  Bulkhead,
  CircuitBreaker,
  Fallback,
  Resilience,
  ResilienceContext,
  ResilienceEvents,
  ResilienceModule,
  ResilienceService,
  Retry,
  Signal,
  Timeout,
  ResilienceTimeoutError,
  type ResilienceEvent,
  type ResiliencePolicy,
} from '../lib/index.js';

const fast = { delay: 1, factor: 1 };

function gate() {
  let open!: () => void;
  const promise = new Promise<void>((resolve) => (open = resolve));
  return { promise, open };
}

const state = {
  calls: {} as Record<string, number>,
  guard: 0,
  signals: [] as AbortSignal[],
  gate: undefined as ReturnType<typeof gate> | undefined,
  events: [] as ResilienceEvent[],
};
const hit = (name: string) => (state.calls[name] = (state.calls[name] ?? 0) + 1);

@Injectable()
class CountingGuard implements CanActivate {
  canActivate() {
    state.guard++;
    return true;
  }
}

/** A service that reads the attempt from ResilienceContext instead of taking a signal parameter. */
@Injectable()
class CatalogRepository {
  constructor(private readonly context: ResilienceContext) {}

  async slowQuery() {
    const signal = this.context.signal!;
    state.signals.push(signal);
    await new Promise((_, reject) => signal.addEventListener('abort', () => reject(signal.reason)));
  }
}

@Controller('catalog')
@UseGuards(CountingGuard)
class CatalogController {
  constructor(
    private readonly context: ResilienceContext,
    private readonly repository: CatalogRepository,
  ) {}

  @Get('flaky')
  @Retry({ attempts: 3, backoff: fast })
  flaky() {
    if (hit('flaky') < 3) {
      throw new Error('flaky dependency');
    }
    return { attempt: this.context.attempt };
  }

  @Post('orders')
  @Retry({ attempts: 3, backoff: fast })
  createOrder() {
    hit('createOrder');
    throw new Error('database down');
  }

  @Post('orders/upsert')
  @Retry({ attempts: 3, backoff: fast, idempotent: true })
  upsertOrder() {
    if (hit('upsertOrder') < 2) {
      throw new Error('database down');
    }
    return { upserted: true };
  }

  @Get('slow')
  @Timeout('50ms')
  async slow(@Signal() signal: AbortSignal) {
    state.signals.push(signal);
    await new Promise((_, reject) => signal.addEventListener('abort', () => reject(signal.reason)));
  }

  @Get('slow-repository')
  @Timeout(50)
  slowRepository() {
    return this.repository.slowQuery();
  }

  @Get('stock')
  @CircuitBreaker({ name: 'inventory', minimumCalls: 2, failureRateThreshold: 50, openDuration: '1m' })
  stock() {
    hit('stock');
    throw new Error('inventory down');
  }

  @Get('reserve')
  @CircuitBreaker('inventory')
  reserve() {
    hit('reserve');
    return { reserved: true };
  }

  @Get('report')
  @Bulkhead({ maxConcurrent: 1 })
  async report() {
    hit('report');
    await state.gate?.promise;
    return { done: true };
  }

  // Ignores its signal: after the timeout it keeps running until the gate opens.
  @Get('report/slow')
  @Timeout(50)
  @Bulkhead({ maxConcurrent: 1 })
  async slowReport() {
    hit('slowReport');
    await state.gate?.promise;
    return { done: true };
  }

  @Get('recommendations')
  @Fallback('cachedRecommendations')
  recommendations() {
    throw new Error('recommender down');
  }

  cachedRecommendations(error: Error, context: ExecutionContext) {
    return { items: ['bestseller'], reason: error.message, path: context.switchToHttp().getRequest().url };
  }

  @Get('missing')
  @Fallback(() => ({ fallback: true }))
  missing() {
    throw new NotFoundException('No such product');
  }
}

@Controller('class-level')
@Timeout(50)
@Retry({ attempts: 2, backoff: fast })
class ClassLevelController {
  @Get('inherits')
  inherits() {
    if (hit('inherits') < 2) {
      throw new Error('first attempt fails');
    }
    return { ok: true };
  }

  @Get('override')
  @Timeout('1s')
  async override() {
    await new Promise((r) => setTimeout(r, 100));
    return { ok: true };
  }
}

@Controller('events')
@Timeout(150)
class EventsController {
  // The class-level Timeout covers the time to the first event (40 ms), not
  // the whole stream (160 ms).
  @Sse('stream')
  stream() {
    return interval(40).pipe(
      take(4),
      map((i) => ({ data: { i } })),
    );
  }
}

@Controller({ path: 'scoped', scope: Scope.REQUEST })
class ScopedController {
  @Get()
  @Retry({ attempts: 2, backoff: fast })
  get() {
    if (hit('scoped') < 2) {
      throw new Error('first attempt fails');
    }
    return { ok: true };
  }
}

@Controller('preset')
class PresetController {
  @Get()
  @Resilience('fragile-upstream')
  get() {
    if (hit('preset') < 2) {
      throw new Error('first attempt fails');
    }
    return { ok: true };
  }
}

@Controller('results')
class ResultsController {
  // Completes without a value. A preset's retryOnResult is for execute(): entrypoint results aren't retried.
  @Get()
  @Resilience('result-retry')
  empty() {
    hit('empty');
    return EMPTY;
  }
}

@Injectable()
class PartnerClient {
  private readonly policy: ResiliencePolicy;
  private readonly sync: ResiliencePolicy;

  constructor(resilience: ResilienceService) {
    this.policy = resilience.preset('partner-api');
    this.sync = resilience.create({ retry: { attempts: 10, backoff: { delay: 20, factor: 1 } } }, 'partner-sync');
  }

  fetch() {
    return this.policy.execute(() => {
      hit('partner');
      throw new Error('partner down');
    });
  }

  /** Retries for up to ~180 ms unless something cancels it. */
  syncAll() {
    return this.sync.execute(() => {
      hit('sync');
      throw new Error('partner flaky');
    });
  }
}

@Controller('partner')
class PartnerController {
  constructor(private readonly partner: PartnerClient) {}
  // No decorators: the error mapping still turns the service's CircuitOpenError into a 503.
  @Get()
  get() {
    return this.partner.fetch();
  }

  // The service's policy inherits this attempt's signal: the timeout stops its retries too.
  @Get('sync')
  @Timeout(50)
  sync() {
    return this.partner.syncAll();
  }
}

@Module({
  imports: [
    ResilienceModule.forRoot({
      presets: {
        'fragile-upstream': { retry: { attempts: 2, backoff: fast }, timeout: '1s' },
        'partner-api': { circuitBreaker: { minimumCalls: 1, openDuration: '30s' } },
        'result-retry': { retry: { attempts: 3, backoff: fast, retryOnResult: () => true } },
      },
    }),
  ],
  controllers: [
    CatalogController,
    ClassLevelController,
    EventsController,
    ScopedController,
    PresetController,
    PartnerController,
    ResultsController,
  ],
  providers: [CountingGuard, CatalogRepository, PartnerClient],
})
class HttpAppModule {}

describe.each(adapters.map((a) => a.name))('Resilience decorators over HTTP (%s)', (adapter) => {
  let app: INestApplication;
  let warnings: string[];

  beforeAll(async () => {
    const warn = vi.spyOn(Logger.prototype, 'warn').mockImplementation(() => undefined);
    app = await createApp(adapter, HttpAppModule, { setup: (a) => a.useLogger(false) });
    warnings = warn.mock.calls.map(([m]) => String(m));
    warn.mockRestore();
    app.get(ResilienceEvents).events$.subscribe((event) => state.events.push(event));
  });
  afterAll(() => app.close());
  beforeEach(() => {
    state.calls = {};
    state.guard = 0;
    state.signals = [];
    state.gate = undefined;
    state.events = [];
  });

  const http = () => request(app.getHttpServer());

  it('retries a failing GET: guards run once, the handler three times', async () => {
    const res = await http().get('/catalog/flaky').expect(200);
    expect(res.body).toEqual({ attempt: 3 }); // ResilienceContext.attempt in the handler
    expect(state.calls.flaky).toBe(3);
    expect(state.guard).toBe(1);
    expect(state.events.filter((e) => e.type === 'retry')).toEqual([
      expect.objectContaining({ policy: 'CatalogController.flaky', attempt: 1 }),
      expect.objectContaining({ policy: 'CatalogController.flaky', attempt: 2 }),
    ]);
  });

  it('does not retry a POST unless the handler is declared idempotent', async () => {
    await http().post('/catalog/orders').expect(500);
    expect(state.calls.createOrder).toBe(1);
    expect(warnings).toContainEqual(expect.stringContaining('@Retry() on CatalogController.createOrder() is inactive'));

    await http().post('/catalog/orders/upsert').expect(201, { upserted: true });
    expect(state.calls.upsertOrder).toBe(2);
  });

  it('times out with 504 and aborts the signal the handler received', async () => {
    const res = await http().get('/catalog/slow').expect(504);
    expect(res.body).toEqual({
      statusCode: 504,
      error: 'Gateway Timeout',
      message: 'The operation timed out',
      code: 'TIMEOUT',
    });
    expect(state.signals).toHaveLength(1);
    expect(state.signals[0].aborted).toBe(true);
    expect(state.signals[0].reason).toBeInstanceOf(ResilienceTimeoutError);
  });

  it('gives services the same signal through ResilienceContext', async () => {
    await http().get('/catalog/slow-repository').expect(504);
    expect(state.signals).toHaveLength(1);
    expect(state.signals[0].reason).toBeInstanceOf(ResilienceTimeoutError);
  });

  it("stops a service policy's retries when the handler times out", async () => {
    await http().get('/partner/sync').expect(504);
    const calls = state.calls.sync;
    expect(calls).toBeGreaterThanOrEqual(2);
    expect(calls).toBeLessThanOrEqual(4); // one attempt every 20 ms until the 50 ms timeout
    await new Promise((r) => setTimeout(r, 250)); // long enough for all ten attempts
    expect(state.calls.sync).toBe(calls);
    expect(state.events).not.toContainEqual(expect.objectContaining({ type: 'retry', attempt: 9 }));
  });

  it('opens a named breaker shared by two routes: 503 with Retry-After', async () => {
    await http().get('/catalog/stock').expect(500);
    await http().get('/catalog/stock').expect(500);
    const open = await http().get('/catalog/stock').expect(503);
    expect(open.headers['retry-after']).toBe('60');
    expect(open.body).toMatchObject({ statusCode: 503, code: 'CIRCUIT_OPEN' });
    expect(open.body.message).not.toContain('inventory'); // internals stay internal

    const other = await http().get('/catalog/reserve').expect(503);
    expect(other.body.code).toBe('CIRCUIT_OPEN');
    expect(state.calls).toEqual({ stock: 2 });
    expect(state.events).toContainEqual(
      expect.objectContaining({ type: 'circuit-open', policy: 'inventory', from: 'closed', to: 'open' }),
    );
  });

  it('rejects requests beyond the bulkhead with 503', async () => {
    state.gate = gate();
    const first = http().get('/catalog/report').then((r) => r);
    while (!state.calls.report) {
      await new Promise((r) => setTimeout(r, 5));
    }
    const second = await http().get('/catalog/report').expect(503);
    expect(second.body.code).toBe('BULKHEAD_FULL');
    expect(second.headers['retry-after']).toBeUndefined();
    state.gate.open();
    expect((await first).status).toBe(200);
    expect(state.calls.report).toBe(1);
  });

  it('frees the bulkhead slot when the attempt times out, although a handler ignoring its signal runs on', async () => {
    // At an entrypoint the timeout unsubscribes the attempt, which is what
    // ends it for the bulkhead: the handler's promise is out of reach. With
    // execute(), a slot stays taken until the function's promise settles.
    state.gate = gate();
    const res = await http().get('/catalog/report/slow').expect(504);
    expect(res.body.code).toBe('TIMEOUT');
    const bulkhead = app.get(ResilienceService).bulkhead('CatalogController.slowReport');
    expect(bulkhead.active).toBe(0);
    state.gate.open();
    expect(state.calls.slowReport).toBe(1);
  });

  it('returns the fallback method result, which gets the error and the ExecutionContext', async () => {
    const res = await http().get('/catalog/recommendations').expect(200);
    expect(res.body).toEqual({ items: ['bestseller'], reason: 'recommender down', path: '/catalog/recommendations' });
    expect(state.events).toContainEqual(expect.objectContaining({ type: 'fallback', policy: 'CatalogController.recommendations' }));
  });

  it('does not replace 4xx errors with the fallback', async () => {
    await http().get('/catalog/missing').expect(404);
  });

  it('applies class-level decorators to every route; the handler overrides per stage', async () => {
    await http().get('/class-level/inherits').expect(200);
    expect(state.calls.inherits).toBe(2);
    await http().get('/class-level/override').expect(200); // 100 ms > class timeout, < handler timeout
  });

  it('lets an SSE stream outlive the timeout once it produced its first event', async () => {
    const res = await http().get('/events/stream').buffer(true).expect(200);
    expect(res.text.match(/data: \{"i":\d\}/g)).toEqual(['data: {"i":0}', 'data: {"i":1}', 'data: {"i":2}', 'data: {"i":3}']);
  });

  it('works on request-scoped controllers', async () => {
    await http().get('/scoped').expect(200, { ok: true });
    expect(state.calls.scoped).toBe(2);
  });

  it('applies a named preset with @Resilience()', async () => {
    await http().get('/preset').expect(200);
    expect(state.calls.preset).toBe(2);
  });

  it("doesn't apply a preset's retryOnResult to an entrypoint", async () => {
    await http().get('/results').expect(500); // no value: EmptyError
    expect(state.calls.empty).toBe(1);
  });

  it('maps resilience errors thrown from services in undecorated handlers', async () => {
    await http().get('/partner').expect(500); // the plain error
    const res = await http().get('/partner').expect(503);
    expect(res.headers['retry-after']).toBe('30');
    expect(res.body.code).toBe('CIRCUIT_OPEN');
    expect(state.calls.partner).toBe(1);
  });
});
