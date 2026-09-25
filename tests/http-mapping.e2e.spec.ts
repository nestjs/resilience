import { Controller, Get, Injectable, Module, type INestApplication } from '@nestjs/common';
import { of } from 'rxjs';
import request from 'supertest';
import { adapters, createApp } from './support/adapters.js';
import {
  Bulkhead,
  BulkheadFullError,
  Fallback,
  OutboundRateLimitError,
  ResilienceEvents,
  ResilienceModule,
  Signal,
  Timeout,
} from '../lib/index.js';

function gate() {
  let open!: () => void;
  const promise = new Promise<void>((resolve) => (open = resolve));
  return { promise, open };
}

const state = { calls: {} as Record<string, number>, gate: undefined as ReturnType<typeof gate> | undefined };
const hit = (name: string) => (state.calls[name] = (state.calls[name] ?? 0) + 1);
const untilAborted = (signal: AbortSignal) =>
  new Promise((_, reject) => signal.addEventListener('abort', () => reject(signal.reason)));

@Injectable()
class ExportsClient {
  full(): never {
    throw new BulkheadFullError('exports-pool', 'full');
  }

  limited(): never {
    throw new OutboundRateLimitError('partner-quota', 1_500);
  }
}

@Controller('mapping')
class MappingController {
  constructor(private readonly exports: ExportsClient) {}

  @Get('full')
  full() {
    return this.exports.full();
  }

  @Get('limited')
  limited() {
    return this.exports.limited();
  }

  @Get('slow')
  @Timeout(30)
  slow(@Signal() signal: AbortSignal) {
    return untilAborted(signal);
  }
}

@Controller('fallbacks')
@Fallback(() => ({ from: 'class' }))
class FallbackController {
  @Get('class')
  fromClass() {
    throw new Error('down');
  }

  @Get('observable')
  @Fallback(() => of({ from: 'observable' }))
  observable() {
    throw new Error('down');
  }

  @Get('promise')
  @Fallback(async (error) => ({ from: 'promise', reason: (error as Error).message }))
  promise() {
    throw new Error('down');
  }

  @Get('timeout')
  @Timeout(30)
  timedOut(@Signal() signal: AbortSignal) {
    return untilAborted(signal);
  }

  @Get('only-timeouts')
  @Fallback(() => ({ from: 'handleIf' }), { handleIf: (error) => (error as Error).name === 'ResilienceTimeoutError' })
  onlyTimeouts() {
    throw new Error('not a timeout');
  }
}

@Controller('pools')
@Bulkhead({ maxConcurrent: 1 })
class PoolsController {
  @Get('a')
  async a() {
    hit('a');
    await state.gate?.promise;
    return { route: 'a' };
  }

  @Get('b')
  b() {
    return { route: 'b' };
  }
}

@Module({
  imports: [ResilienceModule.forRoot()],
  controllers: [MappingController, FallbackController, PoolsController],
  providers: [ExportsClient],
})
class MappingAppModule {}

@Module({
  imports: [ResilienceModule.forRoot({ mapErrors: false })],
  controllers: [MappingController],
  providers: [ExportsClient],
})
class UnmappedAppModule {}

describe.each(adapters.map((a) => a.name))('Resilience errors and fallbacks over HTTP (%s)', (adapter) => {
  let app: INestApplication;

  beforeAll(async () => {
    app = await createApp(adapter, MappingAppModule, { setup: (a) => a.useLogger(false) });
  });
  afterAll(() => app.close());
  beforeEach(() => {
    state.calls = {};
    state.gate = undefined;
  });

  const http = () => request(app.getHttpServer());

  it('answers a BulkheadFullError from a service with 503 and no Retry-After, hiding the bulkhead name', async () => {
    const res = await http().get('/mapping/full').expect(503);
    expect(res.body).toEqual({
      statusCode: 503,
      error: 'Service Unavailable',
      message: 'Server is at capacity',
      code: 'BULKHEAD_FULL',
    });
    expect(res.headers['retry-after']).toBeUndefined();
  });

  it('answers an OutboundRateLimitError with 503 and Retry-After rounded up to whole seconds', async () => {
    const res = await http().get('/mapping/limited').expect(503);
    expect(res.headers['retry-after']).toBe('2');
    expect(res.body).toMatchObject({ code: 'RATE_LIMITED', message: 'Rate limit of a dependency exceeded' });
    expect(JSON.stringify(res.body)).not.toContain('partner-quota');
  });

  it('applies a class-level @Fallback to every route; a handler-level one replaces it', async () => {
    await http().get('/fallbacks/class').expect(200, { from: 'class' });
    await http().get('/fallbacks/observable').expect(200, { from: 'observable' });
    await http().get('/fallbacks/promise').expect(200, { from: 'promise', reason: 'down' });
  });

  it('replaces a timeout with the fallback instead of answering 504', async () => {
    await http().get('/fallbacks/timeout').expect(200, { from: 'class' });
  });

  it("leaves errors the fallback's handleIf rejects to the exception filter", async () => {
    await http().get('/fallbacks/only-timeouts').expect(500);
  });

  it('gives each handler of a class-level @Bulkhead its own slots', async () => {
    state.gate = gate();
    const first = http().get('/pools/a').then((r) => r);
    while (!state.calls.a) {
      await new Promise((r) => setTimeout(r, 5));
    }
    await http().get('/pools/b').expect(200, { route: 'b' });
    expect((await http().get('/pools/a').expect(503)).body.code).toBe('BULKHEAD_FULL');
    state.gate.open();
    expect((await first).status).toBe(200);
  });
});

describe.each(adapters.map((a) => a.name))('Resilience with mapErrors: false over HTTP (%s)', (adapter) => {
  let app: INestApplication;

  beforeAll(async () => {
    app = await createApp(adapter, UnmappedAppModule, { setup: (a) => a.useLogger(false) });
  });
  afterAll(() => app.close());

  const http = () => request(app.getHttpServer());

  it('lets resilience errors reach the exception filters unmapped', async () => {
    const events: string[] = [];
    app.get(ResilienceEvents).events$.subscribe((event) => events.push(event.type));
    const timedOut = await http().get('/mapping/slow').expect(500);
    expect(timedOut.body).toEqual({ statusCode: 500, message: 'Internal server error' });
    const limited = await http().get('/mapping/limited').expect(500);
    expect(limited.headers['retry-after']).toBeUndefined();
    expect(events).toEqual(['timeout']); // the timeout itself still applied
  });
});
