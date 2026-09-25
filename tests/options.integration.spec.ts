/**
 * Module options as a running app sees them: defaults and presets read from
 * configuration through forRootAsync(), the precedence of levels, options
 * overridden in a test, bad configuration failing the boot, and two apps in
 * one process sharing nothing.
 */
import { Controller, Get, Injectable, Module, Post, type INestApplication } from '@nestjs/common';
import request from 'supertest';
import { adapters, createApp } from './support/adapters.js';
import {
  Bulkhead,
  CircuitBreaker,
  RESILIENCE_MODULE_OPTIONS,
  Resilience,
  ResilienceEvents,
  ResilienceModule,
  ResilienceService,
  Retry,
  Signal,
  Timeout,
  type ResilienceEvent,
  type ResilienceModuleOptions,
} from '../lib/index.js';
import { Downstream, fast, send, until } from './downstream.js';

const downstream = new Downstream();

/** Stands in for a ConfigService: values as an environment would give them. */
@Injectable()
class ResilienceConfig {
  readonly env: Record<string, string | undefined> = {
    TIMEOUT_MS: '250',
    RETRY_ATTEMPTS: '2',
    OPEN_DURATION: '7s',
    CATALOG_TIMEOUT: '80ms',
  };
}

@Module({ providers: [ResilienceConfig], exports: [ResilienceConfig] })
class ConfigModule {}

const fromConfig = (config: ResilienceConfig): ResilienceModuleOptions => ({
  defaults: {
    timeout: Number(config.env.TIMEOUT_MS),
    retry: { attempts: Number(config.env.RETRY_ATTEMPTS), backoff: fast },
    circuitBreaker: { minimumCalls: 1, openDuration: config.env.OPEN_DURATION as `${number}s` },
    bulkhead: { maxConcurrent: 1 },
  },
  presets: {
    catalog: { retry: { attempts: 3, backoff: fast }, timeout: config.env.CATALOG_TIMEOUT as `${number}ms` },
  },
});

@Controller('defaults')
class DefaultsController {
  @Get('timeout')
  @Timeout()
  timeout(@Signal() signal: AbortSignal) {
    return downstream.call('/timeout', signal);
  }

  @Get('retry')
  @Retry()
  retry() {
    return downstream.call('/retry');
  }

  @Get('breaker')
  @CircuitBreaker()
  breaker() {
    return downstream.call('/breaker');
  }

  @Get('bulkhead')
  @Bulkhead()
  bulkhead() {
    return downstream.call('/bulkhead');
  }
}

@Controller('catalog')
@Resilience('catalog')
class CatalogController {
  // A bare @Timeout() under a preset takes the preset's duration, not defaults.timeout.
  @Get('slow')
  @Timeout()
  slow(@Signal() signal: AbortSignal) {
    return downstream.call('/slow', signal);
  }

  // The class preset's retry doesn't apply to a POST; the handler's declaration adds only `idempotent`.
  @Post('import')
  @Retry({ idempotent: true })
  import() {
    return downstream.call('/import');
  }

  @Post('export')
  export() {
    return downstream.call('/export');
  }
}

@Module({
  imports: [
    ResilienceModule.forRootAsync({ imports: [ConfigModule], inject: [ResilienceConfig], useFactory: fromConfig }),
  ],
  controllers: [DefaultsController, CatalogController],
})
class ConfiguredModule {}

describe.each(adapters.map((a) => a.name))('Options from configuration, through forRootAsync() (%s)', (adapter) => {
  let app: INestApplication;
  let events: ResilienceEvent[];

  beforeAll(async () => {
    await downstream.start();
    app = await createApp(adapter, ConfiguredModule, { setup: (a) => a.useLogger(false) });
    app.get(ResilienceEvents).events$.subscribe((event) => events.push(event));
  });
  afterAll(async () => {
    await app.close();
    await downstream.stop();
  });
  beforeEach(() => {
    events = [];
    downstream.reset();
  });
  afterEach(() => downstream.release());

  const http = () => request(app.getHttpServer());

  it('gives a bare @Timeout() defaults.timeout', async () => {
    downstream.mode = 'hang';
    await http().get('/defaults/timeout').expect(504);
    expect(events).toEqual([expect.objectContaining({ type: 'timeout', timeoutMs: 250 })]);
    await until(() => downstream.requests[0]?.aborted, 'the dependency call cancelled');
  });

  it('gives a bare @Retry() defaults.retry', async () => {
    downstream.mode = 'fail';
    await http().get('/defaults/retry').expect(500);
    expect(downstream.requests).toHaveLength(2);
  });

  it('gives a bare @CircuitBreaker() defaults.circuitBreaker', async () => {
    downstream.mode = 'fail';
    await http().get('/defaults/breaker').expect(500);
    const res = await http().get('/defaults/breaker').expect(503);
    expect(res.headers['retry-after']).toBe('7');
  });

  it('gives a bare @Bulkhead() defaults.bulkhead', async () => {
    downstream.mode = 'hold';
    const first = send(http().get('/defaults/bulkhead'));
    await until(() => downstream.requests.length === 1, 'the first call to hold the slot');
    await http().get('/defaults/bulkhead').expect(503);
    downstream.release();
    expect((await first).status).toBe(200);
  });

  it("takes a bare @Timeout()'s duration from the class's preset before defaults.timeout", async () => {
    downstream.mode = 'hang';
    await http().get('/catalog/slow').expect(504);
    expect(events.filter((e) => e.type === 'timeout')).toEqual([
      expect.objectContaining({ timeoutMs: 80 }),
      expect.objectContaining({ timeoutMs: 80 }),
      expect.objectContaining({ timeoutMs: 80 }),
    ]);
  });

  it("keeps the preset's attempts when a POST handler declares itself idempotent, and leaves other POSTs alone", async () => {
    downstream.mode = 'fail';
    await http().post('/catalog/import').expect(500);
    expect(downstream.requests).toHaveLength(3);

    downstream.reset();
    downstream.mode = 'fail';
    await http().post('/catalog/export').expect(500);
    expect(downstream.requests).toHaveLength(1);
  });
});

@Controller('partner')
class PartnerController {
  @Get()
  @Resilience('partner')
  get() {
    return downstream.call('/partner');
  }
}

@Module({
  imports: [
    ResilienceModule.forRoot({ presets: { partner: { circuitBreaker: { minimumCalls: 1, openDuration: '30s' } } } }),
  ],
  controllers: [PartnerController],
})
class PartnerModule {}

describe.each(adapters.map((a) => a.name))('Options and state per application (%s)', (adapter) => {
  beforeAll(() => downstream.start());
  afterAll(() => downstream.stop());
  beforeEach(() => downstream.reset());

  it('lets a test override the options through RESILIENCE_MODULE_OPTIONS', async () => {
    const app = await createApp(adapter, PartnerModule, {
      setup: (a) => a.useLogger(false),
      override: (builder) =>
        builder
          .overrideProvider(RESILIENCE_MODULE_OPTIONS)
          .useValue({ presets: { partner: { circuitBreaker: { minimumCalls: 1, openDuration: '2s' } } } }),
    });
    try {
      downstream.mode = 'fail';
      await request(app.getHttpServer()).get('/partner').expect(500);
      const res = await request(app.getHttpServer()).get('/partner').expect(503);
      expect(res.headers['retry-after']).toBe('2');
    } finally {
      await app.close();
    }
  });

  it('shares no breaker and no events between two apps in one process', async () => {
    const first = await createApp(adapter, PartnerModule, { setup: (a) => a.useLogger(false) });
    const second = await createApp(adapter, PartnerModule, { setup: (a) => a.useLogger(false) });
    try {
      const secondEvents: ResilienceEvent[] = [];
      second.get(ResilienceEvents).events$.subscribe((event) => secondEvents.push(event));
      downstream.mode = 'fail';
      await request(first.getHttpServer()).get('/partner').expect(500);
      await request(first.getHttpServer()).get('/partner').expect(503);

      expect(second.get(ResilienceService).circuitBreaker('partner').state).toBe('closed');
      downstream.mode = 'up';
      await request(second.getHttpServer()).get('/partner').expect(200);
      expect(secondEvents).toEqual([]);
    } finally {
      await first.close();
      await second.close();
    }
  });

  it('fails the boot on an invalid option from configuration, naming it', async () => {
    @Module({
      imports: [
        ResilienceModule.forRootAsync({
          useFactory: () => ({
            presets: { exports: { bulkhead: { maxConcurrent: Number(process.env.EXPORTS_UNSET_FOR_TEST) } } },
          }),
        }),
      ],
    })
    class MisconfiguredModule {}

    await expect(createApp(adapter, MisconfiguredModule, { setup: (a) => a.useLogger(false) })).rejects.toThrow(
      'presets.exports.bulkhead.maxConcurrent: Invalid value NaN',
    );
  });
});
