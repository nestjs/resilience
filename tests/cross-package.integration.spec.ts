/**
 * What the README documents about `@nestjs/throttler` next to resilience: the throttler limits
 * incoming requests, so a request counts once however many attempts it takes, and its 429 never
 * reaches the pipeline.
 *
 * The family packages' own suites cover resilience next to them (they depend on this package,
 * so importing them here would be a cycle): idempotency's cross-package spec, and the
 * resilience specs of authentication and authorization.
 */
import { Controller, Get, Module, type INestApplication } from '@nestjs/common';
import { APP_GUARD } from '@nestjs/core';
import { ThrottlerGuard, ThrottlerModule } from '@nestjs/throttler';
import request from 'supertest';
import { adapters, createApp } from './support/adapters.js';
import { CircuitBreaker, ResilienceModule, ResilienceService, Retry, Signal } from '../lib/index.js';
import { Downstream, fast } from './downstream.js';

const downstream = new Downstream();

@Controller('catalog')
class CatalogController {
  @Get()
  @Retry({ attempts: 3, backoff: fast })
  @CircuitBreaker({ minimumCalls: 10 })
  list(@Signal() signal: AbortSignal) {
    return downstream.call('/catalog', signal);
  }
}

@Module({
  imports: [ThrottlerModule.forRoot([{ ttl: 60_000, limit: 2 }]), ResilienceModule.forRoot()],
  controllers: [CatalogController],
  providers: [{ provide: APP_GUARD, useClass: ThrottlerGuard }],
})
class ThrottledModule {}

describe.each(adapters.map((a) => a.name))('Next to @nestjs/throttler (%s)', (adapter) => {
  let app: INestApplication;

  beforeAll(async () => {
    await downstream.start();
    app = await createApp(adapter, ThrottledModule, { setup: (a) => a.useLogger(false) });
  });
  afterAll(async () => {
    await app.close();
    await downstream.stop();
  });

  it("counts a request once, however many attempts it takes; the throttler's 429 never reaches the pipeline", async () => {
    downstream.reset();
    downstream.next('fail', 'fail');
    await request(app.getHttpServer()).get('/catalog').expect(200); // three attempts, one request
    await request(app.getHttpServer()).get('/catalog').expect(200);

    const throttled = await request(app.getHttpServer()).get('/catalog').expect(429);
    expect(throttled.body.code).toBeUndefined();
    expect(downstream.requests).toHaveLength(4);
    // Every attempt is recorded; the throttled request is not.
    expect(app.get(ResilienceService).circuitBreaker('CatalogController.list').stats).toMatchObject({
      total: 4,
      failures: 2,
    });
  });
});
