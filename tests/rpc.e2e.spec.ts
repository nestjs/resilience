import { Controller, Get, Inject, Module, type INestApplication, type INestMicroservice } from '@nestjs/common';
import {
  ClientProxyFactory,
  EventPattern,
  MessagePattern,
  Payload,
  Transport,
  type ClientProxy,
} from '@nestjs/microservices';
import { Test } from '@nestjs/testing';
import type { AddressInfo, Server } from 'node:net';
import { firstValueFrom, switchMap } from 'rxjs';
import request from 'supertest';
import { createApp } from './support/adapters.js';
import {
  CircuitBreaker,
  Fallback,
  Resilience,
  ResilienceModule,
  ResilienceService,
  Retry,
  Signal,
  Timeout,
  ResilienceTimeoutError,
  type ResiliencePolicy,
} from '../lib/index.js';

const fast = { delay: 1, factor: 1 };
const state = { calls: {} as Record<string, number>, events: [] as unknown[], signals: [] as AbortSignal[] };
const hit = (name: string) => (state.calls[name] = (state.calls[name] ?? 0) + 1);

@Controller()
class InventoryHandlers {
  @MessagePattern('inventory.flaky')
  @Retry({ attempts: 3, backoff: fast })
  flaky() {
    if (hit('flaky') < 3) {
      throw new Error('flaky');
    }
    return { ok: true };
  }

  @MessagePattern('inventory.slow')
  @Timeout(30)
  async slow(@Signal() signal: AbortSignal) {
    state.signals.push(signal);
    await new Promise((_, reject) => signal.addEventListener('abort', () => reject(signal.reason)));
  }

  @MessagePattern('inventory.broken')
  @CircuitBreaker({ minimumCalls: 1, openDuration: 5_000 })
  broken() {
    hit('broken');
    throw new Error('warehouse down');
  }

  @MessagePattern('inventory.stock')
  @Fallback(() => ({ stock: 0, stale: true }))
  stock() {
    throw new Error('warehouse down');
  }

  // Events have no reply; retries make at-least-once handling cheaper.
  @EventPattern('order.created')
  @Retry({ attempts: 3, backoff: fast })
  orderCreated(@Payload() data: { orderId: number }) {
    if (hit('orderCreated') < 3) {
      throw new Error('projection store down');
    }
    state.events.push(data);
  }
}

// A preset written with HTTP GETs in mind, reused on message handlers.
@Controller()
@Resilience('dependency')
class PaymentHandlers {
  @MessagePattern('payments.charge')
  charge() {
    hit('charge');
    throw new Error('card network down');
  }

  @MessagePattern('payments.status')
  @Retry()
  status() {
    if (hit('status') < 4) {
      throw new Error('flaky');
    }
    return { ok: true };
  }
}

@Controller()
@Retry({ attempts: 3, backoff: fast })
class LedgerHandlers {
  @MessagePattern('ledger.append')
  append() {
    hit('append');
    throw new Error('ledger down');
  }
}

@Module({
  imports: [
    ResilienceModule.forRoot({ presets: { dependency: { retry: { attempts: 4, backoff: fast }, timeout: '1s' } } }),
  ],
  controllers: [InventoryHandlers, PaymentHandlers, LedgerHandlers],
})
class RpcAppModule {}

describe('Resilience decorators on microservice handlers (TCP)', () => {
  let microservice: INestMicroservice;
  let client: ClientProxy;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({ imports: [RpcAppModule] }).compile();
    microservice = moduleRef.createNestMicroservice({
      transport: Transport.TCP,
      options: { host: '127.0.0.1', port: 0 },
    });
    microservice.useLogger(false);
    await microservice.listen();
    const { port } = microservice.unwrap<Server>().address() as AddressInfo;
    client = ClientProxyFactory.create({ transport: Transport.TCP, options: { host: '127.0.0.1', port } });
    await client.connect();
  });
  afterAll(async () => {
    await client?.close();
    await microservice?.close();
  });
  beforeEach(() => {
    state.calls = {};
    state.events = [];
    state.signals = [];
  });

  const send = (pattern: string, data: unknown = {}) => firstValueFrom(client.send(pattern, data));
  const sendError = (pattern: string, data: unknown = {}) =>
    send(pattern, data).then(
      () => {
        throw new Error('expected an error');
      },
      (error) => error,
    );

  it('retries a message handler', async () => {
    expect(await send('inventory.flaky')).toEqual({ ok: true });
    expect(state.calls.flaky).toBe(3);
  });

  it('answers a timeout with a structured RpcException', async () => {
    expect(await sendError('inventory.slow')).toEqual({
      status: 'error',
      code: 'TIMEOUT',
      statusCode: 504,
      message: 'The operation timed out',
    });
    expect(state.signals[0].reason).toBeInstanceOf(ResilienceTimeoutError);
  });

  it('answers an open breaker with CIRCUIT_OPEN and retryAfter', async () => {
    expect(await sendError('inventory.broken')).toMatchObject({ message: 'Internal server error' });
    expect(await sendError('inventory.broken')).toEqual({
      status: 'error',
      code: 'CIRCUIT_OPEN',
      statusCode: 503,
      message: 'Service temporarily unavailable',
      retryAfter: 5,
    });
    expect(state.calls.broken).toBe(1);
  });

  it('replies with the fallback value', async () => {
    expect(await send('inventory.stock')).toEqual({ stock: 0, stale: true });
  });

  it("does not apply a preset's retry to a message handler", async () => {
    expect(await sendError('payments.charge')).toMatchObject({ message: 'Internal server error' });
    expect(state.calls.charge).toBe(1);
  });

  it('does not apply a class-level @Retry() to a message handler', async () => {
    expect(await sendError('ledger.append')).toMatchObject({ message: 'Internal server error' });
    expect(state.calls.append).toBe(1);
  });

  it("retries when the handler opts in with @Retry(), using the preset's options", async () => {
    expect(await send('payments.status')).toEqual({ ok: true });
    expect(state.calls.status).toBe(4); // the preset's attempts, not the default 3
  });

  it('retries event handlers', async () => {
    client.emit('order.created', { orderId: 7 });
    const start = Date.now();
    while (!state.events.length && Date.now() - start < 2_000) {
      await new Promise((r) => setTimeout(r, 5));
    }
    expect(state.events).toEqual([{ orderId: 7 }]);
    expect(state.calls.orderCreated).toBe(3);
  });
});

// A gateway calling another service through a ClientProxy. The proxy opens
// its TCP connection during the first request's attempt, and runs every later
// response callback in that attempt's async context.
@Controller()
class PricingHandlers {
  @MessagePattern('price')
  price(@Payload() { delay }: { delay: number }) {
    return new Promise((resolve) => setTimeout(() => resolve(42), delay));
  }
}

@Module({ controllers: [PricingHandlers] })
class PricingModule {}

const pricing = { port: 0, delay: 0 };

@Controller('quotes')
class QuotesController {
  private readonly policy: ResiliencePolicy;

  constructor(
    @Inject('PRICING') private readonly client: ClientProxy,
    resilience: ResilienceService,
  ) {
    this.policy = resilience.create({ retry: { attempts: 2, backoff: fast } }, 'quotes');
  }

  @Get()
  @Timeout(100)
  quote() {
    return this.client
      .send<number>('price', { delay: pricing.delay })
      .pipe(switchMap((price) => this.policy.execute(() => ({ price }))));
  }
}

@Module({
  imports: [ResilienceModule.forRoot()],
  controllers: [QuotesController],
  providers: [
    {
      provide: 'PRICING',
      useFactory: () =>
        ClientProxyFactory.create({ transport: Transport.TCP, options: { host: '127.0.0.1', port: pricing.port } }),
    },
  ],
})
class GatewayModule {}

describe('A policy used on a ClientProxy response, after an earlier request timed out', () => {
  let remote: INestMicroservice;
  let gateway: INestApplication;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({ imports: [PricingModule] }).compile();
    remote = moduleRef.createNestMicroservice({ transport: Transport.TCP, options: { host: '127.0.0.1', port: 0 } });
    remote.useLogger(false);
    await remote.listen();
    pricing.port = (remote.unwrap<Server>().address() as AddressInfo).port;
    gateway = await createApp('express', GatewayModule, { setup: (a) => a.useLogger(false) });
  });
  afterAll(async () => {
    await gateway?.close();
    await remote?.close();
  });

  it("doesn't inherit the signal of the request that opened the connection", async () => {
    pricing.delay = 300; // the first request times out, and it opened the connection
    await request(gateway.getHttpServer()).get('/quotes').expect(504);
    await new Promise((resolve) => setTimeout(resolve, 300)); // its late answer arrives

    pricing.delay = 0;
    const res = await request(gateway.getHttpServer()).get('/quotes').expect(200);
    expect(res.body).toEqual({ price: 42 });
  });
});
