/**
 * The stages on every transport the README lists (GraphQL, WebSockets, TCP
 * microservices, and hybrid apps), against a real HTTP dependency, with each
 * failure answered in the transport's own error shape.
 */
import type { AddressInfo, Server } from 'node:net';
import { ApolloDriver, type ApolloDriverConfig } from '@nestjs/apollo';
import { Controller, Get, Module, type INestApplication, type INestMicroservice } from '@nestjs/common';
import { Field, GraphQLModule, Int, Mutation, ObjectType, Query, ResolveField, Resolver } from '@nestjs/graphql';
import {
  ClientProxyFactory,
  EventPattern,
  MessagePattern,
  Payload,
  Transport,
  type ClientProxy,
} from '@nestjs/microservices';
import { WsAdapter } from '@nestjs/platform-ws';
import { Test } from '@nestjs/testing';
import { SubscribeMessage, WebSocketGateway } from '@nestjs/websockets';
import { firstValueFrom } from 'rxjs';
import request from 'supertest';
import { WebSocket } from 'ws';
import { adapters, createApp } from './support/adapters.js';
import {
  Bulkhead,
  CircuitBreaker,
  Fallback,
  Resilience,
  ResilienceModule,
  ResilienceService,
  Retry,
  Signal,
  Timeout,
  type ResiliencePreset,
} from '../lib/index.js';
import { Downstream, fast, send, until } from './downstream.js';

const downstream = new Downstream();

const presets: Record<string, ResiliencePreset> = {
  quota: { outboundRateLimit: { limit: 1, interval: '10s' } },
  warehouse: { timeout: 500, circuitBreaker: { minimumCalls: 2, failureRateThreshold: 50, openDuration: '20s' } },
};

const quote = async (path: string, signal?: AbortSignal) => (await downstream.call(path, signal)).n;

@ObjectType()
class Shipment {
  @Field(() => Int)
  id!: number;
}

// Class-level settings reach queries and mutations (mutations aren't retried), not field resolvers.
@Resolver(() => Shipment)
@Timeout('1s')
@Retry({ attempts: 3, backoff: fast })
class ShippingResolver {
  @Query(() => Int)
  @Retry({ attempts: 3, backoff: fast })
  quote(@Signal() signal: AbortSignal) {
    return quote('/gql/quote', signal);
  }

  @Mutation(() => Int, { nullable: true })
  @Retry({ attempts: 3, backoff: fast })
  book() {
    return quote('/gql/book');
  }

  @Query(() => Int, { nullable: true })
  @Timeout(250)
  slowQuote(@Signal() signal: AbortSignal) {
    return quote('/gql/slow', signal);
  }

  @Query(() => Int, { nullable: true })
  @CircuitBreaker({ minimumCalls: 1, openDuration: '10s' })
  brokenQuote() {
    return quote('/gql/broken');
  }

  @Query(() => Int, { nullable: true })
  @Bulkhead({ maxConcurrent: 1 })
  busyQuote() {
    return quote('/gql/busy');
  }

  @Query(() => Int, { nullable: true })
  @Resilience('quota')
  meteredQuote() {
    return quote('/gql/metered');
  }

  @Query(() => Int)
  @Fallback(() => 0)
  cachedQuote() {
    return quote('/gql/cached');
  }

  @Query(() => Shipment)
  shipment() {
    return { id: 1 };
  }

  @ResolveField(() => Int, { nullable: true })
  @Retry({ attempts: 2, backoff: fast })
  price() {
    return quote('/gql/price');
  }

  @ResolveField(() => Int, { nullable: true })
  eta() {
    return quote('/gql/eta');
  }
}

@Module({
  imports: [
    GraphQLModule.forRoot<ApolloDriverConfig>({
      driver: ApolloDriver,
      autoSchemaFile: true,
      fieldResolverEnhancers: ['interceptors'],
    }),
    ResilienceModule.forRoot({ presets }),
  ],
  providers: [ShippingResolver],
})
class GraphqlModule {}

describe('GraphQL resolvers against a real dependency (apollo on express)', () => {
  let app: INestApplication;

  beforeAll(async () => {
    await downstream.start();
    app = await createApp('express', GraphqlModule, { setup: (a) => a.useLogger(false) });
  });
  afterAll(async () => {
    await app.close();
    await downstream.stop();
  });
  beforeEach(() => downstream.reset());
  afterEach(() => downstream.release());

  const gql = (query: string) => request(app.getHttpServer()).post('/graphql').send({ query });

  it('retries a query until the dependency recovers', async () => {
    downstream.next('fail', 'fail');
    expect((await gql('{ quote }')).body).toEqual({ data: { quote: 3 } });
    expect(downstream.requests).toHaveLength(3);
  });

  it('calls the dependency once for a mutation', async () => {
    downstream.mode = 'fail';
    const res = await gql('mutation { book }');
    expect(res.body.data).toEqual({ book: null });
    expect(downstream.requests).toHaveLength(1);
  });

  it('reports a timeout in errors[].extensions and cancels the dependency call', async () => {
    downstream.mode = 'hang';
    const res = await gql('{ slowQuote }');
    expect(res.status).toBe(200);
    expect(res.body.errors).toEqual([
      expect.objectContaining({
        message: 'The operation timed out',
        path: ['slowQuote'],
        extensions: expect.objectContaining({ code: 'TIMEOUT', httpStatus: 504 }),
      }),
    ]);
    await until(() => downstream.requests[0]?.aborted, 'the dependency call cancelled');
  });

  it('reports an open breaker with retryAfter in seconds', async () => {
    downstream.mode = 'fail';
    await gql('{ brokenQuote }');
    const res = await gql('{ brokenQuote }');
    expect(res.body.errors[0].extensions).toMatchObject({ code: 'CIRCUIT_OPEN', httpStatus: 503, retryAfter: 10 });
    expect(downstream.requests).toHaveLength(1);
  });

  it('rejects a field beyond the bulkhead while a sibling field holds the slot', async () => {
    downstream.mode = 'hold';
    const response = send(gql('{ a: busyQuote b: busyQuote }'));
    await until(() => downstream.requests.length === 1, 'the first field to call the dependency');
    downstream.release();
    const res = await response;
    expect(res.body.data).toEqual({ a: 1, b: null });
    expect(res.body.errors).toEqual([
      expect.objectContaining({
        path: ['b'],
        extensions: expect.objectContaining({ code: 'BULKHEAD_FULL', httpStatus: 503 }),
      }),
    ]);
  });

  it("reports a preset's outbound rate limit with retryAfter", async () => {
    const res = await gql('{ a: meteredQuote b: meteredQuote }');
    expect(res.body.data).toEqual({ a: 1, b: null });
    expect(res.body.errors[0]).toMatchObject({
      path: ['b'],
      message: 'Rate limit of a dependency exceeded',
      extensions: { code: 'RATE_LIMITED', httpStatus: 503, retryAfter: 10 },
    });
    expect(downstream.requests).toHaveLength(1);
  });

  it('resolves the field with the fallback value', async () => {
    downstream.mode = 'fail';
    expect((await gql('{ cachedQuote }')).body).toEqual({ data: { cachedQuote: 0 } });
  });

  it("applies a field resolver's own @Retry() once interceptors are enabled for field resolvers", async () => {
    downstream.next('fail');
    expect((await gql('{ shipment { id price } }')).body).toEqual({ data: { shipment: { id: 1, price: 2 } } });
    expect(downstream.requests.map((r) => r.path)).toEqual(['/gql/price', '/gql/price']);
  });

  it("doesn't apply the class's @Retry() to a field resolver", async () => {
    downstream.next('fail');
    const res = await gql('{ shipment { id eta } }');
    expect(res.body.data).toEqual({ shipment: { id: 1, eta: null } });
    expect(downstream.requests).toHaveLength(1);
  });
});

@WebSocketGateway({ path: '/ws' })
class QuotesGateway {
  @SubscribeMessage('quote')
  @Retry({ attempts: 3, backoff: fast })
  async quote(@Signal() signal: AbortSignal) {
    return { event: 'quote', data: await quote('/ws/quote', signal) };
  }

  @SubscribeMessage('slow')
  @Timeout(250)
  slow(@Signal() signal: AbortSignal) {
    return quote('/ws/slow', signal);
  }

  @SubscribeMessage('broken')
  @CircuitBreaker({ minimumCalls: 1, openDuration: '10s' })
  broken() {
    return quote('/ws/broken');
  }

  @SubscribeMessage('busy')
  @Bulkhead({ maxConcurrent: 1 })
  async busy() {
    return { event: 'busy', data: await quote('/ws/busy') };
  }

  @SubscribeMessage('metered')
  @Resilience('quota')
  async metered() {
    return { event: 'metered', data: await quote('/ws/metered') };
  }

  @SubscribeMessage('cached')
  @Fallback(() => ({ event: 'cached', data: 0 }))
  async cached() {
    return { event: 'cached', data: await quote('/ws/cached') };
  }
}

@Module({ imports: [ResilienceModule.forRoot({ presets })], providers: [QuotesGateway] })
class WsModule {}

class WsClient {
  private readonly inbox: { event: string; data: any }[] = [];
  private readonly waiters: (() => void)[] = [];

  private constructor(readonly socket: WebSocket) {
    socket.on('message', (raw) => {
      this.inbox.push(JSON.parse(raw.toString()));
      for (const waiter of this.waiters.splice(0)) {
        waiter();
      }
    });
  }

  static async connect(url: string) {
    const socket = new WebSocket(url);
    await new Promise((resolve, reject) => socket.once('open', resolve).once('error', reject));
    return new WsClient(socket);
  }

  send(event: string) {
    this.socket.send(JSON.stringify({ event, data: {} }));
  }

  async next() {
    while (!this.inbox.length) {
      await new Promise<void>((resolve) => this.waiters.push(resolve));
    }
    return this.inbox.shift()!;
  }

  request(event: string) {
    this.send(event);
    return this.next();
  }
}

describe.each(adapters.map((a) => a.name))('WebSocket gateways against a real dependency (%s)', (adapter) => {
  let app: INestApplication;
  let client: WsClient;

  beforeAll(async () => {
    await downstream.start();
    app = await createApp(adapter, WsModule, {
      setup: (a) => {
        a.useLogger(false);
        a.useWebSocketAdapter(new WsAdapter(a));
      },
    });
    const { port } = app.getHttpServer().address() as AddressInfo;
    client = await WsClient.connect(`ws://127.0.0.1:${port}/ws`);
  });
  afterAll(async () => {
    client?.socket.close();
    await app.close();
    await downstream.stop();
  });
  beforeEach(() => downstream.reset());
  afterEach(() => downstream.release());

  it('retries a message when the handler opts in', async () => {
    downstream.next('fail', 'fail');
    expect(await client.request('quote')).toEqual({ event: 'quote', data: 3 });
  });

  it('answers a timeout with a TIMEOUT exception and cancels the dependency call', async () => {
    downstream.mode = 'hang';
    expect(await client.request('slow')).toEqual({
      event: 'exception',
      data: { status: 'error', code: 'TIMEOUT', statusCode: 504, message: 'The operation timed out' },
    });
    await until(() => downstream.requests[0]?.aborted, 'the dependency call cancelled');
  });

  it('answers an open breaker with CIRCUIT_OPEN and retryAfter', async () => {
    downstream.mode = 'fail';
    expect((await client.request('broken')).event).toBe('exception');
    expect(await client.request('broken')).toEqual({
      event: 'exception',
      data: {
        status: 'error',
        code: 'CIRCUIT_OPEN',
        statusCode: 503,
        message: 'Service temporarily unavailable',
        retryAfter: 10,
      },
    });
    expect(downstream.requests).toHaveLength(1);
  });

  it('answers a message beyond the bulkhead with BULKHEAD_FULL', async () => {
    downstream.mode = 'hold';
    client.send('busy');
    await until(() => downstream.requests.length === 1, 'the first message to call the dependency');
    expect((await client.request('busy')).data).toEqual({
      status: 'error',
      code: 'BULKHEAD_FULL',
      statusCode: 503,
      message: 'Server is at capacity',
    });
    downstream.release();
    expect(await client.next()).toEqual({ event: 'busy', data: 1 });
  });

  it("answers a preset's outbound rate limit with RATE_LIMITED and retryAfter", async () => {
    expect(await client.request('metered')).toEqual({ event: 'metered', data: 1 });
    expect((await client.request('metered')).data).toEqual({
      status: 'error',
      code: 'RATE_LIMITED',
      statusCode: 503,
      message: 'Rate limit of a dependency exceeded',
      retryAfter: 10,
    });
  });

  it('replies with the fallback', async () => {
    downstream.mode = 'fail';
    expect(await client.request('cached')).toEqual({ event: 'cached', data: 0 });
  });
});

const received: number[] = [];

@Controller()
class QuoteHandlers {
  @MessagePattern('quote')
  @Retry({ attempts: 3, backoff: fast })
  quote(@Signal() signal: AbortSignal) {
    return quote('/rpc/quote', signal);
  }

  @MessagePattern('slow')
  @Timeout(250)
  slow(@Signal() signal: AbortSignal) {
    return quote('/rpc/slow', signal);
  }

  @MessagePattern('broken')
  @CircuitBreaker({ minimumCalls: 1, openDuration: '10s' })
  broken() {
    return quote('/rpc/broken');
  }

  @MessagePattern('busy')
  @Bulkhead({ maxConcurrent: 1 })
  busy() {
    return quote('/rpc/busy');
  }

  @MessagePattern('metered')
  @Resilience('quota')
  metered() {
    return quote('/rpc/metered');
  }

  @MessagePattern('cached')
  @Fallback(() => 0)
  cached() {
    return quote('/rpc/cached');
  }

  @EventPattern('order.created')
  @Retry({ attempts: 3, backoff: fast })
  async orderCreated(@Payload() { id }: { id: number }) {
    await quote(`/rpc/orders/${id}`);
    received.push(id);
  }
}

@Module({ imports: [ResilienceModule.forRoot({ presets })], controllers: [QuoteHandlers] })
class RpcModule {}

describe('TCP microservice handlers against a real dependency', () => {
  let microservice: INestMicroservice;
  let client: ClientProxy;

  beforeAll(async () => {
    await downstream.start();
    const moduleRef = await Test.createTestingModule({ imports: [RpcModule] }).compile();
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
    await downstream.stop();
  });
  beforeEach(() => {
    downstream.reset();
    received.length = 0;
  });
  afterEach(() => downstream.release());

  const sendMessage = (pattern: string) => firstValueFrom(client.send(pattern, {}));
  const sendError = (pattern: string) =>
    sendMessage(pattern).then(
      () => {
        throw new Error('expected an error');
      },
      (error) => error,
    );

  it('retries a message handler that opts in', async () => {
    downstream.next('fail', 'fail');
    expect(await sendMessage('quote')).toBe(3);
  });

  it('answers a timeout with a TIMEOUT RpcException and cancels the dependency call', async () => {
    downstream.mode = 'hang';
    expect(await sendError('slow')).toEqual({
      status: 'error',
      code: 'TIMEOUT',
      statusCode: 504,
      message: 'The operation timed out',
    });
    await until(() => downstream.requests[0]?.aborted, 'the dependency call cancelled');
  });

  it('answers an open breaker with CIRCUIT_OPEN and retryAfter', async () => {
    downstream.mode = 'fail';
    expect(await sendError('broken')).toMatchObject({ message: 'Internal server error' });
    expect(await sendError('broken')).toEqual({
      status: 'error',
      code: 'CIRCUIT_OPEN',
      statusCode: 503,
      message: 'Service temporarily unavailable',
      retryAfter: 10,
    });
    expect(downstream.requests).toHaveLength(1);
  });

  it('answers a message beyond the bulkhead with BULKHEAD_FULL', async () => {
    downstream.mode = 'hold';
    const first = sendMessage('busy');
    await until(() => downstream.requests.length === 1, 'the first message to call the dependency');
    expect(await sendError('busy')).toEqual({
      status: 'error',
      code: 'BULKHEAD_FULL',
      statusCode: 503,
      message: 'Server is at capacity',
    });
    downstream.release();
    expect(await first).toBe(1);
  });

  it("answers a preset's outbound rate limit with RATE_LIMITED and retryAfter", async () => {
    expect(await sendMessage('metered')).toBe(1);
    expect(await sendError('metered')).toEqual({
      status: 'error',
      code: 'RATE_LIMITED',
      statusCode: 503,
      message: 'Rate limit of a dependency exceeded',
      retryAfter: 10,
    });
  });

  it('replies with the fallback value', async () => {
    downstream.mode = 'fail';
    expect(await sendMessage('cached')).toBe(0);
  });

  it('retries an event handler until the dependency takes the event', async () => {
    downstream.next('fail', 'fail');
    client.emit('order.created', { id: 7 });
    await until(() => received.length === 1, 'the event to be handled');
    expect(received).toEqual([7]);
    expect(downstream.requests.map((r) => r.path)).toEqual(['/rpc/orders/7', '/rpc/orders/7', '/rpc/orders/7']);
  });
});

@Controller('warehouse')
class WarehouseController {
  @Get('stock')
  @Resilience('warehouse')
  stock(@Signal() signal: AbortSignal) {
    return quote('/hybrid/http', signal);
  }

  @MessagePattern('warehouse.stock')
  @Resilience('warehouse')
  stockMessage(@Signal() signal: AbortSignal) {
    return quote('/hybrid/rpc', signal);
  }
}

@Module({ imports: [ResilienceModule.forRoot({ presets })], controllers: [WarehouseController] })
class HybridModule {}

describe.each(adapters.map((a) => a.name))('A hybrid app with inheritAppConfig (%s)', (adapter) => {
  let app: INestApplication;
  let client: ClientProxy;

  beforeAll(async () => {
    await downstream.start();
    app = await createApp(adapter, HybridModule, {
      setup: async (a) => {
        a.useLogger(false);
        a.connectMicroservice(
          { transport: Transport.TCP, options: { host: '127.0.0.1', port: 0 } },
          { inheritAppConfig: true },
        );
        await a.startAllMicroservices();
      },
    });
    const [microservice] = app.getMicroservices();
    const { port } = microservice.unwrap<Server>().address() as AddressInfo;
    client = ClientProxyFactory.create({ transport: Transport.TCP, options: { host: '127.0.0.1', port } });
    await client.connect();
  });
  afterAll(async () => {
    await client?.close();
    await app.close();
    await downstream.stop();
  });
  beforeEach(() => downstream.reset());

  it('shares one preset breaker between an HTTP route and a message handler', async () => {
    downstream.mode = 'fail';
    await request(app.getHttpServer()).get('/warehouse/stock').expect(500);
    await request(app.getHttpServer()).get('/warehouse/stock').expect(500);

    const error = await firstValueFrom(client.send('warehouse.stock', {})).catch((e) => e);
    expect(error).toMatchObject({ code: 'CIRCUIT_OPEN', statusCode: 503, retryAfter: 20 });
    expect(downstream.requests.map((r) => r.path)).toEqual(['/hybrid/http', '/hybrid/http']);
    expect(app.get(ResilienceService).circuitBreaker('warehouse').state).toBe('open');
  });

  it("applies the preset's timeout to the message handler, cancelling its dependency call", async () => {
    app.get(ResilienceService).circuitBreaker('warehouse').reset();
    downstream.mode = 'hang';
    const error = await firstValueFrom(client.send('warehouse.stock', {})).catch((e) => e);
    expect(error).toEqual({ status: 'error', code: 'TIMEOUT', statusCode: 504, message: 'The operation timed out' });
    await until(() => downstream.requests[0]?.aborted, 'the dependency call cancelled');
  });
});
