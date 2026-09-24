import type { AddressInfo } from 'node:net';
import { Module, type INestApplication } from '@nestjs/common';
import { WsAdapter } from '@nestjs/platform-ws';
import { SubscribeMessage, WebSocketGateway, WsException } from '@nestjs/websockets';
import { WebSocket } from 'ws';
import { adapters, createApp } from './support/adapters.js';
import {
  Bulkhead,
  CircuitBreaker,
  Fallback,
  Resilience,
  ResilienceModule,
  Retry,
  Signal,
  Timeout,
} from '../lib/index.js';

const fast = { delay: 1, factor: 1 };
const state = { calls: {} as Record<string, number>, signals: [] as AbortSignal[], release: () => {} };
const hit = (name: string) => (state.calls[name] = (state.calls[name] ?? 0) + 1);

@WebSocketGateway({ path: '/ws' })
class ChatGateway {
  @SubscribeMessage('flaky')
  @Retry({ attempts: 3, backoff: fast })
  flaky() {
    const n = hit('flaky');
    if (n < 3) {
      throw new Error('flaky');
    }
    return { event: 'flaky', data: { attempts: n } };
  }

  @SubscribeMessage('slow')
  @Timeout(30)
  async slow(@Signal() signal: AbortSignal) {
    state.signals.push(signal);
    await new Promise((_, reject) => signal.addEventListener('abort', () => reject(signal.reason)));
  }

  @SubscribeMessage('broken')
  @CircuitBreaker({ minimumCalls: 1, openDuration: 5_000 })
  broken() {
    hit('broken');
    throw new WsException('room service down');
  }

  @SubscribeMessage('busy')
  @Bulkhead({ maxConcurrent: 1 })
  async busy() {
    hit('busy');
    await new Promise<void>((resolve) => (state.release = resolve));
    return { event: 'busy', data: 'done' };
  }

  @SubscribeMessage('post')
  @Resilience('dependency')
  post() {
    hit('post');
    throw new WsException('chat store down');
  }

  @SubscribeMessage('history')
  @Fallback(() => ({ event: 'history', data: [] }))
  history() {
    throw new Error('history store down');
  }
}

@Module({
  imports: [ResilienceModule.forRoot({ presets: { dependency: { retry: { attempts: 3, backoff: fast } } } })],
  providers: [ChatGateway],
})
class WsAppModule {}

class Client {
  private readonly inbox: { event: string; data: any }[] = [];
  private readonly waiters: (() => void)[] = [];

  constructor(readonly socket: WebSocket) {
    socket.on('message', (raw) => {
      this.inbox.push(JSON.parse(raw.toString()));
      this.waiters.splice(0).forEach((w) => w());
    });
  }

  static async connect(url: string) {
    const socket = new WebSocket(url);
    await new Promise((resolve, reject) => socket.once('open', resolve).once('error', reject));
    return new Client(socket);
  }

  send(event: string, data: unknown = {}) {
    this.socket.send(JSON.stringify({ event, data }));
  }

  async next() {
    while (!this.inbox.length) {
      await new Promise<void>((resolve) => this.waiters.push(resolve));
    }
    return this.inbox.shift()!;
  }

  request(event: string, data?: unknown) {
    this.send(event, data);
    return this.next();
  }
}

describe.each(adapters.map((a) => a.name))('Resilience decorators on WebSocket gateways (%s)', (adapter) => {
  let app: INestApplication;
  let client: Client;

  beforeAll(async () => {
    app = await createApp(adapter, WsAppModule, {
      setup: (a) => {
        a.useLogger(false);
        a.useWebSocketAdapter(new WsAdapter(a));
      },
    });
    const { port } = app.getHttpServer().address() as AddressInfo;
    client = await Client.connect(`ws://127.0.0.1:${port}/ws`);
  });
  afterAll(async () => {
    client?.socket.close();
    await app.close();
  });
  beforeEach(() => {
    state.calls = {};
    state.signals = [];
  });

  it('retries a message handler', async () => {
    expect(await client.request('flaky')).toEqual({ event: 'flaky', data: { attempts: 3 } });
  });

  it('answers a timeout with a WsException payload', async () => {
    expect(await client.request('slow')).toEqual({
      event: 'exception',
      data: { status: 'error', code: 'TIMEOUT', statusCode: 504, message: 'The operation timed out' },
    });
    expect(state.signals[0].aborted).toBe(true);
  });

  it('answers an open breaker with CIRCUIT_OPEN', async () => {
    expect((await client.request('broken')).data).toMatchObject({ message: 'room service down' });
    expect((await client.request('broken')).data).toEqual({
      status: 'error',
      code: 'CIRCUIT_OPEN',
      statusCode: 503,
      message: 'Service temporarily unavailable',
      retryAfter: 5,
    });
    expect(state.calls.broken).toBe(1);
  });

  it('answers messages beyond the bulkhead with BULKHEAD_FULL', async () => {
    client.send('busy');
    while (!state.calls.busy) {
      await new Promise((r) => setTimeout(r, 5));
    }
    expect((await client.request('busy')).data).toMatchObject({ code: 'BULKHEAD_FULL', statusCode: 503 });
    state.release();
    expect(await client.next()).toEqual({ event: 'busy', data: 'done' });
  });

  it("does not apply a preset's retry to a gateway message", async () => {
    expect((await client.request('post')).data).toMatchObject({ message: 'chat store down' });
    expect(state.calls.post).toBe(1);
  });

  it('replies with the fallback', async () => {
    expect(await client.request('history')).toEqual({ event: 'history', data: [] });
  });
});
