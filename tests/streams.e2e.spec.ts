/**
 * Handlers whose response goes out in pieces: `@Sse()` streams (Nest sends
 * their headers before the first event) and `@Res()` handlers writing their
 * own response. Checked on both adapters, which report "headers sent"
 * differently.
 */
import { get, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import {
  Catch,
  Controller,
  Get,
  Logger,
  Module,
  Res,
  Sse,
  type ArgumentsHost,
  type INestApplication,
} from '@nestjs/common';
import { APP_FILTER, BaseExceptionFilter } from '@nestjs/core';
import { concat, map, mergeMap, NEVER, of, throwError, timer } from 'rxjs';
import request from 'supertest';
import { adapters, createApp } from './support/adapters.js';
import { Bulkhead, Fallback, ResilienceModule, ResilienceService, Retry, Timeout } from '../lib/index.js';

const fast = { delay: 1, factor: 1 };
const state = { calls: {} as Record<string, number> };
const hit = (name: string) => (state.calls[name] = (state.calls[name] ?? 0) + 1);

@Controller('streams')
class StreamsController {
  // No event within the budget: the stream ends with an SSE error event.
  @Sse('silent')
  @Timeout(50)
  silent() {
    return NEVER;
  }

  // The first event would come too late: the fallback's event goes out instead.
  @Sse('late')
  @Timeout(50)
  @Fallback(() => of({ data: 'cached' }))
  late() {
    return timer(1_000).pipe(map(() => ({ data: 'live' })));
  }

  // Fails before its first event, after Nest sent the SSE headers: retried.
  @Sse('flaky')
  @Retry({ attempts: 2, backoff: fast })
  flaky() {
    return hit('flaky') === 1
      ? timer(20).pipe(mergeMap(() => throwError(() => new Error('flaky'))))
      : of({ data: 'ok' });
  }

  // Fails after its first event: the client has part of the stream, so it is neither retried nor replaced.
  @Sse('broken')
  @Retry({ attempts: 2, backoff: fast })
  @Fallback(() => of({ data: 'cached' }))
  broken() {
    hit('broken');
    return concat(
      of({ data: 'first' }),
      timer(20).pipe(mergeMap(() => throwError(() => new Error('mid-stream')))),
    );
  }

  // Holds its only slot until the client goes away.
  @Sse('held')
  @Bulkhead({ maxConcurrent: 1 })
  held() {
    hit('held');
    return concat(of({ data: 'hello' }), NEVER);
  }

  // Writes its own response, then fails: the response is committed, so it is not retried.
  @Get('raw')
  @Retry({ attempts: 3, backoff: fast })
  raw(@Res() response: ServerResponse & { raw?: ServerResponse }) {
    hit('raw');
    const raw = response.raw ?? response;
    raw.writeHead(200, { 'content-type': 'text/plain' });
    raw.write('partial');
    throw new Error('failed mid-response');
  }

  // Sends its own response, so a fallback's result would go nowhere: the fallback is left out.
  @Get('manual')
  @Fallback(() => ({ fallback: true }))
  manual(@Res() _response: unknown) {
    throw new Error('failed before responding');
  }
}

/** Ends a response whose headers already went out, instead of trying to send a 500. */
@Catch()
class EndCommittedResponses extends BaseExceptionFilter {
  catch(exception: unknown, host: ArgumentsHost) {
    const response = host.switchToHttp().getResponse();
    const raw: ServerResponse = response.raw ?? response;
    if (raw.headersSent) {
      return void raw.end();
    }
    super.catch(exception, host);
  }
}

@Module({
  imports: [ResilienceModule.forRoot()],
  controllers: [StreamsController],
  providers: [{ provide: APP_FILTER, useClass: EndCommittedResponses }],
})
class StreamsAppModule {}

describe.each(adapters.map((a) => a.name))('Streams and manual responses (%s)', (adapter) => {
  let app: INestApplication;
  let warnings: string[];

  beforeAll(async () => {
    const warn = vi.spyOn(Logger.prototype, 'warn').mockImplementation(() => undefined);
    app = await createApp(adapter, StreamsAppModule, { setup: (a) => a.useLogger(false) });
    warnings = warn.mock.calls.map(([m]) => String(m));
    warn.mockRestore();
  });
  afterAll(() => app.close());
  beforeEach(() => {
    state.calls = {};
  });

  const sse = (path: string) => request(app.getHttpServer()).get(path).buffer(true).timeout(2_000);

  it('times out an SSE stream that sends no event within the budget', async () => {
    const started = performance.now();
    const res = await sse('/streams/silent');
    expect(performance.now() - started).toBeLessThan(500);
    expect(res.text).toContain('event: error\n');
    expect(res.text).toContain('data: The operation timed out\n');
  });

  it("sends the fallback's events when the first event would come too late", async () => {
    const res = await sse('/streams/late');
    expect(res.text).toContain('data: cached\n');
    expect(res.text).not.toContain('live');
  });

  it('retries an SSE handler that fails before its first event', async () => {
    const res = await sse('/streams/flaky');
    expect(res.text).toContain('data: ok\n');
    expect(res.text).not.toContain('event: error');
    expect(state.calls.flaky).toBe(2);
  });

  it('neither retries nor replaces a stream that failed after its first event', async () => {
    const res = await sse('/streams/broken');
    expect(res.text).toContain('data: first\n');
    expect(res.text).toContain('event: error\n');
    expect(res.text).not.toContain('cached');
    expect(state.calls.broken).toBe(1);
  });

  it("frees a stream's bulkhead slot when its client disconnects", async () => {
    const bulkhead = app.get(ResilienceService).bulkhead('StreamsController.held');
    const { port } = app.getHttpServer().address() as AddressInfo;
    const client = get({ host: '127.0.0.1', port, path: '/streams/held' });
    await new Promise<void>((resolve) => client.on('response', (res) => res.once('data', () => resolve())));
    expect(bulkhead.active).toBe(1);
    expect((await sse('/streams/held')).status).toBe(503); // the only slot is taken

    client.destroy();
    const deadline = performance.now() + 2_000;
    while (bulkhead.active > 0 && performance.now() < deadline) {
      await new Promise((r) => setTimeout(r, 5));
    }
    expect(bulkhead.active).toBe(0);
  });

  it('does not retry a handler that already wrote its own response headers', async () => {
    const res = await request(app.getHttpServer()).get('/streams/raw').timeout(2_000);
    expect(res.status).toBe(200);
    expect(res.text).toBe('partial');
    expect(state.calls.raw).toBe(1);
  });

  it('leaves out a fallback on a @Res() handler, so the error reaches the client, and warns', async () => {
    const res = await request(app.getHttpServer()).get('/streams/manual').timeout(2_000);
    expect(res.status).toBe(500);
    expect(warnings).toContainEqual(
      '@Fallback() on StreamsController.manual() has no effect: the handler sends its own response ' +
        'through @Res() or @Next(). Use @Res({ passthrough: true }) to let Nest send the fallback result.',
    );
  });
});
