import { BadRequestException, HttpException, InternalServerErrorException } from '@nestjs/common';
import {
  BulkheadFullError,
  BulkheadPolicy,
  CircuitOpenError,
  FallbackPolicy,
  ResiliencePolicy,
  ResilienceTimeoutError,
  RetryPolicy,
  TimeoutPolicy,
} from '../lib/index.js';
import { recordEvents } from './events.js';

const down = () => Promise.reject(new Error('down'));

describe('FallbackPolicy', () => {
  afterEach(() => vi.useRealTimers());

  it('returns the call result untouched when it succeeds, without calling the handler', async () => {
    const handler = vi.fn(() => 'fallback');
    expect(await new FallbackPolicy(handler).execute(() => 'live')).toBe('live');
    expect(handler).not.toHaveBeenCalled();
  });

  it('replaces a failure with the handler result, passing the error and the attempt context', async () => {
    const events = recordEvents();
    const handler = vi.fn((_error: unknown, _context: { signal: AbortSignal; attempt: number }) => ['cached']);
    const error = new Error('down');
    const policy = new FallbackPolicy(handler, { name: 'catalog' });

    expect(await policy.execute(() => Promise.reject(error), { source: 'Jobs.sync' })).toEqual(['cached']);
    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler.mock.calls[0][0]).toBe(error);
    expect(handler.mock.calls[0][1]).toMatchObject({ attempt: 1 });
    expect(handler.mock.calls[0][1].signal).toBeInstanceOf(AbortSignal);
    expect(events).toEqual([{ type: 'fallback', policy: 'catalog', source: 'Jobs.sync', error }]);
  });

  it('waits for a handler that returns a Promise', async () => {
    const policy = new FallbackPolicy(async () => {
      await Promise.resolve();
      return 'from cache';
    });
    expect(await policy.execute(down)).toBe('from cache');
  });

  it('turns a synchronous throw of the call into a handled failure', async () => {
    const policy = new FallbackPolicy(() => 'fallback');
    expect(
      await policy.execute(() => {
        throw new Error('sync');
      }),
    ).toBe('fallback');
  });

  it('lets an error thrown by the handler reach the caller', async () => {
    const policy = new FallbackPolicy(() => {
      throw new Error('cache also down');
    });
    await expect(policy.execute(down)).rejects.toThrow('cache also down');
  });

  it('does not replace client errors by default, but does replace 5xx HttpExceptions', async () => {
    const policy = new FallbackPolicy(() => 'fallback');
    const badRequest = new BadRequestException();
    await expect(policy.execute(() => Promise.reject(badRequest))).rejects.toBe(badRequest);
    await expect(
      policy.execute(() => Promise.reject(Object.assign(new Error('unprocessable'), { statusCode: 422 }))),
    ).rejects.toThrow('unprocessable');
    expect(await policy.execute(() => Promise.reject(new InternalServerErrorException()))).toBe('fallback');
    expect(await policy.execute(() => Promise.reject(Object.assign(new Error('slow down'), { status: 429 })))).toBe(
      'fallback',
    );
  });

  it('replaces rejections from inner policies, such as an open breaker or a full bulkhead', async () => {
    const policy = new FallbackPolicy((error) => (error as Error).name);
    expect(await policy.execute(() => Promise.reject(new CircuitOpenError('b', 1_000)))).toBe('CircuitOpenError');
    expect(await policy.execute(() => Promise.reject(new BulkheadFullError('b', 'full')))).toBe('BulkheadFullError');
  });

  it('replaces only the errors handleIf accepts; a custom handleIf replaces the default', async () => {
    const policy = new FallbackPolicy(() => 'fallback', {
      handleIf: (error) => error instanceof HttpException,
    });
    await expect(policy.execute(down)).rejects.toThrow('down');
    expect(await policy.execute(() => Promise.reject(new BadRequestException()))).toBe('fallback');
  });

  it('surfaces a handleIf that returns a Promise instead of a boolean', async () => {
    const handleIf = (async () => true) as unknown as (error: unknown) => boolean;
    const handler = vi.fn(() => 'fallback');
    await expect(new FallbackPolicy(handler, { handleIf }).execute(down)).rejects.toThrow(
      'handleIf returned a Promise. It must return a boolean, synchronously.',
    );
    expect(handler).not.toHaveBeenCalled();
  });

  it('is not applied when the caller aborted: the abort reason reaches the caller', async () => {
    const controller = new AbortController();
    const handler = vi.fn(() => 'fallback');
    const result = new FallbackPolicy(handler).execute(
      ({ signal }) =>
        new Promise((_, reject) => signal.addEventListener('abort', () => reject(signal.reason))),
      { signal: controller.signal },
    );
    controller.abort(new Error('caller gave up'));
    await expect(result).rejects.toThrow('caller gave up');
    expect(handler).not.toHaveBeenCalled();
  });

  it('replaces the timeout of an inner TimeoutPolicy: only the attempt signal aborted, not the caller', async () => {
    vi.useFakeTimers();
    let received: unknown;
    const policy = ResiliencePolicy.wrap(
      new FallbackPolicy((error) => {
        received = error;
        return { stale: true };
      }),
      new TimeoutPolicy({ timeout: 100, name: 'partner' }),
    );
    const result = policy.execute(() => new Promise(() => {}));
    await vi.advanceTimersByTimeAsync(100);
    expect(await result).toEqual({ stale: true });
    expect(received).toBeInstanceOf(ResilienceTimeoutError);
    expect(received).toMatchObject({ timeoutMs: 100, policy: 'partner' });
  });

  it('outside Retry, runs once with the last error after every attempt failed', async () => {
    const handler = vi.fn((error: unknown) => `fallback after ${(error as Error).message}`);
    let calls = 0;
    const policy = ResiliencePolicy.wrap(
      new FallbackPolicy(handler),
      new RetryPolicy({ attempts: 3, backoff: { delay: 0 } }),
    );
    expect(await policy.execute(() => Promise.reject(new Error(`failure ${++calls}`)))).toBe(
      'fallback after failure 3',
    );
    expect(handler).toHaveBeenCalledTimes(1);
  });

  it('inside Retry, turns every failed attempt into a success, so nothing is retried', async () => {
    let calls = 0;
    const policy = ResiliencePolicy.wrap(
      new RetryPolicy({ attempts: 3, backoff: { delay: 0 } }),
      new FallbackPolicy(() => 'fallback'),
    );
    expect(
      await policy.execute(() => {
        calls++;
        return down();
      }),
    ).toBe('fallback');
    expect(calls).toBe(1);
  });

  it('replaces a queue timeout of an inner bulkhead', async () => {
    vi.useFakeTimers();
    const bulkhead = new BulkheadPolicy({ maxConcurrent: 1, maxQueue: 1, queueTimeout: 50 });
    let release!: () => void;
    const running = bulkhead.execute(() => new Promise<void>((resolve) => (release = resolve)));
    const policy = ResiliencePolicy.wrap(
      new FallbackPolicy((error) => (error as BulkheadFullError).reason),
      bulkhead,
    );
    const queued = policy.execute(() => 'ran');
    await vi.advanceTimersByTimeAsync(50);
    expect(await queued).toBe('queue-timeout');
    release();
    await running;
  });

  it('can be reused after a fallback: the next execution runs the call again and leaves no timer behind', async () => {
    vi.useFakeTimers();
    const policy = ResiliencePolicy.wrap(new FallbackPolicy(() => 'fallback'), new TimeoutPolicy(100));
    const first = policy.execute(() => new Promise(() => {}));
    await vi.advanceTimersByTimeAsync(100);
    expect(await first).toBe('fallback');
    expect(await policy.execute(() => 'fresh')).toBe('fresh');
    expect(vi.getTimerCount()).toBe(0);
  });
});
