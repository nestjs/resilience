import { EventEmitter } from 'node:events';
import { ResilienceContext, RetryPolicy, ResilienceTimeoutError, TimeoutPolicy } from '../lib/index.js';

describe('ResilienceContext', () => {
  const context = new ResilienceContext();

  afterEach(() => vi.useRealTimers());

  it('is empty outside an attempt', () => {
    expect(context.signal).toBeUndefined();
    expect(context.attempt).toBeUndefined();
  });

  it('run() opens an attempt for code outside an entrypoint', () => {
    const signal = new AbortController().signal;
    expect(context.run({ signal }, () => [context.signal, context.attempt])).toEqual([signal, 1]);
    expect(context.run({ signal, attempt: 2 }, () => context.attempt)).toBe(2);
    expect(context.signal).toBeUndefined();
  });

  it('reports the attempt of execute() to the code it calls', async () => {
    const seen: Array<[number | undefined, boolean]> = [];
    const policy = new RetryPolicy({ attempts: 3, backoff: { delay: 0 } });
    await policy.execute(async ({ signal }) => {
      await Promise.resolve();
      seen.push([context.attempt, context.signal === signal]);
      if (seen.length < 3) {
        throw new Error('again');
      }
    });
    expect(seen).toEqual([
      [1, true],
      [2, true],
      [3, true],
    ]);
  });

  it("links execute() to the caller's attempt: when it is aborted, the policy stops", async () => {
    vi.useFakeTimers();
    const job = new AbortController();
    let calls = 0;
    const result = context.run({ signal: job.signal }, () =>
      new RetryPolicy({ attempts: 10, backoff: { delay: '1s', factor: 1 } })
        .execute(() => {
          calls++;
          throw new Error('flaky');
        })
        .catch((error: unknown) => error),
    );
    await vi.advanceTimersByTimeAsync(1_500);
    job.abort(new Error('shutting down'));
    expect(((await result) as Error).message).toBe('shutting down');
    await vi.advanceTimersByTimeAsync(10_000);
    expect(calls).toBe(2);
  });

  it("an attempt's timeout also aborts the policies executed inside it", async () => {
    vi.useFakeTimers();
    let inner: AbortSignal | undefined;
    const result = new TimeoutPolicy(100)
      .execute(() =>
        new RetryPolicy(2).execute(({ signal }) => {
          inner = signal;
          return new Promise(() => {});
        }),
      )
      .catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(100);
    expect(await result).toBeInstanceOf(ResilienceTimeoutError);
    expect(inner!.aborted).toBe(true);
    expect(inner!.reason).toBeInstanceOf(ResilienceTimeoutError);
  });

  it('an explicit signal replaces the inherited one', async () => {
    const outer = new AbortController();
    const own = new AbortController().signal;
    const seen = await context.run({ signal: outer.signal }, () =>
      new RetryPolicy().execute(({ signal }) => signal, { signal: own }),
    );
    outer.abort();
    expect(seen.aborted).toBe(false);
  });

  it("doesn't hand an ended attempt to code that outlived it, such as a connection opened during it", async () => {
    // Real timers: a timer callback runs in the async context it was created in.
    const connection = new EventEmitter();
    let heartbeat: NodeJS.Timeout | undefined;
    const first = await new TimeoutPolicy(20)
      .execute(({ signal }) => {
        // Opens a pooled connection: its callbacks run in this attempt's async context from now on.
        heartbeat ??= setInterval(() => connection.emit('data'), 5);
        return new Promise((_, reject) => signal.addEventListener('abort', () => reject(signal.reason)));
      })
      .catch((error: unknown) => error);
    expect(first).toBeInstanceOf(ResilienceTimeoutError);

    // Later work that runs on the connection's callbacks, for another caller.
    const later = await new Promise((resolve) =>
      connection.once('data', () => resolve([context.signal, new RetryPolicy(1).execute(() => 'ok').catch((e) => e)])),
    );
    clearInterval(heartbeat);
    const [signal, outcome] = later as [AbortSignal | undefined, Promise<unknown>];
    expect(signal).toBeUndefined();
    expect(await outcome).toBe('ok'); // not the first attempt's ResilienceTimeoutError
  });

  it('ends the attempt when its signal aborts, for a function that ignores the signal and keeps running', async () => {
    // Real timers: a timer callback runs in the async context it was created in.
    let later!: Promise<[number | undefined, boolean | undefined]>;
    const outcome = await new TimeoutPolicy(20)
      .execute(() => {
        later = new Promise((resolve) => setTimeout(() => resolve([context.attempt, context.signal?.aborted]), 60));
        return new Promise(() => {}); // never settles
      })
      .catch((error: unknown) => error);
    expect(outcome).toBeInstanceOf(ResilienceTimeoutError);
    // Its continuation belongs to no attempt: a policy it executes now starts
    // fresh instead of inheriting a signal aborted long ago (same rule as an
    // entrypoint handler that outlives its timeout).
    expect(await later).toEqual([undefined, undefined]);
  });

  it('keeps the scope of run() for everything started in it: the caller owns its signal', async () => {
    const job = new AbortController();
    // A tick scheduled by the job runs after run() returned, in the job's scope.
    const tick = context.run(
      { signal: job.signal },
      () => new Promise((resolve) => setTimeout(() => resolve(new RetryPolicy(1).execute(() => 'ran')), 5)),
    );
    job.abort(new Error('job cancelled'));
    await expect(tick).rejects.toThrow('job cancelled');
  });
});
