import { PolicyWrap, type Execution, type Next } from '../lib/policies/resilience.policy.js';
import {
  BulkheadPolicy,
  CircuitBreakerPolicy,
  FallbackPolicy,
  ResiliencePolicy,
  RetryPolicy,
  TimeoutPolicy,
} from '../lib/index.js';

describe('ResiliencePolicy.wrap', () => {
  afterEach(() => vi.useRealTimers());

  it('is named after its policies, outermost first', () => {
    const policy = ResiliencePolicy.wrap(
      new RetryPolicy({ name: 'partner' }),
      new TimeoutPolicy({ timeout: '1s', name: 'partner-timeout' }),
    );
    expect(policy.name).toBe('partner > partner-timeout');
  });

  it('with no policies, runs the call as is', async () => {
    const policy = ResiliencePolicy.wrap();
    expect(policy.name).toBe('noop');
    expect(await policy.execute(({ attempt }) => `attempt ${attempt}`)).toBe('attempt 1');
    await expect(policy.execute(() => Promise.reject(new Error('down')))).rejects.toThrow('down');
  });

  it('flattens nested wraps into one pipeline in the same order', () => {
    const fallback = new FallbackPolicy(() => null);
    const retry = new RetryPolicy();
    const breaker = new CircuitBreakerPolicy();
    const timeout = new TimeoutPolicy(100);
    const nested = ResiliencePolicy.wrap(fallback, ResiliencePolicy.wrap(retry, ResiliencePolicy.wrap(breaker)), timeout);
    expect((nested as PolicyWrap).policies).toEqual([fallback, retry, breaker, timeout]);
  });

  it('runs the policies in argument order, the first one outermost', async () => {
    const order: string[] = [];
    class Tracing extends ResiliencePolicy {
      async run<T>(next: Next<T>, execution: Execution): Promise<T> {
        order.push(`${this.name} in`);
        try {
          return await next(execution);
        } finally {
          order.push(`${this.name} out`);
        }
      }
    }
    const tracing = (name: string) => new Tracing(name);
    await ResiliencePolicy.wrap(tracing('a'), tracing('b'), tracing('c')).execute(() => void order.push('call'));
    expect(order).toEqual(['a in', 'b in', 'c in', 'call', 'c out', 'b out', 'a out']);
  });

  it('hands the attempt number of the retry to the call, through the inner policies', async () => {
    const attempts: number[] = [];
    const policy = ResiliencePolicy.wrap(
      new RetryPolicy({ attempts: 3, backoff: { delay: 0 } }),
      new CircuitBreakerPolicy({ minimumCalls: 20 }),
      new TimeoutPolicy('1s'),
      new BulkheadPolicy(),
    );
    const result = await policy.execute(({ attempt }) => {
      attempts.push(attempt);
      return attempt < 3 ? Promise.reject(new Error('flaky')) : 'ok';
    });
    expect(result).toBe('ok');
    expect(attempts).toEqual([1, 2, 3]);
  });

  it("aborts the innermost attempt's signal when the caller aborts, through every layer", async () => {
    const controller = new AbortController();
    let seen: AbortSignal | undefined;
    const policy = ResiliencePolicy.wrap(
      new FallbackPolicy(() => 'fallback'),
      new RetryPolicy({ attempts: 5, backoff: { delay: 0 } }),
      new TimeoutPolicy('1m'),
      new BulkheadPolicy(),
    );
    const result = policy.execute(
      ({ signal }) => {
        seen = signal;
        return new Promise((_, reject) => signal.addEventListener('abort', () => reject(signal.reason)));
      },
      { signal: controller.signal },
    );
    await Promise.resolve();
    controller.abort(new Error('shutdown'));
    // Neither retried nor replaced by the fallback: the caller is gone.
    await expect(result).rejects.toThrow('shutdown');
    expect(seen!.aborted).toBe(true);
  });

  it('can be reused concurrently: executions do not share attempts or signals', async () => {
    vi.useFakeTimers();
    const policy = ResiliencePolicy.wrap(
      new RetryPolicy({ attempts: 2, backoff: { delay: 10, factor: 1 } }),
      new TimeoutPolicy(50),
    );
    const slow = policy.execute(
      ({ signal }) => new Promise((_, reject) => signal.addEventListener('abort', () => reject(signal.reason))),
    );
    const fast = policy.execute(({ attempt }) => `fast ${attempt}`);
    const slowOutcome = slow.catch((e: Error) => e.name);
    expect(await fast).toBe('fast 1');
    await vi.advanceTimersByTimeAsync(200);
    expect(await slowOutcome).toBe('ResilienceTimeoutError');
  });
});
