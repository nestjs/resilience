import { OutboundRateLimitError } from '../errors/outbound-rate-limit.error.js';
import type { OutboundRateLimitOptions } from '../interfaces/outbound-rate-limit-options.interface.js';
import { countOption, durationOption } from '../utils/options.util.js';
import { sleep } from '../utils/timers.util.js';
import { call, ResiliencePolicy, type Execution, type Next } from './resilience.policy.js';

/**
 * Keeps *outgoing* calls to one dependency within its quota (a partner API
 * allowing 100 requests per second). A token bucket holding `limit` tokens,
 * refilled continuously at `limit / interval`, shared by every caller of the
 * dependency and local to the process. Callers that wait for a token are
 * served in arrival order. For limiting *incoming* requests per client, use
 * `@nestjs/throttler`.
 */
export class OutboundRateLimitPolicy extends ResiliencePolicy {
  readonly limit: number;
  /** In milliseconds. */
  readonly interval: number;
  /** In milliseconds. */
  readonly maxWait: number;
  private tokens: number;
  private refilledAt = Date.now();

  constructor(options: OutboundRateLimitOptions) {
    super(options.name ?? 'outbound-rate-limit');
    this.limit = countOption(options.limit, 'limit', 1);
    this.interval = durationOption(options.interval, 'interval', true);
    this.maxWait = durationOption(options.maxWait ?? 0, 'maxWait');
    this.tokens = this.limit;
  }

  /** Tokens available now. */
  get available(): number {
    this.refill();
    return Math.max(0, Math.floor(this.tokens));
  }

  /** @internal */
  async run<T>(next: Next<T>, execution: Execution): Promise<T> {
    await this.acquire(execution);
    return call(next, execution);
  }

  private async acquire(execution: Execution) {
    this.refill();

    if (this.tokens >= 1) {
      this.tokens -= 1;
      return;
    }

    const wait = Math.ceil(((1 - this.tokens) * this.interval) / this.limit);
    if (wait > this.maxWait) {
      this.emit({ type: 'rate-limited', policy: this.name, source: execution.source, retryAfterMs: wait });
      throw new OutboundRateLimitError(this.name, wait);
    }

    // Reserve the token now so later callers queue behind this one.
    this.tokens -= 1;
    try {
      await sleep(wait, execution.signal);
    } catch (error) {
      this.tokens += 1;
      throw error;
    }
  }

  private refill() {
    const now = Date.now();
    const elapsed = now - this.refilledAt;
    this.refilledAt = now;
    this.tokens = Math.min(this.limit, this.tokens + (elapsed * this.limit) / this.interval);
  }
}
