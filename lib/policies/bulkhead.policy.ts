import { BulkheadFullError, type BulkheadRejectionReason } from '../errors/bulkhead-full.error.js';
import type { BulkheadOptions } from '../interfaces/bulkhead-options.interface.js';
import { countOption, durationOption } from '../utils/options.util.js';
import { setTimer } from '../utils/timers.util.js';
import { call, ResiliencePolicy, type Execution, type Next } from './resilience.policy.js';

interface Waiter {
  grant(): void;
}

/**
 * Limits concurrent executions. A slot is held until the call really
 * settles, even when an outer Timeout already gave up on it, so the limit
 * reflects actual load on the protected resource.
 */
export class BulkheadPolicy extends ResiliencePolicy {
  readonly maxConcurrent: number;
  readonly maxQueue: number;
  /** In milliseconds. */
  readonly queueTimeout?: number;
  private running = 0;
  private readonly queue: Waiter[] = [];

  constructor(options: BulkheadOptions = {}) {
    super(options.name ?? 'bulkhead');
    this.maxConcurrent = countOption(options.maxConcurrent ?? 10, 'maxConcurrent', 1, true);
    this.maxQueue = countOption(options.maxQueue ?? 0, 'maxQueue', 0, true);
    this.queueTimeout =
      options.queueTimeout === undefined ? undefined : durationOption(options.queueTimeout, 'queueTimeout');
  }

  /** Calls running now. */
  get active(): number {
    return this.running;
  }

  /** Calls waiting for a slot. */
  get queued(): number {
    return this.queue.length;
  }

  /** @internal */
  async run<T>(next: Next<T>, execution: Execution): Promise<T> {
    await this.acquire(execution);
    try {
      return await call(next, execution);
    } finally {
      this.releaseSlot();
    }
  }

  private acquire(execution: Execution): Promise<void> | void {
    const { signal } = execution;
    if (signal.aborted) {
      throw signal.reason;
    }

    if (this.running < this.maxConcurrent) {
      this.running++;
      return;
    }

    if (this.queue.length >= this.maxQueue) {
      this.reject(execution, 'full');
    }

    return new Promise<void>((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const leave = () => {
        const index = this.queue.indexOf(waiter);
        if (index !== -1) {
          this.queue.splice(index, 1);
        }
        clearTimeout(timer);
        signal.removeEventListener('abort', onAbort);
      };
      const waiter: Waiter = {
        grant: () => {
          leave();
          this.running++;
          resolve();
        },
      };
      const onAbort = () => {
        leave();
        reject(signal.reason);
      };

      this.queue.push(waiter);
      signal.addEventListener('abort', onAbort, { once: true });
      if (this.queueTimeout !== undefined) {
        timer = setTimer(() => {
          leave();
          try {
            this.reject(execution, 'queue-timeout');
          } catch (error) {
            reject(error);
          }
        }, this.queueTimeout);
      }
    });
  }

  private reject(execution: Execution, reason: BulkheadRejectionReason): never {
    this.emit({
      type: 'bulkhead-rejected',
      policy: this.name,
      source: execution.source,
      reason,
      active: this.running,
      queued: this.queue.length,
    });

    throw new BulkheadFullError(this.name, reason);
  }

  private releaseSlot() {
    this.running--;
    this.queue[0]?.grant();
  }
}
