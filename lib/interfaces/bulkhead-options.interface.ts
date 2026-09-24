import type { Duration } from './duration.interface.js';

export interface BulkheadOptions {
  /** Bulkheads with the same name share their slots. Without one, a decorator's bulkhead belongs to its handler. */
  name?: string;
  /** Calls allowed to run at the same time. Default 10. */
  maxConcurrent?: number;
  /** Calls allowed to wait for a slot; beyond that, calls are rejected. Default 0. */
  maxQueue?: number;
  /** Longest a queued call waits for a slot. Default: no limit. */
  queueTimeout?: Duration;
}
