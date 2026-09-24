import { RejectionError } from './rejection.error.js';

export type BulkheadRejectionReason = 'full' | 'queue-timeout';

export class BulkheadFullError extends RejectionError {
  readonly code = 'BULKHEAD_FULL';

  constructor(
    bulkhead: string,
    /** `full`: no free slot and no room in the queue. `queue-timeout`: waited `queueTimeout` for a slot. */
    readonly reason: BulkheadRejectionReason,
  ) {
    super(
      reason === 'full'
        ? `Bulkhead "${bulkhead}" is full`
        : `Timed out waiting for a slot in bulkhead "${bulkhead}"`,
      bulkhead,
    );
  }
}
