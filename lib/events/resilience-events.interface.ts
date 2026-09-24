import type { BulkheadRejectionReason } from '../errors/bulkhead-full.error.js';

export type CircuitState = 'closed' | 'open' | 'half-open';

interface EventBase {
  /** Name of the policy instance: a breaker's or bulkhead's name, a preset's, or the handler's (`OrdersController.list`). */
  policy: string;
  /** The entrypoint (`OrdersController.list`) or the `source` passed to `execute()`, when known. */
  source?: string;
}

/** An attempt failed and another one follows after `delayMs`. */
export interface ResilienceRetryEvent extends EventBase {
  type: 'retry';
  /** The attempt that failed, 1-based. */
  attempt: number;
  /** The wait before the next attempt. */
  delayMs: number;
  error: unknown;
}

/** An attempt ran out of its time budget, `timeoutMs`. */
export interface ResilienceTimeoutEvent extends EventBase {
  type: 'timeout';
  timeoutMs: number;
}

/** A breaker opened: its failure rate reached the threshold, a half-open probe failed, or `trip()` was called. */
export interface ResilienceCircuitOpenEvent extends EventBase {
  type: 'circuit-open';
  from: CircuitState;
  to: 'open';
}

/** A breaker's `openDuration` passed: it lets probe calls through. */
export interface ResilienceCircuitHalfOpenEvent extends EventBase {
  type: 'circuit-half-open';
  from: CircuitState;
  to: 'half-open';
}

/** A breaker closed: its probes succeeded, or `reset()` was called. */
export interface ResilienceCircuitClosedEvent extends EventBase {
  type: 'circuit-closed';
  from: CircuitState;
  to: 'closed';
}

/** An open or half-open breaker refused a call; `retryAfterMs` until it lets one through. */
export interface ResilienceCircuitRejectedEvent extends EventBase {
  type: 'circuit-rejected';
  retryAfterMs: number;
}

/** A bulkhead refused a call: `full`, or `queue-timeout` after waiting for a slot. */
export interface ResilienceBulkheadRejectedEvent extends EventBase {
  type: 'bulkhead-rejected';
  reason: BulkheadRejectionReason;
  /** Calls running when it refused. */
  active: number;
  /** Calls waiting when it refused. */
  queued: number;
}

/** An outbound rate limit refused a call; `retryAfterMs` until a token is free. */
export interface ResilienceRateLimitedEvent extends EventBase {
  type: 'rate-limited';
  retryAfterMs: number;
}

/** A fallback replaced `error` with its own result. */
export interface ResilienceFallbackEvent extends EventBase {
  type: 'fallback';
  error: unknown;
}

/**
 * Every event a policy emits. Each type is also published on its own
 * `node:diagnostics_channel` channel, `nestjs:resilience:<type>`.
 */
export type ResilienceEvent =
  | ResilienceRetryEvent
  | ResilienceTimeoutEvent
  | ResilienceCircuitOpenEvent
  | ResilienceCircuitHalfOpenEvent
  | ResilienceCircuitClosedEvent
  | ResilienceCircuitRejectedEvent
  | ResilienceBulkheadRejectedEvent
  | ResilienceRateLimitedEvent
  | ResilienceFallbackEvent;

/** @internal */
export type ResilienceEventType = ResilienceEvent['type'];

export type EventSink = (event: ResilienceEvent) => void;
