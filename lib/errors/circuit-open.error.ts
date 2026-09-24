import { RejectionError } from './rejection.error.js';

export class CircuitOpenError extends RejectionError {
  readonly code = 'CIRCUIT_OPEN';
  declare readonly retryAfterMs: number;

  constructor(breaker: string, retryAfterMs: number) {
    super(`Circuit breaker "${breaker}" is open`, breaker, retryAfterMs);
  }
}
