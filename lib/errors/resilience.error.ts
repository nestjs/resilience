/**
 * Base class of the errors this package raises: `ResilienceTimeoutError`, and the
 * rejections `CircuitOpenError`, `BulkheadFullError` and
 * `OutboundRateLimitError`. At entrypoints they become each transport's
 * native error (504 or 503 over HTTP).
 */
export abstract class ResilienceError extends Error {
  /** Machine-readable code, also sent to clients (`TIMEOUT`, `CIRCUIT_OPEN`, …). */
  abstract readonly code: string;

  constructor(
    message: string,
    /** Name of the policy instance that raised it: a breaker's name, a preset's, a handler's (`OrdersController.list`). */
    readonly policy: string,
  ) {
    super(message);
    this.name = new.target.name;
  }
}
