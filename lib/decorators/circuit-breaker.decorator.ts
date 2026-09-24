import { SetMetadata } from '@nestjs/common';
import type { CircuitBreakerOptions } from '../interfaces/circuit-breaker-options.interface.js';
import type { EntrypointDecorator } from '../interfaces/entrypoint-options.interface.js';
import { CIRCUIT_BREAKER_METADATA } from '../resilience.constants.js';

/**
 * Circuit breaker, per handler unless named. A per-handler breaker is
 * registered under the handler's name (`OrdersController.list`); see
 * `ResilienceService.circuitBreaker()`. A string refers to a named breaker
 * configured elsewhere. Answers 503 with `Retry-After` while open.
 */
export const CircuitBreaker = (options: CircuitBreakerOptions | string = {}): EntrypointDecorator =>
  SetMetadata(CIRCUIT_BREAKER_METADATA, options);
