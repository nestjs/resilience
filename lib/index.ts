// Nest integration: the module, entrypoint decorators, and what services inject.
export { ResilienceModule } from './resilience.module.js';
export {
  RESILIENCE_MODULE_OPTIONS,
  type ResilienceModuleAsyncOptions,
  type ResilienceOptionsFactory,
} from './resilience.module-definition.js';
export * from './decorators/index.js';
export { ResilienceService } from './resilience.service.js';
export { ResilienceContext } from './context/index.js';

// Policy objects: the building blocks, usable anywhere (services, scripts, other packages).
export {
  ResiliencePolicy,
  isClientError,
  RetryPolicy,
  TimeoutPolicy,
  CircuitBreakerPolicy,
  BulkheadPolicy,
  OutboundRateLimitPolicy,
  FallbackPolicy,
} from './policies/index.js';

// Options and public types.
export type {
  ResilienceDefaults,
  ResilienceModuleOptions,
  ResiliencePreset,
  AttemptContext,
  ExecuteOptions,
  RetryOptions,
  BackoffOptions,
  TimeoutOptions,
  CircuitBreakerOptions,
  BulkheadOptions,
  OutboundRateLimitOptions,
  FallbackOptions,
  Duration,
} from './interfaces/index.js';

// Errors: what policies throw, and what entrypoints turn into 504s and 503s.
export {
  ResilienceError,
  ResilienceTimeoutError,
  CircuitOpenError,
  BulkheadFullError,
  OutboundRateLimitError,
} from './errors/index.js';

// Events: ResilienceEvents.events$, or the `nestjs:resilience:<type>` diagnostics channels.
export {
  ResilienceEvents,
  type CircuitState,
  type ResilienceEvent,
  type ResilienceRetryEvent,
  type ResilienceTimeoutEvent,
  type ResilienceCircuitOpenEvent,
  type ResilienceCircuitHalfOpenEvent,
  type ResilienceCircuitClosedEvent,
  type ResilienceCircuitRejectedEvent,
  type ResilienceBulkheadRejectedEvent,
  type ResilienceRateLimitedEvent,
  type ResilienceFallbackEvent,
} from './events/index.js';
