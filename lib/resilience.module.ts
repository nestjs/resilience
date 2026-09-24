import { Module, type DynamicModule } from '@nestjs/common';
import { APP_INTERCEPTOR, DiscoveryModule } from '@nestjs/core';
import { ResilienceContext } from './context/resilience.context.js';
import { ResilienceEvents } from './events/resilience-events.service.js';
import { ResilienceInterceptor } from './interceptors/resilience.interceptor.js';
import { ResilienceExplorer } from './resilience.explorer.js';
import {
  ConfigurableModuleClass,
  OPTIONS_TYPE,
  type ResilienceModuleAsyncOptions,
} from './resilience.module-definition.js';
import { ResilienceService } from './resilience.service.js';
import { BulkheadRegistry } from './services/bulkhead-registry.service.js';
import { CircuitBreakerRegistry } from './services/circuit-breaker-registry.service.js';
import { EntrypointPlanner } from './services/entrypoint-planner.service.js';
import { OutboundRateLimitRegistry } from './services/outbound-rate-limit-registry.service.js';
import { PolicyFactory } from './services/policy-factory.service.js';

/**
 * `ResilienceModule.forRoot({ defaults, presets })` or
 * `forRootAsync({ imports, inject, useFactory | useClass | useExisting })`.
 * Global by default.
 *
 * Registers one app-wide interceptor that applies the entrypoint decorators,
 * and exports `ResilienceService`, `ResilienceEvents` and `ResilienceContext`
 * for code inside services.
 */
@Module({
  imports: [DiscoveryModule],
  providers: [
    ResilienceEvents,
    ResilienceContext,
    CircuitBreakerRegistry,
    BulkheadRegistry,
    OutboundRateLimitRegistry,
    PolicyFactory,
    ResilienceService,
    EntrypointPlanner,
    ResilienceExplorer,
    ResilienceInterceptor,
    { provide: APP_INTERCEPTOR, useExisting: ResilienceInterceptor },
  ],
  exports: [ResilienceService, ResilienceEvents, ResilienceContext],
})
export class ResilienceModule extends ConfigurableModuleClass {
  static forRoot(options: typeof OPTIONS_TYPE = {}): DynamicModule {
    return super.forRoot(options);
  }

  static forRootAsync(options: ResilienceModuleAsyncOptions): DynamicModule {
    return super.forRootAsync(options);
  }
}
