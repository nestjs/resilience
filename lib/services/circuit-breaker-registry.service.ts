import { Inject, Injectable, Optional } from '@nestjs/common';
import { ResilienceEvents } from '../events/resilience-events.service.js';
import type { CircuitBreakerOptions } from '../interfaces/circuit-breaker-options.interface.js';
import type { ResilienceModuleOptions } from '../interfaces/resilience-module-options.interface.js';
import { CircuitBreakerPolicy } from '../policies/circuit-breaker.policy.js';
import { RESILIENCE_MODULE_OPTIONS } from '../resilience.module-definition.js';
import { ms, NamedRegistry } from './named-registry.service.js';

@Injectable()
export class CircuitBreakerRegistry extends NamedRegistry<CircuitBreakerPolicy, CircuitBreakerOptions> {
  protected readonly kind = 'Circuit breaker';

  constructor(
    @Optional() @Inject(RESILIENCE_MODULE_OPTIONS) moduleOptions?: ResilienceModuleOptions,
    @Optional() events?: ResilienceEvents,
  ) {
    super(moduleOptions, events);
  }

  protected create(name: string, options: CircuitBreakerOptions = {}) {
    return new CircuitBreakerPolicy({ ...this.moduleOptions.defaults?.circuitBreaker, ...options, name });
  }

  protected normalize({ openDuration, slidingWindow, ...rest }: CircuitBreakerOptions) {
    return {
      ...rest,
      openDuration: ms(openDuration, 'openDuration'),
      slidingWindow: slidingWindow && {
        type: slidingWindow.type,
        size: slidingWindow.type === 'time' ? ms(slidingWindow.size, 'slidingWindow.size') : slidingWindow.size,
      },
    };
  }
}
