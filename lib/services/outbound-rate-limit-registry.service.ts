import { Inject, Injectable, Optional } from '@nestjs/common';
import { ResilienceEvents } from '../events/resilience-events.service.js';
import type { OutboundRateLimitOptions } from '../interfaces/outbound-rate-limit-options.interface.js';
import type { ResilienceModuleOptions } from '../interfaces/resilience-module-options.interface.js';
import { OutboundRateLimitPolicy } from '../policies/outbound-rate-limit.policy.js';
import { RESILIENCE_MODULE_OPTIONS } from '../resilience.module-definition.js';
import { ms, NamedRegistry } from './named-registry.service.js';

@Injectable()
export class OutboundRateLimitRegistry extends NamedRegistry<OutboundRateLimitPolicy, OutboundRateLimitOptions> {
  protected readonly kind = 'Outbound rate limit';

  constructor(
    @Optional() @Inject(RESILIENCE_MODULE_OPTIONS) moduleOptions?: ResilienceModuleOptions,
    @Optional() events?: ResilienceEvents,
  ) {
    super(moduleOptions, events);
  }

  protected create(name: string, options: OutboundRateLimitOptions | undefined) {
    if (!options) {
      throw new Error(`Outbound rate limit "${name}" has no configuration (limit and interval are required).`);
    }
    return new OutboundRateLimitPolicy({ ...options, name });
  }

  protected normalize({ interval, maxWait, ...rest }: OutboundRateLimitOptions) {
    return { ...rest, interval: ms(interval, 'interval'), maxWait: ms(maxWait, 'maxWait') };
  }
}
