import { Inject, Injectable, Optional } from '@nestjs/common';
import { ResilienceEvents } from '../events/resilience-events.service.js';
import type { BulkheadOptions } from '../interfaces/bulkhead-options.interface.js';
import type { ResilienceModuleOptions } from '../interfaces/resilience-module-options.interface.js';
import { BulkheadPolicy } from '../policies/bulkhead.policy.js';
import { RESILIENCE_MODULE_OPTIONS } from '../resilience.module-definition.js';
import { ms, NamedRegistry } from './named-registry.service.js';

@Injectable()
export class BulkheadRegistry extends NamedRegistry<BulkheadPolicy, BulkheadOptions> {
  protected readonly kind = 'Bulkhead';

  constructor(
    @Optional() @Inject(RESILIENCE_MODULE_OPTIONS) moduleOptions?: ResilienceModuleOptions,
    @Optional() events?: ResilienceEvents,
  ) {
    super(moduleOptions, events);
  }

  protected create(name: string, options: BulkheadOptions = {}) {
    return new BulkheadPolicy({ ...this.moduleOptions.defaults?.bulkhead, ...options, name });
  }

  protected normalize({ queueTimeout, ...rest }: BulkheadOptions) {
    return { ...rest, queueTimeout: ms(queueTimeout, 'queueTimeout') };
  }
}
