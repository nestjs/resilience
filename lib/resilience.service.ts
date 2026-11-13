import { Injectable } from '@nestjs/common';
import type { ResiliencePreset } from './interfaces/resilience-module-options.interface.js';
import type { BulkheadPolicy } from './policies/bulkhead.policy.js';
import type { CircuitBreakerPolicy } from './policies/circuit-breaker.policy.js';
import type { ResiliencePolicy } from './policies/resilience.policy.js';
import type { NamedRegistry } from './services/named-registry.service.js';
import { PolicyFactory } from './services/policy-factory.service.js';

/**
 * Resilience for code inside services, which the decorators don't reach:
 * presets and inline options as policy objects, and the named breakers and
 * bulkheads that entrypoints share with them.
 */
@Injectable()
export class ResilienceService {
  private readonly presets = new Map<string, ResiliencePolicy>();

  constructor(private readonly factory: PolicyFactory) {}

  /**
   * The preset `name` from `forRoot({ presets })` as one policy, composed in
   * canonical order. Its breaker, bulkhead and outbound rate limit are the
   * same instances that `@Resilience(name)` entrypoints use.
   */
  preset(name: string): ResiliencePolicy {
    let policy = this.presets.get(name);
    if (!policy) {
      // Before bootstrap (a constructor), the presets' named instances aren't
      // declared yet, and this preset may use another's by name.
      this.factory.declarePresets();
      const spec = this.factory.declarePreset(name);
      policy = this.factory.compose(this.factory.build(spec, name, `preset "${name}"`));
      this.presets.set(name, policy);
    }

    return policy;
  }

  /**
   * A policy from inline options, composed in canonical order with module
   * defaults applied. `name` labels its events. Unnamed breakers, bulkheads
   * and outbound rate limits are private to the returned policy, so create it
   * once (in a constructor, say), not per call.
   */
  create(options: ResiliencePreset, name = 'anonymous'): ResiliencePolicy {
    this.factory.declarePresets();
    return this.factory.compose(this.factory.build(options, name));
  }

  /**
   * The circuit breaker called `name`: a named one, a preset's (named after
   * the preset), or a handler's own (`OrdersController.list`). Throws for a
   * name nothing configures.
   */
  circuitBreaker(name: string): CircuitBreakerPolicy {
    return this.lookup(this.factory.breakers, 'circuitBreaker', 'circuit breaker', name);
  }

  /** The bulkhead called `name`, found like `circuitBreaker()`. Its `active` and `queued` counts show its load. */
  bulkhead(name: string): BulkheadPolicy {
    return this.lookup(this.factory.bulkheads, 'bulkhead', 'bulkhead', name);
  }

  /** Every circuit breaker created so far, for health checks and metrics. */
  circuitBreakers(): CircuitBreakerPolicy[] {
    return this.factory.breakers.list();
  }

  /** Every bulkhead created so far. */
  bulkheads(): BulkheadPolicy[] {
    return this.factory.bulkheads.list();
  }

  private lookup<P extends ResiliencePolicy>(
    registry: NamedRegistry<P, object>,
    stage: 'circuitBreaker' | 'bulkhead',
    kind: string,
    name: string,
  ): P {
    const found = registry.find(name);
    if (found) {
      return found;
    }

    // A preset's breaker or bulkhead nothing has used yet: named after the
    // preset, or with a name of its own.
    for (const preset of this.factory.presetNames()) {
      const value = this.factory.presetOptions(preset)[stage];
      if (typeof value === 'object' && value.name === name) {
        this.factory.declarePreset(preset);
        return registry.get(name);
      }
    }

    const known = registry.names();
    throw new Error(`Unknown ${kind} "${name}". Known: ${known.length ? known.join(', ') : '(none)'}.`);
  }
}
