import { Inject, Injectable, Optional } from '@nestjs/common';
import { ResilienceEvents } from '../events/resilience-events.service.js';
import { attachEventSink } from '../events/resilience.channels.js';
import type { FallbackHandler } from '../interfaces/fallback-options.interface.js';
import type {
  ResilienceDefaults,
  ResilienceModuleOptions,
  ResiliencePreset,
} from '../interfaces/resilience-module-options.interface.js';
import type { RetryOptions } from '../interfaces/retry-options.interface.js';
import { BulkheadPolicy } from '../policies/bulkhead.policy.js';
import { CircuitBreakerPolicy } from '../policies/circuit-breaker.policy.js';
import { FallbackPolicy } from '../policies/fallback.policy.js';
import { OutboundRateLimitPolicy } from '../policies/outbound-rate-limit.policy.js';
import { ResiliencePolicy } from '../policies/resilience.policy.js';
import { normalizeRetry, RetryPolicy } from '../policies/retry.policy.js';
import { TimeoutPolicy } from '../policies/timeout.policy.js';
import { RESILIENCE_MODULE_OPTIONS } from '../resilience.module-definition.js';
import { underOption } from '../utils/options.util.js';
import { BulkheadRegistry } from './bulkhead-registry.service.js';
import { CircuitBreakerRegistry } from './circuit-breaker-registry.service.js';
import type { NamedRegistry } from './named-registry.service.js';
import { OutboundRateLimitRegistry } from './outbound-rate-limit-registry.service.js';

/**
 * Outermost first. Fallback sees every failure, including rejections. Retry
 * wraps the breaker, so each attempt is recorded and an open breaker stops
 * the retries. The outbound rate limit sits inside the breaker (its
 * rejections are not failures) and outside the timeout (waiting for a token
 * does not eat the call's budget). Timeout wraps the bulkhead, so time spent
 * queued counts against the budget while the bulkhead still limits real
 * concurrency.
 */
export const CANONICAL_ORDER = [
  'fallback',
  'retry',
  'circuitBreaker',
  'outboundRateLimit',
  'timeout',
  'bulkhead',
] as const;

export type Stage = (typeof CANONICAL_ORDER)[number];
export type Stages = Partial<Record<Stage, ResiliencePolicy>>;

/** The stateful stages: shared by name through a registry. */
export const NAMED_STAGES = ['circuitBreaker', 'bulkhead', 'outboundRateLimit'] as const;
type NamedStage = (typeof NAMED_STAGES)[number];

/**
 * Builds policies from presets and inline options (not exported): applies
 * module defaults, resolves named instances through the registries, attaches
 * the app's event sink and composes the stages in canonical order.
 */
@Injectable()
export class PolicyFactory {
  private readonly options: ResilienceModuleOptions;

  constructor(
    @Optional() @Inject(RESILIENCE_MODULE_OPTIONS) options: ResilienceModuleOptions | undefined,
    private readonly events: ResilienceEvents,
    readonly breakers: CircuitBreakerRegistry,
    readonly bulkheads: BulkheadRegistry,
    readonly outboundRateLimits: OutboundRateLimitRegistry,
  ) {
    this.options = options ?? {};

    // Checked when the app starts, before a service can call preset() in its
    // constructor: `defaults` and every preset, used or not. A preset is
    // checked as it will be built, with the defaults under it.
    validateStages(this.defaults, 'defaults');
    for (const [name, preset] of Object.entries(this.options.presets ?? {})) {
      validateStages(preset, `presets.${name}`, this.defaults);
    }
  }

  get defaults() {
    return this.options.defaults ?? {};
  }

  presetNames(): string[] {
    return Object.keys(this.options.presets ?? {});
  }

  /** The options of preset `name`, with its stateful stages named after the preset. */
  presetOptions(name: string): ResiliencePreset {
    const preset = this.options.presets?.[name];
    if (!preset) {
      const known = this.presetNames();
      throw new Error(
        `Unknown resilience preset "${name}". Known presets: ${known.length ? known.join(', ') : '(none)'}.`,
      );
    }

    return {
      ...preset,
      circuitBreaker: named(preset.circuitBreaker, name),
      bulkhead: named(preset.bulkhead, name),
      outboundRateLimit: named(preset.outboundRateLimit, name),
    };
  }

  /**
   * Bootstrap: records the named instances each preset defines (conflict
   * detection): a preset's breaker, bulkhead and outbound rate limit can't be
   * configured differently anywhere else.
   */
  declarePresets(): void {
    for (const name of this.presetNames()) {
      this.declarePreset(name);
    }
  }

  declarePreset(name: string): ResiliencePreset {
    const spec = this.presetOptions(name);
    this.declare(spec, `preset "${name}"`, true);
    return spec;
  }

  /** Records the named instances `spec` configures. */
  declare(spec: ResiliencePreset, source: string, definition = false): void {
    for (const stage of NAMED_STAGES) {
      const value = spec[stage];
      if (typeof value === 'object' && value.name) {
        (this.registry(stage) as NamedRegistry<ResiliencePolicy, object>).declare(value.name, value, source, definition);
      }
    }
  }

  /**
   * The stages `spec` turns on, named `name` (which labels their events).
   * Named stateful stages come from the registries; unnamed ones are private
   * to the returned stages. For an `entrypoint`, `retryOnResult` is dropped.
   */
  build(spec: ResiliencePreset, name: string, source = name, { entrypoint = false } = {}): Stages {
    const defaults = this.defaults;
    const stages: Stages = {};

    if (spec.fallback) {
      const { handler, handleIf } =
        typeof spec.fallback === 'function'
          ? { handler: spec.fallback as FallbackHandler, handleIf: undefined }
          : spec.fallback;
      stages.fallback = this.sink(new FallbackPolicy(handler, { name, handleIf }));
    }

    if (spec.retry !== undefined) {
      const options = mergeRetry(normalizeDefaultRetry(defaults.retry), normalizeRetry(spec.retry));
      // An entrypoint's result is on its way to the client: never retried.
      if (entrypoint) {
        delete options.retryOnResult;
      }
      const retry = underOption('retry', () => new RetryPolicy({ ...options, name }));
      if (retry.attempts > 1) {
        stages.retry = this.sink(retry);
      }
    }

    if (spec.circuitBreaker !== undefined) {
      stages.circuitBreaker = underOption('circuitBreaker', () =>
        this.stateful(spec.circuitBreaker!, this.breakers, source, (options) =>
          new CircuitBreakerPolicy({ ...defaults.circuitBreaker, ...options, name }),
        ),
      );
    }

    if (spec.outboundRateLimit !== undefined) {
      stages.outboundRateLimit = underOption('outboundRateLimit', () =>
        this.stateful(spec.outboundRateLimit!, this.outboundRateLimits, source, (options) =>
          new OutboundRateLimitPolicy({ ...options, name }),
        ),
      );
    }

    if (spec.timeout !== undefined) {
      stages.timeout = this.sink(underOption('timeout', () => new TimeoutPolicy({ timeout: spec.timeout!, name })));
    }

    if (spec.bulkhead !== undefined) {
      stages.bulkhead = underOption('bulkhead', () =>
        this.stateful(spec.bulkhead!, this.bulkheads, source, (options) =>
          new BulkheadPolicy({ ...defaults.bulkhead, ...options, name }),
        ),
      );
    }

    return stages;
  }

  compose(stages: Stages, { retry = true } = {}): ResiliencePolicy {
    return ResiliencePolicy.wrap(
      ...CANONICAL_ORDER.filter((stage) => stages[stage] && (retry || stage !== 'retry')).map(
        (stage) => stages[stage]!,
      ),
    );
  }

  registry(stage: NamedStage) {
    return { circuitBreaker: this.breakers, bulkhead: this.bulkheads, outboundRateLimit: this.outboundRateLimits }[
      stage
    ];
  }

  private sink<P extends ResiliencePolicy>(policy: P): P {
    return attachEventSink(policy, this.events.emit);
  }

  private stateful<O extends { name?: string }, P extends ResiliencePolicy>(
    stage: O | string,
    registry: NamedRegistry<P, O>,
    source: string,
    create: (options: O) => P,
  ): P {
    if (typeof stage === 'string') {
      return registry.get(stage);
    }
    if (stage.name) {
      return registry.get(stage.name, stage, source);
    }
    return this.sink(create(stage));
  }
}

/**
 * Fails the boot on an invalid option, naming it
 * (`presets.carrier.bulkhead.maxConcurrent: Invalid value NaN…`), by creating
 * each configured stage once: the policies validate their own options.
 */
function validateStages(
  spec: ResiliencePreset | ResilienceDefaults,
  path: string,
  defaults: ResilienceDefaults = {},
): void {
  const check = (stage: string, value: unknown, create: (options: any) => unknown) => {
    if (value !== undefined) {
      underOption(`${path}.${stage}`, () => create(value));
    }
  };
  // A string names a breaker, bulkhead or rate limit configured elsewhere.
  const unlessNamed = (create: (options: any) => unknown) => (value: unknown) =>
    typeof value === 'string' ? undefined : create(value);

  check('retry', spec.retry, (retry) =>
    new RetryPolicy(mergeRetry(normalizeDefaultRetry(defaults.retry), normalizeRetry(retry))),
  );
  check('timeout', spec.timeout, (timeout) => new TimeoutPolicy(timeout));
  check('circuitBreaker', spec.circuitBreaker, unlessNamed((options) =>
    new CircuitBreakerPolicy({ ...defaults.circuitBreaker, ...options }),
  ));
  check('bulkhead', spec.bulkhead, unlessNamed((options) => new BulkheadPolicy({ ...defaults.bulkhead, ...options })));

  if ('outboundRateLimit' in spec) {
    check('outboundRateLimit', spec.outboundRateLimit, unlessNamed((options) => new OutboundRateLimitPolicy(options)));
  }
}

function named<O extends { name?: string }>(stage: O | string | undefined, name: string) {
  if (stage === undefined || typeof stage === 'string') {
    return stage;
  }
  return { ...stage, name: stage.name ?? name };
}

function normalizeDefaultRetry(retry: number | RetryOptions | undefined): RetryOptions {
  return retry === undefined ? {} : normalizeRetry(retry);
}

/** `override`'s fields win; backoff objects merge too. */
export function mergeRetry<R extends RetryOptions>(base: RetryOptions, override: R): R {
  const backoff =
    typeof base.backoff === 'object' && typeof override.backoff === 'object'
      ? { ...base.backoff, ...override.backoff }
      : (override.backoff ?? base.backoff);

  const merged = { ...base, ...definedFields(override) } as R;
  if (backoff !== undefined) {
    merged.backoff = backoff;
  }
  return merged;
}

/** Drops `undefined` fields, so that `{ attempts: undefined }` doesn't erase an inherited value. */
export function definedFields<T extends object>(value: T): Partial<T> {
  return Object.fromEntries(Object.entries(value).filter(([, v]) => v !== undefined)) as Partial<T>;
}
