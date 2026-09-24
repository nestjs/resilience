import { Injectable, type ExecutionContext, type Type } from '@nestjs/common';
import {
  METHOD_METADATA,
  RESPONSE_PASSTHROUGH_METADATA,
  ROUTE_ARGS_METADATA,
  SSE_METADATA,
} from '@nestjs/common/constants.js';
import { RouteParamtypes } from '@nestjs/common/enums/route-paramtypes.enum.js';
import { ModuleRef } from '@nestjs/core';
import { OptionError } from '../errors/option.error.js';
import type { EntrypointRetryOptions, FallbackMetadata } from '../interfaces/entrypoint-options.interface.js';
import type { AttemptContext } from '../interfaces/execute-options.interface.js';
import type { ResiliencePreset } from '../interfaces/resilience-module-options.interface.js';
import type { ResiliencePolicy } from '../policies/resilience.policy.js';
import { normalizeRetry } from '../policies/retry.policy.js';
import {
  BULKHEAD_METADATA,
  CIRCUIT_BREAKER_METADATA,
  FALLBACK_METADATA,
  PRESET_METADATA,
  RESILIENCE_METADATA_KEYS,
  RETRY_METADATA,
  TIMEOUT_METADATA,
} from '../resilience.constants.js';
import { definedFields, mergeRetry, PolicyFactory, type Stage } from './policy-factory.service.js';

// Metadata keys of other official packages, read without importing them.
const GRAPHQL_RESOLVER_TYPE = 'graphql:resolver_type';
const GRAPHQL_FIELD_RESOLVER = 'graphql:resolve_property';
const MICROSERVICES_PATTERN = 'microservices:pattern';

const STAGE_METADATA: Partial<Record<Stage, string>> = {
  fallback: FALLBACK_METADATA,
  retry: RETRY_METADATA,
  circuitBreaker: CIRCUIT_BREAKER_METADATA,
  timeout: TIMEOUT_METADATA,
  bulkhead: BULKHEAD_METADATA,
};

export interface EntrypointPlan {
  /** `OrdersController.list` */
  source: string;
  /** Full pipeline, used when repeating the handler is safe. */
  withRetry: ResiliencePolicy;
  /** Same stages (and state) without Retry. */
  withoutRetry: ResiliencePolicy;
  hasRetry: boolean;
  idempotent: boolean;
  /**
   * A `@Retry()` decorator sits on the handler itself (not only inherited from
   * the class or a preset). This is the opt-in for message, event and
   * WebSocket handlers, which have no protocol-level safety signal.
   */
  retryOnMethod: boolean;
  /**
   * The handler consumes a request stream (a gRPC client or bidirectional
   * stream), which a second attempt can't read again: never retried.
   */
  requestStream: boolean;
  /**
   * An `@Sse()` route. Nest sends its headers before the first event, so only
   * an event commits its response.
   */
  sse: boolean;
}

interface Resolved {
  spec: ResiliencePreset;
  source: string;
  fallbackMethod?: string;
  retryOnMethod: boolean;
  idempotent: boolean;
  requestStream: boolean;
  sse: boolean;
  /** The stateful stages named after the handler (decorator-configured, unnamed). */
  perHandler: Array<'circuitBreaker' | 'bulkhead'>;
  /** Settings that can't apply to this handler, for bootstrap warnings. */
  warnings: string[];
}

export interface Entrypoint {
  cls: Type;
  handler: Function;
  /** Whether the class has one instance (method-name fallbacks need it). */
  isStatic: boolean;
}

/** One level that sets a stage: a decorator or a preset, on the handler or the class. */
interface Match {
  value: any;
  preset: boolean;
  level: Function;
}

/**
 * Turns decorator metadata into policy pipelines, one per handler, cached for
 * the app's lifetime. Each stage is resolved on its own, from the most
 * specific level that sets it: handler decorator, handler preset, class
 * decorator, class preset. Decorators refine the levels below them field by
 * field (`@Retry({ idempotent: true })` keeps a preset's `attempts`, and a
 * handler's `@CircuitBreaker({ minimumCalls: 5 })` keeps a class-level
 * breaker's other settings). A preset is taken whole, and so is a named
 * breaker or bulkhead: levels below them don't contribute. A bare `@Timeout()`
 * takes its duration from the next level that sets one, then
 * `defaults.timeout`.
 *
 * Unnamed breakers and bulkheads from decorators are per handler; they are
 * registered under the handler's name (`OrdersController.list`).
 */
@Injectable()
export class EntrypointPlanner {
  private readonly plans = new Map<Function, Map<Function, EntrypointPlan | null>>();
  private readonly instances = new Map<Function, unknown>();

  constructor(
    private readonly factory: PolicyFactory,
    private readonly moduleRef: ModuleRef,
  ) {}

  planFor(cls: Type, handler: Function): EntrypointPlan | null {
    let byHandler = this.plans.get(cls);
    if (!byHandler) {
      this.plans.set(cls, (byHandler = new Map()));
    }

    if (!byHandler.has(handler)) {
      const resolved = this.resolve(cls, handler);
      const hasStages = !!resolved && Object.keys(resolved.spec).length > 0;
      if (hasStages) {
        this.factory.declare(resolved.spec, resolved.source);
      }
      byHandler.set(handler, hasStages ? this.build(resolved) : null);
    }

    return byHandler.get(handler)!;
  }

  /**
   * Bootstrap: resolves every entrypoint, declares all named instances (the
   * presets' first) before any is created, so conflicting configurations fail
   * regardless of order, then builds and caches the plans. Throws on
   * misconfiguration; returns the plans and warnings about settings that
   * can't apply.
   */
  prepare(entrypoints: Entrypoint[]): {
    planned: Array<Entrypoint & { plan: EntrypointPlan }>;
    warnings: string[];
  } {
    this.factory.declarePresets();
    const warnings: string[] = [];
    const resolved = entrypoints.flatMap((entry) => {
      const r = this.resolve(entry.cls, entry.handler);
      if (r) {
        warnings.push(...r.warnings);
      }
      return r && Object.keys(r.spec).length ? [{ entry, r }] : [];
    });

    const perHandlerOwners = new Map<string, Type>();
    for (const { entry, r } of resolved) {
      if (r.fallbackMethod) {
        if (typeof entry.cls.prototype[r.fallbackMethod] !== 'function') {
          throw new Error(
            `@Fallback('${r.fallbackMethod}') on ${r.source}: ${entry.cls.name} has no method "${r.fallbackMethod}".`,
          );
        }
        if (!entry.isStatic) {
          throw new Error(
            `@Fallback('${r.fallbackMethod}') on ${r.source}: method fallbacks need a singleton ${entry.cls.name}; ` +
              `it is request-scoped or transient. Pass a function to @Fallback() instead.`,
          );
        }
      }

      if (r.perHandler.length) {
        // Per-handler instances are registered under `Class.method`: two
        // classes with the same name would silently share them.
        const owner = perHandlerOwners.get(r.source);
        if (owner && owner !== entry.cls) {
          const [kind, decorator] =
            r.perHandler[0] === 'circuitBreaker' ? ['circuit breaker', 'CircuitBreaker'] : ['bulkhead', 'Bulkhead'];
          throw new Error(
            `Two handlers are named ${r.source} (two classes are called ${entry.cls.name}), so they would share ` +
              `one per-handler ${kind}. Give it a name: @${decorator}({ name: '…' }).`,
          );
        }
        perHandlerOwners.set(r.source, entry.cls);
      }

      onHandler(r.source, () => this.factory.declare(r.spec, r.source));
    }

    const planned = resolved.map(({ entry, r }) => {
      let byHandler = this.plans.get(entry.cls);
      if (!byHandler) {
        this.plans.set(entry.cls, (byHandler = new Map()));
      }
      const plan = this.build(r);
      byHandler.set(entry.handler, plan);
      return { ...entry, plan };
    });

    return { planned, warnings };
  }

  /** The handler's stages and flags, or `null` when it has no resilience settings (and nothing to warn about). */
  private resolve(cls: Type, handler: Function): Resolved | null {
    const method = methodKey(cls, handler);
    const source = `${cls.name}.${method}`;
    const base = {
      source,
      retryOnMethod: false,
      idempotent: false,
      requestStream: false,
      sse: Reflect.getMetadata(SSE_METADATA, handler) === true,
      perHandler: [] as Resolved['perHandler'],
      warnings: [] as string[],
    };

    // A subscription resolver returns an async iterator; none of the stages fit it.
    if (Reflect.getMetadata(GRAPHQL_RESOLVER_TYPE, handler) === 'Subscription') {
      if (RESILIENCE_METADATA_KEYS.some((key) => Reflect.getMetadata(key, handler) !== undefined)) {
        base.warnings.push(
          `Resilience decorators on ${source}() have no effect: GraphQL subscriptions are not supported.`,
        );
      }
      return base.warnings.length ? { ...base, spec: {} } : null;
    }
    // Class-level settings reach queries and mutations, not field resolvers.
    const levels = Reflect.getMetadata(GRAPHQL_FIELD_RESOLVER, handler) ? [handler] : [handler, cls];
    const presets = new Map<Function, ResiliencePreset | undefined>(
      levels.map((level) => {
        const name: string | undefined = Reflect.getMetadata(PRESET_METADATA, level);
        return [level, name === undefined ? undefined : this.factory.presetOptions(name)];
      }),
    );

    /** Every level that sets `stage`, most specific first. */
    const findAll = (stage: Stage): Match[] => {
      const matches: Match[] = [];
      for (const level of levels) {
        const key = STAGE_METADATA[stage];
        const explicit = key ? Reflect.getMetadata(key, level) : undefined;
        if (explicit !== undefined) {
          matches.push({ value: explicit, preset: false, level });
        }

        const preset = presets.get(level)?.[stage];
        if (preset !== undefined) {
          matches.push({ value: preset, preset: true, level });
        }
      }

      return matches;
    };
    /** The matches down to the first one taken whole; less specific levels don't contribute. */
    const upTo = (matches: Match[], whole: (match: Match) => boolean) => {
      const index = matches.findIndex(whole);
      return index === -1 ? matches : matches.slice(0, index + 1);
    };

    const spec: ResiliencePreset = {};
    let fallbackMethod: string | undefined;

    const fallbacks = upTo(findAll('fallback'), (m) => m.preset);
    if (fallbacks.length && handlesOwnResponse(cls, handler, method)) {
      // Nest doesn't send what such a handler returns, so a fallback's result
      // would go nowhere and the request would hang: leave the stage out.
      base.warnings.push(
        `@Fallback() on ${source}() has no effect: the handler sends its own response through @Res() or ` +
          `@Next(). Use @Res({ passthrough: true }) to let Nest send the fallback result.`,
      );
    } else if (fallbacks[0]?.preset) {
      spec.fallback = fallbacks[0].value;
    } else if (fallbacks.length) {
      const decorators = fallbacks.filter((m) => !m.preset).map((m) => m.value as FallbackMetadata);
      const { method: named, fn } = decorators[0];
      fallbackMethod = named;
      const call = fn
        ? (error: unknown, context: ExecutionContext) => fn(error, context)
        : (error: unknown, context: ExecutionContext) => {
            const instance = this.instanceOf(cls) as Record<string, Function>;
            return instance[named!](error, context);
          };
      spec.fallback = {
        handler: (error: unknown, _attempt: AttemptContext, invocation?: unknown) =>
          call(error, invocation as ExecutionContext),
        handleIf: decorators.find((d) => d.handleIf)?.handleIf,
      };
    }

    const retries = upTo(findAll('retry'), (m) => m.preset);
    if (retries.length) {
      const retry = retries.reduceRight<EntrypointRetryOptions>((merged, { value, preset }) => {
        if (!preset) {
          return mergeRetry(merged, value as EntrypointRetryOptions);
        }
        // A preset can't declare handlers safe to repeat: only their author can.
        const { idempotent: _ignored, ...options } = normalizeRetry(value) as EntrypointRetryOptions;
        return mergeRetry(merged, options);
      }, {});

      base.idempotent = !!retry.idempotent;
      const { idempotent: _, ...options } = retry;
      spec.retry = options;
      base.retryOnMethod = retries.some(({ level, preset }) => level === handler && !preset);
      base.requestStream = readsRequestStream(handler);
    }

    for (const stage of ['circuitBreaker', 'bulkhead'] as const) {
      const matches = upTo(findAll(stage), (m) => m.preset || isNamed(m.value));
      if (!matches.length) {
        continue;
      }
      const merged = matches.reduceRight<{ name?: string }>(
        (acc, { value }) => ({ ...acc, ...definedFields(typeof value === 'string' ? { name: value } : value) }),
        {},
      );
      // Decorator-configured breakers and bulkheads are per handler: name them after it.
      if (merged.name) {
        spec[stage] = merged;
      } else {
        spec[stage] = { ...merged, name: source };
        base.perHandler.push(stage);
      }
    }

    const rateLimit = findAll('outboundRateLimit')[0];
    if (rateLimit) {
      spec.outboundRateLimit = rateLimit.value;
    }

    const timeouts = findAll('timeout');
    if (timeouts.length) {
      const timeout =
        timeouts.find(({ value }) => value !== undefined && value !== null)?.value ?? this.factory.defaults.timeout;
      if (timeout === undefined || timeout === null) {
        throw new Error(`@Timeout() on ${source} has no duration and no defaults.timeout is configured.`);
      }
      spec.timeout = timeout;
    }

    if (!Object.keys(spec).length && !base.warnings.length) {
      return null;
    }
    return { ...base, spec, fallbackMethod };
  }

  private build({ spec, source, retryOnMethod, idempotent, requestStream, sse }: Resolved): EntrypointPlan {
    const stages = onHandler(source, () => this.factory.build(spec, source, source, { entrypoint: true }));

    return {
      source,
      withRetry: this.factory.compose(stages),
      withoutRetry: this.factory.compose(stages, { retry: false }),
      hasRetry: !!stages.retry,
      idempotent,
      retryOnMethod,
      requestStream,
      sse,
    };
  }

  private instanceOf(cls: Type): unknown {
    if (!this.instances.has(cls)) {
      this.instances.set(cls, this.moduleRef.get(cls, { strict: false }));
    }

    return this.instances.get(cls);
  }
}

/**
 * Runs `fn`, naming the handler when it reports an invalid option:
 * `Resilience decorators on OrdersController.export: bulkhead.maxConcurrent: …`.
 */
function onHandler<T>(source: string, fn: () => T): T {
  try {
    return fn();
  } catch (error) {
    if (error instanceof OptionError) {
      throw new TypeError(`Resilience decorators on ${source}: ${error.message}`);
    }
    throw error;
  }
}

function isNamed(value: unknown): boolean {
  return typeof value === 'string' || !!(value as { name?: string } | undefined)?.name;
}

/**
 * The property a handler sits under. `handler.name` is empty or different
 * when a decorator replaced the method (`@GrpcStreamMethod()` does).
 */
function methodKey(cls: Type, handler: Function): string {
  if (handler.name && cls.prototype[handler.name] === handler) {
    return handler.name;
  }

  for (let proto = cls.prototype; proto && proto !== Object.prototype; proto = Object.getPrototypeOf(proto)) {
    for (const key of Object.getOwnPropertyNames(proto)) {
      if (key !== 'constructor' && Object.getOwnPropertyDescriptor(proto, key)?.value === handler) {
        return key;
      }
    }
  }
  return handler.name;
}

/** An HTTP route with `@Res()` or `@Next()` without `passthrough`: Nest leaves the response to the handler. */
function handlesOwnResponse(cls: Type, handler: Function, method: string): boolean {
  // Other transports number their parameter types differently (GraphQL's @Context() is 1).
  if (Reflect.getMetadata(METHOD_METADATA, handler) === undefined) {
    return false;
  }

  const args: Record<string, unknown> | undefined = Reflect.getMetadata(ROUTE_ARGS_METADATA, cls, method);
  if (!args) {
    return false;
  }

  const manual = Object.keys(args).some((key) => {
    const type = Number(key.split(':')[0]);
    return type === RouteParamtypes.RESPONSE || type === RouteParamtypes.NEXT;
  });
  return manual && !Reflect.getMetadata(RESPONSE_PASSTHROUGH_METADATA, cls, method);
}

/** A gRPC handler that reads a client stream (`@GrpcStreamMethod()`, `@GrpcStreamCall()`). */
function readsRequestStream(handler: Function): boolean {
  const patterns: unknown = Reflect.getMetadata(MICROSERVICES_PATTERN, handler);
  return (
    Array.isArray(patterns) &&
    patterns.some((p) => {
      const streaming = (p as { streaming?: unknown } | null)?.streaming;
      return streaming === 'rx_stream' || streaming === 'pt_stream';
    })
  );
}
