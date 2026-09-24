import type { ResilienceEvents } from '../events/resilience-events.service.js';
import { attachEventSink } from '../events/resilience.channels.js';
import type { Duration } from '../interfaces/duration.interface.js';
import type { ResilienceModuleOptions } from '../interfaces/resilience-module-options.interface.js';
import type { ResiliencePolicy } from '../policies/resilience.policy.js';
import { durationOption } from '../utils/options.util.js';

interface Declaration<O> {
  options: O;
  source: string;
}

/**
 * Named, shared policy instances (not exported). A name is configured in one
 * place; everything else refers to it by name. State lives in the registry,
 * which is a provider, so each application (and each test app) has its own.
 */
export abstract class NamedRegistry<P extends ResiliencePolicy, O extends { name?: string }> {
  protected abstract readonly kind: string;
  private readonly instances = new Map<string, P>();
  private readonly declarations = new Map<string, Declaration<O>>();

  constructor(
    protected readonly moduleOptions: ResilienceModuleOptions = {},
    private readonly events?: ResilienceEvents,
  ) {}

  /** Creates the instance `name` from its configuration, with module defaults applied. */
  protected abstract create(name: string, options: O | undefined): P;

  /** The configuration in comparable form: durations in milliseconds. */
  protected abstract normalize(options: O): object;

  /**
   * Records `options` as the configuration of `name`. A reference that sets
   * nothing but the name is always fine, unless `definition` says it defines
   * the instance with default settings (a preset's `circuitBreaker: {}`);
   * two different configurations for the same name are a bootstrap error.
   */
  declare(name: string, options: O | undefined, source: string, definition = false): void {
    if (!options || (!definition && !hasSettings(options))) {
      return;
    }

    const existing = this.declarations.get(name);
    if (existing) {
      if (!sameOptions(this.normalize(existing.options), this.normalize(options))) {
        throw new Error(
          `${this.kind} "${name}" is configured differently by ${existing.source} and ${source}. ` +
            `Configure it in one place (for example a preset in ResilienceModule.forRoot({ presets })) ` +
            `and refer to it by name elsewhere.`,
        );
      }
      return;
    }

    if (this.instances.has(name)) {
      throw new Error(`${this.kind} "${name}" was already created before ${source} configured it.`);
    }
    this.declarations.set(name, { options, source });
  }

  /** The instance called `name`, created on first use from its declared configuration or `options`. */
  get(name: string, options?: O, source = 'a get() call'): P {
    if (options) {
      this.declare(name, options, source);
    }

    let instance = this.instances.get(name);
    if (!instance) {
      instance = attachEventSink(this.create(name, this.declarations.get(name)?.options), this.events?.emit);
      this.instances.set(name, instance);
    }
    return instance;
  }

  /** The instance called `name` if it exists or is configured; `undefined` for names nobody configured. */
  find(name: string): P | undefined {
    return this.instances.has(name) || this.declarations.has(name) ? this.get(name) : undefined;
  }

  list(): P[] {
    return [...this.instances.values()];
  }

  names(): string[] {
    return [...new Set([...this.instances.keys(), ...this.declarations.keys()])];
  }
}

/** @internal A `Duration` option in milliseconds, `undefined` when unset. */
export function ms(duration: Duration | undefined, option: string): number | undefined {
  return duration === undefined ? undefined : durationOption(duration, option);
}

function hasSettings(options: object): boolean {
  return Object.keys(options).some((key) => key !== 'name');
}

/** Structural equality; functions compare by identity; `undefined` fields count as absent. */
export function sameOptions(a: unknown, b: unknown): boolean {
  if (a === b) {
    return true;
  }
  if (typeof a !== 'object' || typeof b !== 'object' || !a || !b) {
    return false;
  }
  const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
  for (const key of keys) {
    if (!sameOptions((a as Record<string, unknown>)[key], (b as Record<string, unknown>)[key])) {
      return false;
    }
  }
  return true;
}
