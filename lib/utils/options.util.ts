import { OptionError } from '../errors/option.error.js';
import type { Duration } from '../interfaces/duration.interface.js';
import { toMs } from './duration.util.js';

/** @internal Runs `fn`, putting `prefix` in front of the path of an invalid option it reports. */
export function underOption<T>(prefix: string, fn: () => T): T {
  try {
    return fn();
  } catch (error) {
    if (!(error instanceof OptionError)) {
      throw error;
    }
    throw new OptionError(error.option ? `${prefix}.${error.option}` : prefix, error.problem);
  }
}

/** @internal A `Duration` option in milliseconds; `positive` rejects 0. */
export function durationOption(value: Duration, option: string, positive = false): number {
  let ms: number;
  try {
    ms = toMs(value);
  } catch (error) {
    throw new OptionError(option, (error as Error).message);
  }

  if (positive && ms === 0) {
    throw new OptionError(option, `Invalid duration ${JSON.stringify(value)}. Use a duration longer than 0.`);
  }
  return ms;
}

/** @internal A count option: a whole number of at least `min`, or `Infinity` where that means "no limit". */
export function countOption(value: number, option: string, min: number, infinite = false): number {
  if ((infinite && value === Infinity) || (Number.isInteger(value) && value >= min)) {
    return value;
  }
  throw new OptionError(
    option,
    `Invalid value ${String(value)}. Use a whole number of at least ${min}${infinite ? ', or Infinity' : ''}.`,
  );
}

/** @internal A finite number option within `[min, max]`, `min` excluded when `minExclusive`. */
export function numberOption(value: number, option: string, min: number, max: number, minExclusive = false): number {
  const aboveMin = minExclusive ? value > min : value >= min;
  if (typeof value === 'number' && Number.isFinite(value) && aboveMin && value <= max) {
    return value;
  }
  const range = `${minExclusive ? 'above' : 'of at least'} ${min}${max === Infinity ? '' : ` and at most ${max}`}`;
  throw new OptionError(option, `Invalid value ${String(value)}. Use a number ${range}.`);
}

/**
 * @internal Calls a user predicate (`retryIf`, `recordIf`, …), which must
 * return a boolean. A Promise would count as `true`, and its rejection would
 * go unhandled: it is a bug to surface.
 */
export function callPredicate<A extends unknown[]>(
  name: string,
  predicate: (...args: A) => boolean,
  ...args: A
): boolean {
  const result: unknown = predicate(...args);
  if (typeof (result as PromiseLike<unknown> | null)?.then === 'function') {
    (result as PromiseLike<unknown>).then(undefined, () => undefined);
    throw new TypeError(`${name} returned a Promise. It must return a boolean, synchronously.`);
  }
  return !!result;
}
