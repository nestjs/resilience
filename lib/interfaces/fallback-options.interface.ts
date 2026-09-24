import type { Observable } from 'rxjs';
import type { AttemptContext } from './execute-options.interface.js';

/** Returns what the call should have returned: a value, a Promise or an Observable. */
export type FallbackHandler<R = unknown> = (error: unknown, context: AttemptContext) => R | PromiseLike<R> | Observable<R>;

export interface FallbackOptions {
  /** Labels its events. */
  name?: string;
  /** Which errors the fallback replaces. Default: all but client errors (`isClientError()`). */
  handleIf?: (error: unknown) => boolean;
}
