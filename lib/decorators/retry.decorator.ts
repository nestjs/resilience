import { SetMetadata } from '@nestjs/common';
import type { EntrypointDecorator, EntrypointRetryOptions } from '../interfaces/entrypoint-options.interface.js';
import { normalizeRetry } from '../policies/retry.policy.js';
import { RETRY_METADATA } from '../resilience.constants.js';

/**
 * Re-runs the handler (its pipes, inner interceptors and the handler itself;
 * guards run once) when it fails. A number is shorthand for `{ attempts }`,
 * and `false` turns off a retry the handler would otherwise inherit.
 *
 * On HTTP, only GET, HEAD and OPTIONS are retried, and on GraphQL only
 * queries, unless you set `idempotent: true`. Message, event and WebSocket
 * handlers are retried only when this decorator is on the handler itself.
 * Options merge with a retry from `@Resilience()` or the class, field by field.
 */
export const Retry = (options: number | false | EntrypointRetryOptions = {}): EntrypointDecorator =>
  SetMetadata(RETRY_METADATA, normalizeRetry(options));
