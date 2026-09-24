import { SetMetadata } from '@nestjs/common';
import type { Duration } from '../interfaces/duration.interface.js';
import type { EntrypointDecorator } from '../interfaces/entrypoint-options.interface.js';
import { TIMEOUT_METADATA } from '../resilience.constants.js';

/**
 * Time budget per attempt. Without an argument, the duration comes from a
 * preset or a class-level `@Timeout()`, then from `defaults.timeout`. The
 * handler reads the matching `AbortSignal` with `@Signal()`. Answers 504 on
 * HTTP.
 */
export const Timeout = (timeout?: Duration): EntrypointDecorator => SetMetadata(TIMEOUT_METADATA, timeout ?? null);
