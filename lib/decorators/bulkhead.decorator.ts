import { SetMetadata } from '@nestjs/common';
import type { BulkheadOptions } from '../interfaces/bulkhead-options.interface.js';
import type { EntrypointDecorator } from '../interfaces/entrypoint-options.interface.js';
import { BULKHEAD_METADATA } from '../resilience.constants.js';

/**
 * Concurrency limit, per handler unless named. A per-handler bulkhead is
 * registered under the handler's name; see `ResilienceService.bulkhead()`.
 * A string refers to a named bulkhead configured elsewhere. Answers 503 when
 * full.
 */
export const Bulkhead = (options: BulkheadOptions | string = {}): EntrypointDecorator =>
  SetMetadata(BULKHEAD_METADATA, options);
