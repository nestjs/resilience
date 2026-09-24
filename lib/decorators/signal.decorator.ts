import { createParamDecorator } from '@nestjs/common';
import { signalOfAttempt } from '../context/attempt-scope.context.js';

/**
 * Injects the current attempt's `AbortSignal` into a handler parameter, on
 * every transport. It is aborted when the attempt times out.
 *
 * ```ts
 * @Get(':id') @Timeout('2s')
 * find(@Param('id') id: string, @Signal() signal: AbortSignal) {
 *   return this.catalog.find(id, { signal });
 * }
 * ```
 */
export const Signal: () => ParameterDecorator = createParamDecorator(signalOfAttempt);
