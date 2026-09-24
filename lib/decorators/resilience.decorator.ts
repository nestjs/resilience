import { SetMetadata } from '@nestjs/common';
import type { EntrypointDecorator } from '../interfaces/entrypoint-options.interface.js';
import { PRESET_METADATA } from '../resilience.constants.js';

/**
 * Applies a preset from `ResilienceModule.forRoot({ presets })`, stage by
 * stage. Decorators on the handler refine the preset's stages; the preset's
 * breaker, bulkhead and outbound rate limit are shared by everything using it.
 */
export const Resilience = (preset: string): EntrypointDecorator => SetMetadata(PRESET_METADATA, preset);
