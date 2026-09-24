import { ConfigurableModuleBuilder } from '@nestjs/common';
import type { ResilienceModuleOptions } from './interfaces/resilience-module-options.interface.js';

export const {
  ConfigurableModuleClass,
  MODULE_OPTIONS_TOKEN: RESILIENCE_MODULE_OPTIONS,
  OPTIONS_TYPE,
  ASYNC_OPTIONS_TYPE,
} = new ConfigurableModuleBuilder<ResilienceModuleOptions>({ moduleName: 'Resilience' })
  .setClassMethodName('forRoot')
  .setFactoryMethodName('createResilienceOptions')
  .setExtras({ isGlobal: true }, (definition, extras) => ({
    ...definition,
    global: extras.isGlobal,
  }))
  .build();

/** What a class passed to `forRootAsync({ useClass })` implements. */
export interface ResilienceOptionsFactory {
  createResilienceOptions(): ResilienceModuleOptions | Promise<ResilienceModuleOptions>;
}

/** What `ResilienceModule.forRootAsync()` takes. */
export type ResilienceModuleAsyncOptions = typeof ASYNC_OPTIONS_TYPE;
