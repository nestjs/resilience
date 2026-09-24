import { Injectable, Logger, RequestMethod, type OnModuleInit, type Type } from '@nestjs/common';
import {
  CONTROLLER_WATERMARK,
  ENTRY_PROVIDER_WATERMARK,
  METHOD_METADATA,
  ROUTE_ARGS_METADATA,
} from '@nestjs/common/constants.js';
import { DiscoveryService, MetadataScanner } from '@nestjs/core';
import { signalOfAttempt } from './context/attempt-scope.context.js';
import { ResilienceInterceptor } from './interceptors/resilience.interceptor.js';
import { RESILIENCE_METADATA_KEYS } from './resilience.constants.js';
import { EntrypointPlanner, type Entrypoint } from './services/entrypoint-planner.service.js';

type Wrapper = ReturnType<DiscoveryService['getProviders']>[number];

const GATEWAY_METADATA = 'websockets:is_gateway';
/** Set by `@nestjs/schedule`'s `@Timeout()` (checked against @nestjs/schedule 12). */
const SCHEDULE_TIMEOUT_OPTIONS = 'SCHEDULE_TIMEOUT_OPTIONS';
/** Method metadata that marks a handler: HTTP route, message/event pattern, WS message, GraphQL resolver. */
const HANDLER_METADATA = [
  'path',
  'microservices:pattern',
  'message',
  'graphql:resolver_type',
  'graphql:resolve_property',
];
const UNSAFE_HTTP_METHODS = new Set([
  RequestMethod.POST,
  RequestMethod.PUT,
  RequestMethod.PATCH,
  RequestMethod.DELETE,
]);

/**
 * Runs once at bootstrap (`onModuleInit` of a global module, before any
 * request). It builds every entrypoint's pipeline up front, so misconfiguration
 * (unknown preset, missing fallback method, conflicting named breakers) fails
 * the boot instead of the first request, and it warns about decorators that
 * will have no effect.
 */
@Injectable()
export class ResilienceExplorer implements OnModuleInit {
  private readonly logger = new Logger('ResilienceModule');

  constructor(
    private readonly discovery: DiscoveryService,
    private readonly scanner: MetadataScanner,
    private readonly planner: EntrypointPlanner,
  ) {}

  onModuleInit() {
    const copies = this.discovery
      .getProviders()
      .filter((wrapper) => !wrapper.isAlias && wrapper.metatype === ResilienceInterceptor).length;

    if (copies > 1) {
      throw new Error(
        `ResilienceModule is registered ${copies} times (forRoot() or forRootAsync() in more than one module). ` +
          `Each registration adds an app-wide interceptor, so every handler would run under ${copies} pipelines, ` +
          `with retries multiplied and breakers split. Register it once, in the root module: it is global.`,
      );
    }

    const entrypoints: Entrypoint[] = [];
    const signalUsers: Array<{ handler: Function; name: string }> = [];
    const seen = new Set<Function>();

    const visit = (wrapper: Wrapper, isController: boolean) => {
      const cls = classOf(wrapper);
      if (!cls || seen.has(cls)) {
        return;
      }

      seen.add(cls);
      const methods = this.scanner.getAllMethodNames(cls.prototype);
      const entryClass = isController || isEntryClass(cls);
      if (entryClass) {
        for (const method of methods) {
          const handler = cls.prototype[method];
          if (!isHandler(handler)) {
            continue;
          }

          if (usesSignal(cls, method)) {
            signalUsers.push({ handler, name: `${cls.name}.${method}` });
          }

          // The other @Timeout(): @nestjs/schedule's schedules a one-off job.
          const scheduled: { timeout?: number } | undefined = Reflect.getMetadata(SCHEDULE_TIMEOUT_OPTIONS, handler);
          if (scheduled) {
            this.logger.warn(
              `${cls.name}.${method}() has @nestjs/schedule's @Timeout(${scheduled.timeout ?? ''}), which calls it ` +
                `once after startup. For a time budget on each call, import Timeout from @nestjs/resilience.`,
            );
          }
        }
      }

      const decorated = methods.filter((m) => hasResilienceMetadata(cls.prototype[m]));
      if (!decorated.length && !hasResilienceMetadata(cls)) {
        return;
      }

      if (!entryClass) {
        const targets = decorated.length ? decorated.map((m) => `${cls.name}.${m}()`) : [cls.name];
        this.logger.warn(
          `Resilience decorators on ${targets.join(', ')} have no effect: ${cls.name} is not a controller, ` +
            `resolver or gateway. They apply to entrypoints only. Inside services, use a policy object ` +
            `(ResilienceService.preset() or create(), or new RetryPolicy() etc.).`,
        );
        return;
      }

      for (const method of methods) {
        const handler = cls.prototype[method];
        if (isHandler(handler)) {
          entrypoints.push({ cls, handler, isStatic: wrapper.isDependencyTreeStatic() });
        } else if (decorated.includes(method)) {
          this.logger.warn(
            `Resilience decorators on ${cls.name}.${method}() have no effect: it is not a route, ` +
              `resolver or message handler.`,
          );
        }
      }
    };

    this.discovery.getControllers().forEach((wrapper) => visit(wrapper, true));
    this.discovery.getProviders().forEach((wrapper) => visit(wrapper, false));

    const { planned, warnings } = this.planner.prepare(entrypoints);
    for (const warning of warnings) {
      this.logger.warn(warning);
    }

    const withPlan = new Set(planned.map(({ handler }) => handler));
    for (const { handler, name } of signalUsers) {
      if (withPlan.has(handler)) {
        continue;
      }
      this.logger.warn(
        `@Signal() on ${name}() is always undefined: the handler has no resilience ` +
          `decorators. Add @Timeout() or @Resilience() to give it an attempt signal.`,
      );
    }

    for (const { handler, plan } of planned) {
      if (!plan.hasRetry || !plan.retryOnMethod) {
        continue;
      }
      if (plan.requestStream) {
        this.logger.warn(
          `@Retry() on ${plan.source}() is inactive: the handler reads a gRPC request stream, ` +
            `which a second attempt couldn't read again.`,
        );
        continue;
      }
      if (plan.idempotent) {
        continue;
      }

      const httpMethod: RequestMethod | undefined = Reflect.getMetadata(METHOD_METADATA, handler);
      const unsafe =
        (httpMethod !== undefined && UNSAFE_HTTP_METHODS.has(httpMethod)) ||
        Reflect.getMetadata('graphql:resolver_type', handler) === 'Mutation';
      if (unsafe) {
        this.logger.warn(
          `@Retry() on ${plan.source}() is inactive: it only re-runs safe operations ` +
            `(GET, HEAD, OPTIONS, GraphQL queries). If the handler is safe to repeat, use @Retry({ idempotent: true }).`,
        );
      }
    }
  }
}

function classOf(wrapper: Wrapper): Type | undefined {
  if (wrapper.isAlias) {
    return undefined;
  }

  const { metatype, inject, instance } = wrapper;
  if (!inject && typeof metatype === 'function' && metatype.prototype) {
    return metatype as Type;
  }
  const ctor = instance && typeof instance === 'object' ? (instance as object).constructor : undefined;
  return ctor && ctor !== Object ? (ctor as Type) : undefined;
}

function hasResilienceMetadata(target: unknown): boolean {
  if (typeof target !== 'function') {
    return false;
  }
  return RESILIENCE_METADATA_KEYS.some((key) => Reflect.getMetadata(key, target) !== undefined);
}

function usesSignal(cls: Type, method: string): boolean {
  const args: Record<string, { factory?: unknown }> | undefined = Reflect.getMetadata(ROUTE_ARGS_METADATA, cls, method);
  return !!args && Object.values(args).some((arg) => arg.factory === signalOfAttempt);
}

function isEntryClass(cls: Type): boolean {
  return [CONTROLLER_WATERMARK, ENTRY_PROVIDER_WATERMARK, GATEWAY_METADATA].some((key) =>
    Reflect.getMetadata(key, cls),
  );
}

function isHandler(fn: unknown): fn is Function {
  return (
    typeof fn === 'function' && HANDLER_METADATA.some((key) => Reflect.getMetadata(key, fn) !== undefined)
  );
}
