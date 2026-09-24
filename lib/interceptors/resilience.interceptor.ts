import {
  Inject,
  Injectable,
  Optional,
  type CallHandler,
  type ExecutionContext,
  type NestInterceptor,
} from '@nestjs/common';
import { HttpAdapterHost } from '@nestjs/core';
import { catchError, from, mergeMap, Observable, throwError } from 'rxjs';
import { ResilienceError } from '../errors/resilience.error.js';
import type { ResilienceModuleOptions } from '../interfaces/resilience-module-options.interface.js';
import { observe } from '../policies/resilience.policy.js';
import { RESILIENCE_MODULE_OPTIONS } from '../resilience.module-definition.js';
import { EntrypointPlanner, type EntrypointPlan } from '../services/entrypoint-planner.service.js';
import { headersSent, TransportErrorMapper } from '../utils/transport-errors.util.js';

/** RFC 9110 safe methods: repeating them has no side effects by definition. */
const SAFE_HTTP_METHODS = new Set(['GET', 'HEAD', 'OPTIONS']);

/**
 * Registered globally by `ResilienceModule` (`APP_INTERCEPTOR`), so it runs
 * for every HTTP route, resolver, message handler and gateway message, and is
 * a no-op (apart from error mapping) for handlers without resilience
 * metadata. Being global, it is outside controller- and method-level
 * interceptors: a retry re-runs those, the pipes and the handler, but not
 * guards or middleware.
 */
@Injectable()
export class ResilienceInterceptor implements NestInterceptor {
  private readonly mapper: TransportErrorMapper;
  private readonly mapErrors: boolean;

  constructor(
    private readonly planner: EntrypointPlanner,
    @Optional() private readonly adapterHost?: HttpAdapterHost,
    @Optional() @Inject(RESILIENCE_MODULE_OPTIONS) options?: ResilienceModuleOptions,
  ) {
    this.mapper = new TransportErrorMapper(adapterHost);
    this.mapErrors = options?.mapErrors ?? true;
  }

  intercept(context: ExecutionContext, next: CallHandler): Observable<unknown> {
    const plan = this.planner.planFor(context.getClass(), context.getHandler());
    const result$ = plan ? this.execute(plan, context, next) : next.handle();

    if (!this.mapErrors) {
      return result$;
    }

    return result$.pipe(
      catchError((error) =>
        // Only resilience errors are mapped (and the transport package they
        // need is imported lazily); every other error is rethrown as is.
        error instanceof ResilienceError
          ? from(this.mapper.map(error, context)).pipe(mergeMap((mapped) => throwError(() => mapped)))
          : throwError(() => error),
      ),
    );
  }

  private execute(plan: EntrypointPlan, context: ExecutionContext, next: CallHandler) {
    const policy = plan.hasRetry && this.canRepeat(plan, context) ? plan.withRetry : plan.withoutRetry;
    // A fresh next.handle() per attempt. observe() calls it inside the
    // attempt's async context, and Nest binds pipes and the handler there,
    // so @Signal() and ResilienceContext see this attempt.
    return observe(policy, () => next.handle(), {
      source: plan.source,
      invocation: context,
      // Nest sends an SSE route's headers before its first event: there,
      // only an event (which observe() tracks) commits the response.
      committed: plan.sse ? undefined : () => this.isCommitted(context),
    });
  }

  private canRepeat(plan: EntrypointPlan, context: ExecutionContext): boolean {
    if (plan.requestStream) {
      return false;
    }
    if (plan.idempotent) {
      return true;
    }

    switch (context.getType<string>()) {
      case 'http': {
        const method = context.switchToHttp().getRequest()?.method;
        return SAFE_HTTP_METHODS.has(String(method).toUpperCase());
      }
      case 'graphql':
        return context.getArgByIndex(3)?.operation?.operation === 'query';
      default:
        // Message, event and WebSocket handlers carry no safety signal: only
        // a @Retry() on the handler itself opts in. A retry inherited from a
        // preset or the class (typically written for GET routes) would
        // silently re-run side effects here.
        return plan.retryOnMethod;
    }
  }

  /** A response whose headers went out can't be retried, replaced or turned into a 504. */
  private isCommitted(context: ExecutionContext): boolean {
    if (context.getType() !== 'http') {
      return false;
    }
    return headersSent(context.switchToHttp().getResponse(), this.adapterHost?.httpAdapter);
  }
}
