import { HttpException, HttpStatus, type ExecutionContext } from '@nestjs/common';
import type { HttpAdapterHost } from '@nestjs/core';
import type * as Microservices from '@nestjs/microservices';
import type * as Websockets from '@nestjs/websockets';
import type * as GraphQL from 'graphql';
import { BulkheadFullError } from '../errors/bulkhead-full.error.js';
import { CircuitOpenError } from '../errors/circuit-open.error.js';
import { OutboundRateLimitError } from '../errors/outbound-rate-limit.error.js';
import { RejectionError } from '../errors/rejection.error.js';
import { ResilienceTimeoutError } from '../errors/resilience-timeout.error.js';

export interface ResilienceErrorDescription {
  code: string;
  statusCode: HttpStatus.SERVICE_UNAVAILABLE | HttpStatus.GATEWAY_TIMEOUT;
  /** Generic on purpose: breaker and bulkhead names are internals. */
  message: string;
  /** Whole seconds, when the policy knows. */
  retryAfter?: number;
}

/**
 * - `ResilienceTimeoutError` → 504. The handler ran out of time, usually waiting on a
 *   dependency, which is what 504 says. 408 would claim the *client* was too
 *   slow to send its request, and some clients and proxies repeat 408s
 *   automatically, even for POST.
 * - `CircuitOpenError`, `BulkheadFullError`, `OutboundRateLimitError` → 503.
 *   The server (or a dependency) is temporarily unable to serve anyone. 429
 *   would blame this client's request rate. A breaker also knows when it
 *   will let calls through again, which becomes `Retry-After`.
 */
export function describeResilienceError(error: ResilienceTimeoutError | RejectionError): ResilienceErrorDescription {
  if (error instanceof ResilienceTimeoutError) {
    return { code: error.code, statusCode: HttpStatus.GATEWAY_TIMEOUT, message: 'The operation timed out' };
  }

  const retryAfter =
    error.retryAfterMs === undefined ? undefined : Math.max(1, Math.ceil(error.retryAfterMs / 1000));
  const message =
    error instanceof CircuitOpenError
      ? 'Service temporarily unavailable'
      : error instanceof BulkheadFullError
        ? 'Server is at capacity'
        : error instanceof OutboundRateLimitError
          ? 'Rate limit of a dependency exceeded'
          : error.message;
  return { code: error.code, statusCode: HttpStatus.SERVICE_UNAVAILABLE, message, retryAfter };
}

/**
 * Converts resilience errors into the error type each transport's exception
 * filters understand. Other errors pass through untouched. The transport
 * packages are imported lazily: when a context of that type exists, the
 * package is installed.
 */
export class TransportErrorMapper {
  private microservices?: typeof Microservices;
  private websockets?: typeof Websockets;
  private graphql?: typeof GraphQL;

  constructor(private readonly adapterHost?: HttpAdapterHost) {}

  async map(error: unknown, context: ExecutionContext): Promise<unknown> {
    if (!(error instanceof ResilienceTimeoutError || error instanceof RejectionError)) {
      return error;
    }
    const { code, statusCode, message, retryAfter } = describeResilienceError(error);
    const extra = retryAfter === undefined ? {} : { retryAfter };

    switch (context.getType<string>()) {
      case 'http': {
        const adapter = this.adapterHost?.httpAdapter;
        const response = context.switchToHttp().getResponse();
        if (retryAfter !== undefined && adapter && !headersSent(response, adapter)) {
          adapter.setHeader(response, 'Retry-After', String(retryAfter));
        }
        return new HttpException(
          { statusCode, error: HttpStatusText[statusCode], message, code },
          statusCode,
          { cause: error },
        );
      }
      case 'rpc': {
        this.microservices ??= await import('@nestjs/microservices');
        return new this.microservices.RpcException({ status: 'error', code, statusCode, message, ...extra });
      }
      case 'ws': {
        this.websockets ??= await import('@nestjs/websockets');
        return new this.websockets.WsException({ status: 'error', code, statusCode, message, ...extra });
      }
      case 'graphql': {
        this.graphql ??= await import('graphql');
        return new this.graphql.GraphQLError(message, {
          originalError: error,
          // `httpStatus`, not `extensions.http`: Apollo would apply the latter
          // to the whole response although only this field failed.
          extensions: { code, httpStatus: statusCode, ...extra },
        });
      }
      default:
        return error;
    }
  }
}

const HttpStatusText: Record<number, string> = {
  [HttpStatus.SERVICE_UNAVAILABLE]: 'Service Unavailable',
  [HttpStatus.GATEWAY_TIMEOUT]: 'Gateway Timeout',
};

/**
 * @internal Whether an HTTP response's headers went out. The Node response is
 * checked too: Fastify's `reply.sent`, which its adapter reports, only turns
 * true once the response ended, while headers written through `reply.raw`
 * are already out.
 */
export function headersSent(response: unknown, adapter?: { isHeadersSent(response: unknown): boolean }): boolean {
  const raw = (response as { raw?: unknown } | undefined)?.raw ?? response;
  if ((raw as { headersSent?: unknown } | undefined)?.headersSent === true) {
    return true;
  }
  return !!adapter?.isHeadersSent(response);
}
