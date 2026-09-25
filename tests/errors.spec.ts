import { BadRequestException, ForbiddenException, HttpException, type ExecutionContext } from '@nestjs/common';
import type { HttpAdapterHost } from '@nestjs/core';
import { RpcException } from '@nestjs/microservices';
import { WsException } from '@nestjs/websockets';
import { GraphQLError } from 'graphql';
import { describeResilienceError, headersSent, TransportErrorMapper } from '../lib/utils/transport-errors.util.js';
import {
  BulkheadFullError,
  CircuitOpenError,
  isClientError,
  OutboundRateLimitError,
  ResilienceError,
  ResilienceTimeoutError,
} from '../lib/index.js';

function contextOf(type: string, response: object = {}): ExecutionContext {
  return {
    getType: () => type,
    switchToHttp: () => ({ getResponse: () => response, getRequest: () => ({}) }),
  } as unknown as ExecutionContext;
}

function adapterHost(sent = false) {
  const setHeader = vi.fn();
  const host = { httpAdapter: { setHeader, isHeadersSent: () => sent } } as unknown as HttpAdapterHost;
  return { host, setHeader };
}

describe('isClientError', () => {
  it('treats HttpExceptions below 500 as client errors, whatever their status', () => {
    expect(isClientError(new BadRequestException())).toBe(true);
    expect(isClientError(new ForbiddenException())).toBe(true);
    // The handler rejected its own caller: not a dependency pushing back.
    expect(isClientError(new HttpException('Too many', 429))).toBe(true);
    expect(isClientError(new HttpException('Bad gateway', 502))).toBe(false);
  });

  it("reads a dependency's 4xx from status, then statusCode, except 408 and 429", () => {
    expect(isClientError({ status: 404 })).toBe(true);
    expect(isClientError({ statusCode: 409 })).toBe(true);
    expect(isClientError({ status: 'not-a-number', statusCode: 400 })).toBe(true);
    expect(isClientError({ status: 500, statusCode: 400 })).toBe(false); // status wins
    expect(isClientError({ status: 408 })).toBe(false);
    expect(isClientError({ statusCode: 429 })).toBe(false);
    expect(isClientError({ status: 399 })).toBe(false);
    expect(isClientError({ status: 500 })).toBe(false);
  });

  it('is false for errors without a numeric status, and for non-objects', () => {
    expect(isClientError(new Error('down'))).toBe(false);
    expect(isClientError({ status: '404' })).toBe(false);
    expect(isClientError(null)).toBe(false);
    expect(isClientError(undefined)).toBe(false);
    expect(isClientError('404')).toBe(false);
    expect(isClientError(404)).toBe(false);
  });
});

describe('Resilience errors', () => {
  it('all extend ResilienceError and Error, with their class name, code and policy', () => {
    const errors = [
      [new ResilienceTimeoutError(100, 'a'), 'ResilienceTimeoutError', 'TIMEOUT'],
      [new CircuitOpenError('b', 1_000), 'CircuitOpenError', 'CIRCUIT_OPEN'],
      [new BulkheadFullError('c', 'full'), 'BulkheadFullError', 'BULKHEAD_FULL'],
      [new OutboundRateLimitError('d', 10), 'OutboundRateLimitError', 'RATE_LIMITED'],
    ] as const;
    for (const [error, name, code] of errors) {
      expect(error).toBeInstanceOf(ResilienceError);
      expect(error).toBeInstanceOf(Error);
      expect(error.name).toBe(name);
      expect(error.code).toBe(code);
    }
    expect(errors.map(([error]) => error.policy)).toEqual(['a', 'b', 'c', 'd']);
    expect(new ResilienceTimeoutError(250, 'x').message).toBe('Timed out after 250ms');
    expect(new BulkheadFullError('c', 'full').retryAfterMs).toBeUndefined();
  });
});

describe('describeResilienceError', () => {
  it('maps a timeout to 504 and rejections to 503, with generic messages', () => {
    expect(describeResilienceError(new ResilienceTimeoutError(100, 'secret-name'))).toEqual({
      code: 'TIMEOUT',
      statusCode: 504,
      message: 'The operation timed out',
    });
    expect(describeResilienceError(new BulkheadFullError('secret-name', 'queue-timeout'))).toEqual({
      code: 'BULKHEAD_FULL',
      statusCode: 503,
      message: 'Server is at capacity',
      retryAfter: undefined,
    });
    expect(describeResilienceError(new OutboundRateLimitError('secret-name', 200))).toMatchObject({
      code: 'RATE_LIMITED',
      message: 'Rate limit of a dependency exceeded',
    });
  });

  it('rounds retryAfter up to whole seconds, and never below 1', () => {
    const retryAfter = (ms: number) => describeResilienceError(new CircuitOpenError('b', ms)).retryAfter;
    expect(retryAfter(0)).toBe(1);
    expect(retryAfter(1)).toBe(1);
    expect(retryAfter(1_000)).toBe(1);
    expect(retryAfter(1_001)).toBe(2);
    expect(retryAfter(29_500)).toBe(30);
  });
});

describe('TransportErrorMapper', () => {
  it('turns an HTTP rejection into an HttpException and sets Retry-After', async () => {
    const { host, setHeader } = adapterHost();
    const response = {};
    const error = new CircuitOpenError('inventory', 2_500);
    const mapped = (await new TransportErrorMapper(host).map(error, contextOf('http', response))) as HttpException;
    expect(mapped).toBeInstanceOf(HttpException);
    expect(mapped.getStatus()).toBe(503);
    expect(mapped.getResponse()).toEqual({
      statusCode: 503,
      error: 'Service Unavailable',
      message: 'Service temporarily unavailable',
      code: 'CIRCUIT_OPEN',
    });
    expect(mapped.cause).toBe(error);
    expect(setHeader).toHaveBeenCalledWith(response, 'Retry-After', '3');
  });

  it('sets no Retry-After without a retry time, or once headers were sent', async () => {
    const fresh = adapterHost();
    await new TransportErrorMapper(fresh.host).map(new BulkheadFullError('b', 'full'), contextOf('http'));
    await new TransportErrorMapper(fresh.host).map(new ResilienceTimeoutError(10, 't'), contextOf('http'));
    expect(fresh.setHeader).not.toHaveBeenCalled();

    const sent = adapterHost(true);
    const mapped = (await new TransportErrorMapper(sent.host).map(
      new OutboundRateLimitError('partner', 500),
      contextOf('http'),
    )) as HttpException;
    expect(sent.setHeader).not.toHaveBeenCalled();
    expect(mapped.getStatus()).toBe(503);
  });

  it('maps a timeout to 504 Gateway Timeout over HTTP, even without an adapter', async () => {
    const mapped = (await new TransportErrorMapper().map(
      new ResilienceTimeoutError(10, 't'),
      contextOf('http'),
    )) as HttpException;
    expect(mapped.getStatus()).toBe(504);
    expect(mapped.getResponse()).toMatchObject({ error: 'Gateway Timeout', code: 'TIMEOUT' });
  });

  it('turns rejections into RpcException and WsException payloads', async () => {
    const mapper = new TransportErrorMapper();
    const rpc = await mapper.map(new OutboundRateLimitError('partner', 1_200), contextOf('rpc'));
    expect(rpc).toBeInstanceOf(RpcException);
    expect((rpc as RpcException).getError()).toEqual({
      status: 'error',
      code: 'RATE_LIMITED',
      statusCode: 503,
      message: 'Rate limit of a dependency exceeded',
      retryAfter: 2,
    });

    const ws = await mapper.map(new BulkheadFullError('b', 'full'), contextOf('ws'));
    expect(ws).toBeInstanceOf(WsException);
    expect((ws as WsException).getError()).toEqual({
      status: 'error',
      code: 'BULKHEAD_FULL',
      statusCode: 503,
      message: 'Server is at capacity',
    });
  });

  it('turns them into a GraphQLError with the code in extensions, keeping the original error', async () => {
    const error = new BulkheadFullError('b', 'full');
    const mapped = (await new TransportErrorMapper().map(error, contextOf('graphql'))) as GraphQLError;
    expect(mapped).toBeInstanceOf(GraphQLError);
    expect(mapped.message).toBe('Server is at capacity');
    expect(mapped.extensions).toEqual({ code: 'BULKHEAD_FULL', httpStatus: 503 });
    expect(mapped.originalError).toBe(error);
  });

  it('leaves other errors, and unknown transports, alone', async () => {
    const mapper = new TransportErrorMapper();
    const plain = new Error('down');
    expect(await mapper.map(plain, contextOf('http'))).toBe(plain);
    const timeout = new ResilienceTimeoutError(10, 't');
    expect(await mapper.map(timeout, contextOf('kafka-custom'))).toBe(timeout);
  });
});

describe('headersSent', () => {
  it("checks the Node response, Fastify's raw response, then the adapter", () => {
    expect(headersSent({ headersSent: true })).toBe(true);
    expect(headersSent({ raw: { headersSent: true }, sent: false })).toBe(true);
    expect(headersSent({ headersSent: false })).toBe(false);
    expect(headersSent({}, { isHeadersSent: () => true })).toBe(true);
    expect(headersSent(undefined)).toBe(false);
  });
});
