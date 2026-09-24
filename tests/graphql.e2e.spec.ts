import { ApolloDriver, type ApolloDriverConfig } from '@nestjs/apollo';
import { Module, type INestApplication } from '@nestjs/common';
import { GraphQLModule, Mutation, Query, Resolver } from '@nestjs/graphql';
import request from 'supertest';
import { createApp } from './support/adapters.js';
import { CircuitBreaker, Fallback, ResilienceModule, Retry, Signal, Timeout, ResilienceTimeoutError } from '../lib/index.js';

const fast = { delay: 1, factor: 1 };
const state = { calls: {} as Record<string, number>, signals: [] as AbortSignal[] };
const hit = (name: string) => (state.calls[name] = (state.calls[name] ?? 0) + 1);

@Resolver()
@Retry({ attempts: 3, backoff: fast }) // class-level: queries are retried, mutations are not
class CatalogResolver {
  @Query(() => String)
  flakyQuery() {
    if (hit('flakyQuery') < 3) {
      throw new Error('flaky');
    }
    return 'ok';
  }

  @Mutation(() => String, { nullable: true })
  placeOrder() {
    hit('placeOrder');
    throw new Error('database down');
  }

  @Mutation(() => String)
  @Retry({ attempts: 3, backoff: fast, idempotent: true })
  setPreference() {
    if (hit('setPreference') < 2) {
      throw new Error('database down');
    }
    return 'saved';
  }

  @Query(() => String, { nullable: true })
  @Timeout(30)
  async slowQuery(@Signal() signal: AbortSignal) {
    state.signals.push(signal);
    await new Promise((_, reject) => signal.addEventListener('abort', () => reject(signal.reason)));
    return 'never';
  }

  @Query(() => String, { nullable: true })
  @Retry({ attempts: 1 })
  @CircuitBreaker({ minimumCalls: 1, openDuration: 10_000 })
  brokenQuery() {
    hit('brokenQuery');
    throw new Error('upstream down');
  }

  @Query(() => [String])
  @Fallback(() => ['bestseller'])
  recommendations(): string[] {
    throw new Error('recommender down');
  }
}

@Module({
  imports: [
    GraphQLModule.forRoot<ApolloDriverConfig>({ driver: ApolloDriver, autoSchemaFile: true }),
    ResilienceModule.forRoot(),
  ],
  providers: [CatalogResolver],
})
class GraphqlAppModule {}

describe('Resilience decorators on GraphQL resolvers (apollo on express)', () => {
  let app: INestApplication;

  beforeAll(async () => {
    app = await createApp('express', GraphqlAppModule, { setup: (a) => a.useLogger(false) });
  });
  afterAll(() => app.close());
  beforeEach(() => {
    state.calls = {};
    state.signals = [];
  });

  const gql = (query: string) => request(app.getHttpServer()).post('/graphql').send({ query });

  it('retries queries', async () => {
    const res = await gql('{ flakyQuery }');
    expect(res.body).toEqual({ data: { flakyQuery: 'ok' } });
    expect(state.calls.flakyQuery).toBe(3);
  });

  it('does not retry mutations unless declared idempotent', async () => {
    const res = await gql('mutation { placeOrder }');
    expect(res.body.data).toEqual({ placeOrder: null });
    expect(state.calls.placeOrder).toBe(1);

    expect((await gql('mutation { setPreference }')).body).toEqual({ data: { setPreference: 'saved' } });
    expect(state.calls.setPreference).toBe(2);
  });

  it('reports a timeout as a GraphQL error with extensions.code', async () => {
    const res = await gql('{ slowQuery }');
    expect(res.status).toBe(200);
    expect(res.body.data).toEqual({ slowQuery: null });
    expect(res.body.errors[0]).toMatchObject({
      message: 'The operation timed out',
      path: ['slowQuery'],
      extensions: { code: 'TIMEOUT', httpStatus: 504 },
    });
    expect(state.signals[0].reason).toBeInstanceOf(ResilienceTimeoutError);
  });

  it('reports an open breaker with retryAfter', async () => {
    await gql('{ brokenQuery }');
    const res = await gql('{ brokenQuery }');
    expect(res.body.errors[0].extensions).toMatchObject({ code: 'CIRCUIT_OPEN', httpStatus: 503, retryAfter: 10 });
    expect(state.calls.brokenQuery).toBe(1);
  });

  it('resolves the field with the fallback value', async () => {
    expect((await gql('{ recommendations }')).body).toEqual({ data: { recommendations: ['bestseller'] } });
  });
});
