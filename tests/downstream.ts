import { createServer, type Server, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';

/**
 * - `up`: answers 200 with `{ ok: true, n }` (`n` counts the requests).
 * - `fail`: answers `failStatus` (503 unless set), with a `retry-after-ms` header when `retryAfterMs` is set.
 * - `hang`: never answers; the request ends when the caller aborts it.
 * - `hold`: answers 200 once `release()` is called.
 */
export type DownstreamMode = 'up' | 'fail' | 'hang' | 'hold';

export interface DownstreamRequest {
  path: string;
  method: string;
  /** The caller went away before the answer was sent. */
  aborted: boolean;
}

/** What a dependency's SDK throws for an answer other than 2xx: `status` is how resilience classifies it. */
export class DownstreamError extends Error {
  constructor(
    readonly status: number,
    readonly retryAfterMs?: number,
  ) {
    super(`Downstream answered with status ${status}`);
  }
}

/**
 * A real HTTP dependency on 127.0.0.1 that fails, hangs or throttles on
 * demand. `next(...)` scripts the modes of the upcoming requests; once the
 * script runs out, `mode` applies.
 */
export class Downstream {
  mode: DownstreamMode = 'up';
  failStatus = 503;
  retryAfterMs?: number;
  readonly requests: DownstreamRequest[] = [];
  private script: DownstreamMode[] = [];
  private held: ServerResponse[] = [];
  private server?: Server;

  get url(): string {
    const { port } = this.server!.address() as AddressInfo;
    return `http://127.0.0.1:${port}`;
  }

  async start(): Promise<void> {
    this.server = createServer((req, res) => {
      const entry: DownstreamRequest = { path: req.url ?? '/', method: req.method ?? 'GET', aborted: false };
      this.requests.push(entry);
      res.on('close', () => {
        if (!res.writableFinished) {
          entry.aborted = true;
        }
      });
      this.answer(this.script.shift() ?? this.mode, res);
    });
    await new Promise<void>((resolve) => this.server!.listen(0, '127.0.0.1', resolve));
  }

  async stop(): Promise<void> {
    this.server?.closeAllConnections();
    await new Promise((resolve) => this.server?.close(resolve));
  }

  reset(): void {
    this.mode = 'up';
    this.failStatus = 503;
    this.retryAfterMs = undefined;
    this.requests.length = 0;
    this.script = [];
    this.release();
  }

  next(...modes: DownstreamMode[]): this {
    this.script.push(...modes);
    return this;
  }

  /** Answers every held request with 200. */
  release(): void {
    for (const res of this.held.splice(0)) {
      this.ok(res);
    }
  }

  /** Calls the dependency the way an SDK would: JSON on 2xx, a `DownstreamError` otherwise. */
  async call<T = { ok: true; n: number }>(path: string, signal?: AbortSignal): Promise<T> {
    const response = await fetch(`${this.url}${path}`, { signal });
    if (!response.ok) {
      await response.body?.cancel();
      const header = response.headers.get('retry-after-ms');
      throw new DownstreamError(response.status, header === null ? undefined : Number(header));
    }

    return (await response.json()) as T;
  }

  private answer(mode: DownstreamMode, res: ServerResponse) {
    switch (mode) {
      case 'up':
        this.ok(res);
        break;
      case 'fail': {
        const headers: Record<string, string> = { 'content-type': 'application/json' };
        if (this.retryAfterMs !== undefined) {
          headers['retry-after-ms'] = String(this.retryAfterMs);
        }
        res.writeHead(this.failStatus, headers).end(JSON.stringify({ error: 'unavailable' }));
        break;
      }
      case 'hang':
        break;
      case 'hold':
        this.held.push(res);
        break;
    }
  }

  private ok(res: ServerResponse) {
    if (res.destroyed) {
      return;
    }
    res
      .writeHead(200, { 'content-type': 'application/json' })
      .end(JSON.stringify({ ok: true, n: this.requests.length }));
  }
}

/** Waits in real time, for I/O the test can't await directly. */
export async function until(condition: () => boolean, what: string, timeoutMs = 3_000): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (!condition()) {
    if (performance.now() > deadline) {
      throw new Error(`Timed out waiting for ${what}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
}

/** Sends a supertest request right away (it otherwise waits for then()). */
export function send<T extends PromiseLike<unknown>>(test: T): Promise<Awaited<T>> {
  return Promise.resolve(test.then((res) => res)) as Promise<Awaited<T>>;
}

export const fast = { delay: 1, factor: 1 };
