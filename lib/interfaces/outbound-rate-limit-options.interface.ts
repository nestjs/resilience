import type { Duration } from './duration.interface.js';

export interface OutboundRateLimitOptions {
  /** Limiters with the same name share their tokens (a preset's limiter is named after the preset). */
  name?: string;
  /** Calls per `interval`, which is also the largest burst. */
  limit: number;
  /** Longer than 0. */
  interval: Duration;
  /** Longest a call may wait for a token before it is rejected. Default 0: reject right away. */
  maxWait?: Duration;
}
