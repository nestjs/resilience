import type { Duration } from './duration.interface.js';

export interface TimeoutOptions {
  /** Budget per attempt, longer than 0. */
  timeout: Duration;
  /** Labels its events and errors. */
  name?: string;
}
