import type { Duration } from './duration.interface.js';

export interface BackoffOptions {
  /** Wait before the first retry. Default `200ms`. */
  delay?: Duration;
  /** Growth per retry: each wait is the previous one times `factor` (at least 1). `1` keeps it constant. Default 2. */
  factor?: number;
  /** Cap for a single wait. Default `30s`. */
  maxDelay?: Duration;
  /**
   * `full`: a random wait between 0 and the computed one, which keeps a fleet
   * of callers from retrying in lockstep. `equal`: half the computed wait plus
   * a random half. `none`: exactly the computed wait. Default `full`, or
   * `none` when `factor` is 1.
   */
  jitter?: 'full' | 'equal' | 'none';
}
