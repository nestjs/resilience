import { subscribe, unsubscribe } from 'node:diagnostics_channel';
import { onTestFinished } from 'vitest';
import type { ResilienceEvent } from '../lib/index.js';

const TYPES: ResilienceEvent['type'][] = [
  'retry',
  'timeout',
  'circuit-open',
  'circuit-half-open',
  'circuit-closed',
  'circuit-rejected',
  'bulkhead-rejected',
  'rate-limited',
  'fallback',
];

/** Records what policies publish on the `nestjs:resilience:<type>` channels during the current test. */
export function recordEvents(): ResilienceEvent[] {
  const events: ResilienceEvent[] = [];
  const listener = (message: unknown) => events.push(message as ResilienceEvent);
  for (const type of TYPES) {
    subscribe(`nestjs:resilience:${type}`, listener);
  }
  onTestFinished(() => {
    for (const type of TYPES) {
      unsubscribe(`nestjs:resilience:${type}`, listener);
    }
  });
  return events;
}
