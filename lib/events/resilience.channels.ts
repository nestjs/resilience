import { channel, type Channel } from 'node:diagnostics_channel';
import type { EventSink, ResilienceEvent, ResilienceEventType } from './resilience-events.interface.js';

const TYPES: ResilienceEventType[] = [
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

const channels = Object.fromEntries(
  TYPES.map((type) => [type, channel(`nestjs:resilience:${type}`)]),
) as Record<ResilienceEventType, Channel>;

/** Where module-created policies also deliver their events (the app's `ResilienceEvents`). */
const sinks = new WeakMap<object, EventSink>();

/** @internal Delivers `policy`'s events to `sink` as well as to the diagnostics channels. */
export function attachEventSink<P extends object>(policy: P, sink: EventSink | undefined): P {
  if (sink) {
    sinks.set(policy, sink);
  }
  return policy;
}

/** @internal */
export function publish(policy: object, event: ResilienceEvent): void {
  const target = channels[event.type];
  if (target.hasSubscribers) {
    target.publish(event);
  }

  const sink = sinks.get(policy);
  if (!sink) {
    return;
  }
  try {
    sink(event);
  } catch {
    // A listener must never change the outcome of the call it observes (RxJS
    // only throws from next() here with useDeprecatedSynchronousErrorHandling).
  }
}
