import { Injectable, type OnApplicationShutdown } from '@nestjs/common';
import { Subject, type Observable } from 'rxjs';
import type { ResilienceEvent } from './resilience-events.interface.js';

/**
 * The application's resilience events: every event from the policies it
 * created (decorators, presets, `ResilienceService.create()`). The same
 * events go to the `nestjs:resilience:<type>` diagnostics channels, which
 * also carry events of policies created with `new`. `events$` completes when
 * the application shuts down.
 */
@Injectable()
export class ResilienceEvents implements OnApplicationShutdown {
  private readonly subject = new Subject<ResilienceEvent>();
  readonly events$: Observable<ResilienceEvent> = this.subject.asObservable();

  /** @internal The sink attached to module-created policies. */
  readonly emit = (event: ResilienceEvent): void => {
    this.subject.next(event);
  };

  onApplicationShutdown() {
    this.subject.complete();
  }
}
