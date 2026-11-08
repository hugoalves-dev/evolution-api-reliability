import type { EmitData } from '../event.controller';
import { eventIdFor } from './event-id';
import { rabbitEventEnvelope } from './rabbit-event-envelope';
import type { RabbitSpoolEvent } from './rabbit-spool-store';

export interface RabbitDestinationSettings {
  localEnabled: boolean;
  localEvents: string[];
  globalEnabled: boolean;
  globalExternalQueue?: boolean;
  globalEvents: Record<string, boolean>;
  globalExchange: string;
  prefix: string;
}

/** Capture configured destinations and the sanitized envelope before any broker call. */
export function planRabbitDeliveries(event: EmitData, settings: RabbitDestinationSettings): RabbitSpoolEvent[] {
  const name = event.event.replace(/[.-]/g, '_').toUpperCase();
  const eventName = event.event.replace(/_/g, '.').toLowerCase();
  const eventId = eventIdFor(event.event, event.data);
  const instance = event.instanceName || 'global';
  const payload = JSON.stringify(rabbitEventEnvelope(event));
  const common = { instance, eventId, route: event.event, payload };
  const result: RabbitSpoolEvent[] = [];
  if (settings.localEnabled && settings.localEvents.includes(name)) {
    result.push({ ...common, exchange: instance, queue: `${instance}.${eventName}` });
  }
  if (settings.globalEnabled && settings.globalEvents[name]) {
    const queue = settings.globalExternalQueue ? '' : settings.prefix ? `${settings.prefix}.${eventName}` : eventName;
    result.push({ ...common, exchange: settings.globalExchange, queue });
  }
  return result;
}
