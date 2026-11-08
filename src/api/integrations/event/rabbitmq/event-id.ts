import { createHash, randomUUID } from 'node:crypto';

/** Stable for supported message upserts; other observations receive a distinct ID per emission. */
export function eventIdFor(event: string, data: object): string {
  if (event.replace(/_/g, '.').toLowerCase() === 'messages.upsert') {
    const key = (data as { key?: unknown } | null)?.key;
    const id = key && typeof key === 'object' ? (key as { id?: unknown }).id : undefined;
    if (typeof id === 'string' && id.trim()) {
      return Buffer.byteLength(id, 'utf8') <= 255 ? id : `sha256:${createHash('sha256').update(id).digest('hex')}`;
    }
  }
  return randomUUID();
}
