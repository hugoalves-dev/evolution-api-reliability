import assert from 'node:assert/strict';
import { test } from 'node:test';

import { eventIdFor } from '../src/api/integrations/event/rabbitmq/event-id';

test('uses the WhatsApp message key for repeated message upserts', () => {
  const data = { key: { id: 'message-123' } };
  assert.equal(eventIdFor('messages.upsert', data), 'message-123');
  assert.equal(eventIdFor('messages.upsert', data), 'message-123');
});

test('does not coalesce distinct contact and connection observations', () => {
  assert.notEqual(eventIdFor('contacts.update', {}), eventIdFor('contacts.update', {}));
  assert.notEqual(eventIdFor('connection.update', {}), eventIdFor('connection.update', {}));
});

test('uses an AMQP-safe identity for missing, blank, or oversized keys', () => {
  for (const id of ['', ' ', 'x'.repeat(256), '🔥'.repeat(64)]) {
    const eventId = eventIdFor('messages.upsert', { key: { id } });
    assert.ok(eventId.trim());
    assert.ok(Buffer.byteLength(eventId, 'utf8') <= 255);
    assert.notEqual(eventId, id);
  }
});
