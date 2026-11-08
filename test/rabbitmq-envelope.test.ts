import assert from 'node:assert/strict';
import { test } from 'node:test';

import { rabbitEventEnvelope } from '../src/api/integrations/event/rabbitmq/rabbit-event-envelope';

test('does not expose the configured API key in the RabbitMQ event envelope', () => {
  const message = rabbitEventEnvelope({
    instanceName: 'watcher', event: 'messages.upsert', data: { key: { id: '42' } },
    serverUrl: 'https://example.invalid', dateTime: '2026-11-07T00:00:00Z', sender: 'sender',
    apiKey: 'secret', extra: { apikey: 'other-secret', request: 'safe' },
  });
  assert.equal('apikey' in message, false);
  assert.equal('apiKey' in message, false);
  assert.equal(message.request, 'safe');
  assert.equal(message.instance, 'watcher');
});

test('removes known credential fields from nested broker payloads', () => {
  const event = {
    instanceName: 'watcher', event: 'messages.upsert',
    data: { key: { id: '42' }, token: 'private-token', nested: { apiKey: 'private-key', text: 'hello' } },
    serverUrl: 'https://example.invalid', dateTime: '2026-11-07T00:00:00Z', sender: 'sender',
  };
  const json = JSON.stringify(rabbitEventEnvelope(event));
  assert.equal(json.includes('private-token'), false);
  assert.equal(json.includes('private-key'), false);
  assert.equal(JSON.parse(json).data.key.id, '42');
  assert.equal(JSON.parse(json).data.nested.text, 'hello');
});
