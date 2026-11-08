import assert from 'node:assert/strict';
import { test } from 'node:test';

import { planRabbitDeliveries } from '../src/api/integrations/event/rabbitmq/rabbit-delivery-plan';

const event = { instanceName: 'tenant-a', origin: 'test', event: 'messages.upsert',
  data: { key: { id: 'message-42' } }, serverUrl: 'https://example.invalid',
  dateTime: '2026-11-07T00:00:00Z', sender: 'sender', apiKey: 'private-key' };

test('plans durable local and global deliveries with the same stable identity and no API key', () => {
  const plan = planRabbitDeliveries(event, { localEnabled: true, localEvents: ['MESSAGES_UPSERT'],
    globalEnabled: true, globalEvents: { MESSAGES_UPSERT: true }, globalExchange: 'watcher.evolution', prefix: 'watcher' });
  assert.equal(plan.length, 2);
  assert.equal(plan[0].eventId, 'message-42');
  assert.equal(plan[1].eventId, 'message-42');
  assert.equal(plan[0].exchange, 'tenant-a');
  assert.equal(plan[1].exchange, 'watcher.evolution');
  assert.equal(plan[1].queue, 'watcher.messages.upsert');
  assert.equal(JSON.parse(plan[1].payload).apikey, undefined);
});

test('does not persist events disabled for both destinations', () => {
  assert.deepEqual(planRabbitDeliveries(event, { localEnabled: false, localEvents: [],
    globalEnabled: true, globalEvents: {}, globalExchange: 'watcher.evolution', prefix: 'watcher' }), []);
});

test('can require an externally managed global binding without creating an extra queue', () => {
  const plan = planRabbitDeliveries(event, { localEnabled: false, localEvents: [], globalEnabled: true,
    globalEvents: { MESSAGES_UPSERT: true }, globalExchange: 'watcher.evolution', prefix: 'watcher',
    globalExternalQueue: true });
  assert.equal(plan.length, 1);
  assert.equal(plan[0].exchange, 'watcher.evolution');
  assert.equal(plan[0].queue, '');
});
