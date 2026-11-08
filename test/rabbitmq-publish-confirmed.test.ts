import assert from 'node:assert/strict';
import { test } from 'node:test';
import { EventEmitter } from 'node:events';
import type { ConfirmChannel } from 'amqplib/callback_api';

import { publishConfirmed } from '../src/api/integrations/event/rabbitmq/publish-confirmed';

type Ack = (error?: Error | null) => void;

function channelWithCallback(published: { options?: object; ack?: Ack }, backpressure = true) {
  return Object.assign(new EventEmitter(), {
    publish(_exchange: string, _key: string, _content: Buffer, options: object, callback: Ack) {
      published.options = options;
      published.ack = callback;
      return backpressure;
    },
  }) as unknown as ConfirmChannel;
}

test('resolves only after broker ACK and marks message persistent', async () => {
  const captured: { options?: object; ack?: Ack } = {};
  const result = publishConfirmed(channelWithCallback(captured), 'exchange', 'key', Buffer.from('test'));
  assert.equal((captured.options as { persistent: boolean }).persistent, true);

  let settled = false;
  void result.then(() => { settled = true; });
  await Promise.resolve();
  assert.equal(settled, false);

  captured.ack?.(null);
  await result;
  assert.equal(settled, true);
});

test('broker NACK rejects publication', async () => {
  const captured: { ack?: Ack } = {};
  const result = publishConfirmed(channelWithCallback(captured), 'exchange', 'key', Buffer.from('test'));
  captured.ack?.(new Error('broker nack'));
  await assert.rejects(result, /broker nack/);
});

test('false publish return means backpressure, not a publish failure', async () => {
  const captured: { ack?: Ack } = {};
  const result = publishConfirmed(channelWithCallback(captured, false), 'exchange', 'key', Buffer.from('test'));
  captured.ack?.(null);
  await result;
});

test('synchronous closed-channel failure rejects', async () => {
  const broken = Object.assign(new EventEmitter(), {
    publish() { throw new Error('channel closed'); },
  }) as unknown as ConfirmChannel;
  await assert.rejects(
    publishConfirmed(broken, 'exchange', 'key', Buffer.from('test')),
    /channel closed/,
  );
});

test('rejects a mandatory publication returned as unroutable even if broker confirms it', async () => {
  const channel = new EventEmitter() as EventEmitter & { publish?: (...args: any[]) => boolean };
  let ack: Ack | undefined;
  let properties: { mandatory?: boolean; correlationId?: string } | undefined;
  channel.publish = (_exchange, _key, _content, options, callback) => {
    properties = options;
    ack = callback;
    return true;
  };
  const result = publishConfirmed(channel as unknown as ConfirmChannel, 'exchange', 'missing', Buffer.from('test'));
  assert.equal(properties?.mandatory, true);
  assert.ok(properties?.correlationId);
  channel.emit('return', { properties: { correlationId: properties.correlationId }, fields: { replyCode: 312 } });
  ack?.(null);
  await assert.rejects(result, /unroutable/i);
  assert.equal(channel.listenerCount('return'), 0);
});

test('matches concurrent returns to the right publication without one listener per message', async () => {
  const channel = new EventEmitter() as EventEmitter & { publish?: (...args: any[]) => boolean };
  const pending: { correlationId: string; ack: Ack }[] = [];
  channel.publish = (_exchange, _key, _content, options, callback) => {
    pending.push({ correlationId: options.correlationId, ack: callback });
    return true;
  };
  const first = publishConfirmed(channel as unknown as ConfirmChannel, 'exchange', 'first', Buffer.from('first'));
  const second = publishConfirmed(channel as unknown as ConfirmChannel, 'exchange', 'second', Buffer.from('second'));
  assert.equal(channel.listenerCount('return'), 1);
  channel.emit('return', { properties: { correlationId: pending[0].correlationId } });
  pending[0].ack(null);
  pending[1].ack(null);
  await assert.rejects(first, /unroutable/i);
  await second;
  assert.equal(channel.listenerCount('return'), 0);
});

test('preserves the producer event identity in messageId across publication retries', async () => {
  const captured: { options?: object; ack?: Ack } = {};
  const channel = channelWithCallback(captured);
  const first = publishConfirmed(channel, 'exchange', 'key', Buffer.from('test'), 'event-42');
  assert.equal((captured.options as { messageId: string }).messageId, 'event-42');
  captured.ack?.(new Error('confirm lost'));
  await assert.rejects(first);
  const retry = publishConfirmed(channel, 'exchange', 'key', Buffer.from('test'), 'event-42');
  assert.equal((captured.options as { messageId: string }).messageId, 'event-42');
  captured.ack?.(null);
  await retry;
});

test('rejects a missing confirmation after the deadline and removes its return listener', async () => {
  const captured: { ack?: Ack } = {};
  const channel = channelWithCallback(captured);
  await Promise.race([
    assert.rejects(publishConfirmed(channel, 'exchange', 'key', Buffer.from('test'), 'id', 20), /timed out/i),
    new Promise((_, reject) => setTimeout(() => reject(new Error('confirmation did not settle')), 200)),
  ]);
  assert.equal(channel.listenerCount('return'), 0);
  captured.ack?.(null);
});

test('a late confirmation cannot remove a newer publication return listener', async () => {
  const captured: { ack?: Ack; options?: object } = {};
  const channel = channelWithCallback(captured);
  await Promise.race([
    assert.rejects(publishConfirmed(channel, 'exchange', 'key', Buffer.from('first'), 'first', 20), /timed out/i),
    new Promise((_, reject) => setTimeout(() => reject(new Error('confirmation did not settle')), 200)),
  ]);
  const lateAck = captured.ack;
  const next = publishConfirmed(channel, 'exchange', 'key', Buffer.from('next'), 'next', 1000);
  const nextAck = captured.ack;
  const correlationId = (captured.options as { correlationId: string }).correlationId;
  lateAck?.(null);
  channel.emit('return', { properties: { correlationId } });
  nextAck?.(null);
  await assert.rejects(next, /unroutable/i);
  assert.equal(channel.listenerCount('return'), 0);
});
