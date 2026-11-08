import { randomUUID } from 'node:crypto';

import type { ConfirmChannel, Message } from 'amqplib/callback_api';

type ReturnState = {
  pending: Map<string, { returned: boolean }>;
  onReturn: (message: Message) => void;
};

export class RabbitmqConfirmTimeoutError extends Error {
  constructor() {
    super('RabbitMQ publisher confirmation timed out');
    this.name = 'RabbitmqConfirmTimeoutError';
  }
}

const returns = new WeakMap<ConfirmChannel, ReturnState>();

function trackReturn(channel: ConfirmChannel, correlationId: string): { returned: boolean; cleanup: () => void } {
  let state = returns.get(channel);
  if (!state) {
    const pending = new Map<string, { returned: boolean }>();
    const onReturn = (message: Message) => {
      const entry = pending.get(message.properties.correlationId);
      if (entry) entry.returned = true;
    };
    state = { pending, onReturn };
    returns.set(channel, state);
    channel.on('return', onReturn);
  }
  const current = state;
  const entry = { returned: false };
  current.pending.set(correlationId, entry);
  return {
    get returned() {
      return entry.returned;
    },
    cleanup: () => {
      current.pending.delete(correlationId);
      if (current.pending.size === 0) {
        channel.removeListener('return', current.onReturn);
        returns.delete(channel);
      }
    },
  };
}

/**
 * Resolve only after the RabbitMQ broker confirms the publish.
 * A false return value from publish() is backpressure, NOT a NACK.
 * This does not provide a durable producer outbox or exactly-once delivery.
 */
export function publishConfirmed(
  channel: ConfirmChannel,
  exchange: string,
  routingKey: string,
  content: Buffer,
  messageId?: string,
  confirmTimeoutMs = 10_000,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    if (!Number.isFinite(confirmTimeoutMs) || confirmTimeoutMs <= 0) {
      reject(new RangeError('RabbitMQ confirm timeout must be positive'));
      return;
    }
    const correlationId = randomUUID();
    const tracked = trackReturn(channel, correlationId);
    let settled = false;
    const finish = (error?: unknown) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      tracked.cleanup();
      if (error) reject(error);
      else if (tracked.returned) reject(new Error('RabbitMQ publication was unroutable'));
      else resolve();
    };
    const timer = setTimeout(() => finish(new RabbitmqConfirmTimeoutError()), confirmTimeoutMs);
    try {
      channel.publish(
        exchange,
        routingKey,
        content,
        { persistent: true, mandatory: true, correlationId, ...(messageId ? { messageId } : {}) },
        (error) => finish(error),
      );
    } catch (error) {
      finish(error ?? new Error('RabbitMQ publish failed'));
    }
  });
}
