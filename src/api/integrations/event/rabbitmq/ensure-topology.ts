import type { ConfirmChannel } from 'amqplib/callback_api';

export class RabbitmqTopologyTimeoutError extends Error {
  constructor() {
    super('RabbitMQ topology declaration timed out');
    this.name = 'RabbitmqTopologyTimeoutError';
  }
}

function waitForDeclaration(
  invoke: (callback: (error: Error | null) => void) => void,
  timeoutMs: number,
): Promise<void> {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0)
    return Promise.reject(new Error('Invalid RabbitMQ topology timeout'));
  return new Promise<void>((resolve, reject) => {
    let settled = false;
    const finish = (error: Error | null) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (error) reject(error);
      else resolve();
    };
    const timer = setTimeout(() => finish(new RabbitmqTopologyTimeoutError()), timeoutMs);
    try {
      invoke(finish);
    } catch (error) {
      finish(error instanceof Error ? error : new Error('RabbitMQ topology declaration failed'));
    }
  });
}

/** Wait for each declaration before a confirmed publish is attempted. */
export async function ensureTopology(
  channel: ConfirmChannel,
  exchange: string,
  queue: string,
  route: string,
  timeoutMs = 10_000,
): Promise<void> {
  await ensureExchange(channel, exchange, timeoutMs);
  if (!queue) return;
  await waitForDeclaration((done) => {
    channel.assertQueue(queue, { durable: true, autoDelete: false, arguments: { 'x-queue-type': 'quorum' } }, done);
  }, timeoutMs);
  await waitForDeclaration((done) => {
    channel.bindQueue(queue, exchange, route, {}, done);
  }, timeoutMs);
}

/** An externally managed queue must be bound before a mandatory publish can succeed. */
export async function ensureExchange(channel: ConfirmChannel, exchange: string, timeoutMs = 10_000): Promise<void> {
  await waitForDeclaration((done) => {
    channel.assertExchange(exchange, 'topic', { durable: true, autoDelete: false }, done);
  }, timeoutMs);
}
