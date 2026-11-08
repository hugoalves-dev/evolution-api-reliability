import assert from 'node:assert/strict';
import { createServer, Socket } from 'node:net';
import { test } from 'node:test';

import { configService } from '../src/config/env.config';
import { RabbitmqController } from '../src/api/integrations/event/rabbitmq/rabbitmq.controller';

test('does not start a second connection while the first handshake is pending', async () => {
  const sockets = new Set<Socket>();
  let connections = 0;
  const server = createServer((socket) => {
    connections++;
    sockets.add(socket);
    socket.once('close', () => sockets.delete(socket));
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  const config: any = configService.get('RABBITMQ');
  config.ENABLED = true;
  config.GLOBAL_ENABLED = false;
  config.URI = `amqp://guest:guest@127.0.0.1:${address.port}`;
  const sql: any = { $executeRawUnsafe: async () => 0 };
  const controller = new RabbitmqController(sql, { waInstances: {} } as any);
  const init = controller.init();
  try {
    await new Promise((resolve) => setTimeout(resolve, 6500));
    assert.equal(connections, 1);
  } finally {
    await controller.shutdown();
    for (const socket of sockets) socket.destroy();
    await init;
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});

test('a stalled handshake times out and releases the connection attempt for recovery', async () => {
  const sockets = new Set<Socket>();
  const server = createServer((socket) => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  const config: any = configService.get('RABBITMQ');
  config.ENABLED = true;
  config.GLOBAL_ENABLED = false;
  config.CONNECT_TIMEOUT_MS = 1000;
  config.URI = `amqp://guest:guest@127.0.0.1:${address.port}`;
  const controller = new RabbitmqController({ $executeRawUnsafe: async () => 0 } as any, { waInstances: {} } as any);
  const init = controller.init();
  try {
    await Promise.race([
      init,
      new Promise((_, reject) => setTimeout(() => reject(new Error('handshake did not time out')), 2500)),
    ]);
  } finally {
    await controller.shutdown();
    for (const socket of sockets) socket.destroy();
    await init;
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});
