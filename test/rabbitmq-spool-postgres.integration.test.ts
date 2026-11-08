import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { test } from 'node:test';
import { Client } from 'pg';
import * as amqp from 'amqplib/callback_api';
import { ensureTopology } from '../src/api/integrations/event/rabbitmq/ensure-topology';
import { publishConfirmed } from '../src/api/integrations/event/rabbitmq/publish-confirmed';

import { RabbitSpoolStore } from '../src/api/integrations/event/rabbitmq/rabbit-spool-store';
import { RabbitSpoolWorker } from '../src/api/integrations/event/rabbitmq/rabbit-spool-worker';

const url = process.env.TEST_POSTGRES_URL;

test('persisted events survive worker recreation and stale leases cannot acknowledge them', { skip: !url }, async () => {
  const client = new Client({ connectionString: url });
  await client.connect();
  const schema = `spool_${randomUUID().replace(/-/g, '')}`;
  await client.query(`CREATE SCHEMA "${schema}"`);
  await client.query(`SET search_path TO "${schema}"`);
  try {
    await client.query(await readFile('prisma/postgresql-migrations/20261107000000_rabbitmq_delivery_spool/migration.sql', 'utf8'));
    const sql = {
      $queryRawUnsafe: async (statement: string, ...values: unknown[]) => (await client.query(statement, values)).rows,
      $executeRawUnsafe: async (statement: string, ...values: unknown[]) => (await client.query(statement, values)).rowCount,
      $transaction: async (fn: (tx: any) => Promise<unknown>) => {
        await client.query('BEGIN');
        try { const result = await fn(sql); await client.query('COMMIT'); return result; }
        catch (error) { await client.query('ROLLBACK'); throw error; }
      },
    };
    const first = new RabbitSpoolStore(sql, 'postgresql');
    const now = new Date('2026-11-07T00:00:00Z');
    await first.enqueue([{ instance: 'tenant-a', eventId: 'stable-42', exchange: 'watcher.evolution',
      queue: 'watcher.messages.upsert', route: 'messages.upsert', payload: '{"event":"messages.upsert"}' }], now);

    const afterRestart = new RabbitSpoolStore(sql, 'postgresql');
    const lease = await afterRestart.claim(now, 1000);
    assert.ok(lease);
    assert.equal(lease.eventId, 'stable-42');
    assert.equal(await first.claim(now, 1000), null);
    const recovered = await first.claim(new Date(now.getTime() + 1001), 1000);
    assert.ok(recovered);
    assert.equal(recovered.eventId, lease.eventId);
    assert.notEqual(recovered.token, lease.token);
    assert.equal(await afterRestart.acknowledge(lease), false);
    assert.equal(await first.acknowledge(recovered), true);
    assert.equal(await first.claim(new Date(now.getTime() + 2002), 1000), null);

    const event = { instance: 'tenant-a', eventId: 'retry-43', exchange: 'watcher.evolution',
      queue: 'watcher.messages.upsert', route: 'messages.upsert', payload: '{"event":"messages.upsert"}' };
    await assert.rejects(first.enqueue([event, { ...event, route: 'x'.repeat(256) }], now));
    assert.equal(await first.claim(now, 1000), null, 'partial batch must roll back');
    await first.enqueue([event], now);
    assert.equal((await first.counts()).PENDING, 1);
    const failed = await first.claim(now, 1000);
    assert.ok(failed);
    assert.equal(await first.retry(failed, now, 5000, 3), true);
    assert.equal(await first.claim(new Date(now.getTime() + 4999), 1000), null);
    const retry = await first.claim(new Date(now.getTime() + 5000), 1000);
    assert.ok(retry);
    assert.equal(retry.attempts, 1);
    assert.equal(await first.retry(failed, now, 5000, 3), false, 'stale worker cannot overwrite current lease');
    assert.equal(await first.acknowledge(retry), true);
    await first.enqueue([{ ...event, eventId: 'dead-44' }], now);
    const dead = await first.claim(now, 1000);
    assert.ok(dead);
    assert.equal(await first.retry(dead, now, 5000, 1), true);
    assert.equal((await first.counts()).DEAD, 1);
    assert.equal(await first.purgeExpired(new Date(now.getTime() + 7 * 86400_000)), 1);
    assert.equal((await first.counts()).DEAD, 0);

    await first.enqueue([{ ...event, eventId: 'worker-44' }], now);
    let current = now;
    let attempts = 0;
    let ready = false;
    const delivered: string[] = [];
    const worker = new RabbitSpoolWorker(first, () => ready, async (item) => {
      attempts++;
      delivered.push(item.eventId);
      if (attempts === 1) throw new Error('broker unavailable');
    }, () => current, () => {});
    await worker.drain();
    assert.equal(attempts, 0, 'offline broker must not consume retry budget');
    ready = true;
    await worker.drain();
    assert.equal(attempts, 1);
    current = new Date(now.getTime() + 1001);
    const restarted = new RabbitSpoolWorker(first, () => ready, async (item) => {
      attempts++;
      delivered.push(item.eventId);
    }, () => current, () => {});
    await restarted.drain();
    assert.deepEqual(delivered, ['worker-44', 'worker-44']);
    assert.equal(await first.claim(current, 1000), null, 'confirmed event was removed');

    const child = spawn(process.execPath, ['--import', 'tsx', 'test/helpers/rabbit-spool-crash-child.ts'], {
      env: { ...process.env, TEST_SPOOL_SCHEMA: schema }, stdio: 'ignore',
    });
    const crashed = await new Promise<NodeJS.Signals | null>((resolve, reject) => {
      child.once('error', reject);
      child.once('exit', (_code, signal) => resolve(signal));
    });
    assert.equal(crashed, 'SIGKILL');
    const crashLease = await first.claim(new Date(), 1000);
    assert.ok(crashLease, 'event must survive an actual producer process crash');
    assert.equal(crashLease.eventId, 'crash-47');
    assert.equal(await first.acknowledge(crashLease), true);

    if (process.env.TEST_RABBITMQ_URL) {
      const connection = await new Promise<amqp.Connection>((resolve, reject) => {
        amqp.connect(process.env.TEST_RABBITMQ_URL!, (error, value) => error ? reject(error) : resolve(value));
      });
      const channel = await new Promise<amqp.ConfirmChannel>((resolve, reject) => {
        connection.createConfirmChannel((error, value) => error ? reject(error) : resolve(value));
      });
      const exchange = `watcher.spool.${randomUUID()}`;
      const queue = `${exchange}.queue`;
      try {
        await ensureTopology(channel, exchange, queue, 'messages.upsert');
        await first.enqueue([{ ...event, eventId: 'ambiguous-45', exchange, queue }], current);
        const ambiguous = new RabbitSpoolWorker(first, () => true, async (item) => {
          await publishConfirmed(channel, item.exchange, item.route, Buffer.from(item.payload), item.eventId);
          throw new Error('confirmation lost after publication');
        }, () => current, () => {});
        await ambiguous.drain();
        current = new Date(current.getTime() + 1001);
        const recovery = new RabbitSpoolWorker(first, () => true, async (item) => {
          await publishConfirmed(channel, item.exchange, item.route, Buffer.from(item.payload), item.eventId);
        }, () => current, () => {});
        await recovery.drain();
        const get = () => new Promise<amqp.GetMessage | false>((resolve, reject) => {
          channel.get(queue, { noAck: true }, (error, value) => error ? reject(error) : resolve(value));
        });
        const firstCopy = await get();
        const secondCopy = await get();
        assert.ok(firstCopy && secondCopy, 'ambiguous confirmation can redeliver');
        assert.equal(firstCopy.properties.messageId, 'ambiguous-45');
        assert.equal(secondCopy.properties.messageId, 'ambiguous-45');
        assert.equal(await first.claim(current, 1000), null);

        const { configService } = await import('../src/config/env.config');
        const config: any = configService.get('RABBITMQ');
        config.ENABLED = true;
        config.GLOBAL_ENABLED = true;
        config.GLOBAL_EXTERNAL_QUEUE = true;
        config.URI = process.env.TEST_RABBITMQ_URL;
        config.EXCHANGE_NAME = `${exchange}.controller`;
        config.PREFIX_KEY = `${exchange}.controller`;
        config.EVENTS.MESSAGES_UPSERT = true;
        const { RabbitmqController } = await import('../src/api/integrations/event/rabbitmq/rabbitmq.controller');
        let controller = new RabbitmqController(sql as any, { waInstances: {} } as any);
        try {
          await controller.emit({ instanceName: 'tenant-a', origin: 'test', event: 'messages.upsert',
            data: { key: { id: 'controller-46' }, token: 'private-token' },
            serverUrl: 'https://example.invalid', dateTime: now.toISOString(), sender: 'sender' });
          const before = await client.query('SELECT "eventId", "payload" FROM "RabbitmqDelivery"');
          assert.equal(before.rows.length, 1, 'must persist before broker connection');
          assert.equal(before.rows[0].eventId, 'controller-46');
          assert.equal(before.rows[0].payload.includes('private-token'), false);
          await controller.shutdown();
          controller = new RabbitmqController(sql as any, { waInstances: {} } as any);
          await controller.init();
          let failures = 0;
          for (let i = 0; i < 100 && !failures; i++) {
            await new Promise((resolve) => setTimeout(resolve, 50));
            const state = await client.query('SELECT "attempts" FROM "RabbitmqDelivery"');
            failures = state.rows[0]?.attempts || 0;
          }
          assert.ok(failures > 0, 'unroutable publication must remain persisted');
          const controllerQueue = `${config.PREFIX_KEY}.listener`;
          await ensureTopology(channel, config.EXCHANGE_NAME, controllerQueue, 'messages.upsert');
          let remaining = 1;
          for (let i = 0; i < 100 && remaining; i++) {
            await new Promise((resolve) => setTimeout(resolve, 50));
            remaining = Number((await client.query('SELECT count(*) AS count FROM "RabbitmqDelivery"')).rows[0].count);
          }
          assert.equal(remaining, 0, 'connected producer must confirm and remove persisted event');
          const received = await new Promise<amqp.GetMessage | false>((resolve, reject) => {
            channel.get(controllerQueue, { noAck: true }, (error, value) => error ? reject(error) : resolve(value));
          });
          assert.ok(received);
          assert.equal(received.properties.messageId, 'controller-46');
          assert.equal(received.content.toString().includes('private-token'), false);
          await new Promise<void>((resolve, reject) => channel.deleteQueue(controllerQueue, {}, (error) => error ? reject(error) : resolve()));
          await new Promise<void>((resolve, reject) => channel.deleteExchange(config.EXCHANGE_NAME, {}, (error) => error ? reject(error) : resolve()));
        } finally {
          await controller.shutdown();
        }
        await new Promise<void>((resolve, reject) => channel.deleteQueue(queue, {}, (error) => error ? reject(error) : resolve()));
        await new Promise<void>((resolve, reject) => channel.deleteExchange(exchange, {}, (error) => error ? reject(error) : resolve()));
      } finally {
        await new Promise<void>((resolve) => channel.close(() => resolve()));
        await new Promise<void>((resolve) => connection.close(() => resolve()));
      }
    }
  } finally {
    await client.query(`DROP SCHEMA "${schema}" CASCADE`);
    await client.end();
  }
});
