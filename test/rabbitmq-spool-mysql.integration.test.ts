import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { test } from 'node:test';
import mysql from 'mysql2/promise';

import { RabbitSpoolStore } from '../src/api/integrations/event/rabbitmq/rabbit-spool-store';

const url = process.env.TEST_MYSQL_URL;

test('MySQL preserves events, fencing and retry state across workers', { skip: !url }, async () => {
  const parsed = new URL(url!);
  const client = await mysql.createConnection({ host: parsed.hostname, port: Number(parsed.port),
    user: decodeURIComponent(parsed.username), password: decodeURIComponent(parsed.password), multipleStatements: true });
  const schema = `spool_${randomUUID().replace(/-/g, '')}`;
  await client.query(`CREATE DATABASE \`${schema}\``);
  await client.query(`USE \`${schema}\``);
  try {
    await client.query(await readFile('prisma/mysql-migrations/20261107000000_rabbitmq_delivery_spool/migration.sql', 'utf8'));
    const sql = {
      $queryRawUnsafe: async (statement: string, ...values: unknown[]) => (await client.query(statement, values))[0],
      $executeRawUnsafe: async (statement: string, ...values: unknown[]) =>
        ((await client.query(statement, values))[0] as mysql.ResultSetHeader).affectedRows,
      $transaction: async (fn: (tx: any) => Promise<unknown>) => {
        await client.beginTransaction();
        try { const result = await fn(sql); await client.commit(); return result; }
        catch (error) { await client.rollback(); throw error; }
      },
    };
    const first = new RabbitSpoolStore(sql as any, 'mysql');
    const second = new RabbitSpoolStore(sql as any, 'mysql');
    const now = new Date('2026-11-07T00:00:00Z');
    const event = { instance: 'tenant-a', eventId: 'stable-42', exchange: 'watcher.evolution',
      queue: 'watcher.messages.upsert', route: 'messages.upsert', payload: '{"event":"messages.upsert"}' };
    await assert.rejects(first.enqueue([event, { ...event, route: 'x'.repeat(256) }], now));
    assert.equal(await first.claim(now, 1000), null, 'partial batch must roll back');
    await first.enqueue([event], now);
    const old = await first.claim(now, 1000);
    assert.ok(old);
    assert.equal(await second.claim(now, 1000), null);
    const recovered = await second.claim(new Date(now.getTime() + 1001), 1000);
    assert.ok(recovered);
    assert.equal(recovered.eventId, 'stable-42');
    assert.equal(await first.acknowledge(old), false);
    assert.equal(await second.retry(recovered, now, 5000, 3), true);
    assert.equal(await first.claim(new Date(now.getTime() + 4999), 1000), null);
    const retry = await first.claim(new Date(now.getTime() + 5000), 1000);
    assert.ok(retry);
    assert.equal(retry.attempts, 1);
    assert.equal(await first.acknowledge(retry), true);
  } finally {
    await client.query(`DROP DATABASE \`${schema}\``);
    await client.end();
  }
});
