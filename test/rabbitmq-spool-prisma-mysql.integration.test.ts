import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { test } from 'node:test';
import mysql from 'mysql2/promise';

import { RabbitSpoolStore } from '../src/api/integrations/event/rabbitmq/rabbit-spool-store';

const url = process.env.TEST_PRISMA_MYSQL_URL;

test('Prisma MySQL transactions and parameters preserve the replay contract', { skip: !url }, async () => {
  const parsed = new URL(url!);
  const admin = await mysql.createConnection({ host: parsed.hostname, port: Number(parsed.port),
    user: decodeURIComponent(parsed.username), password: decodeURIComponent(parsed.password), multipleStatements: true });
  const schema = `spool_${randomUUID().replace(/-/g, '')}`;
  await admin.query(`CREATE DATABASE \`${schema}\``);
  await admin.query(`USE \`${schema}\``);
  let prisma: any;
  try {
    await admin.query(await readFile('prisma/mysql-migrations/20261107000000_rabbitmq_delivery_spool/migration.sql', 'utf8'));
    parsed.pathname = `/${schema}`;
    const { PrismaClient } = await import('@prisma/client');
    prisma = new PrismaClient({ datasources: { db: { url: parsed.toString() } } });
    await prisma.$connect();
    const store = new RabbitSpoolStore(prisma, 'mysql');
    const now = new Date();
    await store.enqueue([{ instance: 'tenant-a', eventId: 'prisma-42', exchange: 'watcher.evolution',
      queue: '', route: 'messages.upsert', payload: '{"event":"messages.upsert"}' }], now);
    const lease = await store.claim(now, 1000);
    assert.ok(lease);
    assert.equal(lease.eventId, 'prisma-42');
    assert.equal((await store.counts()).PROCESSING, 1);
    assert.equal(await store.acknowledge(lease), true);
  } finally {
    if (prisma) await prisma.$disconnect();
    await admin.query(`DROP DATABASE \`${schema}\``);
    await admin.end();
  }
});
