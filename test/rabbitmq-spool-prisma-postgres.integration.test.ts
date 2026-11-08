import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { test } from 'node:test';
import { Client } from 'pg';

import { RabbitSpoolStore } from '../src/api/integrations/event/rabbitmq/rabbit-spool-store';

const url = process.env.TEST_PRISMA_POSTGRES_URL;

test('Prisma PostgreSQL transactions and parameters preserve the replay contract', { skip: !url }, async () => {
  const admin = new Client({ connectionString: url });
  await admin.connect();
  const schema = `spool_${randomUUID().replace(/-/g, '')}`;
  await admin.query(`CREATE SCHEMA "${schema}"`);
  await admin.query(`SET search_path TO "${schema}"`);
  let prisma: any;
  try {
    await admin.query(await readFile('prisma/postgresql-migrations/20261107000000_rabbitmq_delivery_spool/migration.sql', 'utf8'));
    const connection = new URL(url!);
    connection.searchParams.set('schema', schema);
    const { PrismaClient } = await import('@prisma/client');
    prisma = new PrismaClient({ datasources: { db: { url: connection.toString() } } });
    await prisma.$connect();
    const store = new RabbitSpoolStore(prisma, 'postgresql');
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
    await admin.query(`DROP SCHEMA "${schema}" CASCADE`);
    await admin.end();
  }
});
