import { Client } from 'pg';

import { RabbitSpoolStore } from '../../src/api/integrations/event/rabbitmq/rabbit-spool-store';

async function main() {
  const schema = process.env.TEST_SPOOL_SCHEMA;
  if (!schema || !/^spool_[a-f0-9]+$/.test(schema)) throw new Error('isolated test schema is required');
  const client = new Client({ connectionString: process.env.TEST_POSTGRES_URL });
  await client.connect();
  await client.query(`SET search_path TO "${schema}"`);
  const sql: any = {
    $queryRawUnsafe: async (statement: string, ...values: unknown[]) => (await client.query(statement, values)).rows,
    $executeRawUnsafe: async (statement: string, ...values: unknown[]) => (await client.query(statement, values)).rowCount,
    $transaction: async (fn: (tx: any) => Promise<unknown>) => {
      await client.query('BEGIN');
      try { const result = await fn(sql); await client.query('COMMIT'); return result; }
      catch (error) { await client.query('ROLLBACK'); throw error; }
    },
  };
  const store = new RabbitSpoolStore(sql, 'postgresql');
  await store.enqueue([{ instance: 'tenant-a', eventId: 'crash-47', exchange: 'watcher.evolution',
    queue: 'watcher.messages.upsert', route: 'messages.upsert', payload: '{"event":"messages.upsert"}' }], new Date());
  process.kill(process.pid, 'SIGKILL');
}
void main();
