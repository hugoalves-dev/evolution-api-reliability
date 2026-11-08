import { randomUUID } from 'node:crypto';

/** Small structural interface implemented by PrismaClient and Prisma transaction clients. */
export interface RabbitSpoolSql {
  $queryRawUnsafe<T = unknown>(sql: string, ...values: unknown[]): Promise<T>;
  $executeRawUnsafe(sql: string, ...values: unknown[]): Promise<number>;
  $transaction?<T>(fn: (tx: RabbitSpoolSql) => Promise<T>): Promise<T>;
}

export interface RabbitSpoolEvent {
  instance: string;
  eventId: string;
  exchange: string;
  queue: string;
  route: string;
  payload: string;
}

export interface RabbitSpoolLease extends RabbitSpoolEvent {
  id: string;
  token: string;
  attempts: number;
}

/** Durable producer handoff. Every update is fenced by a unique lease token. */
export class RabbitSpoolStore {
  private readonly quote: string;

  constructor(
    private readonly sql: RabbitSpoolSql,
    private readonly provider: string,
  ) {
    if (provider !== 'postgresql' && provider !== 'mysql') throw new Error('Unsupported RabbitMQ spool database');
    this.quote = provider === 'postgresql' ? '"' : '`';
  }

  private column(name: string): string {
    return `${this.quote}${name}${this.quote}`;
  }
  private statement(sql: string): string {
    let index = 0;
    return this.provider === 'postgresql' ? sql.replace(/\?/g, () => `$${++index}`) : sql;
  }

  async enqueue(events: RabbitSpoolEvent[], now: Date): Promise<void> {
    if (events.length === 0) return;
    if (!this.sql.$transaction) throw new Error('RabbitMQ spool transaction unavailable');
    const c = (name: string) => this.column(name);
    const fields = [
      'id',
      'instance',
      'eventId',
      'exchange',
      'queue',
      'route',
      'payload',
      'status',
      'attempts',
      'availableAt',
      'createdAt',
      'expiresAt',
    ];
    const statement = this.statement(`INSERT INTO ${c('RabbitmqDelivery')} (${fields.map(c).join(', ')})
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    await this.sql.$transaction(async (tx) => {
      for (const event of events) {
        await tx.$executeRawUnsafe(
          statement,
          randomUUID(),
          event.instance,
          event.eventId,
          event.exchange,
          event.queue,
          event.route,
          event.payload,
          'PENDING',
          0,
          now,
          now,
          new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000),
        );
      }
    });
  }

  async claim(now: Date, leaseMs: number): Promise<RabbitSpoolLease | null> {
    const c = (name: string) => this.column(name);
    const table = c('RabbitmqDelivery');
    const eligible =
      `(${c('status')} = 'PENDING' AND ${c('availableAt')} <= ? OR ` +
      `(${c('status')} = 'PROCESSING' AND ${c('leaseUntil')} <= ?)) AND ${c('expiresAt')} > ?`;
    const candidates = await this.sql.$queryRawUnsafe<Array<{ id: string }>>(
      this.statement(`SELECT ${c('id')} FROM ${table} WHERE ${eligible} ORDER BY ${c('createdAt')} ASC LIMIT 20`),
      now,
      now,
      now,
    );
    for (const candidate of candidates) {
      const token = randomUUID();
      const updated = await this.sql.$executeRawUnsafe(
        this.statement(`UPDATE ${table} SET ${c('status')} = 'PROCESSING',
        ${c('leaseToken')} = ?, ${c('leaseUntil')} = ? WHERE ${c('id')} = ? AND ${eligible}`),
        token,
        new Date(now.getTime() + leaseMs),
        candidate.id,
        now,
        now,
        now,
      );
      if (updated !== 1) continue;
      const rows = await this.sql.$queryRawUnsafe<Array<RabbitSpoolEvent & { id: string; attempts: number }>>(
        this
          .statement(`SELECT ${['id', 'instance', 'eventId', 'exchange', 'queue', 'route', 'payload', 'attempts'].map(c).join(', ')}
          FROM ${table} WHERE ${c('id')} = ? AND ${c('leaseToken')} = ?`),
        candidate.id,
        token,
      );
      if (rows.length) return { ...rows[0], token };
    }
    return null;
  }

  async retry(lease: RabbitSpoolLease, now: Date, delayMs: number, maxAttempts: number): Promise<boolean> {
    const c = (name: string) => this.column(name);
    return (
      (await this.sql.$executeRawUnsafe(
        this.statement(`UPDATE ${c('RabbitmqDelivery')} SET
      ${c('status')} = CASE WHEN ${c('attempts')} + 1 >= ? THEN 'DEAD' ELSE 'PENDING' END,
      ${c('attempts')} = ${c('attempts')} + 1, ${c('availableAt')} = ?,
      ${c('leaseToken')} = NULL, ${c('leaseUntil')} = NULL
      WHERE ${c('id')} = ? AND ${c('instance')} = ? AND ${c('status')} = 'PROCESSING' AND ${c('leaseToken')} = ?`),
        maxAttempts,
        new Date(now.getTime() + delayMs),
        lease.id,
        lease.instance,
        lease.token,
      )) === 1
    );
  }

  async counts(): Promise<Record<'PENDING' | 'PROCESSING' | 'DEAD', number>> {
    const c = (name: string) => this.column(name);
    const rows = await this.sql.$queryRawUnsafe<Array<{ status: string; count: bigint | number | string }>>(
      `SELECT ${c('status')}, COUNT(*) AS ${c('count')} FROM ${c('RabbitmqDelivery')} GROUP BY ${c('status')}`,
    );
    const counts = { PENDING: 0, PROCESSING: 0, DEAD: 0 };
    for (const row of rows) {
      if (row.status === 'PENDING' || row.status === 'PROCESSING' || row.status === 'DEAD')
        counts[row.status] = Number(row.count);
    }
    return counts;
  }

  async purgeExpired(now: Date): Promise<number> {
    const c = (name: string) => this.column(name);
    return this.sql.$executeRawUnsafe(
      this.statement(`DELETE FROM ${c('RabbitmqDelivery')}
      WHERE ${c('expiresAt')} <= ? AND (${c('status')} <> 'PROCESSING' OR ${c('leaseUntil')} <= ?)`),
      now,
      now,
    );
  }

  async acknowledge(lease: RabbitSpoolLease): Promise<boolean> {
    const c = (name: string) => this.column(name);
    return (
      (await this.sql.$executeRawUnsafe(
        this.statement(`DELETE FROM ${c('RabbitmqDelivery')}
      WHERE ${c('id')} = ? AND ${c('instance')} = ? AND ${c('status')} = 'PROCESSING' AND ${c('leaseToken')} = ?`),
        lease.id,
        lease.instance,
        lease.token,
      )) === 1
    );
  }
}
