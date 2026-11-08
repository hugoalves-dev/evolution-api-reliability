import { RabbitSpoolLease, RabbitSpoolStore } from './rabbit-spool-store';

const MAX_ATTEMPTS = 10;
const LEASE_MS = 120_000;
const BATCH_SIZE = 20;

/** Replays persisted producer events with bounded backoff and lease-fenced completion. */
export class RabbitSpoolWorker {
  private draining = false;

  constructor(
    private readonly store: RabbitSpoolStore,
    private readonly ready: () => boolean,
    private readonly publish: (event: RabbitSpoolLease) => Promise<void>,
    private readonly clock: () => Date,
    private readonly report: (event: 'expired' | 'dead' | 'stale', count: number) => void,
  ) {}

  async drain(): Promise<void> {
    if (this.draining) return;
    this.draining = true;
    try {
      const expired = await this.store.purgeExpired(this.clock());
      if (expired) this.report('expired', expired);
      for (let i = 0; i < BATCH_SIZE && this.ready(); i++) {
        const lease = await this.store.claim(this.clock(), LEASE_MS);
        if (!lease) break;
        try {
          await this.publish(lease);
          if (!(await this.store.acknowledge(lease))) this.report('stale', 1);
        } catch {
          const delay = Math.min(15 * 60_000, 1000 * 2 ** Math.min(lease.attempts, 10));
          if (!(await this.store.retry(lease, this.clock(), delay, MAX_ATTEMPTS))) this.report('stale', 1);
          else if (lease.attempts + 1 >= MAX_ATTEMPTS) this.report('dead', 1);
        }
      }
    } finally {
      this.draining = false;
    }
  }
}
