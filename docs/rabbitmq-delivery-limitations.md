# Native RabbitMQ delivery and recovery

The native RabbitMQ publisher writes each enabled destination to the Evolution database before it tries the broker. The spool transaction commits the complete local/global destination set together. A worker leases rows, declares the exchange and queue/binding (unless the global external queue mode is selected), publishes a persistent message with `mandatory`, and removes the row only after a publisher confirm with no correlated return. Lease tokens fence stale workers. A process restart replays due and expired-lease rows from the database.

This provides **at-least-once** delivery after the spool commit, not exactly-once delivery. If a confirm is lost after RabbitMQ accepted a message, the same event is published again with the same AMQP `messageId`. Consumers must deduplicate where their action is not repeat-safe. The message upsert ID comes from `data.key.id`; other event IDs are created once when the event is admitted to the spool and then remain stable over replay. Separate observations are separate spool entries.

## Boundaries and recovery

For the Baileys `messages.upsert` path, Evolution awaits event admission before it continues message handling. Failure before the database transaction commits (including a process or host failure in that window) cannot be replayed from this spool. The WhatsApp library's upstream acknowledgement and retry timing are outside this publisher's control. Stored messages are not automatically scanned to recreate missing RabbitMQ events. Keep the authenticated webhook ingress enabled until the native route has been exercised end to end with the watcher and its recovery path. A failed RabbitMQ destination remains in the spool; this does not retract an event already delivered through another integration.

The worker retries transient failures with exponential delay, capped at 15 minutes. After 10 failed publish attempts, it marks a row `DEAD`; operators should alert and investigate before the seven-day expiry removes it. The worker only publishes when a broker channel is ready. A stalled topology RPC or confirm closes the unhealthy channel and the reconnect loop obtains a new one. Reconnection continues with bounded exponential backoff.

`GET /metrics` reports `evolution_rabbitmq_spool_events{status="pending|processing|dead"}` without instance IDs or message contents. `-1` means the count query failed. Alert on any `dead > 0`, a sustained increase in `pending`, and unavailable (`-1`) counts. The spool stores full event envelopes, which may include message text and personal data, for up to seven days. Protect and back up the database accordingly; disk loss or restoring an older database can lose admitted events. The single-database spool is not a substitute for database replication and backups.

A dead event can be made eligible for a manual retry after fixing the underlying problem. Use the database provider's normal quoting and a maintenance window; inspect counts first, avoid editing `PROCESSING` rows with a live lease, and retain an audit trail. Example for PostgreSQL:

```sql
UPDATE "RabbitmqDelivery"
SET "status" = 'PENDING', "attempts" = 0, "availableAt" = CURRENT_TIMESTAMP,
    "leaseUntil" = NULL, "leaseToken" = NULL
WHERE "status" = 'DEAD' AND "id" = '<reviewed-row-id>';
```

For MySQL, use backticks around table and column identifiers. Replaying can duplicate a message if RabbitMQ accepted an earlier attempt but its confirmation was lost.

## Topology and rollout

With the defaults, Evolution declares a durable topic exchange, durable quorum queues, and bindings for enabled event destinations. The publisher uses mandatory routing and rejects a returned publication even if RabbitMQ also confirms it. Make sure queue naming and routing keys match the watcher configuration.

`RABBITMQ_GLOBAL_EXTERNAL_QUEUE=true` disables Evolution's automatically-created global queues and publishes to the exchange only. Set up and verify consumer-owned durable queues and bindings before enabling this option; mandatory returns keep unbound events in the spool for retry. Instance-local queues remain managed by Evolution. Avoid running both the old webhook and native RabbitMQ watcher consumers against the same actions unless the watcher deduplicates across ingress paths.

## Envelope and secrets

The top-level `apikey` has been removed. Consumers that relied on it must obtain credentials from their own secret configuration. Before persistence, the envelope recursively removes fields whose normalized key is `apikey`, `accesstoken`, `refreshtoken`, `authorization`, `password`, `secret`, `clientsecret`, `token`, `qrcode`, or `pairingcode`. These names are case- and separator-insensitive. The filter can remove application fields with those names. It cannot identify secrets embedded in arbitrary message text or fields with unknown names; do not log event contents and restrict spool-table access and retention.

`RABBITMQ_CONNECT_TIMEOUT_MS` controls the AMQP handshake deadline (1–60 seconds, default 10 seconds). RabbitMQ remains disabled unless `RABBITMQ_ENABLED=true`; the external global queue option defaults to false.
