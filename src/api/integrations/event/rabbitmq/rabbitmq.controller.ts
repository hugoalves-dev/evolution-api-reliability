import { PrismaRepository } from '@api/repository/repository.service';
import { WAMonitoringService } from '@api/services/monitor.service';
import { configService, Database, Rabbitmq } from '@config/env.config';
import { Logger } from '@config/logger.config';
import * as amqp from 'amqplib/callback_api';

import { EmitData, EventController, EventControllerInterface } from '../event.controller';
import { ensureTopology, RabbitmqTopologyTimeoutError } from './ensure-topology';
import { publishConfirmed, RabbitmqConfirmTimeoutError } from './publish-confirmed';
import { planRabbitDeliveries } from './rabbit-delivery-plan';
import { RabbitSpoolSql, RabbitSpoolStore } from './rabbit-spool-store';
import { RabbitSpoolWorker } from './rabbit-spool-worker';

export class RabbitmqController extends EventController implements EventControllerInterface {
  public amqpChannel: amqp.ConfirmChannel | null = null;
  private amqpConnection: amqp.Connection | null = null;
  private readonly logger = new Logger('RabbitmqController');
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 10;
  private reconnectDelay = 5000; // 5 seconds
  private isReconnecting = false;
  private connecting = false;
  private spool: RabbitSpoolStore | null = null;
  private spoolWorker: RabbitSpoolWorker | null = null;
  private spoolTimer: NodeJS.Timeout | null = null;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private stopped = false;

  constructor(prismaRepository: PrismaRepository, waMonitor: WAMonitoringService) {
    super(prismaRepository, waMonitor, configService.get<Rabbitmq>('RABBITMQ')?.ENABLED, 'rabbitmq');
  }

  public async init(): Promise<void> {
    if (!this.status) {
      return;
    }

    this.startSpool();
    try {
      await this.connect();
    } catch {
      // connect() schedules reconnection; persisted messages remain available for replay.
      this.logger.warn('RabbitMQ is unavailable; persisted deliveries will be retried');
    }
  }

  private startSpool(): RabbitSpoolStore {
    if (this.stopped) throw new Error('RabbitMQ publisher is stopped');
    if (this.spool) return this.spool;
    const configured = configService.get<Database>('DATABASE').PROVIDER;
    const provider = configured === 'psql_bouncer' ? 'postgresql' : configured;
    this.spool = new RabbitSpoolStore(this.prismaRepository as unknown as RabbitSpoolSql, provider);
    this.spoolWorker = new RabbitSpoolWorker(
      this.spool,
      () => this.amqpChannel !== null,
      async (delivery) => {
        const channel = this.amqpChannel;
        if (!channel) throw new Error('RabbitMQ confirm channel unavailable');
        try {
          await ensureTopology(channel, delivery.exchange, delivery.queue, delivery.route);
          await publishConfirmed(
            channel,
            delivery.exchange,
            delivery.route,
            Buffer.from(delivery.payload),
            delivery.eventId,
          );
        } catch (error) {
          // A timed-out AMQP RPC leaves the channel's protocol state uncertain; force a fresh channel.
          if (error instanceof RabbitmqTopologyTimeoutError || error instanceof RabbitmqConfirmTimeoutError) {
            try {
              channel.close();
            } catch {
              /* the connection may already be gone */
            }
          }
          throw error;
        }
      },
      () => new Date(),
      (kind, count) => {
        if (kind === 'dead') this.logger.error(`RabbitMQ producer deliveries require intervention: count=${count}`);
        else this.logger.warn(`RabbitMQ producer delivery state: ${kind} count=${count}`);
      },
    );
    this.spoolTimer = setInterval(() => {
      if (!this.amqpChannel) void this.ensureConnection();
      void this.spoolWorker.drain().catch((error) => {
        this.logger.error(`RabbitMQ producer spool processing failed: ${error?.constructor?.name || 'Error'}`);
      });
    }, 1000);
    this.spoolTimer.unref();
    return this.spool;
  }

  private async connect(): Promise<void> {
    if (this.stopped || this.connecting || this.amqpChannel) return;
    this.connecting = true;
    try {
      await new Promise<void>((resolve, reject) => {
        const uri = configService.get<Rabbitmq>('RABBITMQ').URI;
        const frameMax = configService.get<Rabbitmq>('RABBITMQ').FRAME_MAX;

        const url = new URL(uri);
        const connectionOptions = {
          protocol: url.protocol.slice(0, -1),
          hostname: url.hostname,
          port: url.port || 5672,
          username: url.username || 'guest',
          password: url.password || 'guest',
          vhost: url.pathname.slice(1) || '/',
          frameMax: frameMax,
          heartbeat: 30, // Add heartbeat of 30 seconds
        };

        const timeout = configService.get<Rabbitmq>('RABBITMQ').CONNECT_TIMEOUT_MS;
        amqp.connect(connectionOptions, { timeout }, (error: Error, connection: amqp.Connection) => {
          if (error) {
            this.logger.error({
              local: 'RabbitmqController.connect',
              message: 'Failed to connect to RabbitMQ',
              error: error.message || error,
            });
            reject(error);
            return;
          }

          if (this.stopped) {
            connection.close();
            reject(new Error('RabbitMQ publisher is stopped'));
            return;
          }

          // Connection event handlers
          connection.on('error', (err: Error) => {
            this.logger.error(`RabbitMQ connection error: ${err?.constructor?.name || 'Error'}`);
            this.handleConnectionLoss(connection);
          });

          connection.on('close', () => {
            this.logger.warn('RabbitMQ connection closed');
            this.handleConnectionLoss(connection);
          });

          connection.createConfirmChannel((channelError: Error, channel: amqp.ConfirmChannel) => {
            if (channelError) {
              this.logger.error({
                local: 'RabbitmqController.createChannel',
                message: 'Failed to create RabbitMQ channel',
                error: channelError.message || channelError,
              });
              reject(channelError);
              return;
            }

            if (this.stopped) {
              channel.close();
              connection.close();
              reject(new Error('RabbitMQ publisher is stopped'));
              return;
            }

            // Channel event handlers
            channel.on('error', (err: Error) => {
              this.logger.error(`RabbitMQ channel error: ${err?.constructor?.name || 'Error'}`);
              this.handleConnectionLoss(connection, channel);
            });

            channel.on('close', () => {
              this.logger.warn('RabbitMQ channel closed');
              this.handleConnectionLoss(connection, channel);
            });

            this.amqpConnection = connection;
            this.amqpChannel = channel;
            this.reconnectAttempts = 0; // Reset reconnect attempts on successful connection
            this.isReconnecting = false;

            this.logger.info('AMQP initialized successfully');

            resolve();
          });
        });
      })
        .then(() => {
          const config = configService.get<Rabbitmq>('RABBITMQ');
          if (config?.GLOBAL_ENABLED && !config.GLOBAL_EXTERNAL_QUEUE) {
            this.initGlobalQueues();
          }
        })
        .catch((error) => {
          this.logger.error({
            local: 'RabbitmqController.init',
            message: 'Failed to initialize AMQP',
            error: error.message || error,
          });
          this.scheduleReconnect();
          throw error;
        });
    } finally {
      this.connecting = false;
    }
  }

  private handleConnectionLoss(connection?: amqp.Connection, channel?: amqp.ConfirmChannel): void {
    if (this.stopped) return;
    // Ignore delayed close/error notifications from a connection replaced by a newer one.
    if (connection && this.amqpConnection !== connection) return;
    if (channel && this.amqpChannel !== channel) return;
    if (this.isReconnecting) {
      return; // Already attempting to reconnect
    }

    void this.cleanup();
    this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    if (this.stopped) return;
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      this.logger.warn('RabbitMQ remains unavailable; continuing bounded-backoff reconnection');
      this.reconnectAttempts = this.maxReconnectAttempts - 1;
    }

    if (this.isReconnecting) {
      return; // Already scheduled
    }

    this.isReconnecting = true;
    this.reconnectAttempts++;

    const delay = this.reconnectDelay * Math.pow(2, Math.min(this.reconnectAttempts - 1, 5)); // Exponential backoff with max delay

    this.logger.info(
      `Scheduling RabbitMQ reconnection attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts} in ${delay}ms`,
    );

    this.reconnectTimer = setTimeout(async () => {
      this.reconnectTimer = null;
      if (this.stopped) return;
      try {
        this.logger.info(
          `Attempting to reconnect to RabbitMQ (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`,
        );
        await this.connect();
        this.logger.info('Successfully reconnected to RabbitMQ');
      } catch (error) {
        this.logger.error({
          local: 'RabbitmqController.scheduleReconnect',
          message: `Reconnection attempt ${this.reconnectAttempts} failed`,
          error: error.message || error,
        });
        this.isReconnecting = false;
        this.scheduleReconnect();
      }
    }, delay);
  }

  private set channel(channel: amqp.ConfirmChannel) {
    this.amqpChannel = channel;
  }

  public get channel(): amqp.ConfirmChannel {
    return this.amqpChannel;
  }

  private async ensureConnection(): Promise<boolean> {
    if (!this.amqpChannel) {
      if (!this.isReconnecting && !this.connecting && !this.stopped) {
        this.logger.warn('AMQP channel is not available, attempting to reconnect...');
        this.scheduleReconnect();
      }
      return false;
    }
    return true;
  }

  public async emit(eventData: EmitData): Promise<void> {
    if (!this.status || (eventData.integration && !eventData.integration.includes('rabbitmq'))) return;

    const config = configService.get<Rabbitmq>('RABBITMQ');
    const local = await this.get(eventData.instanceName);
    const deliveries = planRabbitDeliveries(eventData, {
      localEnabled: Boolean(local?.enabled),
      localEvents: Array.isArray(local?.events) ? local.events : [],
      globalEnabled: config.GLOBAL_ENABLED,
      globalExternalQueue: config.GLOBAL_EXTERNAL_QUEUE,
      globalEvents: config.EVENTS,
      globalExchange: config.EXCHANGE_NAME,
      prefix: config.PREFIX_KEY || '',
    });
    if (deliveries.length === 0) return;

    // Commit the complete destination set before returning or attempting broker delivery.
    const spool = this.startSpool();
    await spool.enqueue(deliveries, new Date());
    void this.spoolWorker.drain().catch((error) => {
      this.logger.error(`RabbitMQ producer spool processing failed: ${error?.constructor?.name || 'Error'}`);
    });
  }

  private async initGlobalQueues(): Promise<void> {
    this.logger.info('Initializing global queues');

    if (!(await this.ensureConnection())) {
      this.logger.error('Cannot initialize global queues: No AMQP connection');
      return;
    }

    const rabbitmqExchangeName = configService.get<Rabbitmq>('RABBITMQ').EXCHANGE_NAME;
    const events = configService.get<Rabbitmq>('RABBITMQ').EVENTS;
    const prefixKey = configService.get<Rabbitmq>('RABBITMQ').PREFIX_KEY;

    if (!events) {
      this.logger.warn('No events to initialize on AMQP');
      return;
    }

    const eventKeys = Object.keys(events);

    for (const event of eventKeys) {
      if (events[event] === false) continue;

      try {
        const queueName =
          prefixKey !== ''
            ? `${prefixKey}.${event.replace(/_/g, '.').toLowerCase()}`
            : `${event.replace(/_/g, '.').toLowerCase()}`;
        const exchangeName = rabbitmqExchangeName;

        const route = event.replace(/_/g, '.').toLowerCase();
        await ensureTopology(this.amqpChannel, exchangeName, queueName, route);

        this.logger.info(`Global queue initialized: ${queueName}`);
      } catch (error) {
        this.logger.error({
          local: 'RabbitmqController.initGlobalQueues',
          message: `Failed to initialize global queue for event ${event}`,
          error: error.message || error,
        });
        this.handleConnectionLoss();
        break;
      }
    }
  }

  /** Fixed-cardinality operational counts; no event content or tenant identifiers. */
  public async spoolCounts(): Promise<Record<'PENDING' | 'PROCESSING' | 'DEAD', number> | null> {
    if (!this.status) return null;
    return this.startSpool().counts();
  }

  /** Stop periodic recovery and close the connection without scheduling a reconnect. */
  public async shutdown(): Promise<void> {
    this.stopped = true;
    if (this.spoolTimer) clearInterval(this.spoolTimer);
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.spoolTimer = null;
    this.reconnectTimer = null;
    await this.cleanup();
  }

  public async cleanup(): Promise<void> {
    const channel = this.amqpChannel;
    const connection = this.amqpConnection;
    this.amqpChannel = null;
    this.amqpConnection = null;
    if (channel) await new Promise<void>((resolve) => channel.close(() => resolve()));
    if (connection) await new Promise<void>((resolve) => connection.close(() => resolve()));
  }
}
