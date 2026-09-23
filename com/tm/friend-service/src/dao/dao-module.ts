// DI: tạo kết nối MongoDB, client event-gateway và các Dao; lifecycle kết nối / đóng MongoDB.

import { type AwilixContainer, asFunction } from 'awilix';
import { type Db, MongoClient } from 'mongodb';
import type { Lifecycle } from '../utils/lifecycle.js';
import type { Logger } from '../utils/logger.js';
import { FriendshipDao } from './friendship-dao.js';
import { type EventGatewaySettings, type EventPublisher, createEventGatewayClient } from './event-gateway-client.js';
import { type TransactionRunner, createTransactionRunner } from './mongo.js';

/** Phần config module này cần */
export interface DaoSettings extends EventGatewaySettings {
  mongoUri: string;
  mongoDb: string;
}

export interface DaoCradle {
  mongoClient: MongoClient;
  db: Db;
  runInTransaction: TransactionRunner;
  friendshipDao: FriendshipDao;
  /** Gửi event sang event-gateway (HTTP) */
  eventPublisher: EventPublisher;
  /** Ping MongoDB cho health check */
  pingDb: () => Promise<unknown>;
  /** Kết nối MongoDB + tạo index khi start, đóng khi stop */
  mongoLifecycle: Lifecycle;
}

type Deps = DaoCradle & { config: DaoSettings; log: Logger };

export function registerDao(container: AwilixContainer): void {
  container.register({
    mongoClient: asFunction(({ config }: Deps) => new MongoClient(config.mongoUri)).singleton(),
    db: asFunction(({ mongoClient, config }: Deps) => mongoClient.db(config.mongoDb)).singleton(),
    runInTransaction: asFunction(({ mongoClient }: Deps) => createTransactionRunner(mongoClient)).singleton(),
    friendshipDao: asFunction(({ db }: Deps) => new FriendshipDao(db)).singleton(),
    eventPublisher: asFunction(({ config }: Deps) => createEventGatewayClient(config)).singleton(),
    pingDb: asFunction(({ db }: Deps) => () => db.command({ ping: 1 })).singleton(),

    mongoLifecycle: asFunction(
      ({ mongoClient, friendshipDao, config, log }: Deps): Lifecycle => ({
        name: 'mongo',
        async start() {
          await mongoClient.connect();
          await friendshipDao.ensureIndexes();
          log.info({ db: config.mongoDb }, 'mongo connected');
        },
        async stop() {
          await mongoClient.close();
        },
      }),
    ).singleton(),
  });
}
