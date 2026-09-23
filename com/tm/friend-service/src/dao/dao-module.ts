// DI: tạo kết nối MongoDB, Kafka producer và các Dao; lifecycle kết nối / đóng MongoDB và Kafka.

import { type AwilixContainer, asFunction } from 'awilix';
import type { Producer } from 'kafkajs';
import { type Db, MongoClient } from 'mongodb';
import type { Lifecycle } from '../utils/lifecycle.js';
import type { Logger } from '../utils/logger.js';
import { FriendshipDao } from './friendship-dao.js';
import { type EventPublisher, type KafkaSettings, createEventPublisher, createKafkaProducer } from './event-publisher.js';
import { type TransactionRunner, createTransactionRunner } from './mongo.js';

/** Phần config module này cần */
export interface DaoSettings extends KafkaSettings {
  mongoUri: string;
  mongoDb: string;
}

export interface DaoCradle {
  mongoClient: MongoClient;
  db: Db;
  runInTransaction: TransactionRunner;
  friendshipDao: FriendshipDao;
  /** Kafka producer; kết nối trong kafkaLifecycle */
  kafkaProducer: Producer;
  /** Gửi event lên Kafka topic nội bộ, event-gateway đọc */
  eventPublisher: EventPublisher;
  /** Ping MongoDB cho health check */
  pingDb: () => Promise<unknown>;
  /** Kết nối MongoDB + tạo index khi start, đóng khi stop */
  mongoLifecycle: Lifecycle;
  /** Kết nối Kafka producer khi start, ngắt khi stop */
  kafkaLifecycle: Lifecycle;
}

type Deps = DaoCradle & { config: DaoSettings; log: Logger };

export function registerDao(container: AwilixContainer): void {
  container.register({
    mongoClient: asFunction(({ config }: Deps) => new MongoClient(config.mongoUri)).singleton(),
    db: asFunction(({ mongoClient, config }: Deps) => mongoClient.db(config.mongoDb)).singleton(),
    runInTransaction: asFunction(({ mongoClient }: Deps) => createTransactionRunner(mongoClient)).singleton(),
    friendshipDao: asFunction(({ db }: Deps) => new FriendshipDao(db)).singleton(),
    kafkaProducer: asFunction(({ config }: Deps) => createKafkaProducer(config)).singleton(),
    eventPublisher: asFunction(({ kafkaProducer, config }: Deps) =>
      createEventPublisher(kafkaProducer, config.kafkaTopic),
    ).singleton(),
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

    kafkaLifecycle: asFunction(
      ({ kafkaProducer, config, log }: Deps): Lifecycle => ({
        name: 'kafka',
        async start() {
          await kafkaProducer.connect();
          log.info({ brokers: config.kafkaBrokers, topic: config.kafkaTopic }, 'kafka producer connected');
        },
        async stop() {
          await kafkaProducer.disconnect();
        },
      }),
    ).singleton(),
  });
}
