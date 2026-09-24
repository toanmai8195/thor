export interface Config {
  port: number;
  mongoUri: string;
  mongoDb: string;
  kafkaBrokers: string[];
  kafkaClientId: string;
  /** Topic friend-service ghi event vào; event-gateway đọc */
  kafkaTopic: string;
  /** Tên service ghi vào field `source` của event */
  source: string;
  /** 0–1023, phải khác nhau giữa các instance để event_id không trùng */
  workerId: number;
}

function int(v: string | undefined, def: number): number {
  const n = Number.parseInt(v ?? '', 10);
  return Number.isNaN(n) ? def : n;
}

const env = process.env;

export const config: Config = {
  port: int(env.PORT, 3000),

  mongoUri: env.MONGO_URI ?? 'mongodb://localhost:27017/?directConnection=true',
  mongoDb: env.MONGO_DB ?? 'friend_network',

  kafkaBrokers: (env.KAFKA_BROKERS ?? 'localhost:29092').split(',').map((s) => s.trim()),
  kafkaClientId: env.KAFKA_CLIENT_ID ?? 'friend-service',
  kafkaTopic: env.KAFKA_TOPIC ?? 'friend_service_events',

  source: env.EVENT_SOURCE ?? 'friend-service',
  workerId: int(env.WORKER_ID, 0),
};
