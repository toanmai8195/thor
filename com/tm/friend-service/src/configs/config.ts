export interface Config {
  port: number;
  mongoUri: string;
  mongoDb: string;
  /** Base URL của event-gateway, vd http://event-gateway:8080 */
  eventGatewayUrl: string;
  eventGatewayTimeoutMs: number;
  /** Số lần gửi tối đa khi lỗi mạng / timeout / 5xx */
  eventGatewayMaxAttempts: number;
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

  eventGatewayUrl: env.EVENT_GATEWAY_URL ?? 'http://localhost:8080',
  eventGatewayTimeoutMs: int(env.EVENT_GATEWAY_TIMEOUT_MS, 3000),
  eventGatewayMaxAttempts: int(env.EVENT_GATEWAY_MAX_ATTEMPTS, 3),

  source: env.EVENT_SOURCE ?? 'friend-service',
  workerId: int(env.WORKER_ID, 0),
};
