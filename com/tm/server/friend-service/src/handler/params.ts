// Parse + validate tham số HTTP.

import type { Paging } from '../controller/friend-controller.js';
import { DomainError } from '../utils/errors.js';

const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 200;

export function parseUserId(raw: unknown, name = 'user_id'): number {
  const s = String(raw ?? '');
  const n = /^\d{1,16}$/.test(s) ? Number(s) : Number.NaN;
  // Giới hạn ở safe integer để JSON gửi Kafka không mất chính xác (BIGINT phía StarRocks)
  if (!Number.isSafeInteger(n) || n <= 0) {
    throw new DomainError(400, 'INVALID_USER_ID', `${name} phải là số nguyên dương ≤ 2^53-1`);
  }
  return n;
}

/** ?limit=&after= → Paging */
export function parsePaging(query: { limit?: unknown; after?: unknown }): Paging {
  const limit = query.limit === undefined ? DEFAULT_LIMIT : Number(query.limit);
  if (!Number.isInteger(limit) || limit < 1 || limit > MAX_LIMIT) {
    throw new DomainError(400, 'INVALID_LIMIT', `limit phải trong khoảng 1–${MAX_LIMIT}`);
  }
  const after = query.after === undefined ? undefined : parseUserId(query.after, 'after');
  return { limit, after };
}

/** Query string đơn (không phải mảng / object), mặc định `def` khi thiếu. */
export function parseQueryString(raw: unknown, name: string, def: string): string {
  if (raw === undefined) return def;
  if (typeof raw !== 'string') {
    throw new DomainError(400, 'INVALID_QUERY', `${name} phải là 1 giá trị`);
  }
  return raw;
}
