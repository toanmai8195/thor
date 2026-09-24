import type { RequestHandler } from 'express';

/** @param ping kiểm tra kết nối DB, throw nếu lỗi */
export function createHealthHandler(ping: () => Promise<unknown>): RequestHandler {
  return async (_req, res) => {
    await ping();
    res.json({ ok: true });
  };
}
