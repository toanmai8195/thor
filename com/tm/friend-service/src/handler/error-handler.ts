import type { ErrorRequestHandler } from 'express';
import { DomainError } from '../utils/errors.js';
import type { Logger } from '../utils/logger.js';

/** Express error middleware: DomainError → HTTP status + mã lỗi, còn lại → 500. */
export function createErrorHandler(log: Logger): ErrorRequestHandler {
  return (err, req, res, _next) => {
    if (err instanceof DomainError) {
      res.status(err.httpStatus).json({ error: err.code, message: err.message });
      return;
    }
    log.error({ err, method: req.method, path: req.path }, 'unhandled error');
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  };
}
