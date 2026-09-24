// DI: các HTTP handler.

import { type AwilixContainer, asFunction } from 'awilix';
import type { ErrorRequestHandler, RequestHandler } from 'express';
import type { ControllerCradle } from '../controller/controller-module.js';
import type { UtilsCradle } from '../utils/utils-module.js';
import { createErrorHandler } from './error-handler.js';
import { type FriendHandler, createFriendHandler } from './friend-handler.js';
import { createHealthHandler } from './health-handler.js';

export interface HandlerCradle {
  friendHandler: FriendHandler;
  healthHandler: RequestHandler;
  errorHandler: ErrorRequestHandler;
}

type Deps = Pick<ControllerCradle, 'friendController'> &
  Pick<UtilsCradle, 'log'> & {
    /** Do Dao cung cấp; Handler chỉ cần biết đây là hàm ping */
    pingDb: () => Promise<unknown>;
  };

export function registerHandler(container: AwilixContainer): void {
  container.register({
    friendHandler: asFunction(({ friendController }: Deps) => createFriendHandler(friendController)).singleton(),
    healthHandler: asFunction(({ pingDb }: Deps) => createHealthHandler(pingDb)).singleton(),
    errorHandler: asFunction(({ log }: Deps) => createErrorHandler(log)).singleton(),
  });
}
