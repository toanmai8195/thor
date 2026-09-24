// DI container: gom module của từng layer. Thêm layer / thành phần mới = thêm register* ở đây.

import { type AwilixContainer, InjectionMode, asValue, createContainer } from 'awilix';
import type { Config } from './configs/config.js';
import { type ControllerCradle, registerController } from './controller/controller-module.js';
import { type DaoCradle, registerDao } from './dao/dao-module.js';
import { type HandlerCradle, registerHandler } from './handler/handler-module.js';
import { type HttpCradle, registerHttp } from './http-server.js';
import { type RouterCradle, registerRouter } from './router/router-module.js';
import type { Lifecycle } from './utils/lifecycle.js';
import { type UtilsCradle, registerUtils } from './utils/utils-module.js';

export type Cradle = { config: Config } & UtilsCradle &
  DaoCradle &
  ControllerCradle &
  HandlerCradle &
  RouterCradle &
  HttpCradle;

type LifecycleName = { [K in keyof Cradle]: Cradle[K] extends Lifecycle ? K : never }[keyof Cradle];

/** Thứ tự start; stop theo thứ tự ngược lại */
export const LIFECYCLE_ORDER: readonly LifecycleName[] = ['mongoLifecycle', 'kafkaLifecycle', 'httpLifecycle'];

export function buildContainer(config: Config): AwilixContainer<Cradle> {
  // PROXY: factory nhận object cradle, lấy dependency bằng destructuring
  const container = createContainer<Cradle>({ injectionMode: InjectionMode.PROXY, strict: true });
  container.register({ config: asValue(config) });
  registerUtils(container);
  registerDao(container);
  registerController(container);
  registerHandler(container);
  registerRouter(container);
  registerHttp(container);
  return container;
}
