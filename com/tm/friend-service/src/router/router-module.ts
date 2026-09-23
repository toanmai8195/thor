// DI: Express router gắn các handler.

import { type AwilixContainer, asFunction } from 'awilix';
import type { Router } from 'express';
import type { HandlerCradle } from '../handler/handler-module.js';
import { createRouter } from './router.js';

export interface RouterCradle {
  router: Router;
}

type Deps = Pick<HandlerCradle, 'friendHandler' | 'healthHandler'>;

export function registerRouter(container: AwilixContainer): void {
  container.register({
    router: asFunction(({ friendHandler, healthHandler }: Deps) =>
      createRouter({ friend: friendHandler, health: healthHandler }),
    ).singleton(),
  });
}
