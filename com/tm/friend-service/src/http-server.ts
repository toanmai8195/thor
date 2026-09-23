// DI: Express app và lifecycle của HTTP server.

import type { Server } from 'node:http';
import { type AwilixContainer, asFunction } from 'awilix';
import express, { type Express } from 'express';
import type { HandlerCradle } from './handler/handler-module.js';
import type { RouterCradle } from './router/router-module.js';
import type { Lifecycle } from './utils/lifecycle.js';
import type { UtilsCradle } from './utils/utils-module.js';

export interface HttpSettings {
  port: number;
}

export interface HttpCradle {
  app: Express;
  httpLifecycle: Lifecycle;
}

type Deps = HttpCradle &
  RouterCradle &
  Pick<HandlerCradle, 'errorHandler'> &
  Pick<UtilsCradle, 'log'> & {
    config: HttpSettings;
  };

export function registerHttp(container: AwilixContainer): void {
  container.register({
    app: asFunction(({ router, errorHandler }: Deps) => {
      const app = express();
      app.use(express.json());
      app.use(router);
      app.use(errorHandler);
      return app;
    }).singleton(),

    httpLifecycle: asFunction(({ app, config, log }: Deps): Lifecycle => {
      let server: Server | undefined;
      return {
        name: 'http',
        start: () =>
          new Promise<void>((resolve, reject) => {
            server = app.listen(config.port, () => {
              log.info({ port: config.port }, 'http listening');
              resolve();
            });
            server.once('error', reject);
          }),
        stop: () =>
          new Promise<void>((resolve) => {
            if (!server) return resolve();
            server.close(() => resolve());
            server.closeIdleConnections();
          }),
      };
    }).singleton(),
  });
}
