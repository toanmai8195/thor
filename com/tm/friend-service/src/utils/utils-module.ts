// DI: đăng ký tiện ích dùng chung vào container.

import { type AwilixContainer, asFunction, asValue } from 'awilix';
import { type IdGenerator, createIdGenerator } from './ids.js';
import { type Logger, log } from './logger.js';

/** Phần config module này cần */
export interface UtilsSettings {
  workerId: number;
}

export interface UtilsCradle {
  log: Logger;
  /** Sinh event_id */
  nextId: IdGenerator;
}

interface Deps {
  config: UtilsSettings;
}

export function registerUtils(container: AwilixContainer): void {
  container.register({
    log: asValue(log),
    nextId: asFunction(({ config }: Deps) => createIdGenerator(config.workerId)).singleton(),
  });
}
