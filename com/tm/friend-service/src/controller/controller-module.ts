// DI: FriendController.

import { type AwilixContainer, asFunction } from 'awilix';
import type { DaoCradle } from '../dao/dao-module.js';
import type { UtilsCradle } from '../utils/utils-module.js';
import { FriendController } from './friend-controller.js';

/** Phần config module này cần */
export interface ControllerSettings {
  /** Field `source` của event */
  source: string;
}

export interface ControllerCradle {
  friendController: FriendController;
}

type Deps = UtilsCradle &
  Pick<DaoCradle, 'runInTransaction' | 'friendshipDao' | 'eventPublisher'> & {
    config: ControllerSettings;
  };

export function registerController(container: AwilixContainer): void {
  container.register({
    friendController: asFunction(
      ({ runInTransaction, friendshipDao, eventPublisher, nextId, log, config }: Deps) =>
        new FriendController({
          runInTransaction,
          friendshipDao,
          eventPublisher,
          nextId,
          source: config.source,
          log,
        }),
    ).singleton(),
  });
}
