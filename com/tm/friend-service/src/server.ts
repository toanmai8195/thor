// MainServer: đọc config, dựng DI container, start các lifecycle theo thứ tự, dừng ngược lại khi shutdown.

import { config } from './configs/config.js';
import { LIFECYCLE_ORDER, buildContainer } from './container.js';
import type { Lifecycle } from './utils/lifecycle.js';

async function main(): Promise<void> {
  const container = buildContainer(config);
  const { log } = container.cradle;

  const started: Lifecycle[] = [];
  let stopping = false;
  const shutdown = async (reason: string, code: number) => {
    if (stopping) return;
    stopping = true;
    log.info({ reason }, 'shutting down');
    for (const lifecycle of started.reverse()) {
      try {
        await lifecycle.stop();
      } catch (err) {
        log.error({ err, lifecycle: lifecycle.name }, 'stop failed');
      }
    }
    await container.dispose();
    process.exit(code);
  };
  process.on('SIGINT', () => void shutdown('SIGINT', 0));
  process.on('SIGTERM', () => void shutdown('SIGTERM', 0));

  for (const name of LIFECYCLE_ORDER) {
    const lifecycle = container.resolve(name);
    try {
      await lifecycle.start();
    } catch (err) {
      log.error({ err, lifecycle: lifecycle.name }, 'startup failed');
      await shutdown('startup failed', 1);
      return;
    }
    started.push(lifecycle);
  }
}

void main();
