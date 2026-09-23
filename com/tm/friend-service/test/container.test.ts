// DI container: mọi registration resolve được (không kết nối MongoDB / Kafka thật: kết nối nằm trong lifecycle).
// Thiếu hoặc sai tên dependency → test này fail, tương tự Dagger báo lỗi lúc compile.

import assert from 'node:assert/strict';
import { test } from 'node:test';
import { type Cradle, LIFECYCLE_ORDER, buildContainer } from '../src/container.js';

const config: Cradle['config'] = {
  port: 0,
  mongoUri: 'mongodb://localhost:27017/?directConnection=true',
  mongoDb: 'friend_network_test',
  kafkaBrokers: ['localhost:29092'],
  kafkaClientId: 'friend-service-test',
  kafkaTopic: 'friend_service_events',
  source: 'friend-service-test',
  workerId: 0,
};

test('resolve được mọi registration', async () => {
  const container = buildContainer(config);
  const names = Object.keys(container.registrations);
  assert.ok(names.length > 10);
  for (const name of names) {
    assert.doesNotThrow(() => container.resolve(name), `không resolve được "${name}"`);
  }
  await container.dispose();
});

test('mỗi lifecycle trong LIFECYCLE_ORDER đều đăng ký và có start/stop', async () => {
  const container = buildContainer(config);
  for (const name of LIFECYCLE_ORDER) {
    const lifecycle = container.resolve(name);
    assert.equal(typeof lifecycle.start, 'function');
    assert.equal(typeof lifecycle.stop, 'function');
  }
  await container.dispose();
});

test('singleton: cùng 1 instance cho mỗi lần resolve', async () => {
  const container = buildContainer(config);
  assert.equal(container.resolve('friendController'), container.resolve('friendController'));
  assert.equal(container.resolve('mongoClient'), container.resolve('mongoClient'));
  await container.dispose();
});
