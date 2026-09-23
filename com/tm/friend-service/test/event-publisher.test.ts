// Map event → message Kafka: đúng topic, key = user_id, value = JSON event, acks=all.

import assert from 'node:assert/strict';
import { test } from 'node:test';
import { type FriendEventPayload, type KafkaSender, createEventPublisher } from '../src/dao/event-publisher.js';

const events: FriendEventPayload[] = [
  { user_id: 1, friend_id: 2, event_type: 'REQUESTED', event_time: '2026-09-23 10:00:00.000', event_id: '0228440659025985536', source: 'friend-service' },
  { user_id: 2, friend_id: 1, event_type: 'REVIEWED', event_time: '2026-09-23 10:00:00.000', event_id: '0228440659025985537', source: 'friend-service' },
];

test('gửi 2 event trong 1 lần send, key = user_id', async () => {
  const records: Parameters<KafkaSender['send']>[0][] = [];
  const publisher = createEventPublisher({ send: async (r) => records.push(r) }, 'friend_service_events');
  await publisher.publishFriendEvents(events);

  assert.equal(records.length, 1);
  const [record] = records;
  assert.equal(record?.topic, 'friend_service_events');
  assert.equal(record?.acks, -1);
  assert.deepEqual(record?.messages.map((m) => m.key), ['1', '2']);
  assert.deepEqual(JSON.parse(record?.messages[0]?.value ?? ''), events[0]);
});

test('Kafka lỗi → reject để Controller log', async () => {
  const publisher = createEventPublisher(
    {
      send: async () => {
        throw new Error('kafka down');
      },
    },
    'friend_service_events',
  );
  await assert.rejects(publisher.publishFriendEvents(events), /kafka down/);
});
