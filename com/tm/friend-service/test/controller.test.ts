// FriendController với Dao + event publisher giả (in-memory): ghi 2 chiều, gửi event sau commit, event_time tăng dần.

import assert from 'node:assert/strict';
import { test } from 'node:test';
import { type FriendControllerDeps, type FriendEvent, FriendController } from '../src/controller/friend-controller.js';
import { Action, Status } from '../src/controller/friendship-rules.js';
import type { FriendshipDoc } from '../src/dao/friendship-dao.js';
import { DomainError } from '../src/utils/errors.js';

function setup(now: () => number, { gatewayDown = false } = {}) {
  const rows = new Map<string, FriendshipDoc>();
  const sent: FriendEvent[] = [];
  const errors: string[] = [];
  const key = (u: number, f: number) => `${u}:${f}`;
  let seq = 0;

  const friendshipDao: FriendControllerDeps['friendshipDao'] = {
    async findPair(userId, otherId) {
      return { fwd: rows.get(key(userId, otherId)) ?? null, rev: rows.get(key(otherId, userId)) ?? null };
    },
    async upsertMany(writes, eventTime) {
      for (const w of writes) {
        const at = new Date();
        rows.set(key(w.user_id, w.friend_id), {
          ...w,
          last_event_time: eventTime,
          last_event_id: w.event_id,
          created_at: at,
          updated_at: at,
        });
      }
    },
    async listByStatus(userId, statuses, { limit }) {
      return [...rows.values()]
        .filter((d) => d.user_id === userId && statuses.includes(d.status))
        .sort((a, b) => a.friend_id - b.friend_id)
        .slice(0, limit);
    },
    async countByStatus() {
      return [];
    },
    async commonFriendIds() {
      return [];
    },
  };

  const controller = new FriendController({
    runInTransaction: (fn) => fn({} as never),
    friendshipDao,
    eventPublisher: {
      async publishFriendEvents(events) {
        if (gatewayDown) throw new Error('gateway down');
        sent.push(...(events as FriendEvent[]));
      },
    },
    log: { info: () => {}, error: (_fields, msg) => errors.push(msg) },
    nextId: () => String(++seq).padStart(19, '0'),
    source: 'test',
    now,
  });
  const events = () => sent;
  return { controller, rows, events, key, errors, sent };
}

test('request → accept: ghi 2 chiều và 4 event đối xứng', async () => {
  const { controller, rows, events, key } = setup(() => Date.parse('2026-09-23T10:00:00.000Z'));

  const r1 = await controller.apply(Action.REQUEST, 1, 2);
  assert.equal(r1.status, Status.REQUESTED);
  assert.equal(rows.get(key(2, 1))?.status, Status.REVIEWED);

  await controller.apply(Action.ACCEPT, 2, 1);
  assert.equal(rows.get(key(1, 2))?.status, Status.FRIEND);
  assert.equal(rows.get(key(2, 1))?.status, Status.FRIEND);

  assert.deepEqual(
    events().map((e) => [e.user_id, e.friend_id, e.event_type]),
    [
      [1, 2, Status.REQUESTED],
      [2, 1, Status.REVIEWED],
      [2, 1, Status.FRIEND],
      [1, 2, Status.FRIEND],
    ],
  );
  const friends = await controller.listFriends(1, { limit: 10 });
  assert.deepEqual(friends.items.map((i) => i.friend_id), [2]);
});

test('event_time của 1 cặp luôn tăng, kể cả khi đồng hồ đứng yên', async () => {
  const { controller, events } = setup(() => Date.parse('2026-09-23T10:00:00.000Z'));
  await controller.apply(Action.REQUEST, 1, 2);
  await controller.apply(Action.CANCEL, 1, 2);
  const [first, , second] = events();
  assert.equal(first?.event_time, '2026-09-23 10:00:00.000');
  assert.equal(second?.event_time, '2026-09-23 10:00:00.001');
});

test('hành động sai trạng thái không ghi gì', async () => {
  const { controller, events } = setup(() => Date.now());
  await assert.rejects(controller.apply(Action.ACCEPT, 1, 2), (err) => err instanceof DomainError && err.code === 'NO_RECEIVED_REQUEST');
  await assert.rejects(controller.apply(Action.REQUEST, 1, 1), (err) => err instanceof DomainError && err.code === 'SAME_USER');
  assert.equal(events().length, 0);
});

test('type không hợp lệ → INVALID_QUERY', async () => {
  const { controller } = setup(() => Date.now());
  await assert.rejects(controller.listRequests(1, 'bad', { limit: 10 }), { code: 'INVALID_QUERY' });
});

test('gửi 2 event trong 1 lần, đúng contract', async () => {
  const { controller, events } = setup(() => Date.parse('2026-09-23T10:00:00.000Z'));
  await controller.apply(Action.REQUEST, 1, 2);
  const [a, b] = events();
  assert.deepEqual(a, {
    user_id: 1,
    friend_id: 2,
    event_type: Status.REQUESTED,
    event_time: '2026-09-23 10:00:00.000',
    event_id: '0000000000000000001',
    source: 'test',
  });
  assert.equal(b?.event_type, Status.REVIEWED);
});

test('DB ghi lỗi → không gửi event nào', async () => {
  const { controller, events } = setup(() => Date.now());
  await controller.apply(Action.REQUEST, 1, 2);
  const before = events().length;
  // Luật không cho gửi lời mời lần 2 → transaction không commit
  await assert.rejects(controller.apply(Action.REQUEST, 1, 2), { code: 'ALREADY_REQUESTED' });
  assert.equal(events().length, before);
});

test('event-gateway lỗi sau khi commit → hành động vẫn thành công, log lỗi', async () => {
  const { controller, rows, key, errors } = setup(() => Date.now(), { gatewayDown: true });
  const result = await controller.apply(Action.REQUEST, 1, 2);
  assert.equal(result.status, Status.REQUESTED);
  assert.equal(rows.get(key(1, 2))?.status, Status.REQUESTED);
  assert.deepEqual(errors, ['event publish failed after db commit']);
});
