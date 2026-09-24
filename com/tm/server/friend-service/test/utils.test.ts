import assert from 'node:assert/strict';
import { test } from 'node:test';
import { EVENT_ID_LENGTH, createIdGenerator } from '../src/utils/ids.js';
import { formatEventTime } from '../src/utils/time.js';

test('event_id tăng dần, cố định độ dài, so sánh chuỗi đúng', () => {
  let t = 1758600000000n;
  const next = createIdGenerator(7, () => t);
  const ids = [];
  for (let i = 0; i < 5000; i++) ids.push(next()); // vượt 4096 sequence trong 1 ms
  t -= 10n; // đồng hồ lùi
  ids.push(next());
  t += 1000n;
  ids.push(next());
  for (let i = 1; i < ids.length; i++) {
    assert.equal(ids[i].length, EVENT_ID_LENGTH);
    assert.ok(ids[i] > ids[i - 1], `${ids[i]} phải > ${ids[i - 1]}`);
  }
});

test('WORKER_ID ngoài khoảng → lỗi', () => {
  assert.throws(() => createIdGenerator(1024));
});

test('formatEventTime theo yyyy-MM-dd HH:mm:ss.SSS UTC', () => {
  assert.equal(formatEventTime(new Date('2026-09-23T10:10:00.123Z')), '2026-09-23 10:10:00.123');
});
