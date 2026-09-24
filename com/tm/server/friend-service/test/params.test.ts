import assert from 'node:assert/strict';
import { test } from 'node:test';
import { parsePaging, parseUserId } from '../src/handler/params.js';

test('parseUserId', () => {
  assert.equal(parseUserId('1001'), 1001);
  for (const bad of ['0', '-1', 'abc', '1.5', '9007199254740993', '']) {
    assert.throws(() => parseUserId(bad), { code: 'INVALID_USER_ID' });
  }
});

test('parsePaging', () => {
  assert.deepEqual(parsePaging({}), { limit: 50, after: undefined });
  assert.deepEqual(parsePaging({ limit: '10', after: '5' }), { limit: 10, after: 5 });
  for (const limit of ['0', '201', 'x']) {
    assert.throws(() => parsePaging({ limit }), { code: 'INVALID_LIMIT' });
  }
});
