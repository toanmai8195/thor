import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Action, Status, type Transition, plan } from '../src/controller/friendship-rules.js';
import { DomainError } from '../src/utils/errors.js';

const S = Status;

// [action, trạng thái actor→target hiện tại, kết quả mong đợi {fwd, rev} | mã lỗi]
const cases: [Action, Status | null, Transition | string][] = [
  [Action.REQUEST, null, { fwd: S.REQUESTED, rev: S.REVIEWED }],
  [Action.REQUEST, S.CANCEL, { fwd: S.REQUESTED, rev: S.REVIEWED }],
  [Action.REQUEST, S.UNFRIEND, { fwd: S.REQUESTED, rev: S.REVIEWED }],
  [Action.REQUEST, S.REQUESTED, 'ALREADY_REQUESTED'],
  [Action.REQUEST, S.REVIEWED, 'PENDING_FROM_TARGET'],
  [Action.REQUEST, S.FRIEND, 'ALREADY_FRIENDS'],
  [Action.REQUEST, S.BLOCKING, 'BLOCKED'],
  [Action.REQUEST, S.BLOCKED, 'BLOCKED'],

  [Action.CANCEL, S.REQUESTED, { fwd: S.CANCEL, rev: S.CANCEL }],
  [Action.CANCEL, null, 'NO_SENT_REQUEST'],
  [Action.CANCEL, S.REVIEWED, 'NO_SENT_REQUEST'],

  [Action.ACCEPT, S.REVIEWED, { fwd: S.FRIEND, rev: S.FRIEND }],
  [Action.ACCEPT, S.REQUESTED, 'NO_RECEIVED_REQUEST'],
  [Action.ACCEPT, null, 'NO_RECEIVED_REQUEST'],

  [Action.REJECT, S.REVIEWED, { fwd: S.CANCEL, rev: S.CANCEL }],
  [Action.REJECT, S.REQUESTED, 'NO_RECEIVED_REQUEST'],

  [Action.UNFRIEND, S.FRIEND, { fwd: S.UNFRIEND, rev: S.UNFRIEND }],
  [Action.UNFRIEND, S.REQUESTED, 'NOT_FRIENDS'],

  [Action.BLOCK, null, { fwd: S.BLOCKING, rev: S.BLOCKED }],
  [Action.BLOCK, S.FRIEND, { fwd: S.BLOCKING, rev: S.BLOCKED }],
  [Action.BLOCK, S.REVIEWED, { fwd: S.BLOCKING, rev: S.BLOCKED }],
  [Action.BLOCK, S.BLOCKING, 'ALREADY_BLOCKING'],
  [Action.BLOCK, S.BLOCKED, 'BLOCKED_BY_TARGET'],
];

for (const [action, fwd, expected] of cases) {
  test(`${action} từ ${fwd ?? 'null'}`, () => {
    if (typeof expected === 'string') {
      assert.throws(
        () => plan(action, fwd),
        (err) => err instanceof DomainError && err.code === expected,
      );
    } else {
      assert.deepEqual(plan(action, fwd), expected);
    }
  });
}

test('action không hợp lệ → 400', () => {
  assert.throws(
    () => plan('hug' as Action, null),
    (err) => err instanceof DomainError && err.httpStatus === 400,
  );
});
