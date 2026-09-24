// Luật chuyển trạng thái của 1 cặp user. Hàm thuần, không đụng DB → dễ test.
// Mọi status nhìn từ góc của user_id (README 2.1).

import { DomainError } from '../utils/errors.js';

export const Status = {
  REQUESTED: 'REQUESTED',
  REVIEWED: 'REVIEWED',
  FRIEND: 'FRIEND',
  CANCEL: 'CANCEL',
  UNFRIEND: 'UNFRIEND',
  BLOCKING: 'BLOCKING',
  BLOCKED: 'BLOCKED',
} as const;
export type Status = (typeof Status)[keyof typeof Status];

export const Action = {
  REQUEST: 'request', // actor gửi lời mời cho target
  CANCEL: 'cancel', //   actor huỷ lời mời đã gửi
  ACCEPT: 'accept', //   actor chấp nhận lời mời target gửi
  REJECT: 'reject', //   actor từ chối lời mời target gửi
  UNFRIEND: 'unfriend',
  BLOCK: 'block',
} as const;
export type Action = (typeof Action)[keyof typeof Action];

/** Status mới của actor → target (fwd) và target → actor (rev) */
export interface Transition {
  fwd: Status;
  rev: Status;
}

const conflict = (code: string, message: string) => new DomainError(409, code, message);

/**
 * @param action hành động của actor
 * @param fwd    status hiện tại của actor → target (null = chưa có)
 * @throws DomainError khi hành động không hợp lệ với trạng thái hiện tại
 *
 * Service luôn ghi cả 2 chiều trong 1 transaction nên chỉ cần nhìn chiều actor → target.
 */
export function plan(action: Action, fwd: Status | null): Transition {
  switch (action) {
    case Action.REQUEST:
      if (fwd === Status.BLOCKING || fwd === Status.BLOCKED) {
        throw new DomainError(403, 'BLOCKED', 'Không thể gửi lời mời khi đang block / bị block');
      }
      if (fwd === Status.FRIEND) throw conflict('ALREADY_FRIENDS', 'Hai user đã là bạn');
      if (fwd === Status.REQUESTED) throw conflict('ALREADY_REQUESTED', 'Đã gửi lời mời, đang chờ');
      if (fwd === Status.REVIEWED) {
        throw conflict('PENDING_FROM_TARGET', 'Đối phương đã gửi lời mời cho bạn, hãy accept');
      }
      // Còn lại: chưa từng có quan hệ, hoặc đã CANCEL / UNFRIEND
      return { fwd: Status.REQUESTED, rev: Status.REVIEWED };

    case Action.CANCEL:
      if (fwd !== Status.REQUESTED) throw conflict('NO_SENT_REQUEST', 'Không có lời mời đã gửi để huỷ');
      return { fwd: Status.CANCEL, rev: Status.CANCEL };

    case Action.ACCEPT:
      if (fwd !== Status.REVIEWED) throw conflict('NO_RECEIVED_REQUEST', 'Không có lời mời để chấp nhận');
      return { fwd: Status.FRIEND, rev: Status.FRIEND };

    case Action.REJECT:
      if (fwd !== Status.REVIEWED) throw conflict('NO_RECEIVED_REQUEST', 'Không có lời mời để từ chối');
      return { fwd: Status.CANCEL, rev: Status.CANCEL };

    case Action.UNFRIEND:
      if (fwd !== Status.FRIEND) throw conflict('NOT_FRIENDS', 'Hai user không phải là bạn');
      return { fwd: Status.UNFRIEND, rev: Status.UNFRIEND };

    case Action.BLOCK:
      if (fwd === Status.BLOCKING) throw conflict('ALREADY_BLOCKING', 'Đã block user này');
      // Chính sách cho vấn đề mở #2: không cho block ngược, tránh ghi đè mất BLOCKING của đối phương
      if (fwd === Status.BLOCKED) throw conflict('BLOCKED_BY_TARGET', 'Đang bị user này block');
      return { fwd: Status.BLOCKING, rev: Status.BLOCKED };

    default:
      throw new DomainError(400, 'UNKNOWN_ACTION', `Hành động không hợp lệ: ${String(action)}`);
  }
}
