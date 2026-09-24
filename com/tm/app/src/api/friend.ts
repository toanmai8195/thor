// friend-service API (README server mục 10.6).

import { request } from './http';

const BASE = '/api/friend/v1';

export type Status = 'REQUESTED' | 'REVIEWED' | 'FRIEND' | 'CANCEL' | 'UNFRIEND' | 'BLOCKING' | 'BLOCKED';

export type Action = 'request' | 'cancel' | 'accept' | 'reject' | 'unfriend' | 'block';

export interface ActionResult {
  user_id: number;
  friend_id: number;
  status: Status;
  event_time: string;
}

export interface DirectionView {
  status: Status;
  since: string;
  event_id: string;
}

export interface Relationship {
  user_id: number;
  friend_id: number;
  /** user_id → friend_id */
  outgoing: DirectionView | null;
  /** friend_id → user_id */
  incoming: DirectionView | null;
}

export interface ListResult {
  items: { friend_id: number; status: Status; since: string }[];
  next_after: number | null;
}

export interface FriendSummary {
  user_id: number;
  friend_cnt: number;
  pending_sent_cnt: number;
  pending_received_cnt: number;
  blocking_cnt: number;
  blocked_cnt: number;
  last_activity: string | null;
}

const u = (userId: number) => `${BASE}/users/${userId}`;

const ACTION_CALL: Record<Action, (a: number, b: number) => Promise<ActionResult>> = {
  request: (a, b) => request('POST', `${u(a)}/requests/${b}`),
  cancel: (a, b) => request('DELETE', `${u(a)}/requests/${b}`),
  accept: (a, b) => request('POST', `${u(a)}/requests/${b}/accept`),
  reject: (a, b) => request('POST', `${u(a)}/requests/${b}/reject`),
  unfriend: (a, b) => request('DELETE', `${u(a)}/friends/${b}`),
  block: (a, b) => request('POST', `${u(a)}/blocks/${b}`),
};

export const ACTION_LABEL: Record<Action, string> = {
  request: 'Gửi lời mời',
  cancel: 'Huỷ lời mời',
  accept: 'Chấp nhận',
  reject: 'Từ chối',
  unfriend: 'Huỷ kết bạn',
  block: 'Block',
};

/**
 * Hành động hợp lệ của actor theo trạng thái actor → target (luật ở README server 10.5).
 * Không có unblock (friend-service chưa hỗ trợ).
 */
export function allowedActions(status: Status | undefined): Action[] {
  switch (status) {
    case undefined:
    case 'CANCEL':
    case 'UNFRIEND':
      return ['request', 'block'];
    case 'REQUESTED':
      return ['cancel', 'block'];
    case 'REVIEWED':
      return ['accept', 'reject', 'block'];
    case 'FRIEND':
      return ['unfriend', 'block'];
    case 'BLOCKING':
    case 'BLOCKED':
      return [];
  }
}

export const friendApi = {
  act: (action: Action, actor: number, target: number) => ACTION_CALL[action](actor, target),
  relationship: (a: number, b: number) => request<Relationship>('GET', `${u(a)}/relationships/${b}`),
  summary: (a: number) => request<FriendSummary>('GET', `${u(a)}/summary`),
  friends: (a: number) => request<ListResult>('GET', `${u(a)}/friends?limit=200`),
  requests: (a: number, type: 'sent' | 'received' | 'all' = 'all') =>
    request<ListResult>('GET', `${u(a)}/requests?type=${type}&limit=200`),
  blocks: (a: number, type: 'blocking' | 'blocked' | 'all' = 'all') =>
    request<ListResult>('GET', `${u(a)}/blocks?type=${type}&limit=200`),
  mutualFriends: (a: number, b: number) => request<{ items: number[] }>('GET', `${u(a)}/mutual-friends/${b}?limit=200`),
};
