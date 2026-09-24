// DB local (IndexedDB qua Dexie). Web giả lập nhiều user / thiết bị: mọi dữ liệu danh bạ gắn với
// deviceKey = "<userId>#<deviceId>".

import Dexie, { type Table } from 'dexie';

export interface LocalContact {
  id?: number;
  deviceKey: string;
  name: string;
  /** Số như người dùng gõ (đã bỏ ký tự phân cách); số không hợp lệ vẫn giữ nhưng không đồng bộ */
  phones: string[];
  createdAt: number;
  updatedAt: number;
}

export interface SyncState {
  deviceKey: string;
  /** root (base64url) server xác nhận ở lần đồng bộ thành công gần nhất */
  lastRoot?: string;
  lastSyncAt?: number;
}

export interface SyncStep {
  title: string;
  detail: string;
  ms: number;
  ok: boolean;
}

export interface SyncLog {
  id?: number;
  deviceKey: string;
  startedAt: number;
  ok: boolean;
  summary: string;
  changed: number[];
  steps: SyncStep[];
}

export interface FriendUser {
  userId: number;
  label: string;
}

class AppDB extends Dexie {
  contacts!: Table<LocalContact, number>;
  syncStates!: Table<SyncState, string>;
  syncLogs!: Table<SyncLog, number>;
  friendUsers!: Table<FriendUser, number>;

  constructor() {
    super('tm-app');
    this.version(1).stores({
      contacts: '++id, deviceKey, [deviceKey+name], updatedAt',
      syncStates: 'deviceKey',
      syncLogs: '++id, deviceKey, startedAt',
      friendUsers: 'userId',
    });
  }
}

export const db = new AppDB();

export function deviceKeyOf(userId: number, deviceId: string): string {
  return `${userId}#${deviceId}`;
}
