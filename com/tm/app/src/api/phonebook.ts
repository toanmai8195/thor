// phonebook-service API (PHONEBOOK.md mục 2.2).

import type { Entry } from '../lib/digest';
import { request, type RequestInfo } from './http';

const BASE = '/api/phonebook/v1';

export interface CheckResponse {
  status: 'UNCHANGED' | 'NEED_BUCKETS' | 'UPLOAD';
  changed?: number[];
}

export interface SyncResponse {
  root?: string;
  sync_id?: string;
  added: number;
  deleted: number;
  contact_cnt: number;
  device_contact_cnt: number;
  rejected: number;
  published: boolean;
}

export interface ServerContact {
  phone: string;
  name: string;
}

export interface ListResponse {
  contacts: ServerContact[];
  total: number;
  next_cursor: string | null;
}

export interface SummaryResponse {
  user_id: number;
  contact_cnt: number;
  pending: boolean;
  devices: { device_id: string; contact_cnt: number; synced_at: number }[];
}

const dev = (userId: number, deviceId: string) =>
  `${BASE}/users/${userId}/devices/${encodeURIComponent(deviceId)}/phonebook`;

export const phonebookApi = {
  check: (userId: number, deviceId: string, body: { v: number; root: string; buckets?: string }) =>
    request<CheckResponse>('POST', `${dev(userId, deviceId)}/check`, body),

  uploadBuckets: (
    userId: number,
    deviceId: string,
    buckets: Record<string, { d: string; contacts: Entry[] }>,
    info?: RequestInfo,
  ) => request<SyncResponse>('PUT', `${dev(userId, deviceId)}/buckets`, { v: 1, buckets }, info),

  deleteDevice: (userId: number, deviceId: string) => request<SyncResponse>('DELETE', dev(userId, deviceId)),

  list: (userId: number, opts: { limit: number; cursor?: string | null; deviceId?: string }) => {
    const q = new URLSearchParams({ limit: String(opts.limit) });
    if (opts.cursor) q.set('cursor', opts.cursor);
    if (opts.deviceId) q.set('device_id', opts.deviceId);
    return request<ListResponse>('GET', `${BASE}/users/${userId}/phonebook/contacts?${q}`);
  },

  lookup: (userId: number, phone: string) =>
    request<ServerContact>('GET', `${BASE}/users/${userId}/phonebook/contacts/${encodeURIComponent(phone)}`),

  summary: (userId: number) => request<SummaryResponse>('GET', `${BASE}/users/${userId}/phonebook/summary`),
};
