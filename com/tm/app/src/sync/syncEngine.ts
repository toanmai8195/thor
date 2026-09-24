// Đồng bộ danh bạ local ↔ server theo bucket (PHONEBOOK.md mục 2.4, 2.5):
//
//   1. tính digest local (root + 100 digest bucket)
//   2. check {root} (kèm buckets nếu root ≠ last_root) → UNCHANGED / NEED_BUCKETS / UPLOAD {changed}
//   3. upload các bucket đổi, gom ≤ MAX_BATCH contact / request
//   4. root server trả về = root local → lưu last_root

import { phonebookApi, type SyncResponse } from '../api/phonebook';
import { db, deviceKeyOf, type SyncLog, type SyncStep } from '../db/db';
import { toB64Url } from '../lib/b64';
import { DIGEST_VERSION, TRUNC_LEN, splitBuckets, truncated, type Entry } from '../lib/digest';
import { errorText, type RequestInfo } from '../api/http';
import { computeLocalDigest } from './localDigest';

/** Số contact tối đa trong 1 request upload (server nhận tối đa 5.000) */
export const MAX_BATCH = 2000;

export interface SyncOptions {
  userId: number;
  deviceId: string;
  /** Luôn gửi kèm buckets (bỏ qua last_root) */
  forceBuckets?: boolean;
  onStep?: (step: SyncStep) => void;
}

export interface SyncOutcome {
  ok: boolean;
  status: 'UNCHANGED' | 'UPLOADED' | 'ERROR';
  changed: number[];
  added: number;
  deleted: number;
  requests: number;
  steps: SyncStep[];
}

export async function runSync(opt: SyncOptions): Promise<SyncOutcome> {
  const deviceKey = deviceKeyOf(opt.userId, opt.deviceId);
  const steps: SyncStep[] = [];
  const out: SyncOutcome = { ok: false, status: 'ERROR', changed: [], added: 0, deleted: 0, requests: 0, steps };
  const startedAt = Date.now();
  const step = (title: string, detail: string, t0: number, ok = true) => {
    const s = { title, detail, ms: Math.round(performance.now() - t0), ok };
    steps.push(s);
    opt.onStep?.(s);
  };

  try {
    // 1. digest local
    let t = performance.now();
    const contacts = await db.contacts.where('deviceKey').equals(deviceKey).toArray();
    const local = await computeLocalDigest(contacts);
    const state = await db.syncStates.get(deviceKey);
    step(
      'Tính digest local',
      `${local.contacts} contact, ${local.canon.length} số hợp lệ (bỏ ${local.invalid} số sai), root ${local.rootB64.slice(0, 12)}…`,
      t,
    );

    // 2. check
    const withBuckets = opt.forceBuckets || state?.lastRoot !== local.rootB64;
    const bucketsB64 = toB64Url(truncated(local.digests));
    t = performance.now();
    let chk = await phonebookApi.check(opt.userId, opt.deviceId, {
      v: DIGEST_VERSION,
      root: local.rootB64,
      ...(withBuckets ? { buckets: bucketsB64 } : {}),
    });
    out.requests++;
    step('Check', `gửi ${withBuckets ? 'root + 100 digest bucket' : 'chỉ root (root = last_root)'} → ${chk.status}`, t);
    if (chk.status === 'NEED_BUCKETS') {
      t = performance.now();
      chk = await phonebookApi.check(opt.userId, opt.deviceId, { v: DIGEST_VERSION, root: local.rootB64, buckets: bucketsB64 });
      out.requests++;
      step('Check lại kèm buckets', `→ ${chk.status}`, t);
    }

    if (chk.status === 'UNCHANGED') {
      await db.syncStates.put({ deviceKey, lastRoot: local.rootB64, lastSyncAt: Date.now() });
      Object.assign(out, { ok: true, status: 'UNCHANGED' });
      return out;
    }

    // 3. upload bucket đổi theo lô
    const changed = chk.changed ?? [];
    out.changed = changed;
    const split = splitBuckets(local.canon);
    const batches = makeBatches(changed, split);
    step('Bucket cần gửi', `${changed.length} bucket, ${batches.length} request`, performance.now());
    let last: SyncResponse | undefined;
    for (const [i, batch] of batches.entries()) {
      const body: Record<string, { d: string; contacts: Entry[] }> = {};
      let n = 0;
      for (const k of batch) {
        body[String(k).padStart(2, '0')] = {
          d: toB64Url(local.digests.buckets[k].subarray(0, TRUNC_LEN)),
          contacts: split[k],
        };
        n += split[k].length;
      }
      const info: RequestInfo = { sentBytes: 0, gzipped: false };
      t = performance.now();
      last = await phonebookApi.uploadBuckets(opt.userId, opt.deviceId, body, info);
      out.requests++;
      out.added += last.added;
      out.deleted += last.deleted;
      step(
        `Upload ${i + 1}/${batches.length}`,
        `${batch.length} bucket, ${n} contact, ${(info.sentBytes / 1024).toFixed(1)} KB${info.gzipped ? ' gzip' : ''} → +${last.added} −${last.deleted}` +
          `${last.published ? '' : ' (Kafka lỗi, server sẽ gửi bù)'}`,
        t,
        true,
      );
    }

    // 4. so root
    t = performance.now();
    const match = last?.root === local.rootB64;
    if (match) await db.syncStates.put({ deviceKey, lastRoot: local.rootB64, lastSyncAt: Date.now() });
    step(
      'So root',
      match
        ? `root server = root local → lưu last_root; server có ${last?.contact_cnt} contact (mọi thiết bị)`
        : `root server ${last?.root?.slice(0, 12)}… ≠ root local → không lưu last_root (app / server chuẩn hoá khác nhau)`,
      t,
      match,
    );
    Object.assign(out, { ok: match, status: 'UPLOADED' });
    return out;
  } catch (e) {
    step('Lỗi', errorText(e), performance.now(), false);
    return out;
  } finally {
    const log: SyncLog = {
      deviceKey,
      startedAt,
      ok: out.ok,
      summary:
        out.status === 'UNCHANGED'
          ? 'Không đổi'
          : out.status === 'UPLOADED'
            ? `${out.changed.length} bucket, +${out.added} −${out.deleted}`
            : 'Lỗi',
      changed: out.changed,
      steps,
    };
    await db.syncLogs.add(log);
  }
}

/** Gom bucket thành các request ≤ MAX_BATCH contact (1 bucket lớn hơn thì đi 1 mình). */
export function makeBatches(changed: number[], split: Entry[][]): number[][] {
  const out: number[][] = [];
  let cur: number[] = [];
  let n = 0;
  for (const k of changed) {
    const size = split[k].length;
    if (cur.length > 0 && n + size > MAX_BATCH) {
      out.push(cur);
      cur = [];
      n = 0;
    }
    cur.push(k);
    n += size;
  }
  if (cur.length > 0) out.push(cur);
  return out;
}

/** Xoá danh bạ thiết bị trên server (logout / thu hồi quyền) và quên last_root. */
export async function deleteServerDevice(userId: number, deviceId: string): Promise<SyncResponse> {
  const res = await phonebookApi.deleteDevice(userId, deviceId);
  await db.syncStates.delete(deviceKeyOf(userId, deviceId));
  return res;
}
