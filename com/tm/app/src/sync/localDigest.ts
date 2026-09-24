// Danh bạ local của 1 thiết bị → entry → digest (dùng chung cho màn danh bạ và màn đồng bộ).

import { useLiveQuery } from 'dexie-react-hooks';
import { useEffect, useState } from 'react';
import { db, type LocalContact } from '../db/db';
import { toB64Url } from '../lib/b64';
import { canonicalize, computeDigests, type Digests, type Entry } from '../lib/digest';
import { isValidMobile, normalizePhone } from '../lib/phone';

export interface LocalDigest {
  contacts: number;
  /** Tổng số cặp (số, tên) trên máy */
  entries: number;
  /** Số không hợp lệ (không đồng bộ) */
  invalid: number;
  /** Danh sách đã chuẩn hoá — đúng thứ sẽ gửi lên server */
  canon: Entry[];
  digests: Digests;
  rootB64: string;
  ms: number;
}

export function toEntries(contacts: LocalContact[]): { entries: Entry[]; invalid: number } {
  const entries: Entry[] = [];
  let invalid = 0;
  for (const c of contacts) {
    for (const raw of c.phones) {
      const p = normalizePhone(raw);
      if (!isValidMobile(p)) invalid++;
      entries.push({ p, n: c.name });
    }
  }
  return { entries, invalid };
}

export async function computeLocalDigest(contacts: LocalContact[]): Promise<LocalDigest> {
  const t = performance.now();
  const { entries, invalid } = toEntries(contacts);
  const canon = canonicalize(entries);
  const digests = await computeDigests(canon);
  return {
    contacts: contacts.length,
    entries: entries.length,
    invalid,
    canon,
    digests,
    rootB64: toB64Url(digests.root),
    ms: performance.now() - t,
  };
}

/** Digest danh bạ local, tự tính lại khi danh bạ đổi. */
export function useLocalDigest(deviceKey: string): LocalDigest | undefined {
  const contacts = useLiveQuery(() => db.contacts.where('deviceKey').equals(deviceKey).toArray(), [deviceKey]);
  const [digest, setDigest] = useState<LocalDigest>();
  useEffect(() => {
    if (!contacts) return;
    let alive = true;
    computeLocalDigest(contacts).then((d) => alive && setDigest(d));
    return () => {
      alive = false;
    };
  }, [contacts]);
  return digest;
}
