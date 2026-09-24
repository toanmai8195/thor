// Digest danh bạ — bản TypeScript của phonebook-service/internal/utils/phonedigest (PHONEBOOK.md mục 2.5).
// Phải ra đúng kết quả với testdata/vectors.json của server (digest.test.ts).
//
//   entry (số, tên) ─► canonicalize ─► 100 bucket theo 2 số cuối
//     bd[k] = SHA-256("<số>\t<tên>\n"…)     root = SHA-256(bd[00] ‖ … ‖ bd[99])

import { isValidMobile } from './phone';

export const DIGEST_VERSION = 1;
export const BUCKETS = 100;
export const TRUNC_LEN = 4;
export const MAX_NAME_RUNES = 100;

/** 1 cặp (số, tên) — 1 contact có 3 số thì là 3 Entry cùng tên. Tên field khớp JSON server nhận. */
export interface Entry {
  p: string;
  n: string;
}

const SPACE_OR_CONTROL = /[\p{White_Space}\p{Cc}]/u; // = unicode.IsSpace || unicode.IsControl của Go
const FORMAT = /\p{Cf}/u; //                               ký tự vô hình (zero-width, U+FEFF…)
const utf8 = new TextEncoder();

/** NFC → khoảng trắng / điều khiển thành 1 dấu cách → bỏ ký tự vô hình → gộp dấu cách → trim → cắt 100 code point. */
export function normalizeName(s: string): string {
  let out = '';
  let space = false;
  for (const ch of s.normalize('NFC')) {
    if (SPACE_OR_CONTROL.test(ch)) {
      space = true;
      continue;
    }
    if (FORMAT.test(ch)) continue;
    if (space && out.length > 0) out += ' ';
    space = false;
    out += ch;
  }
  const runes = [...out];
  if (runes.length > MAX_NAME_RUNES) out = runes.slice(0, MAX_NAME_RUNES).join('').replace(/ +$/, '');
  return out;
}

/** So sánh theo byte UTF-8 (JS so theo UTF-16 — khác với ký tự ngoài BMP). */
export function compareUtf8(a: string, b: string): number {
  const x = utf8.encode(a);
  const y = utf8.encode(b);
  const n = Math.min(x.length, y.length);
  for (let i = 0; i < n; i++) if (x[i] !== y[i]) return x[i] - y[i];
  return x.length - y.length;
}

/** Bỏ số không hợp lệ, chuẩn hoá tên, 1 số nhiều tên → tên nhỏ nhất theo byte UTF-8, sort theo số. */
export function canonicalize(entries: Entry[]): Entry[] {
  const best = new Map<string, string>();
  for (const e of entries) {
    if (!isValidMobile(e.p)) continue;
    const name = normalizeName(e.n);
    const cur = best.get(e.p);
    if (cur === undefined || compareUtf8(name, cur) < 0) best.set(e.p, name);
  }
  return [...best.entries()].map(([p, n]) => ({ p, n })).sort((a, b) => (a.p < b.p ? -1 : a.p > b.p ? 1 : 0));
}

/** Bucket = 2 chữ số cuối ("0366621555" → 55). */
export function bucketOf(phone: string): number {
  return Number(phone.slice(-2));
}

export function splitBuckets(canon: Entry[]): Entry[][] {
  const out: Entry[][] = Array.from({ length: BUCKETS }, () => []);
  for (const e of canon) out[bucketOf(e.p)].push(e);
  return out;
}

export function canonicalText(canon: Entry[]): string {
  return canon.map((e) => `${e.p}\t${e.n}\n`).join('');
}

async function sha256(data: Uint8Array): Promise<Uint8Array> {
  return new Uint8Array(await crypto.subtle.digest('SHA-256', data as BufferSource));
}

export interface Digests {
  /** 100 digest bucket, mỗi cái 32 byte */
  buckets: Uint8Array[];
  root: Uint8Array;
}

export async function computeDigests(canon: Entry[]): Promise<Digests> {
  const buckets = await Promise.all(splitBuckets(canon).map((es) => sha256(utf8.encode(canonicalText(es)))));
  const all = new Uint8Array(BUCKETS * 32);
  buckets.forEach((d, k) => all.set(d, k * 32));
  return { buckets, root: await sha256(all) };
}

/** 4 byte đầu của mỗi bucket nối lại (400 byte) — trường `buckets` của `check`. */
export function truncated(d: Digests): Uint8Array {
  const out = new Uint8Array(BUCKETS * TRUNC_LEN);
  d.buckets.forEach((b, k) => out.set(b.subarray(0, TRUNC_LEN), k * TRUNC_LEN));
  return out;
}
