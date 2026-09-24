import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { toHex } from './b64';
import { BUCKETS, canonicalText, canonicalize, computeDigests, normalizeName, truncated, type Entry } from './digest';
import { normalizePhone } from './phone';

// Dùng chung test vector với server: app và phonebook-service phải ra cùng digest.
const vectorsPath = fileURLToPath(
  new URL('../../../server/phonebook-service/internal/utils/phonedigest/testdata/vectors.json', import.meta.url),
);
interface Vector {
  name: string;
  input: { p: string; n: string }[];
  canonical: string;
  buckets: Record<string, string>;
  root: string;
}
const vectors: Vector[] = JSON.parse(readFileSync(vectorsPath, 'utf8'));
const EMPTY = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';

describe('test vector dùng chung với server', () => {
  it('có vector', () => expect(vectors.length).toBeGreaterThan(10));
  for (const v of vectors) {
    it(v.name, async () => {
      const canon = canonicalize(v.input as Entry[]);
      expect(canonicalText(canon)).toBe(v.canonical);
      const d = await computeDigests(canon);
      for (let k = 0; k < BUCKETS; k++) {
        expect(toHex(d.buckets[k]), `bucket ${k}`).toBe(v.buckets[String(k).padStart(2, '0')] ?? EMPTY);
      }
      expect(toHex(d.root)).toBe(v.root);
      expect(truncated(d).length).toBe(400);
    });
  }
});

describe('normalizeName', () => {
  it('idempotent', () => {
    for (const s of ['  Anh\t\tTuấn ', 'Mẹ', 'a​b', '﻿X', 'a\u0085b']) {
      expect(normalizeName(normalizeName(s))).toBe(normalizeName(s));
    }
  });
  it('U+FEFF bị bỏ (Cf), U+0085 thành dấu cách (Cc)', () => {
    expect(normalizeName('a﻿b')).toBe('ab');
    expect(normalizeName('a\u0085b')).toBe('a b');
  });
});

describe('normalizePhone', () => {
  it.each([
    ['+84 366 621 555', '0366621555'],
    ['0366.621.555', '0366621555'],
    ['84366621555', '0366621555'],
    ['(036) 662-1555', '0366621555'],
    ['19001234', '19001234'],
  ])('%s → %s', (raw, want) => expect(normalizePhone(raw)).toBe(want));
});
