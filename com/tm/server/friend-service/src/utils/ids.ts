// Snowflake: 41 bit ms (từ EPOCH) | 10 bit worker | 12 bit sequence.
// Trả về chuỗi số padding 19 ký tự → so sánh chuỗi = so sánh số (README 2.3).

const EPOCH = 1735689600000n; // 2025-01-01T00:00:00Z
const WORKER_BITS = 10n;
const SEQ_BITS = 12n;
const MAX_SEQ = (1n << SEQ_BITS) - 1n;
export const EVENT_ID_LENGTH = 19;

export type IdGenerator = () => string;

export function createIdGenerator(workerId: number, now: () => bigint = () => BigInt(Date.now())): IdGenerator {
  if (!Number.isInteger(workerId) || workerId < 0 || workerId >= 1 << Number(WORKER_BITS)) {
    throw new Error(`WORKER_ID phải trong khoảng 0–1023, nhận được ${workerId}`);
  }
  const worker = BigInt(workerId);
  let lastMs = -1n;
  let seq = 0n;

  return function nextId() {
    let ms = now();
    // Đồng hồ lùi: giữ mốc cũ để id vẫn tăng dần
    if (ms < lastMs) ms = lastMs;
    if (ms === lastMs) {
      seq = (seq + 1n) & MAX_SEQ;
      if (seq === 0n) ms = lastMs + 1n; // hết sequence trong 1 ms → mượn ms kế tiếp
    } else {
      seq = 0n;
    }
    lastMs = ms;
    const id = ((ms - EPOCH) << (WORKER_BITS + SEQ_BITS)) | (worker << SEQ_BITS) | seq;
    return id.toString().padStart(EVENT_ID_LENGTH, '0');
  };
}
