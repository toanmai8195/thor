// Chuẩn hoá số người dùng gõ → số di động VN 10 chữ số (PHONEBOOK.md mục 2.2).

const MOBILE = /^0[35789][0-9]{8}$/;

/** "+84 366 621 555", "0366.621.555", "84366621555" → "0366621555". Không đoán được thì trả nguyên (đã bỏ ký tự phân cách). */
export function normalizePhone(raw: string): string {
  let p = raw.trim().replace(/[\s.\-()]/g, '');
  if (p.startsWith('+84')) p = '0' + p.slice(3);
  else if (p.startsWith('84') && p.length === 11) p = '0' + p.slice(2);
  return p;
}

/** Số di động VN 10 chữ số hợp lệ — chỉ số này được đồng bộ lên server. */
export function isValidMobile(phone: string): boolean {
  return MOBILE.test(phone);
}
