// Sinh danh bạ giả để thử đồng bộ (tên tiếng Việt, đầu số di động đang dùng).

const HO = ['Nguyễn', 'Trần', 'Lê', 'Phạm', 'Hoàng', 'Huỳnh', 'Phan', 'Vũ', 'Võ', 'Đặng', 'Bùi', 'Đỗ', 'Hồ', 'Ngô'];
const DEM = ['Văn', 'Thị', 'Minh', 'Ngọc', 'Thanh', 'Đức', 'Hữu', 'Thu', 'Quốc', 'Gia', ''];
const TEN = ['An', 'Bình', 'Chi', 'Dũng', 'Giang', 'Hà', 'Hải', 'Hùng', 'Lan', 'Linh', 'Nam', 'Phương', 'Quân', 'Tuấn', 'Trang', 'Yến', 'Khoa', 'Vy'];
const XUNG = ['Anh', 'Chị', 'Em', 'Cô', 'Chú', 'Bác'];
const TAG = ['Công ty', 'Grab', 'cũ', 'HN', 'SG', 'trường', 'Shopee', 'nhà'];
const PREFIX = ['032', '033', '034', '035', '036', '037', '038', '039', '056', '058', '070', '076', '077', '078', '079',
  '081', '082', '083', '084', '085', '086', '088', '089', '090', '091', '093', '094', '096', '097', '098'];

const pick = <T,>(a: T[]) => a[Math.floor(Math.random() * a.length)];

export function fakeName(): string {
  if (Math.random() < 0.3) return `${pick(XUNG)} ${pick(TEN)} ${pick(TAG)}`;
  return [pick(HO), pick(DEM), pick(TEN)].filter(Boolean).join(' ');
}

export function fakePhone(): string {
  return pick(PREFIX) + String(Math.floor(Math.random() * 1e7)).padStart(7, '0');
}

/** 10% contact có 2 số */
export function fakeContact(): { name: string; phones: string[] } {
  return { name: fakeName(), phones: Math.random() < 0.1 ? [fakePhone(), fakePhone()] : [fakePhone()] };
}
