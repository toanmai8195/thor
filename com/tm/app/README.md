# TM App (web)

React 19 + TypeScript + Vite + Dexie (IndexedDB). Giả lập App mobile để thử phonebook-service và friend-service.

| Màn | Việc |
|---|---|
| Danh bạ local | Thêm / sửa / xoá contact (nhiều số / contact), sinh ngẫu nhiên tới 20.000, báo số không hợp lệ, trạng thái đã / chưa đồng bộ |
| Đồng bộ | Tính digest (100 bucket + root) → `check` → upload bucket đổi theo lô ≤ 2.000 contact; từng bước + thời gian, lưới 100 bucket, lịch sử, xoá thiết bị trên server |
| Danh bạ server | Summary, danh sách phân trang (cursor), lọc theo thiết bị, tra số, so với local; cột Profile / Friend để enrich sau |
| Friend network | Nhiều user: ma trận quan hệ A → B, thao tác theo luật friend-service, bạn chung, chi tiết bạn / lời mời / block, kết bạn hàng loạt |

Thanh trên cùng chọn **user + thiết bị** đang giả lập; danh bạ local tách theo `userId#deviceId` → thử được nhiều máy / nhiều user trong 1 trình duyệt.

## Chạy

**Giống prod** — build tĩnh, Nginx phục vụ + reverse proxy `/api/phonebook` → phonebook-service, `/api/friend` → friend-service (1 origin, không CORS):

```bash
cd com/tm/server/infra
docker compose up -d --build app phonebook-service friend-service   # kéo theo Kafka, HBase, MongoDB
open http://localhost:8088
```

Image: `Dockerfile` (Node build → `nginx:1.27-alpine`), cấu hình `nginx.conf` (SPA fallback, cache `/assets/` 1 năm, `index.html` no-cache, resolve tên service lúc request nên app lên được khi 1 backend chưa chạy).

**Dev** — Vite dev server + hot reload, proxy tới backend đang chạy ở máy:

```bash
cd com/tm/app
pnpm install
pnpm dev                     # http://localhost:5173
# backend khác: PHONEBOOK_URL=http://host:3100 FRIEND_URL=http://host:3000 pnpm dev
```

```bash
pnpm test        # digest khớp test vector của server (server/phonebook-service/.../phonedigest/testdata/vectors.json)
pnpm typecheck
pnpm build       # → dist/
```

## Cấu trúc

```
src/
├── main.tsx  App.tsx  session.tsx   # router, layout, user + thiết bị đang giả lập (localStorage)
├── db/db.ts                         # Dexie: contacts, syncStates (last_root), syncLogs, friendUsers
├── lib/
│   ├── digest.ts                    # bản TS của phonedigest (chuẩn hoá, bucket, root) + digest.test.ts
│   ├── phone.ts  b64.ts  fake.ts
├── api/                             # http.ts (lỗi, gzip body > 8 KB), phonebook.ts, friend.ts
├── sync/                            # syncEngine.ts (check → upload), localDigest.ts
├── pages/                           # 4 màn hình
└── components/SessionBar.tsx
```

- Lưu local bằng IndexedDB (Dexie), không dùng localStorage cho dữ liệu (localStorage chỉ giữ user / thiết bị đang chọn).
- Digest phải khớp server từng byte: đổi quy tắc chuẩn hoá ở bất kỳ bên nào → tăng version và thêm vector (PHONEBOOK.md mục 2.5).
- Chưa có đăng nhập: gọi API với `userId` trên path như backend hiện tại.
