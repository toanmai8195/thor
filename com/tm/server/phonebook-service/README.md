# phonebook-service

Nhận danh bạ từ App theo **bucket** (2 chữ số cuối của số), lưu HBase, chỉ gửi số thêm / bớt lên Kafka `phonebook_service_events`. Thiết kế: [`PHONEBOOK.md`](../PHONEBOOK.md) mục 2.

```
App ──HTTP──► router ─► handler ─► controller ─► dao ─► HBase `phonebook`
                                                   └──► Kafka phonebook_service_events ──► event-gateway ──► StarRocks
```

## 1. Cấu trúc code

Mỗi layer 1 package Bazel; `visibility` trong `BUILD.bazel` chặn phụ thuộc sai chiều (vd handler import dao → lỗi `is not visible` lúc build). DI bằng [`go.uber.org/fx`](https://github.com/uber-go/fx): mỗi layer 1 file `*_module.go` đăng ký thành phần của nó.

```
phonebook-service/
├── main.go  app.go  http_server.go   # MainServer: đọc config, dựng DI, lifecycle, HTTP server
├── app_test.go                       # đồ thị DI resolve được (không kết nối thật)
├── e2e/                              # test với HBase + Kafka thật (build tag e2e)
└── internal/
    ├── configs/      # biến môi trường; chỉ MainServer đọc
    ├── router/       # method + path → handler
    ├── handler/      # parse / validate HTTP, gọi controller, DomainError → HTTP status
    ├── controller/   # nghiệp vụ: check, upload bucket, diff, gửi Kafka, list
    ├── dao/          # HBase (PhonebookDao) + Kafka (EventPublisher)
    │   └── book/     # danh bạ 1 thiết bị + layout 2 cell HBase + AES-GCM
    └── utils/        # DomainError, logger, sync_id
        ├── phonecodec/   # 0366621555 ↔ 366621555, FF1 encode / decode
        ├── phonedigest/  # digest bucket + root (bản tham chiếu cho App, testdata/vectors.json)
        └── phoneset/     # tập số ↔ delta varint, diff
```

| Layer | Được dùng bởi | Không được |
|---|---|---|
| MainServer | | chứa logic |
| router | MainServer | parse request, gọi controller |
| handler | router, MainServer | luật nghiệp vụ, đụng HBase / Kafka |
| controller | handler, MainServer | biết về HTTP |
| dao, dao/book | controller, MainServer | luật nghiệp vụ |
| utils/* | mọi layer | phụ thuộc layer khác |
| configs | MainServer | được layer khác đọc |

### DI (fx)

| Module | Đăng ký | Lifecycle |
|---|---|---|
| `utils.Module` | `*slog.Logger`, `*utils.IDGenerator` | |
| `dao.Module` | `dao.PhonebookDao` (HBase), `dao.EventPublisher` (Kafka), `dao.HealthPinger` | start: tạo bảng (local) → kết nối Kafka; stop: ngắt Kafka → đóng HBase |
| `controller.Module` | `*phonecodec.Codec`, `*book.Codec`, `*controller.PhonebookController` | |
| `handler.Module` | `*handler.PhonebookHandler`, `*handler.HealthHandler` | |
| `router.Module` | `http.Handler` | |
| MainServer (`app.go`) | `Settings` của từng layer (từ `configs.Config`), nối `dao.HealthPinger` → `handler.Pinger`, `*httpServer` | start: listen; stop: Shutdown |

- Mỗi layer khai báo `Settings` nó cần; chỉ `app.go` đọc `configs.Config` rồi `fx.Supply` từng `Settings`.
- Thứ tự start = thứ tự constructor được gọi: HBase → Kafka → HTTP; stop ngược lại. `fx.StartTimeout` 90 s để chờ Kafka / HBase vừa khởi động.
- Class nghiệp vụ nhận dependency qua constructor, không import fx → test dựng trực tiếp với `dao.NewMemoryPhonebookDao()` và publisher giả.
- `app_test.go` chạy `fx.ValidateApp`: thiếu / sai kiểu dependency là test fail.

Thêm thành phần: viết struct / hàm như bình thường → thêm vào `fx.Provide` trong `*_module.go` của layer → cần start / stop thì `lc.Append(fx.Hook{...})` trong constructor.

## 2. Cấu hình

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `PORT` | `3100` | |
| `HBASE_ZK` | `localhost:2181` | ZooKeeper quorum của HBase |
| `HBASE_TABLE` | `phonebook` | production tạo sẵn (PHONEBOOK.md mục 2.3) |
| `HBASE_CREATE_TABLE_REGIONS` | `0` | > 0: tự tạo bảng khi start (chỉ local / test) |
| `KAFKA_BROKERS` | `localhost:29092` | |
| `KAFKA_TOPIC` | `phonebook_service_events` | |
| `KAFKA_CLIENT_ID` | `phonebook-service` | |
| `WORKER_ID` | `0` | 0–1023, mỗi instance khác nhau (sync_id) |
| `PHONE_KEY_V1` | — **bắt buộc** | hex 32 byte, khoá FF1 cho số |
| `DATA_KEY_V1` | — **bắt buộc** | hex 32 byte, AES-256-GCM cho tên + digest bucket |
| `DIGEST_KEY` | — **bắt buộc** | hex 32 byte, HMAC cho root |
| `LEASE_TTL_SECONDS` | `60` | khoá theo user khi sync |
| `MAX_CONTACTS` | `20000` | tối đa / thiết bị |
| `MAX_BATCH_CONTACTS` | `5000` | tối đa / request upload |

Mất / đổi `PHONE_KEY_V1` = mọi `phone_enc` đã lưu (HBase, Kafka, StarRocks) không decode được nữa → giữ trong secret manager, có backup.

## 3. Build, test, chạy

```bash
bazel build //phonebook-service/...
bazel test //phonebook-service/...          # unit test mọi layer + đồ thị DI

# image (chọn đúng kiến trúc máy chạy container)
bazel run --config=linux-arm64 //phonebook-service:phonebook_service_docker   # com.tm.go.phonebook_service:v1.0.0

# local: Kafka + HBase standalone + service (khoá demo trong docker-compose.yml)
cd infra && docker compose up -d phonebook-service
curl localhost:3100/healthz

# e2e: HBase + Kafka thật, mô phỏng App (check → upload bucket → list → xoá thiết bị) và đọc lại event trên Kafka
go test -tags e2e -count=1 -v ./phonebook-service/e2e/
```

- HBase image `harisekhon/hbase:2.1` chỉ có amd64: trên Mac Apple Silicon chạy giả lập, khởi động 1–2 phút. UI: http://localhost:16010.
- HBase báo `Master is initializing` lâu sau khi tạo lại container trên volume cũ: `docker compose rm -sf hbase && docker volume rm infra_hbase-data` rồi `up` lại.
- `gohbase` pin commit `20250203` (bản cuối hỗ trợ Go 1.22 mà Bazel đang dùng); `MODULE.bazel` có `gazelle:proto disable` cho module này để dùng file `.pb.go` sẵn có.

## 4. Thử bằng curl

Digest phải tính theo PHONEBOOK.md mục 2.5 (App làm việc này); để thử nhanh dùng e2e test ở trên. Các API đọc gọi thẳng được:

```bash
curl 'localhost:3100/v1/users/1001/phonebook/summary'
curl 'localhost:3100/v1/users/1001/phonebook/contacts?limit=5'
curl 'localhost:3100/v1/users/1001/phonebook/contacts/0366621555'
curl -X DELETE 'localhost:3100/v1/users/1001/devices/ios-A/phonebook'
curl localhost:3100/debug/vars | grep phonebook_
```

## 5. Chưa làm

- Xác thực: API nhận `userId` trên path, chưa kiểm tra người gọi (JWT / gateway).
- Phần contact của event-gateway (`phonebook_service_events` → `contact_events`), StarRocks (PHONEBOOK.md mục 3, 5).
- CLI decode / encode `phone_enc` cho người có quyền (dùng lại `internal/utils/phonecodec`).
- Job gửi bù cho user không login lại (PHONEBOOK.md mục 9).
