# Phonebook (danh bạ) trên StarRocks

App đồng bộ danh bạ lên server mỗi lần user login; StarRocks trả lời:

- User A có bao nhiêu contact? Là những số nào?
- Có bao nhiêu user có trên 500 contact? Phân bố số contact theo user?
- Contact chung của 2 user? (tuỳ chọn) Những ai đang lưu số X?
- Số contact của A theo ngày (180 ngày), các lần sync gần đây (30 ngày)?

Đã chốt:

| # | Quyết định |
|---|---|
| 1 | Thiết bị chỉ quản lý ở DB của service. StarRocks thấy danh bạ của **user** = hợp các thiết bị. |
| 2 | Số điện thoại được **mã hoá 2 chiều** (FF1, mục 4) ngay khi vào service; DB service, Kafka, StarRocks chỉ có `phone_enc`. |
| 3 | Không làm outbox: mỗi lần login app sync lại toàn bộ danh bạ, service tự gửi bù phần StarRocks có thể còn thiếu (mục 2.4). |
| 4 | Không map số → `user_id`. Tên contact lưu ở HBase (mã hoá), **không** đưa vào StarRocks; đổi tên phải cập nhật lại (mục 2.5). |
| 5 | Diff ở service: **Go + HBase** (công ty có sẵn cụm). Row key `userId#deviceId`: 1 row / thiết bị + 1 row trạng thái / user; danh bạ nén trong 1 cell, không lưu 1 row / contact (mục 2.3). |

Quy mô: trung bình **1.000 contact / user**, tối đa **20.000**, **50 lần sync / s**. Tính tải ở mục 7.

Nguyên tắc: mỗi cặp (`user_id`, `phone_enc`) có đúng 1 trạng thái (`active` 1 / 0), là event mới nhất theo `event_time`.

---

## 1. Kiến trúc

```
App ── check digest / upload bucket đổi ──► phonebook-service (Go)
                                                │ 1. chuẩn hoá, ENCODE FF1, thay bucket của thiết bị
                                                │ 2. hợp các thiết bị, diff với bản đã gửi DW (HBase)
                                                ▼
                                   HBase phonebook: row userId#deviceId (danh bạ) + row userId# (published, pending)
                                                │ 3. chỉ gửi số thêm / bớt
                                                ▼
                        Kafka phonebook_service_events   (1 message = mảng ≤ 1.000 event cùng user, phone_enc)
                                                │
                                                ▼
                        event-gateway (Go, EVENT_KIND=contact): kiểm tra contract ──(sai)──► contact_events_dlq
                                                │
                                                ▼
                        Kafka contact_events
                          ├─ Routine Load ──► ODS ods_contact_event   (30 ngày)
                          └─ Routine Load ──► DWD dwd_contact         (trạng thái hiện tại)
                                                └─ Task 10 phút ──► DWS dws_contact_summary
                                                     └─ Task 00:05 ──► DWS dws_contact_daily (180 ngày)
```

Số gốc chỉ tồn tại trong RAM của phonebook-service lúc xử lý request. Khoá FF1 chỉ ở phonebook-service (package `phonecodec`).

> Đã triển khai: `phonebook-service/` (mục 2, hướng dẫn chạy ở [`phonebook-service/README.md`](phonebook-service/README.md)). Chưa triển khai: phần contact của event-gateway (mục 3), StarRocks (mục 5), App.

---

## 2. phonebook-service (Go + HBase)

### 2.1 Chọn công nghệ

Bài toán diff thực chất là: mỗi request **đọc 1 tập số của user → so với tập mới → ghi lại nếu khác**, 50 lần / s, dữ liệu mỗi user nhỏ (~3 KB trung bình, ~55 KB tối đa). Đây là tải key-value nhẹ; cái quyết định là **cách lưu** (1 dòng / thiết bị thay vì 1 dòng / contact) chứ không phải DB mạnh cỡ nào.

**Ngôn ngữ: Go.** Đo trên máy dev (Apple Silicon), 1 core:

| Việc | 20.000 số | Ghi chú |
|---|---|---|
| Parse JSON body (292 KB) | 2,5 ms | `encoding/json` |
| FF1 encode | 38 ms (1,9 µs / số) | `capitalone/fpe/ff1`; tải trung bình 50.000 số / s ≈ 0,1 core |
| Diff 2 tập đã sort | 16 µs | merge 1 lượt; roaring64 chậm hơn ~35 lần, file cũng lớn hơn |

| | Go (chọn) | Node.js | Java | Python |
|---|---|---|---|---|
| CPU nặng (FF1, parse 20.000 số) | goroutine, không chặn request khác | chặn event loop (38 ms+ / request), phải `worker_threads` | tốt | GIL; FF1 thuần Python chậm hơn hàng chục lần |
| Thư viện FF1 | có (`capitalone/fpe`) | phải tự viết / ít dùng | có (Bouncy Castle) | có, chậm |
| Dùng chung code | cùng stack Bazel `rules_go`, sarama với event-gateway | | thêm toolchain JVM vào repo | |

**DB: HBase** (cụm có sẵn).

| | HBase (chọn) | PostgreSQL | MongoDB |
|---|---|---|---|
| Ghi an toàn | `CheckAndPut` trên row trạng thái của user + thứ tự ghi an toàn khi crash (mục 2.4), không cần transaction nhiều row | `SELECT ... FOR UPDATE` + transaction | transaction |
| Khoá theo user | CAS trên cột `ver` | row lock | CAS / transaction |
| Tải (50 sync / s, ~0,8 TB) | rất nhẹ với cụm có sẵn | 1 primary đủ | đủ |
| Vận hành | **đã có sẵn** | dựng mới | dựng mới |
| Khi cần lớn hơn | thêm region / RegionServer | phải tự tách shard | sharding sẵn |

PostgreSQL chỉ hơn khi phải dựng DB mới. Có sẵn cụm thì HBase gọn hơn: không cần transaction nhiều row, không phải lo tách shard.

**Client Go: `github.com/tsuna/gohbase`** (có `Get`, `Put`, `CheckAndPut`). Cần kiểm tra trước với team HBase:

- Cụm có bật **Kerberos / SASL** không? Theo mình biết gohbase chưa hỗ trợ Kerberos. Nếu cụm bắt buộc Kerberos: viết service bằng **Java** (client chính chủ, có `checkAndMutate` + `RowMutations`), hoặc đi qua HBase Thrift / REST gateway nếu công ty đã có. Thiết kế dữ liệu và luồng bên dưới giữ nguyên.
- Phiên bản HBase (gohbase chạy với 1.x / 2.x).

Nếu hạ tầng bắt buộc app gọi qua service Node.js (auth, API gateway): Node chỉ xác thực rồi proxy nguyên body sang phonebook-service; không diff trong Node.

### 2.2 API

Base path `/v1`. Digest và cursor gửi dạng **base64url không padding**. Body có thể gửi `Content-Encoding: gzip`.

| Method | Path | Việc |
|---|---|---|
| `POST` | `/users/{userId}/devices/{deviceId}/phonebook/check` | mỗi lần login: danh bạ có đổi không, bucket nào đổi |
| `PUT` | `/users/{userId}/devices/{deviceId}/phonebook/buckets` | thay nội dung các bucket đổi (1 request ≤ 5.000 contact) |
| `DELETE` | `/users/{userId}/devices/{deviceId}/phonebook` | logout / thu hồi quyền danh bạ trên thiết bị |
| `GET` | `/users/{userId}/phonebook/contacts?limit=&cursor=&device_id=` | danh sách contact (hợp các thiết bị hoặc 1 thiết bị) |
| `GET` | `/users/{userId}/phonebook/contacts/{phone}` | user có lưu số này không |
| `GET` | `/users/{userId}/phonebook/summary` | số contact, còn pending không, danh sách thiết bị |
| `GET` | `/healthz` | ping HBase |
| `GET` | `/debug/vars` | metric `phonebook_digest_mismatch`, `phonebook_publish_failed`, `phonebook_events_sent` |

**check**

```json
// request: gửi "buckets" khi root khác last_root app lưu (hoặc server trả NEED_BUCKETS)
{"v": 1, "root": "<32 byte>", "buckets": "<400 byte = 100 × 4 byte đầu digest bucket>"}

// response
{"status": "UNCHANGED"}                       // không đổi (còn pending thì server tự gửi bù)
{"status": "NEED_BUCKETS"}                    // root lệch, gửi lại kèm buckets
{"status": "UPLOAD", "changed": [55, 78]}     // upload các bucket này
```

**upload**

```json
// request: key = số bucket; "d" (tuỳ chọn) = 4 byte đầu digest bucket app tính → server so để đếm lệch
{"v": 1, "buckets": {
  "55": {"d": "wQwTCQ", "contacts": [{"p": "0366621555", "n": "Mẹ"}]},
  "78": {"contacts": []}                       // bucket rỗng = xoá hết contact của bucket
}}

// response: root do server tính sau khi ghi (app so với root của mình rồi lưu last_root)
{"root": "...", "sync_id": "0891016339823525888", "added": 1, "deleted": 0,
 "contact_cnt": 5001, "device_contact_cnt": 5000, "rejected": 0, "published": true}
```

- Số: di động VN 10 chữ số `^0[35789][0-9]{8}$`; số khác bị bỏ, đếm `rejected`. Số không thuộc bucket (2 số cuối khác) → `400 BUCKET_MISMATCH`.
- `deviceId`: `^[A-Za-z0-9_-]{1,64}$`. `userId`: 1..2^53−1.
- `published = false`: danh bạ đã lưu nhưng Kafka lỗi → còn pending, lần sau gửi bù (mục 2.4).

**list**: `{"contacts": [{"phone", "name"}], "total", "next_cursor"}`; `limit` mặc định 500, tối đa 2.000; truyền `next_cursor` của trang trước vào `cursor`. Thứ tự theo (bucket, `phone_enc`) — ổn định khi danh bạ đổi giữa 2 trang, chỉ FF1 decode số của trang. Cùng số ở 2 thiết bị khác tên → tên của thiết bị sync gần nhất.

Lỗi: `{"error": "<CODE>", "message": "..."}` — 400 (`INVALID_*`, `BUCKET_MISMATCH`, `UNSUPPORTED_VERSION`), 404 `NOT_FOUND`, 409 `SYNC_IN_PROGRESS`, 413 (`BATCH_TOO_LARGE`, `PHONEBOOK_TOO_LARGE`, `BODY_TOO_LARGE`), 500.

### 2.3 HBase schema

```
create 'phonebook',
       {NAME => 'm', VERSIONS => 1, BLOOMFILTER => 'ROW', IN_MEMORY => 'true'},   # meta nhỏ
       {NAME => 'b', VERSIONS => 1, BLOOMFILTER => 'ROW', COMPRESSION => 'NONE'},  # danh bạ (đã mã hoá)
       {NUMREGIONS => 64, SPLITALGO => 'HexStringSplit'}
```

**Row key** = `salt + userId + "#" + deviceId`, `salt` = 4 ký tự hex đầu của `md5(userId)`:

| Row | Ví dụ (user 1001) | Chứa |
|---|---|---|
| Trạng thái user | `b8c31001#` | published, pending, khoá |
| Thiết bị | `b8c31001#ios-8F2A` | danh bạ của thiết bị |

Mọi row của 1 user nằm liền nhau → 1 Scan theo prefix đọc hết. `salt` rải user đều trên region; `#` ngăn `1001` khớp nhầm `10011`.

**Row thiết bị**

| Cột | Giá trị |
|---|---|
| `b:ph` | `[1 byte format][100 × uvarint số contact / bucket][bucket 00: phone_enc sort, delta varint]…[bucket 99]` — ~3 byte / số |
| `b:nm` | `[1 byte version khoá][12 byte nonce][AES-256-GCM(zstd(tên nối "\n"))]`, AAD = `"nm|" + rowKey + SHA-256(b:ph)` |
| `b:bd` | AES-256-GCM(100 digest bucket × 32 byte), AAD = `"bd|" + rowKey` |
| `m:root` | `HMAC-SHA256(DIGEST_KEY, root)` |
| `m:cnt`, `m:ts`, `m:dv` | số contact, lần sync (ms), version thuật toán digest |

- Số và tên **khớp nhau theo vị trí**: cùng thứ tự (bucket 00→99, trong bucket theo `phone_enc`), ghi cùng 1 lệnh Put. AAD buộc `b:nm` với đúng `b:ph` và đúng row: ghép sai / copy sang row khác → giải mã báo lỗi thay vì gán nhầm tên. Khi đọc, số tên ≠ số `phone_enc` → lỗi dữ liệu hỏng.
- Sắp theo bucket trước để thay 1 bucket mà không phải FF1 decode cả danh bạ.
- 1 row thiết bị ~10 KB với 1.000 contact (3 KB số + ~7 KB tên). Không lưu 1 cell / bucket: 200 cell × ~45 byte phụ phí HBase + 100 × 29 byte phụ phí AES-GCM làm row gấp ~2 lần.
- Không dùng roaring bitmap cho tập số: đo với 500–20.000 số, roaring trên số gốc tốn 4,1–9,8 byte / số (varint + FF1: 2,7–3,4), trên số đã FF1 còn tệ hơn (6,5–9,9). Danh bạ quá thưa so với không gian số.
- `b:bd` mã hoá và `m:root` là HMAC: bucket / danh bạ ít số thì SHA-256 dò ngược được bằng vét cạn.

**Row trạng thái user**

| Cột | Giá trị |
|---|---|
| `b:pub` | tập `phone_enc` (delta varint) **đã gửi Kafka thành công** = cái DW đang có |
| `b:pend` | tập đang gửi; còn lại sau lỗi = "không chắc DW đã nhận" |
| `m:psync` | `sync_id` của pending; 0 = không có |
| `m:lease` | khoá theo user (ms, hết hạn khi < now) |
| `m:ver` | tăng mỗi lần ghi row; cột so sánh của **mọi** `CheckAndPut` |
| `m:cnt`, `m:let` | số contact của `pub`, `event_time` cuối |

### 2.4 Luồng xử lý

```
check (mỗi lần login)
  Get row thiết bị (m + b:bd) + row user (m)
    HMAC(root) == m:root                 → UNCHANGED (còn pending → tự gửi bù, không cần app upload)
    lệch, không kèm buckets              → NEED_BUCKETS
    lệch, kèm buckets                    → UPLOAD {changed = bucket có 4 byte đầu digest khác}
    lệch mà không bucket nào khác (trùng 4 byte) → UPLOAD mọi bucket

upload / xoá thiết bị / gửi bù — cùng 1 luồng (controller.reconcile):
  0. chuẩn hoá, kiểm bucket, FF1 encode              (trước khi lấy khoá, phần tốn CPU nhất)
  1. Scan prefix: row user (ver = v, lease) + mọi thiết bị;  lease > now → 409 SYNC_IN_PROGRESS
  2. danh bạ mới của thiết bị = bản đang lưu, thay các bucket vừa upload; > 20.000 → 413
  3. lấy khoá: CheckAndPut(ver == v): lease = now + 60s, ver = v+1      (hụt → làm lại từ 1, tối đa 3 lần)
  4. Put row thiết bị (hoặc Delete khi xoá thiết bị)
  5. U = hợp phone_enc các thiết bị (chỉ đọc b:ph); diff:
       không pending: ADD = U − pub                DEL = pub − U
       có pending:    ADD = U − (pub ∩ pend)       DEL = (pub ∪ pend) − U        (gửi dư)
  6. không có ADD / DEL (vd chỉ đổi tên):  CheckAndPut(ver == v+1): pub = U, bỏ khoá
  7. có: CheckAndPut(ver == v+1): pend = U, psync, let = max(now, let + 1ms)
         gửi Kafka (lô 1.000 event / message, key = user_id, acks=all)
           lỗi → bỏ khoá, giữ pend; trả 200 published = false
         CheckAndPut(ver == v+2): pub = U, bỏ pend, bỏ khoá
```

- **Mọi CheckAndPut so trên `ver`.** Khoá của mình hết hạn và request khác lấy khoá → `ver` đã tăng → ghi muộn của mình bị từ chối; trạng thái còn lại (pending) được lần sau xử lý. (So trên `psync` rồi ghi `ver` cũ vào có thể kéo `ver` lùi, làm CAS của request kia sai.)
- **Không cần ghi nguyên tử 2 row.** Row thiết bị là "danh bạ mong muốn", `pub` là "cái DW đang có": crash ở bất kỳ bước nào thì lần sau tính lại từ các row thiết bị và tự sửa. Crash giữa 3 và 4 chỉ để lại khoá, hết hạn sau 60 s.
- **Vì sao gửi dư khi còn pending**: lần lỗi có thể đã gửi được `ADD V`; nếu chỉ so với `pub` (không có V) thì user xoá V sau đó sẽ không sinh `DELETE V`. Event dư chỉ ghi đè cùng giá trị ở DWD.
- `event_time` giống nhau trong 1 lần sync và luôn tăng giữa các lần sync của user; `sync_id` snowflake 19 chữ số.

### 2.5 Digest danh bạ

Bản tham chiếu: `phonebook-service/internal/utils/phonedigest/`; test vector dùng chung với App: `.../phonedigest/testdata/vectors.json` (12 case, giá trị tính bằng Python `hashlib`, không lấy từ code Go).

| Bước | Quy tắc |
|---|---|
| 1. Entry | Mỗi (số, tên hiển thị) trên máy là 1 entry. |
| 2. Số | Số di động VN 10 chữ số `^0[35789][0-9]{8}$`; không khớp → bỏ. |
| 3. Tên | Unicode **NFC** → khoảng trắng / ký tự điều khiển (tab, xuống dòng, NBSP…) thành dấu cách → bỏ ký tự vô hình (nhóm `Cf`: zero-width, U+FEFF…) → gộp dấu cách → trim → cắt 100 code point. |
| 4. Trùng | 1 số nhiều tên → giữ tên nhỏ nhất theo byte UTF-8. |
| 5. Bucket | bucket = 2 chữ số cuối của số (`0366621555` → 55); trong bucket sort theo số. |
| 6. Digest bucket | `bd[k] = SHA-256(các dòng "<số>\t<tên>\n" của bucket k)`; bucket rỗng = `SHA-256("")`. |
| 7. Root | `root = SHA-256(bd[00] ‖ bd[01] ‖ … ‖ bd[99])` (3.200 byte). |

- App tính mỗi lần login (20.000 contact ~14 ms trên máy dev). Lưu `last_root` (theo userId + deviceId) sau khi upload xong và root server trả = root app tính; chỉ gửi `buckets` khi root khác `last_root`. `last_root` chỉ là gợi ý: mất thì chỉ tốn thêm 1 lần gửi buckets.
- Server **tự tính** digest từ dữ liệu nhận được và lưu digest của mình; digest app gửi (`d`) chỉ để đếm lệch (metric `phonebook_digest_mismatch`). App tính lệch thì trường hợp xấu nhất là upload thừa mỗi lần login, không sai dữ liệu.
- Đổi quy tắc ở bất kỳ bên nào = tăng `v` + thêm vector; lần login đầu sau khi đổi sẽ upload lại mọi bucket.

### 2.6 Cấu trúc code

Giống friend-service: mỗi layer 1 package Bazel, `visibility` chặn phụ thuộc sai chiều; DI bằng `go.uber.org/fx` (mỗi layer 1 `*_module.go`).

```
MainServer (main.go, app.go, http_server.go): đọc config, dựng DI, lifecycle hbase → kafka → http
router ─► handler ─► controller ─► dao ─► HBase / Kafka
                                     └─ dao/book: layout cell + AES-GCM
utils (DomainError, logger, sync_id) + utils/phonecodec (FF1) + utils/phonedigest (digest) + utils/phoneset (varint, diff)
configs: chỉ MainServer đọc
```

Chi tiết từng layer, cấu hình, cách chạy / test: [`phonebook-service/README.md`](phonebook-service/README.md).

## 3. Event contract

**1 Kafka message = 1 mảng JSON ≤ 1.000 event của cùng user, cùng lần sync.** Routine Load tách mảng thành dòng bằng `strip_outer_array`. Gateway và Kafka chỉ xử lý ~50–1.000 message / s.

`phonebook_service_events` và `contact_events` cùng format (gateway chỉ kiểm tra rồi chuyển tiếp):

```json
[
  {"user_id": 1001, "phone_enc": 1735820019, "event_type": "ADD",    "event_time": "2026-09-24 10:10:00.123", "sync_id": "0228440659126648833"},
  {"user_id": 1001, "phone_enc": 1004419283, "event_type": "DELETE", "event_time": "2026-09-24 10:10:00.123", "sync_id": "0228440659126648833"}
]
```

| Field | Quy tắc |
|---|---|
| `user_id` | 1..2^53−1, giống nhau trong cả mảng, = Kafka key |
| `phone_enc` | 10^9 ≤ x < 10^10 (mục 4) |
| `event_type` | `ADD` / `DELETE` |
| `event_time` | `yyyy-MM-dd HH:mm:ss.SSS` UTC, giống nhau trong 1 lần sync |
| `sync_id` | 19 chữ số |

Không cần `event_id`: trong 1 lần sync mỗi số xuất hiện 1 lần, giữa các lần sync `event_time` tăng ngặt.

### Thay đổi ở event-gateway

Chạy thêm 1 deployment cùng image: `EVENT_KIND=contact`, `KAFKA_GROUP_ID=event-gateway-contact`, `KAFKA_INPUT_TOPIC=phonebook_service_events`, output `contact_events`, DLQ `contact_events_dlq` (đổi `KAFKA_FRIEND_TOPIC` thành `KAFKA_OUTPUT_TOPIC`, giữ tên cũ làm alias).

- `internal/event`: `ContactBatch`, `DecodeContactBatch()` kiểm tra từng phần tử, cùng `user_id`, 1–1.000 phần tử. Sai 1 phần tử → cả message vào DLQ.
- `internal/relay`: nhận hàm decode theo `EVENT_KIND` thay vì gọi cứng `event.Decode`.
- Gateway **không cần khoá FF1**. Gửi tuần tự từng message vẫn đủ nhờ gom mảng.

---

## 4. Encode / decode số điện thoại

Yêu cầu: giải mã được, **tất định** (cùng số → cùng mã, để `COUNT`, `JOIN`, diff chạy trên mã), khoá chính DWD ngắn (mục 7.1).

**FF1** (format-preserving encryption, NIST SP 800-38G), Go `github.com/capitalone/fpe/ff1`, package dùng chung `phonecodec/`:

```
encode("0901234567"):
  d = "901234567"                                  bỏ số 0 đầu → 9 chữ số
  c = FF1.Encrypt(key_v, tweak="phone", d)         9 chữ số (có thể bắt đầu bằng 0)
  phone_enc = v × 10^9 + int(c)                    v = version khoá 1..9 → 10^9 ≤ x < 10^10 (BIGINT)

decode(phone_enc):
  v = phone_enc / 10^9; c = phone_enc % 10^9 (pad 9)
  d = FF1.Decrypt(key_v, "phone", c) → "0" + d
```

- `BIGINT` 8 byte (hash / AES + base64 ra chuỗi 32–60 byte).
- Không có khoá thì không đoán được số và không suy ra mã vùng / nhà mạng. Mã tất định nên vẫn lộ việc 2 dòng cùng số (cần cho đếm / join / diff).
- Khoá ở phonebook-service (`internal/utils/phonecodec`). Cần thêm 1 CLI dùng lại package này để người có quyền decode `phone_enc` từ SR / encode số trước khi query SR (chưa làm).
- Đổi khoá = mã hoá lại bảng HBase và DWD. Digit `v` cho phép 2 phiên bản cùng tồn tại lúc chuyển.

---

## 5. StarRocks

> DDL, Routine Load, task, query đầy đủ: `phonebook.sql`.

| Bảng | Khoá / kiểu | Cột | Giữ |
|---|---|---|---|
| `ods_contact_event` | Duplicate `(user_id, phone_enc, event_time)`, partition ngày | `event_type`, `sync_id`, `ingest_time` | **30 ngày** (mục 7.1) |
| `dwd_contact` | **Primary** `(user_id BIGINT, phone_enc BIGINT)`, hash `user_id` 256 bucket | `active TINYINT`, `last_event_time` | dòng `active = 0` xoá sau 30 ngày |
| `dws_contact_summary` | Primary `user_id` | `contact_cnt`, `last_change_time`, `computed_at` | vĩnh viễn |
| `dws_contact_daily` | Duplicate `(dt, user_id)`, partition ngày | `contact_cnt` | 180 ngày |
| `dwd_contact_by_phone` (tuỳ chọn) | Primary `(phone_enc, user_id)`, hash `phone_enc` | như DWD | chỉ tạo khi cần "ai lưu số X", +100% dung lượng DWD |

- Routine Load: `"strip_outer_array" = "true"`, `active = IF(event_type = 'DELETE', 0, 1)`, `merge_condition = last_event_time`.
- Xoá mềm rồi mới dọn: xoá thật ngay lúc nạp thì event `DELETE` đến trễ có thể xoá mất số vừa được thêm lại. Task hằng ngày xoá dòng `active = 0` quá 30 ngày (lâu hơn mọi độ trễ có thể của pipeline).
- Task summary 10 phút giống friend (chỉ user có event mới trong ODS).

---

## 6. Truy vấn

```sql
-- P1. A có bao nhiêu contact (DWD, chính xác tức thì, 1 bucket)
SELECT COUNT(*) FROM dwd_contact WHERE user_id = ? AND active = 1;

-- P2. A có những số nào (decode bằng phonecodec)
SELECT phone_enc FROM dwd_contact WHERE user_id = ? AND active = 1 ORDER BY phone_enc;

-- P3. Số user có trên 500 contact (DWS, trễ ≤ 10 phút)
SELECT COUNT(*) FROM dws_contact_summary WHERE contact_cnt > 500;

-- P4. Phân bố
SELECT CASE WHEN contact_cnt <= 100 THEN '0-100' WHEN contact_cnt <= 500 THEN '101-500'
            WHEN contact_cnt <= 1000 THEN '501-1000' WHEN contact_cnt <= 5000 THEN '1001-5000'
            ELSE '>5000' END AS bucket, COUNT(*) AS users
FROM dws_contact_summary GROUP BY bucket ORDER BY MIN(contact_cnt);

-- P5. Contact chung của 2 user
SELECT a.phone_enc FROM dwd_contact a JOIN dwd_contact b ON a.phone_enc = b.phone_enc
WHERE a.user_id = ? AND b.user_id = ? AND a.active = 1 AND b.active = 1;
```

Còn lại (ai lưu số X, các lần sync 30 ngày, số contact theo ngày) ở `phonebook.sql` phần 5.

---

## 7. Tính tải

Giả định thêm: **50M user** (cần xác nhận, mục 9 #2), **10% lần sync là lần đầu** của user / thiết bị (gửi ~1.000 ADD), 90% còn lại trung bình **5 thay đổi**.

| Đại lượng | Thường | Xấu nhất (mọi lần sync là lần đầu) |
|---|---|---|
| Sync / ngày | 4,3M | 4,3M |
| Event / s | 50 × (0,1 × 1.000 + 0,9 × 5) ≈ **5,2k** (~450M / ngày) | 50 × 1.000 = **50k** (~4,3 tỉ / ngày) |
| Kafka message / s (≤ 1.000 event / message) | ~50 | ~50–1.000 |
| 1 sync 20.000 số | 20 message ~ 1,5 MB (trước nén) | |

Xấu nhất xảy ra khi **ra mắt tính năng**: 50M user × 1.000 = 50 tỉ event; ở 50 sync / s thì mất ~11–12 ngày để mọi user sync lần đầu. Vì vậy tải mở màn chỉ ≈ tải "xấu nhất" ở trên, không phải đột biến lớn hơn.

### 7.1 StarRocks: chịu được, với 4 điều kiện

| Bảng | Dòng | Nén, 1 bản sao |
|---|---|---|
| `dwd_contact` | 50M × 1.000 = **50 tỉ** (+ dòng đã xoá ≤ 30 ngày) | dữ liệu ~0,4–0,6 TB + **persistent index ~1–1,5 TB** |
| `ods_contact_event` | 450M / ngày × 30 ngày ≈ 13,5 tỉ | ~5 GB / ngày → ~150 GB (xấu nhất ~50 GB / ngày → 1,5 TB) |
| `dws_contact_summary` | 50M | ~0,5 GB |
| `dws_contact_daily` | 50M / ngày × 180 | ~0,3 GB / ngày → ~55 GB |

Tổng ~2–2,5 TB / bản sao → **~6–8 TB với 3 bản sao** (xấu nhất ~11 TB). Điểm khởi đầu để thử tải: 6 BE × (16–32 core, 128 GB RAM, 2–4 TB NVMe).

- **Nạp**: 5k–50k dòng / s vào bảng Primary Key vẫn trong khả năng của Routine Load với 6 BE, NVMe (mỗi upsert tra index trên đĩa). Đặt `contact_events` 24 partition, `desired_concurrent_number` = 6–12.
- **Truy vấn**: P1 / P2 đọc 1 bucket, tối đa 20.000 dòng → vài ms. P3 / P4 quét 50M dòng summary → < 1 s. Task summary 10 phút: ≤ 30k user sync (mà phần lớn không đổi nên không có event) → ≤ 30M dòng DWD mỗi lần.

4 điều kiện:

1. **Khoá chính DWD là 2 BIGINT (16 byte)**. Index bảng Primary Key có kích thước tỉ lệ với số dòng × độ dài khoá: với 50 tỉ dòng, khoá `phone` dạng VARCHAR mã hoá (~40–60 byte) làm index lên ~3–4 TB / bản sao. Đây là lý do chọn FF1 ra BIGINT.
2. **`enable_persistent_index = true` + NVMe**. Index không persistent phải nằm hết trong RAM (hàng TB), không khả thi.
3. **Gom event thành mảng** trên Kafka (mục 3). Nếu mỗi event là 1 message, gateway gửi tuần tự (chờ ack) chỉ đạt vài trăm đến 1–2k message / s → thiếu cho mức 5k–50k.
4. **ODS giữ 30 ngày** thay vì 180 như friend; DWD dọn dòng đã xoá sau 30 ngày. Xu hướng dài hạn lấy từ `dws_contact_daily` (180 ngày, rất nhẹ).

Không nên: MV tính lại từ DWD; câu P3 chạy thẳng trên DWD (`GROUP BY user_id HAVING COUNT(*) > 500` quét 50 tỉ dòng); tạo `dwd_contact_by_phone` khi chưa thật cần.

### 7.2 phonebook-service + HBase: chịu được dễ

Dữ liệu đi qua mạng cho 1 lần sync (đo trên số VN ngẫu nhiên):

| | 1.000 số | 20.000 số |
|---|---|---|
| App → service, JSON | 14 KB (gzip 5 KB) | 292 KB (gzip 99 KB) |
| HBase → service, 1 cell / contact | ~41 KB, 1.000 KeyValue | ~820 KB, 20.000 KeyValue |
| HBase → service, **1 cell / thiết bị (chọn)** | ~3 KB, vài KeyValue (Scan chỉ lấy `m`, `b:phones`, `b:published`, `b:pending`, không lấy `b:names`) | ~55 KB |
| Lần login không đổi (`check`) | ~100 byte lên, Get family `m` ~200 byte | như bên trái |

Với 50 sync / s, giả sử 10% lần login có đổi danh bạ:

| | Không có `check` | **Có `check` (chọn)** |
|---|---|---|
| App upload | 50 × 5 KB = 250 KB / s | 5 × 5 KB = 25 KB / s |
| HBase đọc | 50 Get × ~14 KB (published + thiết bị) = 700 KB / s | 50 Get × 200 B + 5 Get × 14 KB ≈ 80 KB / s |
| HBase ghi | ~10 CheckAndPut / s | ~10 CheckAndPut / s |
| FF1 + parse ở service | 50.000 số / s | 5.000 số / s |

Cả 2 cột đều nhẹ với 1 cụm HBase. `check` chủ yếu tiết kiệm **pin và mạng di động của user** (không upload 14–292 KB mỗi lần login) và bớt 90% việc của service.

So với lưu 1 row / contact (50 tỉ row, đọc 1.000–20.000 KeyValue mỗi lần, ghi lần đầu 1.000–20.000 Put không nguyên tử): 1 cell / thiết bị ít hơn ~13 lần dữ liệu qua mạng và ~1.000 lần số KeyValue. Dung lượng: 50M user × (3 KB `published` + 1,3 thiết bị × (3 KB số + ~7 KB tên)) ≈ **~0,8 TB** (HDFS ×3 ≈ 2,4 TB); không lưu tên thì ~0,35 TB.

- **CPU service**: request 20.000 số ~45 ms (FF1 38 ms). 2–3 pod × 2 core là dư, chủ yếu để HA.
- Compaction: mỗi lần ghi tạo cell mới 6–120 KB, `VERSIONS => 1` nên major compaction bỏ bản cũ; ở ~10 ghi / s không đáng kể.

## 8. Kịch bản kiểm thử

> Đã tự động hoá: `internal/controller/phonebook_controller_test.go` (T1–T6, Kafka lỗi, đổi tên, khoá, CAS hụt, giới hạn, list), `internal/router/router_test.go` (HTTP), `e2e/e2e_test.go` (HBase + Kafka thật).

| T | Request | Event | Số của user trong DW | `contact_cnt` |
|---|---|---|---|---|
| 1 | D1 `PUT` [X, Y, Z] | ADD X, Y, Z | X Y Z | 3 |
| 2 | D2 `PUT` [Z, W] | ADD W (Z đã có) | X Y Z W | 4 |
| 3 | D1 `PUT` [X] | DELETE Y (Z vẫn ở D2) | X Z W | 3 |
| 4 | D1 `PUT` [X, V], **Kafka lỗi giữa chừng** (có thể DW đã nhận ADD V) | `pending` = {X, Z, W, V}, `published` = {X, Z, W} | X Z W (V?) | 3 |
| 5 | D1 login lại, `PUT` [X] (user đã xoá V) | DELETE V (gửi dư: V ở `pending`, không ở U) | X Z W | 3 |
| 6 | D2 `DELETE` | DELETE Z, DELETE W | X | 1 |

- `check` với digest khớp, không pending → `UNCHANGED`, không đọc family `b`.
- Chỉ đổi tên 1 contact → digest đổi → `UPLOAD` → row thiết bị cập nhật `b:names`, `m:dg`; không có event Kafka.
- `PUT` với digest app sai (khác digest server) → vẫn lưu, `m:dg` là digest server, metric `digest_mismatch` tăng.
- `check` với digest khớp, còn pending (T4) → service tự gửi bù từ `b:phones` của các row thiết bị đang lưu, app không upload.
- 2 request `PUT` cùng user cùng lúc → 1 cái `409 SYNC_IN_PROGRESS`.
- `PUT` 20.001 số → `413`; 20.000 số → 20 message trên Kafka.
- T5 biến thể: `PUT` [X, V] → ADD V (gửi bù); dù DW đã có V thì vẫn đúng.
- Gateway: 1 phần tử `phone_enc` ngoài khoảng, hoặc `user_id` khác nhau trong mảng → cả message vào DLQ.
- `phonecodec decode(encode(x)) == x` cho mọi số 10 chữ số hợp lệ; `encode` tất định; `phone_enc` luôn trong [10^9, 10^10).
- Event đến trễ (ADD X với `event_time` của T1 sau khi X đã DELETE) → DWD vẫn `active = 0`.

---

## 9. Vấn đề còn mở

1. **Thiết bị cũ**: bao lâu không sync thì bỏ danh bạ của thiết bị đó (đề xuất 90 ngày)? Có gọi `DELETE` khi logout không?
2. **Tổng số user**: tính tải đang giả định 50M. Dung lượng DWD tỉ lệ thuận với số user × 1.000.
3. **User không login lại**: sync lỗi rồi user không login nữa → DW lệch với user đó. Chấp nhận theo quyết định #3; muốn chặt hơn thì mỗi lần lỗi ghi `user_id` vào topic `phonebook_retry`, 1 worker đọc và chạy lại bước 2–6 (không cần request từ app; tránh phải scan cả bảng HBase).
4. **Quản lý khoá FF1**: nơi lưu (secret manager), ai được dùng `phonecodec`, audit log khi decode.
5. **Kerberos / phiên bản HBase** (mục 2.1): quyết định viết service bằng Go (gohbase) hay Java.
6. **Xác thực**: như friend-service, chưa kiểm tra người gọi đúng là `userId`.
