# Friend Network Tracking trên StarRocks

Theo dõi quan hệ bạn bè giữa các user từ luồng event, trả lời các câu hỏi:

- User có bao nhiêu bạn? Là những ai?
- User đang block ai / đang bị ai block? Số lượng?
- Lời mời đã gửi / đã nhận đang chờ?
- Số lượng các loại trên thay đổi thế nào theo ngày (180 ngày gần nhất)?

Chỉ cần **trạng thái mới nhất**; không trả lời "tại thời điểm X trong quá khứ, partners của user là ai".

Nguyên tắc cốt lõi: **mỗi cặp có hướng (`user_id → friend_id`) chỉ có đúng 1 status tại 1 thời điểm**, là event mới nhất theo `event_time`.

---

## 1. Kiến trúc

```
client ──► friend-service (TypeScript) ──► MongoDB (friendships)
                    │ commit xong
                    ▼
           Kafka (friend_service_events)
                    │ consumer group
                    ▼
           event-gateway (Go): kiểm tra contract ──(sai)──► Kafka (friend_events_dlq)
                    │
                    ▼
           Kafka (friend_events) ──┬─ Routine Load ──► ODS  ods_friend_event      (lịch sử, append)
                                   └─ Routine Load ──► DWD  dwd_friend_status     (trạng thái hiện tại, upsert)
                                                             └─ Task 10 phút ──► DWS  dws_friend_summary (số lượng theo user)
                                                                  └─ Task hằng ngày ──► DWS  dws_friend_daily (theo ngày)
```

| Layer | Bảng | 1 dòng là | Kiểu bảng | Cập nhật | Giữ | Dùng cho |
|---|---|---|---|---|---|---|
| Service | MongoDB `friendships` | 1 cặp có hướng | collection | transaction, ghi 2 chiều | vĩnh viễn | API online, nguồn sự thật (mục 10) |
| ODS | `ods_friend_event` | 1 event | Duplicate Key | Routine Load, append | **180 ngày** | audit, tìm user thay đổi cho summary |
| DWD | `dwd_friend_status` | 1 cặp có hướng | Primary Key | Routine Load, upsert theo `event_time` | **vĩnh viễn** | partners mỗi status của 1 user: số lượng, là ai |
| DWS | `dws_friend_summary` | 1 user | Primary Key | task 10 phút, chỉ user có event mới | **vĩnh viễn** (số mới nhất) | số lượng cho nhiều user cùng lúc |
| DWS | `dws_friend_daily` | 1 user / 1 ngày | Duplicate Key | task 00:05 mỗi ngày, ghi đè partition | **180 ngày** | xu hướng theo ngày |

ODS và DWD **cùng đọc song song từ Kafka**, DWD không đọc từ ODS.

`friend-service` là nguồn sự thật cho API online; StarRocks là bản sao phục vụ phân tích (trễ vài giây ở DWD, ≤ ~10 phút ở `dws_friend_summary`, 1 ngày ở `dws_friend_daily`). Chỉ `event-gateway` ghi vào topic `friend_events` mà StarRocks đọc (mục 11).

---

## 2. Event contract (phía service)

> Đã hiện thực: `com/tm/friend-service/` sinh event (mục 10), `com/tm/event-gateway/` đọc lại, kiểm tra contract rồi chuyển sang `friend_events` (mục 11). Topic `friend_events` chỉ chứa event đúng contract.

### 2.1 Status

Mọi status nhìn từ góc của `user_id`:

| Status | Nghĩa với `user_id` |
|---|---|
| `REQUESTED` | Mình đã gửi lời mời, đang chờ |
| `REVIEWED` | Mình nhận được lời mời, chưa trả lời |
| `FRIEND` | Đang là bạn |
| `CANCEL` | Lời mời đã huỷ, không còn quan hệ |
| `UNFRIEND` | Đã huỷ kết bạn, không còn quan hệ |
| `BLOCKING` | Mình đang block đối phương |
| `BLOCKED` | Mình đang bị đối phương block |

### 2.2 Hành động → event

**Mọi hành động đều sinh 2 event đối xứng**, cùng `event_time`. Routine Load không tách được 1 message thành 2 dòng, nên service phải tự bắn đủ 2 event.

| Hành động | Event 1 | Event 2 |
|---|---|---|
| A gửi lời mời cho B | `A → B  REQUESTED` | `B → A  REVIEWED` |
| B chấp nhận | `A → B  FRIEND` | `B → A  FRIEND` |
| A huỷ lời mời / B từ chối | `A → B  CANCEL` | `B → A  CANCEL` |
| A huỷ kết bạn với B | `A → B  UNFRIEND` | `B → A  UNFRIEND` |
| A block B | `A → B  BLOCKING` | `B → A  BLOCKED` |

### 2.3 Message format

- Topic: `friend_events`
- Key: `user_id` (đảm bảo event của cùng user vào cùng partition, giữ thứ tự)
- Value: JSON

```json
{
  "user_id": 1001,
  "friend_id": 2002,
  "event_type": "FRIEND",
  "event_time": "2026-09-23 10:10:00.123",
  "event_id": "0228440659126648833",
  "source": "friend-service"
}
```

| Field | Bắt buộc | Quy tắc |
|---|---|---|
| `user_id`, `friend_id` | ✅ | BIGINT, khác nhau |
| `event_type` | ✅ | 1 trong 7 status ở 2.1 |
| `event_time` | ✅ | Thời điểm **hành động xảy ra** (không phải lúc gửi Kafka), format `yyyy-MM-dd HH:mm:ss.SSS`, **UTC** |
| `event_id` | ✅ | Tăng dần theo thời gian (snowflake / sequence), padding cố định độ dài để so sánh chuỗi đúng. `friend-service` dùng snowflake padding **19 ký tự** |
| `source` | | Tên service phát event |

---

## 3. DDL

> Bản đầy đủ (DDL, Routine Load, task, query): `friend_network.sql`. Bản local: `com/tm/infra/starrocks/`.
> Số bucket tính cho quy mô 50M user × 500–1000 quan hệ, 100 hành động/s (mục 12); chỉnh theo số liệu thật trước khi tạo bảng, đổi sau phải ghi lại dữ liệu.
> Thời gian dùng `DATETIME` (StarRocks lưu tới micro giây nên giữ đủ mili giây của `event_time`); StarRocks không nhận cú pháp `DATETIME(3)`.

```sql
CREATE DATABASE IF NOT EXISTS social;
USE social;
```

### 3.1 ODS: `ods_friend_event`

```sql
CREATE TABLE IF NOT EXISTS ods_friend_event (
    user_id      BIGINT       NOT NULL,
    friend_id    BIGINT       NOT NULL,
    event_time   DATETIME     NOT NULL COMMENT 'thời điểm hành động, do service sinh (ms)',
    event_type   VARCHAR(16)  NOT NULL COMMENT 'REQUESTED/REVIEWED/FRIEND/CANCEL/UNFRIEND/BLOCKING/BLOCKED',
    event_id     VARCHAR(64)  NOT NULL COMMENT 'tăng dần, tie-break khi trùng event_time',
    source       VARCHAR(32)  NULL,
    ingest_time  DATETIME     NULL     COMMENT 'thời điểm vào StarRocks'
)
DUPLICATE KEY(user_id, friend_id, event_time)
PARTITION BY date_trunc('day', event_time)
DISTRIBUTED BY HASH(user_id) BUCKETS 4
PROPERTIES (
    "replication_num"       = "3",
    "partition_live_number" = "180"   -- TTL: chỉ giữ 180 partition ngày gần nhất
);
```

> **TTL 180 ngày**: mỗi partition là 1 ngày, StarRocks tự drop partition cũ, chỉ giữ 180 partition mới nhất (`partition_live_number` đếm partition, ngày không có event thì không có partition).

### 3.2 DWD: `dwd_friend_status`

```sql
CREATE TABLE IF NOT EXISTS dwd_friend_status (
    user_id          BIGINT       NOT NULL,
    friend_id        BIGINT       NOT NULL,
    status           VARCHAR(16)  NOT NULL COMMENT 'event_type mới nhất của cặp',
    last_event_time  DATETIME     NOT NULL COMMENT 'cột so sánh của merge_condition',
    last_event_id    VARCHAR(64)  NOT NULL,
    updated_at       DATETIME     NULL
)
PRIMARY KEY(user_id, friend_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 256
ORDER BY (user_id, status)
PROPERTIES (
    "replication_num"         = "3",
    "enable_persistent_index" = "true"
);
```

### 3.3 DWS: `dws_friend_summary`

Bảng thường (không phải materialized view): MV phải tính lại toàn bộ DWD (hàng chục tỉ dòng) mỗi lần refresh; task mục 6.2 chỉ tính lại user có event mới.

```sql
CREATE TABLE IF NOT EXISTS dws_friend_summary (
    user_id               BIGINT       NOT NULL,
    friend_cnt            BIGINT,
    pending_sent_cnt      BIGINT,
    pending_received_cnt  BIGINT,
    blocking_cnt          BIGINT,
    blocked_cnt           BIGINT,
    last_activity         DATETIME,
    computed_at           DATETIME     COMMENT 'lần tính gần nhất; mốc để lần sau tìm user thay đổi'
)
PRIMARY KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 16
PROPERTIES ("replication_num" = "3");
```

### 3.4 DWS theo ngày: `dws_friend_daily`

```sql
CREATE TABLE IF NOT EXISTS dws_friend_daily (
    dt                    DATE         NOT NULL,
    user_id               BIGINT       NOT NULL,
    friend_cnt            BIGINT,
    pending_sent_cnt      BIGINT,
    pending_received_cnt  BIGINT,
    blocking_cnt          BIGINT,
    blocked_cnt           BIGINT,
    last_activity         DATETIME,
    snapshot_at           DATETIME     COMMENT 'lần chụp gần nhất'
)
DUPLICATE KEY(dt, user_id)
PARTITION BY dt
DISTRIBUTED BY HASH(user_id) BUCKETS 4
PROPERTIES (
    "replication_num"       = "3",
    "partition_live_number" = "180"
);
```

## 4. Nạp dữ liệu (Routine Load)

> Chạy local: `com/tm/infra/docker-compose.yml` có sẵn StarRocks, tự tạo bảng + Routine Load đọc `friend_events` (bản local của DDL: `replication_num = 1`, ít bucket, broker `kafka:9092`, task summary mỗi 1 phút, task daily mỗi 5 phút). Hướng dẫn: [`com/tm/friend-service/README.md`](com/tm/friend-service/README.md#6-xem-dữ-liệu-trên-starrocks).

### 4.1 Kafka → ODS

```sql
CREATE ROUTINE LOAD social.rl_friend_ods ON ods_friend_event
COLUMNS (user_id, friend_id, event_time, event_type, event_id, source, ingest_time = now())
PROPERTIES (
    "format"               = "json",
    "jsonpaths"            = "[\"$.user_id\",\"$.friend_id\",\"$.event_time\",\"$.event_type\",\"$.event_id\",\"$.source\"]",
    "desired_concurrent_number" = "3",
    "max_error_number"     = "1000"
)
FROM KAFKA (
    "kafka_broker_list"   = "broker1:9092,broker2:9092",
    "kafka_topic"         = "friend_events",
    "property.group.id"   = "sr_friend_ods",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
```

### 4.2 Kafka → DWD

`merge_condition` = chỉ ghi đè khi `event_time` mới **>=** giá trị đang có → event đến trễ không làm hỏng trạng thái.

```sql
CREATE ROUTINE LOAD social.rl_friend_dwd ON dwd_friend_status
COLUMNS (user_id, friend_id, status, last_event_time, last_event_id, updated_at = now())
PROPERTIES (
    "format"               = "json",
    "jsonpaths"            = "[\"$.user_id\",\"$.friend_id\",\"$.event_type\",\"$.event_time\",\"$.event_id\"]",
    "merge_condition"      = "last_event_time",
    "desired_concurrent_number" = "3",
    "max_error_number"     = "1000"
)
FROM KAFKA (
    "kafka_broker_list"   = "broker1:9092,broker2:9092",
    "kafka_topic"         = "friend_events",
    "property.group.id"   = "sr_friend_dwd",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
```

---

## 5. Truy vấn

### Trạng thái mới nhất (DWD / DWS, giữ vĩnh viễn)

DWD luôn giữ event mới nhất của mỗi cặp. Truy vấn theo 1 user chỉ đọc 1 bucket, bảng sắp theo `(user_id, status)` nên trả về trong vài ms kể cả khi có hàng chục tỉ dòng.

```sql
-- Q1. Số partners ở mỗi status của 1 user (DWD, chính xác tức thì)
SELECT status, COUNT(*) AS partners
FROM dwd_friend_status
WHERE user_id = ?
GROUP BY status;

-- Q2. Partners ở mỗi status của 1 user là ai (thêm AND status = '...' để lọc 1 status)
SELECT status, friend_id, last_event_time AS since
FROM dwd_friend_status
WHERE user_id = ?
ORDER BY status, friend_id;

-- Q3. Đang block ai / đang bị ai block
SELECT friend_id, status
FROM dwd_friend_status
WHERE user_id = ? AND status IN ('BLOCKING', 'BLOCKED');

-- Q4. Lời mời đã gửi / đã nhận đang chờ
SELECT friend_id, status, last_event_time
FROM dwd_friend_status
WHERE user_id = ? AND status IN ('REQUESTED', 'REVIEWED');

-- Q5. Quan hệ giữa 2 user cụ thể
SELECT status FROM dwd_friend_status WHERE user_id = ? AND friend_id = ?;

-- Q6. Bạn chung của 2 user
SELECT a.friend_id
FROM dwd_friend_status a
JOIN dwd_friend_status b ON a.friend_id = b.friend_id
WHERE a.user_id = ? AND b.user_id = ?
  AND a.status = 'FRIEND' AND b.status = 'FRIEND';

-- Q7. Số lượng theo status cho nhiều user cùng lúc (DWS, trễ tối đa 1 chu kỳ task 4.1)
--     vd top 100 user nhiều bạn nhất
SELECT user_id, friend_cnt, pending_sent_cnt, pending_received_cnt, blocking_cnt, blocked_cnt
FROM dws_friend_summary
ORDER BY friend_cnt DESC
LIMIT 100;
```

### Lịch sử (ODS / DWS daily, giữ 180 ngày)

```sql
-- Q9. Lịch sử event giữa 2 user trong 180 ngày gần nhất
SELECT event_time, event_type, event_id
FROM ods_friend_event
WHERE user_id = ? AND friend_id = ?
ORDER BY event_time, event_id;

-- Q10. Xu hướng số lượng theo ngày của 1 user
SELECT dt, friend_cnt, pending_sent_cnt, pending_received_cnt, blocking_cnt, blocked_cnt
FROM dws_friend_daily
WHERE user_id = ? AND dt BETWEEN ? AND ?
ORDER BY dt;
```

## 6. Job

Routine Load, task, tạo / xoá partition theo TTL, compaction đều do StarRocks tự chạy. Giờ của task theo múi giờ StarRocks (`@@time_zone`).

### 6.1 Monitor Routine Load (bắt buộc)

Routine Load bị `PAUSED` khi vượt `max_error_number` và **không tự resume**. Cần job giám sát (cron 1–5 phút) + alert:

```sql
SHOW ROUTINE LOAD FROM social;              -- đọc cột State, ReasonOfStateChanged, ErrorLogUrls
RESUME ROUTINE LOAD FOR social.rl_friend_dwd; -- sau khi alert / xử lý nguyên nhân
```

Theo dõi thêm consumer lag của event-gateway (group `event-gateway`) và số message trong `friend_events_dlq`.

### 6.2 Task cập nhật `dws_friend_summary` (10 phút)

```sql
SUBMIT TASK t_friend_summary_refresh
SCHEDULE EVERY (INTERVAL 10 MINUTE)
AS
INSERT INTO dws_friend_summary
SELECT user_id,
       SUM(IF(status = 'FRIEND',    1, 0)),
       SUM(IF(status = 'REQUESTED', 1, 0)),
       SUM(IF(status = 'REVIEWED',  1, 0)),
       SUM(IF(status = 'BLOCKING',  1, 0)),
       SUM(IF(status = 'BLOCKED',   1, 0)),
       MAX(last_event_time),
       NOW()
FROM dwd_friend_status
WHERE user_id IN (
    SELECT DISTINCT user_id FROM ods_friend_event
    WHERE ingest_time >= (SELECT COALESCE(MAX(computed_at), '1970-01-01') FROM dws_friend_summary) - INTERVAL 5 MINUTE
      AND event_time  >= (SELECT COALESCE(MAX(computed_at), '1970-01-01') FROM dws_friend_summary) - INTERVAL 1 DAY
)
GROUP BY user_id;
```

- Chỉ tính lại user có event mới trong ODS kể từ lần trước (mốc = `MAX(computed_at)`), ≤ ~120k user / 10 phút ở 100 hành động/s, thay vì quét toàn bộ DWD.
- Task dừng lâu thì lần sau tự lấy bù từ mốc cũ.
- Lùi 5 phút để chờ DWD (Routine Load riêng) nạp kịp ODS; nếu `rl_friend_dwd` bị PAUSED lâu hơn thì số của user đó chỉ đúng lại khi user có event kế tiếp → giám sát 6.1.
- Lần đầu (sau khi nạp dữ liệu ban đầu vào DWD) chạy 1 lần câu tính toàn bộ ở cuối mục 4 của `friend_network.sql`.

### 6.3 Task chụp `dws_friend_daily` (00:05 mỗi ngày)

```sql
SUBMIT TASK t_friend_daily_snapshot
SCHEDULE START ('2026-09-25 00:05:00') EVERY (INTERVAL 1 DAY)
AS
INSERT /*+SET_VAR(dynamic_overwrite = true)*/ OVERWRITE dws_friend_daily
SELECT DATE(NOW() - INTERVAL 10 MINUTE), user_id,
       friend_cnt, pending_sent_cnt, pending_received_cnt,
       blocking_cnt, blocked_cnt, last_activity, NOW()
FROM dws_friend_summary;
```

- Ghi đè cả partition của ngày (`dynamic_overwrite`) nên chạy lại không nhân đôi.
- ~50M dòng/ngày ≈ 0,5–1 GB nén; giữ 180 ngày.

### 6.4 Xem các lần chạy task

```sql
SELECT task_name, state, error_message, create_time, finish_time
FROM information_schema.task_runs
WHERE task_name IN ('t_friend_summary_refresh', 't_friend_daily_snapshot')
ORDER BY create_time DESC LIMIT 20;
```

---

## 7. Kịch bản kiểm thử

### 7.1 Luồng chuẩn

| T | Hành động | Event gửi Kafka |
|---|---|---|
| 1 | A request B | `A→B REQUESTED`, `B→A REVIEWED` |
| 2 | B accept A | `A→B FRIEND`, `B→A FRIEND` |
| 3 | A request C | `A→C REQUESTED`, `C→A REVIEWED` |
| 4 | C accept A | `A→C FRIEND`, `C→A FRIEND` |
| 5 | A block B | `A→B BLOCKING`, `B→A BLOCKED` |

Kỳ vọng DWD của A sau từng bước:

| Sau T | A→B | A→C | bạn của A | friend_cnt |
|---|---|---|---|---|
| 1 | REQUESTED | – | ∅ | 0 |
| 2 | FRIEND | – | B | 1 |
| 3 | FRIEND | REQUESTED | B | 1 |
| 4 | FRIEND | FRIEND | B, C | 2 |
| 5 | BLOCKING | FRIEND | C | 1 |

Kỳ vọng sau T5:

- Q1 của A: `FRIEND 1, BLOCKING 1`; Q2 của A: `BLOCKING → B`, `FRIEND → C`
- `dws_friend_summary` (sau 1 chu kỳ task) của A: `friend_cnt = 1, blocking_cnt = 1`; của B: `friend_cnt = 0, blocked_cnt = 1`

### 7.2 Event đến trễ

Sau T5, gửi lại event `A→B FRIEND` với `event_time` của T2.

- ODS: có thêm 1 dòng trùng.
- DWD: `A→B` **vẫn là `BLOCKING`** (merge_condition bỏ qua vì event_time cũ hơn).

### 7.3 Event trùng

Gửi 2 lần cùng 1 event (cùng `event_id`). DWD không đổi, summary không đổi.

---

## 8. Cấu trúc repo

Hiện có:

```
.
├── README.md
├── MODULE.bazel  .bazelrc  .bazelversion  BUILD.bazel   # Bazel 8.7 (Go, Node.js, OCI)
├── friend_network.sql            # toàn bộ DDL, Routine Load, task, query Q1–Q10
├── tools/rules/com_tm_container.bzl   # macro binary + OCI image: com_tm_go_image, com_tm_js_image
└── com/tm/
    ├── event-gateway/            # Go: Kafka friend_service_events → friend_events (mục 11)
    │   ├── BUILD.bazel  main.go  README.md
    │   └── internal/{config,consumer,event,producer,relay}/
    ├── friend-service/           # Node.js + MongoDB (mục 10), cấu trúc layer ở 10.2
    │   ├── BUILD.bazel           # npm deps, package.json, tsconfig
    │   ├── README.md             # setup và chạy thử
    │   ├── ts.bzl                # macro ts_layer
    │   ├── package.json  pnpm-lock.yaml  tsconfig.json  tsconfig.build.json
    │   ├── src/
    │   │   ├── BUILD.bazel  server.ts  container.ts  http-server.ts   # MainServer: DI + binary + image
    │   │   └── {router,handler,controller,dao,utils,configs}/   # mỗi layer 1 BUILD.bazel
    │   └── test/                 # BUILD.bazel riêng cho js_test
    └── infra/
        ├── docker-compose.yml    # MongoDB, Kafka, StarRocks, event-gateway, friend-service cho local
        └── starrocks/            # init StarRocks local: schema (01_schema.sql), Routine Load, init.sh
```

Gợi ý khi tách nhỏ phần SQL:

```
friend-network/
├── README.md
├── sql/
│   ├── 01_database.sql
│   ├── 02_ods_friend_event.sql
│   ├── 03_dwd_friend_status.sql
│   ├── 04_dws_friend_summary.sql
│   ├── 05_dws_friend_daily.sql
│   ├── 20_task_friend_summary_refresh.sql
│   ├── 10_rl_friend_ods.sql
│   ├── 11_rl_friend_dwd.sql
│   ├── 20_task_daily_snapshot.sql
│   └── queries/            # Q1–Q10
├── jobs/                    # monitor routine load, task
└── test/
    └── scenarios/           # dữ liệu mẫu JSON cho mục 7
```

---

## 9. Vấn đề còn mở (cần chốt trước khi lên prod)

1. **Unblock**: chưa có event mở block. Hiện block chỉ kết thúc khi có event khác cho cặp đó. Nếu có tính năng unblock, cần thêm status (vd `UNBLOCK`) và định nghĩa trạng thái sau khi unblock.
2. ~~**Block 2 chiều**~~: đã chốt **không cho block ngược**. `friend-service` trả `409 BLOCKED_BY_TARGET` khi user đang bị block cố block lại.
3. ~~**Trùng `event_time`**~~: `friend-service` đảm bảo `event_time` của 1 cặp luôn tăng: lấy `max(now, last_event_time + 1ms)` trong transaction, kể cả khi đồng hồ các instance lệch nhau.
4. **Số bucket / replication**: đã tính cho 50M user × 500–1000 quan hệ (mục 12); kiểm lại bằng số liệu thật trước khi tạo bảng (đổi bucket sau phải ghi lại dữ liệu).
5. ~~**TTL**~~: đã chốt: ODS và `dws_friend_daily` giữ **180 ngày**; DWD và `dws_friend_summary` giữ vĩnh viễn (chỉ cần trạng thái mới nhất).
6. **Xác thực**: API nhận `userId` trên path, chưa kiểm tra người gọi có đúng là user đó. Cần gắn auth (JWT / gateway) trước khi mở ra ngoài.
7. **Unblock** (liên quan #1): service chưa có API unblock vì chưa định nghĩa status.
8. ~~**Truy vấn tại thời điểm quá khứ**~~: không cần (chỉ cần trạng thái mới nhất), đã bỏ truy vấn as-of trên ODS.
9. **Mất event khi Kafka lỗi**: friend-service gửi event lên Kafka ngay sau khi commit MongoDB, không có outbox. Kafka lỗi lúc đó (sau khi kafkajs retry) thì hành động vẫn lưu nhưng event bị mất, StarRocks lệch với MongoDB cho cặp đó tới event kế tiếp. Log `event publish failed after db commit` có đủ nội dung event để gửi bù bằng tay.

---

## 10. Friend service (Node.js + MongoDB)

Service nhận hành động của user qua REST API, lưu trạng thái trong MongoDB và gửi event (contract mục 2) lên Kafka topic `friend_service_events`; event-gateway (mục 11) kiểm tra rồi chuyển sang `friend_events`. Code ở `com/tm/friend-service/`, Docker ở `com/tm/infra/`.

### 10.1 Công nghệ

| Thành phần | Chọn | Ghi chú |
|---|---|---|
| Ngôn ngữ | TypeScript 5.9 (`strict`, ESM `nodenext`) | Bazel typecheck lúc build |
| Runtime | Node.js 22 | |
| HTTP | Express 5 | lỗi async tự đi vào error handler |
| DI | awilix 12 | không dùng decorator; mỗi layer 1 file `*-module.ts` |
| DB | MongoDB 7, **replica set** | cần replica set để dùng transaction (local: 1 node) |
| Kafka client | kafkajs | producer idempotent, `acks=all`, ghi topic `friend_service_events` |
| Test | `node:test` | chạy bằng Bazel (`js_test`) hoặc `tsx` khi dev |
| Package manager | pnpm 9 | `pnpm-lock.yaml` là nguồn cho Bazel |
| Build / image | Bazel 8.7: `aspect_rules_ts` 3.10.1 (tsc), `aspect_rules_js` 3.4.1, `rules_nodejs` (Node 22.22.3), `rules_oci` | macro `ts_layer`, `com_tm_js_image`; image trên `debian:bookworm-slim` |

### 10.2 Cấu trúc code theo layer

Mỗi layer là 1 package Bazel (`ts_layer` = `ts_project` với tsconfig chung, `com/tm/friend-service/ts.bzl`). `visibility` của Bazel chặn phụ thuộc sai chiều: vd Controller khai báo dep vào Router sẽ báo lỗi `is not visible` lúc build.

```
MainServer (src/server.ts, container.ts, http-server.ts)
   │ dựng DI container, start/stop lifecycle
   ▼
router ─► handler ─► controller ─► dao ─► MongoDB / Kafka
                                          utils: dùng chung; configs: chỉ MainServer đọc
```

| Layer | Vị trí | Việc | Không được |
|---|---|---|---|
| MainServer | `src/server.ts`, `src/container.ts`, `src/http-server.ts` | đọc config, dựng container từ module các layer, start lifecycle theo thứ tự, shutdown | chứa logic, tự tạo MongoDB / Kafka |
| Router | `src/router/` | map method + path → handler | parse request, gọi controller |
| Handler | `src/handler/` | parse/validate tham số HTTP, gọi Controller, ghi response, map lỗi → HTTP status, health check | chứa luật nghiệp vụ, đụng DB |
| Controller | `src/controller/` | luật chuyển trạng thái (`plan()`), transaction ghi 2 chiều, gửi event Kafka sau commit, truy vấn | biết về HTTP |
| Dao | `src/dao/` | MongoDB (`friendships`, transaction, index) và Kafka producer | chứa luật nghiệp vụ |
| Utils | `src/utils/` | `DomainError`, snowflake `event_id`, format thời gian, logger, `Lifecycle` | phụ thuộc layer khác |
| Configs | `src/configs/` | đọc biến môi trường (10.7) | được layer khác đọc trực tiếp |

#### Dependency injection (awilix)

Mỗi layer có 1 file `*-module.ts` đăng ký thành phần của nó vào container (giống module Dagger):

| Module | Đăng ký | Lifecycle |
|---|---|---|
| `utils/utils-module.ts` | `log`, `nextId` | |
| `dao/dao-module.ts` | `mongoClient`, `db`, `runInTransaction`, `friendshipDao`, `kafkaProducer`, `eventPublisher`, `pingDb` | `mongoLifecycle`: connect + tạo index / close; `kafkaLifecycle`: connect / disconnect producer |
| `controller/controller-module.ts` | `friendController` | |
| `handler/handler-module.ts` | `friendHandler`, `healthHandler`, `errorHandler` | |
| `router/router-module.ts` | `router` | |
| `http-server.ts` | `app` (Express) | `httpLifecycle`: listen / close |

- Mọi thành phần là singleton; factory lấy dependency bằng destructuring (`InjectionMode.PROXY`), vd `({ db }) => new FriendshipDao(db)`.
- Class nghiệp vụ (`FriendController`, `FriendshipDao`...) không import awilix, nhận dependency qua constructor → test dùng Dao giả trực tiếp (`test/controller.test.ts`).
- Mỗi module khai báo phần config nó cần (`DaoSettings`, `ControllerSettings`...) thay vì phụ thuộc layer Configs.
- `server.ts` start theo `LIFECYCLE_ORDER` (mongo → kafka → http), shutdown (SIGINT / SIGTERM) dừng ngược lại rồi `container.dispose()`.
- `test/container.test.ts` resolve mọi registration mà không kết nối thật: thiếu / sai tên dependency là test fail.

Thêm thành phần mới: viết class / hàm như bình thường → đăng ký trong `*-module.ts` của layer đó → thêm kiểu vào `*Cradle` của module; nếu cần start/stop thì đăng ký `Lifecycle` và thêm tên vào `LIFECYCLE_ORDER`.

### 10.3 Luồng ghi (1 hành động)

```
POST /v1/users/1/requests/2
  1. MongoDB transaction (snapshot, w=majority)
       a. đọc 2 document: 1→2 và 2→1
       b. kiểm tra luật chuyển trạng thái (10.5)
       c. upsert 2 document friendships (2 chiều, cùng event_time)
  2. commit thành công → gửi 2 event lên Kafka friend_service_events (1 lần send, key = user_id, acks=all)
     event-gateway đọc, kiểm tra contract, chuyển sang friend_events (mục 11)
  3. trả 200/201
```

- Transaction lỗi hoặc vi phạm luật → không gửi event nào.
- Kafka lỗi sau khi đã commit (kafkajs đã tự retry): service vẫn trả thành công vì hành động đã lưu, và log `event publish failed after db commit` kèm nội dung 2 event.
- event-gateway dừng không làm mất event: event nằm chờ trong `friend_service_events`, gateway chạy lại thì đọc tiếp từ offset đã commit. **Event đó không tới được DW** (xem vấn đề mở ở mục 9).
- Đồng thời: 2 request cùng cặp chạy song song thì MongoDB báo write conflict, transaction tự retry và thấy trạng thái mới → chỉ 1 request thành công, còn lại trả `409`.

### 10.4 MongoDB schema

Database `friend_network`.

**`friendships`** — 1 document / 1 cặp có hướng, tương đương `dwd_friend_status`:

```js
{
  _id: ObjectId,
  user_id: 1001,                 // Number, safe integer
  friend_id: 2002,
  status: "FRIEND",              // 1 trong 7 status ở 2.1
  last_event_time: ISODate("2026-09-23T10:10:00.123Z"),
  last_event_id: "0228440659126648833",
  created_at: ISODate(...),
  updated_at: ISODate(...)
}
```

| Index | Key | Mục đích |
|---|---|---|
| `uq_pair` (unique) | `{user_id: 1, friend_id: 1}` | 1 document / cặp |
| `ix_user_status_friend` | `{user_id: 1, status: 1, friend_id: 1}` | danh sách theo status, phân trang |

### 10.5 Luật chuyển trạng thái

Nhìn từ status hiện tại của `actor → target`. Mọi hành động hợp lệ ghi 2 chiều + 2 event đối xứng (mục 2.2).

| Hành động | Hợp lệ khi `actor → target` là | Sau đó `actor → target` / `target → actor` | Lỗi |
|---|---|---|---|
| request | chưa có, `CANCEL`, `UNFRIEND` | `REQUESTED` / `REVIEWED` | `409 ALREADY_REQUESTED`, `409 PENDING_FROM_TARGET`, `409 ALREADY_FRIENDS`, `403 BLOCKED` |
| cancel | `REQUESTED` | `CANCEL` / `CANCEL` | `409 NO_SENT_REQUEST` |
| accept | `REVIEWED` | `FRIEND` / `FRIEND` | `409 NO_RECEIVED_REQUEST` |
| reject | `REVIEWED` | `CANCEL` / `CANCEL` | `409 NO_RECEIVED_REQUEST` |
| unfriend | `FRIEND` | `UNFRIEND` / `UNFRIEND` | `409 NOT_FRIENDS` |
| block | mọi trạng thái trừ `BLOCKING`, `BLOCKED` | `BLOCKING` / `BLOCKED` | `409 ALREADY_BLOCKING`, `409 BLOCKED_BY_TARGET` |

Luật nằm trong hàm thuần `plan()` ở `com/tm/friend-service/src/controller/friendship-rules.ts`, có unit test cho từng ô của bảng.

### 10.6 API

Base path `/v1`. `userId`, `targetId` là số nguyên dương ≤ 2^53−1 (giới hạn của JSON number).

**Hành động** — trả `{user_id, friend_id, status, event_time}` với `status` là trạng thái mới của `userId → targetId`:

| Method | Path | Hành động |
|---|---|---|
| `POST` | `/users/:userId/requests/:targetId` | gửi lời mời (201) |
| `DELETE` | `/users/:userId/requests/:targetId` | huỷ lời mời đã gửi |
| `POST` | `/users/:userId/requests/:targetId/accept` | chấp nhận lời mời `targetId` gửi |
| `POST` | `/users/:userId/requests/:targetId/reject` | từ chối lời mời `targetId` gửi |
| `DELETE` | `/users/:userId/friends/:targetId` | huỷ kết bạn |
| `POST` | `/users/:userId/blocks/:targetId` | block (201) |

**Truy vấn** — đọc thẳng MongoDB, luôn là trạng thái mới nhất:

| Method | Path | Trả về | Tương đương SQL |
|---|---|---|---|
| `GET` | `/users/:userId/friends?limit=&after=` | danh sách bạn | Q2 |
| `GET` | `/users/:userId/requests?type=sent\|received\|all` | lời mời đang chờ | Q4 |
| `GET` | `/users/:userId/blocks?type=blocking\|blocked\|all` | block | Q3 |
| `GET` | `/users/:userId/summary` | số lượng theo status | Q1, Q7 |
| `GET` | `/users/:userId/relationships/:targetId` | quan hệ 2 chiều | Q5 |
| `GET` | `/users/:userId/mutual-friends/:targetId?limit=` | bạn chung | Q6 |
| `GET` | `/healthz` | ping MongoDB | |

Danh sách phân trang theo `friend_id`: truyền `next_after` của trang trước vào `after`. `limit` mặc định 50, tối đa 200.

Xu hướng theo ngày (Q10) và lịch sử event (Q9) chỉ có ở StarRocks.

Lỗi trả dạng `{"error": "<CODE>", "message": "..."}`, mã HTTP 400 / 403 / 409 / 500.

Ví dụ:

```bash
curl -X POST localhost:3000/v1/users/1/requests/2
# {"user_id":1,"friend_id":2,"status":"REQUESTED","event_time":"2026-09-23 09:01:37.334"}

curl localhost:3000/v1/users/1/summary
# {"user_id":1,"friend_cnt":1,"pending_sent_cnt":0,"pending_received_cnt":0,"blocking_cnt":1,"blocked_cnt":0,"last_activity":"..."}
```

### 10.7 Cấu hình (biến môi trường)

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `PORT` | `3000` | cổng HTTP |
| `MONGO_URI` | `mongodb://localhost:27017/?directConnection=true` | phải trỏ tới replica set |
| `MONGO_DB` | `friend_network` | |
| `KAFKA_BROKERS` | `localhost:29092` | danh sách cách nhau bởi dấu phẩy |
| `KAFKA_TOPIC` | `friend_service_events` | topic friend-service ghi, event-gateway đọc |
| `KAFKA_CLIENT_ID` | `friend-service` | |
| `EVENT_SOURCE` | `friend-service` | field `source` của event |
| `WORKER_ID` | `0` | 0–1023, **mỗi instance 1 giá trị khác nhau** để `event_id` không trùng |

### 10.8 Build và chạy (Bazel)

> Hướng dẫn từng bước chạy thử trên Docker: [`com/tm/friend-service/README.md`](com/tm/friend-service/README.md).

Service build bằng macro `com_tm_js_image` (`tools/rules/com_tm_container.bzl`), sinh các target:

| Target | Việc |
|---|---|
| `//com/tm/friend-service/src:friend_service` | `js_binary` chạy local |
| `//com/tm/friend-service/src:mainserver` | `ts_project` của MainServer (`server.ts`, `container.ts`, `http-server.ts`) |
| `//com/tm/friend-service/src/<layer>` | `ts_project` của từng layer (biên dịch + typecheck) |
| `//com/tm/friend-service/src:friend_service_image` | `oci_image`: Node toolchain + node_modules + app |
| `//com/tm/friend-service/src:friend_service_docker` | load image `com.tm.js.friend_service:v1.0.0` vào Docker |
| `//com/tm/friend-service/test:<tên>_test` | `js_test` cho mỗi `test/*.test.ts`: `container`, `controller`, `event_publisher`, `friendship_rules`, `params`, `utils` |

```bash
# test
bazel test //com/tm/friend-service/test/...

# chạy local (cần MongoDB + Kafka, xem bên dưới)
bazel run //com/tm/friend-service/src:friend_service

# build image + load vào Docker (chọn đúng kiến trúc máy chạy container)
bazel run --config=linux-arm64 //com/tm/friend-service/src:friend_service_docker   # Mac Apple Silicon
bazel run --config=linux-amd64 //com/tm/friend-service/src:friend_service_docker   # server x86
```

Stack local (MongoDB replica set + Kafka + event-gateway + friend-service từ image trên):

```bash
bazel run --config=linux-arm64 //com/tm/event-gateway:event_gateway_docker
bazel run --config=linux-arm64 //com/tm/friend-service/src:friend_service_docker
cd com/tm/infra && docker compose up -d

# xem event trên Kafka
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic friend_events --from-beginning --property print.key=true
```

Chạy `bazel run ...:friend_service` trên máy thay cho container: `docker compose up -d mongo kafka event-gateway` (không start `friend-service` để trống port 3000; gateway cần chạy để event tới `friend_events`).

Kafka từ máy host ở `localhost:29092`; StarRocks Routine Load trong cùng network dùng `kafka:9092`.

Push image lên registry: truyền `repository = "<registry>/<repo>"` vào `com_tm_js_image` → sinh thêm `:friend_service_push`.

Thêm / đổi npm package: sửa `package.json` → `pnpm install` (cập nhật `pnpm-lock.yaml`) → thêm `//com/tm/friend-service:node_modules/<package>` vào `deps` của layer dùng nó (vd `src/dao/BUILD.bazel`); package không kèm type thì thêm cả `@types/<package>`. Phiên bản TypeScript của Bazel lấy từ `devDependencies.typescript` (phải là phiên bản cụ thể). File `.ts` mới trong 1 layer tự vào build (`glob`). Package có build script phải khai báo trong `pnpm.onlyBuiltDependencies` của `package.json`.

Dev không qua Bazel (trong `com/tm/friend-service`): `pnpm dev` (tsx watch), `pnpm test`, `pnpm typecheck`, `pnpm build && pnpm start` (ra `dist/`).

---

## 11. Event gateway (Go)

Service Go đứng giữa friend-service và StarRocks: đọc event friend-service ghi vào Kafka, kiểm tra đúng contract mục 2.3, chuyển event hợp lệ sang topic `friend_events` cho StarRocks Routine Load, event sai sang topic dead letter. Code ở `com/tm/event-gateway/`, hướng dẫn chạy ở [`com/tm/event-gateway/README.md`](com/tm/event-gateway/README.md).

```
friend-service ──► friend_service_events ──► event-gateway ──► friend_events ──► StarRocks (ODS, DWD)
                                                   └─ sai contract ──► friend_events_dlq
```

Lý do tách: `friend_events` (StarRocks đọc) chỉ chứa event đã kiểm tra; 1 event sai không làm Routine Load lỗi / PAUSED; service khác sau này chỉ cần ghi Kafka theo contract.

### 11.1 Topic

| Topic | Ghi | Đọc | Nội dung |
|---|---|---|---|
| `friend_service_events` | friend-service | event-gateway (group `event-gateway`) | event thô, key = `user_id` |
| `friend_events` | event-gateway | StarRocks `rl_friend_ods`, `rl_friend_dwd` | event đúng contract, JSON chuẩn hoá, key = `user_id` |
| `friend_events_dlq` | event-gateway | người vận hành | event sai nguyên văn, header `x-error`, `x-source-topic`, `x-source-partition`, `x-source-offset` |

### 11.2 Xử lý

- Consumer group đọc tuần tự từng partition → giữ thứ tự event của cùng user.
- Mỗi message: decode JSON (field lạ = lỗi), kiểm tra contract → gửi `friend_events` (key lấy lại từ `user_id`) hoặc `friend_events_dlq`.
- Chỉ commit offset sau khi đã gửi xong (at-least-once). Kafka lỗi thì gửi lại với backoff (200 ms → 10 s) tới khi được, không bỏ qua message; rebalance / shutdown thì message chưa gửi sẽ được đọc lại. Trùng event không làm sai DW (mục 7.3).
- Group mới đọc từ đầu topic (`OffsetOldest`).

Kiểm tra contract: `user_id`, `friend_id` trong 1..2^53−1 và khác nhau; `event_type` 1 trong 7 status (chữ hoa); `event_time` đúng `yyyy-MM-dd HH:mm:ss.SSS`; `event_id` đúng 19 chữ số; `source` tuỳ chọn.

### 11.3 Công nghệ và cấu trúc code

| Thành phần | Chọn |
|---|---|
| Ngôn ngữ | Go 1.22, `log/slog` JSON |
| Kafka | IBM/sarama: `ConsumerGroup` + `SyncProducer` idempotent, `acks=all` |
| Build / image | Bazel `rules_go` + gazelle, macro `com_tm_go_image`, image `gcr.io/distroless/base` |

| Package | Việc |
|---|---|
| `main.go` | đọc config, kết nối Kafka (thử lại tới 60 s khi Kafka chưa sẵn sàng), chạy consumer + HTTP `/healthz`, shutdown khi SIGINT / SIGTERM |
| `internal/config` | biến môi trường |
| `internal/event` | `FriendEvent`, `Decode()`, `Validate()`, `Key()` |
| `internal/relay` | định tuyến hợp lệ / DLQ, gửi lại khi Kafka lỗi |
| `internal/consumer` | sarama consumer group, mark offset sau khi relay xong |
| `internal/producer` | interface `Producer` + bản sarama |

`GET /healthz` (cổng 8080) → `{"ok": true, "consuming": true}`; `consuming` = đã được chia partition.

### 11.4 Cấu hình

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `PORT` | `8080` | cổng HTTP health check |
| `KAFKA_BROKERS` | `localhost:29092` | cách nhau bởi dấu phẩy |
| `KAFKA_GROUP_ID` | `event-gateway` | consumer group |
| `KAFKA_INPUT_TOPIC` | `friend_service_events` | topic đọc |
| `KAFKA_FRIEND_TOPIC` | `friend_events` | topic ghi event hợp lệ (StarRocks đọc) |
| `KAFKA_DLQ_TOPIC` | `friend_events_dlq` | topic ghi event sai |
| `KAFKA_CLIENT_ID` | `event-gateway` | |

### 11.5 Build và test

```bash
bazel test //com/tm/event-gateway/...
bazel run //com/tm/event-gateway:event_gateway                                   # chạy local
bazel run --config=linux-arm64 //com/tm/event-gateway:event_gateway_docker       # image com.tm.go.event_gateway:v1.0.0
bazel run //:gazelle                                                             # sau khi đổi import Go
```

Partition: friend-service (kafkajs) dùng murmur2, gateway (sarama) dùng FNV-1a trên key. Thứ tự vẫn giữ vì mỗi topic chỉ có 1 producer và key giống nhau luôn vào cùng partition của topic đó; nếu thêm producer khác cùng ghi 1 topic thì phải thống nhất partitioner.

---

## 12. Quy mô và dung lượng (ước lượng)

Giả định: **50M user**, trung bình **500–1000 quan hệ / user** (tính cả lời mời, block, quan hệ đã huỷ), **100 hành động / s** (200 event / s). Chưa qua thử tải.

| Bảng | Dòng | Nén, 1 bản sao | Giữ | Bucket |
|---|---|---|---|---|
| `ods_friend_event` | ~17M / ngày → ~3 tỉ (180 ngày) | ~0,5–0,8 GB / ngày → ~90–150 GB | 180 ngày | 4 / partition ngày |
| `dwd_friend_status` | 25–50 tỉ, tăng dần | ~0,5–1,5 TB | vĩnh viễn | 256 |
| `dws_friend_summary` | ~50M | ~1–2 GB | vĩnh viễn | 16 |
| `dws_friend_daily` | 50M / ngày → ~9 tỉ (180 ngày) | ~0,5–1 GB / ngày → ~90–180 GB | 180 ngày | 4 / partition ngày |
| MongoDB `friendships` | 25–50 tỉ document | ~6–12 TB (kèm index) | vĩnh viễn | shard theo `user_id` |

- Luồng ghi (200 event / s) nhẹ với Kafka, event-gateway, Routine Load.
- DWD chiếm phần lớn dung lượng StarRocks (×3 bản sao ≈ 1,5–4,5 TB): BE cần NVMe và RAM lớn (bảng Primary Key giữ index cho mọi dòng).
- `dws_friend_summary` dùng task cập nhật tăng dần (mục 6.2): mỗi 10 phút chỉ ~120k user thay đổi (~120M dòng DWD), thay vì quét 25–50 tỉ dòng.
- Truy vấn theo 1 user (Q1–Q6) đọc 1 bucket, vài ms – vài chục ms.
