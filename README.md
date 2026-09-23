# Friend Network Tracking trên StarRocks

Theo dõi quan hệ bạn bè giữa các user từ luồng event, trả lời các câu hỏi:

- User có bao nhiêu bạn? Là những ai?
- User đang block ai / đang bị ai block? Số lượng?
- Lời mời đã gửi / đã nhận đang chờ?
- Tại một thời điểm trong quá khứ, các câu trả lời trên là gì?

Nguyên tắc cốt lõi: **mỗi cặp có hướng (`user_id → friend_id`) chỉ có đúng 1 status tại 1 thời điểm**, là event mới nhất theo `event_time`.

---

## 1. Kiến trúc

```
client ──► friend-service (TypeScript) ──► MongoDB (friendships)
                    │ commit xong: HTTP POST /v1/friend-events
                    ▼
           event-gateway (Go): kiểm tra contract, gửi Kafka
                    │
                    ▼
           Kafka (friend_events) ──┬─ Routine Load ──► ODS  ods_friend_event      (lịch sử, append)
                                   └─ Routine Load ──► DWD  dwd_friend_status     (trạng thái hiện tại, upsert)
                                                             └─ Async MV ──► DWS  dws_friend_summary (số liệu theo user)
                                                                  └─ Task hằng ngày ──► dws_friend_daily (tuỳ chọn)
```

| Layer | Bảng | 1 dòng là | Kiểu bảng | Cập nhật | Dùng cho |
|---|---|---|---|---|---|
| Service | MongoDB `friendships` | 1 cặp có hướng | collection | transaction, ghi 2 chiều | API online, nguồn sự thật (mục 10) |
| ODS | `ods_friend_event` | 1 event | Duplicate Key | append, **giữ 30 ngày** | audit, truy vấn as-of trong 30 ngày |
| DWD | `dwd_friend_status` | 1 cặp có hướng | Primary Key | upsert theo `event_time` | danh sách hiện tại |
| DWS | `dws_friend_summary` | 1 user | Async MV | refresh 10 phút | số lượng hiện tại |
| DWS | `dws_friend_daily` | 1 user / 1 ngày | Duplicate Key | task hằng ngày | xu hướng (tuỳ chọn) |

ODS và DWD **cùng đọc song song từ Kafka**, DWD không đọc từ ODS.

`friend-service` là nguồn sự thật cho API online; StarRocks là bản sao phục vụ phân tích (trễ vài giây ở DWD, ≤ 10 phút ở DWS). Chỉ `event-gateway` ghi vào Kafka (mục 11).

---

## 2. Event contract (phía service)

> Đã hiện thực: `com/tm/friend-service/` sinh event (mục 10), `com/tm/event-gateway/` kiểm tra contract và gửi Kafka (mục 11).

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

> Số bucket (`32`) là giá trị khởi đầu, chỉnh sao cho mỗi tablet khoảng 1–10 GB.
> Tất cả bảng cùng `HASH(user_id)`, cùng số bucket, cùng `colocate_with` để join không shuffle.

```sql
CREATE DATABASE IF NOT EXISTS social;
USE social;
```

### 3.1 ODS: `ods_friend_event`

```sql
CREATE TABLE IF NOT EXISTS ods_friend_event (
    user_id      BIGINT       NOT NULL,
    friend_id    BIGINT       NOT NULL,
    event_time   DATETIME(3)  NOT NULL COMMENT 'thời điểm hành động, do service sinh',
    event_type   VARCHAR(16)  NOT NULL COMMENT 'REQUESTED/REVIEWED/FRIEND/CANCEL/UNFRIEND/BLOCKING/BLOCKED',
    event_id     VARCHAR(64)  NOT NULL COMMENT 'tăng dần, tie-break khi trùng event_time',
    source       VARCHAR(32)  NULL,
    ingest_time  DATETIME     NULL     COMMENT 'thời điểm vào StarRocks'
)
DUPLICATE KEY(user_id, friend_id, event_time)
PARTITION BY date_trunc('day', event_time)
DISTRIBUTED BY HASH(user_id) BUCKETS 32
PROPERTIES (
    "replication_num" = "3",
    "colocate_with"   = "grp_user",
    "partition_live_number" = "30"   -- TTL: chỉ giữ 30 partition ngày gần nhất
);
```

> **TTL 30 ngày**: mỗi partition là 1 ngày, StarRocks tự drop partition cũ, chỉ giữ 30 partition mới nhất.
> `partition_live_number` đếm số partition chứ không đếm ngày lịch: ngày không có event thì không có partition, nên dữ liệu thực tế có thể cũ hơn 30 ngày một chút.
> DWD và DWS **không bị ảnh hưởng** bởi TTL này: trạng thái hiện tại của mọi cặp vẫn được giữ vĩnh viễn trong `dwd_friend_status`.

### 3.2 DWD: `dwd_friend_status`

```sql
CREATE TABLE IF NOT EXISTS dwd_friend_status (
    user_id          BIGINT       NOT NULL,
    friend_id        BIGINT       NOT NULL,
    status           VARCHAR(16)  NOT NULL COMMENT 'event_type mới nhất của cặp',
    last_event_time  DATETIME(3)  NOT NULL COMMENT 'cột so sánh của merge_condition',
    last_event_id    VARCHAR(64)  NOT NULL,
    updated_at       DATETIME     NULL
)
PRIMARY KEY(user_id, friend_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 32
ORDER BY (user_id, status)
PROPERTIES (
    "replication_num"         = "3",
    "enable_persistent_index" = "true",
    "colocate_with"           = "grp_user"
);
```

### 3.3 DWS: `dws_friend_summary`

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS dws_friend_summary
DISTRIBUTED BY HASH(user_id) BUCKETS 32
REFRESH ASYNC EVERY (INTERVAL 10 MINUTE)
AS
SELECT user_id,
       SUM(IF(status = 'FRIEND',    1, 0)) AS friend_cnt,
       SUM(IF(status = 'REQUESTED', 1, 0)) AS pending_sent_cnt,
       SUM(IF(status = 'REVIEWED',  1, 0)) AS pending_received_cnt,
       SUM(IF(status = 'BLOCKING',  1, 0)) AS blocking_cnt,
       SUM(IF(status = 'BLOCKED',   1, 0)) AS blocked_cnt,
       MAX(last_event_time)                AS last_activity
FROM dwd_friend_status
GROUP BY user_id;
```

### 3.4 (Tuỳ chọn) DWS theo ngày: `dws_friend_daily`

```sql
CREATE TABLE IF NOT EXISTS dws_friend_daily (
    dt                    DATE         NOT NULL,
    user_id               BIGINT       NOT NULL,
    friend_cnt            BIGINT,
    pending_sent_cnt      BIGINT,
    pending_received_cnt  BIGINT,
    blocking_cnt          BIGINT,
    blocked_cnt           BIGINT,
    last_activity         DATETIME(3)
)
DUPLICATE KEY(dt, user_id)
PARTITION BY dt
DISTRIBUTED BY HASH(user_id) BUCKETS 32
PROPERTIES ("replication_num" = "3");
```

---

## 4. Nạp dữ liệu (Routine Load)

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

### Hiện tại (không cần điều kiện thời gian)

Thời gian đã được xử lý lúc ghi: DWD luôn giữ event mới nhất của mỗi cặp.

```sql
-- Q1. Số liệu tổng hợp của user (trễ tối đa = chu kỳ refresh MV)
SELECT * FROM dws_friend_summary WHERE user_id = ?;

-- Q2. Danh sách bạn
SELECT friend_id, last_event_time AS friend_since
FROM dwd_friend_status
WHERE user_id = ? AND status = 'FRIEND';

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

-- Q6. Bạn chung của A và B
SELECT a.friend_id
FROM dwd_friend_status a
JOIN dwd_friend_status b ON a.friend_id = b.friend_id
WHERE a.user_id = ? AND b.user_id = ?
  AND a.status = 'FRIEND' AND b.status = 'FRIEND';

-- Q7. Số bạn chính xác tuyệt đối (không chờ MV refresh)
SELECT COUNT(*) FROM dwd_friend_status WHERE user_id = ? AND status = 'FRIEND';
```

### Tại một thời điểm trong quá khứ (đọc ODS)

> Chỉ dùng được trong **30 ngày gần nhất** (TTL của ODS). Cũ hơn thì dùng `dws_friend_daily` (Q10), chỉ có số lượng theo ngày, không có danh sách.

```sql
-- Q8. Danh sách bạn của user tại thời điểm :ts (:ts trong 30 ngày gần nhất)
SELECT friend_id
FROM (
    SELECT friend_id, event_type,
           ROW_NUMBER() OVER (PARTITION BY friend_id
                              ORDER BY event_time DESC, event_id DESC) AS rn
    FROM ods_friend_event
    WHERE user_id = ? AND event_time <= :ts
) t
WHERE rn = 1 AND event_type = 'FRIEND';
-- Đổi SELECT friend_id → SELECT COUNT(*) để lấy số lượng,
-- đổi 'FRIEND' → 'BLOCKING' / 'BLOCKED' cho câu hỏi block.

-- Q9. Lịch sử quan hệ giữa 2 user trong 30 ngày gần nhất
SELECT event_time, event_type, event_id
FROM ods_friend_event
WHERE user_id = ? AND friend_id = ?
ORDER BY event_time, event_id;

-- Q10. Xu hướng số bạn theo ngày, không giới hạn 30 ngày (cần bảng dws_friend_daily)
SELECT dt, friend_cnt FROM dws_friend_daily
WHERE user_id = ? AND dt BETWEEN ? AND ?
ORDER BY dt;
```

---

## 6. Job ngoài StarRocks

Routine Load, MV refresh, tạo partition, compaction đều do StarRocks tự chạy. Chỉ cần thêm:

### 6.1 Monitor Routine Load (bắt buộc)

Routine Load bị `PAUSED` khi vượt `max_error_number` và **không tự resume**. Job (Java + JDBC, cron 1–5 phút):

```sql
SHOW ROUTINE LOAD FROM social;              -- đọc cột State, ReasonOfStateChanged, ErrorLogUrls
RESUME ROUTINE LOAD FOR social.rl_friend_dwd; -- sau khi alert / xử lý nguyên nhân
```

Theo dõi thêm độ trễ offset Kafka (consumer lag của group `sr_friend_ods`, `sr_friend_dwd`).

### 6.2 Monitor MV refresh

```sql
SELECT task_name, state, error_message, create_time, finish_time
FROM information_schema.task_runs
WHERE definition LIKE '%dws_friend_summary%'
ORDER BY create_time DESC
LIMIT 20;
```

### 6.3 (Tuỳ chọn) Snapshot hằng ngày

StarRocks 3.3+ có task scheduler built-in:

```sql
SUBMIT TASK t_friend_daily_snapshot
SCHEDULE START ('2026-09-24 00:05:00') EVERY (INTERVAL 1 DAY)
AS
INSERT INTO dws_friend_daily
SELECT CURRENT_DATE() - INTERVAL 1 DAY, user_id,
       friend_cnt, pending_sent_cnt, pending_received_cnt,
       blocking_cnt, blocked_cnt, last_activity
FROM dws_friend_summary;
```

Bản cũ hơn: chạy câu `INSERT` này từ Java scheduler (Spring `@Scheduled` / Quartz / cron). Nếu chạy nhiều instance, dùng distributed lock (vd ShedLock).

Kết nối JDBC: `jdbc:mysql://<fe-host>:9030/social?socketTimeout=3600000` (MySQL Connector/J).

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

| Sau T | A→B | A→C | Q2 (bạn của A) | friend_cnt |
|---|---|---|---|---|
| 1 | REQUESTED | – | ∅ | 0 |
| 2 | FRIEND | – | B | 1 |
| 3 | FRIEND | REQUESTED | B | 1 |
| 4 | FRIEND | FRIEND | B, C | 2 |
| 5 | BLOCKING | FRIEND | C | 1 |

Kỳ vọng sau T5:

- `dws_friend_summary` của A: `friend_cnt = 1, blocking_cnt = 1`
- `dws_friend_summary` của B: `friend_cnt = 0, blocked_cnt = 1`
- Q8 với `:ts` = sau T4 → A có 2 bạn (B, C)
- Q8 với `:ts` = sau T5 → A có 1 bạn (C)

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
    ├── event-gateway/            # Go: nhận event qua HTTP → Kafka (mục 11)
    │   ├── BUILD.bazel  main.go  README.md
    │   └── internal/{config,event,handler,producer}/
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
        └── docker-compose.yml    # MongoDB + Kafka + event-gateway + friend-service cho local
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
│   ├── 10_rl_friend_ods.sql
│   ├── 11_rl_friend_dwd.sql
│   ├── 20_task_daily_snapshot.sql
│   └── queries/            # Q1–Q10
├── jobs/                    # Java: monitor routine load, MV, snapshot (nếu < 3.3)
└── test/
    └── scenarios/           # dữ liệu mẫu JSON cho mục 7
```

---

## 9. Vấn đề còn mở (cần chốt trước khi lên prod)

1. **Unblock**: chưa có event mở block. Hiện block chỉ kết thúc khi có event khác cho cặp đó. Nếu có tính năng unblock, cần thêm status (vd `UNBLOCK`) và định nghĩa trạng thái sau khi unblock.
2. ~~**Block 2 chiều**~~: đã chốt **không cho block ngược**. `friend-service` trả `409 BLOCKED_BY_TARGET` khi user đang bị block cố block lại.
3. ~~**Trùng `event_time`**~~: `friend-service` đảm bảo `event_time` của 1 cặp luôn tăng: lấy `max(now, last_event_time + 1ms)` trong transaction, kể cả khi đồng hồ các instance lệch nhau.
4. **Số bucket / replication**: ước lượng theo số cặp thực tế trước khi tạo bảng (đổi bucket sau phải tạo lại bảng).
5. ~~**TTL ODS**~~: đã chốt **30 ngày**. Truy vấn as-of (Q8, Q9) chỉ trong 30 ngày; nếu cần xu hướng dài hơn thì phải bật `dws_friend_daily` (mục 3.4, 6.3).
6. **Xác thực**: API nhận `userId` trên path, chưa kiểm tra người gọi có đúng là user đó. Cần gắn auth (JWT / gateway) trước khi mở ra ngoài.
7. **Unblock** (liên quan #1): service chưa có API unblock vì chưa định nghĩa status.
8. **Q8 bỏ sót cặp cũ**: ODS chỉ giữ 30 ngày, nên cặp có event cuối cùng cũ hơn 30 ngày (vd là bạn từ 2 tháng trước, không đổi gì) không còn dòng nào trong ODS → Q8 không thấy, kể cả khi `:ts` nằm trong 30 ngày. Cách xử lý: snapshot `dwd_friend_status` định kỳ làm mốc, hoặc chấp nhận giới hạn.
9. **Mất event khi gateway / Kafka lỗi**: friend-service gửi event sang event-gateway ngay sau khi commit MongoDB, không có outbox. Gateway hoặc Kafka lỗi lúc đó (sau 3 lần thử) thì hành động vẫn lưu nhưng event bị mất, StarRocks lệch với MongoDB cho cặp đó tới event kế tiếp. Log `event publish failed after db commit` có đủ nội dung event để gửi bù bằng tay.

---

## 10. Friend service (Node.js + MongoDB)

Service nhận hành động của user qua REST API, lưu trạng thái trong MongoDB và gửi event (contract mục 2) sang event-gateway (mục 11) để lên Kafka. Code ở `com/tm/friend-service/`, Docker ở `com/tm/infra/`.

### 10.1 Công nghệ

| Thành phần | Chọn | Ghi chú |
|---|---|---|
| Ngôn ngữ | TypeScript 5.9 (`strict`, ESM `nodenext`) | Bazel typecheck lúc build |
| Runtime | Node.js 22 | |
| HTTP | Express 5 | lỗi async tự đi vào error handler |
| DI | awilix 12 | không dùng decorator; mỗi layer 1 file `*-module.ts` |
| DB | MongoDB 7, **replica set** | cần replica set để dùng transaction (local: 1 node) |
| Gửi event | `fetch` (Node built-in) → event-gateway | timeout 3 s, tối đa 3 lần khi lỗi mạng / 5xx |
| Test | `node:test` | chạy bằng Bazel (`js_test`) hoặc `tsx` khi dev |
| Package manager | pnpm 9 | `pnpm-lock.yaml` là nguồn cho Bazel |
| Build / image | Bazel 8.7: `aspect_rules_ts` 3.10.1 (tsc), `aspect_rules_js` 3.4.1, `rules_nodejs` (Node 22.22.3), `rules_oci` | macro `ts_layer`, `com_tm_js_image`; image trên `debian:bookworm-slim` |

### 10.2 Cấu trúc code theo layer

Mỗi layer là 1 package Bazel (`ts_layer` = `ts_project` với tsconfig chung, `com/tm/friend-service/ts.bzl`). `visibility` của Bazel chặn phụ thuộc sai chiều: vd Controller khai báo dep vào Router sẽ báo lỗi `is not visible` lúc build.

```
MainServer (src/server.ts, container.ts, http-server.ts)
   │ dựng DI container, start/stop lifecycle
   ▼
router ─► handler ─► controller ─► dao ─► MongoDB / event-gateway
                                          utils: dùng chung; configs: chỉ MainServer đọc
```

| Layer | Vị trí | Việc | Không được |
|---|---|---|---|
| MainServer | `src/server.ts`, `src/container.ts`, `src/http-server.ts` | đọc config, dựng container từ module các layer, start lifecycle theo thứ tự, shutdown | chứa logic, tự tạo MongoDB / client gateway |
| Router | `src/router/` | map method + path → handler | parse request, gọi controller |
| Handler | `src/handler/` | parse/validate tham số HTTP, gọi Controller, ghi response, map lỗi → HTTP status, health check | chứa luật nghiệp vụ, đụng DB |
| Controller | `src/controller/` | luật chuyển trạng thái (`plan()`), transaction ghi 2 chiều, gửi event sang gateway sau commit, truy vấn | biết về HTTP |
| Dao | `src/dao/` | MongoDB (`friendships`, transaction, index) và client HTTP gọi event-gateway | chứa luật nghiệp vụ |
| Utils | `src/utils/` | `DomainError`, snowflake `event_id`, format thời gian, logger, `Lifecycle` | phụ thuộc layer khác |
| Configs | `src/configs/` | đọc biến môi trường (10.7) | được layer khác đọc trực tiếp |

#### Dependency injection (awilix)

Mỗi layer có 1 file `*-module.ts` đăng ký thành phần của nó vào container (giống module Dagger):

| Module | Đăng ký | Lifecycle |
|---|---|---|
| `utils/utils-module.ts` | `log`, `nextId` | |
| `dao/dao-module.ts` | `mongoClient`, `db`, `runInTransaction`, `friendshipDao`, `eventPublisher`, `pingDb` | `mongoLifecycle`: connect + tạo index / close |
| `controller/controller-module.ts` | `friendController` | |
| `handler/handler-module.ts` | `friendHandler`, `healthHandler`, `errorHandler` | |
| `router/router-module.ts` | `router` | |
| `http-server.ts` | `app` (Express) | `httpLifecycle`: listen / close |

- Mọi thành phần là singleton; factory lấy dependency bằng destructuring (`InjectionMode.PROXY`), vd `({ db }) => new FriendshipDao(db)`.
- Class nghiệp vụ (`FriendController`, `FriendshipDao`...) không import awilix, nhận dependency qua constructor → test dùng Dao giả trực tiếp (`test/controller.test.ts`).
- Mỗi module khai báo phần config nó cần (`DaoSettings`, `ControllerSettings`...) thay vì phụ thuộc layer Configs.
- `server.ts` start theo `LIFECYCLE_ORDER` (mongo → http), shutdown (SIGINT / SIGTERM) dừng ngược lại rồi `container.dispose()`.
- `test/container.test.ts` resolve mọi registration mà không kết nối thật: thiếu / sai tên dependency là test fail.

Thêm thành phần mới: viết class / hàm như bình thường → đăng ký trong `*-module.ts` của layer đó → thêm kiểu vào `*Cradle` của module; nếu cần start/stop thì đăng ký `Lifecycle` và thêm tên vào `LIFECYCLE_ORDER`.

### 10.3 Luồng ghi (1 hành động)

```
POST /v1/users/1/requests/2
  1. MongoDB transaction (snapshot, w=majority)
       a. đọc 2 document: 1→2 và 2→1
       b. kiểm tra luật chuyển trạng thái (10.5)
       c. upsert 2 document friendships (2 chiều, cùng event_time)
  2. commit thành công → POST 2 event sang event-gateway /v1/friend-events (1 request)
     gateway gửi Kafka topic friend_events, key = user_id, trả 200 khi Kafka đã ghi
  3. trả 200/201
```

- Transaction lỗi hoặc vi phạm luật → không gửi event nào.
- Gateway / Kafka lỗi sau khi đã commit (timeout, lỗi mạng hoặc 5xx; thử tối đa 3 lần): service vẫn trả thành công vì hành động đã lưu, và log `event publish failed after db commit` kèm nội dung 2 event. Gateway trả 4xx (event sai contract) thì không thử lại. **Event đó không tới được DW** (xem vấn đề mở ở mục 9).
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

Truy vấn quá khứ (Q8–Q10) chỉ có ở StarRocks.

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
| `EVENT_GATEWAY_URL` | `http://localhost:8080` | base URL của event-gateway |
| `EVENT_GATEWAY_TIMEOUT_MS` | `3000` | timeout mỗi lần gọi |
| `EVENT_GATEWAY_MAX_ATTEMPTS` | `3` | số lần gửi tối đa khi lỗi mạng / timeout / 5xx |
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
| `//com/tm/friend-service/test:<tên>_test` | `js_test` cho mỗi `test/*.test.ts`: `container`, `controller`, `event_gateway_client`, `friendship_rules`, `params`, `utils` |

```bash
# test
bazel test //com/tm/friend-service/test/...

# chạy local (cần MongoDB + event-gateway, xem bên dưới)
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

Chạy `bazel run ...:friend_service` trên máy thay cho container: `docker compose up -d mongo kafka event-gateway` (không start `friend-service` để trống port 3000).

Kafka từ máy host ở `localhost:29092`; StarRocks Routine Load trong cùng network dùng `kafka:9092`.

Push image lên registry: truyền `repository = "<registry>/<repo>"` vào `com_tm_js_image` → sinh thêm `:friend_service_push`.

Thêm / đổi npm package: sửa `package.json` → `pnpm install` (cập nhật `pnpm-lock.yaml`) → thêm `//com/tm/friend-service:node_modules/<package>` vào `deps` của layer dùng nó (vd `src/dao/BUILD.bazel`); package không kèm type thì thêm cả `@types/<package>`. Phiên bản TypeScript của Bazel lấy từ `devDependencies.typescript` (phải là phiên bản cụ thể). File `.ts` mới trong 1 layer tự vào build (`glob`). Package có build script phải khai báo trong `pnpm.onlyBuiltDependencies` của `package.json`.

Dev không qua Bazel (trong `com/tm/friend-service`): `pnpm dev` (tsx watch), `pnpm test`, `pnpm typecheck`, `pnpm build && pnpm start` (ra `dist/`).

---

## 11. Event gateway (Go)

Service Go đứng giữa friend-service và Kafka: nhận event qua HTTP, kiểm tra đúng contract mục 2.3, gửi lên topic `friend_events` cho StarRocks Routine Load. Code ở `com/tm/event-gateway/`, hướng dẫn chạy ở [`com/tm/event-gateway/README.md`](com/tm/event-gateway/README.md).

```
friend-service ──POST /v1/friend-events──► event-gateway ──► Kafka friend_events ──► StarRocks (ODS, DWD)
```

Lý do tách: chỉ 1 nơi ghi Kafka và kiểm tra contract; service khác (sau này) gửi event cùng một cách, không cần Kafka client.

### 11.1 Công nghệ

| Thành phần | Chọn | Ghi chú |
|---|---|---|
| Ngôn ngữ | Go 1.22 | `net/http` (route theo method), `log/slog` JSON |
| Kafka client | IBM/sarama `SyncProducer` | idempotent, `acks=all`, retry 5; trả về khi Kafka đã ghi |
| Build / image | Bazel `rules_go` + gazelle, macro `com_tm_go_image` | binary static, image trên `gcr.io/distroless/base` |

### 11.2 Cấu trúc code

| Package | Việc |
|---|---|
| `main.go` | đọc config, kết nối Kafka (thử lại tới 60 s khi Kafka chưa sẵn sàng), start HTTP, shutdown khi SIGINT / SIGTERM |
| `internal/config` | biến môi trường |
| `internal/event` | `FriendEvent` + `Validate()` theo contract, `Key()` = `user_id` |
| `internal/handler` | HTTP handler |
| `internal/producer` | interface `Producer` + bản sarama |

### 11.3 API

**`POST /v1/friend-events`**

```json
{
  "events": [
    {"user_id": 1, "friend_id": 2, "event_type": "REQUESTED", "event_time": "2026-09-23 10:00:00.000", "event_id": "0228440659025985536", "source": "friend-service"},
    {"user_id": 2, "friend_id": 1, "event_type": "REVIEWED",  "event_time": "2026-09-23 10:00:00.000", "event_id": "0228440659025985537", "source": "friend-service"}
  ]
}
```

| Status | Khi nào | Body |
|---|---|---|
| `200` | mọi event đã lên Kafka | `{"accepted": 2}` |
| `400` | JSON hỏng / có field lạ (`INVALID_JSON`), `events` rỗng (`EMPTY_EVENTS`), > 1000 event (`TOO_MANY_EVENTS`), event sai contract (`INVALID_EVENT`, message chỉ ra `events[i]`) | `{"error", "message"}` |
| `503` | Kafka lỗi (`KAFKA_UNAVAILABLE`), client nên thử lại | `{"error", "message"}` |

Có 1 event sai thì **không gửi event nào** trong request đó. Body tối đa 1 MiB.

Kiểm tra contract: `user_id`, `friend_id` trong 1..2^53−1 và khác nhau; `event_type` 1 trong 7 status (chữ hoa); `event_time` đúng `yyyy-MM-dd HH:mm:ss.SSS`; `event_id` đúng 19 chữ số; `source` tuỳ chọn.

**`GET /healthz`** → `{"ok": true}`.

### 11.4 Cấu hình

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `PORT` | `8080` | cổng HTTP |
| `KAFKA_BROKERS` | `localhost:29092` | cách nhau bởi dấu phẩy |
| `KAFKA_FRIEND_TOPIC` | `friend_events` | topic nhận friend event |
| `KAFKA_CLIENT_ID` | `event-gateway` | |

### 11.5 Build và test

```bash
bazel test //com/tm/event-gateway/...
bazel run //com/tm/event-gateway:event_gateway                                   # chạy local
bazel run --config=linux-arm64 //com/tm/event-gateway:event_gateway_docker       # image com.tm.go.event_gateway:v1.0.0
bazel run //:gazelle                                                             # sau khi đổi import Go
```

Partition: sarama dùng hash FNV-1a trên key, khác murmur2 của client Java / kafkajs. Không ảnh hưởng vì chỉ gateway ghi topic này; nếu sau này có producer khác cùng ghi `friend_events` thì phải thống nhất partitioner.
