# friend-service: setup và chạy thử

Service Friend Network (TypeScript, Express, MongoDB, Kafka). Event ghi vào Kafka topic `friend_service_events`, [event-gateway](../event-gateway/README.md) (Go) kiểm tra rồi chuyển sang `friend_events` cho StarRocks. Thiết kế, API và schema: [README server, mục 10](../README.md#10-friend-service-nodejs--mongodb).

```
friend-service ──► Kafka friend_service_events ──► event-gateway ──► Kafka friend_events ──► StarRocks
```

## Cần cài

| Công cụ | Dùng cho |
|---|---|
| Bazelisk (`bazel`) | build, test, đóng image; tự tải Bazel 8.7.0 theo `.bazelversion` |
| Docker + Docker Compose | chạy MongoDB, Kafka, service |
| pnpm 9 (tuỳ chọn) | dev không qua Bazel |

Mọi lệnh `bazel` chạy từ `com/tm/server` (Bazel module root).

## 1. Build image và load vào Docker

Cần 2 image: event-gateway (Go) và friend-service.

```bash
bazel run --config=linux-arm64 //event-gateway:event_gateway_docker
bazel run --config=linux-arm64 //friend-service/src:friend_service_docker
```

Máy Mac Apple Silicon dùng `linux-arm64`, còn server x86 thì đổi thành `--config=linux-amd64`. Chạy xong sẽ có image `com.tm.go.event_gateway:v1.0.0` và `com.tm.js.friend_service:v1.0.0`:

```bash
docker image ls 'com.tm.*'
```

## 2. Chạy stack (MongoDB + Kafka + StarRocks + event-gateway + friend-service)

```bash
cd infra
docker compose up -d
docker compose ps -a                     # mongo, kafka, starrocks "healthy"; kafka-init, starrocks-init "Exited (0)"
docker compose logs -f friend-service    # chờ tới dòng "http listening"
```

| Service | Từ máy host | Trong network compose |
|---|---|---|
| friend-service | `localhost:3000` | `friend-service:3000` |
| event-gateway (chỉ `/healthz`) | `localhost:8080` | `event-gateway:8080` |
| StarRocks (MySQL protocol) | `localhost:9030` | `starrocks:9030` |
| StarRocks FE web UI | `localhost:8030` | |
| MongoDB (replica set `rs0`) | `localhost:27017` | `mongo:27017` |
| Kafka | `localhost:29092` | `kafka:9092` |

## 3. Gọi thử API

```bash
curl localhost:3000/healthz                                  # {"ok":true}

curl -X POST localhost:3000/v1/users/1/requests/2            # 1 gửi lời mời cho 2
curl localhost:3000/v1/users/2/requests?type=received        # 2 thấy lời mời đang chờ
curl -X POST localhost:3000/v1/users/2/requests/1/accept     # 2 chấp nhận
curl localhost:3000/v1/users/1/friends                       # danh sách bạn của 1
curl localhost:3000/v1/users/1/summary                       # số lượng theo status
curl -X POST localhost:3000/v1/users/1/blocks/2              # 1 block 2
curl localhost:3000/v1/users/1/relationships/2               # quan hệ 2 chiều
```

Danh sách đầy đủ các API ở README server, mục 10.6.

## 4. Xem event trên Kafka

Chạy trong `infra`:

```bash
# event friend-service ghi (chưa kiểm tra)
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic friend_service_events --from-beginning --property print.key=true

# event đã qua event-gateway (StarRocks đọc topic này)
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic friend_events --from-beginning --property print.key=true
```

Mỗi hành động sinh 2 event đối xứng (vd `1→2 REQUESTED` và `2→1 REVIEWED`).

Có event ở `friend_service_events` mà không có ở `friend_events`: xem `docker compose logs event-gateway` (event sai contract nằm ở `friend_events_dlq`). Không có ở cả 2: tìm dòng `event publish failed after db commit` trong log friend-service.

## 5. Xem dữ liệu MongoDB

```bash
docker compose exec mongo mongosh friend_network --eval 'db.friendships.find().toArray()'
```

## 6. Xem dữ liệu trên StarRocks

Lần đầu StarRocks cần khoảng 30–60 s để khởi động; `starrocks-init` tự tạo database `social`, các bảng, 2 Routine Load đọc `friend_events` và 2 task (file ở `infra/starrocks/`). Event tới StarRocks sau vài giây.

```bash
# mở MySQL client trong container (hoặc DBeaver / mysql trên máy: 127.0.0.1:9030, user root, không mật khẩu)
docker compose exec starrocks mysql -h 127.0.0.1 -P 9030 -uroot social
```

```sql
SHOW ROUTINE LOAD FROM social\G                     -- State phải là RUNNING, xem ErrorRows trong Statistic

-- trạng thái mới nhất của user 1
SELECT status, COUNT(*) FROM dwd_friend_status WHERE user_id = 1 GROUP BY status;   -- số partners mỗi status
SELECT status, friend_id FROM dwd_friend_status WHERE user_id = 1 ORDER BY status;  -- partners là ai

SELECT * FROM ods_friend_event ORDER BY event_time;   -- ODS: mọi event
SELECT * FROM dws_friend_summary ORDER BY user_id;    -- số lượng theo user (task mỗi 1 phút)
SELECT * FROM dws_friend_daily ORDER BY dt, user_id;  -- theo ngày (task mỗi 5 phút)
```

Chu kỳ ở local (prod chậm hơn, xem `friend_network.sql`):

| Bảng | Cập nhật | Giữ |
|---|---|---|
| `ods_friend_event` | Routine Load, vài giây | 180 ngày |
| `dwd_friend_status` | Routine Load, vài giây | vĩnh viễn |
| `dws_friend_summary` | task `t_friend_summary_refresh` mỗi 1 phút (prod 10 phút), chỉ tính lại user có event mới | vĩnh viễn |
| `dws_friend_daily` | task `t_friend_daily_snapshot` mỗi 5 phút (prod 00:05 mỗi ngày), ghi đè partition của ngày (`dt` theo UTC) | 180 ngày |

Xem các lần chạy task:

```sql
SELECT TASK_NAME, STATE, ERROR_MESSAGE, CREATE_TIME, FINISH_TIME
FROM information_schema.task_runs ORDER BY CREATE_TIME DESC LIMIT 10;
```

StarRocks vừa khởi động có thể báo lỗi `task_run_history` ở câu trên trong vài phút đầu; đợi rồi chạy lại.

Các truy vấn Q1–Q10 ở `friend_network.sql` (`com/tm/server`), phần 5.

Routine Load bị `PAUSED` (vd vượt `max_error_number`): xem `ReasonOfStateChanged`, `ErrorLogUrls` trong `SHOW ROUTINE LOAD`, sửa nguyên nhân rồi `RESUME ROUTINE LOAD FOR social.rl_friend_ods;`.

Sửa schema / task local: sửa file trong `infra/starrocks/` rồi `docker compose run --rm starrocks-init` (task luôn được tạo lại theo file; bảng đã có thì giữ nguyên, muốn tạo lại bảng thì `DROP TABLE` trước hoặc `docker compose down -v`).

## 7. Dừng stack

```bash
docker compose down        # dừng, giữ dữ liệu MongoDB / StarRocks
docker compose down -v     # dừng và xoá luôn dữ liệu
```

## Sinh tải giả

Muốn có dữ liệu liên tục mà không gọi tay: [`friend-simulator`](../friend-simulator/README.md) gọi API với 10 request/s.

```bash
bazel run --config=linux-arm64 //friend-simulator:friend_simulator_docker
cd infra && docker compose --profile simulator up -d friend-simulator
```

## Sửa code rồi chạy lại

```bash
bazel run --config=linux-arm64 //friend-service/src:friend_service_docker
cd infra && docker compose up -d friend-service    # container dùng image mới
```

Sửa event-gateway thì build lại `//event-gateway:event_gateway_docker` rồi `docker compose up -d event-gateway`.

## Chạy service trên máy (không qua container)

Vẫn cần MongoDB, Kafka và event-gateway từ compose, nhưng không start container `friend-service` để cổng 3000 còn trống:

```bash
cd infra && docker compose up -d mongo kafka event-gateway && cd -

# Qua Bazel
bazel run //friend-service/src:friend_service

# Hoặc dev với pnpm (trong friend-service)
pnpm install
pnpm dev          # tsx watch, tự reload khi sửa code
```

Mặc định service kết nối MongoDB `localhost:27017` và Kafka `localhost:29092`. Có thể đổi bằng biến môi trường, xem README server mục 10.7.

## Test

```bash
bazel test //friend-service/test/...

# hoặc trong friend-service
pnpm test
pnpm typecheck
```
