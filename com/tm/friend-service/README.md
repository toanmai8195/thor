# friend-service: setup và chạy thử

Service Friend Network (TypeScript, Express, MongoDB, Kafka). Event ghi vào Kafka topic `friend_service_events`, [event-gateway](../event-gateway/README.md) (Go) kiểm tra rồi chuyển sang `friend_events` cho StarRocks. Thiết kế, API và schema: [README gốc, mục 10](../../../README.md#10-friend-service-nodejs--mongodb).

```
friend-service ──► Kafka friend_service_events ──► event-gateway ──► Kafka friend_events ──► StarRocks
```

## Cần cài

| Công cụ | Dùng cho |
|---|---|
| Bazelisk (`bazel`) | build, test, đóng image; tự tải Bazel 8.7.0 theo `.bazelversion` |
| Docker + Docker Compose | chạy MongoDB, Kafka, service |
| pnpm 9 (tuỳ chọn) | dev không qua Bazel |

Mọi lệnh `bazel` chạy từ thư mục gốc repo (`thor`).

## 1. Build image và load vào Docker

Cần 2 image: event-gateway (Go) và friend-service.

```bash
bazel run --config=linux-arm64 //com/tm/event-gateway:event_gateway_docker
bazel run --config=linux-arm64 //com/tm/friend-service/src:friend_service_docker
```

Máy Mac Apple Silicon dùng `linux-arm64`, còn server x86 thì đổi thành `--config=linux-amd64`. Chạy xong sẽ có image `com.tm.go.event_gateway:v1.0.0` và `com.tm.js.friend_service:v1.0.0`:

```bash
docker image ls 'com.tm.*'
```

## 2. Chạy stack (MongoDB + Kafka + event-gateway + friend-service)

```bash
cd com/tm/infra
docker compose up -d
docker compose ps                        # cả 4 service phải "Up"; mongo và kafka "healthy"
docker compose logs -f friend-service    # chờ tới dòng "http listening"
```

| Service | Từ máy host | Trong network compose |
|---|---|---|
| friend-service | `localhost:3000` | `friend-service:3000` |
| event-gateway (chỉ `/healthz`) | `localhost:8080` | `event-gateway:8080` |
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

Danh sách đầy đủ các API ở README gốc, mục 10.6.

## 4. Xem event trên Kafka

Chạy trong `com/tm/infra`:

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

## 6. Dừng stack

```bash
docker compose down        # dừng, giữ dữ liệu MongoDB
docker compose down -v     # dừng và xoá luôn dữ liệu
```

## Sửa code rồi chạy lại

```bash
bazel run --config=linux-arm64 //com/tm/friend-service/src:friend_service_docker
cd com/tm/infra && docker compose up -d friend-service    # container dùng image mới
```

Sửa event-gateway thì build lại `//com/tm/event-gateway:event_gateway_docker` rồi `docker compose up -d event-gateway`.

## Chạy service trên máy (không qua container)

Vẫn cần MongoDB, Kafka và event-gateway từ compose, nhưng không start container `friend-service` để cổng 3000 còn trống:

```bash
cd com/tm/infra && docker compose up -d mongo kafka event-gateway && cd -

# Qua Bazel
bazel run //com/tm/friend-service/src:friend_service

# Hoặc dev với pnpm (trong com/tm/friend-service)
pnpm install
pnpm dev          # tsx watch, tự reload khi sửa code
```

Mặc định service kết nối MongoDB `localhost:27017` và Kafka `localhost:29092`. Có thể đổi bằng biến môi trường, xem README gốc mục 10.7.

## Test

```bash
bazel test //com/tm/friend-service/test/...

# hoặc trong com/tm/friend-service
pnpm test
pnpm typecheck
```
