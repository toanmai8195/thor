# event-gateway: setup và chạy thử

Service Go nhận event qua HTTP, kiểm tra đúng contract rồi gửi Kafka topic `friend_events` cho StarRocks. Thiết kế và API: [README gốc, mục 11](../../../README.md#11-event-gateway-go).

```
friend-service ──POST /v1/friend-events──► event-gateway ──► Kafka friend_events ──► StarRocks
```

Mọi lệnh `bazel` chạy từ thư mục gốc repo (`thor`).

## Build image

```bash
bazel run --config=linux-arm64 //com/tm/event-gateway:event_gateway_docker   # Mac Apple Silicon
bazel run --config=linux-amd64 //com/tm/event-gateway:event_gateway_docker   # server x86
docker image ls com.tm.go.event_gateway                                       # v1.0.0
```

## Chạy cùng stack

Gateway nằm trong `com/tm/infra/docker-compose.yml`, cổng `8080`. Cách chạy toàn bộ stack xem [`com/tm/friend-service/README.md`](../friend-service/README.md).

```bash
cd com/tm/infra
docker compose up -d event-gateway          # tự start kafka trước
docker compose logs -f event-gateway        # chờ "kafka producer connected" và "http listening"
```

## Gửi thử event

```bash
curl -s localhost:8080/healthz              # {"ok":true}

curl -s -X POST localhost:8080/v1/friend-events -H 'content-type: application/json' -d '{
  "events": [
    {"user_id": 900, "friend_id": 901, "event_type": "REQUESTED", "event_time": "2026-09-23 10:00:00.000", "event_id": "0228440659025985536", "source": "manual"},
    {"user_id": 901, "friend_id": 900, "event_type": "REVIEWED",  "event_time": "2026-09-23 10:00:00.000", "event_id": "0228440659025985537", "source": "manual"}
  ]
}'
# {"accepted":2}

# event sai contract → 400, không gửi event nào
curl -s -X POST localhost:8080/v1/friend-events -H 'content-type: application/json' \
  -d '{"events":[{"user_id":1,"friend_id":2,"event_type":"HUG","event_time":"2026-09-23 10:00:00.000","event_id":"0228440659025985536"}]}'
# {"error":"INVALID_EVENT","message":"events[0]: event_type không hợp lệ: \"HUG\""}
```

Xem event trên Kafka (trong `com/tm/infra`):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic friend_events --from-beginning --property print.key=true
```

## Chạy trên máy (không qua container)

```bash
cd com/tm/infra && docker compose up -d kafka && cd -
bazel run //com/tm/event-gateway:event_gateway      # mặc định Kafka localhost:29092, cổng 8080
```

Nhớ dừng container `event-gateway` trước (`docker compose stop event-gateway`) để trống cổng 8080.

## Test

```bash
bazel test //com/tm/event-gateway/...
# hoặc
go test ./com/tm/event-gateway/...
```

Sửa import Go xong thì chạy `bazel run //:gazelle` để cập nhật `BUILD.bazel`.
