# event-gateway: setup và chạy thử

Service Go đọc event friend-service ghi vào Kafka, kiểm tra đúng contract rồi chuyển sang topic `friend_events` cho StarRocks; event sai vào `friend_events_dlq`. Thiết kế: [README server, mục 11](../README.md#11-event-gateway-go).

```
friend-service ──► friend_service_events ──► event-gateway ──► friend_events ──► StarRocks
                                                   └─ sai contract ──► friend_events_dlq
```

Mọi lệnh `bazel` chạy từ `com/tm/server` (Bazel module root).

## Build image

```bash
bazel run --config=linux-arm64 //event-gateway:event_gateway_docker   # Mac Apple Silicon
bazel run --config=linux-amd64 //event-gateway:event_gateway_docker   # server x86
docker image ls com.tm.go.event_gateway                                       # v1.0.0
```

## Chạy cùng stack

Gateway nằm trong `infra/docker-compose.yml`. Cách chạy toàn bộ stack xem [`friend-service/README.md`](../friend-service/README.md).

```bash
cd infra
docker compose up -d event-gateway          # tự start kafka trước
docker compose logs -f event-gateway        # chờ "kafka producer connected" và "consumer assigned"
curl -s localhost:8080/healthz              # {"consuming":true,"ok":true}
```

## Gửi thử event (không qua friend-service)

Ghi thẳng vào topic đầu vào, dạng `key|value` (chạy trong `infra`):

```bash
# event hợp lệ → friend_events
echo '900|{"user_id":900,"friend_id":901,"event_type":"REQUESTED","event_time":"2026-09-23 10:00:00.000","event_id":"0228440659025985536","source":"manual"}' \
  | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
      --topic friend_service_events --property parse.key=true --property key.separator='|'

# event sai contract → friend_events_dlq
echo '901|{"user_id":901,"friend_id":900,"event_type":"HUG","event_time":"2026-09-23 10:00:00.000","event_id":"0228440659025985537"}' \
  | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
      --topic friend_service_events --property parse.key=true --property key.separator='|'
```

Xem kết quả:

```bash
for t in friend_events friend_events_dlq; do
  echo "== $t"
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic $t --from-beginning --timeout-ms 5000 --property print.key=true --property print.headers=true
done
```

Message trong DLQ có header `x-error` (lý do), `x-source-topic`, `x-source-partition`, `x-source-offset` (vị trí gốc).

Xem consumer lag:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group event-gateway
```

## Chạy trên máy (không qua container)

```bash
cd infra && docker compose up -d kafka && docker compose stop event-gateway && cd -
bazel run //event-gateway:event_gateway      # mặc định Kafka localhost:29092, health ở cổng 8080
```

## Test

```bash
bazel test //event-gateway/...
# hoặc
go test ./event-gateway/...
```

Sửa import Go xong thì chạy `bazel run //:gazelle` để cập nhật `BUILD.bazel`.
