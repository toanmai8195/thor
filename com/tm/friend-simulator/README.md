# friend-simulator: sinh tải giả cho friend-service

Service Go gọi REST API của friend-service với hành động giả (mời, chấp nhận, từ chối, huỷ, huỷ kết bạn, block), mặc định **10 request/s**, để có event chạy qua toàn bộ luồng:

```
friend-simulator ──HTTP──► friend-service ──► Kafka ──► event-gateway ──► Kafka ──► StarRocks
```

## Cách chọn hành động

- Giữ trạng thái các cặp user trong bộ nhớ và chỉ chọn hành động hợp lệ theo luật của friend-service (README gốc 10.5), nên phần lớn request thành công.
- Tỉ lệ: mời 40%, chấp nhận 25%, từ chối 5%, huỷ lời mời 5%, huỷ kết bạn 15%, block 10%. Chưa có cặp phù hợp (vd chưa có lời mời để chấp nhận) thì gửi lời mời.
- Mỗi cặp chỉ có 1 request chạy cùng lúc.
- Bị `409` / `403` (trạng thái lệch, vd có người khác thao tác cùng cặp): gọi `GET /relationships` để lấy lại trạng thái đúng.
- Tập user riêng: `USER_ID_START` .. `USER_ID_START + USERS - 1` (mặc định 100000..100999), không đụng dữ liệu thử tay.
- Trạng thái chỉ nằm trong bộ nhớ: restart thì bắt đầu lại từ rỗng, các cặp cũ sẽ được đồng bộ dần qua `409`.

## Chạy

Mọi lệnh `bazel` chạy từ thư mục gốc repo. Stack (friend-service, Kafka, StarRocks...) chạy theo [`com/tm/friend-service/README.md`](../friend-service/README.md).

```bash
bazel run --config=linux-arm64 //com/tm/friend-simulator:friend_simulator_docker

cd com/tm/infra
docker compose --profile simulator up -d friend-simulator   # bắt đầu gửi
docker compose logs -f friend-simulator                     # thống kê mỗi 10 s
curl -s localhost:8090/stats                                # thống kê tích luỹ (JSON)
docker compose stop friend-simulator                        # dừng
```

Chạy trên máy (không qua container): `bazel run //com/tm/friend-simulator:friend_simulator` (mặc định gọi `http://localhost:3000`).

## Cấu hình

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `FRIEND_SERVICE_URL` | `http://localhost:3000` | trong compose: `http://friend-service:3000` |
| `RPS` | `10` | số request / giây |
| `CONCURRENCY` | `8` | số request chạy song song tối đa; hết slot thì bỏ lượt (`skipped`) để giữ đúng nhịp |
| `USERS` | `1000` | số user giả lập |
| `USER_ID_START` | `100000` | user id đầu tiên |
| `SEED` | `0` | seed random; 0 = theo thời gian, đặt cố định để lặp lại kịch bản |
| `DURATION` | `0` | thời gian chạy (vd `10m`); 0 = chạy tới khi dừng |
| `STATS_INTERVAL` | `10s` | chu kỳ in thống kê |
| `PORT` | `8090` | cổng `/healthz`, `/stats` |

Đổi trong `com/tm/infra/docker-compose.yml` (service `friend-simulator`) rồi `docker compose --profile simulator up -d friend-simulator`.

## `/stats`

```json
{
  "requests": 700, "actual_rps": 10, "avg_latency_ms": 12.3, "skipped": 0,
  "by_outcome": {"ok": 690, "conflict": 10},
  "by_action": {"request": {"ok": 290}, "accept": {"ok": 170}, "...": {}},
  "pairs": {"pending": 80, "friends": 120, "blocked": 60, "in_flight": 1}
}
```

- `ok`: 2xx. `conflict`: 409 / 403 đã đồng bộ lại. `error`: lỗi mạng hoặc status khác (xem log `request failed`).
- `pairs`: số cặp simulator đang biết theo loại.

## Kiểm tra dữ liệu tới StarRocks

Sau vài giây (trong `com/tm/infra`):

```bash
docker compose exec starrocks mysql -h 127.0.0.1 -P 9030 -uroot social -e "
  SELECT status, COUNT(*) FROM dwd_friend_status WHERE user_id >= 100000 GROUP BY status;"
docker compose exec mongo mongosh friend_network --quiet --eval 'db.friendships.countDocuments({user_id: {$gte: 100000}})'
```

Số dòng DWD phải bằng số document MongoDB của cùng tập user.

## Test

```bash
bazel test //com/tm/friend-simulator/...
```
