// Package relay kiểm tra event đọc từ topic đầu vào rồi chuyển tiếp:
// hợp lệ → topic friend_events (StarRocks đọc), sai contract → topic DLQ.
package relay

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"time"

	"thor/com/tm/event-gateway/internal/event"
	"thor/com/tm/event-gateway/internal/producer"
)

// Header gắn vào message DLQ
const (
	HeaderError           = "x-error"
	HeaderSourceTopic     = "x-source-topic"
	HeaderSourcePartition = "x-source-partition"
	HeaderSourceOffset    = "x-source-offset"
)

// Source: vị trí message gốc, ghi vào header DLQ để truy vết
type Source struct {
	Topic     string
	Partition int32
	Offset    int64
}

type Relay struct {
	producer    producer.Producer
	friendTopic string
	dlqTopic    string
	log         *slog.Logger
	// Thời gian chờ giữa các lần gửi lại khi Kafka lỗi
	retryBackoff    time.Duration
	maxRetryBackoff time.Duration
}

func New(p producer.Producer, friendTopic, dlqTopic string, log *slog.Logger) *Relay {
	return &Relay{
		producer:        p,
		friendTopic:     friendTopic,
		dlqTopic:        dlqTopic,
		log:             log,
		retryBackoff:    200 * time.Millisecond,
		maxRetryBackoff: 10 * time.Second,
	}
}

// Route quyết định message ra: event hợp lệ đi friend_events (key = user_id),
// event sai đi DLQ nguyên văn kèm lý do lỗi.
func (r *Relay) Route(key, value []byte, src Source) producer.Message {
	e, err := event.Decode(value)
	if err != nil {
		return producer.Message{
			Topic: r.dlqTopic,
			Key:   string(key),
			Value: value,
			Headers: map[string]string{
				HeaderError:           err.Error(),
				HeaderSourceTopic:     src.Topic,
				HeaderSourcePartition: strconv.FormatInt(int64(src.Partition), 10),
				HeaderSourceOffset:    strconv.FormatInt(src.Offset, 10),
			},
		}
	}
	// Ghi lại JSON chuẩn (thứ tự field cố định), key lấy từ user_id chứ không tin key đầu vào
	out, _ := json.Marshal(e)
	return producer.Message{Topic: r.friendTopic, Key: e.Key(), Value: out}
}

// Handle chuyển tiếp 1 message. Kafka lỗi thì gửi lại (backoff tăng dần) tới khi thành công
// hoặc ctx bị huỷ (rebalance / shutdown) — giữ thứ tự trong partition, không bỏ event.
func (r *Relay) Handle(ctx context.Context, key, value []byte, src Source) error {
	msg := r.Route(key, value, src)
	if msg.Topic == r.dlqTopic {
		r.log.Warn("event sai contract → DLQ", "err", msg.Headers[HeaderError], "topic", src.Topic, "partition", src.Partition, "offset", src.Offset)
	}
	backoff := r.retryBackoff
	for {
		err := r.producer.Send(ctx, msg)
		if err == nil {
			return nil
		}
		r.log.Error("kafka send failed, thử lại", "err", err, "topic", msg.Topic, "backoff", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, r.maxRetryBackoff)
	}
}
