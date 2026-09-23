// Package producer gửi message lên Kafka bằng sarama.
package producer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
)

// Message là 1 bản ghi Kafka: key quyết định partition.
type Message struct {
	Key   string
	Value []byte
}

// Producer gửi 1 lô message lên 1 topic, trả về khi Kafka đã ghi xong (acks=all).
type Producer interface {
	Send(ctx context.Context, topic string, msgs []Message) error
	Close() error
}

type kafkaProducer struct {
	p sarama.SyncProducer
}

// NewKafka tạo producer idempotent, acks=all. Kafka vừa khởi động có thể chưa sẵn sàng
// cấp producer id (coordinator đang load) → thử lại tới khi ctx hết hạn.
func NewKafka(ctx context.Context, brokers []string, clientID string, log *slog.Logger) (Producer, error) {
	cfg := sarama.NewConfig()
	cfg.ClientID = clientID
	cfg.Version = sarama.V2_8_0_0
	cfg.Producer.Idempotent = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Return.Successes = true
	cfg.Net.MaxOpenRequests = 1 // bắt buộc khi bật idempotent

	backoff := 500 * time.Millisecond
	for {
		p, err := sarama.NewSyncProducer(brokers, cfg)
		if err == nil {
			return &kafkaProducer{p: p}, nil
		}
		log.Warn("kafka chưa sẵn sàng, thử lại", "err", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("kết nối kafka: %w", err)
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

func (k *kafkaProducer) Send(_ context.Context, topic string, msgs []Message) error {
	out := make([]*sarama.ProducerMessage, len(msgs))
	for i, m := range msgs {
		out[i] = &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(m.Key),
			Value: sarama.ByteEncoder(m.Value),
		}
	}
	return k.p.SendMessages(out)
}

func (k *kafkaProducer) Close() error {
	return k.p.Close()
}
