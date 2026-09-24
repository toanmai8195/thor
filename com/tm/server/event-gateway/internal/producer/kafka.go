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
	Topic   string
	Key     string
	Value   []byte
	Headers map[string]string
}

// Producer gửi 1 message, trả về khi Kafka đã ghi xong (acks=all).
type Producer interface {
	Send(ctx context.Context, msg Message) error
	Close() error
}

type kafkaProducer struct {
	p sarama.SyncProducer
}

// NewConfig: cấu hình sarama dùng chung cho producer và consumer group.
func NewConfig(clientID string) *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.ClientID = clientID
	cfg.Version = sarama.V2_8_0_0
	return cfg
}

// NewKafka tạo producer idempotent, acks=all. Kafka vừa khởi động có thể chưa sẵn sàng
// cấp producer id (coordinator đang load) → thử lại tới khi ctx hết hạn.
func NewKafka(ctx context.Context, brokers []string, clientID string, log *slog.Logger) (Producer, error) {
	cfg := NewConfig(clientID)
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

func (k *kafkaProducer) Send(_ context.Context, m Message) error {
	msg := &sarama.ProducerMessage{
		Topic: m.Topic,
		Key:   sarama.StringEncoder(m.Key),
		Value: sarama.ByteEncoder(m.Value),
	}
	for name, v := range m.Headers {
		msg.Headers = append(msg.Headers, sarama.RecordHeader{Key: []byte(name), Value: []byte(v)})
	}
	_, _, err := k.p.SendMessage(msg)
	return err
}

func (k *kafkaProducer) Close() error {
	return k.p.Close()
}
