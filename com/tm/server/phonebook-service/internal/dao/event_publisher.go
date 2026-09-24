package dao

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/IBM/sarama"
)

// Event gửi lên Kafka (PHONEBOOK.md mục 3): 1 message = mảng JSON ≤ 1.000 event của cùng user, key = user_id.
const (
	EventAdd    = "ADD"
	EventDelete = "DELETE"
	// MaxEventsPerMessage: số event tối đa trong 1 message Kafka
	MaxEventsPerMessage = 1000
)

// Event: 1 số thêm / bớt khỏi danh bạ của user.
type Event struct {
	UserID    int64  `json:"user_id"`
	PhoneEnc  uint64 `json:"phone_enc"`
	EventType string `json:"event_type"`
	EventTime string `json:"event_time"`
	SyncID    string `json:"sync_id"`
}

// EventPublisher: trả nil khi Kafka đã ghi xong (acks=all) mọi message.
type EventPublisher interface {
	Publish(ctx context.Context, userID int64, events []Event) error
}

// Batches chia events thành các mảng ≤ MaxEventsPerMessage (giữ thứ tự).
func Batches(events []Event) [][]Event {
	var out [][]Event
	for len(events) > 0 {
		n := min(len(events), MaxEventsPerMessage)
		out = append(out, events[:n])
		events = events[n:]
	}
	return out
}

// KafkaEventPublisher: producer idempotent, acks=all, nén zstd. Kết nối trong Start (lifecycle).
type KafkaEventPublisher struct {
	brokers []string
	topic   string
	cfg     *sarama.Config
	log     *slog.Logger
	p       sarama.SyncProducer
}

var errNotStarted = errors.New("kafka producer chưa kết nối")

func NewKafkaEventPublisher(brokers []string, clientID, topic string, log *slog.Logger) *KafkaEventPublisher {
	cfg := sarama.NewConfig()
	cfg.ClientID = clientID
	cfg.Version = sarama.V2_8_0_0
	cfg.Producer.Idempotent = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Return.Successes = true
	cfg.Producer.Compression = sarama.CompressionZSTD
	cfg.Producer.MaxMessageBytes = 4 << 20
	cfg.Net.MaxOpenRequests = 1 // bắt buộc khi bật idempotent
	return &KafkaEventPublisher{brokers: brokers, topic: topic, cfg: cfg, log: log}
}

// Start kết nối Kafka; chưa sẵn sàng → thử lại tới khi ctx hết hạn.
func (k *KafkaEventPublisher) Start(ctx context.Context) error {
	backoff := 500 * time.Millisecond
	for {
		p, err := sarama.NewSyncProducer(k.brokers, k.cfg)
		if err == nil {
			k.p = p
			k.log.Info("kafka producer connected", "brokers", k.brokers, "topic", k.topic)
			return nil
		}
		k.log.Warn("kafka chưa sẵn sàng, thử lại", "err", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return fmt.Errorf("kết nối kafka: %w", err)
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

func (k *KafkaEventPublisher) Stop(context.Context) error {
	if k.p == nil {
		return nil
	}
	return k.p.Close()
}

func (k *KafkaEventPublisher) Publish(_ context.Context, userID int64, events []Event) error {
	if k.p == nil {
		return errNotStarted
	}
	key := strconv.FormatInt(userID, 10)
	var msgs []*sarama.ProducerMessage
	for _, b := range Batches(events) {
		value, err := json.Marshal(b)
		if err != nil {
			return err
		}
		msgs = append(msgs, &sarama.ProducerMessage{Topic: k.topic, Key: sarama.StringEncoder(key), Value: sarama.ByteEncoder(value)})
	}
	if len(msgs) == 0 {
		return nil
	}
	return k.p.SendMessages(msgs)
}
