// Package consumer đọc topic đầu vào bằng consumer group và giao từng message cho relay.
package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"

	"thor/com/tm/event-gateway/internal/producer"
	"thor/com/tm/event-gateway/internal/relay"
)

// Consumer chạy consumer group trên 1 topic.
type Consumer struct {
	group sarama.ConsumerGroup
	topic string
	relay *relay.Relay
	log   *slog.Logger
	ready atomic.Bool
}

func New(brokers []string, clientID, groupID, topic string, r *relay.Relay, log *slog.Logger) (*Consumer, error) {
	cfg := producer.NewConfig(clientID)
	// Group mới đọc từ đầu topic, không bỏ event cũ
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Return.Errors = true
	group, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		return nil, fmt.Errorf("tạo consumer group: %w", err)
	}
	return &Consumer{group: group, topic: topic, relay: r, log: log}, nil
}

// Ready: đã được chia partition và đang đọc
func (c *Consumer) Ready() bool { return c.ready.Load() }

// Run đọc tới khi ctx bị huỷ. Consume trả về mỗi lần rebalance → gọi lại.
func (c *Consumer) Run(ctx context.Context) error {
	go func() {
		for err := range c.group.Errors() {
			c.log.Error("consumer group error", "err", err)
		}
	}()
	for {
		if err := c.group.Consume(ctx, []string{c.topic}, c); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return nil
			}
			c.log.Error("consume failed, thử lại", "err", err)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

func (c *Consumer) Close() error { return c.group.Close() }

// Setup / Cleanup / ConsumeClaim: sarama.ConsumerGroupHandler

func (c *Consumer) Setup(s sarama.ConsumerGroupSession) error {
	c.ready.Store(true)
	c.log.Info("consumer assigned", "claims", s.Claims())
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	c.ready.Store(false)
	return nil
}

// ConsumeClaim xử lý tuần tự từng message của 1 partition; chỉ đánh dấu offset
// sau khi relay đã gửi xong (at-least-once).
func (c *Consumer) ConsumeClaim(s sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-s.Context().Done():
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			src := relay.Source{Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset}
			if err := c.relay.Handle(s.Context(), msg.Key, msg.Value, src); err != nil {
				// Chỉ lỗi khi session kết thúc: không mark, message sẽ được đọc lại
				return nil
			}
			s.MarkMessage(msg, "")
		}
	}
}
