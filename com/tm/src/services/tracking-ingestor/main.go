// =============================================================================
// TRACKING INGESTOR - Payment Event Processor
// =============================================================================
// Consume payment events từ Kafka → validate JSON → forward tới topic riêng
// để ClickHouse Kafka Engine tự ingest vào payments_bronze.
//
// Flow:
//   event-producer → "payment-events"
//        → [tracking-ingestor] validate + forward
//        → "payment-events-ingest"
//        → CH Kafka Engine (payments_kafka)
//        → Materialized View (payments_bronze_mv)
//        → payments_bronze (MergeTree)
//
// Lý do có Go service ở giữa thay vì CH đọc "payment-events" thẳng:
//   - Validate schema trước khi vào CH (tránh CH ghi rác)
//   - Tách consumer group: ingestor có thể retry độc lập
//   - Dễ thêm transform/enrichment sau này
// =============================================================================

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

// =============================================================================
// CONFIG
// =============================================================================

type Config struct {
	KafkaBrokers  []string
	InputTopic    string // "payment-events" - từ event-producer
	OutputTopic   string // "payment-events-ingest" - CH Kafka Engine đọc topic này
	ConsumerGroup string
}

func DefaultConfig() *Config {
	return &Config{
		KafkaBrokers:  []string{"localhost:9092"},
		InputTopic:    "payment-events",
		OutputTopic:   "payment-events-ingest",
		ConsumerGroup: "payment-ingestor",
	}
}

// =============================================================================
// PAYMENT EVENT (schema 6 fields - khớp với event-producer)
// =============================================================================

type PaymentEvent struct {
	EventID     string  `json:"event_id"`
	UserID      uint64  `json:"user_id"`
	Amount      float64 `json:"amount"`
	Status      string  `json:"status"`
	PaymentDate string  `json:"payment_date"` // "2024-01-15"
	UpdatedAt   string  `json:"updated_at"`   // ISO8601
}

// =============================================================================
// KAFKA CONSUMER GROUP HANDLER
// =============================================================================

type Handler struct {
	producer  sarama.SyncProducer
	cfg       *Config
	ready     chan bool
	consumed  int64
	forwarded int64
	parseErr  int64
}

func (h *Handler) Setup(s sarama.ConsumerGroupSession) error {
	log.Printf("Session started: member=%s gen=%d", s.MemberID(), s.GenerationID())
	close(h.ready)
	return nil
}

func (h *Handler) Cleanup(s sarama.ConsumerGroupSession) error {
	log.Printf("Session ended: member=%s", s.MemberID())
	return nil
}

// ConsumeClaim: validate JSON → forward raw bytes tới OutputTopic.
// Chỉ mark offset sau khi produce thành công để tránh mất event.
func (h *Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			atomic.AddInt64(&h.consumed, 1)

			// Validate JSON: đảm bảo CH không nhận message rác
			var event PaymentEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				atomic.AddInt64(&h.parseErr, 1)
				log.Printf("parse error partition=%d offset=%d: %v",
					msg.Partition, msg.Offset, err)
				// Vẫn mark để không bị stuck; message lỗi bị drop
				session.MarkMessage(msg, "")
				continue
			}

			fmt.Println(event)

			// Forward raw bytes (không re-serialize để giữ nguyên format)
			// Key = event_id → routing nhất quán theo partition
			outMsg := &sarama.ProducerMessage{
				Topic: h.cfg.OutputTopic,
				Key:   sarama.StringEncoder(event.EventID),
				Value: sarama.ByteEncoder(msg.Value),
			}
			if _, _, err := h.producer.SendMessage(outMsg); err != nil {
				// Không mark offset → reprocess khi restart
				log.Printf("produce error partition=%d offset=%d: %v",
					msg.Partition, msg.Offset, err)
				continue
			}

			atomic.AddInt64(&h.forwarded, 1)
			session.MarkMessage(msg, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

// =============================================================================
// MAIN
// =============================================================================

func main() {
	cfg := DefaultConfig()

	kafkaBroker := flag.String("kafka", "localhost:9092", "Kafka broker")
	flag.StringVar(&cfg.InputTopic, "input-topic", cfg.InputTopic, "Input topic (from event-producer)")
	flag.StringVar(&cfg.OutputTopic, "output-topic", cfg.OutputTopic, "Output topic (to CH Kafka Engine)")
	flag.StringVar(&cfg.ConsumerGroup, "group", cfg.ConsumerGroup, "Consumer group")
	flag.Parse()

	cfg.KafkaBrokers = []string{*kafkaBroker}

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Printf("Payment Ingestor | %s → %s | group=%s",
		cfg.InputTopic, cfg.OutputTopic, cfg.ConsumerGroup)

	// ---------------------------------------------------------------------------
	// Kafka Producer (forward validated events → OutputTopic cho CH Kafka Engine)
	// ---------------------------------------------------------------------------
	prodCfg := sarama.NewConfig()
	prodCfg.Producer.Return.Successes = true
	prodCfg.Producer.RequiredAcks = sarama.WaitForLocal
	prodCfg.Producer.Compression = sarama.CompressionLZ4

	producer, err := sarama.NewSyncProducer(cfg.KafkaBrokers, prodCfg)
	if err != nil {
		log.Fatalf("SyncProducer: %v", err)
	}
	defer producer.Close()
	log.Printf("Kafka producer OK → %s", cfg.OutputTopic)

	// ---------------------------------------------------------------------------
	// Kafka Consumer Group (đọc từ InputTopic)
	// ---------------------------------------------------------------------------
	consCfg := sarama.NewConfig()
	consCfg.Consumer.Return.Errors = true
	consCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	consCfg.Consumer.Offsets.AutoCommit.Enable = true
	consCfg.Consumer.Offsets.AutoCommit.Interval = 5 * time.Second
	consCfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategySticky(),
	}
	consCfg.Consumer.Group.Session.Timeout = 30 * time.Second
	consCfg.Consumer.Group.Heartbeat.Interval = 10 * time.Second

	cg, err := sarama.NewConsumerGroup(cfg.KafkaBrokers, cfg.ConsumerGroup, consCfg)
	if err != nil {
		log.Fatalf("ConsumerGroup: %v", err)
	}
	defer cg.Close()

	handler := &Handler{
		producer: producer,
		cfg:      cfg,
		ready:    make(chan bool),
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for err := range cg.Errors() {
			log.Printf("Kafka error: %v", err)
		}
	}()

	// Stats reporter
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				log.Printf("Stats: consumed=%d forwarded=%d parse_err=%d",
					atomic.LoadInt64(&handler.consumed),
					atomic.LoadInt64(&handler.forwarded),
					atomic.LoadInt64(&handler.parseErr),
				)
			}
		}
	}()

	// Consume loop
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := cg.Consume(ctx, []string{cfg.InputTopic}, handler); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("Consume error: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			handler.ready = make(chan bool)
		}
	}()

	<-handler.ready
	log.Printf("Ready, consuming from %s...", cfg.InputTopic)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	log.Printf("Signal %v, shutting down...", sig)
	cancel()
	wg.Wait()

	log.Printf("Final: consumed=%d forwarded=%d parse_err=%d",
		atomic.LoadInt64(&handler.consumed),
		atomic.LoadInt64(&handler.forwarded),
		atomic.LoadInt64(&handler.parseErr),
	)
}
