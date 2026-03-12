// =============================================================================
// CDP INGESTOR - Generic JSON Event Validator
// =============================================================================
// Generic version của tracking-ingestor: không tied to một event schema cụ thể.
// Chỉ validate:
//   1. Là valid JSON
//   2. Có field event_id (non-empty string)
// → Forward raw bytes sang output topic.
//
// Reusable cho bất kỳ event type nào trong CDP (login, view, click, payment, search,...).
//
// Flow:
//   <source>-events → [cdp-ingestor] validate → <source>-events-ingest → Flink
//
// Chạy:
//   bazel run //com/tm/src/services/cdp-ingestor -- \
//     -kafka=localhost:9092 \
//     -input-topic=login-events \
//     -output-topic=login-events-ingest \
//     -group=cdp-login-ingestor
// =============================================================================

package main

import (
	"context"
	"encoding/json"
	"flag"
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
	InputTopic    string
	OutputTopic   string
	ConsumerGroup string
}

// =============================================================================
// BASE EVENT — minimal schema chỉ để validate event_id
// =============================================================================

type BaseEvent struct {
	EventID string `json:"event_id"`
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

// ConsumeClaim: validate JSON + event_id → forward raw bytes tới OutputTopic.
func (h *Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			atomic.AddInt64(&h.consumed, 1)

			// Validate: valid JSON với event_id non-empty
			var base BaseEvent
			if err := json.Unmarshal(msg.Value, &base); err != nil || base.EventID == "" {
				atomic.AddInt64(&h.parseErr, 1)
				log.Printf("validation failed partition=%d offset=%d: json_err=%v event_id=%q",
					msg.Partition, msg.Offset, err, base.EventID)
				session.MarkMessage(msg, "")
				continue
			}

			// Forward raw bytes — key = event_id để partition nhất quán
			outMsg := &sarama.ProducerMessage{
				Topic: h.cfg.OutputTopic,
				Key:   sarama.StringEncoder(base.EventID),
				Value: sarama.ByteEncoder(msg.Value),
			}
			if _, _, err := h.producer.SendMessage(outMsg); err != nil {
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
	kafkaBroker := flag.String("kafka", "localhost:9092", "Kafka broker")
	inputTopic := flag.String("input-topic", "login-events", "Input topic")
	outputTopic := flag.String("output-topic", "login-events-ingest", "Output topic")
	consumerGroup := flag.String("group", "cdp-login-ingestor", "Consumer group")
	flag.Parse()

	cfg := &Config{
		KafkaBrokers:  []string{*kafkaBroker},
		InputTopic:    *inputTopic,
		OutputTopic:   *outputTopic,
		ConsumerGroup: *consumerGroup,
	}

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Printf("CDP Ingestor | %s → %s | group=%s", cfg.InputTopic, cfg.OutputTopic, cfg.ConsumerGroup)

	// Producer
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

	// Consumer Group
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
