// =============================================================================
// TRACKING INGESTOR - Payment Events
// =============================================================================
// Consume payment events từ Kafka → batch insert vào ClickHouse Bronze.
//
// Flow:
//   Kafka "payment-events" → ConsumerGroup → BatchProcessor → payments_bronze
//
// Kafka concepts:
//   ConsumerGroup: nhiều consumers chia nhau partitions
//   Offset: vị trí message trong partition (auto-commit mỗi 5s)
//   Backpressure: nếu ClickHouse chậm → channel đầy → drop + log warning
//
// ClickHouse BatchInsert:
//   PrepareBatch → Append rows → Send()
//   Tối ưu: gom 10_000 rows thành 1 INSERT thay vì insert từng row
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

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/IBM/sarama"
)

// =============================================================================
// CONFIG
// =============================================================================

type Config struct {
	KafkaBrokers    []string
	KafkaTopic      string
	ConsumerGroup   string
	ClickHouseHost  string
	ClickHousePort  int
	ClickHouseDB    string
	ClickHouseTable string
	BatchSize       int
	BatchTimeout    time.Duration
	NumWorkers      int
	ChannelBuffer   int
	MaxRetries      int
	RetryBackoff    time.Duration
}

func DefaultConfig() *Config {
	return &Config{
		KafkaBrokers:    []string{"localhost:9092"},
		KafkaTopic:      "payment-events",
		ConsumerGroup:   "payment-ingestor",
		ClickHouseHost:  "localhost",
		ClickHousePort:  9000,
		ClickHouseDB:    "tracking",
		ClickHouseTable: "payments_bronze", // Bronze layer trong Medallion Architecture
		BatchSize:       10_000,
		BatchTimeout:    5 * time.Second,
		NumWorkers:      4,
		ChannelBuffer:   100_000,
		MaxRetries:      3,
		RetryBackoff:    time.Second,
	}
}

// =============================================================================
// PAYMENT EVENT (schema 6 fields - khớp với producer)
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
// CLICKHOUSE CLIENT
// =============================================================================

type ClickHouseClient struct {
	conn   clickhouse.Conn
	cfg    *Config
	mu     sync.Mutex
	closed bool
}

func NewClickHouseClient(cfg *Config) (*ClickHouseClient, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.ClickHouseHost, cfg.ClickHousePort)},
		Auth: clickhouse.Auth{
			Database: cfg.ClickHouseDB,
			Username: "default",
			Password: "",
		},
		Settings: clickhouse.Settings{
			"max_execution_time":    60,
			"max_insert_block_size": 100_000,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 10 * time.Minute,
		DialTimeout:     10 * time.Second,
		ReadTimeout:     30 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse.Open: %w", err)
	}
	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("clickhouse.Ping: %w", err)
	}
	return &ClickHouseClient{conn: conn, cfg: cfg}, nil
}

// BatchInsert ghi batch events vào payments_bronze (chỉ 6 columns)
func (c *ClickHouseClient) BatchInsert(ctx context.Context, events []*PaymentEvent) error {
	if len(events) == 0 {
		return nil
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return fmt.Errorf("client is closed")
	}
	c.mu.Unlock()

	// 6 columns khớp với schema payments_bronze
	batch, err := c.conn.PrepareBatch(ctx, fmt.Sprintf(
		`INSERT INTO %s.%s (event_id, user_id, amount, status, payment_date, updated_at)`,
		c.cfg.ClickHouseDB, c.cfg.ClickHouseTable,
	))
	if err != nil {
		return fmt.Errorf("PrepareBatch: %w", err)
	}

	for _, e := range events {
		// Parse payment_date: "2024-01-15" → time.Time
		payDate, err := time.Parse("2006-01-02", e.PaymentDate)
		if err != nil {
			payDate = time.Now()
		}

		// Parse updated_at: ISO8601 → time.Time
		updatedAt, err := time.Parse(time.RFC3339Nano, e.UpdatedAt)
		if err != nil {
			updatedAt = time.Now()
		}

		if err := batch.Append(
			e.EventID,
			e.UserID,
			e.Amount,
			e.Status,
			payDate,
			updatedAt,
		); err != nil {
			return fmt.Errorf("batch.Append: %w", err)
		}
	}

	return batch.Send()
}

func (c *ClickHouseClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return c.conn.Close()
}

// =============================================================================
// BATCH PROCESSOR
// =============================================================================

type BatchProcessor struct {
	cfg       *Config
	client    *ClickHouseClient
	eventChan chan *PaymentEvent
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	processed int64
	inserted  int64
	failed    int64
}

func NewBatchProcessor(cfg *Config, client *ClickHouseClient) *BatchProcessor {
	ctx, cancel := context.WithCancel(context.Background())
	return &BatchProcessor{
		cfg:       cfg,
		client:    client,
		eventChan: make(chan *PaymentEvent, cfg.ChannelBuffer),
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (bp *BatchProcessor) Start() {
	for i := 0; i < bp.cfg.NumWorkers; i++ {
		bp.wg.Add(1)
		go bp.worker(i)
	}
}

func (bp *BatchProcessor) worker(id int) {
	defer bp.wg.Done()

	batch := make([]*PaymentEvent, 0, bp.cfg.BatchSize)
	ticker := time.NewTicker(bp.cfg.BatchTimeout)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		toInsert := make([]*PaymentEvent, len(batch))
		copy(toInsert, batch)
		batch = batch[:0]

		var err error
		for attempt := 0; attempt <= bp.cfg.MaxRetries; attempt++ {
			if attempt > 0 {
				time.Sleep(bp.cfg.RetryBackoff * time.Duration(attempt))
			}
			err = bp.client.BatchInsert(bp.ctx, toInsert)
			if err == nil {
				atomic.AddInt64(&bp.inserted, int64(len(toInsert)))
				return
			}
		}
		atomic.AddInt64(&bp.failed, int64(len(toInsert)))
		log.Printf("[worker-%d] insert failed after %d retries: %v", id, bp.cfg.MaxRetries, err)
	}

	for {
		select {
		case <-bp.ctx.Done():
			flush()
			return
		case event := <-bp.eventChan:
			atomic.AddInt64(&bp.processed, 1)
			batch = append(batch, event)
			if len(batch) >= bp.cfg.BatchSize {
				flush()
				ticker.Reset(bp.cfg.BatchTimeout)
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (bp *BatchProcessor) Process(event *PaymentEvent) {
	select {
	case bp.eventChan <- event:
	default:
		log.Printf("Warning: channel full, dropping event")
		atomic.AddInt64(&bp.failed, 1)
	}
}

func (bp *BatchProcessor) Stop() {
	bp.cancel()
	bp.wg.Wait()
}

func (bp *BatchProcessor) Stats() (processed, inserted, failed int64) {
	return atomic.LoadInt64(&bp.processed),
		atomic.LoadInt64(&bp.inserted),
		atomic.LoadInt64(&bp.failed)
}

// =============================================================================
// KAFKA CONSUMER GROUP HANDLER
// =============================================================================

type Handler struct {
	processor *BatchProcessor
	ready     chan bool
	consumed  int64
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

func (h *Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			var event PaymentEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				atomic.AddInt64(&h.parseErr, 1)
				log.Printf("parse error partition=%d offset=%d: %v",
					msg.Partition, msg.Offset, err)
				session.MarkMessage(msg, "")
				continue
			}

			h.processor.Process(&event)
			atomic.AddInt64(&h.consumed, 1)
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
	flag.StringVar(&cfg.KafkaTopic, "topic", cfg.KafkaTopic, "Kafka topic")
	flag.StringVar(&cfg.ConsumerGroup, "group", cfg.ConsumerGroup, "Consumer group")
	flag.StringVar(&cfg.ClickHouseHost, "ch-host", cfg.ClickHouseHost, "ClickHouse host")
	flag.IntVar(&cfg.ClickHousePort, "ch-port", cfg.ClickHousePort, "ClickHouse port")
	flag.IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "Batch size")
	flag.Parse()

	cfg.KafkaBrokers = []string{*kafkaBroker}

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Printf("Payment Ingestor starting | topic=%s group=%s ch=%s:%d batch=%d",
		cfg.KafkaTopic, cfg.ConsumerGroup, cfg.ClickHouseHost, cfg.ClickHousePort, cfg.BatchSize)

	// Connect ClickHouse
	chClient, err := NewClickHouseClient(cfg)
	if err != nil {
		log.Fatalf("ClickHouse connect: %v", err)
	}
	defer chClient.Close()
	log.Printf("ClickHouse OK → %s.%s", cfg.ClickHouseDB, cfg.ClickHouseTable)

	// Batch processor
	processor := NewBatchProcessor(cfg, chClient)
	processor.Start()
	defer processor.Stop()

	// Kafka consumer group
	saramaCfg := sarama.NewConfig()
	saramaCfg.Consumer.Return.Errors = true
	saramaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaCfg.Consumer.Offsets.AutoCommit.Enable = true
	saramaCfg.Consumer.Offsets.AutoCommit.Interval = 5 * time.Second
	saramaCfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategySticky(),
	}
	saramaCfg.Consumer.Group.Session.Timeout = 30 * time.Second
	saramaCfg.Consumer.Group.Heartbeat.Interval = 10 * time.Second

	cg, err := sarama.NewConsumerGroup(cfg.KafkaBrokers, cfg.ConsumerGroup, saramaCfg)
	if err != nil {
		log.Fatalf("ConsumerGroup: %v", err)
	}
	defer cg.Close()

	handler := &Handler{processor: processor, ready: make(chan bool)}
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
				p, ins, fail := processor.Stats()
				log.Printf("Stats: consumed=%d processed=%d inserted=%d failed=%d",
					atomic.LoadInt64(&handler.consumed), p, ins, fail)
			}
		}
	}()

	// Consume loop
	go func() {
		for {
			if err := cg.Consume(ctx, []string{cfg.KafkaTopic}, handler); err != nil {
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
	log.Printf("Ready, consuming payment events...")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	log.Printf("Signal %v, shutting down...", sig)
	cancel()

	p, ins, fail := processor.Stats()
	log.Printf("Final: consumed=%d inserted=%d failed=%d",
		atomic.LoadInt64(&handler.consumed), ins, fail-p+ins)
}
