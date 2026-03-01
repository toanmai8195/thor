// =============================================================================
// EVENT PRODUCER - Payment Events
// =============================================================================
// Giả lập payment events gửi lên Kafka.
// Schema đơn giản: event_id, user_id, amount, status, payment_date, updated_at
//
// Chạy:
//   bazel run //com/tm/src/services/event-producer -- -kafka=localhost:9092 -rate=1000
//   hoặc: go run main.go -kafka=localhost:9092
// =============================================================================

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

// =============================================================================
// PAYMENT EVENT - Schema 6 fields
// =============================================================================

type PaymentEvent struct {
	EventID     string  `json:"event_id"`     // UUID
	UserID      uint64  `json:"user_id"`      // 1 → 100_000
	Amount      float64 `json:"amount"`       // $1 → $5000 (có thể âm/0 để test)
	Status      string  `json:"status"`       // paid/pending/failed/refunded/cancelled
	PaymentDate string  `json:"payment_date"` // "2024-01-15" (YYYY-MM-DD)
	UpdatedAt   string  `json:"updated_at"`   // ISO8601: "2024-01-15T10:30:00.000Z"
}

// =============================================================================
// STATUS DISTRIBUTION (giả lập thực tế)
// =============================================================================
// paid:       60% (đơn thành công)
// pending:    20% (đang xử lý)
// failed:     10% (thất bại)
// refunded:    5% (hoàn tiền)
// cancelled:   3% (huỷ đơn)
// invalid:     2% (bad data để test silver normalization)

var statusPool = []string{
	"paid", "paid", "paid", "paid", "paid", "paid",        // 60%
	"pending", "pending",                                   // 20%
	"failed",                                               // 10%
	"refunded",                                             // 5%
	"cancelled",                                            // 3%
	"PAID", "invalid_status",                               // 2% bad data → silver sẽ normalize
}

// =============================================================================
// EVENT GENERATOR
// =============================================================================

func generateEvent(rng *rand.Rand) PaymentEvent {
	// Ngày thanh toán: random trong 30 ngày gần đây
	daysAgo := rng.Intn(30)
	payDate := time.Now().AddDate(0, 0, -daysAgo)

	// Amount: $1 → $2000, đôi khi âm hoặc 0 để test bronze
	var amount float64
	switch rng.Intn(20) {
	case 0:
		amount = -float64(rng.Intn(500)) // âm (bad data)
	case 1:
		amount = 0 // zero (bad data)
	default:
		// Số tiền thực tế: $1 → $2000 (làm tròn 2 chữ số thập phân)
		amount = float64(rng.Intn(199900)+100) / 100.0
	}

	return PaymentEvent{
		EventID:     uuid.New().String(),
		UserID:      uint64(rng.Intn(100_000)) + 1,
		Amount:      amount,
		Status:      statusPool[rng.Intn(len(statusPool))],
		PaymentDate: payDate.Format("2006-01-02"),
		UpdatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}
}

// =============================================================================
// MAIN
// =============================================================================

func main() {
	// Flags
	kafkaBroker := flag.String("kafka", "localhost:9092", "Kafka broker")
	topic := flag.String("topic", "payment-events", "Kafka topic")
	ratePerSec := flag.Int("rate", 1000, "Events per second")
	flag.Parse()

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Printf("Event Producer starting | broker=%s topic=%s rate=%d/s",
		*kafkaBroker, *topic, *ratePerSec)

	// Kafka producer
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = false // async → fire-and-forget
	cfg.Producer.Return.Errors = true
	cfg.Producer.Compression = sarama.CompressionSnappy
	cfg.Producer.Flush.Frequency = 100 * time.Millisecond
	cfg.Producer.Flush.Messages = 500

	producer, err := sarama.NewAsyncProducer([]string{*kafkaBroker}, cfg)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Error handler
	go func() {
		for err := range producer.Errors() {
			log.Printf("Kafka error: %v", err)
		}
	}()

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Rate limiter: *ratePerSec events/giây
	ticker := time.NewTicker(time.Second / time.Duration(*ratePerSec))
	defer ticker.Stop()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	var sent, errors int64

	// Stats reporter
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for range t.C {
			log.Printf("Stats: sent=%d errors=%d", atomic.LoadInt64(&sent), atomic.LoadInt64(&errors))
		}
	}()

	log.Printf("Producing events...")
	for {
		select {
		case <-sigChan:
			log.Printf("Shutting down. Total sent=%d", atomic.LoadInt64(&sent))
			return

		case <-ticker.C:
			event := generateEvent(rng)
			data, err := json.Marshal(event)
			if err != nil {
				atomic.AddInt64(&errors, 1)
				continue
			}

			producer.Input() <- &sarama.ProducerMessage{
				Topic: *topic,
				Key:   sarama.StringEncoder(fmt.Sprintf("%d", event.UserID)),
				Value: sarama.ByteEncoder(data),
			}
			atomic.AddInt64(&sent, 1)
		}
	}
}
