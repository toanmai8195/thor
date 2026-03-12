// =============================================================================
// LOGIN EVENT PRODUCER - CDP Step 1
// =============================================================================
// Giả lập login events gửi lên Kafka cho CDP pipeline.
// Schema: event_id, user_id, session_id, device_id, platform, country, event_date, login_at
//
// Chạy:
//   bazel run //com/tm/src/services/login-event-producer -- -kafka=localhost:9092 -rate=500
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
// LOGIN EVENT — Schema 8 fields
// =============================================================================

type LoginEvent struct {
	EventID   string `json:"event_id"`   // UUID
	UserID    uint64 `json:"user_id"`    // 1 → 100_000
	SessionID string `json:"session_id"` // UUID (session scope)
	DeviceID  string `json:"device_id"`  // UUID (device scope, stable per device)
	Platform  string `json:"platform"`   // ios/android/web
	Country   string `json:"country"`    // VN/US/SG/TH/...
	EventDate string `json:"event_date"` // "2025-01-15" (YYYY-MM-DD)
	LoginAt   string `json:"login_at"`   // ISO8601: "2025-01-15T10:30:00.000Z"
}

// =============================================================================
// DISTRIBUTION POOLS
// =============================================================================

var platformPool = []string{
	"ios", "ios", "ios",          // 30%
	"android", "android", "android", "android", // 40%
	"web", "web",                 // 20%
	"unknown",                    // 10% — bad data để test silver normalization
}

var countryPool = []string{
	"VN", "VN", "VN", "VN", "VN", // 50% Vietnam
	"US",                         // 10%
	"SG",                         // 10%
	"TH",                         // 10%
	"ID",                         // 10%
	"",                           // 10% missing (test silver handling)
}

// devicePool: simulate ~20_000 unique devices for 100_000 users
// (nhiều users dùng chung device, hoặc 1 user nhiều devices)
func generateDeviceID(rng *rand.Rand) string {
	return fmt.Sprintf("dev-%06d", rng.Intn(20_000)+1)
}

// =============================================================================
// EVENT GENERATOR
// =============================================================================

func generateEvent(rng *rand.Rand) LoginEvent {
	daysAgo := rng.Intn(30)
	eventDate := time.Now().AddDate(0, 0, -daysAgo)

	userID := uint64(rng.Intn(100_000)) + 1

	return LoginEvent{
		EventID:   uuid.New().String(),
		UserID:    userID,
		SessionID: uuid.New().String(),
		DeviceID:  generateDeviceID(rng),
		Platform:  platformPool[rng.Intn(len(platformPool))],
		Country:   countryPool[rng.Intn(len(countryPool))],
		EventDate: eventDate.Format("2006-01-02"),
		LoginAt:   time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
	}
}

// =============================================================================
// MAIN
// =============================================================================

func main() {
	kafkaBroker := flag.String("kafka", "localhost:9092", "Kafka broker")
	topic := flag.String("topic", "login-events", "Kafka topic")
	ratePerSec := flag.Int("rate", 500, "Events per second")
	flag.Parse()

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Printf("Login Event Producer starting | broker=%s topic=%s rate=%d/s",
		*kafkaBroker, *topic, *ratePerSec)

	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = false
	cfg.Producer.Return.Errors = true
	cfg.Producer.Compression = sarama.CompressionSnappy
	cfg.Producer.Flush.Frequency = 100 * time.Millisecond
	cfg.Producer.Flush.Messages = 500

	producer, err := sarama.NewAsyncProducer([]string{*kafkaBroker}, cfg)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	go func() {
		for err := range producer.Errors() {
			log.Printf("Kafka error: %v", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(time.Second / time.Duration(*ratePerSec))
	defer ticker.Stop()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	var sent, errors int64

	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for range t.C {
			log.Printf("Stats: sent=%d errors=%d", atomic.LoadInt64(&sent), atomic.LoadInt64(&errors))
		}
	}()

	log.Printf("Producing login events...")
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
