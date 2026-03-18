// =============================================================================
// EVENT PRODUCER — CDP Multi-Source (view, click, payment, search)
// =============================================================================
// Chạy 4 producer song song, mỗi producer một topic khác nhau, cùng rate.
// Dùng một shared AsyncProducer (thread-safe) với goroutine riêng per source.
//
// Chạy:
//   bazel run //com/tm/src/services/event-producer -- \
//     -kafka=localhost:9092 -rate=10
//
//   Hoặc override từng topic:
//   bazel run //com/tm/src/services/event-producer -- \
//     -kafka=localhost:9092 -rate=10 \
//     -view-topic=view-events \
//     -click-topic=click-events \
//     -payment-topic=payment-events \
//     -search-topic=search-events
// =============================================================================

package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	kafkaBroker  := flag.String("kafka",         "localhost:9092", "Kafka broker")
	rate         := flag.Int("rate",             10,               "Events per second per producer")
	viewTopic    := flag.String("view-topic",    "view-events",    "Topic for view events")
	clickTopic   := flag.String("click-topic",   "click-events",   "Topic for click events")
	paymentTopic := flag.String("payment-topic", "payment-events", "Topic for payment events")
	searchTopic  := flag.String("search-topic",  "search-events",  "Topic for search events")
	flag.Parse()

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Printf("Event Producer starting | broker=%s rate=%d/s per source", *kafkaBroker, *rate)
	log.Printf("Topics: view=%s click=%s payment=%s search=%s",
		*viewTopic, *clickTopic, *paymentTopic, *searchTopic)

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

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	seed := time.Now().UnixNano()

	wg.Add(4)
	go RunViewProducer(ctx, producer, *viewTopic, *rate,
		rand.New(rand.NewSource(seed+1)), &wg)
	go RunClickProducer(ctx, producer, *clickTopic, *rate,
		rand.New(rand.NewSource(seed+2)), &wg)
	go RunPaymentProducer(ctx, producer, *paymentTopic, *rate,
		rand.New(rand.NewSource(seed+3)), &wg)
	go RunSearchProducer(ctx, producer, *searchTopic, *rate,
		rand.New(rand.NewSource(seed+4)), &wg)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	log.Printf("Signal %v received, shutting down all producers...", sig)
	cancel()
	wg.Wait()
	log.Println("All producers stopped.")
}
