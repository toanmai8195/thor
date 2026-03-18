// =============================================================================
// VIEW EVENT PRODUCER
// =============================================================================
// Schema: event_id, user_id, session_id, device_id, platform, country,
//         page_url, referrer, event_date, viewed_at
// Topic:  view-events  (→ cdp-ingestor → view-events-ingest → Flink)
// =============================================================================

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

// =============================================================================
// SCHEMA
// =============================================================================

type ViewEvent struct {
	EventID   string `json:"event_id"`
	UserID    uint64 `json:"user_id"`
	SessionID string `json:"session_id"`
	DeviceID  string `json:"device_id"`
	Platform  string `json:"platform"`
	Country   string `json:"country"`
	PageURL   string `json:"page_url"`
	Referrer  string `json:"referrer"`  // empty string = direct traffic (→ silver NULL)
	EventDate string `json:"event_date"`
	ViewedAt  string `json:"viewed_at"`
}

// =============================================================================
// DISTRIBUTION POOLS
// =============================================================================

var viewPagePool = []string{
	"/home", "/home", "/home",                         // 15% homepage
	"/products", "/products",                           // 10% catalog
	"/products/1", "/products/2", "/products/3",
	"/products/4", "/products/5", "/products/6",
	"/products/7", "/products/8", "/products/9",
	"/products/10",                                     // 25% product pages
	"/search", "/search",                               // 10% search results
	"/cart",                                            // 5%
	"/checkout",                                        // 5%
	"/account", "/account/orders",                      // 10%
	"/about", "/blog", "/contact",                      // 15% static
	"/",                                                // 5% root
}

var viewReferrerPool = []string{
	"", "", "", "", "",                                 // 40% direct (empty → silver NULL)
	"https://www.google.com",
	"https://www.google.com",
	"https://www.google.com",                           // 15% Google
	"https://www.facebook.com",
	"https://www.facebook.com",                         // 10% Facebook
	"https://www.tiktok.com",                           // 5%
	"/home",                                            // 5% internal referrer
	"/products",                                        // 5% internal referrer
	"https://www.youtube.com",                          // 5%
	"https://zalo.me",                                  // 5%
	"https://mail.google.com",                          // 5%
}

// =============================================================================
// GENERATOR
// =============================================================================

func generateViewEvent(rng *rand.Rand) ViewEvent {
	daysAgo := rng.Intn(30)
	eventDate := time.Now().AddDate(0, 0, -daysAgo)
	userID := uint64(rng.Intn(100_000)) + 1

	return ViewEvent{
		EventID:   uuid.New().String(),
		UserID:    userID,
		SessionID: uuid.New().String(),
		DeviceID:  fmt.Sprintf("dev-%06d", rng.Intn(20_000)+1),
		Platform:  platformPool[rng.Intn(len(platformPool))],
		Country:   countryPool[rng.Intn(len(countryPool))],
		PageURL:   viewPagePool[rng.Intn(len(viewPagePool))],
		Referrer:  viewReferrerPool[rng.Intn(len(viewReferrerPool))],
		EventDate: eventDate.Format("2006-01-02"),
		ViewedAt:  time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
	}
}

// =============================================================================
// PRODUCE LOOP
// =============================================================================

func RunViewProducer(ctx context.Context, producer sarama.AsyncProducer, topic string, ratePerSec int, rng *rand.Rand, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("[view] Starting | topic=%s rate=%d/s", topic, ratePerSec)

	ticker := time.NewTicker(time.Second / time.Duration(ratePerSec))
	defer ticker.Stop()
	statsTicker := time.NewTicker(10 * time.Second)
	defer statsTicker.Stop()

	var sent, errors int64

	for {
		select {
		case <-ctx.Done():
			log.Printf("[view] Shutdown. sent=%d errors=%d", atomic.LoadInt64(&sent), atomic.LoadInt64(&errors))
			return

		case <-statsTicker.C:
			log.Printf("[view] Stats: sent=%d errors=%d", atomic.LoadInt64(&sent), atomic.LoadInt64(&errors))

		case <-ticker.C:
			event := generateViewEvent(rng)
			data, err := json.Marshal(event)
			if err != nil {
				atomic.AddInt64(&errors, 1)
				continue
			}
			producer.Input() <- &sarama.ProducerMessage{
				Topic: topic,
				Key:   sarama.StringEncoder(fmt.Sprintf("%d", event.UserID)),
				Value: sarama.ByteEncoder(data),
			}
			atomic.AddInt64(&sent, 1)
		}
	}
}
