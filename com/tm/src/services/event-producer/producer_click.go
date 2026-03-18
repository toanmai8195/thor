// =============================================================================
// CLICK EVENT PRODUCER
// =============================================================================
// Schema: event_id, user_id, session_id, device_id, platform, country,
//         page_url, element_id, element_type, event_date, clicked_at
// Topic:  click-events  (→ cdp-ingestor → click-events-ingest → Flink)
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

type ClickEvent struct {
	EventID     string `json:"event_id"`
	UserID      uint64 `json:"user_id"`
	SessionID   string `json:"session_id"`
	DeviceID    string `json:"device_id"`
	Platform    string `json:"platform"`
	Country     string `json:"country"`
	PageURL     string `json:"page_url"`
	ElementID   string `json:"element_id"`
	ElementType string `json:"element_type"` // button|link|image|other
	EventDate   string `json:"event_date"`
	ClickedAt   string `json:"clicked_at"`
}

// =============================================================================
// DISTRIBUTION POOLS
// =============================================================================

// elementPool: pairs of (element_id, element_type)
var clickElementPool = []struct {
	ID   string
	Type string
}{
	{"btn-buy-now", "button"},
	{"btn-buy-now", "button"},
	{"btn-add-cart", "button"},
	{"btn-add-cart", "button"},
	{"btn-checkout", "button"},
	{"btn-login", "button"},
	{"btn-signup", "button"},
	{"lnk-product-detail", "link"},
	{"lnk-category-phone", "link"},
	{"lnk-category-laptop", "link"},
	{"lnk-see-more", "link"},
	{"lnk-breadcrumb-home", "link"},
	{"img-banner-sale", "image"},
	{"img-banner-new", "image"},
	{"img-product-thumb", "image"},
	{"btn-search-submit", "button"},
	{"btn-filter-apply", "button"},
	{"lnk-nav-home", "link"},
	{"lnk-nav-products", "link"},
	{"other-carousel-dot", "other"},
}

var clickPagePool = []string{
	"/home", "/home",
	"/products", "/products",
	"/products/1", "/products/2", "/products/3", "/products/4",
	"/cart",
	"/checkout",
	"/search",
}

// =============================================================================
// GENERATOR
// =============================================================================

func generateClickEvent(rng *rand.Rand) ClickEvent {
	daysAgo := rng.Intn(30)
	eventDate := time.Now().AddDate(0, 0, -daysAgo)
	userID := uint64(rng.Intn(100_000)) + 1
	elem := clickElementPool[rng.Intn(len(clickElementPool))]

	return ClickEvent{
		EventID:     uuid.New().String(),
		UserID:      userID,
		SessionID:   uuid.New().String(),
		DeviceID:    fmt.Sprintf("dev-%06d", rng.Intn(20_000)+1),
		Platform:    platformPool[rng.Intn(len(platformPool))],
		Country:     countryPool[rng.Intn(len(countryPool))],
		PageURL:     clickPagePool[rng.Intn(len(clickPagePool))],
		ElementID:   elem.ID,
		ElementType: elem.Type,
		EventDate:   eventDate.Format("2006-01-02"),
		ClickedAt:   time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
	}
}

// =============================================================================
// PRODUCE LOOP
// =============================================================================

func RunClickProducer(ctx context.Context, producer sarama.AsyncProducer, topic string, ratePerSec int, rng *rand.Rand, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("[click] Starting | topic=%s rate=%d/s", topic, ratePerSec)

	ticker := time.NewTicker(time.Second / time.Duration(ratePerSec))
	defer ticker.Stop()
	statsTicker := time.NewTicker(10 * time.Second)
	defer statsTicker.Stop()

	var sent, errors int64

	for {
		select {
		case <-ctx.Done():
			log.Printf("[click] Shutdown. sent=%d errors=%d", atomic.LoadInt64(&sent), atomic.LoadInt64(&errors))
			return

		case <-statsTicker.C:
			log.Printf("[click] Stats: sent=%d errors=%d", atomic.LoadInt64(&sent), atomic.LoadInt64(&errors))

		case <-ticker.C:
			event := generateClickEvent(rng)
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
