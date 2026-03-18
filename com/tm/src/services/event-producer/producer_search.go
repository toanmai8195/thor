// =============================================================================
// SEARCH EVENT PRODUCER
// =============================================================================
// Schema: event_id, user_id, session_id, platform, country, query,
//         result_count, clicked_result_id, event_date, searched_at
// Note:   Không có device_id — search là intent-level signal.
//         clicked_result_id = empty nếu user không click kết quả nào.
// Topic:  search-events  (→ cdp-ingestor → search-events-ingest → Flink)
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

type SearchEvent struct {
	EventID         string `json:"event_id"`
	UserID          uint64 `json:"user_id"`
	SessionID       string `json:"session_id"`
	Platform        string `json:"platform"`
	Country         string `json:"country"`
	Query           string `json:"query"`
	ResultCount     int64  `json:"result_count"`      // 0 = zero-result search
	ClickedResultID string `json:"clicked_result_id"` // empty = no click (→ silver NULL)
	EventDate       string `json:"event_date"`
	SearchedAt      string `json:"searched_at"`
}

// =============================================================================
// DISTRIBUTION POOLS
// =============================================================================

var searchQueryPool = []string{
	"điện thoại samsung", "iphone 15", "laptop gaming",
	"tai nghe bluetooth", "màn hình 4k", "chuột gaming",
	"bàn phím cơ", "máy tính bảng", "sạc dự phòng",
	"đồng hồ thông minh", "loa bluetooth", "camera hành trình",
	"samsung s24", "iphone 16 pro", "macbook air m3",
	"airpods pro", "galaxy buds", "sony wh1000xm5",
	"laptop dell xps", "asus rog", "lenovo thinkpad",
	"pin điện thoại", "ốp lưng iphone", "kính cường lực",
	"cáp sạc type c", "hub usb c", "bộ router wifi",
	"máy lọc không khí", "robot hút bụi", "nồi chiên không dầu",
	// zero-result queries (để test has_results = 0)
	"sản phẩm xyz không tồn tại", "mã sp: #@!",
	"", // empty query — test silver filter
}

// generateResultCount: phân phối thực tế
// 10% zero-result, 90% có kết quả (1-200)
func generateResultCount(rng *rand.Rand) int64 {
	if rng.Intn(10) == 0 {
		return 0
	}
	return int64(rng.Intn(200)) + 1
}

// generateClickedResultID: 60% click, 40% không click
func generateClickedResultID(rng *rand.Rand, resultCount int64) string {
	if resultCount == 0 {
		return "" // không có kết quả → không thể click
	}
	if rng.Intn(10) < 4 {
		return "" // 40% không click
	}
	return fmt.Sprintf("product-%05d", rng.Intn(10_000)+1)
}

// =============================================================================
// GENERATOR
// =============================================================================

func generateSearchEvent(rng *rand.Rand) SearchEvent {
	daysAgo := rng.Intn(30)
	eventDate := time.Now().AddDate(0, 0, -daysAgo)
	userID := uint64(rng.Intn(100_000)) + 1
	resultCount := generateResultCount(rng)

	return SearchEvent{
		EventID:         uuid.New().String(),
		UserID:          userID,
		SessionID:       uuid.New().String(),
		Platform:        platformPool[rng.Intn(len(platformPool))],
		Country:         countryPool[rng.Intn(len(countryPool))],
		Query:           searchQueryPool[rng.Intn(len(searchQueryPool))],
		ResultCount:     resultCount,
		ClickedResultID: generateClickedResultID(rng, resultCount),
		EventDate:       eventDate.Format("2006-01-02"),
		SearchedAt:      time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
	}
}

// =============================================================================
// PRODUCE LOOP
// =============================================================================

func RunSearchProducer(ctx context.Context, producer sarama.AsyncProducer, topic string, ratePerSec int, rng *rand.Rand, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("[search] Starting | topic=%s rate=%d/s", topic, ratePerSec)

	ticker := time.NewTicker(time.Second / time.Duration(ratePerSec))
	defer ticker.Stop()
	statsTicker := time.NewTicker(10 * time.Second)
	defer statsTicker.Stop()

	var sent, errors int64

	for {
		select {
		case <-ctx.Done():
			log.Printf("[search] Shutdown. sent=%d errors=%d", atomic.LoadInt64(&sent), atomic.LoadInt64(&errors))
			return

		case <-statsTicker.C:
			log.Printf("[search] Stats: sent=%d errors=%d", atomic.LoadInt64(&sent), atomic.LoadInt64(&errors))

		case <-ticker.C:
			event := generateSearchEvent(rng)
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
