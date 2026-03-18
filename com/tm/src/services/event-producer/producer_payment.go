// =============================================================================
// PAYMENT EVENT PRODUCER
// =============================================================================
// Schema: event_id, user_id, session_id, order_id, amount, currency,
//         payment_method, status, platform, country, event_date, paid_at
// Note:   Không có device_id — payment là transaction-level event.
// Topic:  payment-events  (→ cdp-ingestor → payment-events-ingest → Flink)
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

type PaymentEvent struct {
	EventID       string `json:"event_id"`
	UserID        uint64 `json:"user_id"`
	SessionID     string `json:"session_id"`
	OrderID       string `json:"order_id"`
	Amount        int64  `json:"amount"`         // VND: đơn vị nhỏ nhất (đồng)
	Currency      string `json:"currency"`       // VND|USD|SGD
	PaymentMethod string `json:"payment_method"` // momo|vnpay|visa|mastercard|other
	Status        string `json:"status"`         // paid|pending|failed|refunded
	Platform      string `json:"platform"`
	Country       string `json:"country"`
	EventDate     string `json:"event_date"`
	PaidAt        string `json:"paid_at"`
}

// =============================================================================
// DISTRIBUTION POOLS
// =============================================================================

var paymentCurrencyPool = []string{
	"VND", "VND", "VND", "VND", "VND",
	"VND", "VND", "VND", // 80% VND
	"USD",               // 10%
	"SGD",               // 10%
}

var paymentMethodPool = []string{
	"momo", "momo", "momo",         // 30%
	"vnpay", "vnpay", "vnpay",      // 30%
	"visa", "visa",                  // 20%
	"mastercard",                    // 10%
	"other",                         // 10%
}

var paymentStatusPool = []string{
	"paid", "paid", "paid", "paid", "paid",
	"paid", "paid",                  // 70% paid
	"pending", "pending",            // 15% pending (→ test idempotency)
	"failed",                        // 10% failed
	"refunded",                      // 5% refunded — test thêm vào silver amount < 0
}

// generateAmount: trả về số tiền hợp lý theo currency
func generateAmount(rng *rand.Rand, currency string) int64 {
	switch currency {
	case "VND":
		// 50,000 → 10,000,000 VND (rounding to 1000)
		return int64((rng.Intn(9950)+50)*1000)
	case "USD":
		// 1 → 500 USD (in cents)
		return int64(rng.Intn(49900)+100) // stored as cents
	default: // SGD
		// 1 → 300 SGD (in cents)
		return int64(rng.Intn(29900)+100)
	}
}

// =============================================================================
// GENERATOR
// =============================================================================

func generatePaymentEvent(rng *rand.Rand) PaymentEvent {
	daysAgo := rng.Intn(30)
	eventDate := time.Now().AddDate(0, 0, -daysAgo)
	userID := uint64(rng.Intn(100_000)) + 1
	currency := paymentCurrencyPool[rng.Intn(len(paymentCurrencyPool))]

	return PaymentEvent{
		EventID:       uuid.New().String(),
		UserID:        userID,
		SessionID:     uuid.New().String(),
		OrderID:       uuid.New().String(),
		Amount:        generateAmount(rng, currency),
		Currency:      currency,
		PaymentMethod: paymentMethodPool[rng.Intn(len(paymentMethodPool))],
		Status:        paymentStatusPool[rng.Intn(len(paymentStatusPool))],
		Platform:      platformPool[rng.Intn(len(platformPool))],
		Country:       countryPool[rng.Intn(len(countryPool))],
		EventDate:     eventDate.Format("2006-01-02"),
		PaidAt:        time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
	}
}

// =============================================================================
// PRODUCE LOOP
// =============================================================================

func RunPaymentProducer(ctx context.Context, producer sarama.AsyncProducer, topic string, ratePerSec int, rng *rand.Rand, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("[payment] Starting | topic=%s rate=%d/s", topic, ratePerSec)

	ticker := time.NewTicker(time.Second / time.Duration(ratePerSec))
	defer ticker.Stop()
	statsTicker := time.NewTicker(10 * time.Second)
	defer statsTicker.Stop()

	var sent, errors int64

	for {
		select {
		case <-ctx.Done():
			log.Printf("[payment] Shutdown. sent=%d errors=%d", atomic.LoadInt64(&sent), atomic.LoadInt64(&errors))
			return

		case <-statsTicker.C:
			log.Printf("[payment] Stats: sent=%d errors=%d", atomic.LoadInt64(&sent), atomic.LoadInt64(&errors))

		case <-ticker.C:
			event := generatePaymentEvent(rng)
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
