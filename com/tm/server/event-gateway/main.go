// event-gateway: đọc event friend-service gửi vào Kafka, kiểm tra contract rồi chuyển tiếp cho StarRocks.
//
//	friend-service ──► Kafka friend_service_events ──► event-gateway ──► Kafka friend_events ──► StarRocks
//	                                                          └─ sai contract ──► Kafka friend_events_dlq
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"thor/server/event-gateway/internal/config"
	"thor/server/event-gateway/internal/consumer"
	"thor/server/event-gateway/internal/producer"
	"thor/server/event-gateway/internal/relay"
)

const (
	kafkaConnectTimeout = 60 * time.Second
	shutdownTimeout     = 10 * time.Second
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("event-gateway stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	connectCtx, cancel := context.WithTimeout(ctx, kafkaConnectTimeout)
	p, err := producer.NewKafka(connectCtx, cfg.KafkaBrokers, cfg.KafkaClientID, log)
	cancel()
	if err != nil {
		return err
	}
	defer p.Close()
	log.Info("kafka producer connected", "brokers", cfg.KafkaBrokers)

	c, err := consumer.New(cfg.KafkaBrokers, cfg.KafkaClientID, cfg.GroupID, cfg.InputTopic,
		relay.New(p, cfg.FriendTopic, cfg.DLQTopic, log), log)
	if err != nil {
		return err
	}
	defer c.Close()

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           healthMux(c),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "err", err)
			stop()
		}
	}()

	log.Info("consuming", "group", cfg.GroupID, "input", cfg.InputTopic, "output", cfg.FriendTopic, "dlq", cfg.DLQTopic)
	runErr := c.Run(ctx)

	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	return runErr
}

// healthMux: /healthz luôn 200 khi process sống; "consuming" = đã được chia partition.
func healthMux(c *consumer.Consumer) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true, "consuming": c.Ready()})
	})
	return mux
}
