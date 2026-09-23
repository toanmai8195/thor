// event-gateway: nhận event từ các service qua HTTP, kiểm tra contract rồi gửi Kafka cho StarRocks.
//
//	friend-service ──POST /v1/friend-events──► event-gateway ──► Kafka friend_events ──► StarRocks
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"thor/com/tm/event-gateway/internal/config"
	"thor/com/tm/event-gateway/internal/handler"
	"thor/com/tm/event-gateway/internal/producer"
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
	log.Info("kafka producer connected", "brokers", cfg.KafkaBrokers, "topic", cfg.FriendTopic)

	mux := http.NewServeMux()
	handler.New(p, cfg.FriendTopic, log).Routes(mux)
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("http listening", "port", cfg.Port)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
