// friend-simulator: sinh hành động giả (mời, chấp nhận, huỷ, block...) và gọi friend-service
// với tốc độ cố định, để có event chạy qua toàn bộ luồng:
//
//	friend-simulator ──HTTP──► friend-service ──► Kafka ──► event-gateway ──► Kafka ──► StarRocks
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
	"sync"
	"syscall"
	"time"

	"thor/server/friend-simulator/internal/client"
	"thor/server/friend-simulator/internal/config"
	"thor/server/friend-simulator/internal/sim"
)

const requestTimeout = 5 * time.Second

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("friend-simulator stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg := config.Load()
	if cfg.RPS <= 0 || cfg.Users < 2 || cfg.Concurrency < 1 {
		return fmt.Errorf("cấu hình không hợp lệ: RPS > 0, USERS >= 2, CONCURRENCY >= 1")
	}
	seed := cfg.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if cfg.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Duration)
		defer cancel()
	}

	state := sim.NewState(cfg.UserIDStart, cfg.Users, seed, sim.DefaultWeights)
	stats := sim.NewStats()
	api := client.New(cfg.FriendServiceURL, requestTimeout)

	srv := &http.Server{Addr: fmt.Sprintf(":%d", cfg.Port), Handler: mux(state, stats), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "err", err)
		}
	}()

	if err := waitReady(ctx, cfg.FriendServiceURL, log); err != nil {
		return err
	}

	log.Info("simulating", "target", cfg.FriendServiceURL, "rps", cfg.RPS,
		"users", fmt.Sprintf("%d..%d", cfg.UserIDStart, cfg.UserIDStart+int64(cfg.Users)-1), "seed", seed)

	var wg sync.WaitGroup
	slots := make(chan struct{}, cfg.Concurrency)
	tick := time.NewTicker(time.Duration(float64(time.Second) / cfg.RPS))
	defer tick.Stop()
	report := time.NewTicker(cfg.StatsInterval)
	defer report.Stop()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-report.C:
			snap := stats.Snapshot(state.Counts())
			log.Info("stats", "requests", snap.Requests, "actual_rps", round(snap.ActualRPS),
				"avg_latency_ms", round(snap.AvgLatencyMs), "outcomes", snap.ByOutcome,
				"skipped", snap.Skipped, "pairs", snap.Pairs)
		case <-tick.C:
			select {
			case slots <- struct{}{}:
			default:
				stats.Skip() // friend-service chậm, hết slot song song: bỏ lượt để giữ đúng nhịp
				continue
			}
			step, ok := state.Next()
			if !ok {
				<-slots
				stats.Skip()
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-slots }()
				execute(ctx, api, state, stats, log, step)
			}()
		}
	}

	log.Info("shutting down, chờ request đang chạy")
	wg.Wait()
	snap := stats.Snapshot(state.Counts())
	log.Info("final stats", "requests", snap.Requests, "outcomes", snap.ByOutcome, "by_action", snap.ByAction, "pairs", snap.Pairs)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

// execute gọi 1 hành động; 409 / 403 nghĩa là trạng thái local lệch → lấy lại từ friend-service.
func execute(ctx context.Context, api *client.Client, state *sim.State, stats *sim.Stats, log *slog.Logger, step sim.Step) {
	// Request đang chạy vẫn chạy nốt khi shutdown để trạng thái không lệch
	reqCtx := context.WithoutCancel(ctx)
	start := time.Now()
	code, err := api.Do(reqCtx, step)
	latency := time.Since(start)

	switch {
	case err == nil && code >= 200 && code < 300:
		state.Done(step, true)
		stats.Record(step.Action, sim.OutcomeOK, latency)
	case err == nil && (code == http.StatusConflict || code == http.StatusForbidden):
		if out, rerr := api.Outgoing(reqCtx, step.Actor, step.Target); rerr == nil {
			state.Resync(step.Actor, step.Target, out)
		}
		state.Done(step, false)
		stats.Record(step.Action, sim.OutcomeConflict, latency)
	default:
		state.Done(step, false)
		stats.Record(step.Action, sim.OutcomeError, latency)
		log.Warn("request failed", "action", step.Action, "actor", step.Actor, "target", step.Target, "status", code, "err", err)
	}
}

// waitReady đợi friend-service trả /healthz 200 (vd khi cả stack vừa khởi động).
func waitReady(ctx context.Context, baseURL string, log *slog.Logger) error {
	hc := &http.Client{Timeout: 2 * time.Second}
	for {
		res, err := hc.Get(baseURL + "/healthz")
		if err == nil {
			res.Body.Close()
			if res.StatusCode == http.StatusOK {
				return nil
			}
		}
		log.Info("chờ friend-service sẵn sàng", "url", baseURL)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func mux(state *sim.State, stats *sim.Stats) *http.ServeMux {
	m := http.NewServeMux()
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]bool{"ok": true})
	})
	m.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, stats.Snapshot(state.Counts()))
	})
	return m
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func round(f float64) float64 { return float64(int(f*100)) / 100 }
