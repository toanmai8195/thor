package handler

import (
	"context"
	"net/http"
)

// Pinger: kiểm tra kết nối storage (MainServer nối vào dao.HealthPinger).
type Pinger func(ctx context.Context) error

// HealthHandler: GET /healthz — 200 khi ping HBase được, 503 khi không.
type HealthHandler struct {
	ping Pinger
}

func NewHealthHandler(ping Pinger) *HealthHandler {
	return &HealthHandler{ping: ping}
}

func (h *HealthHandler) Healthz(w http.ResponseWriter, r *http.Request) {
	if h.ping != nil {
		if err := h.ping(r.Context()); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ok": false, "hbase": err.Error()})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
