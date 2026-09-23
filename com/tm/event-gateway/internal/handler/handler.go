// Package handler nhận event qua HTTP, kiểm tra contract rồi gửi Kafka.
package handler

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"thor/com/tm/event-gateway/internal/event"
	"thor/com/tm/event-gateway/internal/producer"
)

const (
	// MaxEventsPerRequest giới hạn số event mỗi request
	MaxEventsPerRequest = 1000
	maxBodyBytes        = 1 << 20 // 1 MiB
)

// FriendEventsRequest: body của POST /v1/friend-events
type FriendEventsRequest struct {
	Events []event.FriendEvent `json:"events"`
}

// FriendEventsResponse: trả về khi Kafka đã ghi xong toàn bộ event
type FriendEventsResponse struct {
	Accepted int `json:"accepted"`
}

// ErrorResponse: body khi lỗi
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type Handler struct {
	producer    producer.Producer
	friendTopic string
	log         *slog.Logger
}

func New(p producer.Producer, friendTopic string, log *slog.Logger) *Handler {
	return &Handler{producer: p, friendTopic: friendTopic, log: log}
}

// Routes đăng ký route vào mux.
func (h *Handler) Routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", h.health)
	mux.HandleFunc("POST /v1/friend-events", h.friendEvents)
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// friendEvents: 200 khi toàn bộ event đã lên Kafka; 400 nếu có event sai contract
// (không gửi event nào); 503 nếu Kafka lỗi (client nên retry).
func (h *Handler) friendEvents(w http.ResponseWriter, r *http.Request) {
	var req FriendEventsRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", err.Error())
		return
	}
	switch n := len(req.Events); {
	case n == 0:
		writeError(w, http.StatusBadRequest, "EMPTY_EVENTS", "events không được rỗng")
		return
	case n > MaxEventsPerRequest:
		writeError(w, http.StatusBadRequest, "TOO_MANY_EVENTS", fmt.Sprintf("tối đa %d event mỗi request", MaxEventsPerRequest))
		return
	}

	msgs := make([]producer.Message, len(req.Events))
	for i, e := range req.Events {
		if err := e.Validate(); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_EVENT", fmt.Sprintf("events[%d]: %v", i, err))
			return
		}
		value, err := json.Marshal(e)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
			return
		}
		msgs[i] = producer.Message{Key: e.Key(), Value: value}
	}

	if err := h.producer.Send(r.Context(), h.friendTopic, msgs); err != nil {
		h.log.Error("kafka send failed", "err", err, "topic", h.friendTopic, "events", len(msgs))
		writeError(w, http.StatusServiceUnavailable, "KAFKA_UNAVAILABLE", "không gửi được lên Kafka, hãy thử lại")
		return
	}
	writeJSON(w, http.StatusOK, FriendEventsResponse{Accepted: len(msgs)})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, ErrorResponse{Error: code, Message: msg})
}
