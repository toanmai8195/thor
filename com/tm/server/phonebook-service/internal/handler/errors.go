package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"thor/server/phonebook-service/internal/utils"
)

// writeError: DomainError → HTTP status + mã lỗi; còn lại → 500 (log chi tiết, không lộ ra client).
func writeError(w http.ResponseWriter, log *slog.Logger, err error) {
	if e, ok := utils.AsDomainError(err); ok {
		writeJSON(w, e.HTTPStatus, map[string]string{"error": e.Code, "message": e.Message})
		return
	}
	if errors.Is(err, context.Canceled) {
		return
	}
	log.Error("request failed", "err", err)
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "INTERNAL", "message": "lỗi hệ thống"})
}

func badRequest(code, format string, args ...any) error {
	return utils.BadRequest(code, format, args...)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
