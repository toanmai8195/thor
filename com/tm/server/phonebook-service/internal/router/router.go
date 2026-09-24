// Package router: map method + path → handler. Không parse request, không gọi Controller.
package router

import (
	"expvar"
	"net/http"

	"thor/server/phonebook-service/internal/handler"
)

// New trả http.Handler của toàn bộ API.
func New(pb *handler.PhonebookHandler, health *handler.HealthHandler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/users/{userId}/devices/{deviceId}/phonebook/check", pb.Check)
	mux.HandleFunc("PUT /v1/users/{userId}/devices/{deviceId}/phonebook/buckets", pb.Upload)
	mux.HandleFunc("DELETE /v1/users/{userId}/devices/{deviceId}/phonebook", pb.DeleteDevice)
	mux.HandleFunc("GET /v1/users/{userId}/phonebook/contacts", pb.List)
	mux.HandleFunc("GET /v1/users/{userId}/phonebook/contacts/{phone}", pb.Lookup)
	mux.HandleFunc("GET /v1/users/{userId}/phonebook/summary", pb.Summary)
	mux.HandleFunc("GET /healthz", health.Healthz)
	mux.Handle("GET /debug/vars", expvar.Handler()) // metric: digest_mismatch, publish_failed, events_sent
	return mux
}
