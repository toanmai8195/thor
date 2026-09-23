package handler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"thor/com/tm/event-gateway/internal/producer"
)

type fakeProducer struct {
	sent []producer.Message
	err  error
}

func (f *fakeProducer) Send(_ context.Context, topic string, msgs []producer.Message) error {
	if topic != "friend_events" {
		panic("sai topic " + topic)
	}
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, msgs...)
	return nil
}

func (f *fakeProducer) Close() error { return nil }

var errKafka = errors.New("kafka down")

const validBody = `{"events":[
  {"user_id":1,"friend_id":2,"event_type":"REQUESTED","event_time":"2026-09-23 10:00:00.000","event_id":"0228440659025985536","source":"friend-service"},
  {"user_id":2,"friend_id":1,"event_type":"REVIEWED","event_time":"2026-09-23 10:00:00.000","event_id":"0228440659025985537","source":"friend-service"}
]}`

func do(t *testing.T, p *fakeProducer, body string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	mux := http.NewServeMux()
	New(p, "friend_events", slog.New(slog.NewTextHandler(io.Discard, nil))).Routes(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/friend-events", strings.NewReader(body)))
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec, out
}

func TestFriendEvents_OK(t *testing.T) {
	p := &fakeProducer{}
	rec, out := do(t, p, validBody)
	if rec.Code != http.StatusOK || out["accepted"] != float64(2) {
		t.Fatalf("code=%d body=%v", rec.Code, out)
	}
	if len(p.sent) != 2 || p.sent[0].Key != "1" || p.sent[1].Key != "2" {
		t.Fatalf("sent=%+v", p.sent)
	}
	var first map[string]any
	_ = json.Unmarshal(p.sent[0].Value, &first)
	if first["event_type"] != "REQUESTED" || first["event_id"] != "0228440659025985536" {
		t.Fatalf("value=%s", p.sent[0].Value)
	}
}

func TestFriendEvents_InvalidEvent_SendsNothing(t *testing.T) {
	p := &fakeProducer{}
	body := strings.Replace(validBody, `"REVIEWED"`, `"HUG"`, 1)
	rec, out := do(t, p, body)
	if rec.Code != http.StatusBadRequest || out["error"] != "INVALID_EVENT" {
		t.Fatalf("code=%d body=%v", rec.Code, out)
	}
	if !strings.HasPrefix(out["message"].(string), "events[1]") {
		t.Fatalf("message phải chỉ ra event lỗi: %v", out["message"])
	}
	if len(p.sent) != 0 {
		t.Fatal("event sai thì không gửi event nào")
	}
}

func TestFriendEvents_BadRequests(t *testing.T) {
	cases := map[string]struct{ body, code string }{
		"json hỏng":    {`{"events":`, "INVALID_JSON"},
		"field lạ":     {`{"events":[],"x":1}`, "INVALID_JSON"},
		"events rỗng":  {`{"events":[]}`, "EMPTY_EVENTS"},
		"thiếu events": {`{}`, "EMPTY_EVENTS"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			rec, out := do(t, &fakeProducer{}, c.body)
			if rec.Code != http.StatusBadRequest || out["error"] != c.code {
				t.Fatalf("code=%d body=%v", rec.Code, out)
			}
		})
	}
}

func TestFriendEvents_KafkaDown_503(t *testing.T) {
	rec, out := do(t, &fakeProducer{err: errKafka}, validBody)
	if rec.Code != http.StatusServiceUnavailable || out["error"] != "KAFKA_UNAVAILABLE" {
		t.Fatalf("code=%d body=%v", rec.Code, out)
	}
}
