package relay

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"thor/com/tm/event-gateway/internal/producer"
)

type fakeProducer struct {
	sent     []producer.Message
	failures int // số lần Send lỗi trước khi thành công
}

func (f *fakeProducer) Send(_ context.Context, m producer.Message) error {
	if f.failures > 0 {
		f.failures--
		return errors.New("kafka down")
	}
	f.sent = append(f.sent, m)
	return nil
}

func (f *fakeProducer) Close() error { return nil }

func newRelay(p producer.Producer) *Relay {
	r := New(p, "friend_events", "friend_events_dlq", slog.New(slog.NewTextHandler(io.Discard, nil)))
	r.retryBackoff = time.Millisecond
	r.maxRetryBackoff = time.Millisecond
	return r
}

var src = Source{Topic: "friend_service_events", Partition: 2, Offset: 41}

const validEvent = `{"source":"friend-service","user_id":1,"friend_id":2,"event_type":"REQUESTED","event_time":"2026-09-23 10:00:00.000","event_id":"0228440659025985536"}`

func TestHandle_Valid_GoesToFriendTopic(t *testing.T) {
	p := &fakeProducer{}
	// key đầu vào sai vẫn dùng user_id làm key đầu ra
	if err := newRelay(p).Handle(context.Background(), []byte("wrong"), []byte(validEvent), src); err != nil {
		t.Fatal(err)
	}
	if len(p.sent) != 1 {
		t.Fatalf("sent=%d", len(p.sent))
	}
	m := p.sent[0]
	if m.Topic != "friend_events" || m.Key != "1" || len(m.Headers) != 0 {
		t.Fatalf("message=%+v", m)
	}
	var got map[string]any
	_ = json.Unmarshal(m.Value, &got)
	if got["event_type"] != "REQUESTED" || got["event_id"] != "0228440659025985536" || got["user_id"] != float64(1) {
		t.Fatalf("value=%s", m.Value)
	}
}

func TestHandle_Invalid_GoesToDLQ(t *testing.T) {
	p := &fakeProducer{}
	bad := `{"user_id":1,"friend_id":2,"event_type":"HUG","event_time":"2026-09-23 10:00:00.000","event_id":"0228440659025985536"}`
	if err := newRelay(p).Handle(context.Background(), []byte("1"), []byte(bad), src); err != nil {
		t.Fatal(err)
	}
	m := p.sent[0]
	if m.Topic != "friend_events_dlq" || m.Key != "1" || string(m.Value) != bad {
		t.Fatalf("message=%+v", m)
	}
	if m.Headers[HeaderError] == "" || m.Headers[HeaderSourceTopic] != "friend_service_events" ||
		m.Headers[HeaderSourcePartition] != "2" || m.Headers[HeaderSourceOffset] != "41" {
		t.Fatalf("headers=%v", m.Headers)
	}
}

func TestHandle_KafkaDown_RetriesUntilSent(t *testing.T) {
	p := &fakeProducer{failures: 3}
	if err := newRelay(p).Handle(context.Background(), nil, []byte(validEvent), src); err != nil {
		t.Fatal(err)
	}
	if len(p.sent) != 1 {
		t.Fatalf("sent=%d", len(p.sent))
	}
}

func TestHandle_ContextCancelled_StopsRetry(t *testing.T) {
	p := &fakeProducer{failures: 1 << 30}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := newRelay(p).Handle(ctx, nil, []byte(validEvent), src); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
	if len(p.sent) != 0 {
		t.Fatal("không được gửi")
	}
}
