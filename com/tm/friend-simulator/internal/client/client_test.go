package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"thor/com/tm/friend-simulator/internal/sim"
)

func TestDo_Endpoints(t *testing.T) {
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.Method+" "+r.URL.Path)
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()
	c := New(srv.URL, time.Second)

	steps := []sim.Step{
		{Action: sim.ActRequest, Actor: 1, Target: 2},
		{Action: sim.ActCancel, Actor: 1, Target: 2},
		{Action: sim.ActAccept, Actor: 2, Target: 1},
		{Action: sim.ActReject, Actor: 2, Target: 1},
		{Action: sim.ActUnfriend, Actor: 1, Target: 2},
		{Action: sim.ActBlock, Actor: 1, Target: 2},
	}
	want := []string{
		"POST /v1/users/1/requests/2",
		"DELETE /v1/users/1/requests/2",
		"POST /v1/users/2/requests/1/accept",
		"POST /v1/users/2/requests/1/reject",
		"DELETE /v1/users/1/friends/2",
		"POST /v1/users/1/blocks/2",
	}
	for _, s := range steps {
		if code, err := c.Do(context.Background(), s); err != nil || code != http.StatusCreated {
			t.Fatalf("%+v: code=%d err=%v", s, code, err)
		}
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("[%d] got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestOutgoing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/users/1/relationships/2" {
			_, _ = w.Write([]byte(`{"user_id":1,"friend_id":2,"outgoing":{"status":"BLOCKED"},"incoming":{"status":"BLOCKING"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"outgoing":null,"incoming":null}`))
	}))
	defer srv.Close()
	c := New(srv.URL, time.Second)
	if s, err := c.Outgoing(context.Background(), 1, 2); err != nil || s != "BLOCKED" {
		t.Fatalf("s=%q err=%v", s, err)
	}
	if s, err := c.Outgoing(context.Background(), 3, 4); err != nil || s != "" {
		t.Fatalf("chưa có quan hệ: s=%q err=%v", s, err)
	}
}
