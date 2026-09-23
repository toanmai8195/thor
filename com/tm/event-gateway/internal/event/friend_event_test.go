package event

import "testing"

func valid() FriendEvent {
	return FriendEvent{
		UserID:    1001,
		FriendID:  2002,
		EventType: StatusFriend,
		EventTime: "2026-09-23 10:10:00.123",
		EventID:   "0228440659126648833",
		Source:    "friend-service",
	}
}

func TestValidate_OK(t *testing.T) {
	if err := valid().Validate(); err != nil {
		t.Fatalf("event hợp lệ nhưng báo lỗi: %v", err)
	}
	e := valid()
	e.Source = "" // source không bắt buộc
	if err := e.Validate(); err != nil {
		t.Fatalf("thiếu source vẫn hợp lệ: %v", err)
	}
}

func TestValidate_Invalid(t *testing.T) {
	cases := map[string]func(*FriendEvent){
		"user_id = 0":         func(e *FriendEvent) { e.UserID = 0 },
		"friend_id âm":        func(e *FriendEvent) { e.FriendID = -1 },
		"user_id > 2^53-1":    func(e *FriendEvent) { e.UserID = 1 << 53 },
		"cùng user":           func(e *FriendEvent) { e.FriendID = e.UserID },
		"event_type lạ":       func(e *FriendEvent) { e.EventType = "HUG" },
		"event_type thường":   func(e *FriendEvent) { e.EventType = "friend" },
		"event_id ngắn":       func(e *FriendEvent) { e.EventID = "123" },
		"event_id có chữ":     func(e *FriendEvent) { e.EventID = "02284406591266488x3" },
		"event_time ISO":      func(e *FriendEvent) { e.EventTime = "2026-09-23T10:10:00.123Z" },
		"event_time thiếu ms": func(e *FriendEvent) { e.EventTime = "2026-09-23 10:10:00" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			e := valid()
			mutate(&e)
			if err := e.Validate(); err == nil {
				t.Fatal("phải báo lỗi")
			}
		})
	}
}

func TestKey(t *testing.T) {
	if got := valid().Key(); got != "1001" {
		t.Fatalf("Key() = %q, muốn 1001", got)
	}
}
