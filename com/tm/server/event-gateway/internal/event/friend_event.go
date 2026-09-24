// Package event định nghĩa event contract (README mục 2) và kiểm tra hợp lệ.
package event

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"time"
)

// Status của cặp user_id → friend_id, nhìn từ góc user_id (README 2.1).
const (
	StatusRequested = "REQUESTED"
	StatusReviewed  = "REVIEWED"
	StatusFriend    = "FRIEND"
	StatusCancel    = "CANCEL"
	StatusUnfriend  = "UNFRIEND"
	StatusBlocking  = "BLOCKING"
	StatusBlocked   = "BLOCKED"
)

var validStatus = map[string]bool{
	StatusRequested: true,
	StatusReviewed:  true,
	StatusFriend:    true,
	StatusCancel:    true,
	StatusUnfriend:  true,
	StatusBlocking:  true,
	StatusBlocked:   true,
}

// EventTimeLayout: yyyy-MM-dd HH:mm:ss.SSS (UTC)
const EventTimeLayout = "2006-01-02 15:04:05.000"

// maxUserID: giới hạn JSON number an toàn (2^53-1), giống friend-service
const maxUserID = 1<<53 - 1

var eventIDPattern = regexp.MustCompile(`^\d{19}$`)

// FriendEvent là 1 message trên topic friend_events (README 2.3).
type FriendEvent struct {
	UserID    int64  `json:"user_id"`
	FriendID  int64  `json:"friend_id"`
	EventType string `json:"event_type"`
	EventTime string `json:"event_time"`
	EventID   string `json:"event_id"`
	Source    string `json:"source,omitempty"`
}

// Key trả về Kafka key: user_id, để event của cùng user vào cùng partition.
func (e FriendEvent) Key() string {
	return strconv.FormatInt(e.UserID, 10)
}

// Validate kiểm tra event đúng contract.
func (e FriendEvent) Validate() error {
	switch {
	case e.UserID <= 0 || e.UserID > maxUserID:
		return fmt.Errorf("user_id phải trong khoảng 1..2^53-1")
	case e.FriendID <= 0 || e.FriendID > maxUserID:
		return fmt.Errorf("friend_id phải trong khoảng 1..2^53-1")
	case e.UserID == e.FriendID:
		return fmt.Errorf("user_id và friend_id phải khác nhau")
	case !validStatus[e.EventType]:
		return fmt.Errorf("event_type không hợp lệ: %q", e.EventType)
	case !eventIDPattern.MatchString(e.EventID):
		return fmt.Errorf("event_id phải là 19 chữ số")
	}
	if _, err := time.Parse(EventTimeLayout, e.EventTime); err != nil {
		return fmt.Errorf("event_time phải theo định dạng yyyy-MM-dd HH:mm:ss.SSS: %q", e.EventTime)
	}
	return nil
}

// Decode đọc 1 message JSON thành FriendEvent và kiểm tra contract.
// Field lạ, JSON hỏng hoặc sai contract đều trả lỗi.
func Decode(value []byte) (FriendEvent, error) {
	var e FriendEvent
	dec := json.NewDecoder(bytes.NewReader(value))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&e); err != nil {
		return FriendEvent{}, fmt.Errorf("json không hợp lệ: %w", err)
	}
	if dec.More() {
		return FriendEvent{}, fmt.Errorf("json không hợp lệ: có dữ liệu thừa sau event")
	}
	if err := e.Validate(); err != nil {
		return FriendEvent{}, err
	}
	return e, nil
}
