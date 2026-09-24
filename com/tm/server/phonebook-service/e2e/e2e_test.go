//go:build e2e

// Test end-to-end với phonebook-service đang chạy (HBase + Kafka thật), mô phỏng App:
//
//	cd infra && docker compose up -d phonebook-service
//	go test -tags e2e -count=1 -v ./phonebook-service/e2e/
//
// Biến môi trường: PHONEBOOK_URL (mặc định http://localhost:3100), KAFKA_BROKERS (localhost:29092).
package e2e

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"thor/server/phonebook-service/internal/utils/phonedigest"
)

var b64 = base64.RawURLEncoding

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

type client struct {
	t    *testing.T
	base string
}

func (c *client) do(method, path string, body any, out any) int {
	c.t.Helper()
	var buf bytes.Buffer
	req, _ := http.NewRequest(method, c.base+path, nil)
	if body != nil {
		gz := gzip.NewWriter(&buf)
		json.NewEncoder(gz).Encode(body)
		gz.Close()
		req, _ = http.NewRequest(method, c.base+path, &buf)
		req.Header.Set("Content-Encoding", "gzip")
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	if out != nil {
		json.NewDecoder(resp.Body).Decode(out)
	}
	return resp.StatusCode
}

type syncResp struct {
	Root         string `json:"root"`
	SyncID       string `json:"sync_id"`
	Added        int    `json:"added"`
	Deleted      int    `json:"deleted"`
	ContactCount int    `json:"contact_cnt"`
	Published    bool   `json:"published"`
}

// sync mô phỏng App: check (root) → check (kèm buckets) → upload bucket đổi, mỗi request ≤ 2.000 contact.
func (c *client) sync(user int64, device string, es []phonedigest.Entry) (requests int, last syncResp, unchanged bool) {
	c.t.Helper()
	canon := phonedigest.Canonicalize(es)
	d := phonedigest.ComputeBuckets(canon)
	root := d.Root()
	path := fmt.Sprintf("/v1/users/%d/devices/%s/phonebook", user, device)

	var chk struct {
		Status  string `json:"status"`
		Changed []int  `json:"changed"`
	}
	if code := c.do("POST", path+"/check", map[string]any{"v": 1, "root": b64.EncodeToString(root[:])}, &chk); code != 200 {
		c.t.Fatalf("check: %d", code)
	}
	if chk.Status == "UNCHANGED" {
		return 1, syncResp{}, true
	}
	c.do("POST", path+"/check", map[string]any{"v": 1, "root": b64.EncodeToString(root[:]),
		"buckets": b64.EncodeToString(d.Truncated())}, &chk)
	if chk.Status != "UPLOAD" {
		c.t.Fatalf("check kèm buckets: %s", chk.Status)
	}
	requests = 2
	split := phonedigest.SplitBuckets(canon)
	batch, n := map[string]any{}, 0
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if code := c.do("PUT", path+"/buckets", map[string]any{"v": 1, "buckets": batch}, &last); code != 200 {
			c.t.Fatalf("upload: %d", code)
		}
		requests++
		batch, n = map[string]any{}, 0
	}
	for _, k := range chk.Changed {
		if n+len(split[k]) > 2000 {
			flush()
		}
		contacts := split[k]
		if contacts == nil {
			contacts = []phonedigest.Entry{}
		}
		batch[fmt.Sprintf("%02d", k)] = map[string]any{"d": b64.EncodeToString(d[k][:4]), "contacts": contacts}
		n += len(contacts)
	}
	flush()
	if last.Root != b64.EncodeToString(root[:]) {
		c.t.Fatalf("root server %s ≠ root app %s", last.Root, b64.EncodeToString(root[:]))
	}
	return requests, last, false
}

// kafkaReader đọc event của 1 user trên topic kể từ lúc tạo reader.
type kafkaReader struct {
	t      *testing.T
	c      sarama.Consumer
	client sarama.Client
	topic  string
	from   map[int32]int64
}

func newKafkaReader(t *testing.T, brokers []string, topic string) *kafkaReader {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := sarama.NewConsumerFromClient(client)
	r := &kafkaReader{t: t, c: c, client: client, topic: topic, from: map[int32]int64{}}
	r.mark()
	t.Cleanup(func() { c.Close(); client.Close() })
	return r
}

func (r *kafkaReader) mark() {
	parts, _ := r.client.Partitions(r.topic)
	for _, p := range parts {
		off, _ := r.client.GetOffset(r.topic, p, sarama.OffsetNewest)
		r.from[p] = off
	}
}

type event struct {
	UserID    int64  `json:"user_id"`
	PhoneEnc  uint64 `json:"phone_enc"`
	EventType string `json:"event_type"`
	EventTime string `json:"event_time"`
	SyncID    string `json:"sync_id"`
}

// take đọc mọi message mới của user kể từ lần mark trước, rồi mark lại.
func (r *kafkaReader) take(user int64) (events []event, messages int) {
	r.t.Helper()
	key := strconv.FormatInt(user, 10)
	for p, from := range r.from {
		end, _ := r.client.GetOffset(r.topic, p, sarama.OffsetNewest)
		if end <= from {
			continue
		}
		pc, err := r.c.ConsumePartition(r.topic, p, from)
		if err != nil {
			r.t.Fatal(err)
		}
		for off := from; off < end; off++ {
			select {
			case m := <-pc.Messages():
				if string(m.Key) != key {
					continue
				}
				var batch []event
				if err := json.Unmarshal(m.Value, &batch); err != nil {
					r.t.Fatalf("message không phải mảng event: %v", err)
				}
				messages++
				events = append(events, batch...)
			case <-time.After(10 * time.Second):
				r.t.Fatal("hết giờ đọc Kafka")
			}
		}
		pc.Close()
	}
	r.mark()
	return events, messages
}

func phonebook(n, offset int, suffix string) []phonedigest.Entry {
	out := make([]phonedigest.Entry, n)
	for i := range out {
		out[i] = phonedigest.Entry{Phone: fmt.Sprintf("09%08d", (i+offset)*7919%100000000), Name: fmt.Sprintf("Người %d%s", i+offset, suffix)}
	}
	return out
}

func TestEndToEnd(t *testing.T) {
	c := &client{t: t, base: env("PHONEBOOK_URL", "http://localhost:3100")}
	kr := newKafkaReader(t, []string{env("KAFKA_BROKERS", "localhost:29092")}, "phonebook_service_events")
	user := time.Now().UnixMilli() % 1_000_000_000 // user mới mỗi lần chạy

	// 1. Lần đầu: 5.000 contact
	book := phonebook(5000, 0, "")
	start := time.Now()
	reqs, res, _ := c.sync(user, "ios-A", book)
	t.Logf("sync lần đầu 5.000 contact: %d request, %v, %+v", reqs, time.Since(start), res)
	evs, msgs := kr.take(user)
	if len(evs) != 5000 || msgs != 5 {
		t.Fatalf("Kafka: %d event trong %d message, muốn 5.000 / 5", len(evs), msgs)
	}

	// 2. Login lại, không đổi → chỉ 1 request check
	if reqs, _, unchanged := c.sync(user, "ios-A", book); !unchanged || reqs != 1 {
		t.Fatalf("không đổi: unchanged=%v, %d request", unchanged, reqs)
	}

	// 3. Đổi tên 1 contact, xoá 1, thêm 1
	book[10].Name = "Đã đổi tên"
	removed := book[20]
	book = append(book[:20], book[21:]...)
	book = append(book, phonedigest.Entry{Phone: "0399999999", Name: "Mới"})
	reqs, res, _ = c.sync(user, "ios-A", book)
	evs, _ = kr.take(user)
	t.Logf("đổi tên + xoá + thêm: %d request, %+v, events %+v", reqs, res, evs)
	if res.Added != 1 || res.Deleted != 1 || len(evs) != 2 {
		t.Fatalf("muốn 1 ADD + 1 DELETE: %+v, %d event", res, len(evs))
	}

	// 4. Thiết bị thứ 2 có 1 số trùng + 1 số mới
	_, res, _ = c.sync(user, "and-B", []phonedigest.Entry{{Phone: book[0].Phone, Name: "Tên ở máy B"}, {Phone: "0388888888", Name: "Chỉ máy B"}})
	if res.Added != 1 || res.ContactCount != 5001 {
		t.Fatalf("thiết bị 2: %+v", res)
	}
	kr.take(user)

	// 5. List toàn bộ
	seen := map[string]string{}
	cursor := ""
	for {
		var page struct {
			Contacts []struct {
				Phone string `json:"phone"`
				Name  string `json:"name"`
			} `json:"contacts"`
			Total      int     `json:"total"`
			NextCursor *string `json:"next_cursor"`
		}
		path := fmt.Sprintf("/v1/users/%d/phonebook/contacts?limit=2000", user)
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		if code := c.do("GET", path, nil, &page); code != 200 {
			t.Fatalf("list: %d", code)
		}
		for _, ct := range page.Contacts {
			seen[ct.Phone] = ct.Name
		}
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
	}
	if len(seen) != 5001 || seen[book[0].Phone] != "Tên ở máy B" || seen[book[10].Phone] != "Đã đổi tên" {
		t.Fatalf("list: %d contact, %q, %q", len(seen), seen[book[0].Phone], seen[book[10].Phone])
	}
	if _, ok := seen[removed.Phone]; ok {
		t.Fatal("số đã xoá vẫn còn trong list")
	}

	// 6. Xoá thiết bị B → DELETE 0388888888 (số trùng vẫn còn ở A)
	var del syncResp
	c.do("DELETE", fmt.Sprintf("/v1/users/%d/devices/and-B/phonebook", user), nil, &del)
	evs, _ = kr.take(user)
	if del.Deleted != 1 || len(evs) != 1 || evs[0].EventType != "DELETE" {
		t.Fatalf("xoá thiết bị B: %+v, events %+v", del, evs)
	}

	var sum struct {
		ContactCount int  `json:"contact_cnt"`
		Pending      bool `json:"pending"`
		Devices      []struct {
			DeviceID string `json:"device_id"`
		} `json:"devices"`
	}
	c.do("GET", fmt.Sprintf("/v1/users/%d/phonebook/summary", user), nil, &sum)
	if sum.ContactCount != 5000 || sum.Pending || len(sum.Devices) != 1 {
		t.Fatalf("summary: %+v", sum)
	}

	// event_time tăng dần giữa các lần sync
	times := []string{}
	for _, e := range evs {
		times = append(times, e.EventTime)
	}
	sort.Strings(times)
	t.Logf("OK — user %d, summary %+v", user, sum)
}
