package router

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"thor/server/phonebook-service/internal/controller"
	"thor/server/phonebook-service/internal/dao"
	"thor/server/phonebook-service/internal/dao/book"
	"thor/server/phonebook-service/internal/handler"
	"thor/server/phonebook-service/internal/utils"
	"thor/server/phonebook-service/internal/utils/phonecodec"
	"thor/server/phonebook-service/internal/utils/phonedigest"
)

var b64 = base64.RawURLEncoding

type nopPub struct{ n int }

func (p *nopPub) Publish(_ context.Context, _ int64, ev []dao.Event) error {
	p.n += len(ev)
	return nil
}
func (p *nopPub) Close() error { return nil }

func newServer(t *testing.T) (*httptest.Server, *nopPub) {
	t.Helper()
	phones, _ := phonecodec.New(1, bytes.Repeat([]byte{1}, 32))
	sealer, _ := book.NewSealer(1, map[byte][]byte{1: bytes.Repeat([]byte{2}, 32)})
	bc, _ := book.NewCodec(sealer)
	pub := &nopPub{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctrl := controller.NewPhonebookController(dao.NewMemoryPhonebookDao(), pub, phones, bc, bytes.Repeat([]byte{3}, 32),
		utils.NewIDGenerator(0), controller.Options{LeaseTTL: time.Minute, MaxContacts: 20000, MaxBatchContacts: 5000}, log)
	srv := httptest.NewServer(New(handler.NewPhonebookHandler(ctrl, log), handler.NewHealthHandler(nil)))
	t.Cleanup(srv.Close)
	return srv, pub
}

func call(t *testing.T, method, url string, body any, gz bool) (int, map[string]any) {
	t.Helper()
	var r io.Reader
	if body != nil {
		data, _ := json.Marshal(body)
		if gz {
			var buf bytes.Buffer
			w := gzip.NewWriter(&buf)
			w.Write(data)
			w.Close()
			data = buf.Bytes()
		}
		r = bytes.NewReader(data)
	}
	req, _ := http.NewRequest(method, url, r)
	if gz {
		req.Header.Set("Content-Encoding", "gzip")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestFlowOverHTTP(t *testing.T) {
	srv, pub := newServer(t)
	base := srv.URL + "/v1/users/1001"
	es := []phonedigest.Entry{{Phone: "0366621555", Name: "Mẹ"}, {Phone: "0901234567", Name: "Bố"}}
	bd := phonedigest.ComputeBuckets(phonedigest.Canonicalize(es))
	root := bd.Root()

	code, out := call(t, "POST", base+"/devices/ios-1/phonebook/check", map[string]any{"v": 1, "root": b64.EncodeToString(root[:])}, false)
	if code != 200 || out["status"] != "NEED_BUCKETS" {
		t.Fatalf("check chỉ root: %d %v", code, out)
	}
	code, out = call(t, "POST", base+"/devices/ios-1/phonebook/check", map[string]any{
		"v": 1, "root": b64.EncodeToString(root[:]), "buckets": b64.EncodeToString(bd.Truncated())}, false)
	if code != 200 || out["status"] != "UPLOAD" {
		t.Fatalf("check kèm buckets: %d %v", code, out)
	}
	upload := map[string]any{"v": 1, "buckets": map[string]any{
		"55": map[string]any{"d": b64.EncodeToString(bd[55][:4]), "contacts": es[:1]},
		"67": map[string]any{"contacts": es[1:]},
	}}
	code, out = call(t, "PUT", base+"/devices/ios-1/phonebook/buckets", upload, true)
	if code != 200 || out["root"] != b64.EncodeToString(root[:]) || out["added"] != float64(2) || out["published"] != true {
		t.Fatalf("upload gzip: %d %v", code, out)
	}
	if pub.n != 2 {
		t.Fatalf("events = %d", pub.n)
	}
	code, out = call(t, "POST", base+"/devices/ios-1/phonebook/check", map[string]any{"v": 1, "root": b64.EncodeToString(root[:])}, false)
	if out["status"] != "UNCHANGED" {
		t.Fatalf("check sau upload: %d %v", code, out)
	}
	code, out = call(t, "GET", base+"/phonebook/contacts?limit=1", nil, false)
	if code != 200 || out["total"] != float64(2) || out["next_cursor"] == nil {
		t.Fatalf("list trang 1: %d %v", code, out)
	}
	code, out = call(t, "GET", base+"/phonebook/contacts?limit=1&cursor="+out["next_cursor"].(string), nil, false)
	if code != 200 || out["next_cursor"] != nil || len(out["contacts"].([]any)) != 1 {
		t.Fatalf("list trang 2: %d %v", code, out)
	}
	code, out = call(t, "GET", base+"/phonebook/contacts/0901234567", nil, false)
	if code != 200 || out["name"] != "Bố" {
		t.Fatalf("lookup: %d %v", code, out)
	}
	code, out = call(t, "GET", base+"/phonebook/summary", nil, false)
	if code != 200 || out["contact_cnt"] != float64(2) || len(out["devices"].([]any)) != 1 {
		t.Fatalf("summary: %d %v", code, out)
	}
	code, out = call(t, "DELETE", base+"/devices/ios-1/phonebook", nil, false)
	if code != 200 || out["deleted"] != float64(2) {
		t.Fatalf("delete device: %d %v", code, out)
	}
}

func TestErrors(t *testing.T) {
	srv, _ := newServer(t)
	cases := []struct {
		method, path string
		body         any
		status       int
		code         string
	}{
		{"POST", "/v1/users/0/devices/d/phonebook/check", map[string]any{"v": 1}, 400, "INVALID_USER_ID"},
		{"POST", "/v1/users/1/devices/có dấu/phonebook/check", map[string]any{"v": 1}, 400, "INVALID_DEVICE_ID"},
		{"POST", "/v1/users/1/devices/d/phonebook/check", map[string]any{"v": 1, "root": "!!"}, 400, "INVALID_ROOT"},
		{"POST", "/v1/users/1/devices/d/phonebook/check", map[string]any{"v": 1, "root": "AAAA"}, 400, "INVALID_ROOT"},
		{"POST", "/v1/users/1/devices/d/phonebook/check", map[string]any{"v": 1, "x": 1}, 400, "INVALID_BODY"},
		{"PUT", "/v1/users/1/devices/d/phonebook/buckets", map[string]any{"v": 1, "buckets": map[string]any{"ab": map[string]any{}}}, 400, "INVALID_BUCKET"},
		{"PUT", "/v1/users/1/devices/d/phonebook/buckets", map[string]any{"v": 1, "buckets": map[string]any{"55": map[string]any{"d": "AA"}}}, 400, "INVALID_DIGEST"},
		{"GET", "/v1/users/1/phonebook/contacts?limit=5000", nil, 400, "INVALID_LIMIT"},
		{"GET", "/v1/users/1/phonebook/contacts?cursor=abc", nil, 400, "INVALID_CURSOR"},
		{"GET", "/v1/users/1/phonebook/contacts/12345", nil, 400, "INVALID_PHONE"},
		{"GET", "/v1/users/1/phonebook/contacts/0366621555", nil, 404, "NOT_FOUND"},
	}
	for _, c := range cases {
		code, out := call(t, c.method, srv.URL+c.path, c.body, false)
		if code != c.status || out["error"] != c.code {
			t.Errorf("%s %s: %d %v, muốn %d %s", c.method, c.path, code, out, c.status, c.code)
		}
	}
}
