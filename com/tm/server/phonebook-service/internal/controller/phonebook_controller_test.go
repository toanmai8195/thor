package controller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"thor/server/phonebook-service/internal/dao"
	"thor/server/phonebook-service/internal/dao/book"
	"thor/server/phonebook-service/internal/utils"
	"thor/server/phonebook-service/internal/utils/phonecodec"
	"thor/server/phonebook-service/internal/utils/phonedigest"
)

type fakePub struct {
	mu     sync.Mutex
	events []dao.Event
	fail   bool
}

func (p *fakePub) Publish(_ context.Context, _ int64, ev []dao.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail {
		return errors.New("kafka down")
	}
	p.events = append(p.events, ev...)
	return nil
}

func (p *fakePub) Close() error { return nil }

// take trả event đã gửi kể từ lần take trước.
func (p *fakePub) take() []dao.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.events
	p.events = nil
	return out
}

type env struct {
	svc    *PhonebookController
	st     *dao.MemoryPhonebookDao
	pub    *fakePub
	phones *phonecodec.Codec
	now    time.Time
}

func newEnv(t *testing.T) *env {
	t.Helper()
	phones, err := phonecodec.New(1, bytes.Repeat([]byte{1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	sealer, _ := book.NewSealer(1, map[byte][]byte{1: bytes.Repeat([]byte{2}, 32)})
	bc, _ := book.NewCodec(sealer)
	e := &env{st: dao.NewMemoryPhonebookDao(), pub: &fakePub{}, phones: phones, now: time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)}
	e.svc = NewPhonebookController(e.st, e.pub, phones, bc, bytes.Repeat([]byte{3}, 32), utils.NewIDGenerator(1),
		Options{LeaseTTL: time.Minute, MaxContacts: 20000, MaxBatchContacts: 5000},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	e.svc.now = func() time.Time { e.now = e.now.Add(time.Millisecond); return e.now }
	return e
}

var ctx = context.Background()

const user = int64(1001)

// sync mô phỏng app: tính root + digest bucket → check → upload các bucket đổi theo lô.
// Trả kết quả của lô cuối (hoặc nil nếu UNCHANGED).
func (e *env) sync(t *testing.T, device string, entries []phonedigest.Entry) *SyncResult {
	t.Helper()
	canon := phonedigest.Canonicalize(entries)
	d := phonedigest.ComputeBuckets(canon)
	root := d.Root()
	res, err := e.svc.Check(ctx, user, device, CheckRequest{V: 1, Root: root[:]})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status == StatusUnchanged {
		return nil
	}
	if res.Status != StatusNeedBuckets {
		t.Fatalf("check không kèm buckets: status %s", res.Status)
	}
	res, err = e.svc.Check(ctx, user, device, CheckRequest{V: 1, Root: root[:], Buckets: d.Truncated()})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusUpload {
		t.Fatalf("status %s, muốn UPLOAD", res.Status)
	}
	split := phonedigest.SplitBuckets(canon)
	var last SyncResult
	batch := map[int]BucketUpload{}
	n := 0
	flush := func() {
		if len(batch) == 0 {
			return
		}
		r, err := e.svc.Upload(ctx, user, device, UploadRequest{V: 1, Buckets: batch})
		if err != nil {
			t.Fatal(err)
		}
		last = r
		batch, n = map[int]BucketUpload{}, 0
	}
	for _, k := range res.Changed {
		if n+len(split[k]) > 2000 {
			flush()
		}
		batch[k] = BucketUpload{Digest: d[k][:4], Contacts: split[k]}
		n += len(split[k])
	}
	flush()
	if !bytes.Equal(last.Root, root[:]) {
		t.Fatalf("root server %x ≠ root app %x", last.Root, root)
	}
	return &last
}

// dwState áp các event đã gửi lên 1 "DW giả": phone_enc → active.
type dw map[uint64]bool

func (d dw) apply(evs []dao.Event) {
	for _, ev := range evs {
		d[ev.PhoneEnc] = ev.EventType == dao.EventAdd
	}
}

func (e *env) enc(t *testing.T, phone string) uint64 {
	n, err := phonecodec.Parse(phone)
	if err != nil {
		t.Fatal(err)
	}
	v, _ := e.phones.Encode(n)
	return v
}

// active: các số đang active ở DW giả, dạng 10 chữ số, sort.
func (e *env) active(t *testing.T, d dw) []string {
	var out []string
	for enc, on := range d {
		if on {
			n, err := e.phones.Decode(enc)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, phonecodec.Format(n))
		}
	}
	sort.Strings(out)
	return out
}

func entries(pairs ...string) []phonedigest.Entry {
	var out []phonedigest.Entry
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, phonedigest.Entry{Phone: pairs[i], Name: pairs[i+1]})
	}
	return out
}

const (
	X = "0366621555"
	Y = "0901234567"
	Z = "0912345678"
	W = "0987654321"
	V = "0356000011"
)

// Kịch bản PHONEBOOK.md mục 8: 2 thiết bị, Kafka lỗi giữa chừng, gửi dư, xoá thiết bị.
func TestScenarioMultiDevice(t *testing.T) {
	e := newEnv(t)
	d := dw{}
	step := func(name string, want ...string) {
		t.Helper()
		d.apply(e.pub.take())
		if got := e.active(t, d); !slices.Equal(got, want) {
			t.Fatalf("%s: DW = %v, muốn %v", name, got, want)
		}
	}

	e.sync(t, "D1", entries(X, "An", Y, "Bình", Z, "Chi"))
	step("T1", X, Y, Z)

	r := e.sync(t, "D2", entries(Z, "Chi", W, "Dũng"))
	if r.Added != 1 || r.Deleted != 0 || r.ContactCount != 4 {
		t.Fatalf("T2: %+v", r)
	}
	step("T2", X, Y, Z, W)

	e.sync(t, "D1", entries(X, "An"))
	step("T3 (Z vẫn ở D2)", X, Z, W)

	// T4: Kafka lỗi → danh bạ vẫn lưu, pending còn
	e.pub.fail = true
	r = e.sync(t, "D1", entries(X, "An", V, "Vũ"))
	if r.Published {
		t.Fatal("T4: Kafka lỗi thì published phải false")
	}
	e.pub.fail = false
	step("T4 (không event nào tới DW)", X, Z, W)
	if s, _ := e.svc.Summary(ctx, user); !s.Pending {
		t.Fatal("T4: phải còn pending")
	}

	// T5: user xoá V trên D1 rồi login lại → gửi dư DELETE V
	e.sync(t, "D1", entries(X, "An"))
	evs := e.pub.take()
	var delV bool
	for _, ev := range evs {
		if ev.PhoneEnc == e.enc(t, V) && ev.EventType == dao.EventDelete {
			delV = true
		}
	}
	if !delV {
		t.Fatalf("T5: phải gửi DELETE V dù V chưa từng tới DW, events %+v", evs)
	}
	d.apply(evs)
	step("T5", X, Z, W)
	if s, _ := e.svc.Summary(ctx, user); s.Pending || s.ContactCount != 3 {
		t.Fatalf("T5: summary %+v", s)
	}

	del, err := e.svc.DeleteDevice(ctx, user, "D2")
	if err != nil {
		t.Fatal(err)
	}
	if del.Deleted != 2 {
		t.Fatalf("T6: xoá D2 phải DELETE Z, W: %+v", del)
	}
	step("T6", X)
}

func TestCheckUnchangedAndNewDevice(t *testing.T) {
	e := newEnv(t)
	es := entries(X, "An", Y, "Bình")
	canon := phonedigest.Canonicalize(es)
	bd := phonedigest.ComputeBuckets(canon)
	root := bd.Root()

	res, _ := e.svc.Check(ctx, user, "D1", CheckRequest{V: 1, Root: root[:], Buckets: bd.Truncated()})
	if !slices.Equal(res.Changed, []int{55, 67}) {
		t.Fatalf("thiết bị mới: changed = %v, muốn chỉ bucket khác rỗng [55 67]", res.Changed)
	}
	e.sync(t, "D1", es)
	if r := e.sync(t, "D1", es); r != nil {
		t.Fatalf("danh bạ không đổi phải UNCHANGED, được %+v", r)
	}
	// thứ tự khác, tên có khoảng trắng thừa → vẫn UNCHANGED
	if r := e.sync(t, "D1", entries(Y, "  Bình ", X, "An")); r != nil {
		t.Fatal("thứ tự / khoảng trắng không được làm đổi root")
	}
}

func TestRenameOnlyUploadsOneBucketNoEvent(t *testing.T) {
	e := newEnv(t)
	e.sync(t, "D1", entries(X, "An", Y, "Bình", Z, "Chi"))
	e.pub.take()

	es := entries(X, "An", Y, "Bình Nguyễn", Z, "Chi")
	bd := phonedigest.ComputeBuckets(phonedigest.Canonicalize(es))
	root := bd.Root()
	res, _ := e.svc.Check(ctx, user, "D1", CheckRequest{V: 1, Root: root[:], Buckets: bd.Truncated()})
	if !slices.Equal(res.Changed, []int{67}) {
		t.Fatalf("đổi tên Y (bucket 67): changed = %v", res.Changed)
	}
	r := e.sync(t, "D1", es)
	if r.Added+r.Deleted != 0 || r.SyncID != "" {
		t.Fatalf("đổi tên không sinh event: %+v", r)
	}
	if evs := e.pub.take(); len(evs) != 0 {
		t.Fatalf("đổi tên không gửi Kafka: %+v", evs)
	}
	c, err := e.svc.Lookup(ctx, user, Y)
	if err != nil || c.Name != "Bình Nguyễn" {
		t.Fatalf("lookup sau đổi tên: %+v %v", c, err)
	}
}

func TestCheckRepublishesPending(t *testing.T) {
	e := newEnv(t)
	es := entries(X, "An", Y, "Bình")
	e.pub.fail = true
	e.sync(t, "D1", es)
	e.pub.fail = false
	if evs := e.pub.take(); len(evs) != 0 {
		t.Fatal("Kafka lỗi thì chưa có event")
	}
	// Login lại, danh bạ không đổi: check trả UNCHANGED nhưng server tự gửi bù
	if r := e.sync(t, "D1", es); r != nil {
		t.Fatal("phải UNCHANGED")
	}
	d := dw{}
	d.apply(e.pub.take())
	if got := e.active(t, d); !slices.Equal(got, []string{X, Y}) {
		t.Fatalf("gửi bù: DW = %v", got)
	}
	if s, _ := e.svc.Summary(ctx, user); s.Pending {
		t.Fatal("sau gửi bù không còn pending")
	}
}

func TestEventsHaveIncreasingTimeAndBatchSyncID(t *testing.T) {
	e := newEnv(t)
	e.sync(t, "D1", entries(X, "An", Y, "Bình"))
	first := e.pub.take()
	e.sync(t, "D1", entries(X, "An"))
	second := e.pub.take()
	if len(first) != 2 || len(second) != 1 {
		t.Fatalf("events %d, %d", len(first), len(second))
	}
	if first[0].SyncID != first[1].SyncID || first[0].EventTime != first[1].EventTime {
		t.Error("event cùng lần sync phải cùng sync_id và event_time")
	}
	if !(second[0].EventTime > first[0].EventTime) || !(second[0].SyncID > first[0].SyncID) {
		t.Errorf("event_time / sync_id phải tăng: %v → %v", first[0], second[0])
	}
}

func TestLeaseConflict(t *testing.T) {
	e := newEnv(t)
	e.sync(t, "D1", entries(X, "An"))
	u, _, _ := e.st.LoadUser(ctx, "1001")
	m := UserMetaOf(u)
	m.Ver, m.Lease = u.Ver+1, e.now.Add(time.Hour).UnixMilli()
	e.st.CASUserMeta(ctx, "1001", dao.Cond{Column: dao.ColVer, Value: u.Ver}, m)

	_, err := e.svc.Upload(ctx, user, "D1", UploadRequest{V: 1, Buckets: map[int]BucketUpload{55: {Contacts: entries(X, "B")}}})
	if !errors.Is(err, ErrSyncInProgress) {
		t.Fatalf("đang có lease: err = %v, muốn SYNC_IN_PROGRESS", err)
	}
	// lease hết hạn → chạy được
	e.now = e.now.Add(2 * time.Hour)
	if _, err := e.svc.Upload(ctx, user, "D1", UploadRequest{V: 1, Buckets: map[int]BucketUpload{55: {Contacts: entries(X, "B")}}}); err != nil {
		t.Fatalf("lease hết hạn: %v", err)
	}
}

func TestCASRetryOnConcurrentWrite(t *testing.T) {
	e := newEnv(t)
	e.sync(t, "D1", entries(X, "An"))
	// Lần CAS đầu tiên: 1 request khác chen vào tăng ver → phải đọc lại và thử lại
	once := true
	e.st.BeforeCAS = func() {
		if !once {
			return
		}
		once = false
		u, _, _ := e.st.LoadUser(ctx, "1001")
		m := UserMetaOf(u)
		m.Ver = u.Ver + 1
		e.st.BeforeCAS = nil
		e.st.CASUserMeta(ctx, "1001", dao.Cond{Column: dao.ColVer, Value: u.Ver}, m)
	}
	r, err := e.svc.Upload(ctx, user, "D1", UploadRequest{V: 1, Buckets: map[int]BucketUpload{67: {Contacts: entries(Y, "Bình")}}})
	if err != nil || r.Added != 1 {
		t.Fatalf("CAS hụt 1 lần phải tự thử lại: %+v %v", r, err)
	}
}

func TestUploadValidation(t *testing.T) {
	e := newEnv(t)
	cases := []struct {
		name string
		req  UploadRequest
		code string
	}{
		{"sai version", UploadRequest{V: 2, Buckets: map[int]BucketUpload{55: {}}}, "UNSUPPORTED_VERSION"},
		{"rỗng", UploadRequest{V: 1}, "EMPTY_UPLOAD"},
		{"bucket ngoài 0..99", UploadRequest{V: 1, Buckets: map[int]BucketUpload{100: {}}}, "INVALID_BUCKET"},
		{"số sai bucket", UploadRequest{V: 1, Buckets: map[int]BucketUpload{54: {Contacts: entries(X, "An")}}}, "BUCKET_MISMATCH"},
	}
	for _, c := range cases {
		_, err := e.svc.Upload(ctx, user, "D1", c.req)
		if ae, ok := utils.AsDomainError(err); !ok || ae.Code != c.code {
			t.Errorf("%s: err = %v, muốn %s", c.name, err, c.code)
		}
	}
	// số không hợp lệ bị bỏ, đếm rejected
	r, err := e.svc.Upload(ctx, user, "D1", UploadRequest{V: 1, Buckets: map[int]BucketUpload{
		55: {Contacts: entries(X, "An", "01666621555", "cũ", "+84366621555", "có +84")}}})
	if err != nil || r.Rejected != 2 || r.DeviceCount != 1 {
		t.Fatalf("rejected: %+v %v", r, err)
	}
}

func TestLimits(t *testing.T) {
	e := newEnv(t)
	e.svc.opt.MaxContacts = 3
	e.svc.opt.MaxBatchContacts = 3
	big := entries("0300000055", "a", "0300000155", "b", "0300000255", "c", "0300000355", "d")
	_, err := e.svc.Upload(ctx, user, "D1", UploadRequest{V: 1, Buckets: map[int]BucketUpload{55: {Contacts: big}}})
	if ae, ok := utils.AsDomainError(err); !ok || ae.HTTPStatus != http.StatusRequestEntityTooLarge || ae.Code != "BATCH_TOO_LARGE" {
		t.Fatalf("batch quá lớn: %v", err)
	}
	e.svc.opt.MaxBatchContacts = 10
	_, err = e.svc.Upload(ctx, user, "D1", UploadRequest{V: 1, Buckets: map[int]BucketUpload{55: {Contacts: big}}})
	if ae, ok := utils.AsDomainError(err); !ok || ae.Code != "PHONEBOOK_TOO_LARGE" {
		t.Fatalf("danh bạ quá lớn: %v", err)
	}
	// bị từ chối thì không để lại khoá
	if _, err := e.svc.Upload(ctx, user, "D1", UploadRequest{V: 1, Buckets: map[int]BucketUpload{55: {Contacts: big[:3]}}}); err != nil {
		t.Fatalf("sau lỗi 413 vẫn upload được: %v", err)
	}
}

func TestListPaginationAndMerge(t *testing.T) {
	e := newEnv(t)
	var es []phonedigest.Entry
	for i := 0; i < 1234; i++ {
		es = append(es, phonedigest.Entry{Phone: fmt.Sprintf("09%08d", i*7919%100000000), Name: fmt.Sprintf("Người %d", i)})
	}
	e.sync(t, "D1", es)
	// D2 có 1 số trùng với tên khác (sync sau → tên của D2 thắng) và 1 số mới
	e.sync(t, "D2", []phonedigest.Entry{{Phone: es[0].Phone, Name: "Tên ở D2"}, {Phone: "0399999999", Name: "Chỉ D2"}})

	seen := map[string]string{}
	var cursor uint64
	pages := 0
	for {
		res, err := e.svc.List(ctx, user, "", 500, cursor)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		if res.Total != 1235 {
			t.Fatalf("total = %d, muốn 1235", res.Total)
		}
		for _, c := range res.Contacts {
			if _, dup := seen[c.Phone]; dup {
				t.Fatalf("trùng %s giữa các trang", c.Phone)
			}
			seen[c.Phone] = c.Name
		}
		if res.NextCursor == 0 {
			break
		}
		cursor = res.NextCursor
	}
	if pages != 3 || len(seen) != 1235 {
		t.Fatalf("pages %d, contacts %d", pages, len(seen))
	}
	if seen[es[0].Phone] != "Tên ở D2" || seen[es[1].Phone] != es[1].Name {
		t.Errorf("gộp tên sai: %q %q", seen[es[0].Phone], seen[es[1].Phone])
	}
	only, _ := e.svc.List(ctx, user, "D2", 500, 0)
	if only.Total != 2 {
		t.Errorf("device_id=D2: total %d", only.Total)
	}
	if _, err := e.svc.Lookup(ctx, user, "0311111111"); !errors.Is(err, ErrNotFound) {
		t.Errorf("lookup số không có: %v", err)
	}
}

func TestLargePhonebookBatches(t *testing.T) {
	e := newEnv(t)
	var es []phonedigest.Entry
	for i := 0; i < 20000; i++ {
		es = append(es, phonedigest.Entry{Phone: fmt.Sprintf("03%08d", i*4999%100000000), Name: fmt.Sprintf("C%d", i)})
	}
	r := e.sync(t, "D1", es)
	if r.ContactCount != 20000 || r.DeviceCount != 20000 {
		t.Fatalf("20.000 contact: %+v", r)
	}
	if n := len(e.pub.take()); n != 20000 {
		t.Fatalf("events = %d", n)
	}
	if r := e.sync(t, "D1", es); r != nil {
		t.Fatal("upload xong phải UNCHANGED")
	}
}

func TestDiff(t *testing.T) {
	cases := []struct {
		union, pub, pend []uint64
		hasPend          bool
		add, del         []uint64
	}{
		{[]uint64{1, 2}, []uint64{2, 3}, nil, false, []uint64{1}, []uint64{3}},
		// pending = {1,2,4}: 4 có thể đã tới DW → DELETE 4; 1 chưa chắc tới → ADD lại
		{[]uint64{1, 2}, []uint64{2}, []uint64{1, 2, 4}, true, []uint64{1}, []uint64{4}},
		{[]uint64{2}, []uint64{2}, []uint64{2}, true, nil, nil},
	}
	for _, c := range cases {
		add, del := diff(c.union, c.pub, c.pend, c.hasPend)
		if !slices.Equal(add, c.add) || !slices.Equal(del, c.del) {
			t.Errorf("diff(%v, pub %v, pend %v) = +%v -%v, muốn +%v -%v", c.union, c.pub, c.pend, add, del, c.add, c.del)
		}
	}
}
