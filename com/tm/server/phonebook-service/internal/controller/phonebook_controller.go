// Package controller: nghiệp vụ đồng bộ danh bạ (PHONEBOOK.md mục 2.2–2.5). Không biết về HTTP.
//
//	check  → so root / digest bucket với HBase, trả bucket cần upload
//	upload → thay nội dung các bucket của 1 thiết bị, hợp các thiết bị, diff với cái DW đã có,
//	         gửi ADD / DELETE lên Kafka
//	list / lookup / summary → đọc danh bạ đã lưu
package controller

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"expvar"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"time"

	"thor/server/phonebook-service/internal/dao"
	"thor/server/phonebook-service/internal/dao/book"
	"thor/server/phonebook-service/internal/utils"
	"thor/server/phonebook-service/internal/utils/phonecodec"
	"thor/server/phonebook-service/internal/utils/phonedigest"
	"thor/server/phonebook-service/internal/utils/phoneset"
)

// Kết quả của `check`
const (
	StatusUnchanged   = "UNCHANGED"
	StatusNeedBuckets = "NEED_BUCKETS"
	StatusUpload      = "UPLOAD"
)

// casRetries: số lần thử lại khi CheckAndPut hụt vì request khác của cùng user vừa ghi
const casRetries = 3

var (
	metricDigestMismatch = expvar.NewInt("phonebook_digest_mismatch")
	metricPublishFailed  = expvar.NewInt("phonebook_publish_failed")
	metricEventsSent     = expvar.NewInt("phonebook_events_sent")
)

func badRequest(code, format string, args ...any) error {
	return utils.BadRequest(code, format, args...)
}

var (
	ErrSyncInProgress = utils.NewDomainError(http.StatusConflict, "SYNC_IN_PROGRESS",
		"user đang có 1 lần sync khác, thử lại sau vài giây")
	ErrNotFound = utils.NewDomainError(http.StatusNotFound, "NOT_FOUND", "không có trong danh bạ")
)

// Options: giới hạn và thời gian khoá.
type Options struct {
	LeaseTTL         time.Duration
	MaxContacts      int
	MaxBatchContacts int
}

// PhonebookController: nghiệp vụ check / upload / xoá thiết bị / đọc danh bạ.
type PhonebookController struct {
	dao       dao.PhonebookDao
	pub       dao.EventPublisher
	phones    *phonecodec.Codec
	book      *book.Codec
	digestKey []byte
	ids       *utils.IDGenerator
	opt       Options
	log       *slog.Logger
	now       func() time.Time
}

func NewPhonebookController(d dao.PhonebookDao, pub dao.EventPublisher, phones *phonecodec.Codec, bc *book.Codec,
	digestKey []byte, ids *utils.IDGenerator, opt Options, log *slog.Logger) *PhonebookController {
	return &PhonebookController{dao: d, pub: pub, phones: phones, book: bc, digestKey: digestKey,
		ids: ids, opt: opt, log: log, now: time.Now}
}

// rootMAC: HMAC(root) lưu ở m:root — không lưu root trần (danh bạ ít số dò ngược được).
func (s *PhonebookController) rootMAC(root []byte) []byte {
	m := hmac.New(sha256.New, s.digestKey)
	m.Write(root)
	return m.Sum(nil)
}

func uid(userID int64) string { return strconv.FormatInt(userID, 10) }

// ---------------------------------------------------------------------------
// check
// ---------------------------------------------------------------------------

type CheckRequest struct {
	V       int
	Root    []byte // 32 byte
	Buckets []byte // nil, hoặc 400 byte = 100 × 4 byte đầu digest bucket
}

type CheckResult struct {
	Status  string
	Changed []int
}

func (s *PhonebookController) Check(ctx context.Context, userID int64, deviceID string, req CheckRequest) (CheckResult, error) {
	if req.V != phonedigest.Version {
		return CheckResult{}, badRequest("UNSUPPORTED_VERSION", "v phải là %d", phonedigest.Version)
	}
	if len(req.Root) != sha256.Size {
		return CheckResult{}, badRequest("INVALID_ROOT", "root phải 32 byte")
	}
	if req.Buckets != nil && len(req.Buckets) != phonedigest.Buckets*phonedigest.TruncLen {
		return CheckResult{}, badRequest("INVALID_BUCKETS", "buckets phải %d byte", phonedigest.Buckets*phonedigest.TruncLen)
	}
	meta, dev, err := s.dao.LoadCheck(ctx, uid(userID), deviceID)
	if err != nil {
		return CheckResult{}, err
	}
	if dev != nil && dev.DigestV == phonedigest.Version && hmac.Equal(dev.RootMAC, s.rootMAC(req.Root)) {
		// Danh bạ không đổi. Còn pending (lần trước gửi Kafka lỗi) → tự gửi bù, app không cần upload.
		if meta.PendingSync != 0 && meta.Lease < s.now().UnixMilli() {
			if _, err := s.reconcile(ctx, userID, nil); err != nil {
				s.log.Warn("gửi bù pending lỗi", "user_id", userID, "err", err)
			}
		}
		return CheckResult{Status: StatusUnchanged}, nil
	}
	if req.Buckets == nil {
		return CheckResult{Status: StatusNeedBuckets}, nil
	}
	var stored phonedigest.BucketDigests
	if dev != nil && dev.DigestV == phonedigest.Version {
		if stored, err = s.book.OpenDigests(dev.Digests, []byte(dao.RowKey(uid(userID), deviceID))); err != nil {
			return CheckResult{}, err
		}
	} else {
		stored, _ = s.book.OpenDigests(nil, nil)
	}
	var changed []int
	for k := 0; k < phonedigest.Buckets; k++ {
		app := req.Buckets[k*phonedigest.TruncLen : (k+1)*phonedigest.TruncLen]
		if !hmac.Equal(app, stored[k][:phonedigest.TruncLen]) {
			changed = append(changed, k)
		}
	}
	if len(changed) == 0 {
		// root lệch mà không bucket nào lệch (trùng 4 byte, rất hiếm) → gửi lại tất cả
		for k := 0; k < phonedigest.Buckets; k++ {
			changed = append(changed, k)
		}
	}
	return CheckResult{Status: StatusUpload, Changed: changed}, nil
}

// ---------------------------------------------------------------------------
// upload
// ---------------------------------------------------------------------------

type BucketUpload struct {
	Digest   []byte // tuỳ chọn: 4 byte đầu digest bucket app tính, để phát hiện app tính lệch
	Contacts []phonedigest.Entry
}

type UploadRequest struct {
	V       int
	Buckets map[int]BucketUpload
}

type SyncResult struct {
	Root         []byte // root do server tính sau khi ghi (app so với root của mình)
	SyncID       string // rỗng nếu không có event
	Added        int
	Deleted      int
	ContactCount int // số contact của user (hợp mọi thiết bị)
	DeviceCount  int // số contact của thiết bị
	Rejected     int // số entry bị bỏ vì số không hợp lệ
	Published    bool
}

type preparedBucket struct {
	contacts []book.Contact
	digest   [sha256.Size]byte
}

func (s *PhonebookController) Upload(ctx context.Context, userID int64, deviceID string, req UploadRequest) (SyncResult, error) {
	if req.V != phonedigest.Version {
		return SyncResult{}, badRequest("UNSUPPORTED_VERSION", "v phải là %d", phonedigest.Version)
	}
	if len(req.Buckets) == 0 {
		return SyncResult{}, badRequest("EMPTY_UPLOAD", "phải có ít nhất 1 bucket")
	}
	total := 0
	for k, b := range req.Buckets {
		if k < 0 || k >= phonedigest.Buckets {
			return SyncResult{}, badRequest("INVALID_BUCKET", "bucket %d ngoài 0..99", k)
		}
		total += len(b.Contacts)
	}
	if total > s.opt.MaxBatchContacts {
		return SyncResult{}, utils.NewDomainError(http.StatusRequestEntityTooLarge, "BATCH_TOO_LARGE",
			"1 request tối đa %d contact, gửi %d", s.opt.MaxBatchContacts, total)
	}

	// Chuẩn hoá + FF1 trước khi lấy khoá (phần tốn CPU nhất)
	prepared := make(map[int]preparedBucket, len(req.Buckets))
	rejected := 0
	for k, b := range req.Buckets {
		p, rej, err := s.prepareBucket(userID, deviceID, k, b)
		if err != nil {
			return SyncResult{}, err
		}
		prepared[k] = p
		rejected += rej
	}

	var root []byte
	var deviceCount int
	res, err := s.reconcile(ctx, userID, func(devs []dao.DeviceRow) (*deviceChange, error) {
		row, count, r, err := s.applyBuckets(userID, deviceID, devs, prepared)
		if err != nil {
			return nil, err
		}
		root, deviceCount = r, count
		return &deviceChange{put: row}, nil
	})
	if err != nil {
		return SyncResult{}, err
	}
	res.Root, res.DeviceCount, res.Rejected = root, deviceCount, rejected
	return res, nil
}

func (s *PhonebookController) prepareBucket(userID int64, deviceID string, k int, b BucketUpload) (preparedBucket, int, error) {
	rejected := 0
	for _, e := range b.Contacts {
		if _, err := phonecodec.Parse(e.Phone); err != nil {
			rejected++
		}
	}
	canon := phonedigest.Canonicalize(b.Contacts)
	p := preparedBucket{digest: phonedigest.BucketDigest(canon), contacts: make([]book.Contact, 0, len(canon))}
	for _, e := range canon {
		if phonedigest.BucketOf(e.Phone) != k {
			return p, 0, badRequest("BUCKET_MISMATCH", "số %s không thuộc bucket %02d", e.Phone, k)
		}
		n, _ := phonecodec.Parse(e.Phone)
		enc, err := s.phones.Encode(n)
		if err != nil {
			return p, 0, err
		}
		p.contacts = append(p.contacts, book.Contact{Enc: enc, Name: e.Name})
	}
	if b.Digest != nil && !hmac.Equal(b.Digest, p.digest[:phonedigest.TruncLen]) {
		// App chuẩn hoá khác server: vẫn nhận (server lưu digest do mình tính), báo metric để sửa app
		metricDigestMismatch.Add(1)
		s.log.Warn("digest bucket app gửi khác server tính", "user_id", userID, "device_id", deviceID, "bucket", k)
	}
	return p, rejected, nil
}

// applyBuckets: danh bạ mới của thiết bị = bản đang lưu, thay các bucket vừa upload.
func (s *PhonebookController) applyBuckets(userID int64, deviceID string, devs []dao.DeviceRow,
	prepared map[int]preparedBucket) (*dao.DeviceRow, int, []byte, error) {
	key := []byte(dao.RowKey(uid(userID), deviceID))
	b := &book.Book{}
	digests, _ := s.book.OpenDigests(nil, nil)
	for _, d := range devs {
		if d.DeviceID != deviceID {
			continue
		}
		var err error
		if b, err = s.book.Decode(d.Phones, d.Names, key); err != nil {
			return nil, 0, nil, fmt.Errorf("đọc danh bạ thiết bị %s: %w", deviceID, err)
		}
		if d.DigestV == phonedigest.Version {
			if digests, err = s.book.OpenDigests(d.Digests, key); err != nil {
				return nil, 0, nil, err
			}
		} else {
			// Digest version cũ: không dùng được → root sẽ lệch, lần check sau app được yêu cầu
			// upload mọi bucket và digest được tính lại hết theo version mới
			digests, _ = s.book.OpenDigests(nil, nil)
		}
	}
	for k, p := range prepared {
		b.SetBucket(k, p.contacts)
		digests[k] = p.digest
	}
	if n := b.Count(); n > s.opt.MaxContacts {
		return nil, 0, nil, utils.NewDomainError(http.StatusRequestEntityTooLarge, "PHONEBOOK_TOO_LARGE",
			"1 thiết bị tối đa %d contact, sau khi ghi là %d", s.opt.MaxContacts, n)
	}
	root := digests.Root()
	phones, names, err := s.book.Encode(b, key)
	if err != nil {
		return nil, 0, nil, err
	}
	bd, err := s.book.SealDigests(&digests, key)
	if err != nil {
		return nil, 0, nil, err
	}
	return &dao.DeviceRow{
		DeviceID: deviceID,
		RootMAC:  s.rootMAC(root[:]),
		Count:    int64(b.Count()),
		SyncedAt: s.now().UnixMilli(),
		DigestV:  phonedigest.Version,
		Phones:   phones,
		Names:    names,
		Digests:  bd,
	}, b.Count(), root[:], nil
}

// DeleteDevice: logout / thu hồi quyền danh bạ — bỏ danh bạ của thiết bị, số chỉ có ở thiết bị này → DELETE.
func (s *PhonebookController) DeleteDevice(ctx context.Context, userID int64, deviceID string) (SyncResult, error) {
	return s.reconcile(ctx, userID, func(devs []dao.DeviceRow) (*deviceChange, error) {
		for _, d := range devs {
			if d.DeviceID == deviceID {
				return &deviceChange{deleteID: deviceID}, nil
			}
		}
		return nil, nil // thiết bị chưa từng sync: vẫn chạy để gửi bù pending nếu có
	})
}

// ---------------------------------------------------------------------------
// reconcile: khoá theo user → ghi thiết bị → hợp các thiết bị → diff → Kafka
// ---------------------------------------------------------------------------

type deviceChange struct {
	put      *dao.DeviceRow
	deleteID string
}

// reconcile là luồng chung của upload / xoá thiết bị / gửi bù (change = nil).
//
//  1. LoadUser; lease còn hạn → SYNC_IN_PROGRESS
//  2. change(devs) → row thiết bị mới
//  3. lấy khoá: CheckAndPut(ver == v) lease = now + TTL, ver = v+1   (hụt → làm lại từ 1)
//  4. ghi / xoá row thiết bị
//  5. U = hợp các thiết bị; diff với published (và pending nếu còn)
//  6. không có thay đổi → CheckAndPut(ver == v+1) published = U, bỏ khoá
//  7. có → CheckAndPut(ver == v+1) pending = U, psync; gửi Kafka;
//     CheckAndPut(ver == v+2) published = U, bỏ pending, bỏ khoá
//
// Mọi CheckAndPut đều so trên ver: ai lấy khoá sau khi khoá của mình hết hạn đều đã tăng ver,
// nên ghi muộn của mình tự bị từ chối, trạng thái còn lại (pending) được lần sync sau xử lý.
func (s *PhonebookController) reconcile(ctx context.Context, userID int64,
	change func(devs []dao.DeviceRow) (*deviceChange, error)) (SyncResult, error) {
	id := uid(userID)
	for attempt := 0; attempt < casRetries; attempt++ {
		user, devs, err := s.dao.LoadUser(ctx, id)
		if err != nil {
			return SyncResult{}, err
		}
		nowMs := s.now().UnixMilli()
		if user.Lease > nowMs {
			return SyncResult{}, ErrSyncInProgress
		}
		var ch *deviceChange
		if change != nil {
			if ch, err = change(devs); err != nil {
				return SyncResult{}, err
			}
		}
		v := user.Ver
		lease := UserMetaOf(user)
		lease.Ver, lease.Lease = v+1, nowMs+s.opt.LeaseTTL.Milliseconds()
		ok, err := s.dao.CASUserMeta(ctx, id, dao.Cond{Column: dao.ColVer, Value: v}, lease)
		if err != nil {
			return SyncResult{}, err
		}
		if !ok {
			continue // request khác của user vừa ghi: đọc lại
		}
		return s.syncLocked(ctx, userID, user, devs, ch, lease)
	}
	return SyncResult{}, ErrSyncInProgress
}

func UserMetaOf(u dao.UserRow) dao.UserMeta {
	return dao.UserMeta{Ver: u.Ver, Lease: u.Lease, PendingSync: u.PendingSync, Count: u.Count, LastEvent: u.LastEvent}
}

func (s *PhonebookController) syncLocked(ctx context.Context, userID int64, user dao.UserRow, devs []dao.DeviceRow,
	ch *deviceChange, lease dao.UserMeta) (SyncResult, error) {
	id := uid(userID)
	v := lease.Ver // = user.Ver + 1

	// 4. ghi thiết bị (dưới khoá)
	if ch != nil {
		var err error
		switch {
		case ch.put != nil:
			err = s.dao.PutDevice(ctx, id, *ch.put)
		case ch.deleteID != "":
			err = s.dao.DeleteDevice(ctx, id, ch.deleteID)
		}
		if err != nil {
			s.release(ctx, id, lease)
			return SyncResult{}, err
		}
		devs = applyChange(devs, ch)
	}

	// 5. hợp các thiết bị, diff
	union, err := unionPhones(devs)
	if err != nil {
		s.release(ctx, id, lease)
		return SyncResult{}, err
	}
	published, err := phoneset.Decode(user.Published)
	if err != nil {
		s.release(ctx, id, lease)
		return SyncResult{}, err
	}
	var pending []uint64
	if user.PendingSync != 0 {
		if pending, err = phoneset.Decode(user.Pending); err != nil {
			s.release(ctx, id, lease)
			return SyncResult{}, err
		}
	}
	add, del := diff(union, published, pending, user.PendingSync != 0)
	unionBlob, _ := phoneset.Encode(union)
	res := SyncResult{Added: len(add), Deleted: len(del), ContactCount: len(union)}

	// 6. không có gì gửi DW
	if len(add) == 0 && len(del) == 0 {
		done := dao.UserRow{Ver: v + 1, Count: int64(len(union)), LastEvent: user.LastEvent, Published: unionBlob}
		ok, err := s.dao.CASUser(ctx, id, dao.Cond{Column: dao.ColVer, Value: v}, done)
		if err != nil {
			return SyncResult{}, err
		}
		if !ok {
			s.log.Warn("mất khoá trước khi chốt (quá hạn lease)", "user_id", userID)
		}
		res.Published = ok
		return res, nil
	}

	// 7. ghi pending → Kafka → chốt published
	syncID := s.ids.Next()
	eventTime := max(s.now().UnixMilli(), user.LastEvent+1)
	pend := dao.UserRow{Ver: v + 1, Lease: lease.Lease, PendingSync: syncID, Count: user.Count,
		LastEvent: eventTime, Published: user.Published, Pending: unionBlob}
	ok, err := s.dao.CASUser(ctx, id, dao.Cond{Column: dao.ColVer, Value: v}, pend)
	if err != nil {
		return SyncResult{}, err
	}
	if !ok {
		s.log.Warn("mất khoá trước khi ghi pending (quá hạn lease)", "user_id", userID)
		return res, nil
	}
	res.SyncID = utils.FormatID(syncID)
	events := buildEvents(userID, add, del, eventTime, res.SyncID)
	if err := s.pub.Publish(ctx, userID, events); err != nil {
		// Danh bạ đã lưu; pending còn lại → lần check / sync sau gửi bù. App vẫn nhận 200.
		metricPublishFailed.Add(1)
		s.log.Error("gửi Kafka lỗi, để pending", "user_id", userID, "sync_id", res.SyncID, "err", err)
		unlock := UserMetaOf(pend)
		unlock.Ver, unlock.Lease = v+2, 0
		if _, err := s.dao.CASUserMeta(ctx, id, dao.Cond{Column: dao.ColVer, Value: v + 1}, unlock); err != nil {
			s.log.Warn("bỏ khoá lỗi, chờ hết hạn", "user_id", userID, "err", err)
		}
		return res, nil
	}
	metricEventsSent.Add(int64(len(events)))
	done := dao.UserRow{Ver: v + 2, Count: int64(len(union)), LastEvent: eventTime, Published: unionBlob}
	if ok, err := s.dao.CASUser(ctx, id, dao.Cond{Column: dao.ColVer, Value: v + 1}, done); err != nil || !ok {
		// Event đã gửi nhưng chưa chốt: pending còn → lần sau gửi lại (trùng, không sai DW)
		s.log.Warn("chưa chốt được published sau khi gửi Kafka", "user_id", userID, "ok", ok, "err", err)
	}
	res.Published = true
	return res, nil
}

// release: bỏ khoá khi lỗi giữa chừng (không đổi published / pending).
func (s *PhonebookController) release(ctx context.Context, id string, lease dao.UserMeta) {
	unlock := lease
	unlock.Ver, unlock.Lease = lease.Ver+1, 0
	if _, err := s.dao.CASUserMeta(ctx, id, dao.Cond{Column: dao.ColVer, Value: lease.Ver}, unlock); err != nil {
		s.log.Warn("bỏ khoá lỗi, chờ hết hạn", "user_id", id, "err", err)
	}
}

func applyChange(devs []dao.DeviceRow, ch *deviceChange) []dao.DeviceRow {
	out := make([]dao.DeviceRow, 0, len(devs)+1)
	replaced := false
	for _, d := range devs {
		switch {
		case ch.deleteID != "" && d.DeviceID == ch.deleteID:
			continue
		case ch.put != nil && d.DeviceID == ch.put.DeviceID:
			out = append(out, *ch.put)
			replaced = true
		default:
			out = append(out, d)
		}
	}
	if ch.put != nil && !replaced {
		out = append(out, *ch.put)
	}
	return out
}

// unionPhones: hợp phone_enc của mọi thiết bị (chỉ đọc b:ph, không mở tên).
func unionPhones(devs []dao.DeviceRow) ([]uint64, error) {
	var all []uint64
	for _, d := range devs {
		encs, err := book.PhonesOnly(d.Phones)
		if err != nil {
			return nil, fmt.Errorf("thiết bị %s: %w", d.DeviceID, err)
		}
		all = append(all, encs...)
	}
	return phoneset.Normalize(all), nil
}

// diff: U là tập muốn có ở DW.
//
//	không pending: ADD = U − published                DEL = published − U
//	có pending:    ADD = U − (published ∩ pending)    DEL = (published ∪ pending) − U
//
// Có pending = lần trước gửi Kafka dở, không biết DW đang ở published hay pending → gửi dư.
func diff(union, published, pending []uint64, hasPending bool) (add, del []uint64) {
	sure := published  // chắc chắn DW có
	maybe := published // có thể DW đang giữ
	if hasPending {
		sure = intersect(published, pending)
		maybe = phoneset.Normalize(append(slices.Clone(published), pending...))
	}
	add, _ = phoneset.Diff(sure, union)
	_, del = phoneset.Diff(maybe, union)
	return add, del
}

func intersect(a, b []uint64) []uint64 {
	var out []uint64
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			out = append(out, a[i])
			i++
			j++
		case a[i] < b[j]:
			i++
		default:
			j++
		}
	}
	return out
}

const eventTimeLayout = "2006-01-02 15:04:05.000"

func buildEvents(userID int64, add, del []uint64, eventTimeMs int64, syncID string) []dao.Event {
	t := time.UnixMilli(eventTimeMs).UTC().Format(eventTimeLayout)
	out := make([]dao.Event, 0, len(add)+len(del))
	for _, e := range add {
		out = append(out, dao.Event{UserID: userID, PhoneEnc: e, EventType: dao.EventAdd, EventTime: t, SyncID: syncID})
	}
	for _, e := range del {
		out = append(out, dao.Event{UserID: userID, PhoneEnc: e, EventType: dao.EventDelete, EventTime: t, SyncID: syncID})
	}
	return out
}

// ---------------------------------------------------------------------------
// đọc
// ---------------------------------------------------------------------------

type Contact struct {
	Phone string
	Name  string
}

type ListResult struct {
	Contacts   []Contact
	Total      int
	NextCursor uint64 // 0 = hết
}

type mergedContact struct {
	name     string
	syncedAt int64
}

// merged: hợp danh bạ các thiết bị (hoặc 1 thiết bị), key = SortKey(bucket, phone_enc).
// Cùng số khác tên giữa 2 thiết bị → lấy tên của thiết bị sync gần nhất.
func (s *PhonebookController) merged(ctx context.Context, userID int64, deviceID string) (map[uint64]mergedContact, error) {
	_, devs, err := s.dao.LoadUser(ctx, uid(userID))
	if err != nil {
		return nil, err
	}
	out := map[uint64]mergedContact{}
	for _, d := range devs {
		if deviceID != "" && d.DeviceID != deviceID {
			continue
		}
		b, err := s.book.Decode(d.Phones, d.Names, []byte(dao.RowKey(uid(userID), d.DeviceID)))
		if err != nil {
			return nil, fmt.Errorf("thiết bị %s: %w", d.DeviceID, err)
		}
		for k, cs := range b.Buckets {
			for _, c := range cs {
				key := book.SortKey(k, c.Enc)
				if cur, ok := out[key]; !ok || d.SyncedAt > cur.syncedAt {
					out[key] = mergedContact{name: c.Name, syncedAt: d.SyncedAt}
				}
			}
		}
	}
	return out, nil
}

// List: phân trang theo (bucket, phone_enc) — chỉ FF1 decode các số của trang.
func (s *PhonebookController) List(ctx context.Context, userID int64, deviceID string, limit int, cursor uint64) (ListResult, error) {
	m, err := s.merged(ctx, userID, deviceID)
	if err != nil {
		return ListResult{}, err
	}
	keys := make([]uint64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	start, _ := slices.BinarySearch(keys, cursor+1)
	end := min(start+limit, len(keys))
	res := ListResult{Total: len(keys), Contacts: make([]Contact, 0, end-start)}
	for _, key := range keys[start:end] {
		_, enc := book.SplitSortKey(key)
		n, err := s.phones.Decode(enc)
		if err != nil {
			return ListResult{}, err
		}
		res.Contacts = append(res.Contacts, Contact{Phone: phonecodec.Format(n), Name: m[key].name})
	}
	if end < len(keys) {
		res.NextCursor = keys[end-1]
	}
	return res, nil
}

// Lookup: user có lưu số này không (hợp mọi thiết bị).
func (s *PhonebookController) Lookup(ctx context.Context, userID int64, phone string) (Contact, error) {
	n, err := phonecodec.Parse(phone)
	if err != nil {
		return Contact{}, badRequest("INVALID_PHONE", "%v", err)
	}
	enc, err := s.phones.Encode(n)
	if err != nil {
		return Contact{}, err
	}
	m, err := s.merged(ctx, userID, "")
	if err != nil {
		return Contact{}, err
	}
	c, ok := m[book.SortKey(phonedigest.BucketOf(phone), enc)]
	if !ok {
		return Contact{}, ErrNotFound
	}
	return Contact{Phone: phone, Name: c.name}, nil
}

type DeviceSummary struct {
	DeviceID     string
	ContactCount int64
	SyncedAt     int64
}

type Summary struct {
	ContactCount int64 // số contact đã gửi DW (published)
	Pending      bool  // còn thay đổi chưa chắc tới DW
	Devices      []DeviceSummary
}

func (s *PhonebookController) Summary(ctx context.Context, userID int64) (Summary, error) {
	user, devs, err := s.dao.LoadUser(ctx, uid(userID))
	if err != nil {
		return Summary{}, err
	}
	out := Summary{ContactCount: user.Count, Pending: user.PendingSync != 0}
	for _, d := range devs {
		out.Devices = append(out.Devices, DeviceSummary{DeviceID: d.DeviceID, ContactCount: d.Count, SyncedAt: d.SyncedAt})
	}
	return out, nil
}
