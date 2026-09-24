// Package dao: truy cập HBase (bảng `phonebook`, PHONEBOOK.md mục 2.3) và gửi event lên Kafka.
// Không chứa luật nghiệp vụ; chỉ Controller (và MainServer để wire) dùng.
//
// Bảng `phonebook`:
//
//	row key = salt(4 hex md5(userId)) + userId + "#" + deviceId
//	  "<salt><userId>#"            row trạng thái user
//	  "<salt><userId>#<deviceId>"  row danh bạ 1 thiết bị
//
// Mọi row của 1 user nằm liền nhau → 1 Scan theo prefix đọc hết.
package dao

import (
	"context"
	"crypto/md5"
	"encoding/hex"
)

// Family / qualifier. Qualifier ngắn vì HBase lặp lại qualifier trong mỗi cell.
const (
	FamMeta = "m" // meta nhỏ, IN_MEMORY
	FamBlob = "b" // danh bạ đã mã hoá

	// row trạng thái user
	ColVer         = "ver"   // m: int64, tăng mỗi lần ghi, cột so sánh của CheckAndPut
	ColLease       = "lease" // m: int64 ms, khoá theo user
	ColPendingSync = "psync" // m: int64, sync_id của pending; 0 = không có
	ColCount       = "cnt"   // m: int64, số contact (user: của published; thiết bị: của thiết bị)
	ColLastEvent   = "let"   // m: int64 ms, event_time cuối
	ColPublished   = "pub"   // b: tập phone_enc đã gửi Kafka thành công
	ColPending     = "pend"  // b: tập phone_enc đang gửi

	// row thiết bị
	ColRootMAC  = "root" // m: HMAC(root)
	ColSyncedAt = "ts"   // m: int64 ms
	ColDigestV  = "dv"   // m: int64, version thuật toán digest
	ColPhones   = "ph"   // b: số (layout ở package book)
	ColNames    = "nm"   // b: tên, AES-GCM
	ColDigests  = "bd"   // b: 100 digest bucket, AES-GCM
)

// UserRow: row trạng thái user. Ver = 0 nghĩa là row chưa tồn tại.
type UserRow struct {
	Ver         uint64
	Lease       int64
	PendingSync uint64
	Count       int64
	LastEvent   int64
	Published   []byte
	Pending     []byte
}

// UserMeta: phần family m của row user (ghi riêng khi chỉ đổi khoá / trạng thái).
type UserMeta struct {
	Ver         uint64
	Lease       int64
	PendingSync uint64
	Count       int64
	LastEvent   int64
}

// DeviceRow: danh bạ 1 thiết bị.
type DeviceRow struct {
	DeviceID string
	RootMAC  []byte
	Count    int64
	SyncedAt int64
	DigestV  int64
	Phones   []byte
	Names    []byte
	Digests  []byte
}

// Cond: điều kiện của CheckAndPut trên 1 cột family m của row user.
// Value = 0 với ColVer nghĩa là "row chưa có cột ver" (user mới).
type Cond struct {
	Column string
	Value  uint64
}

// PhonebookDao: đọc / ghi row user và row thiết bị. Mọi ghi row user đều có điều kiện (CheckAndPut).
type PhonebookDao interface {
	// LoadUser: Scan prefix → row user (Ver = 0 nếu chưa có) + mọi thiết bị, đủ 2 family.
	LoadUser(ctx context.Context, userID string) (UserRow, []DeviceRow, error)
	// LoadCheck: đọc nhẹ cho `check` — family m của row user; family m + b:bd của 1 thiết bị.
	// Thiết bị chưa có → (nil, nil).
	LoadCheck(ctx context.Context, userID, deviceID string) (UserMeta, *DeviceRow, error)
	// CASUserMeta: ghi family m của row user nếu cond đúng. false = cond sai, không ghi.
	CASUserMeta(ctx context.Context, userID string, cond Cond, m UserMeta) (bool, error)
	// CASUser: như CASUserMeta, ghi thêm published / pending.
	CASUser(ctx context.Context, userID string, cond Cond, u UserRow) (bool, error)
	PutDevice(ctx context.Context, userID string, d DeviceRow) error
	DeleteDevice(ctx context.Context, userID, deviceID string) error
}

// RowPrefix = salt + userId + "#": prefix chung của mọi row của user.
func RowPrefix(userID string) string {
	sum := md5.Sum([]byte(userID))
	return hex.EncodeToString(sum[:])[:4] + userID + "#"
}

// RowKey: row thiết bị; deviceID rỗng = row trạng thái user.
func RowKey(userID, deviceID string) string {
	return RowPrefix(userID) + deviceID
}
