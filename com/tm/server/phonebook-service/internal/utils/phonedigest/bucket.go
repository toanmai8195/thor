package phonedigest

import (
	"bytes"
	"crypto/sha256"
)

// Buckets: danh bạ chia cố định 100 bucket theo 2 chữ số cuối của số.
const Buckets = 100

// TruncLen: số byte đầu của digest bucket app gửi trong `check` (100 × 4 = 400 byte).
const TruncLen = 4

// EmptyBucket = SHA-256("") — digest của bucket không có contact nào.
var EmptyBucket = sha256.Sum256(nil)

// BucketOf: bucket của 1 số 10 chữ số = 2 chữ số cuối ("0366621555" → 55).
// Số phải hợp lệ (đã qua Canonicalize).
func BucketOf(phone string) int {
	n := len(phone)
	return int(phone[n-2]-'0')*10 + int(phone[n-1]-'0')
}

// BucketDigests: digest đủ 32 byte của 100 bucket, theo thứ tự 00..99.
type BucketDigests [Buckets][sha256.Size]byte

// BucketDigest = SHA-256 của các dòng "<số>\t<tên>\n" thuộc 1 bucket, canon đã Canonicalize
// và chỉ chứa số của bucket đó.
func BucketDigest(canon []Entry) [sha256.Size]byte {
	return sha256.Sum256(CanonicalBytes(canon))
}

// SplitBuckets chia danh sách đã Canonicalize thành 100 bucket (giữ thứ tự tăng dần).
func SplitBuckets(canon []Entry) [Buckets][]Entry {
	var out [Buckets][]Entry
	for _, e := range canon {
		k := BucketOf(e.Phone)
		out[k] = append(out[k], e)
	}
	return out
}

// ComputeBuckets tính digest 100 bucket từ danh sách đã Canonicalize.
func ComputeBuckets(canon []Entry) BucketDigests {
	var d BucketDigests
	for k, entries := range SplitBuckets(canon) {
		d[k] = BucketDigest(entries)
	}
	return d
}

// Root = SHA-256(bd[00] ‖ bd[01] ‖ … ‖ bd[99]) — đổi bất kỳ bucket nào thì root đổi.
func (d *BucketDigests) Root() [sha256.Size]byte {
	var buf bytes.Buffer
	for k := range d {
		buf.Write(d[k][:])
	}
	return sha256.Sum256(buf.Bytes())
}

// Truncated: 4 byte đầu của mỗi bucket nối lại, 400 byte — trường `buckets` của `check`.
func (d *BucketDigests) Truncated() []byte {
	out := make([]byte, 0, Buckets*TruncLen)
	for k := range d {
		out = append(out, d[k][:TruncLen]...)
	}
	return out
}

// Root tiện dụng: digest gốc của cả danh bạ (entries chưa cần Canonicalize).
func Root(entries []Entry) [sha256.Size]byte {
	d := ComputeBuckets(Canonicalize(entries))
	return d.Root()
}
