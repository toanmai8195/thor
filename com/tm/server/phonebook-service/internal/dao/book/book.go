// Package book: danh bạ của 1 thiết bị trong bộ nhớ và cách đóng gói vào 2 cell HBase
// (PHONEBOOK.md mục 2.3).
//
//	b:ph = [1 byte format][100 × uvarint số contact / bucket][bucket 00: delta varint]…[bucket 99: …]
//	b:nm = Seal(zstd(tên nối "\n" theo đúng thứ tự trong b:ph), aad = rowKey ‖ SHA-256(b:ph))
//
// Trong mỗi bucket, contact sort theo phone_enc tăng dần. Số và tên khớp nhau theo vị trí.
package book

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/klauspost/compress/zstd"

	"thor/server/phonebook-service/internal/utils/phonedigest"
)

const (
	Buckets = phonedigest.Buckets
	// phonesFormat: byte đầu của b:ph, đổi khi đổi layout
	phonesFormat = 1
	// sortKeyBase: phone_enc < 10^10 → sortKey = bucket × 10^10 + phone_enc giữ đúng thứ tự (bucket, phone_enc)
	sortKeyBase = 10_000_000_000
)

var ErrCorrupt = errors.New("book: dữ liệu hỏng")

// Contact: 1 số (đã FF1) và tên (đã chuẩn hoá).
type Contact struct {
	Enc  uint64
	Name string
}

// Book: danh bạ 1 thiết bị, chia 100 bucket, mỗi bucket sort theo Enc.
type Book struct {
	Buckets [Buckets][]Contact
}

// SortKey: khoá sắp xếp chung (bucket, phone_enc) → 1 số, dùng làm cursor phân trang.
func SortKey(bucket int, enc uint64) uint64 { return uint64(bucket)*sortKeyBase + enc }

// SplitSortKey: ngược lại của SortKey.
func SplitSortKey(k uint64) (bucket int, enc uint64) { return int(k / sortKeyBase), k % sortKeyBase }

// SetBucket thay toàn bộ bucket k (sort theo Enc).
func (b *Book) SetBucket(k int, contacts []Contact) {
	cs := slices.Clone(contacts)
	slices.SortFunc(cs, func(x, y Contact) int {
		switch {
		case x.Enc < y.Enc:
			return -1
		case x.Enc > y.Enc:
			return 1
		}
		return 0
	})
	b.Buckets[k] = cs
}

// Count: tổng số contact.
func (b *Book) Count() int {
	n := 0
	for _, cs := range b.Buckets {
		n += len(cs)
	}
	return n
}

// Encs: mọi phone_enc, sort tăng dần (dùng để hợp các thiết bị và diff).
func (b *Book) Encs() []uint64 {
	out := make([]uint64, 0, b.Count())
	for _, cs := range b.Buckets {
		for _, c := range cs {
			out = append(out, c.Enc)
		}
	}
	slices.Sort(out)
	return out
}

// EncodePhones đóng gói số của mọi bucket thành b:ph.
func (b *Book) EncodePhones() []byte {
	out := make([]byte, 0, 1+Buckets+b.Count()*4)
	out = append(out, phonesFormat)
	for _, cs := range b.Buckets {
		out = binary.AppendUvarint(out, uint64(len(cs)))
	}
	for _, cs := range b.Buckets {
		var prev uint64
		for _, c := range cs {
			out = binary.AppendUvarint(out, c.Enc-prev)
			prev = c.Enc
		}
	}
	return out
}

// DecodePhones: b:ph → phone_enc theo từng bucket. blob rỗng = danh bạ rỗng.
func DecodePhones(blob []byte) ([Buckets][]uint64, error) {
	var out [Buckets][]uint64
	if len(blob) == 0 {
		return out, nil
	}
	if blob[0] != phonesFormat {
		return out, fmt.Errorf("%w: format b:ph %d", ErrCorrupt, blob[0])
	}
	p := blob[1:]
	var counts [Buckets]uint64
	for k := range counts {
		n, w := binary.Uvarint(p)
		if w <= 0 || n > uint64(len(blob)) {
			return out, fmt.Errorf("%w: header b:ph", ErrCorrupt)
		}
		counts[k] = n
		p = p[w:]
	}
	for k := range out {
		out[k] = make([]uint64, 0, counts[k])
		var prev uint64
		for i := uint64(0); i < counts[k]; i++ {
			d, w := binary.Uvarint(p)
			if w <= 0 || (i > 0 && d == 0) {
				return out, fmt.Errorf("%w: bucket %d", ErrCorrupt, k)
			}
			prev += d
			out[k] = append(out[k], prev)
			p = p[w:]
		}
	}
	if len(p) != 0 {
		return out, fmt.Errorf("%w: thừa %d byte cuối b:ph", ErrCorrupt, len(p))
	}
	return out, nil
}

// namesText: tên nối "\n" theo thứ tự bucket 00..99, trong bucket theo Enc.
// Tên đã chuẩn hoá không chứa "\n" nên tách lại được.
func (b *Book) namesText() string {
	names := make([]string, 0, b.Count())
	for _, cs := range b.Buckets {
		for _, c := range cs {
			names = append(names, c.Name)
		}
	}
	return strings.Join(names, "\n")
}

// Codec đóng gói / mở Book cho 1 row HBase: số (không cần khoá) + tên (AES-GCM).
type Codec struct {
	sealer *Sealer
	enc    *zstd.Encoder
	dec    *zstd.Decoder
}

func NewCodec(s *Sealer) (*Codec, error) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, err
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		return nil, err
	}
	return &Codec{sealer: s, enc: enc, dec: dec}, nil
}

// Encode → (b:ph, b:nm). rowKey gắn vào AAD: copy b:nm sang row khác sẽ không mở được.
func (c *Codec) Encode(b *Book, rowKey []byte) (phones, names []byte, err error) {
	phones = b.EncodePhones()
	z := c.enc.EncodeAll([]byte(b.namesText()), nil)
	names, err = c.sealer.Seal(z, namesAAD(rowKey, phones))
	return phones, names, err
}

// Decode (b:ph, b:nm) → Book. b:nm phải được tạo cùng lúc với đúng b:ph này.
func (c *Codec) Decode(phones, names, rowKey []byte) (*Book, error) {
	encs, err := DecodePhones(phones)
	if err != nil {
		return nil, err
	}
	total := 0
	for _, e := range encs {
		total += len(e)
	}
	b := &Book{}
	if total == 0 {
		return b, nil
	}
	z, err := c.sealer.Open(names, namesAAD(rowKey, phones))
	if err != nil {
		return nil, fmt.Errorf("mở b:nm: %w", err)
	}
	text, err := c.dec.DecodeAll(z, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: zstd b:nm: %v", ErrCorrupt, err)
	}
	list := strings.Split(string(text), "\n")
	if len(list) != total {
		return nil, fmt.Errorf("%w: %d tên cho %d số", ErrCorrupt, len(list), total)
	}
	i := 0
	for k, es := range encs {
		cs := make([]Contact, len(es))
		for j, e := range es {
			cs[j] = Contact{Enc: e, Name: list[i]}
			i++
		}
		b.Buckets[k] = cs
	}
	return b, nil
}

// PhonesOnly: chỉ đọc số (không cần khoá, không mở tên) — dùng khi hợp các thiết bị.
func PhonesOnly(phones []byte) ([]uint64, error) {
	encs, err := DecodePhones(phones)
	if err != nil {
		return nil, err
	}
	var out []uint64
	for _, e := range encs {
		out = append(out, e...)
	}
	slices.Sort(out)
	return out, nil
}

func namesAAD(rowKey, phones []byte) []byte {
	h := sha256.Sum256(phones)
	aad := append([]byte("nm|"), rowKey...)
	return append(aad, h[:]...)
}

// SealDigests / OpenDigests: 100 digest bucket đủ 32 byte (3.200 byte) → b:bd, mã hoá để
// không dò ngược được bucket ít contact.
func (c *Codec) SealDigests(d *phonedigest.BucketDigests, rowKey []byte) ([]byte, error) {
	raw := make([]byte, 0, Buckets*sha256.Size)
	for k := range d {
		raw = append(raw, d[k][:]...)
	}
	return c.sealer.Seal(raw, append([]byte("bd|"), rowKey...))
}

// OpenDigests: blob rỗng (thiết bị mới) → 100 bucket rỗng.
func (c *Codec) OpenDigests(blob, rowKey []byte) (phonedigest.BucketDigests, error) {
	var d phonedigest.BucketDigests
	if len(blob) == 0 {
		for k := range d {
			d[k] = phonedigest.EmptyBucket
		}
		return d, nil
	}
	raw, err := c.sealer.Open(blob, append([]byte("bd|"), rowKey...))
	if err != nil {
		return d, fmt.Errorf("mở b:bd: %w", err)
	}
	if len(raw) != Buckets*sha256.Size {
		return d, fmt.Errorf("%w: b:bd dài %d", ErrCorrupt, len(raw))
	}
	for k := range d {
		copy(d[k][:], raw[k*sha256.Size:])
	}
	return d, nil
}
