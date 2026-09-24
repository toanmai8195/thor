package book

import (
	"bytes"
	"errors"
	"testing"

	"thor/server/phonebook-service/internal/utils/phonedigest"
)

func testCodec(t *testing.T) *Codec {
	t.Helper()
	s, err := NewSealer(1, map[byte][]byte{1: bytes.Repeat([]byte{7}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewCodec(s)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func sampleBook() *Book {
	b := &Book{}
	b.SetBucket(55, []Contact{{1745561990, "Mẹ"}, {1100000055, ""}})
	b.SetBucket(21, []Contact{{1803898459, "Anh Tuấn"}})
	b.SetBucket(78, []Contact{{1085342603, "Chị Lan Anh"}})
	return b
}

func TestPhonesRoundTrip(t *testing.T) {
	b := sampleBook()
	encs, err := DecodePhones(b.EncodePhones())
	if err != nil {
		t.Fatal(err)
	}
	for k := range encs {
		if len(encs[k]) != len(b.Buckets[k]) {
			t.Fatalf("bucket %d: %d số, muốn %d", k, len(encs[k]), len(b.Buckets[k]))
		}
		for i, e := range encs[k] {
			if e != b.Buckets[k][i].Enc {
				t.Errorf("bucket %d[%d] = %d, muốn %d", k, i, e, b.Buckets[k][i].Enc)
			}
		}
	}
	// bucket 55 phải sort theo Enc
	if b.Buckets[55][0].Enc != 1100000055 {
		t.Errorf("SetBucket phải sort theo Enc: %+v", b.Buckets[55])
	}
}

func TestCodecRoundTrip(t *testing.T) {
	c := testCodec(t)
	b := sampleBook()
	key := []byte("b8c31001#ios-8F2A")
	ph, nm, err := c.Encode(b, key)
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.Decode(ph, nm, key)
	if err != nil {
		t.Fatal(err)
	}
	if !equalBooks(got, b) {
		t.Errorf("Decode(Encode) khác: %+v", got.Buckets)
	}
}

func TestCodecEmpty(t *testing.T) {
	c := testCodec(t)
	ph, nm, err := c.Encode(&Book{}, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.Decode(ph, nm, []byte("k"))
	if err != nil || got.Count() != 0 {
		t.Fatalf("book rỗng: %v, count %d", err, got.Count())
	}
	if got, err := c.Decode(nil, nil, []byte("k")); err != nil || got.Count() != 0 {
		t.Fatalf("thiết bị chưa có dữ liệu: %v", err)
	}
}

func TestNamesBoundToPhonesAndRow(t *testing.T) {
	c := testCodec(t)
	key := []byte("b8c31001#ios-8F2A")
	ph, nm, _ := c.Encode(sampleBook(), key)

	other := sampleBook()
	other.SetBucket(10, []Contact{{1234567890, "X"}})
	ph2, _, _ := c.Encode(other, key)
	if _, err := c.Decode(ph2, nm, key); err == nil {
		t.Error("b:nm ghép với b:ph khác phải lỗi")
	}
	if _, err := c.Decode(ph, nm, []byte("b8c31001#and-1")); err == nil {
		t.Error("b:nm copy sang row khác phải lỗi")
	}
}

func TestDigestsRoundTrip(t *testing.T) {
	c := testCodec(t)
	d := phonedigest.ComputeBuckets(phonedigest.Canonicalize([]phonedigest.Entry{{Phone: "0366621555", Name: "Mẹ"}}))
	blob, err := c.SealDigests(&d, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.OpenDigests(blob, []byte("k"))
	if err != nil || got != d {
		t.Fatalf("OpenDigests(SealDigests) khác: %v", err)
	}
	empty, _ := c.OpenDigests(nil, []byte("k"))
	if empty[0] != phonedigest.EmptyBucket || empty[99] != phonedigest.EmptyBucket {
		t.Error("blob rỗng phải ra 100 bucket rỗng")
	}
}

func TestDecodePhonesCorrupt(t *testing.T) {
	good := sampleBook().EncodePhones()
	for name, blob := range map[string][]byte{
		"sai format": append([]byte{9}, good[1:]...),
		"cắt cụt":    good[:len(good)-1],
		"thừa byte":  append(append([]byte{}, good...), 1),
	} {
		if _, err := DecodePhones(blob); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: err = %v, muốn ErrCorrupt", name, err)
		}
	}
}

func TestSealerKeyVersions(t *testing.T) {
	k1, k2 := bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32)
	old, _ := NewSealer(1, map[byte][]byte{1: k1})
	blob, _ := old.Seal([]byte("abc"), nil)
	rotated, _ := NewSealer(2, map[byte][]byte{1: k1, 2: k2})
	if got, err := rotated.Open(blob, nil); err != nil || string(got) != "abc" {
		t.Fatalf("đọc blob version cũ sau khi đổi khoá: %q %v", got, err)
	}
	nb, _ := rotated.Seal([]byte("abc"), nil)
	if nb[0] != 2 {
		t.Errorf("ghi mới phải dùng version 2, được %d", nb[0])
	}
	if _, err := old.Open(nb, nil); !errors.Is(err, ErrKeyVersion) {
		t.Errorf("thiếu khoá version 2: err = %v", err)
	}
}

func TestSortKey(t *testing.T) {
	k := SortKey(55, 1745561990)
	if b, e := SplitSortKey(k); b != 55 || e != 1745561990 {
		t.Errorf("SplitSortKey = %d, %d", b, e)
	}
	if SortKey(21, 9_999_999_999) >= SortKey(22, 1_000_000_000) {
		t.Error("SortKey phải giữ thứ tự bucket trước")
	}
}

func equalBooks(a, b *Book) bool {
	for k := range a.Buckets {
		if len(a.Buckets[k]) != len(b.Buckets[k]) {
			return false
		}
		for i := range a.Buckets[k] {
			if a.Buckets[k][i] != b.Buckets[k][i] {
				return false
			}
		}
	}
	return true
}
