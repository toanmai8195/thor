package phonedigest

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

// testdata/vectors.json dùng chung với App: iOS / Android chạy cùng file, phải ra cùng
// canonical, digest từng bucket và root. Giá trị trong file tính bằng Python hashlib
// (không lấy từ code này). "buckets" chỉ liệt kê bucket khác rỗng; bucket còn lại = SHA-256("").
type vector struct {
	Name      string            `json:"name"`
	Input     []Entry           `json:"input"`
	Canonical string            `json:"canonical"`
	Buckets   map[string]string `json:"buckets"`
	Root      string            `json:"root"`
}

func loadVectors(t *testing.T) []vector {
	t.Helper()
	data, err := os.ReadFile("testdata/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vs []vector
	if err := json.Unmarshal(data, &vs); err != nil {
		t.Fatal(err)
	}
	if len(vs) == 0 {
		t.Fatal("vectors.json rỗng")
	}
	return vs
}

func TestVectors(t *testing.T) {
	for _, v := range loadVectors(t) {
		t.Run(v.Name, func(t *testing.T) {
			canon := Canonicalize(v.Input)
			if got := string(CanonicalBytes(canon)); got != v.Canonical {
				t.Errorf("canonical = %q, muốn %q", got, v.Canonical)
			}
			d := ComputeBuckets(canon)
			for k := 0; k < Buckets; k++ {
				want := hex.EncodeToString(EmptyBucket[:])
				if h, ok := v.Buckets[fmt.Sprintf("%02d", k)]; ok {
					want = h
				}
				if got := hex.EncodeToString(d[k][:]); got != want {
					t.Errorf("bucket %02d = %s, muốn %s", k, got, want)
				}
			}
			root := d.Root()
			if got := hex.EncodeToString(root[:]); got != v.Root {
				t.Errorf("root = %s, muốn %s", got, v.Root)
			}
			if r := Root(v.Input); r != root {
				t.Error("Root(entries) khác ComputeBuckets(...).Root()")
			}
		})
	}
}

func TestBucketOf(t *testing.T) {
	for phone, want := range map[string]int{"0366621555": 55, "0901234567": 67, "0912345600": 0, "0987654399": 99} {
		if got := BucketOf(phone); got != want {
			t.Errorf("BucketOf(%s) = %d, muốn %d", phone, got, want)
		}
	}
}

func TestTruncated(t *testing.T) {
	d := ComputeBuckets(Canonicalize([]Entry{{"0366621555", "Mẹ"}}))
	tr := d.Truncated()
	if len(tr) != Buckets*TruncLen {
		t.Fatalf("len = %d, muốn %d", len(tr), Buckets*TruncLen)
	}
	if !bytes.Equal(tr[55*TruncLen:56*TruncLen], d[55][:TruncLen]) {
		t.Error("bucket 55 không nằm ở vị trí 55")
	}
	if !bytes.Equal(tr[:TruncLen], EmptyBucket[:TruncLen]) {
		t.Error("bucket 00 rỗng phải là 4 byte đầu của SHA-256(\"\")")
	}
}

func TestRootIgnoresOrder(t *testing.T) {
	a := []Entry{{"0901234567", "Mẹ"}, {"0366621555", "Anh"}}
	b := []Entry{{"0366621555", "Anh"}, {"0901234567", "Mẹ"}}
	if Root(a) != Root(b) {
		t.Error("thứ tự contact trên máy không được làm đổi root")
	}
}

func TestRenameChangesOnlyItsBucket(t *testing.T) {
	a := ComputeBuckets(Canonicalize([]Entry{{"0901234567", "Mẹ"}, {"0366621555", "Anh"}}))
	b := ComputeBuckets(Canonicalize([]Entry{{"0901234567", "Mẹ yêu"}, {"0366621555", "Anh"}}))
	for k := 0; k < Buckets; k++ {
		if changed := a[k] != b[k]; changed != (k == 67) {
			t.Errorf("bucket %02d changed = %v", k, changed)
		}
	}
	if a.Root() == b.Root() {
		t.Error("đổi tên phải làm đổi root")
	}
}

func TestNormalizeNameIdempotent(t *testing.T) {
	for _, s := range []string{"  Anh\t\tTuấn ", "Mẹ", "a​b", "Nguyễn Văn A"} {
		once := NormalizeName(s)
		if twice := NormalizeName(once); twice != once {
			t.Errorf("NormalizeName không idempotent: %q → %q → %q", s, once, twice)
		}
	}
}
