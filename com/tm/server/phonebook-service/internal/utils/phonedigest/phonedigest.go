// Package phonedigest là bản tham chiếu cách tính digest danh bạ (PHONEBOOK.md mục 2.5).
// App (iOS / Android) và phonebook-service phải ra cùng digest cho cùng 1 danh bạ:
// mọi thay đổi ở đây phải tăng Version và cập nhật testdata/vectors.json.
//
//	contact trên máy ─► Canonicalize ─► chia 100 bucket theo 2 số cuối
//	  bucket k: "0366621555\tMẹ\n..." ─► SHA-256 ─► bd[k]
//	  root = SHA-256(bd[00] ‖ … ‖ bd[99])
package phonedigest

import (
	"bytes"
	"slices"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"

	"thor/server/phonebook-service/internal/utils/phonecodec"
)

// Version của thuật toán (trường "v" trong request); đổi thuật toán → tăng version.
const Version = 1

// MaxNameRunes: tên dài hơn bị cắt (tính theo code point, sau khi chuẩn hoá).
const MaxNameRunes = 100

// Entry là 1 cặp (số, tên) — 1 contact có 3 số thì là 3 Entry cùng tên.
type Entry struct {
	Phone string `json:"p"`
	Name  string `json:"n"`
}

// NormalizeName: NFC → ký tự khoảng trắng / điều khiển thành 1 dấu cách → bỏ ký tự
// định dạng vô hình (zero-width...) → gộp dấu cách liên tiếp → trim → cắt MaxNameRunes.
func NormalizeName(s string) string {
	s = norm.NFC.String(s)
	var b strings.Builder
	space := false
	for _, r := range s {
		switch {
		case unicode.IsSpace(r) || unicode.IsControl(r):
			space = true
			continue
		case unicode.Is(unicode.Cf, r): // U+200B..U+200F, U+FEFF...
			continue
		}
		if space && b.Len() > 0 {
			b.WriteByte(' ')
		}
		space = false
		b.WriteRune(r)
	}
	out := b.String()
	if r := []rune(out); len(r) > MaxNameRunes {
		out = strings.TrimRight(string(r[:MaxNameRunes]), " ")
	}
	return out
}

// Canonicalize trả về danh sách đã chuẩn hoá, dùng cho cả digest lẫn dữ liệu upload:
//   - bỏ số không phải di động VN 10 chữ số
//   - chuẩn hoá tên (NormalizeName)
//   - 1 số có nhiều tên → giữ tên nhỏ nhất theo thứ tự byte UTF-8
//   - sort theo số tăng dần
func Canonicalize(entries []Entry) []Entry {
	best := make(map[string]string, len(entries))
	for _, e := range entries {
		if _, err := phonecodec.Parse(e.Phone); err != nil {
			continue
		}
		name := NormalizeName(e.Name)
		if cur, ok := best[e.Phone]; !ok || name < cur {
			best[e.Phone] = name
		}
	}
	out := make([]Entry, 0, len(best))
	for p, n := range best {
		out = append(out, Entry{Phone: p, Name: n})
	}
	slices.SortFunc(out, func(a, b Entry) int { return strings.Compare(a.Phone, b.Phone) })
	return out
}

// CanonicalBytes: mỗi entry 1 dòng "<số>\t<tên>\n", UTF-8. entries phải đã Canonicalize.
func CanonicalBytes(canon []Entry) []byte {
	var b bytes.Buffer
	for _, e := range canon {
		b.WriteString(e.Phone)
		b.WriteByte('\t')
		b.WriteString(e.Name)
		b.WriteByte('\n')
	}
	return b.Bytes()
}
