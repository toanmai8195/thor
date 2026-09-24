// Package phonecodec chuyển số di động VN 10 chữ số ↔ số nguyên và mã hoá / giải mã
// tất định bằng FF1 (PHONEBOOK.md mục 4).
//
//	"0366621555" ──Parse──► 366621555 ──Encode──► phone_enc = v × 10^9 + FF1(366621555)
//	phone_enc ──Decode──► 366621555 ──Format──► "0366621555"
package phonecodec

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"

	"github.com/capitalone/fpe/ff1"
)

const (
	// Số chữ số sau khi bỏ số 0 đầu
	digits = 9
	// 10^9: số sau khi Parse luôn < space; phone_enc = version × space + bản mã
	space = 1_000_000_000
	// tweak cố định: cùng khoá dùng cho mục đích khác thì đổi tweak
	tweak = "phone"
)

var (
	ErrInvalidPhone   = errors.New("số điện thoại phải là số di động VN 10 chữ số (0[35789]xxxxxxxx)")
	ErrInvalidEncoded = errors.New("phone_enc không hợp lệ")
)

var mobilePattern = regexp.MustCompile(`^0[35789][0-9]{8}$`)

// Parse: "0366621555" → 366621555 (bỏ số 0 đầu). Chỉ nhận số di động VN 10 chữ số.
func Parse(phone string) (uint64, error) {
	if !mobilePattern.MatchString(phone) {
		return 0, ErrInvalidPhone
	}
	n, err := strconv.ParseUint(phone[1:], 10, 64)
	if err != nil {
		return 0, ErrInvalidPhone
	}
	return n, nil
}

// Format: 366621555 → "0366621555"
func Format(n uint64) string {
	return fmt.Sprintf("0%0*d", digits, n)
}

// Codec mã hoá FF1 với 1 phiên bản khoá. Dùng chung được giữa các goroutine.
type Codec struct {
	version uint64
	cipher  ff1.Cipher
}

// New tạo Codec. version 1..9 là chữ số đầu của phone_enc (để đổi khoá dần);
// key 16 / 24 / 32 byte (AES-128 / 192 / 256).
func New(version int, key []byte) (*Codec, error) {
	if version < 1 || version > 9 {
		return nil, fmt.Errorf("version phải trong 1..9: %d", version)
	}
	c, err := ff1.NewCipher(10, len(tweak), key, []byte(tweak))
	if err != nil {
		return nil, fmt.Errorf("tạo FF1: %w", err)
	}
	return &Codec{version: uint64(version), cipher: c}, nil
}

// Encode: số đã Parse (< 10^9) → phone_enc trong [version × 10^9, (version + 1) × 10^9).
func (c *Codec) Encode(n uint64) (uint64, error) {
	if n >= space {
		return 0, ErrInvalidPhone
	}
	ct, err := c.cipher.Encrypt(fmt.Sprintf("%0*d", digits, n))
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseUint(ct, 10, 64)
	if err != nil {
		return 0, err
	}
	return c.version*space + v, nil
}

// Decode: phone_enc → số đã Parse. phone_enc của phiên bản khoá khác → lỗi.
func (c *Codec) Decode(enc uint64) (uint64, error) {
	if enc/space != c.version {
		return 0, fmt.Errorf("%w: version %d, codec version %d", ErrInvalidEncoded, enc/space, c.version)
	}
	pt, err := c.cipher.Decrypt(fmt.Sprintf("%0*d", digits, enc%space))
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(pt, 10, 64)
}
