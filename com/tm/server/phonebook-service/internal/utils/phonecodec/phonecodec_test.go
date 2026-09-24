package phonecodec

import (
	"bytes"
	"errors"
	"testing"
)

func newTestCodec(t *testing.T, version int, keyByte byte) *Codec {
	t.Helper()
	c, err := New(version, bytes.Repeat([]byte{keyByte}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestParseFormat(t *testing.T) {
	for _, phone := range []string{"0366621555", "0901234567", "0500000000", "0999999999"} {
		n, err := Parse(phone)
		if err != nil {
			t.Fatalf("Parse(%q): %v", phone, err)
		}
		if got := Format(n); got != phone {
			t.Errorf("Format(Parse(%q)) = %q", phone, got)
		}
	}
	if n, _ := Parse("0366621555"); n != 366621555 {
		t.Errorf("Parse(0366621555) = %d, muốn 366621555", n)
	}
}

func TestParseInvalid(t *testing.T) {
	for _, phone := range []string{
		"", "366621555", "01666621555", "+84366621555", "84366621555",
		"0266621555", "02436621555", "036662155", "036662155a", "0 366621555",
	} {
		if _, err := Parse(phone); !errors.Is(err, ErrInvalidPhone) {
			t.Errorf("Parse(%q) err = %v, muốn ErrInvalidPhone", phone, err)
		}
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	c := newTestCodec(t, 1, 0x00)
	for _, phone := range []string{"0366621555", "0366621556", "0300000000", "0999999999"} {
		n, _ := Parse(phone)
		enc, err := c.Encode(n)
		if err != nil {
			t.Fatal(err)
		}
		if enc < 1_000_000_000 || enc >= 2_000_000_000 {
			t.Errorf("Encode(%s) = %d, ngoài [10^9, 2·10^9)", phone, enc)
		}
		back, err := c.Decode(enc)
		if err != nil {
			t.Fatal(err)
		}
		if Format(back) != phone {
			t.Errorf("Decode(Encode(%s)) = %s", phone, Format(back))
		}
	}
}

func TestEncodeDeterministicAndKeyed(t *testing.T) {
	a := newTestCodec(t, 1, 0x00)
	b := newTestCodec(t, 1, 0x01)
	n, _ := Parse("0366621555")
	a1, _ := a.Encode(n)
	a2, _ := a.Encode(n)
	b1, _ := b.Encode(n)
	if a1 != a2 {
		t.Errorf("cùng khoá phải ra cùng kết quả: %d != %d", a1, a2)
	}
	if a1 == b1 {
		t.Errorf("khác khoá nên khác kết quả, cùng ra %d", a1)
	}
	// Giá trị cố định với khoá 32 byte 0x00: đổi thư viện / cách pad làm test này fail
	if a1 != 1745561990 {
		t.Errorf("Encode(0366621555) với khoá 0x00 = %d, muốn 1745561990", a1)
	}
}

func TestDecodeWrongVersion(t *testing.T) {
	v1 := newTestCodec(t, 1, 0x00)
	v2 := newTestCodec(t, 2, 0x00)
	n, _ := Parse("0366621555")
	enc, _ := v1.Encode(n)
	if _, err := v2.Decode(enc); !errors.Is(err, ErrInvalidEncoded) {
		t.Errorf("Decode phiên bản khác: err = %v, muốn ErrInvalidEncoded", err)
	}
}

func TestNewInvalid(t *testing.T) {
	if _, err := New(0, make([]byte, 32)); err == nil {
		t.Error("version 0 phải lỗi")
	}
	if _, err := New(1, make([]byte, 10)); err == nil {
		t.Error("khoá 10 byte phải lỗi")
	}
}
