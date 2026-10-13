package errtext

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestSanitize(t *testing.T) {
	for in, want := range map[string]string{
		"plain":      "plain",
		"a\x00b":     "a�b",
		"a\xffb":     "a�b",
		"a\xe2\x82b": "a�b",
		"\xff\xff":   "�",
		"":           "",
	} {
		if got := Sanitize(in); got != want {
			t.Errorf("Sanitize(%q) = %q; want %q", in, got, want)
		}
	}
	for _, in := range []string{
		strings.Repeat("€", 1000),
		strings.Repeat("\x00", 3000),
		strings.Repeat("\xff\x00", 3000),
		strings.Repeat("x", MaxBytes-1) + "€",
	} {
		got := Sanitize(in)
		if len(got) > MaxBytes || len(got) < MaxBytes-utf8.UTFMax || !utf8.ValidString(got) || strings.ContainsRune(got, 0) {
			t.Errorf("Sanitize of %d bytes = %d bytes, valid %v", len(in), len(got), utf8.ValidString(got))
		}
	}
}
