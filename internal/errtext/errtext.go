// Package errtext makes error text storable in a Postgres text column.
package errtext

import (
	"strings"
	"unicode/utf8"
)

// MaxBytes bounds the error text flowkit stores.
const MaxBytes = 2 << 10

// Sanitize makes s storable in a Postgres text column: invalid UTF-8 and
// U+0000 become U+FFFD, and s is cut to MaxBytes at a rune boundary.
func Sanitize(s string) string {
	s = strings.ToValidUTF8(s, "�")
	s = strings.ReplaceAll(s, "\x00", "�")
	if len(s) <= MaxBytes {
		return s
	}
	// s is valid UTF-8, so a rune starts within the last UTFMax bytes.
	cut := MaxBytes
	for cut > MaxBytes-utf8.UTFMax && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}
