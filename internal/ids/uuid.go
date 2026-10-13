// Package ids generates the UUIDs flowkit stores: random version 4 values for
// drafts and claim tokens, and name-based version 5 values (RFC 9562) for run
// ids. It uses only the standard library.
package ids

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
)

// UUID is a 128-bit identifier in RFC 9562 byte order. It encodes to and
// scans from the Postgres uuid type through pgx.
type UUID [16]byte

// Nil is the all-zero UUID.
var Nil UUID

// NewV4 returns a random version 4 UUID read from crypto/rand.
func NewV4() UUID {
	var u UUID
	// crypto/rand.Read never returns an error; it aborts the program if the
	// system random source fails.
	_, _ = rand.Read(u[:])
	u.setVersion(4)
	return u
}

// NewV5 returns the version 5 UUID of name within namespace ns: the first 16
// bytes of SHA-1(ns || name) with version and variant bits set (RFC 9562 §5.5).
// The same inputs always give the same UUID.
func NewV5(ns UUID, name string) UUID {
	h := sha1.New()
	h.Write(ns[:])
	h.Write([]byte(name))
	var u UUID
	copy(u[:], h.Sum(nil))
	u.setVersion(5)
	return u
}

func (u *UUID) setVersion(v byte) {
	u[6] = u[6]&0x0f | v<<4
	u[8] = u[8]&0x3f | 0x80 // RFC 9562 variant 10xx
}

// Parse decodes the canonical 36-character form, for example
// "6ba7b810-9dad-11d1-80b4-00c04fd430c8". Hex digits may be either case.
func Parse(s string) (UUID, error) {
	var u UUID
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return u, fmt.Errorf("ids: invalid UUID %q", s)
	}
	src := []byte(s[0:8] + s[9:13] + s[14:18] + s[19:23] + s[24:36])
	if _, err := hex.Decode(u[:], src); err != nil {
		return Nil, fmt.Errorf("ids: invalid UUID %q", s)
	}
	return u, nil
}

// MustParse is Parse for constants; it panics on malformed input.
func MustParse(s string) UUID {
	u, err := Parse(s)
	if err != nil {
		panic(err)
	}
	return u
}

// String returns the canonical lowercase hyphenated form.
func (u UUID) String() string {
	var b [36]byte
	hex.Encode(b[0:8], u[0:4])
	b[8] = '-'
	hex.Encode(b[9:13], u[4:6])
	b[13] = '-'
	hex.Encode(b[14:18], u[6:8])
	b[18] = '-'
	hex.Encode(b[19:23], u[8:10])
	b[23] = '-'
	hex.Encode(b[24:36], u[10:16])
	return string(b[:])
}

// MarshalText encodes u in canonical form, so JSON carries a string rather
// than an array of 16 numbers.
func (u UUID) MarshalText() ([]byte, error) {
	return []byte(u.String()), nil
}

// UnmarshalText decodes the canonical form.
func (u *UUID) UnmarshalText(b []byte) error {
	v, err := Parse(string(b))
	if err != nil {
		return err
	}
	*u = v
	return nil
}

// UUIDValue implements pgtype.UUIDValuer.
func (u UUID) UUIDValue() (pgtype.UUID, error) {
	return pgtype.UUID{Bytes: u, Valid: true}, nil
}

// ScanUUID implements pgtype.UUIDScanner. Scanning SQL NULL into a UUID is an
// error; nullable columns scan into *UUID instead.
func (u *UUID) ScanUUID(v pgtype.UUID) error {
	if !v.Valid {
		return fmt.Errorf("ids: cannot scan NULL into UUID")
	}
	*u = v.Bytes
	return nil
}
