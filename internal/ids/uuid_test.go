package ids

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/g0-up/flowkit/internal/testpg"
)

// nsDNS is the DNS namespace from RFC 9562 §6.6.
var nsDNS = MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

func TestNewV5MatchesRFC9562Vector(t *testing.T) {
	// RFC 9562 Appendix A.4.
	got := NewV5(nsDNS, "www.example.com")
	if want := "2ed6657d-e927-568b-95e1-2665a8aea6a2"; got.String() != want {
		t.Fatalf("NewV5 = %s, want %s", got, want)
	}
	if NewV5(nsDNS, "www.example.com") != got {
		t.Fatal("NewV5 is not deterministic")
	}
	if NewV5(nsDNS, "www.example.org") == got {
		t.Fatal("different names gave the same UUID")
	}
}

func TestNewV4VersionVariantAndUniqueness(t *testing.T) {
	seen := make(map[UUID]bool)
	for range 1000 {
		u := NewV4()
		if v := u[6] >> 4; v != 4 {
			t.Fatalf("%s: version %d, want 4", u, v)
		}
		if u[8]&0xc0 != 0x80 {
			t.Fatalf("%s: variant bits %02x, want 10xxxxxx", u, u[8])
		}
		if seen[u] {
			t.Fatalf("duplicate UUID %s", u)
		}
		seen[u] = true
	}
}

func TestParseRoundTripAndRejects(t *testing.T) {
	u := NewV4()
	got, err := Parse(u.String())
	if err != nil || got != u {
		t.Fatalf("Parse(%s) = %s, %v", u, got, err)
	}
	upper, err := Parse("6BA7B810-9DAD-11D1-80B4-00C04FD430C8")
	if err != nil || upper != nsDNS {
		t.Fatalf("uppercase parse = %s, %v", upper, err)
	}
	for _, bad := range []string{
		"",
		"6ba7b810-9dad-11d1-80b4-00c04fd430c",   // short
		"6ba7b810-9dad-11d1-80b4-00c04fd430c8a", // long
		"6ba7b8109dad-11d1-80b4-00c04fd430c8a",  // hyphen misplaced
		"6ba7b810-9dad-11d1-80b4-00c04fd430cg",  // not hex
		"{6ba7b810-9dad-11d1-80b4-00c04fd430c}", // braces
	} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("Parse(%q) succeeded, want error", bad)
		}
	}
}

func TestJSONUsesCanonicalString(t *testing.T) {
	type wrapper struct {
		ID UUID `json:"id"`
	}
	b, err := json.Marshal(wrapper{ID: nsDNS})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8"}`; string(b) != want {
		t.Fatalf("json = %s, want %s", b, want)
	}
	var back wrapper
	if err := json.Unmarshal(b, &back); err != nil || back.ID != nsDNS {
		t.Fatalf("unmarshal = %v, %v", back.ID, err)
	}
	if err := json.Unmarshal([]byte(`{"id":"nope"}`), &back); err == nil {
		t.Fatal("unmarshal of malformed UUID succeeded")
	}
}

func TestPgxRoundTrip(t *testing.T) {
	pool := testpg.Pool(t)
	ctx := context.Background()
	u := NewV4()

	var text string
	var back UUID
	if err := pool.QueryRow(ctx, "SELECT $1::uuid::text, $1::uuid", u).Scan(&text, &back); err != nil {
		t.Fatal(err)
	}
	if text != u.String() || back != u {
		t.Fatalf("round trip: text %s, scanned %s, want %s", text, back, u)
	}

	// Nullable columns scan into *UUID.
	var ptr *UUID
	if err := pool.QueryRow(ctx, "SELECT NULL::uuid").Scan(&ptr); err != nil || ptr != nil {
		t.Fatalf("NULL into *UUID = %v, %v", ptr, err)
	}
	if err := pool.QueryRow(ctx, "SELECT $1::uuid", u).Scan(&ptr); err != nil || ptr == nil || *ptr != u {
		t.Fatalf("value into *UUID = %v, %v", ptr, err)
	}
	if err := pool.QueryRow(ctx, "SELECT NULL::uuid").Scan(&back); err == nil {
		t.Fatal("NULL into UUID succeeded, want error")
	}
	var isNull bool
	if err := pool.QueryRow(ctx, "SELECT $1::uuid IS NULL", (*UUID)(nil)).Scan(&isNull); err != nil || !isNull {
		t.Fatalf("nil *UUID param: null=%v err=%v", isNull, err)
	}
}
