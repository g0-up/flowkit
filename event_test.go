package flowkit

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

func TestCanonicalNumber(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"100", "100"},
		{"1e2", "100"},
		{"1E+2", "100"},
		{"100.0", "100"},
		{"10000e-2", "100"},
		{"-1e2", "-100"},
		{"-0", "0"},
		{"0.0e5", "0"},
		{"9007199254740993", "9007199254740993"},
		{"9007199254740993e0", "9007199254740993"},
		{"123456789012345678901234567890", "123456789012345678901234567890"},
		{"1.5", "1.5"}, // not an integer: kept as spelled
		{"1e-2", "1e-2"},
	}
	for _, tt := range tests {
		got, err := canonicalNumber(json.Number(tt.in))
		if err != nil || got != tt.want {
			t.Errorf("canonicalNumber(%s) = %q, %v; want %q", tt.in, got, err, tt.want)
		}
	}
	if got, err := canonicalNumber("1e1000"); err == nil {
		t.Errorf("canonicalNumber(1e1000) = %d digits, want an error", len(got))
	}
}

func TestScalarFieldCanonicalisesIntegers(t *testing.T) {
	fields := map[string]any{"a": json.Number("1e2"), "b": json.Number("100"), "s": "1e2"}
	a, okA, errA := scalarField(fields, "a")
	b, okB, errB := scalarField(fields, "b")
	if a != "100" || b != "100" || !okA || !okB || errA != nil || errB != nil {
		t.Fatalf("1e2 and 100 give %q and %q", a, b)
	}
	if s, _, _ := scalarField(fields, "s"); s != "1e2" {
		t.Fatalf("a string is changed to %q", s)
	}
}

func TestTextProblem(t *testing.T) {
	for _, s := range []string{"", "plain", "héllo", "\uFFFD"} {
		if p := textProblem(s); p != "" {
			t.Errorf("textProblem(%q) = %q", s, p)
		}
	}
	for _, s := range []string{"a\x00b", "\xff", "ok\xc3"} {
		if textProblem(s) == "" {
			t.Errorf("textProblem(%q) accepts it", s)
		}
	}
}

func TestHasBadUnicodeEscape(t *testing.T) {
	tests := []struct {
		raw  string
		want bool
	}{
		{`{"a":"\ufffd"}`, true},
		{`{"a":"x\uFFFDy"}`, true},
		{`{"\ufffd":1}`, true},
		{`{"a":"\\ufffd"}`, false}, // an escaped backslash, then plain text
		{`{"a":"\\\ufffd"}`, true},
		{`{"a":"` + "\uFFFD" + `"}`, false}, // the character itself is valid
		{`{"a":"\u0041"}`, false},
		{`{"a":1}`, false},
		{`{"a":"\ud83d\ude00"}`, false}, // a valid surrogate pair
		{`{"a":"\uD83D\uDE00x"}`, false},
		{`{"a":"\ud800"}`, true},       // high surrogate at the end of a string
		{`{"a":"\ud800x"}`, true},      // high surrogate before plain text
		{`{"a":"\ud800\u0041"}`, true}, // high surrogate before a non-surrogate escape
		{`{"a":"\udc00"}`, true},       // low surrogate alone
		{`{"\udfff":1}`, true},
		{`{"a":"\\ud800"}`, false}, // an escaped backslash, then plain text
	}
	for _, tt := range tests {
		if got := hasBadUnicodeEscape([]byte(tt.raw)); got != tt.want {
			t.Errorf("hasBadUnicodeEscape(%s) = %v, want %v", tt.raw, got, tt.want)
		}
	}
}

func TestFindNUL(t *testing.T) {
	tests := []struct {
		doc  any
		want []string
	}{
		{map[string]any{"a": "x\x00"}, []string{"a"}},
		{map[string]any{"a": map[string]any{"b\x00": 1.0}}, []string{"a", "b\x00"}},
		{map[string]any{"a": []any{"ok", "x\x00"}}, []string{"a", "1"}},
		{map[string]any{"b": "x\x00", "a": "y\x00"}, []string{"a"}}, // first in key order
		{"\x00", nil},
	}
	for _, tt := range tests {
		got, found := findNUL(tt.doc, nil)
		if !found || !slices.Equal(got, tt.want) {
			t.Errorf("findNUL(%v) = %q, %v; want %q", tt.doc, got, found, tt.want)
		}
	}
	if _, found := findNUL(map[string]any{"a": []any{"x", 1.0, nil, true}, "b": strings.Repeat("y", 10)}, nil); found {
		t.Error("findNUL found a NUL in clean text")
	}
}

func TestNumberProblem(t *testing.T) {
	for _, n := range []string{"0", "-0", "1", "9007199254740993", "1e308", "-1.7976931348623157e308", "5e-324", "1.5e-300", "0e1000", "0e-1000", "123.456E+2"} {
		if problem := numberProblem(json.Number(n)); problem != "" {
			t.Errorf("numberProblem(%s) = %q, want none", n, problem)
		}
	}
	for n, want := range map[string]string{
		"1e131000":                        "out of the float64 range",
		"1e131072":                        "out of the float64 range",
		"-1e309":                          "out of the float64 range",
		"1e-1000000000":                   "too close to zero",
		"2e-324":                          "too close to zero",
		"0e-1001":                         "exponent beyond",
		"0e99999999999999999999":          "exponent beyond",
		"0." + strings.Repeat("0", 16384): "digits after the decimal point",
	} {
		if problem := numberProblem(json.Number(n)); !strings.Contains(problem, want) {
			t.Errorf("numberProblem(%.40s) = %q, want %q", n, problem, want)
		}
	}
	path, problem := findBadNumber(map[string]any{"a": []any{1.0, json.Number("1"), json.Number("1e400")}, "b": json.Number("1e400")}, nil)
	if !slices.Equal(path, []string{"a", "2"}) || problem == "" {
		t.Errorf("findBadNumber = %q, %q", path, problem)
	}
}
