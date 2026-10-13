package executor

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

func TestDecodeOutputNormalisesByFactType(t *testing.T) {
	facts := []OutputFact{
		{"b", FactBool}, {"i", FactInt}, {"d", FactDouble}, {"s", FactString}, {"ts", FactTimestamp},
	}
	got, err := decodeOutput(facts, json.RawMessage(
		`{"b": true, "i": 9007199254740993, "d": 2, "s": "x", "ts": "2026-10-12T09:30:00.5+07:00"}`))
	if err != nil {
		t.Fatal(err)
	}
	ts := time.Date(2026, 10, 12, 2, 30, 0, 500_000_000, time.UTC)
	if got["b"] != true || got["i"] != int64(9007199254740993) || got["d"] != 2.0 || got["s"] != "x" ||
		!got["ts"].(time.Time).Equal(ts) {
		t.Fatalf("decodeOutput = %#v", got)
	}
}

func TestDecodeOutputRejectsMismatches(t *testing.T) {
	facts := []OutputFact{{"n", FactInt}}
	for raw, want := range map[string]string{
		`{}`:                    `"n" is missing`,
		`{"n": null}`:           `"n" must be an integer, got null`,
		`{"n": 1.5}`:            `"n" must be a 64-bit integer, got 1.5`,
		`{"n": 1e30}`:           `"n" must be a 64-bit integer`,
		`{"n": "1"}`:            `"n" must be an integer, got a string`,
		`{"n": 1, "extra": 2}`:  `"extra" is not a declared output fact`,
		`[1]`:                   `not a JSON object`,
		`{"n": 1, "m": [1, 2]}`: `"m" is not a declared output fact`,
	} {
		_, err := decodeOutput(facts, json.RawMessage(raw))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("decodeOutput(%s) = %v; want %q", raw, err, want)
		}
	}
	for _, tc := range []struct {
		fact OutputFact
		raw  string
	}{
		{OutputFact{"v", FactBool}, `{"v": 1}`},
		{OutputFact{"v", FactDouble}, `{"v": "1"}`},
		{OutputFact{"v", FactString}, `{"v": "a\u0000b"}`},
		{OutputFact{"v", FactString}, `{"v": {}}`},
		{OutputFact{"v", FactTimestamp}, `{"v": "yesterday"}`},
		{OutputFact{"v", FactTimestamp}, `{"v": 0}`},
		{OutputFact{"v", FactType(42)}, `{"v": 0}`},
	} {
		if _, err := decodeOutput([]OutputFact{tc.fact}, json.RawMessage(tc.raw)); err == nil {
			t.Errorf("decodeOutput(%s) as %d accepted", tc.raw, tc.fact.Type)
		}
	}
}

func TestEncodeOutput(t *testing.T) {
	raw, out, err := encodeOutput(nil, nil, 2)
	if err != nil || string(raw) != "{}" || len(out) != 0 {
		t.Fatalf("encodeOutput(nil) = %s, %v, %v", raw, out, err)
	}
	if _, _, err := encodeOutput([]OutputFact{{"d", FactDouble}}, map[string]any{"d": math.NaN()}, 100); err == nil {
		t.Fatal("encodeOutput accepted NaN")
	}
	if _, _, err := encodeOutput([]OutputFact{{"s", FactString}}, map[string]any{"s": "abc"}, 8); err == nil {
		t.Fatal("encodeOutput accepted an output above the limit")
	}
}
