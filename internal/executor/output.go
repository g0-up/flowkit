package executor

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// encodeOutput checks an action's output against its OutputFacts and the
// size limit. It returns the JSON to store and the output as decodeOutput
// returns it, so a fresh and a cached result look the same to the caller.
func encodeOutput(facts []OutputFact, out map[string]any, maxBytes int) (json.RawMessage, map[string]any, error) {
	if out == nil {
		out = map[string]any{}
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return nil, nil, fmt.Errorf("output is not valid JSON: %w", err)
	}
	if len(raw) > maxBytes {
		return nil, nil, fmt.Errorf("output is %d bytes, above the limit of %d", len(raw), maxBytes)
	}
	norm, err := decodeOutput(facts, raw)
	if err != nil {
		return nil, nil, err
	}
	return raw, norm, nil
}

// decodeOutput parses a stored output and checks it against facts: every
// declared fact present with its type, nothing else. Values become bool,
// int64, float64, string or time.Time by fact type.
func decodeOutput(facts []OutputFact, raw json.RawMessage) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("output is not a JSON object: %w", err)
	}
	var problems []string
	out := make(map[string]any, len(facts))
	declared := make(map[string]bool, len(facts))
	for _, f := range facts {
		declared[f.Name] = true
		v, ok := m[f.Name]
		if !ok {
			problems = append(problems, fmt.Sprintf("%q is missing", f.Name))
			continue
		}
		nv, err := factValue(f.Type, v)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%q %v", f.Name, err))
			continue
		}
		out[f.Name] = nv
	}
	for k := range m {
		if !declared[k] {
			problems = append(problems, fmt.Sprintf("%q is not a declared output fact", k))
		}
	}
	if len(problems) > 0 {
		slices.Sort(problems)
		return nil, errors.New("output does not match OutputFacts: " + strings.Join(problems, "; "))
	}
	return out, nil
}

// factValue converts a JSON-decoded value to the Go value of type t.
func factValue(t FactType, v any) (any, error) {
	switch t {
	case FactBool:
		if b, ok := v.(bool); ok {
			return b, nil
		}
		return nil, fmt.Errorf("must be a bool, got %s", jsonKind(v))
	case FactInt:
		if n, ok := v.(json.Number); ok {
			if i, err := n.Int64(); err == nil {
				return i, nil
			}
			return nil, fmt.Errorf("must be a 64-bit integer, got %s", n)
		}
		return nil, fmt.Errorf("must be an integer, got %s", jsonKind(v))
	case FactDouble:
		if n, ok := v.(json.Number); ok {
			if f, err := n.Float64(); err == nil {
				return f, nil
			}
			return nil, fmt.Errorf("must be a finite double, got %s", n)
		}
		return nil, fmt.Errorf("must be a number, got %s", jsonKind(v))
	case FactString:
		if s, ok := v.(string); ok {
			if strings.ContainsRune(s, 0) {
				return nil, errors.New("must not contain U+0000, which Postgres cannot store")
			}
			return s, nil
		}
		return nil, fmt.Errorf("must be a string, got %s", jsonKind(v))
	case FactTimestamp:
		if s, ok := v.(string); ok {
			ts, err := time.Parse(time.RFC3339Nano, s)
			if err != nil {
				return nil, fmt.Errorf("must be an RFC 3339 timestamp, got %q", s)
			}
			return ts, nil
		}
		return nil, fmt.Errorf("must be an RFC 3339 timestamp, got %s", jsonKind(v))
	}
	return nil, fmt.Errorf("has unknown fact type %d", int(t))
}

func jsonKind(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "a bool"
	case json.Number:
		return "a number"
	case string:
		return "a string"
	case []any:
		return "an array"
	case map[string]any:
		return "an object"
	}
	return fmt.Sprintf("%T", v)
}
