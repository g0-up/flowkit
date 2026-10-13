package expr

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/g0-up/flowkit/internal/schema"
)

// Activation decodes a JSON object payload for evaluation and coerces it to
// s: an integer field becomes int64 (a value with a fractional part is an
// error, never rounded), a number field float64, a date-time field
// time.Time. Untyped (dyn) numbers become int64 when integral and float64
// otherwise. Without this, a payload that went through jsonb would compare
// float64(3) with the int 3 and CEL would report them unequal.
func Activation(payload json.RawMessage, s *Shape) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("payload is not valid JSON: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("payload has data after the JSON value")
	}
	if _, ok := v.(map[string]any); !ok {
		return nil, errors.New("payload must be a JSON object")
	}
	out, err := coerce(v, s.root, "")
	if err != nil {
		return nil, err
	}
	return out.(map[string]any), nil
}

func coerce(v any, n *node, path string) (any, error) {
	switch n.kind {
	case kindDyn:
		return coerceDyn(v)
	case kindObject:
		m, ok := v.(map[string]any)
		if !ok {
			return nil, mismatch(path, "an object", v)
		}
		out := make(map[string]any, len(m))
		for k, fv := range m {
			var c any
			var err error
			if f, ok := n.fields[k]; ok {
				c, err = coerce(fv, f.node, path+schema.Pointer(k))
			} else {
				c, err = coerceDyn(fv)
			}
			if err != nil {
				return nil, err
			}
			out[k] = c
		}
		return out, nil
	case kindMap:
		m, ok := v.(map[string]any)
		if !ok {
			return nil, mismatch(path, "an object", v)
		}
		out := make(map[string]any, len(m))
		for k, fv := range m {
			c, err := coerce(fv, n.elem, path+schema.Pointer(k))
			if err != nil {
				return nil, err
			}
			out[k] = c
		}
		return out, nil
	case kindList:
		l, ok := v.([]any)
		if !ok {
			return nil, mismatch(path, "an array", v)
		}
		out := make([]any, len(l))
		for i, e := range l {
			c, err := coerce(e, n.elem, path+"/"+strconv.Itoa(i))
			if err != nil {
				return nil, err
			}
			out[i] = c
		}
		return out, nil
	}
	c, err := coerceScalar(v, n.kind)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", pathOrRoot(path), err)
	}
	return c, nil
}

// coerceScalar converts a decoded JSON value or a Go value to the
// representation of k.
func coerceScalar(v any, k kind) (any, error) {
	switch k {
	case kindBool:
		if b, ok := v.(bool); ok {
			return b, nil
		}
		return nil, describe("a boolean", v)
	case kindString:
		if s, ok := v.(string); ok {
			return s, nil
		}
		return nil, describe("a string", v)
	case kindTimestamp:
		switch x := v.(type) {
		case time.Time:
			return x, nil
		case string:
			t, err := time.Parse(time.RFC3339Nano, x)
			if err != nil {
				return nil, fmt.Errorf("must be an RFC 3339 date-time, got %q", x)
			}
			return t, nil
		}
		return nil, describe("a date-time string", v)
	case kindInt:
		return toInt(v)
	case kindDouble:
		return toDouble(v)
	}
	return coerceDyn(v)
}

func toInt(v any) (any, error) {
	switch x := v.(type) {
	case json.Number:
		if i, err := strconv.ParseInt(x.String(), 10, 64); err == nil {
			return i, nil
		}
		// Exponent forms such as 1e2 or 3.0. The precision keeps every
		// written digit, so a fraction is not rounded away, but a value
		// below big.Float's exponent range, such as 1e-1000000000, becomes
		// zero: a zero result from a mantissa with a nonzero digit is a
		// fraction.
		f, _, err := big.ParseFloat(x.String(), 10, uint(len(x))*4+64, big.ToNearestEven)
		mantissa, _, _ := strings.Cut(strings.ToLower(x.String()), "e")
		if err == nil && f.IsInf() {
			return nil, fmt.Errorf("integer %s does not fit in 64 bits", x)
		}
		if err != nil || !f.IsInt() || f.Sign() == 0 && strings.ContainsAny(mantissa, "123456789") {
			return nil, fmt.Errorf("must be an integer, got %s", x)
		}
		i, acc := f.Int64()
		if acc != big.Exact {
			return nil, fmt.Errorf("integer %s does not fit in 64 bits", x)
		}
		return i, nil
	case int:
		return int64(x), nil
	case int8:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int64:
		return x, nil
	case uint8:
		return int64(x), nil
	case uint16:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case uint:
		if uint64(x) > math.MaxInt64 {
			return nil, fmt.Errorf("integer %d does not fit in 64 bits", x)
		}
		return int64(x), nil
	case uint64:
		if x > math.MaxInt64 {
			return nil, fmt.Errorf("integer %d does not fit in 64 bits", x)
		}
		return int64(x), nil
	case float64:
		if x != math.Trunc(x) || x < math.MinInt64 || x >= math.MaxInt64 {
			return nil, fmt.Errorf("must be an integer, got %v", x)
		}
		return int64(x), nil
	}
	return nil, describe("an integer", v)
}

func toDouble(v any) (any, error) {
	switch x := v.(type) {
	case json.Number:
		f, err := strconv.ParseFloat(x.String(), 64)
		if err != nil {
			return nil, fmt.Errorf("number %s is out of range", x)
		}
		return f, nil
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case int:
		return float64(x), nil
	case int32:
		return float64(x), nil
	case int64:
		return float64(x), nil
	}
	return nil, describe("a number", v)
}

// coerceDyn converts the numbers of an untyped value: int64 when integral
// and in range, float64 otherwise.
func coerceDyn(v any) (any, error) {
	switch x := v.(type) {
	case json.Number:
		if i, err := strconv.ParseInt(x.String(), 10, 64); err == nil {
			return i, nil
		}
		f, err := strconv.ParseFloat(x.String(), 64)
		if err != nil {
			return nil, fmt.Errorf("number %s is out of range", x)
		}
		return f, nil
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			c, err := coerceDyn(e)
			if err != nil {
				return nil, err
			}
			out[k] = c
		}
		return out, nil
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			c, err := coerceDyn(e)
			if err != nil {
				return nil, err
			}
			out[i] = c
		}
		return out, nil
	}
	return v, nil
}

func mismatch(path, want string, v any) error {
	return fmt.Errorf("%s: %w", pathOrRoot(path), describe(want, v))
}

func describe(want string, v any) error {
	switch v.(type) {
	case nil:
		return fmt.Errorf("must be %s, got null", want)
	case map[string]any:
		return fmt.Errorf("must be %s, got an object", want)
	case []any:
		return fmt.Errorf("must be %s, got an array", want)
	}
	return fmt.Errorf("must be %s, got %T", want, v)
}

func pathOrRoot(path string) string {
	if path == "" {
		return "payload"
	}
	return path
}
