package flowkit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/g0-up/flowkit/internal/schema"
	"github.com/g0-up/flowkit/internal/store"
)

// EventDef registers an event type. Type and PayloadSchema are required.
type EventDef struct {
	// Type names the event, e.g. "order.paid".
	Type    string
	Version int
	// PayloadSchema is the JSON Schema every emitted payload must satisfy.
	// It also types the payload for definition expressions.
	PayloadSchema json.RawMessage
	// DedupKeyField optionally names a top-level payload field, declared
	// with type "string" or "integer" in PayloadSchema, whose value
	// deduplicates events of this type when Event.DedupKey is empty. The
	// field need not be required: an event without it is not deduplicated.
	// An integer is used in plain base-10 form, so 100 and 1e2 are the same
	// key; so are entity ids.
	DedupKeyField string
	// EntityKind is the kind of entity the event is about, e.g. "student".
	// Empty means the event carries no scope.
	EntityKind string
	// EntityIDField names the top-level payload field holding the entity id.
	// Required when EntityKind is set; Emit rejects a payload where it is
	// missing or empty.
	EntityIDField string
	// ScopeFields maps Scope.Extra keys to top-level payload fields.
	ScopeFields map[string]string
}

// Event is one occurrence of a registered event type.
type Event struct {
	Type    string
	Payload map[string]any
	// DedupKey makes Emit a no-op returning ErrDuplicateEvent when an event
	// of the same type with the same key is stored. When empty, the payload
	// field EventDef.DedupKeyField is used; when both are empty the event is
	// not deduplicated.
	DedupKey string
	// OccurredAt is when the event happened. Zero means Config.Clock.Now().
	OccurredAt time.Time
}

const (
	// maxDedupKeyBytes caps the length of a dedup key.
	maxDedupKeyBytes = 512
	// maxKeyIntegerBits caps an integer key or scope value before it is
	// spelled out in base 10, so a short 1e99999 cannot expand into a huge
	// string. 2048 bits is more than 600 digits, past every length limit.
	maxKeyIntegerBits = 2048
	// maxNumberExponent bounds the exponent written in a JSON number.
	// Postgres stores jsonb numbers in plain decimal, so a short 1e131000
	// would grow to 131 kB and 0e-999999 is refused.
	maxNumberExponent = 1000
	// maxNumberScale is the most digits after the decimal point a Postgres
	// numeric holds.
	maxNumberScale = 16383
)

// Emit stores evt and enqueues its routing, both on tx, the caller's
// transaction. Nothing happens unless the caller commits tx; a rollback
// leaves neither the event nor its job.
//
// Emit validates before it writes. ErrUnknownEvent, ValidationErrors (from
// the size, nesting-depth and dedup-key limits, text or numbers Postgres
// cannot store, the PayloadSchema, or a missing entity id) and ErrDuplicateEvent leave tx
// untouched and usable: the caller may ignore them and commit. Any other
// error, from the database or from enqueuing the job, can abort tx; the
// caller must roll it back.
//
// Every string and object key in the payload, and DedupKey, must be valid
// UTF-8 without U+0000, which Postgres does not store. A json.RawMessage
// inside the payload must therefore not spell U+FFFD as the escape \ufffd
// (encoding/json writes that escape only for invalid UTF-8, so Emit treats
// it as such), nor hold a surrogate escape outside a high-low pair.
// Every number must be a finite float64 that does not underflow to zero,
// with an exponent within ±1000.
//
// Under READ COMMITTED, the default, a concurrent duplicate waits for the
// other transaction and then returns ErrDuplicateEvent. Under REPEATABLE
// READ or SERIALIZABLE, a duplicate committed after tx took its snapshot
// fails with a serialization error (SQLSTATE 40001) that aborts tx; retry
// the whole transaction, and the retry gets ErrDuplicateEvent.
//
// Deduplication holds while the earlier event is kept (RetentionConfig.Event).
func (e *Engine) Emit(ctx context.Context, tx pgx.Tx, evt Event) error {
	if tx == nil {
		return errors.New("flowkit: Emit: nil transaction")
	}
	if e.pg == nil {
		return fmt.Errorf("flowkit: Emit: %w", errNoClient)
	}
	return e.emit(ctx, e.pg.WrapTx(tx), evt)
}

// emit validates evt, stores it and enqueues its routing in stx.
func (e *Engine) emit(ctx context.Context, stx store.Tx, evt Event) error {
	def, ok := e.reg.reg.events[evt.Type]
	if !ok {
		return fmt.Errorf("%w %q", ErrUnknownEvent, evt.Type)
	}
	raw, dedupKey, err := e.checkEvent(def, evt)
	if err != nil {
		return err
	}

	now := e.cfg.Clock.Now()
	occurredAt := evt.OccurredAt
	if occurredAt.IsZero() {
		occurredAt = now
	}
	row := store.EventRow{Type: evt.Type, Payload: raw, OccurredAt: occurredAt, InsertedAt: now}
	if dedupKey != "" {
		row.DedupKey = &dedupKey
	}
	id, inserted, err := e.store.InsertEvent(ctx, stx, row)
	if err != nil {
		return fmt.Errorf("flowkit: emit %q: %w", evt.Type, err)
	}
	if !inserted {
		return fmt.Errorf("%w: type %q matches event %d", ErrDuplicateEvent, evt.Type, id)
	}
	if err := e.jobs.EnqueueRoute(ctx, stx, id); err != nil {
		return fmt.Errorf("flowkit: emit %q: %w", evt.Type, err)
	}
	e.metrics.emitted.Add(ctx, 1, metric.WithAttributes(attribute.String("type", evt.Type)))
	e.cfg.Logger.DebugContext(ctx, "flowkit: event emitted", "type", evt.Type, "event_id", id)
	return nil
}

// checkEvent applies the input limits and the payload schema to evt and
// derives its dedup key. It touches no database. Problems come back as
// ValidationErrors.
func (e *Engine) checkEvent(def EventDef, evt Event) (raw json.RawMessage, dedupKey string, err error) {
	var verrs ValidationErrors
	fail := func(path, format string, args ...any) {
		verrs = append(verrs, &ValidationError{Path: path, Msg: fmt.Sprintf(format, args...)})
	}

	if len(evt.DedupKey) > maxDedupKeyBytes {
		fail("", "dedup key is %d bytes, more than %d", len(evt.DedupKey), maxDedupKeyBytes)
	}
	if problem := textProblem(evt.DedupKey); problem != "" {
		fail("", "dedup key %s", problem)
	}
	payload := evt.Payload
	if payload == nil {
		payload = map[string]any{}
	}
	raw, merr := json.Marshal(payload)
	switch {
	case merr != nil:
		fail("", "payload cannot be encoded as JSON: %v", merr)
	case len(raw) > e.cfg.MaxPayloadBytes:
		fail("", "payload is %d bytes of JSON, more than %d", len(raw), e.cfg.MaxPayloadBytes)
	case schema.Depth(raw) > schema.MaxDepth:
		fail("", "payload nests deeper than %d levels", schema.MaxDepth)
	case !utf8.Valid(raw) || hasBadUnicodeEscape(raw):
		fail("", "payload has a string or key that is not valid UTF-8")
	}
	if len(verrs) > 0 {
		return nil, "", verrs
	}

	doc, derr := schema.Decode(raw)
	if derr != nil {
		fail("", "payload cannot be decoded: %v", derr)
		return nil, "", verrs
	}
	if path, found := findNUL(doc, nil); found {
		fail(schema.Pointer(path...), "payload has a U+0000 character, which Postgres cannot store")
		return nil, "", verrs
	}
	if path, problem := findBadNumber(doc, nil); problem != "" {
		fail(schema.Pointer(path...), "payload number %s", problem)
		return nil, "", verrs
	}
	for _, v := range e.reg.payloads[def.Type].Validate(doc) {
		fail(v.Path, "%s", v.Msg)
	}
	if len(verrs) > 0 {
		return nil, "", verrs
	}

	fields, _ := doc.(map[string]any)
	dedupKey = evt.DedupKey
	if dedupKey == "" && def.DedupKeyField != "" {
		v, ok, ferr := scalarField(fields, def.DedupKeyField)
		switch {
		case ferr != nil:
			fail(schema.Pointer(def.DedupKeyField), "dedup key field %v", ferr)
		case ok && len(v) > maxDedupKeyBytes:
			fail(schema.Pointer(def.DedupKeyField), "dedup key is %d bytes, more than %d", len(v), maxDedupKeyBytes)
		case ok:
			dedupKey = v
		}
	}
	if def.EntityKind != "" {
		_, ok, ferr := scalarField(fields, def.EntityIDField)
		switch {
		case ferr != nil:
			fail(schema.Pointer(def.EntityIDField), "entity id %v", ferr)
		case !ok:
			fail(schema.Pointer(def.EntityIDField), "entity id of %s is missing or empty", def.EntityKind)
		}
	}
	for _, key := range slices.Sorted(maps.Keys(def.ScopeFields)) {
		if _, _, ferr := scalarField(fields, def.ScopeFields[key]); ferr != nil {
			fail(schema.Pointer(def.ScopeFields[key]), "scope field %q %v", key, ferr)
		}
	}
	if len(verrs) > 0 {
		return nil, "", verrs
	}
	return raw, dedupKey, nil
}

// scalarField reads a top-level payload field as the string flowkit stores
// for keys and scope values. A missing, null or empty field reports ok
// false; a value that is neither a string nor a number is an error. An
// integer comes back in plain base-10 form, however the payload spelled it.
func scalarField(fields map[string]any, name string) (value string, ok bool, err error) {
	switch v := fields[name].(type) {
	case nil:
		return "", false, nil
	case string:
		return v, v != "", nil
	case json.Number:
		s, err := canonicalNumber(v)
		return s, err == nil, err
	default:
		return "", false, fmt.Errorf("must be a string or a number, got %T", v)
	}
}

// canonicalNumber spells an integral number in plain base 10, so 100, 1e2
// and 100.0 give "100". It is exact at any size. A number with a fraction
// keeps its JSON spelling.
func canonicalNumber(n json.Number) (string, error) {
	r, ok := new(big.Rat).SetString(n.String())
	if !ok || !r.IsInt() {
		return n.String(), nil
	}
	if r.Num().BitLen() > maxKeyIntegerBits {
		return "", fmt.Errorf("is an integer too large to use as a key, more than %d bits", maxKeyIntegerBits)
	}
	return r.Num().String(), nil
}

// textProblem says why Postgres would refuse s as text, or returns "".
func textProblem(s string) string {
	switch {
	case !utf8.ValidString(s):
		return "is not valid UTF-8"
	case strings.ContainsRune(s, 0):
		return "has a U+0000 character, which Postgres cannot store"
	}
	return ""
}

// hasBadUnicodeEscape reports whether the JSON text raw holds an escape that
// stands for invalid text: \ufffd, which encoding/json writes in place of
// invalid UTF-8 in a string, or a surrogate escape that is not part of a
// high-low pair, which Postgres rejects in jsonb.
func hasBadUnicodeEscape(raw []byte) bool {
	inString := false
	for i := 0; i < len(raw); i++ {
		switch c := raw[i]; {
		case c == '"':
			inString = !inString
		case c == '\\' && inString:
			i++ // the escaped character
			if i >= len(raw) || raw[i] != 'u' {
				continue
			}
			r, ok := hex4(raw, i+1)
			if !ok {
				continue // not valid JSON; decoding reports it
			}
			i += 4
			switch {
			case r == 0xFFFD:
				return true
			case r >= 0xDC00 && r <= 0xDFFF:
				return true // low surrogate without a high one before it
			case r >= 0xD800 && r <= 0xDBFF:
				low, ok := hex4(raw, i+3)
				if i+2 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' || !ok || low < 0xDC00 || low > 0xDFFF {
					return true
				}
				i += 6
			}
		}
	}
	return false
}

// hex4 decodes the four hex digits at raw[i:i+4].
func hex4(raw []byte, i int) (rune, bool) {
	if i < 0 || i+4 > len(raw) {
		return 0, false
	}
	v, err := strconv.ParseUint(string(raw[i:i+4]), 16, 16)
	return rune(v), err == nil
}

// findNUL returns the path to the first string or key holding U+0000 in a
// decoded JSON document.
func findNUL(v any, path []string) ([]string, bool) {
	switch v := v.(type) {
	case string:
		return path, strings.ContainsRune(v, 0)
	case []any:
		for i, e := range v {
			if p, found := findNUL(e, append(path, strconv.Itoa(i))); found {
				return p, true
			}
		}
	case map[string]any:
		for _, k := range slices.Sorted(maps.Keys(v)) {
			p := append(path, k)
			if strings.ContainsRune(k, 0) {
				return p, true
			}
			if p, found := findNUL(v[k], p); found {
				return p, true
			}
		}
	}
	return nil, false
}

// findBadNumber returns the path to the first number in a decoded JSON
// document that numberProblem rejects, and the problem.
func findBadNumber(v any, path []string) ([]string, string) {
	switch v := v.(type) {
	case json.Number:
		return path, numberProblem(v)
	case []any:
		for i, e := range v {
			if p, problem := findBadNumber(e, append(path, strconv.Itoa(i))); problem != "" {
				return p, problem
			}
		}
	case map[string]any:
		for _, k := range slices.Sorted(maps.Keys(v)) {
			if p, problem := findBadNumber(v[k], append(path, k)); problem != "" {
				return p, problem
			}
		}
	}
	return nil, ""
}

// numberProblem says why a JSON number cannot be stored, or returns "". A
// number must be a finite float64 that does not underflow to zero, so its
// size is bounded, and its spelling must stay within the exponent and scale
// limits, so Postgres neither refuses it nor expands it far beyond its text.
func numberProblem(n json.Number) string {
	s := n.String()
	mantissa, exp := s, 0
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		e, err := strconv.Atoi(s[i+1:])
		if err != nil {
			e = maxNumberExponent + 1 // too long for an int
		}
		mantissa, exp = s[:i], e
	}
	f, err := strconv.ParseFloat(s, 64)
	switch {
	case err != nil:
		return "is out of the float64 range"
	case f == 0 && strings.ContainsAny(mantissa, "123456789"):
		return "is too close to zero for a float64"
	case exp > maxNumberExponent || exp < -maxNumberExponent:
		return fmt.Sprintf("has an exponent beyond ±%d", maxNumberExponent)
	}
	if _, frac, ok := strings.Cut(mantissa, "."); ok && len(frac)-exp > maxNumberScale {
		return fmt.Sprintf("has more than %d digits after the decimal point", maxNumberScale)
	}
	return ""
}
