package model

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	m2 "github.com/g0-up/flowkit/schema"

	"github.com/g0-up/flowkit/internal/schema"
)

// Body limits.
const (
	// MaxBodyBytes caps the size of a definition body.
	MaxBodyBytes = 1 << 20
	// MaxSteps caps the number of steps; the metamodel schema enforces it.
	MaxSteps = 500
	// MaxDuration caps a wait timeout and a dedup window.
	MaxDuration = 365 * 24 * time.Hour
)

var (
	metamodelMu    sync.Mutex
	metamodelCache = map[int]*schema.Compiled{}
)

// metamodelSchema returns the compiled JSON Schema of metamodel version v.
func metamodelSchema(v int) (*schema.Compiled, bool, error) {
	metamodelMu.Lock()
	defer metamodelMu.Unlock()
	if c, ok := metamodelCache[v]; ok {
		return c, true, nil
	}
	mm, ok := m2.Lookup(v)
	if !ok {
		return nil, false, nil
	}
	c, err := schema.Compile(mm.Schema)
	if err != nil {
		return nil, true, fmt.Errorf("metamodel %d: %w", v, err)
	}
	metamodelCache[v] = c
	return c, true, nil
}

type rawDefinition struct {
	Description string `json:"description"`
	Trigger     struct {
		Event string `json:"event"`
	} `json:"trigger"`
	Concurrency *struct {
		Policy      string `json:"policy"`
		SerialKey   string `json:"serial_key"`
		DedupWindow string `json:"dedup_window"`
	} `json:"concurrency"`
	Steps []rawStep `json:"steps"`
}

type rawStep struct {
	ID        string          `json:"id"`
	Type      StepType        `json:"type"`
	Action    string          `json:"action"`
	Params    json.RawMessage `json:"params"`
	When      string          `json:"when"`
	Next      string          `json:"next"`
	Event     string          `json:"event"`
	Match     string          `json:"match"`
	Timeout   string          `json:"timeout"`
	OnTimeout string          `json:"on_timeout"`
	Cases     []Case          `json:"cases"`
	Default   string          `json:"default"`
	Name      string          `json:"name"`
}

// Parse reads a definition body and checks everything that needs no
// registry: size, JSON, the metamodel schema, then the structure (unique
// step ids, transitions to existing steps, durations, the concurrency
// block, expression placeholders in params, and that every step is
// reachable and can reach an end step).
//
// When the body fails the metamodel schema, Parse returns a nil Definition
// and the schema violations. Otherwise it returns the Definition together
// with every structural violation, so a caller can keep checking the
// definition and report all problems at once. Violations are sorted by
// path.
func Parse(body []byte) (*Definition, []schema.Violation) {
	if len(body) > MaxBodyBytes {
		return nil, []schema.Violation{{Msg: fmt.Sprintf("definition is %d bytes; the limit is %d", len(body), MaxBodyBytes)}}
	}
	if !json.Valid(body) {
		return nil, []schema.Violation{{Msg: "definition is not valid JSON"}}
	}
	if d := schema.Depth(body); d > schema.MaxDepth {
		return nil, []schema.Violation{{Msg: fmt.Sprintf("definition nests %d levels deep; the limit is %d", d, schema.MaxDepth)}}
	}
	doc, err := schema.Decode(body)
	if err != nil {
		return nil, []schema.Violation{{Msg: "definition is not valid JSON: " + err.Error()}}
	}
	obj, ok := doc.(map[string]any)
	if !ok {
		return nil, []schema.Violation{{Msg: "definition must be a JSON object"}}
	}
	version, vs := metamodelVersion(obj)
	if vs != nil {
		return nil, vs
	}
	compiled, _, err := metamodelSchema(version)
	if err != nil {
		return nil, []schema.Violation{{Msg: err.Error()}}
	}
	if vs := compiled.Validate(doc); len(vs) > 0 {
		return nil, vs
	}
	var raw rawDefinition
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, []schema.Violation{{Msg: "definition does not match the metamodel: " + err.Error()}}
	}
	d, vs := build(raw)
	d.MetamodelVersion = version
	reported := make(map[string]bool, len(vs))
	for _, v := range vs {
		reported[v.Path] = true
	}
	vs = append(vs, check(d, reported)...)
	sortViolations(vs)
	return d, vs
}

// metamodelVersion reads metamodel_version, which selects the schema the
// rest of the body is checked against.
func metamodelVersion(obj map[string]any) (int, []schema.Violation) {
	const path = "/metamodel_version"
	raw, ok := obj["metamodel_version"]
	if !ok {
		return 0, []schema.Violation{{Path: path, Msg: "missing required property"}}
	}
	n, ok := raw.(json.Number)
	if !ok {
		return 0, []schema.Violation{{Path: path, Msg: "must be an integer"}}
	}
	v, ok := integral(n)
	if !ok {
		return 0, []schema.Violation{{Path: path, Msg: "must be an integer"}}
	}
	if _, known, _ := metamodelSchema(int(v)); v > math.MaxInt32 || v < 1 || !known {
		return 0, []schema.Violation{{Path: path, Msg: fmt.Sprintf("unsupported metamodel version %d; supported: %s", v, joinInts(m2.Versions()))}}
	}
	return int(v), nil
}

// integral reads n as an int64 when it has no fraction, however it is
// spelled: 1, 1.0 and 10e-1 are all 1, as JSON Schema's "integer" has it.
func integral(n json.Number) (int64, bool) {
	if v, err := strconv.ParseInt(n.String(), 10, 64); err == nil {
		return v, true
	}
	// big.Rat expands the exponent; one this large cannot spell an int64 in
	// a body of bounded size anyway.
	if i := strings.IndexAny(n.String(), "eE"); i >= 0 {
		if e, err := strconv.Atoi(n.String()[i+1:]); err != nil || e > 1000 || e < -1000 {
			return 0, false
		}
	}
	r, ok := new(big.Rat).SetString(n.String())
	if !ok || !r.IsInt() || !r.Num().IsInt64() {
		return 0, false
	}
	return r.Num().Int64(), true
}

func joinInts(vs []int) string {
	s := make([]string, len(vs))
	for i, v := range vs {
		s[i] = strconv.Itoa(v)
	}
	return strings.Join(s, ", ")
}

// build converts a body that satisfies the metamodel schema.
func build(raw rawDefinition) (*Definition, []schema.Violation) {
	var vs []schema.Violation
	d := &Definition{
		Description: raw.Description,
		Trigger:     Trigger{Event: raw.Trigger.Event},
		Concurrency: Concurrency{Policy: PolicyConcurrent},
		Steps:       make([]*Step, len(raw.Steps)),
		index:       make(map[string]int, len(raw.Steps)),
	}
	if c := raw.Concurrency; c != nil {
		d.Concurrency.Policy = c.Policy
		d.Concurrency.SerialKey = c.SerialKey
		if c.DedupWindow != "" {
			w, err := ParseDuration(c.DedupWindow)
			if err != nil {
				vs = append(vs, schema.Violation{Path: "/concurrency/dedup_window", Msg: err.Error()})
			}
			d.Concurrency.DedupWindow = w
		}
	}
	for i, r := range raw.Steps {
		s := &Step{ID: r.ID, Type: r.Type, Index: i}
		switch r.Type {
		case StepAction:
			s.Action = &ActionStep{Action: r.Action, When: r.When, Next: r.Next}
			if len(r.Params) > 0 {
				s.Action.Params = r.Params
			}
		case StepWait:
			t, err := ParseDuration(r.Timeout)
			if err != nil {
				vs = append(vs, schema.Violation{Path: s.Path() + "/timeout", Msg: err.Error()})
			}
			s.Wait = &WaitStep{Event: r.Event, Match: r.Match, Timeout: t, OnTimeout: r.OnTimeout, Next: r.Next}
		case StepSwitch:
			s.Switch = &SwitchStep{Cases: r.Cases, Default: r.Default}
		case StepEnd:
			s.End = &EndStep{Name: r.Name}
		}
		d.Steps[i] = s
		if _, dup := d.index[s.ID]; !dup {
			d.index[s.ID] = i
		}
	}
	return d, vs
}

// ParseDuration reads a duration written as one or more <number><unit>
// groups, unit s, m, h or d (a day is 24 hours), e.g. "90s", "1h30m", "7d".
func ParseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}
	const maxSeconds = math.MaxInt64 / int64(time.Second)
	var total int64
	for rest := s; rest != ""; {
		i := 0
		for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' {
			i++
		}
		if i == 0 || i == len(rest) {
			return 0, fmt.Errorf("invalid duration %q", s)
		}
		n, err := strconv.ParseInt(rest[:i], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q", s)
		}
		var unit int64
		switch rest[i] {
		case 's':
			unit = 1
		case 'm':
			unit = 60
		case 'h':
			unit = 3600
		case 'd':
			unit = 86400
		default:
			return 0, fmt.Errorf("invalid duration %q: unit must be s, m, h or d", s)
		}
		if n > (maxSeconds-total)/unit {
			return 0, fmt.Errorf("duration %q is too long", s)
		}
		total += n * unit
		rest = rest[i+1:]
	}
	return time.Duration(total) * time.Second, nil
}
