package interpreter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"

	"github.com/g0-up/flowkit/internal/expr"
	"github.com/g0-up/flowkit/internal/store"
)

// A run's facts are stored in runs.facts as one flat JSON object keyed by
// full fact name ("customer.vip"). In memory they are always the decoded
// form of that object (numbers as json.Number, times as RFC 3339 strings),
// so a pump that resumes an entered step sees exactly the values the step
// was entered with; Env.FactValues coerces them to the declared types.

// decodeFacts reads runs.facts.
func decodeFacts(raw json.RawMessage) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("decode facts: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("decode facts: data after the facts object")
	}
	if m == nil {
		m = map[string]any{}
	}
	return m, nil
}

func encodeFacts(m map[string]any) (json.RawMessage, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("encode facts: %w", err)
	}
	return b, nil
}

// mergeFacts returns facts with values set over it, in stored form. A nil
// value removes the fact.
func mergeFacts(facts, values map[string]any) (map[string]any, error) {
	next := maps.Clone(facts)
	if next == nil {
		next = map[string]any{}
	}
	for k, v := range values {
		if v == nil {
			delete(next, k)
			continue
		}
		next[k] = v
	}
	raw, err := encodeFacts(next)
	if err != nil {
		return nil, err
	}
	return decodeFacts(raw)
}

// errFactResolution marks a failure of a registered fact's resolver.
var errFactResolution = errors.New("fact resolution failed")

// resolveFacts resolves the plan's registered facts for the run's scope in
// one read-only transaction bounded by the fact timeout. A resolver error
// or panic, a write attempt or a timeout returns an error wrapping
// errFactResolution: the run cannot go on with an unknown fact. A nil
// value leaves the fact absent.
func (in *Interpreter) resolveFacts(ctx context.Context, p *Plan, scope expr.Scope) (map[string]any, error) {
	if len(p.Facts) == 0 {
		return nil, nil
	}
	out := make(map[string]any, len(p.Facts))
	err := in.store.ReadOnly(ctx, in.factTimeout, func(ctx context.Context, q store.ReadQuerier) error {
		for _, f := range p.Facts {
			v, err := resolveFact(ctx, f, q, scope)
			if err != nil {
				return fmt.Errorf("fact %q: %w", f.Name, err)
			}
			out[f.Name] = v
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errFactResolution, err)
	}
	return out, nil
}

// resolveFact runs one resolver, turning a panic into an error so that it
// fails the run instead of the pump.
func resolveFact(ctx context.Context, f FactSource, q store.ReadQuerier, scope expr.Scope) (v any, err error) {
	defer func() {
		if p := recover(); p != nil {
			v, err = nil, fmt.Errorf("resolver panicked: %v", p)
		}
	}()
	return f.Resolve(ctx, q, scope)
}
