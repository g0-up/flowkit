package model

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"

	"github.com/g0-up/flowkit/internal/schema"
)

// ExprKey marks an expression in action params: an object whose only key is
// ExprKey and whose value is a CEL expression, such as
// {"to": {"$expr": "event.email"}}, is replaced by the expression's value
// when the step runs. It may appear at any depth.
const ExprKey = "$expr"

// ParamExpr is an expression placeholder in action params.
type ParamExpr struct {
	// Pointer is the JSON Pointer of the placeholder relative to the
	// params object, e.g. "/to" or "/items/0/sku".
	Pointer string
	Source  string
}

// ParamExpressions lists the expression placeholders in params, in a
// stable order (object keys sorted, array elements in order).
func ParamExpressions(params json.RawMessage) ([]ParamExpr, error) {
	if len(params) == 0 {
		return nil, nil
	}
	v, err := schema.Decode(params)
	if err != nil {
		return nil, fmt.Errorf("params: %w", err)
	}
	var out []ParamExpr
	err = walkParams(v, "", func(p, src string) error {
		out = append(out, ParamExpr{Pointer: p, Source: src})
		return nil
	})
	return out, err
}

// ResolveParams decodes params and replaces each expression placeholder
// with eval's result. Numbers that are not replaced stay json.Number.
func ResolveParams(params json.RawMessage, eval func(ParamExpr) (any, error)) (map[string]any, error) {
	if len(params) == 0 {
		return map[string]any{}, nil
	}
	v, err := schema.Decode(params)
	if err != nil {
		return nil, fmt.Errorf("params: %w", err)
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("params must be a JSON object")
	}
	out, err := resolve(obj, "", eval)
	if err != nil {
		return nil, err
	}
	return out.(map[string]any), nil
}

func resolve(v any, p string, eval func(ParamExpr) (any, error)) (any, error) {
	if src, ok, problem := exprOf(v); problem != "" {
		return nil, fmt.Errorf("params%s: %s", p, problem)
	} else if ok {
		return eval(ParamExpr{Pointer: p, Source: src})
	}
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for _, k := range slices.Sorted(maps.Keys(x)) {
			r, err := resolve(x[k], p+schema.Pointer(k), eval)
			if err != nil {
				return nil, err
			}
			out[k] = r
		}
		return out, nil
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			r, err := resolve(e, p+"/"+strconv.Itoa(i), eval)
			if err != nil {
				return nil, err
			}
			out[i] = r
		}
		return out, nil
	}
	return v, nil
}

// checkParams reports malformed expression placeholders under base, the
// path of the params object.
func checkParams(base string, params json.RawMessage) []schema.Violation {
	v, err := schema.Decode(params)
	if err != nil {
		return []schema.Violation{{Path: base, Msg: err.Error()}}
	}
	var vs []schema.Violation
	_ = walkParams(v, "", func(string, string) error { return nil }, func(p, problem string) {
		vs = append(vs, schema.Violation{Path: base + p, Msg: problem})
	})
	return vs
}

// walkParams calls fn for each expression placeholder under v, at path p.
// A malformed placeholder is passed to the optional bad callback, or else
// returned as an error.
func walkParams(v any, p string, fn func(p, src string) error, bad ...func(p, problem string)) error {
	if src, ok, problem := exprOf(v); problem != "" {
		if len(bad) > 0 {
			bad[0](p, problem)
			return nil
		}
		return fmt.Errorf("params%s: %s", p, problem)
	} else if ok {
		return fn(p, src)
	}
	switch x := v.(type) {
	case map[string]any:
		for _, k := range slices.Sorted(maps.Keys(x)) {
			if err := walkParams(x[k], p+schema.Pointer(k), fn, bad...); err != nil {
				return err
			}
		}
	case []any:
		for i, e := range x {
			if err := walkParams(e, p+"/"+strconv.Itoa(i), fn, bad...); err != nil {
				return err
			}
		}
	}
	return nil
}

// exprOf reports whether v is an expression placeholder, or why it is a
// malformed one.
func exprOf(v any) (src string, ok bool, problem string) {
	m, isObj := v.(map[string]any)
	if !isObj {
		return "", false, ""
	}
	raw, has := m[ExprKey]
	if !has {
		return "", false, ""
	}
	if len(m) != 1 {
		return "", false, fmt.Sprintf("an expression object must have %q as its only key", ExprKey)
	}
	s, isStr := raw.(string)
	if !isStr || s == "" {
		return "", false, fmt.Sprintf("%q must be a non-empty string", ExprKey)
	}
	return s, true, ""
}
