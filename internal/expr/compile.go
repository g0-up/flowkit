package expr

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"time"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"cel.dev/cel-go/common/types/traits"
	"cel.dev/cel-go/interpreter"
)

// Error is a compile problem in one expression. Line and Column (both
// 1-based) locate it in the expression; they are 0 when unknown.
type Error struct {
	Line, Column int
	Msg          string
}

func (e *Error) Error() string {
	if e.Line <= 0 {
		return e.Msg
	}
	return fmt.Sprintf("%d:%d: %s", e.Line, e.Column, e.Msg)
}

// Want is the type an expression must produce.
type Want struct {
	name  string
	types []*types.Type // empty: any type
}

// Wants of the expression fields.
var (
	WantAny    = Want{name: "any value"}
	WantBool   = Want{name: "bool", types: []*types.Type{types.BoolType}}
	WantString = Want{name: "string", types: []*types.Type{types.StringType}}
)

func (w Want) String() string { return w.name }

// accepts reports whether a value of the checked type out may satisfy w;
// dyn is accepted and checked at run time where possible.
func (w Want) accepts(out *types.Type) bool {
	if len(w.types) == 0 {
		return true
	}
	return slices.ContainsFunc(w.types, func(t *types.Type) bool { return mayAssign(t, out) })
}

func mayAssign(to, from *types.Type) bool {
	if to.Kind() == types.DynKind || from.Kind() == types.DynKind {
		return true
	}
	if to.Kind() != from.Kind() || to.TypeName() != from.TypeName() || len(to.Parameters()) != len(from.Parameters()) {
		return false
	}
	for i, p := range to.Parameters() {
		if !mayAssign(p, from.Parameters()[i]) {
			return false
		}
	}
	return true
}

// runtimeAccepts checks an evaluated value when w names only primitive
// types; composite results are left to the caller's schema validation.
func (w Want) runtimeAccepts(v ref.Val) bool {
	if len(w.types) == 0 {
		return true
	}
	for _, t := range w.types {
		switch t.Kind() {
		case types.BoolKind, types.IntKind, types.DoubleKind, types.StringKind, types.TimestampKind:
			if v.Type().TypeName() == t.TypeName() {
				return true
			}
		default:
			return true
		}
	}
	return false
}

// Want returns what an expression placed at pointer inside a value of this
// shape must produce, such as an action param. A number accepts int and
// double, a date-time accepts a timestamp or a string; objects, maps and
// undescribed places accept anything and are left to schema validation of
// the resolved value.
func (s *Shape) Want(pointer string) Want {
	n := s.at(pointer)
	if n == nil {
		return WantAny
	}
	switch n.kind {
	case kindBool:
		return WantBool
	case kindInt:
		return Want{name: "int", types: []*types.Type{types.IntType}}
	case kindDouble:
		return Want{name: "number", types: []*types.Type{types.DoubleType, types.IntType}}
	case kindString:
		return WantString
	case kindTimestamp:
		return Want{name: "timestamp or string", types: []*types.Type{types.TimestampType, types.StringType}}
	case kindList:
		elem := types.DynType
		switch n.elem.kind {
		case kindBool:
			elem = types.BoolType
		case kindInt:
			elem = types.IntType
		case kindString:
			elem = types.StringType
		}
		t := types.NewListType(elem)
		return Want{name: t.String(), types: []*types.Type{t}}
	}
	return WantAny
}

// Program is a compiled expression. It is safe for concurrent use.
type Program struct {
	src     string
	prg     cel.Program
	want    Want
	refs    []string
	timeout time.Duration
}

// Compile parses and type-checks src. Beyond CEL's own checks it requires
// the result to satisfy want, every optional field read to be guarded by
// has() (see checkPresence), and the estimated cost to stay within
// MaxCost. It returns every problem found.
func (e *Env) Compile(src string, want Want) (*Program, []error) {
	ast, iss := e.cel.Compile(src)
	if iss != nil && iss.Err() != nil {
		var errs []error
		for _, ce := range iss.Errors() {
			errs = append(errs, &Error{Line: ce.Location.Line(), Column: ce.Location.Column() + 1, Msg: ce.Message})
		}
		return nil, errs
	}
	var errs []error
	if out := ast.OutputType(); !want.accepts(out) {
		errs = append(errs, &Error{Msg: fmt.Sprintf("expression has type %s, want %s", out, want)})
	}
	errs = append(errs, e.checkPresence(ast)...)
	est, err := e.cel.EstimateCost(ast, estimator{env: e})
	switch {
	case err != nil:
		errs = append(errs, &Error{Msg: "estimate cost: " + err.Error()})
	case est.Max > MaxCost:
		errs = append(errs, &Error{Msg: fmt.Sprintf(
			"estimated cost %d exceeds the limit %d; bound the lists and strings the expression iterates with maxItems or maxLength in their schema",
			est.Max, MaxCost)})
	}
	if len(errs) > 0 {
		return nil, errs
	}
	prg, err := e.cel.Program(ast,
		cel.CostLimit(MaxCost),
		cel.EvalOptions(cel.OptTrackCost),
		cel.InterruptCheckFrequency(interruptEvery))
	if err != nil {
		return nil, []error{&Error{Msg: err.Error()}}
	}
	return &Program{src: src, prg: prg, want: want, refs: e.factRefs(ast), timeout: EvalTimeout}, nil
}

// Source returns the expression text.
func (p *Program) Source() string { return p.src }

// Refs returns the full names of the facts the expression reads, sorted.
func (p *Program) Refs() []string { return slices.Clone(p.refs) }

// Eval evaluates the program within MaxCost and EvalTimeout. Exceeding
// either returns a *LimitError; cancellation of ctx returns ctx's error.
func (p *Program) Eval(ctx context.Context, vars Vars) (ref.Val, error) {
	evalCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	v, _, err := p.prg.ContextEval(evalCtx, vars.activation())
	if err != nil {
		var cancelled interpreter.EvalCancelledError
		switch {
		case errors.As(err, &cancelled) && cancelled.Cause == interpreter.CostLimitExceeded:
			return nil, &LimitError{}
		case ctx.Err() != nil:
			return nil, fmt.Errorf("expression evaluation cancelled: %w", ctx.Err())
		case evalCtx.Err() != nil:
			return nil, &LimitError{Timeout: true}
		}
		return nil, fmt.Errorf("evaluate expression: %w", err)
	}
	if !p.want.runtimeAccepts(v) {
		return nil, fmt.Errorf("expression returned %s, want %s", v.Type().TypeName(), p.want)
	}
	return v, nil
}

// ToNative converts an evaluated value to plain Go: bool, int64, uint64,
// float64, string, nil, []any and map[string]any; a timestamp becomes an
// RFC 3339 string, a duration its Go string, bytes base64.
func ToNative(v ref.Val) (any, error) {
	switch x := v.(type) {
	case types.Bool:
		return bool(x), nil
	case types.Int:
		return int64(x), nil
	case types.Uint:
		return uint64(x), nil
	case types.Double:
		return float64(x), nil
	case types.String:
		return string(x), nil
	case types.Bytes:
		return base64.StdEncoding.EncodeToString(x), nil
	case types.Null:
		return nil, nil
	case types.Timestamp:
		return x.Time.UTC().Format(time.RFC3339Nano), nil
	case types.Duration:
		return x.Duration.String(), nil
	}
	switch x := v.(type) {
	case traits.Lister:
		n, _ := x.Size().(types.Int)
		out := make([]any, 0, int(n))
		for it := x.Iterator(); it.HasNext() == types.True; {
			e, err := ToNative(it.Next())
			if err != nil {
				return nil, err
			}
			out = append(out, e)
		}
		return out, nil
	case traits.Mapper:
		out := map[string]any{}
		for it := x.Iterator(); it.HasNext() == types.True; {
			k := it.Next()
			ks, ok := k.(types.String)
			if !ok {
				return nil, fmt.Errorf("map key %v is %s, want string", k, k.Type().TypeName())
			}
			e, err := ToNative(x.Get(k))
			if err != nil {
				return nil, err
			}
			out[string(ks)] = e
		}
		return out, nil
	}
	if types.IsError(v) {
		return nil, fmt.Errorf("%v", v)
	}
	return nil, fmt.Errorf("cannot convert %s to JSON", v.Type().TypeName())
}
