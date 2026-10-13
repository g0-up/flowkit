package expr

import (
	"maps"
	"slices"
	"strings"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/ast"
)

// factRefs returns the full names of the facts a checked expression reads,
// presence tests included. Reading an intermediate object, or facts as a
// whole, reads every fact under it.
func (e *Env) factRefs(a *cel.Ast) []string {
	if len(e.facts) == 0 {
		return nil
	}
	found := map[string]bool{}
	var visit func(x ast.Expr, shadowed bool)
	visit = func(x ast.Expr, shadowed bool) {
		if segs, ok := factPath(x); ok {
			if !shadowed {
				e.addRefs(found, segs)
			}
			return
		}
		if x.Kind() == ast.ComprehensionKind {
			// A macro variable named facts hides the declared one inside
			// the loop (and, for the accumulator, in the result).
			c := x.AsComprehension()
			loop := shadowed || slices.Contains([]string{c.IterVar(), c.IterVar2(), c.AccuVar()}, VarFacts)
			visit(c.IterRange(), shadowed)
			visit(c.AccuInit(), shadowed)
			visit(c.LoopCondition(), loop)
			visit(c.LoopStep(), loop)
			visit(c.Result(), shadowed || c.AccuVar() == VarFacts)
			return
		}
		for _, c := range children(x) {
			visit(c, shadowed)
		}
	}
	visit(a.NativeRep().Expr(), false)
	return slices.Sorted(maps.Keys(found))
}

// factPath returns the field names of a selection chain rooted at the
// facts variable, e.g. ["customer", "revenue_90d"].
func factPath(x ast.Expr) ([]string, bool) {
	var rev []string
	for x.Kind() == ast.SelectKind {
		rev = append(rev, x.AsSelect().FieldName())
		x = x.AsSelect().Operand()
	}
	if x.Kind() != ast.IdentKind || x.AsIdent() != VarFacts {
		return nil, false
	}
	slices.Reverse(rev)
	return rev, true
}

func (e *Env) addRefs(found map[string]bool, segs []string) {
	for i := 1; i <= len(segs); i++ {
		name := strings.Join(segs[:i], ".")
		if _, ok := e.facts[name]; ok {
			found[name] = true
			return
		}
	}
	prefix := strings.Join(segs, ".")
	for name := range e.facts {
		if prefix == "" || strings.HasPrefix(name, prefix+".") {
			found[name] = true
		}
	}
}

func children(x ast.Expr) []ast.Expr {
	switch x.Kind() {
	case ast.SelectKind:
		return []ast.Expr{x.AsSelect().Operand()}
	case ast.CallKind:
		call := x.AsCall()
		out := slices.Clone(call.Args())
		if call.IsMemberFunction() {
			out = append(out, call.Target())
		}
		return out
	case ast.ListKind:
		return x.AsList().Elements()
	case ast.MapKind:
		var out []ast.Expr
		for _, en := range x.AsMap().Entries() {
			out = append(out, en.AsMapEntry().Key(), en.AsMapEntry().Value())
		}
		return out
	case ast.StructKind:
		var out []ast.Expr
		for _, f := range x.AsStruct().Fields() {
			out = append(out, f.AsStructField().Value())
		}
		return out
	}
	return nil
}
