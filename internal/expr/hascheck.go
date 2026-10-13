package expr

import (
	"fmt"
	"strconv"
	"strings"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/ast"
	"cel.dev/cel-go/common/operators"
	"cel.dev/cel-go/common/types"
)

// checkPresence requires every read of a field that may be absent to be
// guarded by has() on the same logical branch. A field may be absent when
// it is not in its object schema's "required" list, or when it is a key of
// a map, read as m.k or m["k"]. A read is guarded when has() of the same
// path (or of a longer path through it), or a map membership test, must be
// true for the read to matter:
//
//	has(x.f) && x.f > 0          x.f > 0 && has(x.f)
//	!has(x.f) || x.f > 0         has(x.f) ? x.f : 0
//	"k" in m && m["k"] > 0       has(m.k) && m["k"] > 0
//
// A comprehension over a map guards m[k] for its key variable k. Paths are
// compared as written (identifiers, field names, and indexes that are
// constants or paths), so the check is conservative: a guard it cannot
// match, such as one through a call result or a variable bound by a macro
// over another path, is an error and the expression must be rewritten.
// Reading a list index out of range is not covered; it fails at run time.
func (e *Env) checkPresence(a *cel.Ast) []error {
	c := &presence{env: e, ast: a.NativeRep()}
	c.walk(c.ast.Expr(), guards{})
	return c.errs
}

type presence struct {
	env  *Env
	ast  *ast.AST
	errs []error
}

// guards is a set of rendered paths known to be present.
type guards map[string]bool

func (g guards) with(more guards) guards {
	if len(more) == 0 {
		return g
	}
	out := make(guards, len(g)+len(more))
	for k := range g {
		out[k] = true
	}
	for k := range more {
		out[k] = true
	}
	return out
}

// without drops the paths that mention the given variables, as their root
// or in an index, which a comprehension rebinds.
func (g guards) without(vars ...string) guards {
	out := make(guards, len(g))
	for k := range g {
		drop := false
		for _, v := range vars {
			if v != "" && mentions(k, v) {
				drop = true
			}
		}
		if !drop {
			out[k] = true
		}
	}
	return out
}

// mentions reports whether the rendered path p starts with the variable v
// or uses it at the start of an index.
func mentions(p, v string) bool {
	for i := 0; i+len(v) <= len(p); i++ {
		if (i == 0 || p[i-1] == '[') && strings.HasPrefix(p[i:], v) {
			if end := i + len(v); end == len(p) || strings.ContainsRune(".[]", rune(p[end])) {
				return true
			}
		}
	}
	return false
}

func intersect(a, b guards) guards {
	out := guards{}
	for k := range a {
		if b[k] {
			out[k] = true
		}
	}
	return out
}

func (c *presence) walk(e ast.Expr, g guards) {
	switch e.Kind() {
	case ast.SelectKind:
		sel := e.AsSelect()
		if !sel.IsTestOnly() {
			c.checkSelect(e, g)
		}
		c.walk(sel.Operand(), g)
	case ast.CallKind:
		call := e.AsCall()
		args := call.Args()
		switch call.FunctionName() {
		case operators.LogicalAnd:
			// CEL's && and || commute, even over errors, so each side is
			// guarded by what the other side asserts.
			c.walk(args[0], g.with(c.whenTrue(args[1])))
			c.walk(args[1], g.with(c.whenTrue(args[0])))
			return
		case operators.LogicalOr:
			c.walk(args[0], g.with(c.whenFalse(args[1])))
			c.walk(args[1], g.with(c.whenFalse(args[0])))
			return
		case operators.Conditional:
			c.walk(args[0], g)
			c.walk(args[1], g.with(c.whenTrue(args[0])))
			c.walk(args[2], g.with(c.whenFalse(args[0])))
			return
		}
		if call.FunctionName() == operators.Index && len(args) == 2 {
			c.checkIndex(e, g)
		}
		if call.IsMemberFunction() {
			c.walk(call.Target(), g)
		}
		for _, a := range args {
			c.walk(a, g)
		}
	case ast.ComprehensionKind:
		comp := e.AsComprehension()
		c.walk(comp.IterRange(), g)
		c.walk(comp.AccuInit(), g)
		inner := g.without(comp.IterVar(), comp.IterVar2(), comp.AccuVar())
		if p, ok := render(comp.IterRange()); ok && c.ast.GetType(comp.IterRange().ID()).Kind() == types.MapKind {
			inner = inner.with(guards{p + "[" + comp.IterVar() + "]": true})
		}
		c.walk(comp.LoopCondition(), inner)
		c.walk(comp.LoopStep(), inner)
		c.walk(comp.Result(), g.without(comp.AccuVar()))
	case ast.ListKind:
		for _, el := range e.AsList().Elements() {
			c.walk(el, g)
		}
	case ast.MapKind:
		for _, en := range e.AsMap().Entries() {
			me := en.AsMapEntry()
			c.walk(me.Key(), g)
			c.walk(me.Value(), g)
		}
	case ast.StructKind:
		for _, f := range e.AsStruct().Fields() {
			c.walk(f.AsStructField().Value(), g)
		}
	}
}

// checkSelect reports the field read e unless it is always present or g
// guards it.
func (c *presence) checkSelect(e ast.Expr, g guards) {
	sel := e.AsSelect()
	t := c.ast.GetType(sel.Operand().ID())
	switch {
	case t.Kind() == types.MapKind:
	case t.Kind() == types.StructKind:
		o, ok := c.env.prov.object(t)
		if !ok {
			return
		}
		if f, ok := o.node.fields[sel.FieldName()]; !ok || f.required {
			return
		}
	default:
		return
	}
	p, ok := render(e)
	if ok && g[p] {
		return
	}
	loc := c.ast.SourceInfo().GetStartLocation(e.ID())
	err := &Error{Line: loc.Line(), Column: loc.Column() + 1}
	if ok {
		err.Msg = fmt.Sprintf("%s may be absent; guard the read with has(%s)", p, p)
	} else {
		err.Msg = fmt.Sprintf("field %q may be absent and its presence cannot be checked here; read it through a path has() can test", sel.FieldName())
	}
	c.errs = append(c.errs, err)
}

// checkIndex reports the map read m[k] at e unless g guards it.
func (c *presence) checkIndex(e ast.Expr, g guards) {
	args := e.AsCall().Args()
	if c.ast.GetType(args[0].ID()).Kind() != types.MapKind {
		return
	}
	p, ok := render(e)
	if ok && g[p] {
		return
	}
	loc := c.ast.SourceInfo().GetStartLocation(e.ID())
	err := &Error{Line: loc.Line(), Column: loc.Column() + 1}
	m, mok := render(args[0])
	key, kok := renderKey(args[1])
	switch {
	case ok && mok && strings.HasPrefix(p, m+"."):
		err.Msg = fmt.Sprintf("%s may be absent; guard the read with has(%s)", p, p)
	case ok && mok && kok:
		err.Msg = fmt.Sprintf("%s may be absent; guard the read with %s in %s", p, key, m)
	default:
		err.Msg = "a map key may be absent and its presence cannot be checked here; read it through a path a key in map test can guard"
	}
	c.errs = append(c.errs, err)
}

// whenTrue returns the paths present whenever e evaluates to true.
func (c *presence) whenTrue(e ast.Expr) guards {
	switch e.Kind() {
	case ast.SelectKind:
		if e.AsSelect().IsTestOnly() {
			return presentPaths(e)
		}
	case ast.CallKind:
		call := e.AsCall()
		args := call.Args()
		switch call.FunctionName() {
		case operators.LogicalAnd:
			return c.whenTrue(args[0]).with(c.whenTrue(args[1]))
		case operators.LogicalOr:
			return intersect(c.whenTrue(args[0]), c.whenTrue(args[1]))
		case operators.LogicalNot:
			return c.whenFalse(args[0])
		case operators.In:
			return c.memberPaths(args[0], args[1])
		}
	}
	return nil
}

// memberPaths returns the paths a true key in m test proves present: m[key]
// and every select path through m.
func (c *presence) memberPaths(key, m ast.Expr) guards {
	if c.ast.GetType(m.ID()).Kind() != types.MapKind {
		return nil
	}
	p, ok := render(m)
	if !ok {
		return nil
	}
	g := throughPaths(m)
	if k, ok := renderIndex(p, key); ok {
		g[k] = true
	}
	return g
}

// whenFalse returns the paths present whenever e evaluates to false.
func (c *presence) whenFalse(e ast.Expr) guards {
	if e.Kind() != ast.CallKind {
		return nil
	}
	call := e.AsCall()
	args := call.Args()
	switch call.FunctionName() {
	case operators.LogicalOr:
		return c.whenFalse(args[0]).with(c.whenFalse(args[1]))
	case operators.LogicalAnd:
		return intersect(c.whenFalse(args[0]), c.whenFalse(args[1]))
	case operators.LogicalNot:
		return c.whenTrue(args[0])
	}
	return nil
}

// presentPaths returns the path a has() test proves present and every
// select path it runs through.
func presentPaths(test ast.Expr) guards {
	sel := test.AsSelect()
	g := throughPaths(sel.Operand())
	if p, ok := render(sel.Operand()); ok {
		g[p+"."+sel.FieldName()] = true
	}
	return g
}

// throughPaths returns e and the select paths under it, which must be
// present for e to evaluate.
func throughPaths(e ast.Expr) guards {
	g := guards{}
	for {
		switch {
		case e.Kind() == ast.SelectKind:
			if p, ok := render(e); ok {
				g[p] = true
			}
			e = e.AsSelect().Operand()
			continue
		case e.Kind() == ast.CallKind && e.AsCall().FunctionName() == operators.Index:
			e = e.AsCall().Args()[0]
			continue
		}
		return g
	}
}

// render spells a path of identifiers, field selections and indexes that
// are constants or paths, e.g. event.items[0].sku. A map read with a
// string key that is an identifier spells like a field selection, m.k, as
// CEL reads both the same way.
func render(e ast.Expr) (string, bool) {
	switch e.Kind() {
	case ast.IdentKind:
		return e.AsIdent(), true
	case ast.SelectKind:
		sel := e.AsSelect()
		if sel.IsTestOnly() {
			return "", false
		}
		p, ok := render(sel.Operand())
		if !ok {
			return "", false
		}
		return p + "." + sel.FieldName(), true
	case ast.CallKind:
		call := e.AsCall()
		if call.FunctionName() != operators.Index || len(call.Args()) != 2 {
			return "", false
		}
		p, ok := render(call.Args()[0])
		if !ok {
			return "", false
		}
		return renderIndex(p, call.Args()[1])
	}
	return "", false
}

// renderIndex spells the index key of the path p.
func renderIndex(p string, key ast.Expr) (string, bool) {
	if key.Kind() == ast.LiteralKind {
		if k, ok := key.AsLiteral().(types.String); ok && isIdentifier(string(k)) {
			return p + "." + string(k), true
		}
	}
	k, ok := renderKey(key)
	if !ok {
		return "", false
	}
	return p + "[" + k + "]", true
}

// renderKey spells an index key: a constant or a path.
func renderKey(key ast.Expr) (string, bool) {
	if key.Kind() != ast.LiteralKind {
		return render(key)
	}
	switch k := key.AsLiteral().(type) {
	case types.Int:
		return strconv.FormatInt(int64(k), 10), true
	case types.Uint:
		return strconv.FormatUint(uint64(k), 10) + "u", true
	case types.String:
		return strconv.Quote(string(k)), true
	case types.Bool:
		return strconv.FormatBool(bool(k)), true
	}
	return "", false
}
