package expr

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"

	m2 "github.com/g0-up/flowkit/schema"
)

// Variable names.
const (
	VarEvent = "event"
	VarFacts = "facts"
	VarNow   = "now"
	VarScope = "scope"
)

// Type is the type of a fact.
type Type int

// Fact types.
const (
	Bool Type = iota + 1
	Int
	Double
	String
	Timestamp
)

func (t Type) kind() (kind, bool) {
	switch t {
	case Bool:
		return kindBool, true
	case Int:
		return kindInt, true
	case Double:
		return kindDouble, true
	case String:
		return kindString, true
	case Timestamp:
		return kindTimestamp, true
	}
	return kindDyn, false
}

// Fact declares a fact expressions may read as facts.<Name>. A dotted name
// nests: "customer.revenue_90d" is facts.customer.revenue_90d.
type Fact struct {
	Name string
	Type Type
	// Optional facts may be absent, so reading one needs a has() guard.
	Optional bool
}

// MetamodelOptions are the expression options a metamodel version fixes.
type MetamodelOptions struct {
	Version int
	Macros  []cel.Macro
}

var macrosByName = map[string]cel.Macro{
	"has":        cel.HasMacro,
	"all":        cel.AllMacro,
	"exists":     cel.ExistsMacro,
	"exists_one": cel.ExistsOneMacro,
	"map":        cel.MapMacro,
	"filter":     cel.FilterMacro,
}

// Metamodel returns the options of metamodel version v.
func Metamodel(v int) (MetamodelOptions, error) {
	mm, ok := m2.Lookup(v)
	if !ok {
		return MetamodelOptions{}, fmt.Errorf("unsupported metamodel version %d", v)
	}
	if len(mm.Extensions) > 0 {
		return MetamodelOptions{}, fmt.Errorf("metamodel %d: unsupported CEL extensions %q", v, mm.Extensions)
	}
	o := MetamodelOptions{Version: v}
	for _, name := range mm.Macros {
		m, ok := macrosByName[name]
		if !ok {
			return MetamodelOptions{}, fmt.Errorf("metamodel %d: unknown macro %q", v, name)
		}
		o.Macros = append(o.Macros, m)
		// map has a three-argument form, map(x, cond, value).
		if name == "map" {
			o.Macros = append(o.Macros, cel.MapFilterMacro)
		}
	}
	return o, nil
}

// Parser limits.
const (
	MaxExpressionBytes = 4096
	MaxRecursionDepth  = 64
)

// Env is a compile environment: the declared variables and their types.
// It is safe for concurrent use once built.
type Env struct {
	cel  *cel.Env
	prov *provider
	// roots holds the shape of each declared variable, for the cost
	// estimator and the has() check.
	roots map[string]*node
	facts map[string]Type // declared facts by full name
}

// scopeNode types the scope variable.
var scopeNode = &node{kind: kindObject, fields: map[string]*field{
	"entity_kind": {node: &node{kind: kindString}, required: true},
	"entity_id":   {node: &node{kind: kindString}, required: true},
	"extra":       {node: &node{kind: kindMap, elem: &node{kind: kindString}}, required: true},
}}

// NewEnv declares event (typed by the trigger payload), facts, now and
// scope: the environment of step expressions.
func NewEnv(event *Shape, facts []Fact, mm MetamodelOptions) (*Env, error) {
	return newEnv(event, facts, true, mm)
}

// NewEventEnv declares event, now and scope but no facts: the environment
// of a wait match (event is the waited event) and of the serial key.
func NewEventEnv(event *Shape, mm MetamodelOptions) (*Env, error) {
	return newEnv(event, nil, false, mm)
}

func newEnv(event *Shape, facts []Fact, withFacts bool, mm MetamodelOptions) (*Env, error) {
	if event == nil {
		return nil, fmt.Errorf("expr: nil event shape")
	}
	prov, err := newProvider()
	if err != nil {
		return nil, fmt.Errorf("expr: %w", err)
	}
	e := &Env{prov: prov, roots: map[string]*node{VarEvent: event.root, VarScope: scopeNode}}
	opts := []cel.EnvOption{
		cel.CustomTypeProvider(prov),
		cel.ClearMacros(),
		cel.Macros(mm.Macros...),
		cel.ParserExpressionSizeLimit(MaxExpressionBytes),
		cel.ParserRecursionLimit(MaxRecursionDepth),
		cel.Variable(VarEvent, prov.typeOf(event.root, "flowkit:event")),
		cel.Variable(VarNow, types.TimestampType),
		cel.Variable(VarScope, prov.typeOf(scopeNode, "flowkit:scope")),
	}
	if withFacts {
		root, err := factTree(facts)
		if err != nil {
			return nil, err
		}
		e.roots[VarFacts] = root
		e.facts = make(map[string]Type, len(facts))
		for _, f := range facts {
			e.facts[f.Name] = f.Type
		}
		opts = append(opts, cel.Variable(VarFacts, prov.typeOf(root, "flowkit:facts")))
	}
	ce, err := cel.NewEnv(opts...)
	if err != nil {
		return nil, fmt.Errorf("expr: %w", err)
	}
	e.cel = ce
	return e, nil
}

// ValidFactName reports whether expressions can read a fact named name:
// dot-separated CEL identifiers, none of them a reserved word.
func ValidFactName(name string) bool {
	for part := range strings.SplitSeq(name, ".") {
		if !isIdentifier(part) {
			return false
		}
	}
	return true
}

// celReserved lists the words CEL keeps from identifiers and field
// selections.
var celReserved = map[string]bool{
	"as": true, "break": true, "const": true, "continue": true, "else": true,
	"false": true, "for": true, "function": true, "if": true, "import": true,
	"in": true, "let": true, "loop": true, "package": true, "namespace": true,
	"null": true, "return": true, "true": true, "var": true, "void": true,
	"while": true,
}

// isIdentifier reports whether s can be written as a CEL identifier or
// field selection.
func isIdentifier(s string) bool {
	if s == "" || celReserved[s] {
		return false
	}
	for i, r := range s {
		switch {
		case r == '_', r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z':
		case i > 0 && r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}

// factTree nests facts by the dots in their names. Intermediate objects
// are always present; a leaf is required unless the fact is optional.
func factTree(facts []Fact) (*node, error) {
	root := &node{kind: kindObject, fields: map[string]*field{}}
	sorted := slices.SortedFunc(slices.Values(facts), func(a, b Fact) int { return strings.Compare(a.Name, b.Name) })
	for _, f := range sorted {
		k, ok := f.Type.kind()
		if !ok {
			return nil, fmt.Errorf("expr: fact %q has invalid type %d", f.Name, f.Type)
		}
		parts := strings.Split(f.Name, ".")
		cur := root
		for i, part := range parts {
			if part == "" {
				return nil, fmt.Errorf("expr: fact %q has an empty name segment", f.Name)
			}
			existing, ok := cur.fields[part]
			last := i == len(parts)-1
			switch {
			case last && ok:
				return nil, fmt.Errorf("expr: fact %q clashes with another fact of the same path", f.Name)
			case last:
				cur.fields[part] = &field{node: &node{kind: k}, required: !f.Optional}
			case ok && existing.node.kind != kindObject:
				return nil, fmt.Errorf("expr: fact %q nests under fact %q", f.Name, strings.Join(parts[:i+1], "."))
			case ok:
				cur = existing.node
			default:
				next := &node{kind: kindObject, fields: map[string]*field{}}
				cur.fields[part] = &field{node: next, required: true}
				cur = next
			}
		}
	}
	return root, nil
}

// Scope is the run scope as expressions see it.
type Scope struct {
	EntityKind string
	EntityID   string
	Extra      map[string]string
}

// Vars are the values of the declared variables for one evaluation.
type Vars struct {
	// Event is the event payload as returned by Activation.
	Event map[string]any
	// Facts are the fact values as returned by Env.FactValues; ignored by
	// an environment without facts.
	Facts map[string]any
	// Now is frozen for one pump of a run.
	Now   time.Time
	Scope Scope
}

func (v Vars) activation() map[string]any {
	extra := v.Scope.Extra
	if extra == nil {
		extra = map[string]string{}
	}
	event := v.Event
	if event == nil {
		event = map[string]any{}
	}
	facts := v.Facts
	if facts == nil {
		facts = map[string]any{}
	}
	return map[string]any{
		VarEvent: event,
		VarFacts: facts,
		VarNow:   v.Now,
		VarScope: map[string]any{
			"entity_kind": v.Scope.EntityKind,
			"entity_id":   v.Scope.EntityID,
			"extra":       extra,
		},
	}
}

// FactValues builds the facts variable from flat fact values keyed by full
// fact name. Each declared fact present in values is coerced to its type;
// undeclared names are ignored. Intermediate objects of dotted names are
// always created.
func (e *Env) FactValues(values map[string]any) (map[string]any, error) {
	out := map[string]any{}
	for _, name := range slices.Sorted(maps.Keys(e.facts)) {
		parts := strings.Split(name, ".")
		cur := out
		for _, part := range parts[:len(parts)-1] {
			next, ok := cur[part].(map[string]any)
			if !ok {
				next = map[string]any{}
				cur[part] = next
			}
			cur = next
		}
		v, ok := values[name]
		if !ok {
			continue
		}
		k, _ := e.facts[name].kind()
		c, err := coerceScalar(v, k)
		if err != nil {
			return nil, fmt.Errorf("fact %q: %w", name, err)
		}
		cur[parts[len(parts)-1]] = c
	}
	return out, nil
}
