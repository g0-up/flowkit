package expr

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"

	"github.com/g0-up/flowkit/internal/schema"
)

// kind is how expressions see a JSON value.
type kind int

const (
	kindDyn kind = iota
	kindBool
	kindInt
	kindDouble
	kindString
	kindTimestamp
	kindObject
	kindMap
	kindList
)

// node is a JSON Schema converted to expression types. Nodes form a DAG:
// a $ref target is converted once and shared.
type node struct {
	kind   kind
	fields map[string]*field // kindObject
	elem   *node             // kindList items, kindMap values
	// max is the schema's maxLength, maxItems or maxProperties; 0 when
	// the schema sets no bound.
	max uint64
}

type field struct {
	node     *node
	required bool
}

var dynNode = &node{kind: kindDyn}

// Shape is a JSON Schema converted for expressions: it types a variable at
// compile time and coerces JSON values at run time. A Shape is immutable and
// safe for concurrent use.
type Shape struct {
	root *node
	doc  any // the decoded schema
}

// NewShape converts a JSON Schema document. The mapping:
//
//   - "string" is a string, or a timestamp with "format": "date-time";
//   - "integer" is int, "number" is double, "boolean" is bool;
//   - "object" with "properties" is an object type whose fields are checked
//     at compile time; "object" without "properties" is map(string, T),
//     T from "additionalProperties" (dyn when absent);
//   - "array" is list(T), T from "items";
//   - anything else ("oneOf", "anyOf", a list of types, no "type", a
//     recursive "$ref") is dyn, which compiles but is checked only at run
//     time.
//
// Internal "$ref"s ("#/...") are followed.
func NewShape(doc json.RawMessage) (*Shape, error) {
	v, err := schema.Decode(doc)
	if err != nil {
		return nil, fmt.Errorf("schema is not valid JSON: %w", err)
	}
	c := &converter{root: v, refs: map[string]*node{}, active: map[string]bool{}}
	n, err := c.convert(v)
	if err != nil {
		return nil, err
	}
	return &Shape{root: n, doc: v}, nil
}

type converter struct {
	root   any
	refs   map[string]*node
	active map[string]bool
}

func (c *converter) convert(s any) (*node, error) {
	m, ok := s.(map[string]any)
	if !ok {
		// true, false or something malformed: nothing to type.
		return dynNode, nil
	}
	if ref, ok := m["$ref"].(string); ok {
		return c.ref(ref)
	}
	t, _ := m["type"].(string)
	switch t {
	case "boolean":
		return &node{kind: kindBool}, nil
	case "integer":
		return &node{kind: kindInt}, nil
	case "number":
		return &node{kind: kindDouble}, nil
	case "string":
		if f, _ := m["format"].(string); f == "date-time" {
			return &node{kind: kindTimestamp}, nil
		}
		return &node{kind: kindString, max: bound(m, "maxLength")}, nil
	case "array":
		n := &node{kind: kindList, elem: dynNode, max: bound(m, "maxItems")}
		if items, ok := m["items"]; ok {
			elem, err := c.convert(items)
			if err != nil {
				return nil, err
			}
			n.elem = elem
		}
		return n, nil
	case "object":
		props, hasProps := m["properties"].(map[string]any)
		if !hasProps {
			n := &node{kind: kindMap, elem: dynNode, max: bound(m, "maxProperties")}
			if ap, ok := m["additionalProperties"].(map[string]any); ok {
				elem, err := c.convert(ap)
				if err != nil {
					return nil, err
				}
				n.elem = elem
			}
			return n, nil
		}
		required := map[string]bool{}
		if rs, ok := m["required"].([]any); ok {
			for _, r := range rs {
				if name, ok := r.(string); ok {
					required[name] = true
				}
			}
		}
		n := &node{kind: kindObject, fields: make(map[string]*field, len(props))}
		for _, name := range slices.Sorted(maps.Keys(props)) {
			fn, err := c.convert(props[name])
			if err != nil {
				return nil, err
			}
			n.fields[name] = &field{node: fn, required: required[name]}
		}
		return n, nil
	}
	return dynNode, nil
}

// ref follows an internal reference. A reference cycle types as dyn.
func (c *converter) ref(ref string) (*node, error) {
	if n, ok := c.refs[ref]; ok {
		return n, nil
	}
	if c.active[ref] {
		return dynNode, nil
	}
	target, err := resolvePointer(c.root, ref)
	if err != nil {
		return nil, err
	}
	c.active[ref] = true
	n, err := c.convert(target)
	delete(c.active, ref)
	if err != nil {
		return nil, err
	}
	c.refs[ref] = n
	return n, nil
}

// resolvePointer resolves "#" or "#/a/b" against doc.
func resolvePointer(doc any, ref string) (any, error) {
	p, ok := strings.CutPrefix(ref, "#")
	if !ok {
		return nil, fmt.Errorf("$ref %q: only references inside the schema are supported", ref)
	}
	if p == "" {
		return doc, nil
	}
	if !strings.HasPrefix(p, "/") {
		return nil, fmt.Errorf("$ref %q: anchors are not supported", ref)
	}
	cur := doc
	for _, tok := range strings.Split(p[1:], "/") {
		tok = strings.ReplaceAll(strings.ReplaceAll(tok, "~1", "/"), "~0", "~")
		switch x := cur.(type) {
		case map[string]any:
			v, ok := x[tok]
			if !ok {
				return nil, fmt.Errorf("$ref %q does not resolve", ref)
			}
			cur = v
		case []any:
			i, err := strconv.Atoi(tok)
			if err != nil || i < 0 || i >= len(x) {
				return nil, fmt.Errorf("$ref %q does not resolve", ref)
			}
			cur = x[i]
		default:
			return nil, fmt.Errorf("$ref %q does not resolve", ref)
		}
	}
	return cur, nil
}

// bound reads a non-negative integer keyword, 0 when absent or invalid.
func bound(m map[string]any, keyword string) uint64 {
	n, ok := m[keyword].(json.Number)
	if !ok {
		return 0
	}
	v, err := strconv.ParseUint(n.String(), 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// at returns the node a JSON Pointer inside a value of this shape leads to,
// or nil when the shape does not describe it.
func (s *Shape) at(pointer string) *node {
	n := s.root
	if pointer == "" {
		return n
	}
	for _, tok := range strings.Split(pointer[1:], "/") {
		tok = strings.ReplaceAll(strings.ReplaceAll(tok, "~1", "/"), "~0", "~")
		switch n.kind {
		case kindObject:
			f, ok := n.fields[tok]
			if !ok {
				return nil
			}
			n = f.node
		case kindMap, kindList:
			n = n.elem
		default:
			return nil
		}
	}
	return n
}

// provider types the expression variables for the checker. Object types
// get names that are not CEL identifiers, so no expression can name or
// construct them; values stay map[string]any at run time.
type provider struct {
	*types.Registry
	objects map[string]*object
	names   map[*node]string
}

type object struct {
	node   *node
	fields map[string]*types.Type
	sorted []string
}

func newProvider() (*provider, error) {
	r, err := types.NewRegistry()
	if err != nil {
		return nil, err
	}
	return &provider{Registry: r, objects: map[string]*object{}, names: map[*node]string{}}, nil
}

// typeOf returns the CEL type of n, registering object types under name
// (and names derived from it for nested objects).
func (p *provider) typeOf(n *node, name string) *types.Type {
	switch n.kind {
	case kindBool:
		return types.BoolType
	case kindInt:
		return types.IntType
	case kindDouble:
		return types.DoubleType
	case kindString:
		return types.StringType
	case kindTimestamp:
		return types.TimestampType
	case kindList:
		return types.NewListType(p.typeOf(n.elem, name+"[]"))
	case kindMap:
		return types.NewMapType(types.StringType, p.typeOf(n.elem, name+"{}"))
	case kindObject:
		if existing, ok := p.names[n]; ok {
			return types.NewObjectType(existing)
		}
		unique := name
		for i := 2; p.objects[unique] != nil; i++ {
			unique = name + "#" + strconv.Itoa(i)
		}
		o := &object{node: n, fields: make(map[string]*types.Type, len(n.fields)), sorted: slices.Sorted(maps.Keys(n.fields))}
		p.names[n] = unique
		p.objects[unique] = o
		for _, f := range o.sorted {
			o.fields[f] = p.typeOf(n.fields[f].node, unique+"."+f)
		}
		return types.NewObjectType(unique)
	}
	return types.DynType
}

// object returns the object type behind t, if t is one of ours.
func (p *provider) object(t *types.Type) (*object, bool) {
	if t == nil || t.Kind() != types.StructKind {
		return nil, false
	}
	o, ok := p.objects[t.TypeName()]
	return o, ok
}

// FindStructType implements types.Provider.
func (p *provider) FindStructType(name string) (*types.Type, bool) {
	if _, ok := p.objects[name]; ok {
		return types.NewTypeTypeWithParam(types.NewObjectType(name)), true
	}
	return p.Registry.FindStructType(name)
}

// FindStructFieldNames implements types.Provider.
func (p *provider) FindStructFieldNames(name string) ([]string, bool) {
	if o, ok := p.objects[name]; ok {
		return slices.Clone(o.sorted), true
	}
	return p.Registry.FindStructFieldNames(name)
}

// FindStructFieldType implements types.Provider. The field type carries no
// accessors, so the interpreter reads fields as map keys.
func (p *provider) FindStructFieldType(name, fieldName string) (*types.FieldType, bool) {
	if o, ok := p.objects[name]; ok {
		t, ok := o.fields[fieldName]
		if !ok {
			return nil, false
		}
		return &types.FieldType{Type: t}, true
	}
	return p.Registry.FindStructFieldType(name, fieldName)
}

var errConstruct = errors.New("object types of definition variables cannot be constructed")

// NewValue implements types.Provider.
func (p *provider) NewValue(name string, fields map[string]ref.Val) ref.Val {
	if _, ok := p.objects[name]; ok {
		return types.NewErrFromString(errConstruct.Error())
	}
	return p.Registry.NewValue(name, fields)
}
