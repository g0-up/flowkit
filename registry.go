package flowkit

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/g0-up/flowkit/internal/expr"
	"github.com/g0-up/flowkit/internal/schema"
)

// Registry holds the events, actions and facts an Engine knows. Register
// everything before calling New: New takes a copy, so later registrations do
// not reach an existing Engine. A Registry is not safe for concurrent use.
type Registry struct {
	events  map[string]EventDef
	actions map[string]ActionDef
	facts   map[string]FactDef
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		events:  map[string]EventDef{},
		actions: map[string]ActionDef{},
		facts:   map[string]FactDef{},
	}
}

// MustAddEvent registers an event type. It panics when Type or
// PayloadSchema is missing, PayloadSchema is not JSON, the type is already
// registered, EntityKind and EntityIDField are not both set or both empty,
// or DedupKeyField, EntityIDField or a ScopeFields value is not a top-level
// property declared with type "string" or "integer" in PayloadSchema.
func (r *Registry) MustAddEvent(d EventDef) {
	const op = "MustAddEvent"
	if d.Type == "" {
		panicf(op, "Type is required")
	}
	if _, dup := r.events[d.Type]; dup {
		panicf(op, "event %q is already registered", d.Type)
	}
	mustJSON(op, "event "+d.Type+": PayloadSchema", d.PayloadSchema)
	if d.EntityKind != "" && d.EntityIDField == "" {
		panicf(op, "event %q: EntityIDField is required when EntityKind is set", d.Type)
	}
	if d.EntityKind == "" && d.EntityIDField != "" {
		panicf(op, "event %q: EntityKind is required when EntityIDField is set", d.Type)
	}
	if d.DedupKeyField != "" {
		mustScalarProperty(op, d, "DedupKeyField", d.DedupKeyField)
	}
	if d.EntityIDField != "" {
		mustScalarProperty(op, d, "EntityIDField", d.EntityIDField)
	}
	for _, key := range slices.Sorted(maps.Keys(d.ScopeFields)) {
		if key == "" {
			panicf(op, "event %q: ScopeFields has an empty key", d.Type)
		}
		mustScalarProperty(op, d, "ScopeFields["+key+"]", d.ScopeFields[key])
	}
	r.events[d.Type] = cloneEvent(d)
}

// MustAddAction registers an action. It panics when Name, ParamsSchema or
// Execute is missing, ParamsSchema is not JSON, the name is already
// registered, a RetryPolicy field is negative, or an OutputFact has no name,
// a name that is not a valid fact name (see FactDef.Name), an invalid type,
// or a name used twice.
func (r *Registry) MustAddAction(a ActionDef) {
	const op = "MustAddAction"
	if a.Name == "" {
		panicf(op, "Name is required")
	}
	if _, dup := r.actions[a.Name]; dup {
		panicf(op, "action %q is already registered", a.Name)
	}
	mustJSON(op, "action "+a.Name+": ParamsSchema", a.ParamsSchema)
	if a.Execute == nil {
		panicf(op, "action %q: Execute is required", a.Name)
	}
	if a.Retry.MaxAttempts < 0 || a.Retry.BaseBackoff < 0 || a.Retry.MaxBackoff < 0 {
		panicf(op, "action %q: RetryPolicy fields must not be negative", a.Name)
	}
	seen := map[string]bool{}
	for _, f := range a.OutputFacts {
		switch {
		case f.Name == "":
			panicf(op, "action %q: OutputFact Name is required", a.Name)
		case !expr.ValidFactName(f.Name):
			panicf(op, "action %q: OutputFact %q is not a valid fact name; use dot-separated identifiers", a.Name, f.Name)
		case !f.Type.valid():
			panicf(op, "action %q: OutputFact %q has invalid Type %d", a.Name, f.Name, f.Type)
		case seen[f.Name]:
			panicf(op, "action %q: OutputFact %q is declared twice", a.Name, f.Name)
		}
		seen[f.Name] = true
	}
	r.actions[a.Name] = cloneAction(a)
}

// MustAddFact registers a fact. It panics when Name, a valid Type,
// AvailableOn or Resolve is missing, Name is not a valid fact name, an
// AvailableOn pattern is empty, or the name is already registered. That
// every pattern matches a registered event, and that no fact name nests
// under another, is checked by New.
func (r *Registry) MustAddFact(d FactDef) {
	const op = "MustAddFact"
	if d.Name == "" {
		panicf(op, "Name is required")
	}
	if !expr.ValidFactName(d.Name) {
		panicf(op, "fact %q is not a valid fact name; use dot-separated identifiers", d.Name)
	}
	if _, dup := r.facts[d.Name]; dup {
		panicf(op, "fact %q is already registered", d.Name)
	}
	if !d.Type.valid() {
		panicf(op, "fact %q: invalid Type %d", d.Name, d.Type)
	}
	if len(d.AvailableOn) == 0 {
		panicf(op, "fact %q: AvailableOn is required", d.Name)
	}
	if slices.Contains(d.AvailableOn, "") {
		panicf(op, "fact %q: AvailableOn has an empty pattern", d.Name)
	}
	if d.Resolve == nil {
		panicf(op, "fact %q: Resolve is required", d.Name)
	}
	d.AvailableOn = slices.Clone(d.AvailableOn)
	r.facts[d.Name] = d
}

func panicf(op, format string, args ...any) {
	panic(fmt.Sprintf("flowkit: %s: %s", op, fmt.Sprintf(format, args...)))
}

func mustJSON(op, what string, doc json.RawMessage) {
	if len(bytes.TrimSpace(doc)) == 0 {
		panicf(op, "%s is required", what)
	}
	if !json.Valid(doc) {
		panicf(op, "%s is not valid JSON", what)
	}
}

// mustScalarProperty panics unless field is a top-level string or integer
// property of d's payload schema; its value becomes a key or Scope string.
func mustScalarProperty(op string, d EventDef, what, field string) {
	switch schema.TopLevelType(d.PayloadSchema, field) {
	case "string", "integer":
	default:
		panicf(op, "event %q: %s %q must be a top-level property of type \"string\" or \"integer\" in PayloadSchema",
			d.Type, what, field)
	}
}

func cloneEvent(d EventDef) EventDef {
	d.PayloadSchema = bytes.Clone(d.PayloadSchema)
	d.ScopeFields = maps.Clone(d.ScopeFields)
	return d
}

func cloneAction(a ActionDef) ActionDef {
	a.ParamsSchema = bytes.Clone(a.ParamsSchema)
	a.OutputFacts = slices.Clone(a.OutputFacts)
	return a
}

// clone returns a deep copy, so an Engine is unaffected by registrations
// made after New.
func (r *Registry) clone() *Registry {
	c := NewRegistry()
	for k, v := range r.events {
		c.events[k] = cloneEvent(v)
	}
	for k, v := range r.actions {
		c.actions[k] = cloneAction(v)
	}
	for k, v := range r.facts {
		v.AvailableOn = slices.Clone(v.AvailableOn)
		c.facts[k] = v
	}
	return c
}

// compiledRegistry is a validated registry snapshot with its schemas
// compiled.
type compiledRegistry struct {
	reg      *Registry
	payloads map[string]*schema.Compiled // by event type
	params   map[string]*schema.Compiled // by action name
}

// compile cross-validates r and compiles its schemas. It reports every
// problem at once.
func (r *Registry) compile() (*compiledRegistry, error) {
	c := &compiledRegistry{
		reg:      r,
		payloads: map[string]*schema.Compiled{},
		params:   map[string]*schema.Compiled{},
	}
	var errs []error
	for _, name := range slices.Sorted(maps.Keys(r.events)) {
		s, err := schema.Compile(r.events[name].PayloadSchema)
		if err != nil {
			errs = append(errs, fmt.Errorf("event %q: PayloadSchema: %w", name, err))
			continue
		}
		c.payloads[name] = s
	}
	outputTypes := map[string]FactType{}
	outputOwner := map[string]string{}
	for _, name := range slices.Sorted(maps.Keys(r.actions)) {
		a := r.actions[name]
		s, err := schema.Compile(a.ParamsSchema)
		if err != nil {
			errs = append(errs, fmt.Errorf("action %q: ParamsSchema: %w", name, err))
		} else {
			c.params[name] = s
		}
		for _, f := range a.OutputFacts {
			if _, clash := r.facts[f.Name]; clash {
				errs = append(errs, fmt.Errorf("action %q: OutputFact %q has the name of a registered fact", name, f.Name))
			}
			if t, ok := outputTypes[f.Name]; ok && t != f.Type {
				errs = append(errs, fmt.Errorf("action %q: OutputFact %q has type %d, but action %q declares it with type %d",
					name, f.Name, f.Type, outputOwner[f.Name], t))
			} else if !ok {
				outputTypes[f.Name], outputOwner[f.Name] = f.Type, name
			}
		}
	}
	errs = append(errs, nestedFactNames(r.facts, outputOwner)...)
	for _, name := range slices.Sorted(maps.Keys(r.facts)) {
		for _, p := range r.facts[name].AvailableOn {
			if !r.patternMatchesAny(p) {
				errs = append(errs, fmt.Errorf("fact %q: AvailableOn pattern %q matches no registered event", name, p))
			}
		}
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("flowkit: invalid registry: %w", errors.Join(errs...))
	}
	return c, nil
}

// nestedFactNames reports every fact or output fact name that runs through
// another one, as "customer" and "customer.tier" would: expressions read
// facts as nested fields, so facts.customer cannot be both a value and an
// object.
func nestedFactNames(facts map[string]FactDef, outputOwner map[string]string) []error {
	names := map[string]string{} // name to how it is declared
	for name := range facts {
		names[name] = fmt.Sprintf("fact %q", name)
	}
	for name, action := range outputOwner {
		if _, ok := names[name]; !ok {
			names[name] = fmt.Sprintf("OutputFact %q of action %q", name, action)
		}
	}
	var errs []error
	for _, name := range slices.Sorted(maps.Keys(names)) {
		for i := range len(name) {
			if name[i] != '.' {
				continue
			}
			if _, ok := names[name[:i]]; ok {
				errs = append(errs, fmt.Errorf("%s nests under %s", names[name], names[name[:i]]))
				break
			}
		}
	}
	return errs
}

func (r *Registry) patternMatchesAny(pattern string) bool {
	for t := range r.events {
		if matchEventPattern(pattern, t) {
			return true
		}
	}
	return false
}

// matchEventPattern reports whether eventType matches pattern: "*" matches
// every type, "prefix.*" every type that starts with "prefix.", and any other
// pattern only the identical type.
func matchEventPattern(pattern, eventType string) bool {
	if pattern == "*" {
		return true
	}
	if prefix, ok := strings.CutSuffix(pattern, "*"); ok && strings.HasSuffix(prefix, ".") {
		return strings.HasPrefix(eventType, prefix) && len(eventType) > len(prefix)
	}
	return pattern == eventType
}
