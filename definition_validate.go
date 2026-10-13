package flowkit

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/g0-up/flowkit/internal/expr"
	"github.com/g0-up/flowkit/internal/interpreter/model"
	"github.com/g0-up/flowkit/internal/schema"
)

// Component kinds of a ComponentRef.
const (
	ComponentEvent  = "event"
	ComponentAction = "action"
	ComponentFact   = "fact"
)

// ValidateDefinition checks a definition body against r without a database:
// the metamodel schema, step ids and edges, that every path ends, that the
// trigger and waited events and the actions are registered, that literal
// action params satisfy the ParamsSchema, and that every expression
// compiles against the payload, fact and param types with has() guards for
// values that may be absent and within the cost limit. It returns nil or
// ValidationErrors listing every problem, each with a JSON Pointer into the
// body. A registry that New would reject returns that error instead.
//
// Use it in tests and CI with the Registry the application builds.
func (r *Registry) ValidateDefinition(body json.RawMessage) error {
	c, err := r.compile()
	if err != nil {
		return err
	}
	if _, verrs := c.validateDefinition(body); len(verrs) > 0 {
		return verrs
	}
	return nil
}

// validatedDefinition is a definition that passed validation, with what a
// publish stores besides the body and what the planner reuses.
type validatedDefinition struct {
	def   *model.Definition
	refs  []ComponentRef
	check *definitionCheck
}

// definitionCheck collects the problems of one validation.
type definitionCheck struct {
	reg    *compiledRegistry
	mm     expr.MetamodelOptions
	verrs  ValidationErrors
	shapes map[string]*expr.Shape // payload and params shapes by "event:"/"action:" name
	envs   map[string]*expr.Env
	facts  map[string]bool // registered facts the expressions read
	// progs are the compiled expressions by the JSON Pointer of their
	// source, and stepEnvs the environments of step expressions by step
	// index; the planner reuses them.
	progs    map[string]*expr.Program
	stepEnvs map[int]*expr.Env
}

func (c *compiledRegistry) validateDefinition(body json.RawMessage) (*validatedDefinition, ValidationErrors) {
	var verrs ValidationErrors
	if !utf8.Valid(body) || hasBadUnicodeEscape(body) {
		return nil, append(verrs, &ValidationError{Msg: "definition has a string or key that is not valid UTF-8"})
	}
	def, vs := model.Parse(body)
	for _, v := range vs {
		verrs = append(verrs, &ValidationError{Path: v.Path, Msg: v.Msg})
	}
	if def == nil {
		return nil, verrs
	}
	if doc, err := schema.Decode(body); err == nil {
		if path, found := findNUL(doc, nil); found {
			verrs = append(verrs, &ValidationError{Path: schema.Pointer(path...), Msg: "has a U+0000 character, which Postgres cannot store"})
		}
		if path, problem := findBadNumber(doc, nil); problem != "" {
			verrs = append(verrs, &ValidationError{Path: schema.Pointer(path...), Msg: "number " + problem})
		}
	}
	mm, err := expr.Metamodel(def.MetamodelVersion)
	if err != nil {
		return nil, append(verrs, &ValidationError{Path: "/metamodel_version", Msg: err.Error()})
	}
	dc := &definitionCheck{
		reg:      c,
		mm:       mm,
		verrs:    verrs,
		shapes:   map[string]*expr.Shape{},
		envs:     map[string]*expr.Env{},
		facts:    map[string]bool{},
		progs:    map[string]*expr.Program{},
		stepEnvs: map[int]*expr.Env{},
	}
	dc.check(def)
	if len(dc.verrs) > 0 {
		sortValidationErrors(dc.verrs)
		return nil, dc.verrs
	}
	return &validatedDefinition{def: def, refs: dc.refs(def), check: dc}, nil
}

func (dc *definitionCheck) fail(path, format string, args ...any) {
	dc.verrs = append(dc.verrs, &ValidationError{Path: path, Msg: fmt.Sprintf(format, args...)})
}

func (dc *definitionCheck) check(def *model.Definition) {
	trigger := def.Trigger.Event
	triggerShape := dc.eventShape("/trigger/event", trigger)
	if triggerShape != nil && def.Concurrency.SerialKey != "" {
		if env := dc.eventEnv("/concurrency/serial_key", trigger, triggerShape); env != nil {
			dc.compile(env, "/concurrency/serial_key", def.Concurrency.SerialKey, expr.WantString)
		}
	}
	flows := def.Flows(func(s *model.Step) (certain, possible []string) {
		if s.Action == nil {
			return nil, nil
		}
		a, ok := dc.reg.reg.actions[s.Action.Action]
		if !ok {
			return nil, nil
		}
		names := make([]string, len(a.OutputFacts))
		for i, f := range a.OutputFacts {
			names[i] = f.Name
		}
		if s.Action.When != "" {
			return nil, names
		}
		return names, nil
	})
	for _, s := range def.Steps {
		var env *expr.Env
		if triggerShape != nil && flows[s.Index] != nil {
			env = dc.stepEnv(s.Path(), trigger, triggerShape, flows[s.Index])
			dc.stepEnvs[s.Index] = env
		}
		switch s.Type {
		case model.StepAction:
			dc.checkAction(s, env)
		case model.StepWait:
			dc.checkWait(s)
		case model.StepSwitch:
			if env == nil {
				continue
			}
			for i, cs := range s.Switch.Cases {
				dc.compile(env, fmt.Sprintf("%s/cases/%d/when", s.Path(), i), cs.When, expr.WantBool)
			}
		}
	}
}

func (dc *definitionCheck) checkAction(s *model.Step, env *expr.Env) {
	a := s.Action
	paramsPath := s.Path() + "/params"
	if _, ok := dc.reg.reg.actions[a.Action]; !ok {
		dc.fail(s.Path()+"/action", "action %q is not registered", a.Action)
		return
	}
	if env != nil && a.When != "" {
		dc.compile(env, s.Path()+"/when", a.When, expr.WantBool)
	}
	placeholders, err := model.ParamExpressions(a.Params)
	if err != nil {
		// Parse already reported malformed params.
		return
	}
	var shape *expr.Shape
	if len(placeholders) > 0 {
		shape = dc.shape("action:"+a.Action, dc.reg.reg.actions[a.Action].ParamsSchema, paramsPath)
	}
	// Literal params must satisfy the schema; a placeholder is checked by
	// compiling its expression against the type expected where it sits.
	// Below a oneOf, anyOf, not or if above a placeholder, which literals
	// are valid can depend on the placeholder's value, so they are left to
	// validation of the resolved params.
	unchecked := make([]string, 0, len(placeholders))
	for _, p := range placeholders {
		branch, ok := "", false
		if shape != nil {
			branch, ok = shape.Branch(p.Pointer)
		}
		if !ok {
			branch = p.Pointer
		}
		unchecked = append(unchecked, branch)
	}
	params := a.Params
	if len(params) == 0 {
		params = json.RawMessage(`{}`)
	}
	doc, err := schema.Decode(params)
	if err != nil {
		return
	}
	for _, v := range dc.reg.params[a.Action].Validate(doc) {
		if !slices.ContainsFunc(unchecked, func(p string) bool { return under(v.Path, p) }) {
			dc.fail(paramsPath+v.Path, "%s", v.Msg)
		}
	}
	if env == nil || shape == nil {
		return
	}
	for _, p := range placeholders {
		dc.compile(env, paramsPath+p.Pointer+"/"+model.ExprKey, p.Source, shape.Want(p.Pointer))
	}
}

func (dc *definitionCheck) checkWait(s *model.Step) {
	w := s.Wait
	shape := dc.eventShape(s.Path()+"/event", w.Event)
	if shape == nil || w.Match == "" {
		return
	}
	if env := dc.eventEnv(s.Path()+"/match", w.Event, shape); env != nil {
		dc.compile(env, s.Path()+"/match", w.Match, expr.WantBool)
	}
}

// under reports whether path is at or below the JSON Pointer prefix.
func under(path, prefix string) bool {
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}

func (dc *definitionCheck) compile(env *expr.Env, path, src string, want expr.Want) {
	p, errs := env.Compile(src, want)
	for _, err := range errs {
		dc.fail(path, "%v", err)
	}
	if p == nil {
		return
	}
	dc.progs[path] = p
	for _, name := range p.Refs() {
		if _, ok := dc.reg.reg.facts[name]; ok {
			dc.facts[name] = true
		}
	}
}

// eventShape returns the payload shape of a registered event, or reports
// at path that it is not registered.
func (dc *definitionCheck) eventShape(path, eventType string) *expr.Shape {
	def, ok := dc.reg.reg.events[eventType]
	if !ok {
		dc.fail(path, "event %q is not registered", eventType)
		return nil
	}
	return dc.shape("event:"+eventType, def.PayloadSchema, path)
}

func (dc *definitionCheck) shape(key string, doc json.RawMessage, path string) *expr.Shape {
	if s, ok := dc.shapes[key]; ok {
		return s
	}
	s, err := expr.NewShape(doc)
	if err != nil {
		dc.fail(path, "%s schema cannot type expressions: %v", key, err)
	}
	dc.shapes[key] = s
	return s
}

func (dc *definitionCheck) eventEnv(path, eventType string, shape *expr.Shape) *expr.Env {
	return dc.env(path, "event:"+eventType, func() (*expr.Env, error) { return expr.NewEventEnv(shape, dc.mm) })
}

// stepEnv returns the environment of a step's expressions: the trigger
// payload, the registered facts available on the trigger, and the output
// facts of the actions before the step, optional unless every path to the
// step produces them.
func (dc *definitionCheck) stepEnv(path, trigger string, shape *expr.Shape, flow *model.Flow) *expr.Env {
	var facts []expr.Fact
	for _, name := range slices.Sorted(maps.Keys(dc.reg.reg.facts)) {
		f := dc.reg.reg.facts[name]
		if slices.ContainsFunc(f.AvailableOn, func(p string) bool { return matchEventPattern(p, trigger) }) {
			facts = append(facts, expr.Fact{Name: name, Type: exprType(f.Type)})
		}
	}
	outTypes := dc.reg.outputTypes()
	for _, name := range flow.Certain {
		facts = append(facts, expr.Fact{Name: name, Type: exprType(outTypes[name])})
	}
	for _, name := range flow.Possible {
		facts = append(facts, expr.Fact{Name: name, Type: exprType(outTypes[name]), Optional: true})
	}
	var key strings.Builder
	key.WriteString("step:" + trigger)
	for _, f := range facts {
		fmt.Fprintf(&key, "|%s:%d:%t", f.Name, f.Type, f.Optional)
	}
	return dc.env(path, key.String(), func() (*expr.Env, error) { return expr.NewEnv(shape, facts, dc.mm) })
}

func (dc *definitionCheck) env(path, key string, build func() (*expr.Env, error)) *expr.Env {
	if env, ok := dc.envs[key]; ok {
		return env
	}
	env, err := build()
	if err != nil {
		dc.fail(path, "cannot type the expressions here: %v", err)
	}
	dc.envs[key] = env
	return env
}

// outputTypes maps every output fact name to its type; compile ensures
// actions agree on the type of a shared name.
func (c *compiledRegistry) outputTypes() map[string]FactType {
	out := map[string]FactType{}
	for _, a := range c.reg.actions {
		for _, f := range a.OutputFacts {
			out[f.Name] = f.Type
		}
	}
	return out
}

func exprType(t FactType) expr.Type {
	switch t {
	case FactBool:
		return expr.Bool
	case FactInt:
		return expr.Int
	case FactDouble:
		return expr.Double
	case FactString:
		return expr.String
	case FactTimestamp:
		return expr.Timestamp
	}
	return 0
}

// refs lists the components def uses: the trigger and waited events, the
// actions, and the registered facts its expressions read. Output facts are
// part of their action and not listed.
func (dc *definitionCheck) refs(def *model.Definition) []ComponentRef {
	seen := map[ComponentRef]bool{{Kind: ComponentEvent, Ref: def.Trigger.Event}: true}
	for _, s := range def.Steps {
		switch {
		case s.Action != nil:
			seen[ComponentRef{Kind: ComponentAction, Ref: s.Action.Action}] = true
		case s.Wait != nil:
			seen[ComponentRef{Kind: ComponentEvent, Ref: s.Wait.Event}] = true
		}
	}
	for name := range dc.facts {
		seen[ComponentRef{Kind: ComponentFact, Ref: name}] = true
	}
	return slices.SortedFunc(maps.Keys(seen), func(a, b ComponentRef) int {
		if c := strings.Compare(a.Kind, b.Kind); c != 0 {
			return c
		}
		return strings.Compare(a.Ref, b.Ref)
	})
}

func sortValidationErrors(es ValidationErrors) {
	slices.SortStableFunc(es, func(a, b *ValidationError) int { return strings.Compare(a.Path, b.Path) })
}
