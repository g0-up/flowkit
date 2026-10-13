package interpreter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/g0-up/flowkit/internal/executor"
	"github.com/g0-up/flowkit/internal/expr"
	"github.com/g0-up/flowkit/internal/interpreter/model"
	"github.com/g0-up/flowkit/internal/sched"
	"github.com/g0-up/flowkit/internal/store"
)

// Planner compiles a definition body against the registry. A body that no
// longer compiles, for example because an action it calls was removed from
// the registry, returns an error, and the runs pinned to it fail.
type Planner interface {
	Plan(body json.RawMessage) (*Plan, error)
}

// Plan is a compiled definition: what a pump needs to run its steps.
type Plan struct {
	Def *model.Definition
	// TriggerShape types the trigger payload, the event variable of step
	// expressions.
	TriggerShape *expr.Shape
	// Steps is indexed like Def.Steps.
	Steps []StepPlan
	// Facts are the registered facts the definition's expressions read,
	// resolved at the start of every pump that walks steps.
	Facts []FactSource
}

// StepPlan is the compiled form of one step. Fields that do not apply to
// the step's type are nil.
type StepPlan struct {
	// Env is the environment of the step's expressions; it types the
	// facts variable.
	Env *expr.Env
	// When is the condition of an action; nil runs it unconditionally.
	When *expr.Program
	// Cases are the conditions of a switch, in order.
	Cases []*expr.Program
	// Params are the expression placeholders of an action's params, by
	// JSON Pointer relative to the params object.
	Params map[string]*expr.Program
	Action *ActionPlan
	// Match is the condition of a wait on the waited payload; nil accepts
	// every event of the type.
	Match *expr.Program
	// MatchShape types the waited payload.
	MatchShape *expr.Shape
}

// ActionPlan is the registered action a step calls.
type ActionPlan struct {
	Action executor.Action
	// Validate checks resolved params, decoded by schema.Decode, against
	// the full ParamsSchema.
	Validate func(doc any) error
	// ParamsShape types the resolved params for Execute: integer values
	// become int64, numbers float64, date-time strings time.Time. A shape
	// of the empty schema leaves every value untyped.
	ParamsShape *expr.Shape
}

// FactSource resolves one registered fact for a run.
type FactSource struct {
	Name string
	// Resolve returns the fact's value, or nil when it has none. q reads in
	// a read-only transaction bounded by the fact timeout.
	Resolve func(ctx context.Context, q store.ReadQuerier, s expr.Scope) (any, error)
}

// step returns the step with id and its plan.
func (p *Plan) step(id string) (*model.Step, *StepPlan, error) {
	s, ok := p.Def.Step(id)
	if !ok {
		return nil, nil, fmt.Errorf("step %q does not exist in the definition", id)
	}
	if s.Index < 0 || s.Index >= len(p.Steps) {
		return nil, nil, fmt.Errorf("step %q has no plan", id)
	}
	return s, &p.Steps[s.Index], nil
}

type planKey struct {
	name    string
	version int
}

// errPlan marks a run whose pinned definition this instance cannot compile,
// or in which the run's position is not a step of the expected type. The
// run cannot go on here: planFailed ends it.
var errPlan = errors.New("flowkit interpreter: the run's definition does not compile on this instance")

// plan returns the compiled definition of run, compiling it once per
// definition version. Errors compiling it wrap errPlan. run must carry its
// pinned definition unless planned reports the version compiled.
func (in *Interpreter) plan(run store.RunRow) (*Plan, error) {
	key := planKey{name: run.DefName, version: run.DefVersion}
	in.plansMu.Lock()
	p, ok := in.plans[key]
	in.plansMu.Unlock()
	if ok {
		return p, nil
	}
	if run.Definition == nil {
		// A caller bug, not a property of the definition: retried, never
		// failing the run.
		return nil, fmt.Errorf("flowkit interpreter: run %s read without its definition", run.ID)
	}
	p, err := in.planner.Plan(run.Definition)
	if err != nil {
		return nil, fmt.Errorf("%w: %s version %d: %w", errPlan, run.DefName, run.DefVersion, err)
	}
	if err := p.complete(); err != nil {
		return nil, fmt.Errorf("%w: %s version %d: %w", errPlan, run.DefName, run.DefVersion, err)
	}
	in.plansMu.Lock()
	in.plans[key] = p
	in.plansMu.Unlock()
	return p, nil
}

// planned reports whether the definition version of run is compiled.
func (in *Interpreter) planned(run store.RunRow) bool {
	in.plansMu.Lock()
	defer in.plansMu.Unlock()
	_, ok := in.plans[planKey{name: run.DefName, version: run.DefVersion}]
	return ok
}

// planFailed ends run, as read, failed with TerminalFailed because of err,
// an errPlan. Every path that compiles a run's definition, the pump, the
// look-back, the timeout and the route of an event to a wait, ends the run
// this way: an instance that misses a component the definition uses fails
// the runs it handles, so components must be deployed to every instance
// before a definition using them is published.
func (in *Interpreter) planFailed(ctx context.Context, run store.RunRow, err error) sched.Result {
	in.log.ErrorContext(ctx, "flowkit: run's definition does not compile on this instance; is every component it uses deployed here?",
		"run_id", run.ID.String(), "def_name", run.DefName, "def_version", run.DefVersion, "error", in.redact(err))
	return in.failRun(ctx, &turn{run: run}, StepRun, TerminalFailed, in.redact(err), nil)
}

// complete checks that the planner filled in what the pump relies on.
func (p *Plan) complete() error {
	if p == nil || p.Def == nil || p.TriggerShape == nil || len(p.Steps) != len(p.Def.Steps) {
		return errors.New("the planner returned an incomplete plan")
	}
	for i, s := range p.Def.Steps {
		sp := &p.Steps[i]
		var ok bool
		switch s.Type {
		case model.StepAction:
			ok = sp.Env != nil && sp.Action != nil && sp.Action.Action.Execute != nil &&
				sp.Action.Validate != nil && sp.Action.ParamsShape != nil
		case model.StepSwitch:
			ok = sp.Env != nil && len(sp.Cases) == len(s.Switch.Cases)
		case model.StepWait:
			ok = sp.Match == nil || sp.MatchShape != nil
		default:
			ok = true
		}
		if !ok {
			return fmt.Errorf("the planner returned an incomplete plan of step %q", s.ID)
		}
	}
	return nil
}
