package flowkit

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	"github.com/g0-up/flowkit/internal/executor"
	"github.com/g0-up/flowkit/internal/expr"
	"github.com/g0-up/flowkit/internal/interpreter"
	"github.com/g0-up/flowkit/internal/interpreter/model"
	"github.com/g0-up/flowkit/internal/store"
)

// planner compiles the published definition bodies runs are pinned to
// against the Engine's registry, for the interpreter.
type planner struct {
	reg     *compiledRegistry
	actions map[string]*interpreter.ActionPlan
}

func newPlanner(reg *compiledRegistry) (*planner, error) {
	untyped, err := expr.NewShape(json.RawMessage(`{}`))
	if err != nil {
		return nil, fmt.Errorf("flowkit: New: %w", err)
	}
	p := &planner{reg: reg, actions: make(map[string]*interpreter.ActionPlan, len(reg.reg.actions))}
	for name, a := range reg.reg.actions {
		// A params schema that cannot type expressions leaves the resolved
		// params as decoded from JSON.
		shape, err := expr.NewShape(a.ParamsSchema)
		if err != nil {
			shape = untyped
		}
		params := reg.params[name]
		p.actions[name] = &interpreter.ActionPlan{
			Action: executorAction(a),
			Validate: func(doc any) error {
				var verrs ValidationErrors
				for _, v := range params.Validate(doc) {
					verrs = append(verrs, &ValidationError{Path: v.Path, Msg: v.Msg})
				}
				if len(verrs) > 0 {
					return verrs
				}
				return nil
			},
			ParamsShape: shape,
		}
	}
	return p, nil
}

// executorAction adapts a registered action to the executor.
func executorAction(a ActionDef) executor.Action {
	out := make([]executor.OutputFact, len(a.OutputFacts))
	for i, f := range a.OutputFacts {
		out[i] = executor.OutputFact{Name: f.Name, Type: executor.FactType(f.Type)}
	}
	execute := a.Execute
	return executor.Action{
		Name:        a.Name,
		Idempotent:  a.Idempotent,
		Retry:       executor.RetryPolicy(a.Retry),
		OutputFacts: out,
		Execute: func(ctx context.Context, in executor.Input) (map[string]any, error) {
			res, err := execute(ctx, ActionInput{
				Params: in.Params, IdempotencyKey: in.IdemKey, RunID: in.RunID, StepID: in.StepID, Scope: Scope(in.Scope),
			})
			return res.Output, err
		},
	}
}

// Plan validates body against the registry, as a publish did, and returns
// its compiled steps. A body that no longer validates, because a component
// it uses changed or was removed, returns the validation errors.
func (p *planner) Plan(body json.RawMessage) (*interpreter.Plan, error) {
	v, verrs := p.reg.validateDefinition(body)
	if len(verrs) > 0 {
		return nil, fmt.Errorf("definition no longer validates against the registry: %w", verrs)
	}
	dc, def := v.check, v.def
	plan := &interpreter.Plan{
		Def:          def,
		TriggerShape: dc.shapes["event:"+def.Trigger.Event],
		Steps:        make([]interpreter.StepPlan, len(def.Steps)),
	}
	prog := func(path string) (*expr.Program, error) {
		if pr, ok := dc.progs[path]; ok {
			return pr, nil
		}
		return nil, fmt.Errorf("expression at %s was not compiled", path)
	}
	for _, s := range def.Steps {
		sp := &plan.Steps[s.Index]
		sp.Env = dc.stepEnvs[s.Index]
		var err error
		switch s.Type {
		case model.StepAction:
			if s.Action.When != "" {
				if sp.When, err = prog(s.Path() + "/when"); err != nil {
					return nil, err
				}
			}
			placeholders, err := model.ParamExpressions(s.Action.Params)
			if err != nil {
				return nil, err
			}
			sp.Params = make(map[string]*expr.Program, len(placeholders))
			for _, ph := range placeholders {
				if sp.Params[ph.Pointer], err = prog(s.Path() + "/params" + ph.Pointer + "/" + model.ExprKey); err != nil {
					return nil, err
				}
			}
			sp.Action = p.actions[s.Action.Action]
		case model.StepSwitch:
			sp.Cases = make([]*expr.Program, len(s.Switch.Cases))
			for i := range s.Switch.Cases {
				if sp.Cases[i], err = prog(fmt.Sprintf("%s/cases/%d/when", s.Path(), i)); err != nil {
					return nil, err
				}
			}
		case model.StepWait:
			if s.Wait.Match != "" {
				if sp.Match, err = prog(s.Path() + "/match"); err != nil {
					return nil, err
				}
				sp.MatchShape = dc.shapes["event:"+s.Wait.Event]
			}
		}
	}
	for _, name := range slices.Sorted(maps.Keys(dc.facts)) {
		resolve := p.reg.reg.facts[name].Resolve
		plan.Facts = append(plan.Facts, interpreter.FactSource{
			Name: name,
			Resolve: func(ctx context.Context, q store.ReadQuerier, s expr.Scope) (any, error) {
				return resolve(ctx, q, Scope(s))
			},
		})
	}
	return plan, nil
}
