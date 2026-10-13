package interpreter

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/g0-up/flowkit/internal/executor"
	"github.com/g0-up/flowkit/internal/expr"
	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/interpreter/model"
	"github.com/g0-up/flowkit/internal/sched"
	"github.com/g0-up/flowkit/internal/schema"
	"github.com/g0-up/flowkit/internal/store"
	"github.com/g0-up/flowkit/internal/testhooks"
)

// Step kinds a pump records.
const (
	stepSkipped  = "skipped"
	stepBranched = "branched"
	stepEntered  = "entered"
	stepExecuted = "executed"
	stepWaiting  = "waiting"
)

// walk moves the run from its current frame through switches and skipped
// actions, in memory, and ends the turn at the first action, wait or end.
func (in *Interpreter) walk(ctx context.Context, t *turn) sched.Result {
	seen := map[string]bool{}
	for {
		fr := t.current()
		s, sp, err := t.plan.step(fr.Step)
		if err != nil {
			return in.stepFailed(ctx, t, StepRun, err)
		}
		if seen[s.ID] {
			return in.stepFailed(ctx, t, s.ID, fmt.Errorf(
				"step %q reached twice in one pump: the loop through it runs no action and waits for no event", s.ID))
		}
		seen[s.ID] = true
		if res, stop := in.walkStep(ctx, t, s, sp, fr); stop {
			return res
		}
	}
}

// walkStep walks step s in a flowkit.step span. It returns stop false
// when the turn goes on to the next step in memory.
func (in *Interpreter) walkStep(ctx context.Context, t *turn, s *model.Step, sp *StepPlan, fr frame) (res sched.Result, stop bool) {
	ctx, span := in.tracer.Start(ctx, "flowkit.step", trace.WithAttributes(
		attribute.String("step_id", s.ID), attribute.String("step_type", string(s.Type))))
	defer func() {
		if err := res.Err(); err != nil {
			span.SetStatus(codes.Error, in.noteText(err))
		}
		span.End()
	}()
	switch s.Type {
	case model.StepAction:
		if fr.Entered {
			return in.runAction(ctx, t, s, sp, fr), true
		}
		ok, err := in.when(ctx, t, sp)
		if err != nil {
			return in.stepFailed(ctx, t, s.ID, fmt.Errorf("when: %w", err)), true
		}
		if ok {
			return in.enterAction(ctx, t, s, sp), true
		}
		t.note(s.ID, stepSkipped, "when is false", map[string]any{
			"when": sp.When.Source(), "facts": t.read(sp.When), "now": t.now, "next": s.Action.Next,
		})
		t.frames.moveTo(s.Action.Next)
		return sched.Result{}, false
	case model.StepSwitch:
		next, snapshot, err := in.branch(ctx, t, s, sp)
		if err != nil {
			return in.stepFailed(ctx, t, s.ID, err), true
		}
		t.note(s.ID, stepBranched, next, snapshot)
		t.frames.moveTo(next)
		return sched.Result{}, false
	case model.StepWait:
		return in.enterWait(ctx, t, s), true
	case model.StepEnd:
		return in.end(ctx, t, s), true
	default:
		return in.stepFailed(ctx, t, s.ID, fmt.Errorf("step %q has unknown type %q", s.ID, s.Type)), true
	}
}

// stepFailed fails the run at stepID because of err, an error of the
// interpreter's own, such as an expression or a fact resolver error.
func (in *Interpreter) stepFailed(ctx context.Context, t *turn, stepID string, err error) sched.Result {
	return in.failRun(ctx, t, stepID, TerminalFailed, in.redact(err), nil)
}

// vars are the variables of the step's expressions in this turn.
func (t *turn) vars(sp *StepPlan) (expr.Vars, error) {
	facts, err := sp.Env.FactValues(t.facts)
	if err != nil {
		return expr.Vars{}, err
	}
	return expr.Vars{Event: t.trigger, Facts: facts, Now: t.now, Scope: t.scope}, nil
}

// read returns the stored values of the facts the programs read, for a
// step's snapshot.
func (t *turn) read(progs ...*expr.Program) map[string]any {
	out := map[string]any{}
	for _, p := range progs {
		if p == nil {
			continue
		}
		for _, name := range p.Refs() {
			if v, ok := t.facts[name]; ok {
				out[name] = v
			}
		}
	}
	return out
}

// evalBool evaluates a compiled bool expression.
func evalBool(ctx context.Context, p *expr.Program, vars expr.Vars) (bool, error) {
	v, err := p.Eval(ctx, vars)
	if err != nil {
		return false, err
	}
	b, ok := v.Value().(bool)
	if !ok {
		return false, fmt.Errorf("expression returned %s, want bool", v.Type().TypeName())
	}
	return b, nil
}

// when evaluates an action's condition; an action without one runs.
func (in *Interpreter) when(ctx context.Context, t *turn, sp *StepPlan) (bool, error) {
	if sp.When == nil {
		return true, nil
	}
	vars, err := t.vars(sp)
	if err != nil {
		return false, err
	}
	return evalBool(ctx, sp.When, vars)
}

// branch picks the next step of a switch: the first case whose condition
// holds, or the default. The snapshot records the facts and time the
// conditions saw.
func (in *Interpreter) branch(ctx context.Context, t *turn, s *model.Step, sp *StepPlan) (string, map[string]any, error) {
	vars, err := t.vars(sp)
	if err != nil {
		return "", nil, err
	}
	for i, p := range sp.Cases {
		ok, err := evalBool(ctx, p, vars)
		if err != nil {
			return "", nil, fmt.Errorf("case %d: %w", i, err)
		}
		if ok {
			next := s.Switch.Cases[i].Next
			return next, map[string]any{
				"case": i, "when": p.Source(), "facts": t.read(sp.Cases[:i+1]...), "now": t.now, "next": next,
			}, nil
		}
	}
	return s.Switch.Default, map[string]any{
		"case": "default", "facts": t.read(sp.Cases...), "now": t.now, "next": s.Switch.Default,
	}, nil
}

// enterAction commits the entry of an action: a new visit, the frame
// marked entered with the turn's time, and the turn's facts. From then on
// the branch to the action is decided: a pump after a crash runs this
// visit's effect again with the same facts and time.
func (in *Interpreter) enterAction(ctx context.Context, t *turn, s *model.Step, sp *StepPlan) sched.Result {
	next := t.frames.clone()
	fr := next.enter(s.ID, t.now)
	framesRaw, err := next.encode()
	if err != nil {
		return in.stepFailed(ctx, t, s.ID, err)
	}
	factsRaw, err := encodeFacts(t.facts)
	if err != nil {
		return in.stepFailed(ctx, t, s.ID, err)
	}
	t.note(s.ID, stepEntered, s.Action.Action, map[string]any{
		"visit": fr.Visit, "when": whenSource(sp), "facts": t.read(sp.When), "now": t.now,
	})
	res, ok := in.commit(ctx, t, s.ID, func(tx store.Tx) error {
		ok, err := in.store.UpdateRun(ctx, tx, store.RunUpdate{
			ID: t.run.ID, Rev: t.run.Rev, Frames: framesRaw, Facts: factsRaw, At: in.clock.Now(),
		})
		if err != nil {
			return err
		}
		if !ok {
			return ErrStale
		}
		return in.writePending(ctx, tx, t)
	})
	if !ok {
		return res
	}
	t.run.Rev++
	t.run.Frames, t.run.Facts = framesRaw, factsRaw
	t.frames, t.pending = next, nil
	if in.hooks.afterEnter != nil {
		if err := in.hooks.afterEnter(t.run.ID); err != nil {
			return sched.Retry(err)
		}
	}
	testhooks.Reach(testhooks.PointActionEntered)
	return in.runAction(ctx, t, s, sp, fr)
}

func whenSource(sp *StepPlan) string {
	if sp.When == nil {
		return ""
	}
	return sp.When.Source()
}

// idemKey is the idempotency key of one visit of an action step.
func idemKey(runID ids.UUID, stepID string, visit int) string {
	return runID.String() + ":" + stepID + ":" + strconv.Itoa(visit)
}

// runAction resolves the params of an entered action, validates them
// against the action's full params schema and runs the effect.
func (in *Interpreter) runAction(ctx context.Context, t *turn, s *model.Step, sp *StepPlan, fr frame) sched.Result {
	ap := sp.Action
	params, err := in.resolveParams(ctx, t, s, sp)
	if err != nil {
		return in.stepFailed(ctx, t, s.ID, err)
	}
	key := idemKey(t.run.ID, s.ID, fr.Visit)
	out := in.exec.Run(ctx, executor.ExecRequest{
		RunID: t.run.ID, StepID: s.ID, IdemKey: key, DefName: t.run.DefName, Action: ap.Action, Params: params,
		Scope: executor.Scope{EntityKind: t.scope.EntityKind, EntityID: t.scope.EntityID, Extra: t.scope.Extra},
	})
	log := in.log.With("run_id", t.run.ID.String(), "step_id", s.ID, "idem_key", key)
	switch out.Kind {
	case executor.Done:
		if out.Err != nil {
			return in.failRun(ctx, t, s.ID, TerminalActionFailed, out.Err.Error(), nil)
		}
		return in.actionDone(ctx, t, s, fr, key, out)
	case executor.Failed:
		return in.failRun(ctx, t, s.ID, TerminalActionFailed, out.Err.Error(), in.exhausted(t, s, key, out.Err))
	case executor.RetryLater:
		log.DebugContext(ctx, "flowkit: action to be retried", "delay", out.Delay)
		return sched.Snooze(out.Delay)
	case executor.BlockedInDoubt:
		// Whoever resolves the effect pumps the run again.
		log.InfoContext(ctx, "flowkit: action in doubt; run waits for its resolution")
		return sched.Done()
	case executor.Abandoned:
		// The job's context ended. Retrying the job pumps the run again
		// even when the executor gave its claim back, which no janitor
		// pass would find.
		return sched.Retry(fmt.Errorf("flowkit interpreter: run %s at step %s abandoned: %w", t.run.ID, s.ID, out.Err))
	case executor.Errored:
		return sched.Retry(fmt.Errorf("flowkit interpreter: run %s at step %s: %w", t.run.ID, s.ID, out.Err))
	}
	return sched.Retry(fmt.Errorf("flowkit interpreter: run %s at step %s: unknown outcome %s", t.run.ID, s.ID, out.Kind))
}

// resolveParams replaces the expressions in an action's params by their
// values, checks the result against the action's params schema, and types
// it for Execute.
func (in *Interpreter) resolveParams(ctx context.Context, t *turn, s *model.Step, sp *StepPlan) (map[string]any, error) {
	vars, err := t.vars(sp)
	if err != nil {
		return nil, err
	}
	params, err := model.ResolveParams(s.Action.Params, func(pe model.ParamExpr) (any, error) {
		p, ok := sp.Params[pe.Pointer]
		if !ok {
			return nil, fmt.Errorf("params%s: the expression is not compiled", pe.Pointer)
		}
		v, err := p.Eval(ctx, vars)
		if err != nil {
			return nil, fmt.Errorf("params%s: %w", pe.Pointer, err)
		}
		return expr.ToNative(v)
	})
	if err != nil {
		return nil, err
	}
	doc, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("encode params: %w", err)
	}
	decoded, err := schema.Decode(doc)
	if err != nil {
		return nil, fmt.Errorf("decode params: %w", err)
	}
	if err := sp.Action.Validate(decoded); err != nil {
		return nil, fmt.Errorf("params of action %s do not fit its params schema: %w", s.Action.Action, err)
	}
	typed, err := expr.Activation(doc, sp.Action.ParamsShape)
	if err != nil {
		return nil, fmt.Errorf("params of action %s: %w", s.Action.Action, err)
	}
	return typed, nil
}

// exhausted is the dead letter of an action whose attempts are used up.
func (in *Interpreter) exhausted(t *turn, s *model.Step, key string, cause error) *store.DeadLetterRow {
	// The fields are strings, which always encode.
	raw, _ := json.Marshal(map[string]any{
		"run_id": t.run.ID.String(), "step_id": s.ID, "idem_key": key, "action": s.Action.Action, "def_name": t.run.DefName,
	})
	note := in.noteText(cause)
	runID := t.run.ID
	return &store.DeadLetterRow{
		Kind: deadLetterExhausted, Ref: key, RunID: &runID, Raw: raw, Note: &note, CreatedAt: in.clock.Now(),
	}
}

// actionDone commits a done action: its output merged into the facts and
// the run moved to the next step. The next step runs in a new pump.
func (in *Interpreter) actionDone(ctx context.Context, t *turn, s *model.Step, fr frame, key string, out executor.ExecOutcome) sched.Result {
	facts, err := mergeFacts(t.facts, out.Output)
	if err != nil {
		return in.stepFailed(ctx, t, s.ID, err)
	}
	factsRaw, err := encodeFacts(facts)
	if err != nil {
		return in.stepFailed(ctx, t, s.ID, err)
	}
	next := t.frames.clone()
	next.moveTo(s.Action.Next)
	framesRaw, err := next.encode()
	if err != nil {
		return in.stepFailed(ctx, t, s.ID, err)
	}
	res, ok := in.commit(ctx, t, s.ID, func(tx store.Tx) error {
		ok, err := in.store.UpdateRun(ctx, tx, store.RunUpdate{
			ID: t.run.ID, Rev: t.run.Rev, Frames: framesRaw, Facts: factsRaw, At: in.clock.Now(),
		})
		if err != nil {
			return err
		}
		if !ok {
			return ErrStale
		}
		action := s.Action.Action
		// The snapshot holds ints, strings and bools, which always encode.
		snapshot, _ := json.Marshal(map[string]any{
			"visit": fr.Visit, "idem_key": key, "cached": out.Cached, "next": s.Action.Next,
		})
		if _, err := in.store.AppendStep(ctx, tx, store.StepRow{
			RunID: t.run.ID, At: in.clock.Now(), StepID: s.ID, Kind: stepExecuted, Detail: &action, Snapshot: snapshot,
		}); err != nil {
			return err
		}
		return in.pumps.EnqueuePump(ctx, tx, t.run.ID)
	})
	if !ok {
		return res
	}
	in.log.InfoContext(ctx, "flowkit: action done",
		"run_id", t.run.ID.String(), "step_id", s.ID, "idem_key", key, "cached", out.Cached)
	return sched.Done()
}

// enterWait parks the run on a wait (TX-A): the next wait sequence, the
// subscription and the timeout, committed before the look-back reads the
// stored events. An event committed before the look-back reads is found
// by it; one committed later is routed to the subscription.
func (in *Interpreter) enterWait(ctx context.Context, t *turn, s *model.Step) sched.Result {
	next := t.frames.clone()
	next.enter(s.ID, t.now)
	framesRaw, err := next.encode()
	if err != nil {
		return in.stepFailed(ctx, t, s.ID, err)
	}
	factsRaw, err := encodeFacts(t.facts)
	if err != nil {
		return in.stepFailed(ctx, t, s.ID, err)
	}
	due := t.now.Add(s.Wait.Timeout)
	res, ok := in.commit(ctx, t, s.ID, func(tx store.Tx) error {
		now := in.clock.Now()
		seq, ok, err := in.store.EnterWait(ctx, tx, store.RunUpdate{
			ID: t.run.ID, Rev: t.run.Rev, Frames: framesRaw, Facts: factsRaw, At: now,
		}, s.ID)
		if err != nil {
			return err
		}
		if !ok {
			return ErrStale
		}
		if err := in.writePending(ctx, tx, t); err != nil {
			return err
		}
		if err := in.store.InsertSubscription(ctx, tx, store.SubscriptionRow{
			RunID: t.run.ID, WaitSeq: seq, StepID: s.ID, EventType: s.Wait.Event, MatchCEL: s.Wait.Match, CreatedAt: now,
		}); err != nil {
			return err
		}
		if err := in.timers.ScheduleTimeout(ctx, tx, sched.WaitTimeoutArgs{
			RunID: t.run.ID, StepID: s.ID, WaitSeq: seq, DueAt: due,
		}); err != nil {
			return err
		}
		event := s.Wait.Event
		// The snapshot holds an int, a string and a time, which always
		// encode.
		snapshot, _ := json.Marshal(map[string]any{"wait_seq": seq, "due_at": due})
		_, err = in.store.AppendStep(ctx, tx, store.StepRow{
			RunID: t.run.ID, At: now, StepID: s.ID, Kind: stepWaiting, Detail: &event, Snapshot: snapshot,
		})
		return err
	})
	if !ok {
		return res
	}
	if in.hooks.afterWait != nil {
		if err := in.hooks.afterWait(t.run.ID); err != nil {
			return sched.Retry(err)
		}
	}
	testhooks.Reach(testhooks.PointWaitEntered)
	return in.lookback(ctx, t.run.ID)
}

// end finishes the run completed with the end step's name.
func (in *Interpreter) end(ctx context.Context, t *turn, s *model.Step) sched.Result {
	res, ok := in.commit(ctx, t, s.ID, func(tx store.Tx) error {
		// The serial lock comes before the run row, which the pending
		// steps lock.
		if t.run.SerialKey != nil {
			if err := in.store.LockSerial(ctx, tx, t.run.DefName, *t.run.SerialKey); err != nil {
				return err
			}
		}
		if err := in.writePending(ctx, tx, t); err != nil {
			return err
		}
		return in.FinishRun(ctx, tx, t.run, Finish{State: stateCompleted, Name: s.End.Name, StepID: s.ID})
	})
	if !ok {
		return res
	}
	in.log.InfoContext(ctx, "flowkit: run completed",
		"run_id", t.run.ID.String(), "def_name", t.run.DefName, "terminal", s.End.Name)
	return sched.Done()
}
