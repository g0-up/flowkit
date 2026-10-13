package interpreter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/g0-up/flowkit/internal/expr"
	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/sched"
	"github.com/g0-up/flowkit/internal/store"
)

// stepStalePump records a pump that found the run changed by another one.
const stepStalePump = "stale_pump"

// Pump handles a pump_run job: one turn of the run.
//
// A terminal run is left alone. A waiting run gets its look-back again when
// the one after entering the wait has not finished; otherwise it is left
// alone. A running run of an enabled definition walks its steps from its
// current frame: switches and skipped actions in memory, up to the first
// action, wait or end, which the turn commits and stops at. So a turn runs
// at most one action and fits the pump job's timeout.
//
// Every write of the run checks the rev the turn read. A turn that finds
// the run changed by a concurrent one records stale_pump and stops.
//
// The turn is traced as a flowkit.run span with a flowkit.step child for
// each step it walks.
func (in *Interpreter) Pump(ctx context.Context, args sched.PumpRunArgs) sched.Result {
	ctx, span := in.tracer.Start(ctx, "flowkit.run", trace.WithAttributes(
		attribute.String("run_id", args.RunID.String()), attribute.String("instance_id", in.instanceID)))
	defer span.End()
	res := in.pump(ctx, span, args)
	if err := res.Err(); err != nil {
		span.SetStatus(codes.Error, in.noteText(err))
	}
	return res
}

func (in *Interpreter) pump(ctx context.Context, span trace.Span, args sched.PumpRunArgs) sched.Result {
	var (
		run     store.RunRow
		trigger store.EventRow
		enabled bool
	)
	err := in.inTx(ctx, func(tx store.Tx) error {
		var err error
		if run, err = in.store.GetRun(ctx, tx, args.RunID); err != nil {
			return err
		}
		if run.Terminal || run.State != stateRunning {
			return nil
		}
		def, err := in.store.GetDefinition(ctx, tx, run.DefName)
		if err != nil {
			return err
		}
		if enabled = def.Enabled; !enabled {
			return nil
		}
		trigger, err = in.store.GetEvent(ctx, tx, run.TriggerEventID)
		return err
	})
	if errors.Is(err, store.ErrNotFound) && run.ID == (ids.UUID{}) {
		in.log.WarnContext(ctx, "flowkit: pumped run does not exist", "run_id", args.RunID.String())
		return sched.Done()
	}
	if err != nil {
		return sched.Retry(fmt.Errorf("flowkit interpreter: pump run %s: %w", args.RunID, err))
	}
	span.SetAttributes(
		attribute.String("def_name", run.DefName),
		attribute.Int("def_version", run.DefVersion),
		attribute.Int("metamodel_version", run.MetamodelVersion),
		attribute.String("lib_version", run.LibVersion),
	)
	switch {
	case run.Terminal:
		return sched.Done()
	case run.State == stateWaiting:
		if !run.LookbackPending {
			return sched.Done()
		}
		return in.lookback(ctx, run.ID)
	case run.State != stateRunning:
		return sched.Retry(fmt.Errorf("flowkit interpreter: pump run %s: unknown state %q", run.ID, run.State))
	case !enabled:
		// The kill switch: the run keeps its state and frame; enabling the
		// definition pumps it again.
		in.log.DebugContext(ctx, "flowkit: definition disabled; run not advanced",
			"run_id", run.ID.String(), "def_name", run.DefName)
		return sched.Done()
	}
	if in.hooks.afterRead != nil {
		in.hooks.afterRead(run.ID)
	}
	t, res, ok := in.startTurn(ctx, run, trigger)
	if !ok {
		return res
	}
	return in.walk(ctx, t)
}

// turn is one pump of a running run: its state as read, its position and
// facts as the turn moves them, and the values frozen for the turn.
type turn struct {
	run    store.RunRow
	plan   *Plan
	frames frames
	// facts are the run's facts in stored form: those stored on the run,
	// with the registered facts resolved by this turn set over them.
	facts   map[string]any
	now     time.Time
	trigger map[string]any
	scope   expr.Scope
	// pending are the steps the turn walked in memory, written by the
	// turn's next write.
	pending []store.StepRow
}

// startTurn prepares the walk of run. When it cannot, it returns false and
// the result of the job.
func (in *Interpreter) startTurn(ctx context.Context, run store.RunRow, trigger store.EventRow) (*turn, sched.Result, bool) {
	t := &turn{run: run}
	fail := func(err error) (*turn, sched.Result, bool) {
		return nil, in.failRun(ctx, t, StepRun, TerminalFailed, in.redact(err), nil), false
	}
	var err error
	if t.plan, err = in.plan(run); err != nil {
		if errors.Is(err, errPlan) {
			return nil, in.planFailed(ctx, run, err), false
		}
		return nil, sched.Retry(err), false
	}
	if t.frames, err = decodeFrames(run.Frames); err != nil {
		return fail(err)
	}
	if t.facts, err = decodeFacts(run.Facts); err != nil {
		return fail(err)
	}
	if t.scope, err = runScope(run); err != nil {
		return fail(err)
	}
	if t.trigger, err = expr.Activation(trigger.Payload, t.plan.TriggerShape); err != nil {
		return fail(fmt.Errorf("trigger event %d: %w", trigger.ID, err))
	}
	if fr, ok := t.frames.top(); ok && fr.Entered {
		// The entry of the action froze its facts and time.
		t.now = fr.Now
		return t, sched.Result{}, true
	}
	t.now = in.clock.Now()
	resolved, err := in.resolveFacts(ctx, t.plan, t.scope)
	if err != nil {
		if ctx.Err() != nil {
			return nil, sched.Retry(err), false
		}
		return fail(err)
	}
	if t.facts, err = mergeFacts(t.facts, resolved); err != nil {
		return fail(err)
	}
	return t, sched.Result{}, true
}

// runScope reads the scope stored on run.
func runScope(run store.RunRow) (expr.Scope, error) {
	var s expr.Scope
	if run.EntityKind != nil {
		s.EntityKind = *run.EntityKind
	}
	if run.EntityID != nil {
		s.EntityID = *run.EntityID
	}
	if len(run.ScopeExtra) > 0 {
		if err := json.Unmarshal(run.ScopeExtra, &s.Extra); err != nil {
			return expr.Scope{}, fmt.Errorf("decode scope: %w", err)
		}
	}
	return s, nil
}

// current returns the step the run is at and its frame.
func (t *turn) current() frame {
	if fr, ok := t.frames.top(); ok {
		return fr
	}
	return frame{Step: t.plan.Def.Entry().ID}
}

// note queues a step record for the turn's next write.
func (t *turn) note(stepID, kind string, detail string, snapshot map[string]any) {
	row := store.StepRow{RunID: t.run.ID, StepID: stepID, Kind: kind}
	if detail != "" {
		row.Detail = &detail
	}
	if snapshot != nil {
		// Snapshots hold decoded JSON values, strings and times, which
		// always encode.
		row.Snapshot, _ = json.Marshal(snapshot)
	}
	t.pending = append(t.pending, row)
}

// writePending appends the queued step records in tx, stamped now.
func (in *Interpreter) writePending(ctx context.Context, tx store.Tx, t *turn) error {
	now := in.clock.Now()
	for _, s := range t.pending {
		s.At = now
		if _, err := in.store.AppendStep(ctx, tx, s); err != nil {
			return err
		}
	}
	return nil
}

// stale records that a write of the turn found the run changed.
func (in *Interpreter) stale(ctx context.Context, t *turn, stepID string) sched.Result {
	detail := "the run changed concurrently"
	err := in.inTx(ctx, func(tx store.Tx) error {
		_, err := in.store.AppendStep(ctx, tx, store.StepRow{
			RunID: t.run.ID, At: in.clock.Now(), StepID: stepID, Kind: stepStalePump, Detail: &detail,
		})
		return err
	})
	if err != nil {
		return sched.Retry(fmt.Errorf("flowkit interpreter: pump run %s: %w", t.run.ID, err))
	}
	in.log.InfoContext(ctx, "flowkit: stale pump stopped", "run_id", t.run.ID.String(), "step_id", stepID)
	return sched.Done()
}

// commit runs fn in a transaction and maps its end to the job's result: a
// stale run records stale_pump, another error retries the job.
func (in *Interpreter) commit(ctx context.Context, t *turn, stepID string, fn func(tx store.Tx) error) (sched.Result, bool) {
	err := in.inTx(ctx, fn)
	if errors.Is(err, ErrStale) {
		return in.stale(ctx, t, stepID), false
	}
	if err != nil {
		return sched.Retry(fmt.Errorf("flowkit interpreter: pump run %s at step %s: %w", t.run.ID, stepID, err)), false
	}
	return sched.Result{}, true
}

// failRun ends the run failed with the terminal name, recording reason, an
// already redacted text, and the steps the turn walked. A non-nil
// deadLetter is stored with it. A pump whose context ended retries instead:
// the failure may be the context's.
func (in *Interpreter) failRun(ctx context.Context, t *turn, stepID, name, reason string, deadLetter *store.DeadLetterRow) sched.Result {
	if err := ctx.Err(); err != nil {
		return sched.Retry(err)
	}
	res, ok := in.commit(ctx, t, stepID, func(tx store.Tx) error {
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
		if err := in.FinishRun(ctx, tx, t.run, Finish{State: stateFailed, Name: name, StepID: stepID, Error: &reason}); err != nil {
			return err
		}
		if deadLetter == nil {
			return nil
		}
		_, inserted, err := in.store.InsertDeadLetter(ctx, tx, *deadLetter)
		if err == nil && inserted {
			in.countDeadLetter(ctx, tx, deadLetter.Kind)
		}
		return err
	})
	if !ok {
		return res
	}
	in.log.WarnContext(ctx, "flowkit: run failed",
		"run_id", t.run.ID.String(), "def_name", t.run.DefName, "step_id", stepID, "terminal", name, "error", reason)
	return sched.Done()
}
