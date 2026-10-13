package interpreter

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/g0-up/flowkit/internal/errtext"
	"github.com/g0-up/flowkit/internal/store"
)

// stepTerminal is the run_steps kind recording a run's terminal state.
const stepTerminal = "terminal"

// Finish describes a terminal transition.
type Finish struct {
	// State is "completed", "failed" or "cancelled".
	State string
	// Name is the terminal name: the end step's name, or a reserved name
	// such as TerminalCancelledByNewer.
	Name string
	// StepID is the step the run ended at; StepRun when it ended outside
	// any step.
	StepID string
	// Error is the already redacted reason of a failure or cancellation,
	// if any. FinishRun sanitizes it before storing it.
	Error *string
}

// FinishRun is the only way a run becomes terminal. In tx, it
//
//  1. takes the serial lock of the run's key, if it has one, so the lock
//     is always taken before the run row;
//  2. moves the run to f.State if its rev is still run.Rev, or returns
//     ErrStale;
//  3. deletes the run's subscriptions;
//  4. records the terminal step;
//  5. promotes the head of the key's run queue if the run was exclusive.
//
// The serial lock must come before the run row lock in every transaction,
// or two transactions can deadlock on them. So a caller must not update the
// runs row, or call AppendStep for the run, earlier in tx than FinishRun:
// both lock the run row. A caller that has to write a step first must take
// the key's serial lock first, with LockSerial on the run's definition and
// serial key; FinishRun taking it again is then a no-op. On any error the
// caller must roll tx back.
//
// f.Error is stored sanitized, as the executor stores an action's error.
func (in *Interpreter) FinishRun(ctx context.Context, tx store.Tx, run store.RunRow, f Finish) error {
	switch f.State {
	case stateCompleted, stateFailed, stateCancelled:
	default:
		return fmt.Errorf("flowkit interpreter: finish run %s: %q is not a terminal state", run.ID, f.State)
	}
	if f.Name == "" || f.StepID == "" {
		return fmt.Errorf("flowkit interpreter: finish run %s: terminal name and step id are required", run.ID)
	}
	if run.SerialKey != nil {
		if err := in.store.LockSerial(ctx, tx, run.DefName, *run.SerialKey); err != nil {
			return err
		}
	}
	now := in.clock.Now()
	ok, err := in.store.FinishRun(ctx, tx, store.FinishRunRow{
		ID: run.ID, Rev: run.Rev, State: f.State, TerminalName: f.Name, At: now,
	})
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("finish run %s: %w", run.ID, ErrStale)
	}
	if err := in.store.DeleteRunSubscriptions(ctx, tx, run.ID); err != nil {
		return err
	}
	name := f.Name
	var stored *string
	if f.Error != nil {
		e := errtext.Sanitize(*f.Error)
		stored = &e
	}
	if _, err := in.store.AppendStep(ctx, tx, store.StepRow{
		RunID: run.ID, At: now, StepID: f.StepID, Kind: stepTerminal, Detail: &name, Error: stored,
	}); err != nil {
		return err
	}
	if run.Exclusive && run.SerialKey != nil {
		if _, _, err := in.PromoteQueued(ctx, tx, run.DefName, *run.SerialKey); err != nil {
			return err
		}
	}
	in.onCommit(tx, func() {
		in.runsTerminal.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(attribute.String("state", f.State))))
	})
	return nil
}
