package interpreter

import (
	"context"
	"errors"
	"fmt"

	"github.com/g0-up/flowkit/internal/errtext"
	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/interpreter/model"
	"github.com/g0-up/flowkit/internal/store"
)

// TerminalCancelled is the terminal name of a run cancelled by Cancel.
const TerminalCancelled = "$cancelled"

// cancelTries is how many times Cancel reads the run again after losing it
// to a concurrent write.
const cancelTries = 3

// Cancel ends a non-terminal run cancelled with TerminalCancelled, the
// reason recorded on its terminal step, through FinishRun: the serial lock
// before the run row, the subscriptions deleted, the key's queue promoted.
// A run that is already terminal is left as it is. A run that changes
// between the read and the write is read again, up to cancelTries times.
// A run that does not exist returns store.ErrNotFound.
func (in *Interpreter) Cancel(ctx context.Context, runID ids.UUID, reason string) error {
	var err error
	for range cancelTries {
		err = in.inTx(ctx, func(tx store.Tx) error {
			run, err := in.store.GetRun(ctx, tx, runID)
			if err != nil {
				return err
			}
			if run.Terminal {
				return nil
			}
			f := Finish{State: stateCancelled, Name: TerminalCancelled, StepID: StepRun}
			if reason != "" {
				f.Error = &reason
			}
			return in.FinishRun(ctx, tx, run, f)
		})
		if !errors.Is(err, ErrStale) {
			break
		}
	}
	if err != nil {
		return fmt.Errorf("flowkit interpreter: cancel run %s: %w", runID, err)
	}
	return nil
}

// Resolution kinds, as dead_letters.resolution stores them.
const (
	ResolveRedrive  = "redrive"
	ResolveMarkDone = "mark_done"
	ResolveSkip     = "skip"
)

// Resolution is an operator's decision on a dead letter.
type Resolution struct {
	Kind   string // ResolveRedrive, ResolveMarkDone or ResolveSkip
	Actor  string
	Reason string
	// Output is the action's output a mark_done asserts.
	Output map[string]any
}

// ErrResolved reports a dead letter that is already resolved.
var ErrResolved = errors.New("flowkit interpreter: dead letter already resolved")

// InvalidResolution reports a resolution the dead letter does not accept,
// or an output that does not fit the action. Path is a JSON Pointer into
// the resolution.
type InvalidResolution struct {
	Path string
	Msg  string
}

func (e *InvalidResolution) Error() string {
	return "flowkit interpreter: invalid resolution: " + e.Path + ": " + e.Msg
}

// ResolveDeadLetter resolves the open dead letter id with r in one
// transaction: it claims the dead letter, recording r's kind, actor and
// reason, then acts on it, enqueueing the jobs the action needs in the
// same transaction. On any error nothing is written and the dead letter
// stays open, so of two concurrent resolutions only one takes effect; the
// other returns ErrResolved.
//
//   - effect_in_doubt: redrive releases the effect for the action to run
//     again; mark_done stores r.Output, checked against the action's
//     OutputFacts, as the effect's result. Both pump the run. Skip leaves
//     the run blocked until it is cancelled. An effect that kept the output
//     of a late success refuses redrive, and the effect of a run that has
//     ended accepts only skip.
//   - exhausted: the run already failed; only skip is accepted.
//   - schema_mismatch, unroutable_event: redrive routes the event again;
//     mark_done is refused.
//
// A dead letter that does not exist, or whose event is gone on a redrive,
// returns store.ErrNotFound. A refused kind returns an *InvalidResolution.
func (in *Interpreter) ResolveDeadLetter(ctx context.Context, id int64, r Resolution) error {
	switch r.Kind {
	case ResolveRedrive, ResolveMarkDone, ResolveSkip:
	default:
		return &InvalidResolution{Path: "/kind", Msg: fmt.Sprintf("unknown resolution kind %q", r.Kind)}
	}
	err := in.inTx(ctx, func(tx store.Tx) error {
		claim := store.DeadLetterClaim{ID: id, Resolution: r.Kind, At: in.clock.Now()}
		if r.Actor != "" {
			actor := errtext.Sanitize(r.Actor)
			claim.ResolvedBy = &actor
		}
		if r.Reason != "" {
			reason := errtext.Sanitize(r.Reason)
			claim.Reason = &reason
		}
		dl, ok, err := in.store.ClaimDeadLetter(ctx, tx, claim)
		if err != nil {
			return err
		}
		if !ok {
			// Missing, or resolved by another transaction.
			if _, err := in.store.GetDeadLetter(ctx, tx, id); err != nil {
				return err
			}
			return ErrResolved
		}
		if r.Kind == ResolveSkip {
			return nil
		}
		switch dl.Kind {
		case deadLetterInDoubt:
			return in.resolveInDoubt(ctx, tx, dl, r)
		case deadLetterSchemaMismatch, deadLetterUnroutable:
			if r.Kind != ResolveRedrive {
				return refused(dl.Kind, r.Kind)
			}
			return in.redriveEvent(ctx, tx, dl)
		default:
			return refused(dl.Kind, r.Kind)
		}
	})
	if err != nil {
		return fmt.Errorf("flowkit interpreter: resolve dead letter %d: %w", id, err)
	}
	return nil
}

func refused(dlKind, kind string) error {
	return &InvalidResolution{Path: "/kind", Msg: fmt.Sprintf("a %s dead letter cannot be resolved with %s", dlKind, kind)}
}

// resolveInDoubt redrives or marks done the effect in doubt dl names, then
// pumps its run. The effect of a run that has ended takes only skip, and an
// effect that kept the output of a late success is not redriven: the action
// ran, so it is marked done.
func (in *Interpreter) resolveInDoubt(ctx context.Context, tx store.Tx, dl store.DeadLetterRow, r Resolution) error {
	effect, err := in.store.GetEffect(ctx, tx, dl.Ref)
	if err != nil {
		return fmt.Errorf("effect %s: %w", dl.Ref, err)
	}
	run, err := in.store.GetRun(ctx, tx, effect.RunID)
	if err != nil {
		return fmt.Errorf("run %s of effect %s: %w", effect.RunID, effect.IdemKey, err)
	}
	if run.Terminal {
		return &InvalidResolution{Path: "/kind", Msg: fmt.Sprintf("run %s has ended; only skip is accepted", run.ID)}
	}
	var ok bool
	if r.Kind == ResolveRedrive {
		if ok, err = in.store.RedriveInDoubtEffect(ctx, tx, effect.IdemKey); err != nil {
			return err
		}
		if !ok {
			// Read again: a late success may have kept its output since.
			if effect, err = in.store.GetEffect(ctx, tx, effect.IdemKey); err != nil {
				return fmt.Errorf("effect %s: %w", dl.Ref, err)
			}
			if effect.InDoubt && effect.DoneAt == nil && effect.Result != nil {
				return &InvalidResolution{Path: "/kind", Msg: "the effect has the output of a late success; resolve with mark_done"}
			}
		}
	} else {
		result, err := in.checkOutput(run, effect, r.Output)
		if err != nil {
			return err
		}
		if ok, err = in.store.MarkInDoubtEffectDone(ctx, tx, effect.IdemKey, in.clock.Now(), result); err != nil {
			return err
		}
	}
	if !ok {
		return fmt.Errorf("effect %s is no longer in doubt", effect.IdemKey)
	}
	return in.pumps.EnqueuePump(ctx, tx, effect.RunID)
}

// checkOutput checks out, an operator's output for effect, against the
// OutputFacts of the action of run's pinned definition.
func (in *Interpreter) checkOutput(run store.RunRow, effect store.EffectRow, out map[string]any) ([]byte, error) {
	p, err := in.plan(run)
	if err != nil {
		return nil, err
	}
	s, sp, err := p.step(effect.StepID)
	if err != nil {
		return nil, fmt.Errorf("run %s: %w", run.ID, err)
	}
	if s.Type != model.StepAction {
		return nil, fmt.Errorf("run %s: step %q of effect %s is not an action", run.ID, s.ID, effect.IdemKey)
	}
	raw, err := in.exec.CheckOutput(sp.Action.Action, out)
	if err != nil {
		return nil, &InvalidResolution{Path: "/output", Msg: err.Error()}
	}
	return raw, nil
}

// redriveEvent routes the event of dl again.
func (in *Interpreter) redriveEvent(ctx context.Context, tx store.Tx, dl store.DeadLetterRow) error {
	if dl.EventID == nil {
		return fmt.Errorf("%s dead letter has no event: %w", dl.Kind, store.ErrNotFound)
	}
	if _, err := in.store.GetEvent(ctx, tx, *dl.EventID); err != nil {
		return fmt.Errorf("event %d: %w", *dl.EventID, err)
	}
	return in.routeJobs.EnqueueRoute(ctx, tx, *dl.EventID)
}
