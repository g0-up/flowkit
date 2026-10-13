package interpreter

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/g0-up/flowkit/internal/expr"
	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/interpreter/model"
	"github.com/g0-up/flowkit/internal/sched"
	"github.com/g0-up/flowkit/internal/store"
)

// stepMatchError records a wait match that failed to evaluate on an event.
const stepMatchError = "match_error"

// waitPlan returns the wait step of run with id stepID, its plan and the
// run's scope. Errors wrap errPlan.
func (in *Interpreter) waitPlan(run store.RunRow, stepID string) (*model.Step, *StepPlan, expr.Scope, error) {
	p, err := in.plan(run)
	if err != nil {
		return nil, nil, expr.Scope{}, err
	}
	s, sp, err := p.step(stepID)
	if err != nil {
		return nil, nil, expr.Scope{}, fmt.Errorf("%w: %w", errPlan, err)
	}
	if s.Type != model.StepWait {
		return nil, nil, expr.Scope{}, fmt.Errorf("%w: step %q is a %s step, not a wait", errPlan, s.ID, s.Type)
	}
	scope, err := runScope(run)
	if err != nil {
		return nil, nil, expr.Scope{}, fmt.Errorf("%w: %w", errPlan, err)
	}
	return s, sp, scope, nil
}

// match evaluates a wait's match on ev; a wait without one accepts every
// event of its type.
func (in *Interpreter) match(ctx context.Context, sp *StepPlan, ev store.EventRow, scope expr.Scope) (bool, error) {
	if sp.Match == nil {
		return true, nil
	}
	payload, err := expr.Activation(ev.Payload, sp.MatchShape)
	if err != nil {
		return false, err
	}
	return evalBool(ctx, sp.Match, expr.Vars{Event: payload, Now: in.clock.Now(), Scope: scope})
}

// recordMatchError records in tx that the match of the wait at stepID,
// visit waitSeq, failed on the event. It does not change the run's rev. A
// failure already recorded, by a retried route job or by the look-back and
// the route of the same event, is not recorded again; then it reports
// false. Two transactions recording the same failure at once may both
// record it.
func (in *Interpreter) recordMatchError(ctx context.Context, tx store.Tx, runID ids.UUID, stepID string, waitSeq int, eventID int64, cause error) (bool, error) {
	detail := fmt.Sprintf("event %d, wait %d", eventID, waitSeq)
	recorded, err := in.store.StepRecorded(ctx, tx, runID, stepID, stepMatchError, detail)
	if err != nil || recorded {
		return false, err
	}
	note := in.noteText(cause)
	_, err = in.store.AppendStep(ctx, tx, store.StepRow{
		RunID: runID, At: in.clock.Now(), StepID: stepID, Kind: stepMatchError, Detail: &detail, Error: &note,
	})
	return err == nil, err
}

// countMatchErrors adds n newly recorded failed matches of a definition's waits to the
// metric and the log.
func (in *Interpreter) countMatchErrors(ctx context.Context, run store.RunRow, stepID string, n int) {
	if n == 0 {
		return
	}
	in.matchErrors.Add(ctx, int64(n), metric.WithAttributeSet(attribute.NewSet(attribute.String("def", run.DefName))))
	in.log.WarnContext(ctx, "flowkit: wait match failed",
		"run_id", run.ID.String(), "def_name", run.DefName, "step_id", stepID, "count", n)
}

// lookback is TX-B of a wait: it reads the events stored before the
// subscription committed, oldest first, and claims the wait with the first
// eligible event its match accepts. When none does, it clears the run's
// lookback_pending without changing its rev, so a concurrent claim by the
// route of a new event still wins.
//
// It reads at most maxRows events in keyset pages of pageSize. Stopping at
// that limit logs a warning and counts flowkit.lookback_truncated; the run
// keeps waiting for new events.
func (in *Interpreter) lookback(ctx context.Context, runID ids.UUID) sched.Result {
	var (
		run       store.RunRow
		stepID    string
		c         *claim
		won       bool
		matchErrs int
		truncated bool
	)
	err := in.inTx(ctx, func(tx store.Tx) error {
		var err error
		if run, err = in.store.GetRun(ctx, tx, runID); err != nil {
			return err
		}
		if run.State != stateWaiting || !run.LookbackPending || run.WaitStep == nil {
			return nil
		}
		stepID = *run.WaitStep
		s, sp, scope, err := in.waitPlan(run, stepID)
		if err != nil {
			return err
		}
		elig := in.eligibility(run, s.Wait.Event)
		afterAt, afterID := elig.since, int64(0)
		for scanned := 0; ; {
			limit := min(in.pageSize, in.maxRows-scanned)
			page, err := in.store.LookbackPage(ctx, tx, store.LookbackQuery{
				RunID: run.ID, EventType: s.Wait.Event, AfterAt: afterAt, AfterID: afterID, Limit: limit,
			})
			if err != nil {
				return err
			}
			scanned += len(page)
			for _, ev := range page {
				afterAt, afterID = ev.InsertedAt, ev.ID
				if !in.eligible(elig, ev) {
					continue
				}
				ok, merr := in.match(ctx, sp, ev, scope)
				if merr != nil {
					if err := ctx.Err(); err != nil {
						return err
					}
					recorded, err := in.recordMatchError(ctx, tx, run.ID, stepID, run.WaitSeq, ev.ID, merr)
					if err != nil {
						return err
					}
					if recorded {
						matchErrs++
					}
					continue
				}
				if !ok {
					continue
				}
				c = &claim{run: run, stepID: stepID, wait: s.Wait, waitSeq: run.WaitSeq, event: &ev}
				won, err = in.claimInTx(ctx, tx, *c)
				return err
			}
			if len(page) < limit {
				break
			}
			if scanned >= in.maxRows {
				truncated = true
				break
			}
		}
		_, err = in.store.ClearLookback(ctx, tx, run.ID, run.WaitSeq)
		return err
	})
	switch {
	case errors.Is(err, store.ErrNotFound) && run.ID == (ids.UUID{}):
		in.log.WarnContext(ctx, "flowkit: pumped run does not exist", "run_id", runID.String())
		return sched.Done()
	case errors.Is(err, errPlan):
		return in.planFailed(ctx, run, err)
	case errors.Is(err, errAlreadyConsumed):
		if err := in.inTx(ctx, func(tx store.Tx) error { return in.recordClaimLost(ctx, tx, *c) }); err != nil {
			return sched.Retry(fmt.Errorf("flowkit interpreter: look-back of run %s: %w", runID, err))
		}
		return sched.Done()
	case err != nil:
		return sched.Retry(fmt.Errorf("flowkit interpreter: look-back of run %s: %w", runID, err))
	}
	in.countMatchErrors(ctx, run, stepID, matchErrs)
	if truncated {
		in.truncated.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(attribute.String("def", run.DefName))))
		in.log.WarnContext(ctx, "flowkit: wait look-back stopped at its row limit; the run waits for new events",
			"run_id", run.ID.String(), "def_name", run.DefName, "step_id", stepID, "rows", in.maxRows)
	}
	if won {
		in.logClaim(ctx, *c)
	}
	return sched.Done()
}

// eligible applies the eligibility rule to a stored event, and leaves out
// an event that no longer fits its registered type, as its route does.
func (in *Interpreter) eligible(e eligibility, ev store.EventRow) bool {
	if !e.admits(ev) {
		return false
	}
	_, err := in.events.Check(ev.Type, ev.Payload)
	return err == nil
}
