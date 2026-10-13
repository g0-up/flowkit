package interpreter

import (
	"context"
	"errors"
	"fmt"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/obs"
	"github.com/g0-up/flowkit/internal/sched"
	"github.com/g0-up/flowkit/internal/store"
)

// Timeout handles a wait_timeout job. Before its DueAt by the flowkit
// clock it snoozes until then. After, it claims the wait it was scheduled
// for, moving the run to the wait's on_timeout step. A timer of a wait the
// run left, or of an earlier visit of the same wait, loses the claim and
// records claim_lost; timers are never cancelled. A handled timeout
// records how late it was handled.
func (in *Interpreter) Timeout(ctx context.Context, args sched.WaitTimeoutArgs) sched.Result {
	now := in.clock.Now()
	if now.Before(args.DueAt) {
		return sched.Snooze(args.DueAt.Sub(now))
	}
	var (
		run store.RunRow
		c   claim
		won bool
	)
	err := in.inTx(ctx, func(tx store.Tx) error {
		var err error
		if run, err = in.store.GetRun(ctx, tx, args.RunID); err != nil {
			return err
		}
		c = claim{run: run, stepID: args.StepID, waitSeq: args.WaitSeq}
		if run.State != stateWaiting || run.WaitSeq != args.WaitSeq || run.WaitStep == nil || *run.WaitStep != args.StepID {
			return in.recordClaimLost(ctx, tx, c)
		}
		s, _, _, err := in.waitPlan(run, args.StepID)
		if err != nil {
			return err
		}
		c.wait = s.Wait
		won, err = in.claimInTx(ctx, tx, c)
		return err
	})
	switch {
	case errors.Is(err, store.ErrNotFound) && run.ID == (ids.UUID{}):
		in.log.WarnContext(ctx, "flowkit: timed out run does not exist", "run_id", args.RunID.String())
		return sched.Done()
	case errors.Is(err, errPlan):
		return in.planFailed(ctx, run, err)
	case err != nil:
		return sched.Retry(fmt.Errorf("flowkit interpreter: timeout of run %s at step %s: %w", args.RunID, args.StepID, err))
	}
	obs.RecordDelay(ctx, in.timerLag, now.Sub(args.DueAt))
	if won {
		in.logClaim(ctx, c)
	}
	return sched.Done()
}
