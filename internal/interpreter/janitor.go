package interpreter

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/g0-up/flowkit/internal/sched"
	"github.com/g0-up/flowkit/internal/store"
)

const (
	// janitorBatch is how many rows one janitor transaction handles.
	janitorBatch = 500
	// janitorMaxBatches bounds the batches of one step in a pass, so a pass
	// fits its job's timeout; the next pass goes on where it stopped.
	janitorMaxBatches = 20
)

// Janitor handles a janitor job: one pass over what no other job is bound
// to do. Every step is idempotent and safe to run again, on another
// instance, after a pass dies midway. A pass
//
//  1. resolves effects whose claim expired, as the executor does;
//  2. pumps waiting runs whose look-back did not finish within the claim
//     timeout;
//  3. promotes the run queues of serial keys that no run will promote,
//     such as a queue left while the definition was disabled;
//  4. pumps running runs not written for StuckRunAfter that have no live
//     pump job, no effect in doubt and no effect claimed within the claim
//     timeout;
//  5. applies the retention: terminal runs past Retention.StepDetail are
//     summarised and their detail deleted, then summaries past
//     Retention.Run, unreferenced events past Retention.Event and dead
//     letters resolved before Retention.DeadLetter are deleted.
//
// A step that fails does not stop the others; the job fails with every
// step's error. All times come from the flowkit clock, read once.
func (in *Interpreter) Janitor(ctx context.Context, _ sched.JanitorArgs) sched.Result {
	now := in.clock.Now()
	steps := []struct {
		name string
		run  func(context.Context, time.Time) error
	}{
		{"resolve expired claims", in.resolveExpiredClaims},
		{"resume look-backs", in.resumeLookbacks},
		{"promote orphaned queues", in.promoteOrphanedQueues},
		{"pump stuck runs", in.pumpStuckRuns},
		{"apply retention", in.applyRetention},
	}
	var errs []error
	for _, s := range steps {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if err := s.run(ctx, now); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", s.name, err))
		}
	}
	if len(errs) > 0 {
		return sched.Retry(fmt.Errorf("flowkit interpreter: janitor: %w", errors.Join(errs...)))
	}
	return sched.Done()
}

func (in *Interpreter) resolveExpiredClaims(ctx context.Context, now time.Time) error {
	return batches(ctx, func() (int, error) {
		return in.exec.ResolveExpiredClaims(ctx, now, janitorBatch)
	})
}

// resumeLookbacks pumps the waiting runs whose look-back a dead pump left
// unfinished. A pump of a waiting run runs its look-back again.
func (in *Interpreter) resumeLookbacks(ctx context.Context, now time.Time) error {
	return in.inTx(ctx, func(tx store.Tx) error {
		runIDs, err := in.store.ListStaleLookbacks(ctx, tx, now.Add(-in.claimTimeout), janitorBatch)
		if err != nil {
			return err
		}
		for _, id := range runIDs {
			if err := in.pumps.EnqueuePump(ctx, tx, id); err != nil {
				return err
			}
		}
		if len(runIDs) > 0 {
			in.log.InfoContext(ctx, "flowkit: janitor resumed look-backs", "runs", len(runIDs))
		}
		return nil
	})
}

// promoteOrphanedQueues promotes each queue no run will promote, in its
// own transaction. PromoteQueued takes the key's serial lock and checks
// again, so a run started meanwhile keeps the queue waiting.
func (in *Interpreter) promoteOrphanedQueues(ctx context.Context, _ time.Time) error {
	var keys []store.SerialKeyRef
	err := in.inTx(ctx, func(tx store.Tx) error {
		var err error
		keys, err = in.store.ListOrphanedQueues(ctx, tx, janitorBatch)
		return err
	})
	if err != nil {
		return err
	}
	var errs []error
	for _, k := range keys {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(errs, err)...)
		}
		var (
			runID    string
			promoted bool
		)
		err := in.inTx(ctx, func(tx store.Tx) error {
			id, ok, err := in.PromoteQueued(ctx, tx, k.DefName, k.SerialKey)
			runID, promoted = id.String(), ok
			return err
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("definition %q: %w", k.DefName, err))
			continue
		}
		if promoted {
			in.log.InfoContext(ctx, "flowkit: janitor promoted an orphaned queue", "def_name", k.DefName, "run_id", runID)
		}
	}
	return errors.Join(errs...)
}

// pumpStuckRuns pumps the running runs that lost their pump. A run whose
// effect is in doubt waits for ResolveDeadLetter, and one whose effect is
// claimed within the claim timeout is still being driven, so neither is
// pumped. A pump of a run that moved meanwhile stops at its rev check.
func (in *Interpreter) pumpStuckRuns(ctx context.Context, now time.Time) error {
	return in.inTx(ctx, func(tx store.Tx) error {
		runIDs, err := in.store.ListStuckRuns(ctx, tx, store.StuckRunQuery{
			UpdatedBefore: now.Add(-in.stuckRunAfter),
			ClaimedAfter:  now.Add(-in.claimTimeout),
			PumpKind:      sched.KindPumpRun,
			Limit:         janitorBatch,
		})
		if err != nil {
			return err
		}
		for _, id := range runIDs {
			if err := in.pumps.EnqueuePump(ctx, tx, id); err != nil {
				return err
			}
		}
		if len(runIDs) > 0 {
			in.log.WarnContext(ctx, "flowkit: janitor pumped stuck runs", "runs", len(runIDs))
		}
		return nil
	})
}

// applyRetention deletes what outlived its retention, in batches of
// janitorBatch rows, one short transaction each.
func (in *Interpreter) applyRetention(ctx context.Context, now time.Time) error {
	r := in.retention
	steps := []struct {
		name   string
		before time.Time
		run    func(context.Context, store.Tx, time.Time, int) (int, error)
	}{
		{"summarise runs", now.Add(-r.StepDetail), in.store.SummarizeRuns},
		{"delete summaries", now.Add(-r.Run), in.store.DeleteExpiredSummaries},
		{"delete events", now.Add(-r.Event), in.store.DeleteExpiredEvents},
		{"delete dead letters", now.Add(-r.DeadLetter), in.store.DeleteResolvedDeadLetters},
	}
	var errs []error
	for _, s := range steps {
		total := 0
		err := batches(ctx, func() (int, error) {
			var n int
			err := in.inTx(ctx, func(tx store.Tx) error {
				var err error
				n, err = s.run(ctx, tx, s.before, janitorBatch)
				return err
			})
			total += n
			return n, err
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", s.name, err))
		}
		if total > 0 {
			in.log.InfoContext(ctx, "flowkit: janitor applied retention", "step", s.name, "rows", total)
		}
	}
	return errors.Join(errs...)
}

// batches calls batch until it handles fewer than janitorBatch rows, fails,
// ctx ends or janitorMaxBatches calls were made.
func batches(ctx context.Context, batch func() (int, error)) error {
	for range janitorMaxBatches {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := batch()
		if err != nil || n < janitorBatch {
			return err
		}
	}
	return nil
}
