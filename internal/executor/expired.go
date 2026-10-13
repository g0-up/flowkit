package executor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/g0-up/flowkit/internal/store"
)

// ResolveExpiredClaims resolves up to limit effects claimed before now minus
// the claim timeout that are neither done nor in doubt; the request that
// claimed them died or overran. An idempotent action's claim counts as a
// failed attempt and is released, and its run pumped again; once its attempts
// are used up, the next request ends it as Failed without executing it. Any
// other effect may have happened, so it goes in doubt, which also revokes the
// claim. It returns how many effects it
// resolved; each is resolved in its own transaction, and an effect whose claim
// changed meanwhile is skipped.
func (e *Executor) ResolveExpiredClaims(ctx context.Context, now time.Time, limit int) (int, error) {
	switch {
	case limit <= 0:
		return 0, fmt.Errorf("flowkit executor: limit %d must be positive", limit)
	case e.pumps == nil:
		return 0, errors.New("flowkit executor: resolving expired claims needs a pump enqueuer")
	case e.isIdempotent == nil:
		return 0, errors.New("flowkit executor: resolving expired claims needs IsIdempotent")
	}
	var rows []store.EffectRow
	err := e.inTx(ctx, func(tx store.Tx) error {
		var err error
		rows, err = e.store.ListExpiredClaims(ctx, tx, now.Add(-e.claimTimeout), limit)
		return err
	})
	if err != nil {
		return 0, err
	}
	resolved := 0
	for _, row := range rows {
		ok, err := e.resolveExpired(ctx, row)
		if err != nil {
			return resolved, fmt.Errorf("flowkit executor: resolve expired claim on %s: %w", row.IdemKey, err)
		}
		if ok {
			resolved++
		}
	}
	return resolved, nil
}

func (e *Executor) resolveExpired(ctx context.Context, row store.EffectRow) (bool, error) {
	log := e.log.With("idem_key", row.IdemKey, "run_id", row.RunID.String(), "step_id", row.StepID, "action", row.Action)
	idempotent := e.isIdempotent(row.Action)
	var ok, inserted bool
	err := e.inTx(ctx, func(tx store.Tx) error {
		var err error
		if idempotent {
			ok, err = e.failExpired(ctx, tx, row)
		} else {
			ok, inserted, err = e.expiredInDoubt(ctx, tx, row)
		}
		return err
	})
	if err == nil && inserted {
		e.countInDoubtDeadLetter(ctx)
	}
	switch {
	case err != nil:
		return false, err
	case !ok:
		log.Debug("flowkit executor: expired claim changed before it was resolved")
	case idempotent:
		log.Info("flowkit executor: counted an expired claim as a failed attempt and pumped the run again")
	default:
		log.Warn("flowkit executor: expired claim put in doubt")
	}
	return ok, nil
}

// failExpired records the expired claim of an idempotent action as a failed
// attempt, which releases it, and pumps its run. A request that keeps dying
// mid-action so uses up the action's attempts like one that keeps failing.
func (e *Executor) failExpired(ctx context.Context, tx store.Tx, row store.EffectRow) (bool, error) {
	msg := e.redact(fmt.Errorf("claim expired after %s without a result", e.claimTimeout))
	attempt, ok, err := e.store.FailEffect(ctx, tx, row.IdemKey, *row.ClaimToken, msg)
	if err != nil || !ok {
		return ok, err
	}
	if _, err := e.store.AppendStep(ctx, tx, store.StepRow{
		RunID: row.RunID, At: e.clock.Now(), StepID: row.StepID, Kind: stepAttemptFailed,
		Detail: &row.Action, Attempt: &attempt, Error: &msg,
	}); err != nil {
		return false, err
	}
	return true, e.pumps.EnqueuePump(ctx, tx, row.RunID)
}

// expiredInDoubt puts an effect whose claim expired in doubt. inserted
// reports whether its dead letter is new.
func (e *Executor) expiredInDoubt(ctx context.Context, tx store.Tx, row store.EffectRow) (ok, inserted bool, err error) {
	msg := e.redact(fmt.Errorf("claim expired after %s without a result; the action may have run", e.claimTimeout))
	attempt, ok, err := e.store.MarkEffectInDoubt(ctx, tx, row.IdemKey, *row.ClaimToken, msg)
	if err != nil || !ok {
		return ok, false, err
	}
	inserted, err = e.recordInDoubt(ctx, tx, effectRef{
		IdemKey: row.IdemKey, RunID: row.RunID, StepID: row.StepID, Action: row.Action, Attempt: attempt + 1,
	}, msg)
	return true, inserted, err
}
