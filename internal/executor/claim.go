package executor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/g0-up/flowkit/internal/store"
)

// decide applies the claim rules to an existing effect. final false means the
// effect is free to claim.
func (e *Executor) decide(a Action, row store.EffectRow, now time.Time) (ExecOutcome, bool) {
	switch {
	case row.DoneAt != nil:
		return cachedOutcome(a, row), true
	case row.InDoubt:
		return ExecOutcome{Kind: BlockedInDoubt}, true
	case row.ClaimedAt == nil:
		if p := a.Retry.withDefaults(); row.Attempt >= p.MaxAttempts {
			return ExecOutcome{Kind: Failed, Err: exhaustedError(a.Name, row.Attempt, lastError(row))}, true
		}
		return ExecOutcome{}, false
	}
	if age := now.Sub(*row.ClaimedAt); age < e.claimTimeout {
		return ExecOutcome{Kind: RetryLater, Delay: withJitter(e.claimTimeout-age, e.rand)}, true
	}
	// An expired claim is the janitor's to resolve.
	return ExecOutcome{Kind: BlockedInDoubt}, true
}

func cachedOutcome(a Action, row store.EffectRow) ExecOutcome {
	if row.Result == nil {
		return ExecOutcome{Kind: Done, Cached: true, Err: lastError(row)}
	}
	out, err := decodeOutput(a.OutputFacts, row.Result)
	if err != nil {
		return ExecOutcome{Kind: Done, Cached: true, Err: fmt.Errorf("action %s: stored %w", a.Name, err)}
	}
	return ExecOutcome{Kind: Done, Cached: true, Output: out}
}

func lastError(row store.EffectRow) error {
	if row.LastError != nil {
		return storedError(row.Action, *row.LastError, nil)
	}
	return fmt.Errorf("action %s: effect recorded without output", row.Action)
}

func exhaustedError(action string, attempts int, last error) error {
	return fmt.Errorf("action %s failed %d times: %w", action, attempts, last)
}

func (e *Executor) readEffect(ctx context.Context, idemKey string) (row store.EffectRow, found bool, err error) {
	err = e.inTx(ctx, func(tx store.Tx) error {
		r, err := e.store.GetEffect(ctx, tx, idemKey)
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		row, found = r, err == nil
		return err
	})
	return row, found, err
}

// claim takes the effect for c.token. claimed false comes with the outcome to
// return instead.
func (e *Executor) claim(ctx context.Context, c *call) (out ExecOutcome, claimed bool) {
	req := c.ExecRequest
	err := e.inTx(ctx, func(tx store.Tx) error {
		now := e.clock.Now()
		ins, err := e.store.InsertEffectClaim(ctx, tx, store.EffectClaim{
			IdemKey: req.IdemKey, RunID: req.RunID, StepID: req.StepID, Action: req.Action.Name, Token: c.token, At: now,
		})
		if err != nil || ins {
			claimed = ins
			return err
		}
		row, err := e.store.GetEffect(ctx, tx, req.IdemKey)
		if errors.Is(err, store.ErrNotFound) {
			out = e.contended()
			return nil
		}
		if err != nil {
			return err
		}
		if o, final := e.decide(req.Action, row, now); final {
			out = o
			return nil
		}
		ok, err := e.store.ReclaimEffect(ctx, tx, req.IdemKey, c.token, now, req.Action.Retry.withDefaults().MaxAttempts)
		if err != nil || ok {
			claimed = ok
			return err
		}
		// The effect changed after it was read, for example another request
		// used up its last attempt: decide again on what it is now.
		if row, err = e.store.GetEffect(ctx, tx, req.IdemKey); err != nil {
			return err
		}
		if o, final := e.decide(req.Action, row, now); final {
			out = o
			return nil
		}
		out = e.contended()
		return nil
	})
	if err != nil {
		return e.storeFailure(ctx, err), false
	}
	return out, claimed
}

func (e *Executor) contended() ExecOutcome {
	return ExecOutcome{Kind: RetryLater, Delay: withJitter(contendedRetryDelay, e.rand)}
}
