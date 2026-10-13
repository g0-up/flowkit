package interpreter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/g0-up/flowkit/internal/interpreter/model"
	"github.com/g0-up/flowkit/internal/obs"
	"github.com/g0-up/flowkit/internal/store"
)

// Step kinds of claims.
const (
	stepClaimed   = "claimed"
	stepClaimLost = "claim_lost"
)

// errAlreadyConsumed reports that the run consumed the event in another
// transaction; the claim's transaction must be rolled back.
var errAlreadyConsumed = errors.New("flowkit interpreter: the run already consumed the event")

// claim is one attempt to resume a run waiting on a wait step, by an event
// or by the wait's timeout. The route of new events, the look-back and the
// timeout worker all claim through it; the rev and wait sequence the
// attempt read decide which one wins. A claim only records the resumption,
// it runs no effect, so a disabled definition does not block it.
type claim struct {
	run     store.RunRow // as read; its Rev must still hold
	stepID  string
	wait    *model.WaitStep // nil only when recording a lost claim
	waitSeq int
	event   *store.EventRow // nil for the timeout
}

func (c claim) via() string {
	if c.event == nil {
		return "timeout"
	}
	return "event " + strconv.FormatInt(c.event.ID, 10)
}

func (c claim) snapshot(next string) json.RawMessage {
	m := map[string]any{"wait_seq": c.waitSeq}
	if c.event != nil {
		m["event_id"] = c.event.ID
	}
	if next != "" {
		m["next"] = next
	}
	b, _ := json.Marshal(m) // ints and strings always encode
	return b
}

// claimInTx tries c in tx. The winner moves the run to the wait's next
// step (or its on_timeout step), consumes the event, drops the
// subscription and enqueues a pump. A loser records claim_lost in tx and
// gets false. When the run consumed the event concurrently, claimInTx
// returns errAlreadyConsumed and tx must be rolled back.
func (in *Interpreter) claimInTx(ctx context.Context, tx store.Tx, c claim) (bool, error) {
	fr, err := decodeFrames(c.run.Frames)
	if err != nil {
		return false, fmt.Errorf("flowkit interpreter: claim run %s: %w", c.run.ID, err)
	}
	next := c.wait.Next
	if c.event == nil {
		next = c.wait.OnTimeout
	}
	fr.moveTo(next)
	raw, err := fr.encode()
	if err != nil {
		return false, fmt.Errorf("flowkit interpreter: claim run %s: %w", c.run.ID, err)
	}
	now := in.clock.Now()
	won, err := in.store.ClaimRun(ctx, tx, store.ClaimRow{
		ID: c.run.ID, Rev: c.run.Rev, WaitStep: c.stepID, WaitSeq: c.waitSeq, Frames: raw, At: now,
	})
	if err != nil {
		return false, err
	}
	if !won {
		return false, in.recordClaimLost(ctx, tx, c)
	}
	if c.event != nil {
		fresh, err := in.store.InsertConsumedEventIfAbsent(ctx, tx, store.ConsumedEventRow{
			RunID: c.run.ID, EventID: c.event.ID, StepID: c.stepID, At: now,
		})
		if err != nil {
			return false, err
		}
		if !fresh {
			return false, errAlreadyConsumed
		}
	}
	if err := in.store.DeleteSubscription(ctx, tx, c.run.ID, c.waitSeq); err != nil {
		return false, err
	}
	detail := c.via()
	if _, err := in.store.AppendStep(ctx, tx, store.StepRow{
		RunID: c.run.ID, At: now, StepID: c.stepID, Kind: stepClaimed, Detail: &detail, Snapshot: c.snapshot(next),
	}); err != nil {
		return false, err
	}
	return true, in.pumps.EnqueuePump(ctx, tx, c.run.ID)
}

// claimOnce tries c in its own transaction.
func (in *Interpreter) claimOnce(ctx context.Context, c claim) (bool, error) {
	var won bool
	err := in.inTx(ctx, func(tx store.Tx) error {
		var err error
		won, err = in.claimInTx(ctx, tx, c)
		return err
	})
	if errors.Is(err, errAlreadyConsumed) {
		return false, in.inTx(ctx, func(tx store.Tx) error { return in.recordClaimLost(ctx, tx, c) })
	}
	if err == nil && won {
		in.logClaim(ctx, c)
	}
	return won, err
}

// logClaim logs a committed claim and, for an event's, records how long
// the event took to wake the run.
func (in *Interpreter) logClaim(ctx context.Context, c claim) {
	if c.event != nil {
		obs.RecordDelay(ctx, in.wakeLatency, in.clock.Now().Sub(c.event.InsertedAt))
	}
	in.log.InfoContext(ctx, "flowkit: wait claimed",
		"run_id", c.run.ID.String(), "step_id", c.stepID, "wait_seq", c.waitSeq, "via", c.via())
}

// recordClaimLost records in tx that c found the wait gone. It does not
// change the run's rev.
func (in *Interpreter) recordClaimLost(ctx context.Context, tx store.Tx, c claim) error {
	detail := c.via()
	_, err := in.store.AppendStep(ctx, tx, store.StepRow{
		RunID: c.run.ID, At: in.clock.Now(), StepID: c.stepID, Kind: stepClaimLost, Detail: &detail, Snapshot: c.snapshot(""),
	})
	return err
}
