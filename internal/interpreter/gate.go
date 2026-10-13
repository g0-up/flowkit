package interpreter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/interpreter/model"
	"github.com/g0-up/flowkit/internal/store"
)

// Route outcomes, one per definition and trigger event.
const (
	outcomeStarted           = "started"
	outcomeQueued            = "queued"
	outcomeDeduped           = "deduped"
	outcomeCancelledPrevious = "cancelled_previous"
	outcomeAlreadyRouted     = "already_routed"
	outcomeUnroutable        = "unroutable"
	outcomeSuperseded        = "superseded"
)

// trigger is a definition about to start a run for an event, with the
// version read in the routing transaction.
type trigger struct {
	def     store.DefinitionRow
	version store.DefinitionVersionRow
	event   store.EventRow
	info    EventInfo
}

// admit applies the definition's concurrency policy in tx, which holds the
// serial lock of key unless the policy is concurrent. It returns the outcome
// and the id of the run it started or queued.
//
// Triggers of a key are ordered by emit time, (inserted_at, id) of their
// events, not by the order they are routed in: the serial queue runs in
// emit order, cancel_previous never lets an older trigger replace a newer
// one, and the dedup window is measured back from the trigger's emit time.
func (in *Interpreter) admit(ctx context.Context, tx store.Tx, t trigger, key string) (string, ids.UUID, error) {
	name := t.def.Name
	runID := RunID(name, t.event.ID)
	switch t.def.ConcurrencyPolicy {
	case model.PolicyConcurrent:
		return outcomeStarted, runID, in.startRun(ctx, tx, t, nil, false)

	case model.PolicySerial:
		active, err := in.activeExclusive(ctx, tx, name, key)
		if err != nil {
			return "", ids.UUID{}, err
		}
		waiting, err := in.store.RunQueueNonEmpty(ctx, tx, name, key)
		if err != nil {
			return "", ids.UUID{}, err
		}
		if active == nil && !waiting {
			return outcomeStarted, runID, in.startRun(ctx, tx, t, &key, true)
		}
		// Queue with the waiting triggers, which run in emit order.
		if _, err := in.store.InsertRunQueue(ctx, tx, store.RunQueueRow{
			DefName: name, SerialKey: key, TriggerEventID: t.event.ID,
			EventAt: t.event.InsertedAt, CreatedAt: in.clock.Now(),
		}); err != nil {
			return "", ids.UUID{}, err
		}
		if active == nil {
			// The queue is stuck behind no run: start its head now.
			if _, _, err := in.PromoteQueued(ctx, tx, name, key); err != nil {
				return "", ids.UUID{}, err
			}
		}
		return outcomeQueued, runID, nil

	case model.PolicyCancelPrevious:
		// A run of a newer trigger, in any state, already replaced this
		// one. Every run is checked, not only the active one, so the
		// outcome stays the same when the event is routed again.
		newer, err := in.store.NewerRunExists(ctx, tx, name, key, t.event.InsertedAt, t.event.ID)
		if err != nil {
			return "", ids.UUID{}, err
		}
		if newer {
			return outcomeSuperseded, ids.UUID{}, nil
		}
		cancelled := false
		// A run promoted from a queue left by an earlier serial version
		// takes the old run's place; cancel until none is left.
		for {
			active, err := in.activeExclusive(ctx, tx, name, key)
			if err != nil {
				return "", ids.UUID{}, err
			}
			if active == nil {
				break
			}
			if err := in.FinishRun(ctx, tx, *active, Finish{
				State: stateCancelled, Name: TerminalCancelledByNewer, StepID: StepRun,
			}); err != nil {
				return "", ids.UUID{}, err
			}
			cancelled = true
		}
		if err := in.startRun(ctx, tx, t, &key, true); err != nil {
			return "", ids.UUID{}, err
		}
		if cancelled {
			return outcomeCancelledPrevious, runID, nil
		}
		return outcomeStarted, runID, nil

	case model.PolicyDedupWindow:
		if t.def.DedupWindowSeconds == nil || *t.def.DedupWindowSeconds <= 0 {
			return "", ids.UUID{}, fmt.Errorf("definition %q has policy %s without a window", name, model.PolicyDedupWindow)
		}
		window := time.Duration(*t.def.DedupWindowSeconds) * time.Second
		// Measured from the emit time, so routing late or again does not
		// move the window.
		recent, err := in.store.RecentRunExists(ctx, tx, name, key, t.event.InsertedAt.Add(-window))
		if err != nil {
			return "", ids.UUID{}, err
		}
		if recent {
			return outcomeDeduped, ids.UUID{}, nil
		}
		return outcomeStarted, runID, in.startRun(ctx, tx, t, &key, false)
	}
	return "", ids.UUID{}, fmt.Errorf("definition %q has unknown concurrency policy %q", name, t.def.ConcurrencyPolicy)
}

// activeExclusive locks and returns the non-terminal exclusive run of a
// key, or nil.
func (in *Interpreter) activeExclusive(ctx context.Context, tx store.Tx, defName, key string) (*store.RunRow, error) {
	r, err := in.store.LockActiveExclusiveRun(ctx, tx, defName, key)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// startRun creates the run of t pinned to t.version, records the trigger
// as consumed and enqueues the run's first pump, all in tx.
func (in *Interpreter) startRun(ctx context.Context, tx store.Tx, t trigger, key *string, exclusive bool) error {
	now := in.clock.Now()
	runID := RunID(t.def.Name, t.event.ID)
	scope := t.info.Scope
	r := store.RunRow{
		ID:               runID,
		DefName:          t.def.Name,
		DefVersion:       t.version.Version,
		MetamodelVersion: t.version.MetamodelVersion,
		LibVersion:       in.libVersion,
		Definition:       bytes.Clone(t.version.Body),
		TriggerEventID:   t.event.ID,
		State:            stateRunning,
		SerialKey:        key,
		Exclusive:        exclusive,
		Frames:           initialFrames,
		Facts:            json.RawMessage(`{}`),
		Rev:              1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if scope.EntityKind != "" {
		r.EntityKind, r.EntityID = &scope.EntityKind, &scope.EntityID
	}
	if len(scope.Extra) > 0 {
		extra, err := json.Marshal(scope.Extra)
		if err != nil {
			return fmt.Errorf("flowkit interpreter: encode scope: %w", err)
		}
		r.ScopeExtra = extra
	}
	if err := in.store.InsertRun(ctx, tx, r); err != nil {
		return err
	}
	if err := in.store.InsertConsumedEvent(ctx, tx, store.ConsumedEventRow{
		RunID: runID, EventID: t.event.ID, StepID: stepTrigger, At: now,
	}); err != nil {
		return err
	}
	if err := in.pumps.EnqueuePump(ctx, tx, runID); err != nil {
		return err
	}
	in.onCommit(tx, func() { in.runsStarted.Add(ctx, 1) })
	return nil
}

// PromoteQueued starts the run of the earliest emitted trigger queued under
// a serial key, in tx, unless the key has a non-terminal exclusive run or the
// definition is disabled or unpublished; then the queue stays as it is. It
// takes the key's serial lock, a no-op when tx holds it already, so it is
// safe to call at any time, and returns the id of the run it started.
//
// A queued trigger whose event no longer fits its registered type is
// dropped with a schema_mismatch dead letter, and the next one is tried.
func (in *Interpreter) PromoteQueued(ctx context.Context, tx store.Tx, defName, key string) (ids.UUID, bool, error) {
	if err := in.store.LockSerial(ctx, tx, defName, key); err != nil {
		return ids.UUID{}, false, err
	}
	active, err := in.activeExclusive(ctx, tx, defName, key)
	if err != nil || active != nil {
		return ids.UUID{}, false, err
	}
	def, err := in.store.GetDefinition(ctx, tx, defName)
	if errors.Is(err, store.ErrNotFound) {
		return ids.UUID{}, false, nil
	}
	if err != nil {
		return ids.UUID{}, false, err
	}
	if !def.Enabled || def.CurrentVersion == nil {
		return ids.UUID{}, false, nil
	}
	version, err := in.store.GetDefinitionVersion(ctx, tx, defName, *def.CurrentVersion)
	if err != nil {
		return ids.UUID{}, false, err
	}
	for {
		head, err := in.store.PeekRunQueue(ctx, tx, defName, key)
		if errors.Is(err, store.ErrNotFound) {
			return ids.UUID{}, false, nil
		}
		if err != nil {
			return ids.UUID{}, false, err
		}
		if _, err := in.store.DeleteRunQueue(ctx, tx, head); err != nil {
			return ids.UUID{}, false, err
		}
		runID := RunID(defName, head.TriggerEventID)
		exists, err := in.store.RunExists(ctx, tx, runID)
		if err != nil {
			return ids.UUID{}, false, err
		}
		if exists {
			continue
		}
		event, err := in.store.GetEvent(ctx, tx, head.TriggerEventID)
		if err != nil {
			return ids.UUID{}, false, err
		}
		info, err := in.events.Check(event.Type, event.Payload)
		if err != nil {
			if err := in.schemaMismatch(ctx, tx, event, err); err != nil {
				return ids.UUID{}, false, err
			}
			continue
		}
		// A promoted run stays exclusive whatever the current policy, so
		// its end promotes the rest of the queue.
		t := trigger{def: def, version: version, event: event, info: info}
		if err := in.startRun(ctx, tx, t, &head.SerialKey, true); err != nil {
			return ids.UUID{}, false, err
		}
		in.log.InfoContext(ctx, "flowkit: queued run promoted",
			"event_id", event.ID, "def_name", defName, "run_id", runID.String())
		return runID, true, nil
	}
}
