package interpreter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/g0-up/flowkit/internal/expr"
	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/interpreter/model"
	"github.com/g0-up/flowkit/internal/sched"
	"github.com/g0-up/flowkit/internal/store"
	"github.com/g0-up/flowkit/internal/testhooks"
)

// Route handles a route_event job. A failure to route the event to some
// definition fails the job so River retries it; the retry skips every
// definition the event was already routed to.
func (in *Interpreter) Route(ctx context.Context, args sched.RouteEventArgs) sched.Result {
	if err := in.RouteEvent(ctx, args.EventID); err != nil {
		return sched.Retry(err)
	}
	return sched.Done()
}

// RouteEvent starts, queues or skips a run of every enabled definition the
// stored event triggers, each definition in its own transaction, so one
// failing definition does not hold back the others, then offers the event
// to the waits subscribed to its type. Routing an event again changes
// nothing for the definitions it was routed to, nor for the waits it
// satisfied.
//
// An event that no longer fits its registered type gets a schema_mismatch
// dead letter and is not routed, to definitions or to waits.
func (in *Interpreter) RouteEvent(ctx context.Context, eventID int64) error {
	var (
		event store.EventRow
		defs  []store.DefinitionRow
	)
	err := in.inTx(ctx, func(tx store.Tx) error {
		var err error
		if event, err = in.store.GetEvent(ctx, tx, eventID); err != nil {
			return err
		}
		defs, err = in.store.ListTriggeredDefinitions(ctx, tx, event.Type)
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		// Only retention deletes an event, long after its routing.
		in.log.WarnContext(ctx, "flowkit: routed event no longer exists", "event_id", eventID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("flowkit interpreter: route event %d: %w", eventID, err)
	}
	info, cerr := in.events.Check(event.Type, event.Payload)
	if cerr != nil {
		err := in.inTx(ctx, func(tx store.Tx) error { return in.schemaMismatch(ctx, tx, event, cerr) })
		if err != nil {
			return fmt.Errorf("flowkit interpreter: route event %d: %w", eventID, err)
		}
		in.log.WarnContext(ctx, "flowkit: event does not fit its registered type; not routed",
			"event_id", eventID, "type", event.Type, "error", in.noteText(cerr))
		return nil
	}
	var errs []error
	for _, d := range defs {
		if err := in.routeDefinition(ctx, event, info, d.Name); err != nil {
			in.log.ErrorContext(ctx, "flowkit: route failed", "event_id", eventID, "def_name", d.Name, "error", err)
			errs = append(errs, fmt.Errorf("definition %q: %w", d.Name, err))
		}
	}
	if err := in.routeWaits(ctx, event); err != nil {
		errs = append(errs, fmt.Errorf("waits: %w", err))
	}
	if len(errs) > 0 {
		return fmt.Errorf("flowkit interpreter: route event %d: %w", eventID, errors.Join(errs...))
	}
	return nil
}

// routeDefinition routes the event to one definition in one transaction.
func (in *Interpreter) routeDefinition(ctx context.Context, event store.EventRow, info EventInfo, name string) error {
	var (
		outcome string
		runID   ids.UUID
	)
	err := in.inTx(ctx, func(tx store.Tx) error {
		var err error
		outcome, runID, err = in.routeInTx(ctx, tx, event, info, name)
		return err
	})
	if err != nil || outcome == "" {
		return err
	}
	if outcome == outcomeStarted {
		testhooks.Reach(testhooks.PointRouted)
	}
	attrs := []any{"event_id", event.ID, "def_name", name, "outcome", outcome}
	if runID != (ids.UUID{}) {
		attrs = append(attrs, "run_id", runID.String())
	}
	in.log.InfoContext(ctx, "flowkit: event routed", attrs...)
	in.routes.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
		attribute.String("def", name), attribute.String("outcome", outcome))))
	return nil
}

// routeInTx returns the outcome, or "" when the definition stopped matching
// the event since it was listed.
func (in *Interpreter) routeInTx(ctx context.Context, tx store.Tx, event store.EventRow, info EventInfo, name string) (string, ids.UUID, error) {
	def, err := in.store.GetDefinition(ctx, tx, name)
	if err != nil {
		return "", ids.UUID{}, err
	}
	if !def.Enabled || def.CurrentVersion == nil || def.TriggerEventType == nil || *def.TriggerEventType != event.Type {
		return "", ids.UUID{}, nil
	}
	runID := RunID(name, event.ID)
	if routed, err := in.alreadyRouted(ctx, tx, name, runID, event.ID); err != nil || routed {
		return outcomeAlreadyRouted, runID, err
	}
	version, err := in.store.GetDefinitionVersion(ctx, tx, name, *def.CurrentVersion)
	if err != nil {
		return "", ids.UUID{}, err
	}
	t := trigger{def: def, version: version, event: event, info: info}
	var key string
	if def.ConcurrencyPolicy != model.PolicyConcurrent {
		var kerr error
		key, kerr = in.serialKey(ctx, t)
		if kerr != nil {
			if ctx.Err() != nil {
				// The job is ending, not the key failing.
				return "", ids.UUID{}, ctx.Err()
			}
			inserted, err := in.unroutable(ctx, tx, t, kerr)
			if err != nil || !inserted {
				// An open dead letter means an earlier route got here.
				return outcomeAlreadyRouted, ids.UUID{}, err
			}
			in.log.WarnContext(ctx, "flowkit: serial key failed; event not routed to definition",
				"event_id", event.ID, "def_name", name, "error", in.noteText(kerr))
			return outcomeUnroutable, ids.UUID{}, nil
		}
		if err := in.store.LockSerial(ctx, tx, name, key); err != nil {
			return "", ids.UUID{}, err
		}
		// A concurrent route of this event may have committed while the
		// lock was awaited.
		if routed, err := in.alreadyRouted(ctx, tx, name, runID, event.ID); err != nil || routed {
			return outcomeAlreadyRouted, runID, err
		}
	}
	return in.admit(ctx, tx, t, key)
}

// alreadyRouted reports whether the event started a run of the definition
// or waits in its queue.
func (in *Interpreter) alreadyRouted(ctx context.Context, tx store.Tx, name string, runID ids.UUID, eventID int64) (bool, error) {
	exists, err := in.store.RunExists(ctx, tx, runID)
	if err != nil || exists {
		return exists, err
	}
	return in.store.QueuedEventExists(ctx, tx, name, eventID)
}

// serialKeyProgram is the compiled serial key of one definition version.
type serialKeyProgram struct {
	version int
	source  string
	prog    *expr.Program
}

// serialKey evaluates the definition's serial key on the trigger payload.
// Every error makes the event unroutable for the definition.
func (in *Interpreter) serialKey(ctx context.Context, t trigger) (string, error) {
	if t.def.SerialKeyCEL == nil || *t.def.SerialKeyCEL == "" {
		return "", fmt.Errorf("policy %s needs a serial key, and the definition has none", t.def.ConcurrencyPolicy)
	}
	prog, err := in.serialKeyProgram(t)
	if err != nil {
		return "", err
	}
	payload, err := expr.Activation(t.event.Payload, t.info.Shape)
	if err != nil {
		return "", fmt.Errorf("serial key: %w", err)
	}
	v, err := prog.Eval(ctx, expr.Vars{Event: payload, Now: in.clock.Now(), Scope: t.info.Scope})
	if err != nil {
		return "", fmt.Errorf("serial key: %w", err)
	}
	key, ok := v.Value().(string)
	switch {
	case !ok:
		return "", fmt.Errorf("serial key is %s, not a string", v.Type().TypeName())
	case key == "":
		return "", errors.New("serial key is empty")
	case len(key) > maxSerialKeyBytes:
		return "", fmt.Errorf("serial key is %d bytes, more than %d", len(key), maxSerialKeyBytes)
	case strings.ContainsRune(key, 0):
		return "", errors.New("serial key has a U+0000 character, which Postgres cannot store")
	}
	return key, nil
}

// serialKeyProgram compiles the serial key of t's definition version once
// per version.
func (in *Interpreter) serialKeyProgram(t trigger) (*expr.Program, error) {
	src := *t.def.SerialKeyCEL
	in.keysMu.Lock()
	cached, ok := in.keys[t.def.Name]
	in.keysMu.Unlock()
	if ok && cached.version == t.version.Version && cached.source == src {
		return cached.prog, nil
	}
	if t.info.Shape == nil {
		return nil, fmt.Errorf("serial key: the payload schema of %q cannot type expressions", t.event.Type)
	}
	mm, err := expr.Metamodel(t.version.MetamodelVersion)
	if err != nil {
		return nil, fmt.Errorf("serial key: %w", err)
	}
	env, err := expr.NewEventEnv(t.info.Shape, mm)
	if err != nil {
		return nil, fmt.Errorf("serial key: %w", err)
	}
	prog, errs := env.Compile(src, expr.WantString)
	if len(errs) > 0 {
		return nil, fmt.Errorf("serial key does not compile: %w", errors.Join(errs...))
	}
	in.keysMu.Lock()
	in.keys[t.def.Name] = serialKeyProgram{version: t.version.Version, source: src, prog: prog}
	in.keysMu.Unlock()
	return prog, nil
}

// unroutable records in tx that the event cannot start a run of the
// definition. An open dead letter for the same event and definition is
// kept as it is; then it reports false.
func (in *Interpreter) unroutable(ctx context.Context, tx store.Tx, t trigger, cause error) (bool, error) {
	raw, err := json.Marshal(map[string]any{
		"event_id": t.event.ID, "event_type": t.event.Type, "def_name": t.def.Name, "def_version": t.version.Version,
	})
	if err != nil {
		return false, fmt.Errorf("flowkit interpreter: encode dead letter: %w", err)
	}
	note := in.noteText(cause)
	eventID := t.event.ID
	_, inserted, err := in.store.InsertDeadLetter(ctx, tx, store.DeadLetterRow{
		Kind: deadLetterUnroutable, Ref: strconv.FormatInt(t.event.ID, 10) + ":" + t.def.Name,
		EventID: &eventID, Raw: raw, Note: &note, CreatedAt: in.clock.Now(),
	})
	if err == nil && inserted {
		in.countDeadLetter(ctx, tx, deadLetterUnroutable)
	}
	return inserted, err
}

// schemaMismatch records in tx that the event no longer fits its type.
func (in *Interpreter) schemaMismatch(ctx context.Context, tx store.Tx, event store.EventRow, cause error) error {
	raw, err := json.Marshal(map[string]any{"event_id": event.ID, "event_type": event.Type})
	if err != nil {
		return fmt.Errorf("flowkit interpreter: encode dead letter: %w", err)
	}
	note := in.noteText(cause)
	eventID := event.ID
	_, inserted, err := in.store.InsertDeadLetter(ctx, tx, store.DeadLetterRow{
		Kind: deadLetterSchemaMismatch, Ref: strconv.FormatInt(event.ID, 10),
		EventID: &eventID, Raw: raw, Note: &note, CreatedAt: in.clock.Now(),
	})
	if err == nil && inserted {
		in.countDeadLetter(ctx, tx, deadLetterSchemaMismatch)
	}
	return err
}

// routeWaits offers the event to every wait subscribed to its type whose
// run has not consumed it. It reads the waits in pages of run id order, each
// page in its own transaction, and each offer runs in its own transactions:
// a wait whose match fails records match_error and the others still get the
// event. An error of one offer does not stop the others; they are joined so
// the job retries, and a retry finds the claimed waits gone.
func (in *Interpreter) routeWaits(ctx context.Context, event store.EventRow) error {
	var (
		errs  []error
		after ids.UUID
	)
	for {
		var page []store.WaitingRun
		err := in.inTx(ctx, func(tx store.Tx) error {
			var err error
			page, err = in.store.ListEventSubscriptions(ctx, tx, store.SubscriptionQuery{
				EventType: event.Type, EventID: event.ID, AfterRunID: after, Limit: in.subsPage,
			})
			return err
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("list subscriptions: %w", err))
			return errors.Join(errs...)
		}
		for _, w := range page {
			if err := in.offer(ctx, event, w); err != nil {
				in.log.ErrorContext(ctx, "flowkit: wait claim failed",
					"event_id", event.ID, "run_id", w.Run.ID.String(), "step_id", w.Sub.StepID, "error", err)
				errs = append(errs, fmt.Errorf("run %s: %w", w.Run.ID, err))
			}
		}
		if len(page) < in.subsPage {
			return errors.Join(errs...)
		}
		after = page[len(page)-1].Sub.RunID
	}
}

// offer claims one subscribed wait with the event when the event is
// eligible for it and its match accepts the event. w.Run lacks the pinned
// definition; offer reads it only when the definition version is not
// compiled yet. A definition that does not compile fails the run, as every
// other path does.
func (in *Interpreter) offer(ctx context.Context, event store.EventRow, w store.WaitingRun) error {
	if !in.eligibility(w.Run, w.Sub.EventType).admits(event) {
		return nil
	}
	run := w.Run
	if !in.planned(run) {
		var gone bool
		err := in.inTx(ctx, func(tx store.Tx) error {
			var err error
			run, err = in.store.GetRun(ctx, tx, w.Run.ID)
			if errors.Is(err, store.ErrNotFound) {
				gone = true
				return nil
			}
			return err
		})
		if err != nil {
			return err
		}
		if gone || run.State != stateWaiting || run.WaitSeq != w.Sub.WaitSeq {
			// The wait ended since it was listed.
			return nil
		}
	}
	s, sp, scope, err := in.waitPlan(run, w.Sub.StepID)
	if errors.Is(err, errPlan) {
		return in.planFailed(ctx, run, err).Err()
	}
	if err != nil {
		return err
	}
	ok, err := in.match(ctx, sp, event, scope)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var recorded bool
		if err := in.inTx(ctx, func(tx store.Tx) error {
			var rerr error
			recorded, rerr = in.recordMatchError(ctx, tx, run.ID, w.Sub.StepID, w.Sub.WaitSeq, event.ID, err)
			return rerr
		}); err != nil {
			return err
		}
		if recorded {
			in.countMatchErrors(ctx, run, w.Sub.StepID, 1)
		}
		return nil
	}
	if !ok {
		return nil
	}
	_, err = in.claimOnce(ctx, claim{run: run, stepID: w.Sub.StepID, wait: s.Wait, waitSeq: w.Sub.WaitSeq, event: &event})
	return err
}
