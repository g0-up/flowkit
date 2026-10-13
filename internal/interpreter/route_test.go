package interpreter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/g0-up/flowkit/internal/interpreter/model"
	"github.com/g0-up/flowkit/internal/sched"
)

func TestRouteStartsARunPinnedToTheCurrentVersion(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "enrol", policy: model.PolicyConcurrent})
	eventID := e.emitType(testEvent, `{"key":"k","student":7,"branch":"hn"}`)

	if r := e.in.Route(context.Background(), sched.RouteEventArgs{EventID: eventID}); r != sched.Done() {
		t.Fatalf("Route = %+v, want Done", r)
	}

	runID := RunID("enrol", eventID)
	r := e.run(runID)
	switch {
	case r.DefName != "enrol" || r.DefVersion != 1 || r.MetamodelVersion != 1:
		t.Errorf("run definition = %s v%d mm%d", r.DefName, r.DefVersion, r.MetamodelVersion)
	case r.LibVersion != "v0.0.0-test":
		t.Errorf("lib version = %q", r.LibVersion)
	case r.TriggerEventID != eventID || r.State != stateRunning || r.Terminal || r.Rev != 1:
		t.Errorf("run = trigger %d state %s terminal %v rev %d", r.TriggerEventID, r.State, r.Terminal, r.Rev)
	case r.SerialKey != nil || r.Exclusive:
		t.Errorf("concurrent run has serial key %v, exclusive %v", r.SerialKey, r.Exclusive)
	case string(r.Frames) != `{"stack": [], "visits": {}}` || string(r.Facts) != `{}`:
		t.Errorf("frames %s facts %s", r.Frames, r.Facts)
	case r.EntityKind == nil || *r.EntityKind != "student" || r.EntityID == nil || *r.EntityID != "7":
		t.Errorf("entity = %v/%v", r.EntityKind, r.EntityID)
	case !r.CreatedAt.Equal(t0) || !r.UpdatedAt.Equal(t0):
		t.Errorf("times = %v %v, want the clock's", r.CreatedAt, r.UpdatedAt)
	}
	var extra map[string]string
	if err := json.Unmarshal(r.ScopeExtra, &extra); err != nil || extra["branch"] != "hn" {
		t.Errorf("scope extra = %s (%v)", r.ScopeExtra, err)
	}
	if !strings.Contains(string(r.Definition), `"v1"`) {
		t.Errorf("run body is not version 1: %s", r.Definition)
	}
	if n := e.count(`SELECT count(*) FROM {s}consumed_events WHERE run_id = $1 AND event_id = $2 AND step_id = '$trigger'`,
		runID, eventID); n != 1 {
		t.Errorf("consumed $trigger rows = %d, want 1", n)
	}
	if n := e.pumps.count(runID); n != 1 {
		t.Errorf("pumps enqueued = %d, want 1", n)
	}
	if n := e.meter.count("enrol", outcomeStarted); n != 1 {
		t.Errorf("started outcomes = %d, want 1", n)
	}
}

func TestRouteAgainChangesNothing(t *testing.T) {
	for _, policy := range []string{model.PolicyConcurrent, model.PolicySerial, model.PolicyCancelPrevious, model.PolicyDedupWindow} {
		t.Run(policy, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			e.publish(defSpec{name: "enrol", policy: policy, serialKey: "event.key", window: 3600})
			eventID := e.emit("a")
			for range 3 {
				e.route(eventID)
			}
			runID := RunID("enrol", eventID)
			r := e.run(runID)
			if r.State != stateRunning || r.Terminal {
				t.Errorf("run state = %s terminal %v; a route again must not end its own run", r.State, r.Terminal)
			}
			if n := e.count(`SELECT count(*) FROM {s}runs`); n != 1 {
				t.Errorf("runs = %d, want 1", n)
			}
			if n := e.pumps.count(runID); n != 1 {
				t.Errorf("pumps enqueued = %d, want 1", n)
			}
			if n := e.meter.count("enrol", outcomeStarted); n != 1 {
				t.Errorf("started = %d, want 1", n)
			}
			if n := e.meter.count("enrol", outcomeAlreadyRouted); n != 2 {
				t.Errorf("already_routed = %d, want 2", n)
			}
			for _, o := range []string{outcomeDeduped, outcomeCancelledPrevious, outcomeQueued} {
				if n := e.meter.count("enrol", o); n != 0 {
					t.Errorf("%s = %d, want 0", o, n)
				}
			}
		})
	}
}

func TestRouteAgainOfAQueuedEventChangesNothing(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "enrol", policy: model.PolicySerial, serialKey: "event.key"})
	first, second := e.emit("a"), e.emit("a")
	e.route(first)
	for range 3 {
		e.route(second)
	}
	if n := e.count(`SELECT count(*) FROM {s}run_queue WHERE trigger_event_id = $1`, second); n != 1 {
		t.Fatalf("queued rows = %d, want 1", n)
	}
	if e.runExists(RunID("enrol", second)) {
		t.Fatal("queued event started a run")
	}
	if n := e.meter.count("enrol", outcomeQueued); n != 1 {
		t.Errorf("queued = %d, want 1", n)
	}

	// Promoted, the event's run exists, so a route again still does nothing.
	e.finish(RunID("enrol", first))
	e.route(second)
	if n := e.count(`SELECT count(*) FROM {s}runs`); n != 2 {
		t.Errorf("runs = %d, want 2", n)
	}
	if n := e.count(`SELECT count(*) FROM {s}run_queue`); n != 0 {
		t.Errorf("queued rows = %d, want 0", n)
	}
}

func TestPublishBetweenRoutesStartsNoSecondRun(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "enrol", policy: model.PolicyConcurrent})
	eventID := e.emit("a")
	e.route(eventID)
	if v := e.publish(defSpec{name: "enrol", policy: model.PolicyConcurrent}); v != 2 {
		t.Fatalf("published version %d, want 2", v)
	}
	e.route(eventID)
	if n := e.count(`SELECT count(*) FROM {s}runs`); n != 1 {
		t.Fatalf("runs = %d, want 1", n)
	}
	if r := e.run(RunID("enrol", eventID)); r.DefVersion != 1 {
		t.Errorf("run version = %d, want 1", r.DefVersion)
	}
	// A new event runs the new version.
	next := e.emit("b")
	e.route(next)
	if r := e.run(RunID("enrol", next)); r.DefVersion != 2 {
		t.Errorf("new run version = %d, want 2", r.DefVersion)
	}
}

func TestRouteSkipsDisabledAndOtherDefinitions(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "off", policy: model.PolicyConcurrent, disabled: true})
	e.publish(defSpec{name: "on", policy: model.PolicyConcurrent})
	eventID := e.emit("a")
	e.route(eventID)
	if e.runExists(RunID("off", eventID)) {
		t.Error("disabled definition started a run")
	}
	if !e.runExists(RunID("on", eventID)) {
		t.Error("enabled definition did not start a run")
	}
}

func TestRouteOfAMissingEventIsDone(t *testing.T) {
	e := newEnv(t)
	if err := e.in.RouteEvent(context.Background(), 987654); err != nil {
		t.Fatalf("RouteEvent = %v, want nil", err)
	}
}

func TestEventThatNoLongerFitsGetsOneSchemaMismatchDeadLetter(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "enrol", policy: model.PolicyConcurrent})
	eventID := e.emitType(testEvent, `{"key":"a","misfit":true}`)
	e.route(eventID)
	e.route(eventID)
	if n := e.count(`SELECT count(*) FROM {s}runs`); n != 0 {
		t.Errorf("runs = %d, want 0", n)
	}
	if n := e.count(`SELECT count(*) FROM {s}dead_letters WHERE kind = 'schema_mismatch' AND ref = $1 AND event_id = $2`,
		strconv.FormatInt(eventID, 10), eventID); n != 1 {
		t.Errorf("schema_mismatch dead letters = %d, want 1", n)
	}
	var note string
	if err := e.pool.QueryRow(context.Background(), e.sql(`SELECT note FROM {s}dead_letters`)).Scan(&note); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(note, "redacted: ") {
		t.Errorf("note %q was not redacted", note)
	}
}

func TestBadSerialKeyGetsOneUnroutableDeadLetterPerDefinition(t *testing.T) {
	for name, c := range map[string]struct{ payload, key string }{
		"empty":   {`{"key":""}`, "event.key"},
		"missing": {`{}`, "event.key"},
		"long":    {fmt.Sprintf(`{"key":%q}`, strings.Repeat("k", maxSerialKeyBytes+1)), "event.key"},
		"nul":     {`{"key":"a"}`, `event.key + "\x00"`},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			e.publish(defSpec{name: "keyed", policy: model.PolicySerial, serialKey: c.key})
			e.publish(defSpec{name: "free", policy: model.PolicyConcurrent})
			eventID := e.emitType(testEvent, c.payload)
			e.route(eventID)
			e.route(eventID)
			if e.runExists(RunID("keyed", eventID)) {
				t.Error("unroutable event started a run")
			}
			if !e.runExists(RunID("free", eventID)) {
				t.Error("an unroutable definition held back another one")
			}
			ref := strconv.FormatInt(eventID, 10) + ":keyed"
			if n := e.count(`SELECT count(*) FROM {s}dead_letters WHERE kind = 'unroutable_event' AND ref = $1 AND resolved_at IS NULL`,
				ref); n != 1 {
				t.Errorf("unroutable dead letters = %d, want 1", n)
			}
			if n := e.count(`SELECT count(*) FROM {s}dead_letters`); n != 1 {
				t.Errorf("dead letters = %d, want 1", n)
			}
			if n := e.meter.count("keyed", outcomeUnroutable); n != 1 {
				t.Errorf("unroutable outcomes = %d, want 1", n)
			}
			if n := e.meter.count("keyed", outcomeAlreadyRouted); n != 1 {
				t.Errorf("already_routed outcomes = %d, want 1: the open dead letter records the first route", n)
			}
		})
	}
}

func TestFailingDefinitionDoesNotHoldBackOthersAndRetries(t *testing.T) {
	e := newEnv(t)
	// A dedup_window definition without a window cannot be routed.
	e.publish(defSpec{name: "broken", policy: model.PolicyDedupWindow, serialKey: "event.key"})
	e.publish(defSpec{name: "fine", policy: model.PolicyConcurrent})
	eventID := e.emit("a")

	err := e.in.RouteEvent(context.Background(), eventID)
	if err == nil || !strings.Contains(err.Error(), `"broken"`) {
		t.Fatalf("RouteEvent = %v, want an error naming the failing definition", err)
	}
	if r := e.in.Route(context.Background(), sched.RouteEventArgs{EventID: eventID}); r == sched.Done() {
		t.Error("Route = Done, want a retry")
	}
	if !e.runExists(RunID("fine", eventID)) {
		t.Error("failing definition held back another one")
	}
	if n := e.meter.count("fine", outcomeStarted); n != 1 {
		t.Errorf("fine started = %d, want 1", n)
	}
}

func TestFailedRouteRollsBackAndRetries(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "enrol", policy: model.PolicySerial, serialKey: "event.key"})
	eventID := e.emit("a")

	e.pumps.fail = errInjected
	if err := e.in.RouteEvent(context.Background(), eventID); !errors.Is(err, errInjected) {
		t.Fatalf("RouteEvent = %v, want the pump failure", err)
	}
	if e.runExists(RunID("enrol", eventID)) {
		t.Fatal("run of a failed route was committed")
	}
	if n := e.count(`SELECT count(*) FROM {s}consumed_events`); n != 0 {
		t.Fatalf("consumed rows of a failed route = %d", n)
	}

	e.pumps.fail = nil
	e.route(eventID)
	if !e.runExists(RunID("enrol", eventID)) {
		t.Fatal("retried route did not start the run")
	}
}
