package interpreter

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/interpreter/model"
	"github.com/g0-up/flowkit/internal/store"
)

const raceRounds = 200

// TestConcurrentTriggersRespectThePolicy routes two triggers of one key at
// once, plus a duplicate route of the first, while another goroutine keeps
// promoting the key's queue as the janitor does.
func TestConcurrentTriggersRespectThePolicy(t *testing.T) {
	for _, policy := range []string{model.PolicyConcurrent, model.PolicySerial, model.PolicyCancelPrevious, model.PolicyDedupWindow} {
		t.Run(policy, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			const def = "enrol"
			e.publish(defSpec{name: def, policy: policy, serialKey: "event.key", window: 3600})
			ctx := context.Background()

			var current atomic.Pointer[string]
			current.Store(new(string))
			stop := make(chan struct{})
			promoterDone := make(chan struct{})
			go func() {
				defer close(promoterDone)
				for {
					select {
					case <-stop:
						return
					default:
					}
					if key := *current.Load(); key != "" {
						if err := e.in.inTx(ctx, func(tx store.Tx) error {
							_, _, err := e.in.PromoteQueued(ctx, tx, def, key)
							return err
						}); err != nil {
							t.Errorf("promote %s: %v", key, err)
							return
						}
					}
					time.Sleep(200 * time.Microsecond)
				}
			}()
			defer func() {
				close(stop)
				<-promoterDone
			}()

			for round := range raceRounds {
				key := fmt.Sprintf("r%03d", round)
				current.Store(&key)
				a, b := e.emit(key), e.emit(key)
				var wg sync.WaitGroup
				for _, id := range []int64{a, b, a} {
					wg.Go(func() { e.routeRacing(policy, id) })
				}
				wg.Wait()
				if t.Failed() {
					return
				}
				e.checkRound(policy, def, key, a, b)
				if t.Failed() {
					t.Fatalf("round %d broke the %s policy", round, policy)
				}
			}
		})
	}
}

// routeRacing routes an event that other routes of its key race with. The
// serial lock orders every route of a gated policy, so none may fail. Under
// the concurrent policy two routes of one event may conflict on the run's
// primary key, and only that conflict is retried, as the route job would.
// A deadlock or serialization failure fails the test under every policy.
func (e *env) routeRacing(policy string, eventID int64) {
	var err error
	for range 3 {
		err = e.in.RouteEvent(context.Background(), eventID)
		var pgErr *pgconn.PgError
		if err == nil || policy != model.PolicyConcurrent || !errors.As(err, &pgErr) || pgErr.Code != "23505" {
			break
		}
	}
	if err != nil {
		e.t.Errorf("route event %d under %s: %v", eventID, policy, err)
	}
}

func (e *env) checkRound(policy, def, key string, a, b int64) {
	t := e.t
	runs := func(where string) int {
		return e.count(`SELECT count(*) FROM {s}runs WHERE def_name = $1 AND trigger_event_id IN ($2, $3) AND `+where, def, a, b)
	}
	switch policy {
	case model.PolicyConcurrent:
		if n := runs("true"); n != 2 {
			t.Errorf("runs = %d, want 2", n)
		}
	case model.PolicySerial:
		if n := runs("true"); n != 1 {
			t.Fatalf("runs = %d, want 1", n)
		}
		if n := e.count(`SELECT count(*) FROM {s}run_queue WHERE def_name = $1 AND serial_key = $2`, def, key); n != 1 {
			t.Fatalf("queued = %d, want 1", n)
		}
		first, second := RunID(def, a), RunID(def, b)
		if !e.runExists(first) {
			first, second = second, first
		}
		e.finish(first)
		r := e.run(second)
		if r.State != stateRunning || !r.Exclusive || r.SerialKey == nil || *r.SerialKey != key {
			t.Errorf("promoted run = %s exclusive %v key %v", r.State, r.Exclusive, r.SerialKey)
		}
		if n := e.count(`SELECT count(*) FROM {s}run_queue WHERE def_name = $1 AND serial_key = $2`, def, key); n != 0 {
			t.Errorf("queued after the first run ended = %d, want 0", n)
		}
	case model.PolicyCancelPrevious:
		// b was emitted after a: b runs, and a was either cancelled by b or,
		// routed after b, superseded without a run.
		if r := e.run(RunID(def, b)); r.State != stateRunning {
			t.Errorf("run of the newer trigger is %s, want running", r.State)
		}
		if n := runs("NOT terminal"); n != 1 {
			t.Errorf("non-terminal runs = %d, want 1", n)
		}
		if e.runExists(RunID(def, a)) {
			if r := e.run(RunID(def, a)); r.State != stateCancelled || r.TerminalName == nil || *r.TerminalName != TerminalCancelledByNewer {
				t.Errorf("run of the older trigger is %s, want cancelled by the newer", r.State)
			}
		}
	case model.PolicyDedupWindow:
		if n := runs("true"); n != 1 {
			t.Errorf("runs = %d, want 1", n)
		}
	}
}

// emitAt stores a trigger of key emitted at the given instant.
func (e *env) emitAt(at time.Time, key string) int64 {
	e.t.Helper()
	saved := e.clock.Now()
	e.clock.Advance(at.Sub(saved))
	defer e.clock.Advance(saved.Sub(at))
	return e.emit(key)
}

// wantRunsInOrder checks that the serial runs of def start one at a time
// in the order of events, finishing each in turn.
func (e *env) wantRunsInOrder(def string, events ...int64) {
	e.t.Helper()
	for i, id := range events {
		runID := RunID(def, id)
		if !e.runExists(runID) {
			e.t.Fatalf("run %d: event %d has no run; want order %v", i, id, events)
		}
		for _, later := range events[i+1:] {
			if e.runExists(RunID(def, later)) {
				e.t.Fatalf("run %d: event %d started before event %d", i, later, id)
			}
		}
		e.finish(runID)
	}
}

func TestSerialRunsInEmitOrder(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "enrol", policy: model.PolicySerial, serialKey: "event.key"})
	head := e.emitAt(t0, "k")
	// late has a smaller id than early and tied, but was emitted after
	// them; tied was emitted with early, after it.
	late := e.emitAt(t0.Add(10*time.Second), "k")
	early := e.emitAt(t0.Add(5*time.Second), "k")
	tied := e.emitAt(t0.Add(5*time.Second), "k")

	// Routed in reverse emit order, they still run in emit order.
	e.clock.Advance(time.Minute)
	for _, id := range []int64{head, late, tied, early} {
		e.clock.Advance(time.Second)
		e.route(id)
	}
	e.wantRunsInOrder("enrol", head, early, tied, late)
	if n := e.count(`SELECT count(*) FROM {s}run_queue`); n != 0 {
		t.Errorf("queued = %d, want 0", n)
	}
	if n := e.count(`SELECT count(*) FROM {s}serial_locks WHERE def_name = 'enrol' AND serial_key = 'k'`); n != 1 {
		t.Errorf("serial lock rows = %d, want 1: they are never deleted", n)
	}
}

func TestSerialStartsTheFirstTriggerRoutedToAnIdleKey(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "enrol", policy: model.PolicySerial, serialKey: "event.key"})
	older := e.emitAt(t0, "k")
	newer := e.emitAt(t0.Add(time.Second), "k")
	e.route(newer)
	e.route(older)
	e.wantRunsInOrder("enrol", newer, older)
}

func TestSerialQueueStuckBehindNoRunStartsItsHead(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "enrol", policy: model.PolicySerial, serialKey: "event.key"})
	first, second := e.emit("k"), e.emit("k")
	e.route(first)
	e.route(second)
	// Ending the run while the definition is disabled leaves the queue
	// behind no run.
	e.setEnabled("enrol", false)
	e.finish(RunID("enrol", first))
	e.setEnabled("enrol", true)

	third := e.emit("k")
	e.route(third)
	if r := e.run(RunID("enrol", second)); r.State != stateRunning || !r.Exclusive {
		t.Fatalf("queue head = %s exclusive %v, want started", r.State, r.Exclusive)
	}
	if e.runExists(RunID("enrol", third)) {
		t.Fatal("the new trigger started ahead of the queue")
	}
	if n := e.meter.count("enrol", outcomeQueued); n != 2 {
		t.Errorf("queued outcomes = %d, want 2", n)
	}
	e.wantRunsInOrder("enrol", second, third)
}

func TestDedupWindowIsMeasuredFromEmitTime(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "enrol", policy: model.PolicyDedupWindow, serialKey: "event.key", window: 60})
	first := e.emit("k")
	e.route(first)
	e.clock.Advance(time.Second)
	dup := e.emit("k")
	e.route(dup)

	// Routed again after the window, dup is still within it of its run.
	e.clock.Advance(2 * time.Minute)
	e.route(dup)
	if n := e.count(`SELECT count(*) FROM {s}runs`); n != 1 {
		t.Fatalf("runs = %d, want 1", n)
	}
	if n := e.meter.count("enrol", outcomeDeduped); n != 2 {
		t.Errorf("deduped outcomes = %d, want 2, one per route", n)
	}

	// Emitted after the window, a trigger starts a run, and routing it
	// again a window later finds that run.
	next := e.emit("k")
	e.route(next)
	e.clock.Advance(2 * time.Minute)
	e.route(next)
	if !e.runExists(RunID("enrol", next)) {
		t.Fatal("a trigger emitted after the window started no run")
	}
	if n := e.count(`SELECT count(*) FROM {s}runs`); n != 2 {
		t.Errorf("runs = %d, want 2", n)
	}
	if n := e.meter.count("enrol", outcomeAlreadyRouted); n != 1 {
		t.Errorf("already_routed outcomes = %d, want 1", n)
	}
}

func TestCancelPreviousNeverReplacesANewerTrigger(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "enrol", policy: model.PolicyCancelPrevious, serialKey: "event.key"})
	older := e.emit("k")
	e.clock.Advance(time.Second)
	newer := e.emit("k")

	e.route(newer)
	e.route(older)
	if e.runExists(RunID("enrol", older)) {
		t.Fatal("an older trigger started after a newer one")
	}
	if r := e.run(RunID("enrol", newer)); r.State != stateRunning {
		t.Fatalf("newer run = %s, want running", r.State)
	}

	// The newer run has ended: the older trigger is still superseded.
	e.finish(RunID("enrol", newer))
	e.route(older)
	if e.runExists(RunID("enrol", older)) {
		t.Fatal("an older trigger started after the newer run ended")
	}
	if n := e.meter.count("enrol", outcomeSuperseded); n != 2 {
		t.Errorf("superseded outcomes = %d, want 2, one per route", n)
	}

	// Emitted at one instant, triggers are ordered by event id.
	first, second := e.emit("k"), e.emit("k")
	e.route(second)
	e.route(first)
	if e.runExists(RunID("enrol", first)) || !e.runExists(RunID("enrol", second)) {
		t.Fatal("want the trigger with the larger id to win a tie on emit time")
	}

	// A newer trigger still cancels the active run.
	e.clock.Advance(time.Second)
	last := e.emit("k")
	e.route(last)
	if r := e.run(RunID("enrol", second)); r.State != stateCancelled {
		t.Errorf("replaced run = %s, want cancelled", r.State)
	}
	if n := e.meter.count("enrol", outcomeCancelledPrevious); n != 1 {
		t.Errorf("cancelled_previous outcomes = %d, want 1", n)
	}
}

func TestCancelPreviousEndsTheQueueOfAnEarlierSerialVersion(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "enrol", policy: model.PolicySerial, serialKey: "event.key"})
	queued := make([]int64, 4)
	for i := range queued {
		queued[i] = e.emit("k")
		e.route(queued[i])
	}
	e.publish(defSpec{name: "enrol", policy: model.PolicyCancelPrevious, serialKey: "event.key"})
	e.clock.Advance(time.Second)
	last := e.emit("k")
	e.route(last)

	for _, id := range queued {
		r := e.run(RunID("enrol", id))
		if r.State != stateCancelled || r.TerminalName == nil || *r.TerminalName != TerminalCancelledByNewer {
			t.Errorf("run of event %d = %s, want cancelled by the newer trigger", id, r.State)
		}
	}
	if r := e.run(RunID("enrol", last)); r.State != stateRunning || !r.Exclusive {
		t.Errorf("new run = %s exclusive %v, want running", r.State, r.Exclusive)
	}
	if n := e.count(`SELECT count(*) FROM {s}run_queue`); n != 0 {
		t.Errorf("queued = %d, want 0", n)
	}
	if n := e.count(`SELECT count(*) FROM {s}runs WHERE NOT terminal`); n != 1 {
		t.Errorf("non-terminal runs = %d, want 1", n)
	}
}

func TestDisabledDefinitionKeepsItsQueue(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "enrol", policy: model.PolicySerial, serialKey: "event.key"})
	first, second := e.emit("k"), e.emit("k")
	e.route(first)
	e.route(second)

	e.setEnabled("enrol", false)
	e.finish(RunID("enrol", first))
	if e.runExists(RunID("enrol", second)) {
		t.Fatal("a disabled definition promoted its queue")
	}
	if n := e.count(`SELECT count(*) FROM {s}run_queue`); n != 1 {
		t.Fatalf("queued = %d, want 1", n)
	}

	e.setEnabled("enrol", true)
	ctx := context.Background()
	var runID ids.UUID
	var promoted bool
	if err := e.in.inTx(ctx, func(tx store.Tx) error {
		var err error
		runID, promoted, err = e.in.PromoteQueued(ctx, tx, "enrol", "k")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !promoted || runID != RunID("enrol", second) || !e.runExists(runID) {
		t.Fatalf("PromoteQueued = %s, %v; want the queued run started", runID, promoted)
	}
	if n := e.pumps.count(runID); n != 1 {
		t.Errorf("pumps of the promoted run = %d, want 1", n)
	}
}

func TestPromoteDropsAQueuedEventThatNoLongerFits(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "enrol", policy: model.PolicySerial, serialKey: "event.key"})
	misfit := e.emitType(testEvent, `{"key":"k","misfit":true}`)
	good := e.emit("k")
	for _, id := range []int64{misfit, good} {
		if _, err := e.pool.Exec(context.Background(),
			e.sql(`INSERT INTO {s}run_queue (def_name, serial_key, trigger_event_id, event_at, created_at) VALUES ('enrol', 'k', $1, $2, $2)`),
			id, t0); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	if err := e.in.inTx(ctx, func(tx store.Tx) error {
		_, _, err := e.in.PromoteQueued(ctx, tx, "enrol", "k")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if e.runExists(RunID("enrol", misfit)) || !e.runExists(RunID("enrol", good)) {
		t.Fatal("want the misfit dropped and the next trigger started")
	}
	if n := e.count(`SELECT count(*) FROM {s}dead_letters WHERE kind = 'schema_mismatch' AND event_id = $1`, misfit); n != 1 {
		t.Errorf("schema_mismatch dead letters = %d, want 1", n)
	}
	if n := e.count(`SELECT count(*) FROM {s}run_queue`); n != 0 {
		t.Errorf("queued = %d, want 0", n)
	}
}

// deleteSerialLocks matches a statement deleting serial_locks rows, with
// or without the schema token or a schema qualifier.
var deleteSerialLocks = regexp.MustCompile(`(?i)DELETE\s+FROM\s+(/\*[^*]*\*/\s*)?("?\w+"?\.)?"?serial_locks\b`)

func TestNoCodeDeletesSerialLocks(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("module root not found at %s: %v", root, err)
	}
	scanned := 0
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); path != root && (strings.HasPrefix(name, ".") || name == "plans" || name == "docs") {
				return filepath.SkipDir
			}
			return nil
		}
		ext := filepath.Ext(path)
		if (ext != ".go" && ext != ".sql") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		scanned++
		if loc := deleteSerialLocks.FindIndex(b); loc != nil {
			t.Errorf("%s deletes serial_locks rows (%q); the concurrency gate needs them to stay",
				path, b[loc[0]:loc[1]])
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned == 0 {
		t.Fatal("no source scanned")
	}
	if !deleteSerialLocks.MatchString("DELETE FROM /* TEMPLATE: schema */serial_locks") ||
		!deleteSerialLocks.MatchString(`delete from "fk".serial_locks`) {
		t.Fatal("the pattern misses a deletion")
	}
}
