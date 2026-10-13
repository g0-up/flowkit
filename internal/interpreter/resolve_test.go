package interpreter

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/g0-up/flowkit/internal/executor"
	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/interpreter/model"
	"github.com/g0-up/flowkit/internal/sched"
	"github.com/g0-up/flowkit/internal/store"
)

// actFlow runs the action act, then ends.
var actFlow = steps(`{"id":"act","type":"action","action":"act","next":"done"}`, endStep("done", "finished"))

// seatOutput is the output facts of the act action of the resolve tests.
var seatOutput = []executor.OutputFact{{Name: "seat", Type: executor.FactInt}}

// inDoubt starts a run of actFlow whose non-idempotent act fails, which
// puts its effect in doubt, then lets act succeed. It returns the run and
// its effect_in_doubt dead letter.
func (e *env) inDoubt(act *recorder) (ids.UUID, int64) {
	e.t.Helper()
	act.fail(errors.New("connection reset"))
	e.planner.action("act", testAction{notIdempotent: true, output: seatOutput, execute: act.execute})
	e.publish(defSpec{name: "flow", steps: actFlow})
	id := e.start("flow", "k")
	e.mustPump(id)
	act.fail(nil)
	if n := e.countKind(id, "effect_in_doubt"); n != 1 {
		e.t.Fatalf("run %s did not go in doubt; steps %v", id, e.kinds(id))
	}
	return id, e.deadLetter("effect_in_doubt")
}

// deadLetter returns the id of the only open dead letter of kind.
func (e *env) deadLetter(kind string) int64 {
	e.t.Helper()
	var id int64
	if err := e.pool.QueryRow(context.Background(),
		e.sql(`SELECT id FROM {s}dead_letters WHERE kind = $1 AND resolved_at IS NULL`), kind).Scan(&id); err != nil {
		e.t.Fatalf("open %s dead letter: %v", kind, err)
	}
	return id
}

// resolve resolves the dead letter id with kind and an output.
func (e *env) resolve(id int64, kind string, output map[string]any) error {
	return e.in.ResolveDeadLetter(context.Background(), id, Resolution{
		Kind: kind, Actor: "ops@example.com", Reason: "checked with the provider", Output: output,
	})
}

// open reports whether the dead letter id is unresolved.
func (e *env) open(id int64) bool {
	e.t.Helper()
	return e.count(`SELECT count(*) FROM {s}dead_letters WHERE id = $1 AND resolved_at IS NULL`, id) == 1
}

// effectInDoubt reports whether the run's effect of act is in doubt and
// not done.
func (e *env) effectInDoubt(runID ids.UUID) bool {
	e.t.Helper()
	return e.count(`SELECT count(*) FROM {s}executed_effects WHERE run_id = $1 AND in_doubt AND done_at IS NULL`, runID) == 1
}

func wantInvalid(t *testing.T, err error, path string) {
	t.Helper()
	var inv *InvalidResolution
	if !errors.As(err, &inv) || inv.Path != path {
		t.Fatalf("err = %v, want an InvalidResolution at %s", err, path)
	}
}

func TestResolveInDoubtEffect(t *testing.T) {
	t.Run("redrive runs the action again", func(t *testing.T) {
		e := newEnv(t)
		act := &recorder{output: map[string]any{"seat": 9}}
		id, dl := e.inDoubt(act)
		pumps := e.pumps.count(id)

		if err := e.resolve(dl, ResolveRedrive, nil); err != nil {
			t.Fatalf("redrive = %v", err)
		}
		if e.open(dl) || e.effectInDoubt(id) {
			t.Fatal("redrive left the dead letter open or the effect in doubt")
		}
		if n := e.count(`SELECT count(*) FROM {s}dead_letters WHERE id = $1 AND resolution = 'redrive'
			AND resolved_by = 'ops@example.com' AND resolve_reason = 'checked with the provider' AND resolved_at = $2`,
			dl, e.clock.Now()); n != 1 {
			t.Error("the resolution, actor, reason or time was not recorded")
		}
		if n := e.pumps.count(id); n != pumps+1 {
			t.Fatalf("pumps = %d, want one more than %d", n, pumps)
		}
		e.mustPump(id)
		if n := len(act.calls()); n != 2 {
			t.Fatalf("act ran %d times, want the failed try and the redriven one", n)
		}
		e.mustPump(id)
		if r := e.run(id); r.State != stateCompleted || string(r.Facts) != `{"seat": 9}` {
			t.Fatalf("run = %s facts %s, want completed with the redriven output", r.State, r.Facts)
		}
		if err := e.resolve(dl, ResolveRedrive, nil); !errors.Is(err, ErrResolved) {
			t.Fatalf("second redrive = %v, want ErrResolved", err)
		}
	})

	t.Run("mark done continues with the asserted output", func(t *testing.T) {
		e := newEnv(t)
		act := &recorder{output: map[string]any{"seat": 9}}
		id, dl := e.inDoubt(act)
		pumps := e.pumps.count(id)

		err := e.resolve(dl, ResolveMarkDone, map[string]any{"seat": "front"})
		wantInvalid(t, err, "/output")
		if !e.open(dl) || !e.effectInDoubt(id) || e.pumps.count(id) != pumps {
			t.Fatal("a refused output changed the dead letter, the effect or the pumps")
		}

		if err := e.resolve(dl, ResolveMarkDone, map[string]any{"seat": 4}); err != nil {
			t.Fatalf("mark done = %v", err)
		}
		if e.open(dl) || e.effectInDoubt(id) || e.pumps.count(id) != pumps+1 {
			t.Fatal("mark done left the dead letter open or the effect undone, or did not pump the run")
		}
		e.mustPump(id)
		e.mustPump(id)
		if n := len(act.calls()); n != 1 {
			t.Fatalf("act ran %d times, want only the failed try", n)
		}
		if r := e.run(id); r.State != stateCompleted || string(r.Facts) != `{"seat": 4}` {
			t.Fatalf("run = %s facts %s, want completed with the asserted output", r.State, r.Facts)
		}
	})

	t.Run("skip leaves the run blocked", func(t *testing.T) {
		e := newEnv(t)
		act := &recorder{output: map[string]any{"seat": 9}}
		id, dl := e.inDoubt(act)
		pumps := e.pumps.count(id)

		if err := e.resolve(dl, ResolveSkip, nil); err != nil {
			t.Fatalf("skip = %v", err)
		}
		if e.open(dl) || !e.effectInDoubt(id) || e.pumps.count(id) != pumps {
			t.Fatal("skip changed the effect or pumped the run")
		}
		e.mustPump(id)
		if step, entered := e.at(id); step != "act" || !entered || len(act.calls()) != 1 {
			t.Fatalf("run at %s (entered %v) after %d calls, want still blocked at act", step, entered, len(act.calls()))
		}
	})

	t.Run("effect no longer in doubt", func(t *testing.T) {
		e := newEnv(t)
		id, dl := e.inDoubt(&recorder{output: map[string]any{"seat": 9}})
		if _, err := e.pool.Exec(context.Background(), e.sql(`UPDATE {s}executed_effects SET in_doubt = false WHERE run_id = $1`), id); err != nil {
			t.Fatal(err)
		}
		if err := e.resolve(dl, ResolveRedrive, nil); err == nil {
			t.Fatal("redrive of an effect no longer in doubt succeeded")
		}
		if !e.open(dl) {
			t.Fatal("a failed resolution resolved the dead letter")
		}
	})
}

func TestConcurrentRedrivesRunTheActionOnce(t *testing.T) {
	e := newEnv(t)
	act := &recorder{output: map[string]any{"seat": 9}}
	id, dl := e.inDoubt(act)
	pumps := e.pumps.count(id)

	// Both resolutions queue on the dead letter's row, held meanwhile, so
	// both claim it concurrently.
	ctx := context.Background()
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, e.sql(`SELECT 1 FROM {s}dead_letters WHERE id = $1 FOR UPDATE`), dl); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	var done sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		done.Go(func() { errs[i] = e.resolve(dl, ResolveRedrive, nil) })
	}
	e.awaitLockWaiters(2)
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	done.Wait()

	won, lost := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			won++
		case errors.Is(err, ErrResolved):
			lost++
		default:
			t.Fatalf("redrive = %v", err)
		}
	}
	if won != 1 || lost != 1 {
		t.Fatalf("redrives: %d won, %d already resolved; want one each", won, lost)
	}
	if n := e.pumps.count(id); n != pumps+1 {
		t.Fatalf("pumps = %d, want the winner's only", n-pumps)
	}
	e.mustPump(id)
	e.mustPump(id)
	if n := len(act.calls()); n != 2 {
		t.Fatalf("act ran %d times, want the failed try and one redriven run", n)
	}
	if r := e.run(id); r.State != stateCompleted {
		t.Fatalf("run = %s, want completed", r.State)
	}
}

func TestResolveInDoubtEffectRefusals(t *testing.T) {
	t.Run("redrive of an effect with a late result", func(t *testing.T) {
		e := newEnv(t)
		act := &recorder{output: map[string]any{"seat": 9}}
		id, dl := e.inDoubt(act)
		pumps := e.pumps.count(id)
		if _, err := e.pool.Exec(context.Background(),
			e.sql(`UPDATE {s}executed_effects SET result = '{"seat": 7}' WHERE run_id = $1`), id); err != nil {
			t.Fatal(err)
		}

		wantInvalid(t, e.resolve(dl, ResolveRedrive, nil), "/kind")
		if !e.open(dl) || !e.effectInDoubt(id) || e.pumps.count(id) != pumps {
			t.Fatal("a refused redrive changed the dead letter, the effect or the pumps")
		}
		if err := e.resolve(dl, ResolveMarkDone, map[string]any{"seat": 7}); err != nil {
			t.Fatalf("mark done = %v", err)
		}
		e.mustPump(id)
		e.mustPump(id)
		if n := len(act.calls()); n != 1 {
			t.Fatalf("act ran %d times, want only the failed try", n)
		}
		if r := e.run(id); r.State != stateCompleted || string(r.Facts) != `{"seat": 7}` {
			t.Fatalf("run = %s facts %s, want completed with the late output", r.State, r.Facts)
		}
	})

	t.Run("effect of a run that has ended", func(t *testing.T) {
		e := newEnv(t)
		act := &recorder{output: map[string]any{"seat": 9}}
		id, dl := e.inDoubt(act)
		if err := e.in.Cancel(context.Background(), id, "customer left"); err != nil {
			t.Fatal(err)
		}
		pumps := e.pumps.count(id)

		for _, kind := range []string{ResolveRedrive, ResolveMarkDone} {
			wantInvalid(t, e.resolve(dl, kind, map[string]any{"seat": 4}), "/kind")
			if !e.open(dl) || !e.effectInDoubt(id) || e.pumps.count(id) != pumps {
				t.Fatalf("a refused %s changed the dead letter, the effect or the pumps", kind)
			}
		}
		if err := e.resolve(dl, ResolveSkip, nil); err != nil {
			t.Fatalf("skip = %v", err)
		}
		if e.open(dl) || e.pumps.count(id) != pumps || e.run(id).State != stateCancelled {
			t.Fatal("skip left the dead letter open, pumped the run or reopened it")
		}
	})

	t.Run("mark done with U+0000 in a string", func(t *testing.T) {
		e := newEnv(t)
		act := &recorder{output: map[string]any{"seat": "9a"}}
		act.fail(errors.New("connection reset"))
		e.planner.action("act", testAction{
			notIdempotent: true, output: []executor.OutputFact{{Name: "seat", Type: executor.FactString}}, execute: act.execute,
		})
		e.publish(defSpec{name: "flow", steps: actFlow})
		id := e.start("flow", "k")
		e.mustPump(id)
		dl := e.deadLetter("effect_in_doubt")

		err := e.resolve(dl, ResolveMarkDone, map[string]any{"seat": "9\x00a"})
		wantInvalid(t, err, "/output")
		if !strings.Contains(err.Error(), "U+0000") {
			t.Fatalf("err = %v, want it to name U+0000", err)
		}
		if !e.open(dl) || !e.effectInDoubt(id) {
			t.Fatal("a refused output changed the dead letter or the effect")
		}
	})
}

// blocking is a non-idempotent action whose first call blocks until release
// is closed, then succeeds; later calls succeed at once.
type blocking struct {
	started, release chan struct{}
	calls            atomic.Int64
}

func (b *blocking) execute(context.Context, executor.Input) (map[string]any, error) {
	if b.calls.Add(1) == 1 {
		close(b.started)
		<-b.release
	}
	return map[string]any{"seat": 9}, nil
}

// TestLateSuccessRacesAResolution resolves an in-doubt effect while its
// action, still running, succeeds. Both queue on the effect's dead letter,
// held meanwhile, in a set order, so either may lock it first. Neither may
// deadlock, and an action known to have succeeded is not run again.
func TestLateSuccessRacesAResolution(t *testing.T) {
	for _, resolutionFirst := range []bool{true, false} {
		name := map[bool]string{true: "resolution first", false: "late success first"}[resolutionFirst]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			act := &blocking{started: make(chan struct{}), release: make(chan struct{})}
			e.planner.action("act", testAction{notIdempotent: true, output: seatOutput, execute: act.execute})
			e.publish(defSpec{name: "flow", steps: actFlow})
			id := e.start("flow", "k")
			ctx := context.Background()
			pumped := make(chan string, 1)
			go func() { pumped <- e.in.Pump(ctx, sched.PumpRunArgs{RunID: id}).String() }()
			<-act.started
			e.clock.Advance(testClaimTimeout + time.Second)
			e.mustJanitor()
			dl := e.deadLetter("effect_in_doubt")
			pumps := e.pumps.count(id)

			tx, err := e.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, e.sql(`SELECT 1 FROM {s}dead_letters WHERE id = $1 FOR UPDATE`), dl); err != nil {
				_ = tx.Rollback(ctx)
				t.Fatal(err)
			}
			resolved := make(chan error, 1)
			resolve := func() { resolved <- e.resolve(dl, ResolveRedrive, nil) }
			succeed := func() { close(act.release) }
			if !resolutionFirst {
				resolve, succeed = succeed, resolve
			}
			go resolve()
			e.awaitLockWaiters(1)
			go succeed()
			e.awaitLockWaiters(2)
			if err := tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			err = <-resolved
			res := <-pumped

			if failed := e.logs.logged("orphaned effect failed"); len(failed) > 0 {
				t.Fatalf("keeping the late success failed: %v", failed)
			}
			if resolutionFirst {
				if err != nil {
					t.Fatalf("redrive = %v", err)
				}
				if len(e.logs.logged("orphaned output is dropped")) != 1 {
					t.Fatalf("the late success of a redriven effect was not dropped; logs %v", e.logs.logged(""))
				}
				if e.open(dl) || e.effectInDoubt(id) || e.pumps.count(id) != pumps+1 {
					t.Fatal("the redrive left the dead letter open or the effect in doubt, or did not pump the run")
				}
				// Nothing was known of the late success when the redrive
				// released the effect, so the action runs again.
				e.mustPump(id)
				if n := act.calls.Load(); n != 2 {
					t.Fatalf("act ran %d times, want the late success and the redriven run", n)
				}
			} else {
				wantInvalid(t, err, "/kind")
				if !e.open(dl) || !e.effectInDoubt(id) || e.pumps.count(id) != pumps {
					t.Fatalf("a refused redrive changed the dead letter, the effect or the pumps (pump %s)", res)
				}
				if n := e.count(`SELECT count(*) FROM {s}executed_effects WHERE run_id = $1 AND result = '{"seat": 9}'`, id); n != 1 {
					t.Fatal("the late success's output was not kept")
				}
				if err := e.resolve(dl, ResolveMarkDone, map[string]any{"seat": 9}); err != nil {
					t.Fatalf("mark done = %v", err)
				}
				e.mustPump(id)
				if n := act.calls.Load(); n != 1 {
					t.Fatalf("act ran %d times, want only the late success", n)
				}
			}
			e.mustPump(id)
			if r := e.run(id); r.State != stateCompleted || string(r.Facts) != `{"seat": 9}` {
				t.Fatalf("run = %s facts %s, want completed", r.State, r.Facts)
			}
		})
	}
}

func TestResolveExhaustedOnlySkips(t *testing.T) {
	e := newEnv(t)
	act := &recorder{err: errors.New("card declined")}
	e.planner.action("act", testAction{retry: executor.RetryPolicy{MaxAttempts: 1}, execute: act.execute})
	e.publish(defSpec{name: "flow", steps: actFlow})
	id := e.start("flow", "k")
	e.mustPump(id)
	dl := e.deadLetter("exhausted")
	pumps := e.pumps.count(id)

	for _, kind := range []string{ResolveRedrive, ResolveMarkDone} {
		wantInvalid(t, e.resolve(dl, kind, map[string]any{}), "/kind")
		if !e.open(dl) {
			t.Fatalf("a refused %s resolved the dead letter", kind)
		}
	}
	if err := e.resolve(dl, ResolveSkip, nil); err != nil {
		t.Fatalf("skip = %v", err)
	}
	if e.open(dl) || e.pumps.count(id) != pumps || e.run(id).State != stateFailed {
		t.Fatal("skip left the dead letter open, pumped the run or reopened it")
	}
}

func TestResolveEventDeadLetters(t *testing.T) {
	cases := map[string]struct {
		kind  string
		setup func(e *env) int64 // returns the event
	}{
		"schema_mismatch": {"schema_mismatch", func(e *env) int64 {
			e.publish(defSpec{name: "flow"})
			ev := e.emitType(testEvent, `{"key":"a","misfit":true}`)
			e.route(ev)
			return ev
		}},
		"unroutable_event": {"unroutable_event", func(e *env) int64 {
			e.publish(defSpec{name: "flow", policy: model.PolicySerial, serialKey: "event.key"})
			ev := e.emitType(testEvent, `{"key":""}`)
			e.route(ev)
			return ev
		}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			ev := c.setup(e)
			dl := e.deadLetter(c.kind)

			wantInvalid(t, e.resolve(dl, ResolveMarkDone, map[string]any{}), "/kind")
			if err := e.resolve(dl, ResolveRedrive, nil); err != nil {
				t.Fatalf("redrive = %v", err)
			}
			if n := e.pumps.routed(ev); n != 1 {
				t.Fatalf("event routed %d times, want 1", n)
			}
			if e.open(dl) {
				t.Fatal("redrive left the dead letter open")
			}
		})
		t.Run(name+" without its event", func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			ev := c.setup(e)
			dl := e.deadLetter(c.kind)
			if _, err := e.pool.Exec(context.Background(), e.sql(`DELETE FROM {s}domain_events WHERE id = $1`), ev); err != nil {
				t.Fatal(err)
			}
			if err := e.resolve(dl, ResolveRedrive, nil); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("redrive = %v, want ErrNotFound", err)
			}
			if !e.open(dl) || e.pumps.routed(ev) != 0 {
				t.Fatal("a failed redrive resolved the dead letter or routed the event")
			}
			if err := e.resolve(dl, ResolveSkip, nil); err != nil {
				t.Fatalf("skip = %v", err)
			}
		})
	}
}

func TestResolveRejectsUnknownKindsAndIDs(t *testing.T) {
	e := newEnv(t)
	wantInvalid(t, e.resolve(1, "retry", nil), "/kind")
	if err := e.resolve(424242, ResolveSkip, nil); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("resolve of a missing dead letter = %v, want ErrNotFound", err)
	}
}

func TestResolveStoresSanitizedActorAndReason(t *testing.T) {
	e := newEnv(t)
	_, dl := e.inDoubt(&recorder{output: map[string]any{"seat": 9}})
	err := e.in.ResolveDeadLetter(context.Background(), dl, Resolution{Kind: ResolveSkip, Actor: "ops\x00", Reason: "bad\x00byte"})
	if err != nil {
		t.Fatalf("skip = %v", err)
	}
	if n := e.count(`SELECT count(*) FROM {s}dead_letters WHERE id = $1 AND resolved_by = $2 AND resolve_reason = $3`,
		dl, "ops\uFFFD", "bad\uFFFDbyte"); n != 1 {
		t.Error("actor and reason were not sanitized")
	}
}

// cancelReason returns the error recorded on the run's terminal step.
func (e *env) cancelReason(id ids.UUID) string {
	e.t.Helper()
	s := e.terminal(id)
	if s.Error == nil {
		return ""
	}
	return *s.Error
}

func TestCancelAWaitingRun(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "flow", policy: model.PolicySerial, serialKey: "event.key", steps: waitFlow("")})
	first := e.start("flow", "k")
	e.mustPump(first)
	args := e.timers.last(t, first)
	second := e.emit("k")
	e.route(second)
	if e.runExists(RunID("flow", second)) {
		t.Fatal("the second trigger started a run beside the waiting one")
	}

	if err := e.in.Cancel(context.Background(), first, "customer left"); err != nil {
		t.Fatalf("Cancel = %v", err)
	}
	r := e.run(first)
	if r.State != stateCancelled || !r.Terminal || r.TerminalName == nil || *r.TerminalName != TerminalCancelled {
		t.Fatalf("run = %s terminal %v name %v, want cancelled %s", r.State, r.Terminal, r.TerminalName, TerminalCancelled)
	}
	if got := e.cancelReason(first); got != "customer left" {
		t.Errorf("terminal error = %q, want the reason", got)
	}
	if n := e.count(`SELECT count(*) FROM {s}subscriptions WHERE run_id = $1`, first); n != 0 {
		t.Errorf("%d subscriptions left", n)
	}
	next := RunID("flow", second)
	if !e.runExists(next) || e.pumps.count(next) != 1 {
		t.Fatal("the queued run was not promoted")
	}

	e.clock.Advance(2 * time.Hour)
	if res := e.timeout(args); res != "done" {
		t.Fatalf("timeout = %s", res)
	}
	if n := e.countKind(first, stepClaimLost); n != 1 {
		t.Fatalf("claim_lost records = %d, want the timeout's; steps %v", n, e.kinds(first))
	}
	if after := e.run(first); after.State != stateCancelled || after.Rev != r.Rev {
		t.Fatalf("the timeout changed the cancelled run: %s rev %d, was rev %d", after.State, after.Rev, r.Rev)
	}

	// Cancelling again changes nothing.
	steps := len(e.steps(first))
	if err := e.in.Cancel(context.Background(), first, "again"); err != nil {
		t.Fatalf("second Cancel = %v", err)
	}
	if n := len(e.steps(first)); n != steps {
		t.Errorf("second Cancel recorded %d steps", n-steps)
	}
}

func TestCancelEdgeCases(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "flow", steps: waitFlow("")})
	if err := e.in.Cancel(context.Background(), ids.NewV4(), ""); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Cancel of a missing run = %v, want ErrNotFound", err)
	}
	id := e.start("flow", "k")
	if err := e.in.Cancel(context.Background(), id, ""); err != nil {
		t.Fatalf("Cancel = %v", err)
	}
	if got := e.terminal(id); got.Error != nil {
		t.Errorf("terminal error = %q, want none without a reason", *got.Error)
	}
	if n := e.meter.added("flowkit.runs_terminal", attribute.String("state", stateCancelled)); n != 1 {
		t.Errorf("runs_terminal = %d, want 1", n)
	}
}

// TestCancelAndRouteTakeLocksInOneOrder races Cancel of a run with the
// route of a newer trigger of its key, which cancels the run under
// cancel_previous and queues behind it under serial. Each round holds the
// key's serial lock while both queue on it, Cancel first in some rounds
// and the route first in others. Both take the serial lock before the run
// row, so neither deadlocks and neither fails, whichever goes first.
func TestCancelAndRouteTakeLocksInOneOrder(t *testing.T) {
	for _, policy := range []string{model.PolicyCancelPrevious, model.PolicySerial} {
		t.Run(policy, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			e.publish(defSpec{name: "flow", policy: policy, serialKey: "event.key", steps: waitFlow("")})
			ctx := context.Background()
			const rounds = 200
			for i := range rounds {
				cancelFirst := i%2 == 0
				key := "k" + strconv.Itoa(i)
				first := e.start("flow", key)
				if i%4 >= 2 {
					e.mustPump(first) // waiting, with a subscription
				}
				newer := e.emit(key)

				release := e.holdSerial("flow", key)
				var done sync.WaitGroup
				var cancelled, routed error
				ends := []func(){
					func() { cancelled = e.in.Cancel(ctx, first, "round "+strconv.Itoa(i)) },
					func() { routed = e.in.RouteEvent(ctx, newer) },
				}
				if !cancelFirst {
					ends[0], ends[1] = ends[1], ends[0]
				}
				for n, end := range ends {
					done.Go(end)
					e.awaitLockWaiters(n + 1)
				}
				release()
				done.Wait()

				if cancelled != nil || routed != nil {
					t.Fatalf("round %d: cancel %v, route %v", i, cancelled, routed)
				}
				r := e.run(first)
				want := TerminalCancelled
				if !cancelFirst && policy == model.PolicyCancelPrevious {
					want = TerminalCancelledByNewer
				}
				if r.State != stateCancelled || r.TerminalName == nil || *r.TerminalName != want {
					t.Fatalf("round %d: first run %s %v, want cancelled %s", i, r.State, r.TerminalName, want)
				}
				if n := e.countKind(first, stepTerminal); n != 1 {
					t.Fatalf("round %d: %d terminal records; steps %v", i, n, e.kinds(first))
				}
				if !e.runExists(RunID("flow", newer)) {
					t.Fatalf("round %d: the newer trigger started no run", i)
				}
				if n := e.count(`SELECT count(*) FROM {s}subscriptions WHERE run_id = $1`, first); n != 0 {
					t.Fatalf("round %d: %d subscriptions left", i, n)
				}
			}
		})
	}
}

// TestResolveEnqueuesInTheResolutionTransaction checks that a pump that
// cannot be enqueued rolls the whole resolution back.
func TestResolveEnqueuesInTheResolutionTransaction(t *testing.T) {
	e := newEnv(t)
	id, dl := e.inDoubt(&recorder{output: map[string]any{"seat": 9}})
	e.pumps.fail = errInjected
	if err := e.resolve(dl, ResolveRedrive, nil); !errors.Is(err, errInjected) {
		t.Fatalf("redrive = %v, want the enqueue failure", err)
	}
	e.pumps.fail = nil
	if !e.open(dl) || !e.effectInDoubt(id) {
		t.Fatal("a resolution whose pump failed was committed")
	}
}
