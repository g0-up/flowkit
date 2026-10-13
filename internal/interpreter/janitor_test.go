package interpreter

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/g0-up/flowkit/internal/executor"
	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/interpreter/model"
	"github.com/g0-up/flowkit/internal/sched"
)

// janitor runs one janitor pass and returns the job's result.
func (e *env) janitor() string {
	e.t.Helper()
	return e.in.Janitor(context.Background(), sched.JanitorArgs{}).String()
}

// mustJanitor runs one janitor pass and fails the test unless it is done.
func (e *env) mustJanitor() {
	e.t.Helper()
	if res := e.janitor(); res != "done" {
		e.t.Fatalf("janitor = %s", res)
	}
}

// abandoned is an action that blocks until its request is abandoned.
type abandoned struct{ started chan struct{} }

func (a abandoned) execute(ctx context.Context, _ executor.Input) (map[string]any, error) {
	close(a.started)
	<-ctx.Done()
	return nil, ctx.Err()
}

// abandon pumps the run into act and abandons the pump while act runs, so
// the effect's claim stays until it expires.
func (e *env) abandon(id ids.UUID, idempotent bool) {
	e.t.Helper()
	act := abandoned{started: make(chan struct{})}
	e.planner.action("act", testAction{notIdempotent: !idempotent, execute: act.execute})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-act.started
		cancel()
	}()
	if res := e.in.Pump(ctx, sched.PumpRunArgs{RunID: id}).String(); !strings.HasPrefix(res, "retry: ") {
		e.t.Fatalf("abandoned pump = %s, want a retry", res)
	}
	if n := e.count(`SELECT count(*) FROM {s}executed_effects WHERE run_id = $1 AND claimed_at IS NOT NULL AND done_at IS NULL`, id); n != 1 {
		e.t.Fatalf("claimed effects of run %s = %d, want 1", id, n)
	}
}

func TestJanitorResolvesExpiredClaims(t *testing.T) {
	for _, idempotent := range []bool{true, false} {
		name := map[bool]string{true: "idempotent", false: "not idempotent"}[idempotent]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			e.publish(defSpec{name: "flow", steps: actFlow})
			id := e.start("flow", "k")
			e.abandon(id, idempotent)
			pumps := e.pumps.count(id)

			e.clock.Advance(testClaimTimeout - time.Second)
			e.mustJanitor()
			if n := len(e.steps(id)); e.countKind(id, "attempt_failed")+e.countKind(id, "effect_in_doubt") != 0 {
				t.Fatalf("a live claim was resolved; %d steps %v", n, e.kinds(id))
			}

			e.clock.Advance(2 * time.Second)
			e.mustJanitor()
			if idempotent {
				if n := e.countKind(id, "attempt_failed"); n != 1 || e.pumps.count(id) != pumps+1 {
					t.Fatalf("attempt_failed = %d, pumps %d; want the expired claim counted as an attempt and the run pumped", n, e.pumps.count(id)-pumps)
				}
				return
			}
			if !e.effectInDoubt(id) || e.count(`SELECT count(*) FROM {s}dead_letters WHERE kind = 'effect_in_doubt' AND run_id = $1`, id) != 1 {
				t.Fatal("an expired claim of a non-idempotent action did not go in doubt")
			}
			if n := e.pumps.count(id); n != pumps {
				t.Fatalf("pumps = %d, want none of a run in doubt", n-pumps)
			}
		})
	}
}

func TestJanitorResumesAStaleLookback(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "flow", steps: waitFlow("")})
	id := e.start("flow", "k")
	ev := e.emitWait("k")
	e.route(ev)
	e.in.hooks.afterWait = func(ids.UUID) error { return errInjected }
	if res := e.pump(id); res != "retry: "+errInjected.Error() {
		t.Fatalf("pump = %s, want the injected failure", res)
	}
	e.in.hooks.afterWait = nil
	pumps := e.pumps.count(id)

	e.mustJanitor()
	if n := e.pumps.count(id); n != pumps {
		t.Fatalf("pumps = %d of a look-back within the claim timeout", n-pumps)
	}
	e.clock.Advance(testClaimTimeout + time.Second)
	e.mustJanitor()
	if n := e.pumps.count(id); n != pumps+1 {
		t.Fatalf("pumps = %d, want one of the stale look-back", n-pumps)
	}
	e.mustPump(id)
	e.claimedBy(id, ev)
}

func TestJanitorPromotesAQueueLeftWhileDisabled(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "enrol", policy: model.PolicySerial, serialKey: "event.key"})
	first, second := e.emit("k"), e.emit("k")
	e.route(first)
	e.route(second)
	e.setEnabled("enrol", false)
	e.finish(RunID("enrol", first))

	e.mustJanitor()
	next := RunID("enrol", second)
	if e.runExists(next) {
		t.Fatal("the janitor promoted the queue of a disabled definition")
	}

	e.setEnabled("enrol", true)
	e.mustJanitor()
	if !e.runExists(next) || e.pumps.count(next) != 1 {
		t.Fatal("the janitor did not promote the queue after Enable")
	}
	if n := e.count(`SELECT count(*) FROM {s}run_queue`); n != 0 {
		t.Fatalf("queued = %d, want 0", n)
	}
	e.mustJanitor()
	if n := e.pumps.count(next); n != 1 {
		t.Fatalf("pumps of the promoted run = %d after another pass, want 1", n)
	}
}

func TestJanitorPumpsStuckRuns(t *testing.T) {
	t.Run("lost pump", func(t *testing.T) {
		e := newEnv(t)
		e.publish(defSpec{name: "flow", steps: actFlow})
		e.planner.action("act", testAction{execute: (&recorder{output: map[string]any{}}).execute})
		id := e.start("flow", "k")
		pumps := e.pumps.count(id)

		e.clock.Advance(testStuckRunAfter - time.Second)
		e.mustJanitor()
		if n := e.pumps.count(id); n != pumps {
			t.Fatalf("pumps = %d of a run written within StuckRunAfter", n-pumps)
		}
		e.clock.Advance(2 * time.Second)
		e.mustJanitor()
		if n := e.pumps.count(id); n != pumps+1 {
			t.Fatalf("pumps = %d, want one of the stuck run", n-pumps)
		}
	})

	t.Run("effect in doubt", func(t *testing.T) {
		e := newEnv(t)
		id, _ := e.inDoubt(&recorder{output: map[string]any{"seat": 1}})
		pumps := e.pumps.count(id)
		e.clock.Advance(10 * testStuckRunAfter)
		e.mustJanitor()
		if n := e.pumps.count(id); n != pumps {
			t.Fatalf("pumps = %d of a run whose effect is in doubt", n-pumps)
		}
	})

	t.Run("effect claimed", func(t *testing.T) {
		e := newEnv(t)
		e.publish(defSpec{name: "flow", steps: actFlow})
		id := e.start("flow", "k")
		e.abandon(id, true)
		// The run was last written long ago, but its effect is claimed now.
		if _, err := e.pool.Exec(context.Background(), e.sql(`UPDATE {s}runs SET updated_at = $2 WHERE id = $1`),
			id, e.clock.Now().Add(-10*testStuckRunAfter)); err != nil {
			t.Fatal(err)
		}
		pumps := e.pumps.count(id)
		e.clock.Advance(testClaimTimeout - time.Second)
		e.mustJanitor()
		if n := e.pumps.count(id); n != pumps {
			t.Fatalf("pumps = %d of a run whose effect is claimed", n-pumps)
		}
	})

	t.Run("disabled definition", func(t *testing.T) {
		e := newEnv(t)
		e.publish(defSpec{name: "flow", steps: actFlow})
		id := e.start("flow", "k")
		e.setEnabled("flow", false)
		pumps := e.pumps.count(id)
		e.clock.Advance(10 * testStuckRunAfter)
		e.mustJanitor()
		if n := e.pumps.count(id); n != pumps {
			t.Fatalf("pumps = %d of a run of a disabled definition", n-pumps)
		}
	})
}

func TestJanitorAppliesRetention(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "flow", steps: waitFlow("")})
	done := e.start("flow", "a")
	e.mustPump(done)
	e.route(e.emitWait("a"))
	e.mustPump(done)
	if r := e.run(done); r.State != stateCompleted {
		t.Fatalf("run = %s, want completed", r.State)
	}
	e.clock.Advance(time.Minute) // a's events are before b's look-back
	waiting := e.start("flow", "b")
	e.mustPump(waiting)
	if r := e.run(waiting); r.State != stateWaiting {
		t.Fatalf("run = %s, want waiting", r.State)
	}
	waitingTrigger := e.run(waiting).TriggerEventID

	e.route(e.emitType(testEvent, `{"key":"c","misfit":true}`))
	openDL := e.deadLetter("schema_mismatch")
	e.route(e.emitType(testEvent, `{"key":"d","misfit":true}`))
	var resolvedDL int64
	if err := e.pool.QueryRow(context.Background(),
		e.sql(`SELECT id FROM {s}dead_letters WHERE id <> $1`), openDL).Scan(&resolvedDL); err != nil {
		t.Fatal(err)
	}
	if err := e.resolve(resolvedDL, ResolveSkip, nil); err != nil {
		t.Fatal(err)
	}

	// The completed run ended a minute ago.
	e.clock.Advance(testRetention.StepDetail - 2*time.Minute)
	e.mustJanitor()
	if !e.runExists(done) || e.count(`SELECT count(*) FROM {s}run_summaries`) != 0 {
		t.Fatal("the janitor summarised a run within Retention.StepDetail")
	}

	e.clock.Advance(24 * time.Hour) // 91 days
	e.mustJanitor()
	if e.runExists(done) {
		t.Fatal("the terminal run kept its detail past Retention.StepDetail")
	}
	for _, table := range []string{"run_steps", "consumed_events", "subscriptions", "executed_effects"} {
		if n := e.count(`SELECT count(*) FROM {s}`+table+` WHERE run_id = $1`, done); n != 0 {
			t.Errorf("%s keeps %d rows of the summarised run", table, n)
		}
	}
	if n := e.count(`SELECT count(*) FROM {s}run_summaries WHERE run_id = $1 AND state = 'completed'`, done); n != 1 {
		t.Fatal("the summarised run has no summary")
	}
	if !e.runExists(waiting) || e.count(`SELECT count(*) FROM {s}domain_events WHERE id = $1`, waitingTrigger) != 1 {
		t.Fatal("the janitor deleted a run that is not terminal or the event it references")
	}
	if n := e.count(`SELECT count(*) FROM {s}domain_events`); n != 1 {
		t.Errorf("events = %d, want only the waiting run's trigger", n)
	}
	if !e.open(openDL) {
		t.Fatal("the janitor deleted an open dead letter")
	}
	if n := e.count(`SELECT count(*) FROM {s}dead_letters WHERE id = $1`, resolvedDL); n != 0 {
		t.Fatal("the janitor kept a dead letter resolved past Retention.DeadLetter")
	}

	e.clock.Advance(testRetention.Run)
	e.mustJanitor()
	if n := e.count(`SELECT count(*) FROM {s}run_summaries`); n != 0 {
		t.Fatalf("summaries = %d past Retention.Run, want 0", n)
	}
}

func TestJanitorStepFailureDoesNotStopTheOthers(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "flow", steps: actFlow})
	stuck := e.start("flow", "a")
	e.publish(defSpec{name: "short"})
	ended := e.start("short", "b")
	e.mustPump(ended)

	e.clock.Advance(testRetention.StepDetail + time.Hour)
	e.pumps.fail = errInjected
	res := e.janitor()
	e.pumps.fail = nil
	if !strings.HasPrefix(res, "retry: ") || !strings.Contains(res, "pump stuck runs") || !strings.Contains(res, errInjected.Error()) {
		t.Fatalf("janitor = %s, want a retry naming the failed step", res)
	}
	if e.runExists(ended) {
		t.Fatal("a failed step stopped the retention")
	}
	if !e.runExists(stuck) {
		t.Fatal("the stuck run was deleted")
	}
}
