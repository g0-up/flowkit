package interpreter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/g0-up/flowkit/internal/executor"
	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/sched"
	"github.com/g0-up/flowkit/internal/store"
)

// waitFlow waits for testWaitEvent with match, then ends at got, or at late
// on the timeout.
func waitFlow(match string) string {
	return steps(waitStep("w", match, "got", "late"), endStep("got", "got"), endStep("late", "late"))
}

// claims returns the snapshots of the run's claimed steps in order.
func (e *env) claims(id ids.UUID) []map[string]any {
	e.t.Helper()
	var out []map[string]any
	for _, s := range e.steps(id) {
		if s.Kind != stepClaimed {
			continue
		}
		var snap map[string]any
		if err := json.Unmarshal(s.Snapshot, &snap); err != nil {
			e.t.Fatalf("claimed snapshot %s: %v", s.Snapshot, err)
		}
		out = append(out, snap)
	}
	return out
}

// claimedBy fails the test unless the run's only claim is by the event.
func (e *env) claimedBy(id ids.UUID, eventID int64) {
	e.t.Helper()
	c := e.claims(id)
	if len(c) != 1 || c[0]["event_id"] != float64(eventID) {
		e.t.Fatalf("claims of run %s = %v, want one by event %d; steps %v", id, c, eventID, e.kinds(id))
	}
}

func (e *env) event(id int64) store.EventRow {
	e.t.Helper()
	var ev store.EventRow
	e.inTx(func(tx store.Tx) {
		var err error
		if ev, err = e.store.GetEvent(context.Background(), tx, id); err != nil {
			e.t.Fatal(err)
		}
	})
	return ev
}

func TestEventAndTimeoutClaimAWaitOnce(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "flow", steps: waitFlow("")})
	ctx := context.Background()

	// park starts a run, parks it on the wait with its timer due and
	// stores an event for it.
	park := func(i int) (ids.UUID, int64, sched.WaitTimeoutArgs) {
		// Events of earlier rounds were stored before this run.
		e.clock.Advance(2 * time.Hour)
		id := e.start("flow", fmt.Sprintf("k%d", i))
		e.mustPump(id)
		args := e.timers.last(t, id)
		ev := e.emitWait("k")
		e.clock.Advance(time.Hour)
		return id, ev, args
	}
	check := func(i int, id ids.UUID, ev int64) string {
		r := e.run(id)
		step, entered := e.at(id)
		if r.State != stateRunning || entered || (step != "got" && step != "late") {
			t.Fatalf("round %d: run %s at %s (entered %v), want running at got or late", i, r.State, step, entered)
		}
		if n := len(e.claims(id)); n != 1 {
			t.Fatalf("round %d: %d claims, want 1; steps %v", i, n, e.kinds(id))
		}
		consumed := e.count(`SELECT count(*) FROM {s}consumed_events WHERE run_id = $1 AND event_id = $2`, id, ev)
		if want := map[string]int{"got": 1, "late": 0}[step]; consumed != want {
			t.Fatalf("round %d: event consumed %d times with the run at %s", i, consumed, step)
		}
		if n := e.count(`SELECT count(*) FROM {s}subscriptions WHERE run_id = $1`, id); n != 0 {
			t.Fatalf("round %d: %d subscriptions left", i, n)
		}
		return step
	}

	// Each round holds the run row while both claims read the run and
	// queue on its update, the event's first in even rounds and the
	// timer's in odd ones; releasing it lets them write in queue order. So
	// every round is a write conflict, and the second writer must find the
	// wait gone and record claim_lost. The event claims through offer, with
	// the subscription listed before the race, and in the last rounds
	// through the whole route of the event.
	const offers, routes = 1000, 100
	wins := map[string]int{}
	for i := range offers + routes {
		id, ev, args := park(i)
		eventClaim := func() error { return e.in.RouteEvent(ctx, ev) }
		if i < offers {
			event := e.event(ev)
			var waiting []store.WaitingRun
			e.inTx(func(tx store.Tx) {
				var err error
				if waiting, err = e.store.ListEventSubscriptions(ctx, tx, store.SubscriptionQuery{
					EventType: testWaitEvent, EventID: ev, Limit: 10,
				}); err != nil {
					t.Fatal(err)
				}
			})
			if len(waiting) != 1 || waiting[0].Run.ID != id {
				t.Fatalf("round %d: subscriptions %v, want the run's", i, waiting)
			}
			eventClaim = func() error { return e.in.offer(ctx, event, waiting[0]) }
		}

		release := e.holdRun(id)
		var done sync.WaitGroup
		var offered error
		var timedOut sched.Result
		claims := []func(){
			func() { offered = eventClaim() },
			func() { timedOut = e.in.Timeout(ctx, args) },
		}
		want := "got"
		if i%2 == 1 {
			claims[0], claims[1] = claims[1], claims[0]
			want = "late"
		}
		for n, claim := range claims {
			done.Go(claim)
			e.awaitLockWaiters(n + 1)
		}
		release()
		done.Wait()

		if offered != nil || timedOut.String() != "done" {
			t.Fatalf("round %d: event claim %v, timeout %s", i, offered, timedOut)
		}
		if got := check(i, id, ev); got != want {
			t.Fatalf("round %d: run at %s, want %s by the first queued claim", i, got, want)
		}
		wins[want]++
		if n := e.countKind(id, stepClaimLost); n != 1 {
			t.Fatalf("round %d: %d claim_lost records, want the loser's; steps %v", i, n, e.kinds(id))
		}
	}
	t.Logf("the event won %d and the timeout %d of %d rounds", wins["got"], wins["late"], offers+routes)
}

func TestTimeoutSnoozesUntilDueThenClaims(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "flow", steps: waitFlow("")})
	id := e.start("flow", "k")
	e.mustPump(id)
	args := e.timers.last(t, id)
	if !args.DueAt.Equal(t0.Add(time.Hour)) || args.StepID != "w" || args.WaitSeq != 1 {
		t.Fatalf("timer = %+v, want wait w seq 1 due in an hour", args)
	}
	if res := e.timeout(args); res != "snooze 1h0m0s" {
		t.Fatalf("early timeout = %s", res)
	}
	e.clock.Advance(40 * time.Minute)
	if res := e.timeout(args); res != "snooze 20m0s" {
		t.Fatalf("early timeout = %s", res)
	}
	if n := len(e.steps(id)); n != 1 {
		t.Fatalf("an early timer wrote steps %v", e.kinds(id))
	}
	e.clock.Advance(20 * time.Minute)
	if res := e.timeout(args); res != "done" {
		t.Fatalf("due timeout = %s", res)
	}
	c := e.claims(id)
	if len(c) != 1 || c[0]["next"] != "late" || c[0]["wait_seq"] != float64(1) || c[0]["event_id"] != nil {
		t.Fatalf("claims = %v, want one by the timeout", c)
	}
	if n := e.pumps.count(id); n != 2 {
		t.Errorf("pumps enqueued = %d, want the start's and the claim's", n)
	}
	e.mustPump(id)
	if r := e.run(id); r.State != stateCompleted || *r.TerminalName != "late" {
		t.Fatalf("run = %s %v, want completed at late", r.State, r.TerminalName)
	}
	// Timers are never cancelled; one that fires again finds the wait gone.
	if res := e.timeout(args); res != "done" || e.countKind(id, stepClaimLost) != 1 {
		t.Fatalf("repeated timeout = %s, steps %v", res, e.kinds(id))
	}
}

func TestWaitFindsAnEventStoredWhileTheActionBeforeItRetried(t *testing.T) {
	e := newEnv(t)
	prep := &recorder{err: errors.New("try again"), output: map[string]any{}}
	e.planner.action("prep", testAction{
		retry: executor.RetryPolicy{MaxAttempts: 3, BaseBackoff: time.Minute}, execute: prep.execute,
	})
	e.publish(defSpec{name: "flow", steps: steps(
		`{"id":"prep","type":"action","action":"prep","next":"w"}`,
		waitStep("w", "", "got", "late"), endStep("got", "got"), endStep("late", "late"),
	)})
	id := e.start("flow", "k")
	if res := e.pump(id); !strings.HasPrefix(res, "snooze ") {
		t.Fatalf("pump = %s, want the action snoozed", res)
	}
	// Routed while the run waits on nothing: no subscription takes it.
	ev := e.emitWait("k")
	e.route(ev)
	if n := len(e.claims(id)); n != 0 {
		t.Fatalf("claims before the wait = %d", n)
	}

	prep.fail(nil)
	e.clock.Advance(time.Hour)
	e.mustPump(id) // the retry of prep succeeds
	e.mustPump(id) // the wait's look-back finds the event
	e.claimedBy(id, ev)
	e.mustPump(id)
	if r := e.run(id); r.State != stateCompleted || *r.TerminalName != "got" {
		t.Fatalf("run = %s %v, want completed at got", r.State, r.TerminalName)
	}
}

func TestPumpRerunsTheLookbackOfAWaitItCrashedIn(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "flow", steps: waitFlow("")})
	id := e.start("flow", "k")
	ev := e.emitWait("k")
	e.route(ev)

	// The process dies after committing the wait, before its look-back.
	e.in.hooks.afterWait = func(ids.UUID) error { return errInjected }
	if res := e.pump(id); res != "retry: "+errInjected.Error() {
		t.Fatalf("pump = %s, want the injected failure", res)
	}
	r := e.run(id)
	if r.State != stateWaiting || !r.LookbackPending || len(e.claims(id)) != 0 {
		t.Fatalf("run = %s lookback_pending %v, claims %v; want waiting with its look-back pending",
			r.State, r.LookbackPending, e.claims(id))
	}

	e.in.hooks.afterWait = nil
	e.mustPump(id)
	e.claimedBy(id, ev)
	if r := e.run(id); r.State != stateRunning || r.LookbackPending {
		t.Fatalf("run = %s lookback_pending %v after the rerun", r.State, r.LookbackPending)
	}
	if step, _ := e.at(id); step != "got" {
		t.Fatalf("run at %s, want got", step)
	}
	// A pump of the waiting run after its look-back is done leaves it alone.
	e.mustPump(id)
	if r := e.run(id); r.State != stateCompleted {
		t.Fatalf("run = %s, want completed", r.State)
	}
}

func TestAnEventSatisfiesOneWaitOfARun(t *testing.T) {
	t.Run("two waits for one type", func(t *testing.T) {
		e := newEnv(t)
		e.publish(defSpec{name: "flow", steps: steps(
			waitStep("w1", "", "w2", "late"), waitStep("w2", "", "got", "late"),
			endStep("got", "got"), endStep("late", "late"),
		)})
		id := e.start("flow", "k")
		e.mustPump(id)
		ev := e.emitWait("k")
		e.route(ev)
		e.claimedBy(id, ev)

		// The look-back of w2 reads the event w1 consumed and leaves it.
		e.mustPump(id)
		r := e.run(id)
		if r.State != stateWaiting || *r.WaitStep != "w2" || r.LookbackPending || len(e.claims(id)) != 1 {
			t.Fatalf("run = %s at %v lookback_pending %v, steps %v; want waiting at w2",
				r.State, r.WaitStep, r.LookbackPending, e.kinds(id))
		}
		// Routing it again satisfies no wait either.
		e.route(ev)
		if n := len(e.claims(id)); n != 1 || e.countKind(id, stepClaimLost) != 0 {
			t.Fatalf("steps after routing again = %v", e.kinds(id))
		}

		next := e.emitWait("k")
		e.route(next)
		c := e.claims(id)
		if len(c) != 2 || c[1]["event_id"] != float64(next) || c[1]["wait_seq"] != float64(2) {
			t.Fatalf("claims = %v, want w2 claimed by event %d", c, next)
		}
		if n := e.count(`SELECT count(*) FROM {s}consumed_events WHERE run_id = $1 AND step_id <> $2`, id, stepTrigger); n != 2 {
			t.Errorf("consumed events = %d, want one per wait", n)
		}
	})

	t.Run("the trigger", func(t *testing.T) {
		e := newEnv(t)
		e.publish(defSpec{name: "flow", steps: steps(
			fmt.Sprintf(`{"id":"w","type":"wait_for_event","event":%q,"timeout":"1h","on_timeout":"late","next":"got"}`, testEvent),
			endStep("got", "got"), endStep("late", "late"),
		)})
		id := e.start("flow", "k")
		e.mustPump(id)
		trigger := e.run(id).TriggerEventID
		e.route(trigger)
		if r := e.run(id); r.State != stateWaiting || r.LookbackPending || len(e.claims(id)) != 0 {
			t.Fatalf("run = %s lookback_pending %v, steps %v; want the trigger left out",
				r.State, r.LookbackPending, e.kinds(id))
		}

		// The next event of the type starts a run and satisfies the wait.
		// It is stored later, so the new run's wait does not see the first
		// trigger.
		e.clock.Advance(time.Minute)
		next := e.emit("k")
		e.route(next)
		e.claimedBy(id, next)
		second := RunID("flow", next)
		e.mustPump(second)
		if r := e.run(second); r.State != stateWaiting || len(e.claims(second)) != 0 {
			t.Fatalf("second run = %s, steps %v; want waiting without its trigger", r.State, e.kinds(second))
		}
	})
}

func TestLoopVisitsGetTheirOwnEffectsAndWaits(t *testing.T) {
	e := newEnv(t)
	var (
		mu   sync.Mutex
		keys []string
	)
	e.planner.action("bump", testAction{
		output: []executor.OutputFact{{Name: "count", Type: executor.FactInt}},
		execute: func(_ context.Context, in executor.Input) (map[string]any, error) {
			mu.Lock()
			defer mu.Unlock()
			keys = append(keys, in.IdemKey)
			return map[string]any{"count": len(keys)}, nil
		},
	})
	e.publish(defSpec{name: "flow", steps: steps(
		`{"id":"bump","type":"action","action":"bump","next":"w"}`,
		waitStep("w", "", "check", "check"),
		`{"id":"check","type":"switch","cases":[{"when":"facts.count < 2","next":"bump"}],"default":"done"}`,
		endStep("done", "finished"),
	)})
	id := e.start("flow", "k")
	e.mustPump(id) // bump, visit 1
	e.mustPump(id) // w, visit 1
	first := e.timers.last(t, id)
	ev1 := e.emitWait("k")
	e.route(ev1)
	e.mustPump(id) // check, then bump, visit 2
	e.mustPump(id) // w, visit 2
	second := e.timers.last(t, id)
	if first.WaitSeq != 1 || second.WaitSeq != 2 {
		t.Fatalf("timer sequences = %d, %d; want 1, 2", first.WaitSeq, second.WaitSeq)
	}
	if r := e.run(id); r.State != stateWaiting || r.WaitSeq != 2 {
		t.Fatalf("run = %s seq %d, want waiting on the second visit", r.State, r.WaitSeq)
	}

	// The first visit's timer fires late, after the run waits again.
	e.clock.Advance(time.Hour)
	if res := e.timeout(first); res != "done" || e.countKind(id, stepClaimLost) != 1 {
		t.Fatalf("late timer = %s, steps %v; want claim_lost", res, e.kinds(id))
	}
	if r := e.run(id); r.State != stateWaiting || r.WaitSeq != 2 {
		t.Fatalf("run = %s seq %d after the late timer, want still waiting", r.State, r.WaitSeq)
	}

	ev2 := e.emitWait("k")
	e.route(ev2)
	e.mustPump(id)
	r := e.run(id)
	if r.State != stateCompleted || *r.TerminalName != "finished" {
		t.Fatalf("run = %s %v, want completed", r.State, r.TerminalName)
	}
	want := []string{id.String() + ":bump:1", id.String() + ":bump:2"}
	if !reflect.DeepEqual(keys, want) {
		t.Errorf("effects = %v, want one per visit %v", keys, want)
	}
	c := e.claims(id)
	if len(c) != 2 || c[0]["event_id"] != float64(ev1) || c[1]["event_id"] != float64(ev2) {
		t.Errorf("claims = %v, want one event per visit", c)
	}
	f, err := decodeFrames(r.Frames)
	if err != nil || f.Visits["bump"] != 2 || f.Visits["w"] != 2 {
		t.Errorf("visits = %v (%v), want two of bump and w", f.Visits, err)
	}
	// The second visit's timer finds the run ended.
	if res := e.timeout(second); res != "done" || e.countKind(id, stepClaimLost) != 2 {
		t.Fatalf("timer after the end = %s, steps %v", res, e.kinds(id))
	}
}

func TestMatchErrorDoesNotBlockOtherWaits(t *testing.T) {
	t.Run("route", func(t *testing.T) {
		e := newEnv(t)
		e.publish(defSpec{name: "strict", steps: waitFlow("int(event.key) > 0")})
		e.publish(defSpec{name: "loose", steps: waitFlow("")})
		trigger := e.emit("k")
		e.route(trigger)
		strict, loose := RunID("strict", trigger), RunID("loose", trigger)
		e.mustPump(strict)
		e.mustPump(loose)

		bad := e.emitWait("x")
		e.route(bad)
		e.claimedBy(loose, bad)
		if r := e.run(strict); r.State != stateWaiting || e.countKind(strict, stepMatchError) != 1 {
			t.Fatalf("strict run = %s, steps %v; want waiting after a match_error", r.State, e.kinds(strict))
		}
		if a, b := e.meter.total("flowkit.match_errors", "strict"), e.meter.total("flowkit.match_errors", "loose"); a != 1 || b != 0 {
			t.Errorf("match errors = strict %d, loose %d", a, b)
		}

		good := e.emitWait("7")
		e.route(good)
		e.claimedBy(strict, good)
	})

	t.Run("look-back", func(t *testing.T) {
		e := newEnv(t)
		e.publish(defSpec{name: "strict", steps: waitFlow("int(event.key) > 0")})
		id := e.start("strict", "k")
		bad := e.emitWait("x")
		good := e.emitWait("3")
		e.mustPump(id)
		e.claimedBy(id, good)
		if n := e.countKind(id, stepMatchError); n != 1 {
			t.Errorf("match_error records = %d, want the one for event %d; steps %v", n, bad, e.kinds(id))
		}
		if n := e.meter.total("flowkit.match_errors", "strict"); n != 1 {
			t.Errorf("match errors = %d", n)
		}
	})
}

func TestLookbackReadsInPagesUpToItsRowLimit(t *testing.T) {
	e := newEnv(t)
	e.in.pageSize, e.in.maxRows = 2, 5
	e.publish(defSpec{name: "flow", steps: waitFlow("event.key == 'hit'")})

	// The fifth event is on the third page, the last within the limit.
	id := e.start("flow", "a")
	for range 4 {
		e.emitWait("miss")
	}
	hit := e.emitWait("hit")
	e.mustPump(id)
	e.claimedBy(id, hit)
	if n := e.meter.total("flowkit.lookback_truncated", "flow"); n != 0 {
		t.Fatalf("truncated = %d within the limit", n)
	}

	// The sixth is past it: the run waits for new events.
	e.clock.Advance(2 * time.Hour)
	id = e.start("flow", "b")
	for range 5 {
		e.emitWait("miss")
	}
	hit = e.emitWait("hit")
	e.mustPump(id)
	if r := e.run(id); r.State != stateWaiting || r.LookbackPending || len(e.claims(id)) != 0 {
		t.Fatalf("run = %s lookback_pending %v, steps %v; want waiting after a truncated look-back",
			r.State, r.LookbackPending, e.kinds(id))
	}
	if n := e.meter.total("flowkit.lookback_truncated", "flow"); n != 1 {
		t.Fatalf("truncated = %d, want 1", n)
	}
	e.route(hit)
	e.claimedBy(id, hit)
}

func TestKillSwitchDoesNotBlockClaims(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "flow", steps: waitFlow("")})
	id := e.start("flow", "k")
	e.mustPump(id)
	e.setEnabled("flow", false)

	ev := e.emitWait("k")
	e.route(ev)
	e.claimedBy(id, ev)
	e.mustPump(id)
	if r := e.run(id); r.State != stateRunning {
		t.Fatalf("run = %s, want left running while disabled", r.State)
	}
	if step, _ := e.at(id); step != "got" {
		t.Fatalf("run at %s, want got", step)
	}
	e.setEnabled("flow", true)
	e.mustPump(id)
	if r := e.run(id); r.State != stateCompleted || *r.TerminalName != "got" {
		t.Fatalf("run = %s %v, want completed at got", r.State, r.TerminalName)
	}
}

func TestADefinitionThatNoLongerCompilesFailsTheRunOnEveryPath(t *testing.T) {
	// lose makes the interpreter behave as an instance that lacks a
	// component the definition uses: nothing compiled, and compiling fails.
	lose := func(e *env) {
		e.planner.fail = errors.New("action enrol is not registered")
		e.in.plansMu.Lock()
		clear(e.in.plans)
		e.in.plansMu.Unlock()
	}
	cases := []struct {
		name string
		// fail parks the run, loses the definition and handles one job of
		// the run's.
		fail func(e *env, id ids.UUID)
	}{
		{"pump", func(e *env, id ids.UUID) {
			lose(e)
			e.mustPump(id)
		}},
		{"look-back", func(e *env, id ids.UUID) {
			e.in.hooks.afterWait = func(ids.UUID) error { return errInjected }
			if res := e.pump(id); res != "retry: "+errInjected.Error() {
				e.t.Fatalf("pump = %s, want the injected failure", res)
			}
			e.in.hooks.afterWait = nil
			lose(e)
			e.mustPump(id)
		}},
		{"timeout", func(e *env, id ids.UUID) {
			e.mustPump(id)
			lose(e)
			e.clock.Advance(2 * time.Hour)
			if res := e.timeout(e.timers.last(e.t, id)); res != "done" {
				e.t.Fatalf("timeout = %s", res)
			}
		}},
		{"route", func(e *env, id ids.UUID) {
			e.mustPump(id)
			lose(e)
			e.route(e.emitWait("k"))
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			e.publish(defSpec{name: "flow", steps: waitFlow("event.key == 'k'")})
			id := e.start("flow", "k")
			c.fail(e, id)
			r := e.run(id)
			term := e.terminal(id)
			if r.State != stateFailed || *r.TerminalName != TerminalFailed || term.StepID != StepRun {
				t.Fatalf("run = %s %v at %s, want failed %s at %s; steps %v",
					r.State, *r.TerminalName, term.StepID, TerminalFailed, StepRun, e.kinds(id))
			}
			if term.Error == nil || !strings.HasPrefix(*term.Error, "redacted: ") || !strings.Contains(*term.Error, "not registered") {
				t.Errorf("reason = %v, want the redacted compile error", term.Error)
			}
			if n := e.countKind(id, stepMatchError); n != 0 {
				t.Errorf("match_error records = %d, want none", n)
			}
		})
	}
}

func TestAMatchErrorIsRecordedOnce(t *testing.T) {
	e := newEnv(t)
	e.publish(defSpec{name: "strict", steps: waitFlow("int(event.key) > 0")})
	id := e.start("strict", "k")
	// The look-back records the failure, then the route of the same event
	// and a retry of that route find it recorded.
	bad := e.emitWait("x")
	e.mustPump(id)
	e.route(bad)
	e.route(bad)
	if n := e.countKind(id, stepMatchError); n != 1 {
		t.Fatalf("match_error records = %d, want 1; steps %v", n, e.kinds(id))
	}
	if n := e.meter.total("flowkit.match_errors", "strict"); n != 1 {
		t.Errorf("match errors counted = %d, want 1", n)
	}
	good := e.emitWait("2")
	e.route(good)
	e.claimedBy(id, good)
}

func TestRouteOffersAnEventToEveryPageOfWaits(t *testing.T) {
	e := newEnv(t)
	e.in.subsPage = 2
	e.publish(defSpec{name: "flow", steps: waitFlow("")})
	var runs []ids.UUID
	for i := range 5 {
		id := e.start("flow", fmt.Sprint(i))
		e.mustPump(id)
		runs = append(runs, id)
	}
	ev := e.emitWait("k")
	e.route(ev)
	for _, id := range runs {
		e.claimedBy(id, ev)
	}
}
