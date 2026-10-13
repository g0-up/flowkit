package interpreter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/g0-up/flowkit/internal/executor"
	"github.com/g0-up/flowkit/internal/expr"
	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/interpreter/model"
	"github.com/g0-up/flowkit/internal/store"
)

// recorder is an action body that records its inputs and returns output.
type recorder struct {
	mu     sync.Mutex
	inputs []executor.Input
	output map[string]any
	err    error // returned instead of output while set
}

func (r *recorder) execute(_ context.Context, in executor.Input) (map[string]any, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inputs = append(r.inputs, in)
	if r.err != nil {
		return nil, r.err
	}
	return r.output, nil
}

func (r *recorder) calls() []executor.Input {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]executor.Input(nil), r.inputs...)
}

func (r *recorder) fail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

const enrolParams = `{"type":"object","properties":{"student":{"type":"integer"},"tag":{"type":"string"}},"required":["student","tag"],"additionalProperties":false}`

func TestPumpRunsOneActionPerTurn(t *testing.T) {
	e := newEnv(t)
	enrol := &recorder{output: map[string]any{"seat": 4}}
	e.planner.action("enrol", testAction{
		params: enrolParams, output: []executor.OutputFact{{Name: "seat", Type: executor.FactInt}}, execute: enrol.execute,
	})
	e.publish(defSpec{name: "flow", steps: steps(
		`{"id":"route","type":"switch","cases":[{"when":"has(event.branch) && event.branch == 'hn'","next":"enrol"}],"default":"done"}`,
		`{"id":"enrol","type":"action","action":"enrol","params":{"student":{"$expr":"has(event.student) ? event.student : 0"},"tag":"x"},"next":"done"}`,
		endStep("done", "finished"),
	)})
	id := e.startWith("flow", `{"key":"k","student":7,"branch":"hn"}`)

	e.mustPump(id)
	if got, want := e.kinds(id), []string{"route:branched", "enrol:entered", "enrol:executed"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("steps after the first pump = %v, want %v", got, want)
	}
	calls := enrol.calls()
	if len(calls) != 1 {
		t.Fatalf("enrol ran %d times, want 1", len(calls))
	}
	in := calls[0]
	key := id.String() + ":enrol:1"
	switch {
	case in.IdemKey != key || in.RunID != id.String() || in.StepID != "enrol":
		t.Errorf("input = %+v, want key %s", in, key)
	case !reflect.DeepEqual(in.Params, map[string]any{"student": int64(7), "tag": "x"}):
		t.Errorf("params = %#v, want typed by the params schema", in.Params)
	case in.Scope.EntityKind != "student" || in.Scope.EntityID != "7" || in.Scope.Extra["branch"] != "hn":
		t.Errorf("scope = %+v", in.Scope)
	}
	r := e.run(id)
	if step, entered := e.at(id); step != "done" || entered || r.State != stateRunning || r.Rev != 3 {
		t.Errorf("run at %s (entered %v), state %s, rev %d; want done, running, rev 3", step, entered, r.State, r.Rev)
	}
	var facts map[string]any
	if err := json.Unmarshal(r.Facts, &facts); err != nil || facts["seat"] != float64(4) {
		t.Errorf("facts = %s (%v), want the output merged", r.Facts, err)
	}
	if n := e.pumps.count(id); n != 2 {
		t.Errorf("pumps enqueued = %d, want the start's and the executed action's", n)
	}
	exec := e.steps(id)[2]
	var snap map[string]any
	if err := json.Unmarshal(exec.Snapshot, &snap); err != nil || snap["visit"] != float64(1) || snap["idem_key"] != key ||
		snap["cached"] != false || snap["next"] != "done" {
		t.Errorf("executed snapshot = %s (%v)", exec.Snapshot, err)
	}

	e.mustPump(id)
	r = e.run(id)
	if r.State != stateCompleted || !r.Terminal || r.TerminalName == nil || *r.TerminalName != "finished" {
		t.Errorf("run = state %s terminal %v name %v, want completed by the end step", r.State, r.Terminal, r.TerminalName)
	}
	if len(enrol.calls()) != 1 {
		t.Error("the end pump ran the action again")
	}
	e.mustPump(id) // a terminal run is left alone
	if n := len(e.steps(id)); n != 4 {
		t.Errorf("steps = %v after pumping a terminal run", e.kinds(id))
	}
}

func TestPumpSkipsAnActionWhoseConditionIsFalse(t *testing.T) {
	e := newEnv(t)
	enrol := &recorder{}
	e.planner.action("enrol", testAction{execute: enrol.execute})
	e.publish(defSpec{name: "flow", steps: steps(
		`{"id":"enrol","type":"action","action":"enrol","when":"has(event.branch) && event.branch == 'hn'","next":"done"}`,
		endStep("done", "finished"),
	)})
	id := e.start("flow", "k")
	e.mustPump(id)
	if got, want := e.kinds(id), []string{"enrol:skipped", "done:terminal"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("steps = %v, want %v", got, want)
	}
	if n := len(enrol.calls()); n != 0 {
		t.Errorf("skipped action ran %d times", n)
	}
	if r := e.run(id); r.State != stateCompleted {
		t.Errorf("state = %s, want completed in the same pump", r.State)
	}
}

func TestPumpFailsOnParamsOutsideTheSchema(t *testing.T) {
	e := newEnv(t)
	enrol := &recorder{}
	e.planner.action("enrol", testAction{params: enrolParams, execute: enrol.execute})
	e.publish(defSpec{name: "flow", steps: steps(
		`{"id":"enrol","type":"action","action":"enrol","params":{"student":{"$expr":"event.key"},"tag":"x"},"next":"done"}`,
		endStep("done", "finished"),
	)})
	id := e.start("flow", "not-a-number")
	e.mustPump(id)
	if n := len(enrol.calls()); n != 0 {
		t.Fatalf("Execute ran %d times with invalid params", n)
	}
	r := e.run(id)
	term := e.terminal(id)
	switch {
	case r.State != stateFailed || *r.TerminalName != TerminalFailed:
		t.Errorf("run = %s %v, want failed %s", r.State, *r.TerminalName, TerminalFailed)
	case term.StepID != "enrol" || term.Error == nil || !strings.HasPrefix(*term.Error, "redacted: ") ||
		!strings.Contains(*term.Error, "params schema"):
		t.Errorf("terminal step = %s %v, want the redacted params error at enrol", term.StepID, term.Error)
	}
}

func TestPumpMapsActionOutcomes(t *testing.T) {
	flow := steps(
		`{"id":"act","type":"action","action":"act","next":"done"}`,
		endStep("done", "finished"),
	)

	t.Run("failed for good", func(t *testing.T) {
		e := newEnv(t)
		act := &recorder{err: errors.New("card declined")}
		e.planner.action("act", testAction{retry: executor.RetryPolicy{MaxAttempts: 1}, execute: act.execute})
		e.publish(defSpec{name: "flow", steps: flow})
		id := e.start("flow", "k")
		e.mustPump(id)
		r := e.run(id)
		if r.State != stateFailed || *r.TerminalName != TerminalActionFailed {
			t.Fatalf("run = %s %v, want failed %s", r.State, *r.TerminalName, TerminalActionFailed)
		}
		key := id.String() + ":act:1"
		if n := e.count(`SELECT count(*) FROM {s}dead_letters WHERE kind = 'exhausted' AND ref = $1 AND run_id = $2
			AND raw->>'idem_key' = $1 AND note LIKE 'redacted: %'`, key, id); n != 1 {
			t.Errorf("exhausted dead letters for %s = %d, want 1", key, n)
		}
	})

	t.Run("output outside its facts", func(t *testing.T) {
		e := newEnv(t)
		act := &recorder{output: map[string]any{"seat": "front"}}
		e.planner.action("act", testAction{
			output: []executor.OutputFact{{Name: "seat", Type: executor.FactInt}}, execute: act.execute,
		})
		e.publish(defSpec{name: "flow", steps: flow})
		id := e.start("flow", "k")
		e.mustPump(id)
		if r := e.run(id); r.State != stateFailed || *r.TerminalName != TerminalActionFailed {
			t.Fatalf("run = %s %v, want failed %s", r.State, *r.TerminalName, TerminalActionFailed)
		}
		if n := e.count(`SELECT count(*) FROM {s}dead_letters`); n != 0 {
			t.Errorf("%d dead letters, want none for a done action", n)
		}
	})

	t.Run("retried later", func(t *testing.T) {
		e := newEnv(t)
		act := &recorder{err: errors.New("try again"), output: map[string]any{}}
		e.planner.action("act", testAction{
			retry: executor.RetryPolicy{MaxAttempts: 3, BaseBackoff: time.Minute}, execute: act.execute,
		})
		e.publish(defSpec{name: "flow", steps: flow})
		id := e.start("flow", "k")
		if res := e.pump(id); !strings.HasPrefix(res, "snooze ") {
			t.Fatalf("pump = %s, want a snooze", res)
		}
		if step, entered := e.at(id); step != "act" || !entered || e.run(id).State != stateRunning {
			t.Fatalf("run at %s (entered %v), want still entered at act", step, entered)
		}
		act.fail(nil)
		e.clock.Advance(time.Hour)
		e.mustPump(id)
		calls := act.calls()
		if len(calls) != 2 || calls[0].IdemKey != calls[1].IdemKey {
			t.Fatalf("calls = %+v, want two attempts of one visit", calls)
		}
		if step, _ := e.at(id); step != "done" {
			t.Errorf("run at %s after the retry, want done", step)
		}
	})
}

func TestKillSwitchStopsPumpsAndKeepsTheFrame(t *testing.T) {
	e := newEnv(t)
	first, second := &recorder{output: map[string]any{}}, &recorder{output: map[string]any{}}
	e.planner.action("first", testAction{execute: first.execute})
	e.planner.action("second", testAction{execute: second.execute})
	e.publish(defSpec{name: "flow", steps: steps(
		`{"id":"a","type":"action","action":"first","next":"b"}`,
		`{"id":"b","type":"action","action":"second","next":"done"}`,
		endStep("done", "finished"),
	)})
	id := e.start("flow", "k")
	e.mustPump(id)

	e.setEnabled("flow", false)
	before := e.run(id)
	e.mustPump(id)
	after := e.run(id)
	if after.Rev != before.Rev || after.State != stateRunning || string(after.Frames) != string(before.Frames) {
		t.Fatalf("disabled pump changed the run: rev %d -> %d, state %s", before.Rev, after.Rev, after.State)
	}
	if n := len(second.calls()); n != 0 {
		t.Fatalf("second ran %d times while disabled", n)
	}
	if got := e.kinds(id); len(got) != 2 {
		t.Fatalf("steps = %v, want nothing recorded while disabled", got)
	}

	e.setEnabled("flow", true)
	e.mustPump(id)
	calls := second.calls()
	if len(calls) != 1 || calls[0].IdemKey != id.String()+":b:1" {
		t.Fatalf("second calls = %+v, want one at visit 1", calls)
	}
	if len(first.calls()) != 1 {
		t.Error("first ran again after enabling")
	}
	e.mustPump(id)
	if r := e.run(id); r.State != stateCompleted {
		t.Errorf("state = %s, want completed", r.State)
	}
}

func TestConcurrentPumpsRunTheActionOnce(t *testing.T) {
	e := newEnv(t)
	act := &recorder{output: map[string]any{}}
	e.planner.action("act", testAction{execute: act.execute})
	e.publish(defSpec{name: "flow", steps: steps(
		`{"id":"act","type":"action","action":"act","next":"done"}`,
		endStep("done", "finished"),
	)})
	id := e.start("flow", "k")

	// Both pumps read the run before either writes it.
	var read sync.WaitGroup
	read.Add(2)
	e.in.hooks.afterRead = func(ids.UUID) {
		read.Done()
		read.Wait()
	}
	results := make([]string, 2)
	var wg sync.WaitGroup
	for i := range results {
		wg.Go(func() { results[i] = e.pump(id) })
	}
	wg.Wait()
	e.in.hooks.afterRead = nil

	for _, res := range results {
		if res != "done" {
			t.Errorf("pump = %s, want done", res)
		}
	}
	if n := len(act.calls()); n != 1 {
		t.Errorf("action ran %d times, want 1", n)
	}
	for kind, want := range map[string]int{stepEntered: 1, stepExecuted: 1, stepStalePump: 1} {
		if n := e.countKind(id, kind); n != want {
			t.Errorf("%s steps = %d, want %d; steps %v", kind, n, want, e.kinds(id))
		}
	}
}

func TestEnteredActionKeepsItsBranchAfterACrash(t *testing.T) {
	e := newEnv(t)
	var tier atomic.Value
	tier.Store("gold")
	var resolved atomic.Int32
	e.planner.fact(testFact{name: "tier", typ: expr.String,
		resolve: func(ctx context.Context, q store.ReadQuerier, _ expr.Scope) (any, error) {
			resolved.Add(1)
			var v string
			err := q.QueryRow(ctx, "SELECT $1::text", tier.Load()).Scan(&v)
			return v, err
		}})
	gift, plain := &recorder{output: map[string]any{}}, &recorder{output: map[string]any{}}
	e.planner.action("gift", testAction{execute: gift.execute})
	e.planner.action("plain", testAction{execute: plain.execute})
	e.publish(defSpec{name: "flow", steps: steps(
		`{"id":"pick","type":"switch","cases":[{"when":"facts.tier == 'gold'","next":"gift"}],"default":"plain"}`,
		`{"id":"gift","type":"action","action":"gift","params":{"tier":{"$expr":"facts.tier"},"at":{"$expr":"string(now)"}},"next":"done"}`,
		`{"id":"plain","type":"action","action":"plain","next":"done"}`,
		endStep("done", "finished"),
	)})
	id := e.start("flow", "k")

	e.in.hooks.afterEnter = func(ids.UUID) error { return errInjected }
	if res := e.pump(id); !strings.Contains(res, errInjected.Error()) {
		t.Fatalf("pump = %s, want the injected crash", res)
	}
	e.in.hooks.afterEnter = nil
	if step, entered := e.at(id); step != "gift" || !entered {
		t.Fatalf("run at %s (entered %v), want entered at gift", step, entered)
	}
	if len(gift.calls()) != 0 {
		t.Fatal("the crashed pump ran the action")
	}

	tier.Store("basic")
	e.clock.Advance(time.Hour)
	calls := resolved.Load()
	e.mustPump(id)
	if resolved.Load() != calls {
		t.Error("the pump of an entered action resolved the facts again")
	}
	if n := len(plain.calls()); n != 0 {
		t.Fatalf("plain ran %d times: the branch changed with the facts", n)
	}
	got := gift.calls()
	if len(got) != 1 {
		t.Fatalf("gift ran %d times, want 1", len(got))
	}
	if got[0].Params["tier"] != "gold" || got[0].Params["at"] != "2026-10-12T09:30:00Z" {
		t.Errorf("params = %v, want the facts and time of the entry", got[0].Params)
	}
	if got, want := e.kinds(id), []string{"pick:branched", "gift:entered", "gift:executed"}; !reflect.DeepEqual(got, want) {
		t.Errorf("steps = %v, want %v", got, want)
	}
}

func TestFactResolverIsBoundedReadOnlyAndRecovered(t *testing.T) {
	cases := []struct {
		name    string
		resolve func(e *env) func(context.Context, store.ReadQuerier, expr.Scope) (any, error)
		want    []string // any of them
	}{
		{"slow query", func(*env) func(context.Context, store.ReadQuerier, expr.Scope) (any, error) {
			return func(ctx context.Context, q store.ReadQuerier, _ expr.Scope) (any, error) {
				var v string
				err := q.QueryRow(ctx, "SELECT pg_sleep(5)::text").Scan(&v)
				return v, err
			}
		}, []string{"statement timeout", "deadline exceeded"}},
		{"slow resolver", func(*env) func(context.Context, store.ReadQuerier, expr.Scope) (any, error) {
			return func(ctx context.Context, _ store.ReadQuerier, _ expr.Scope) (any, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			}
		}, []string{"deadline exceeded"}},
		{"write", func(e *env) func(context.Context, store.ReadQuerier, expr.Scope) (any, error) {
			return func(ctx context.Context, q store.ReadQuerier, _ expr.Scope) (any, error) {
				var id int64
				err := q.QueryRow(ctx, e.sql(`INSERT INTO {s}domain_events (type, payload, occurred_at, inserted_at)
					VALUES ('x', '{}', $1, $1) RETURNING id`), t0).Scan(&id)
				return id, err
			}
		}, []string{"read-only transaction"}},
		{"panic", func(*env) func(context.Context, store.ReadQuerier, expr.Scope) (any, error) {
			return func(context.Context, store.ReadQuerier, expr.Scope) (any, error) {
				panic("tier lookup broke")
			}
		}, []string{`fact "tier": resolver panicked: tier lookup broke`}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			e.in.factTimeout = 200 * time.Millisecond
			e.planner.fact(testFact{name: "tier", typ: expr.String, resolve: c.resolve(e)})
			e.publish(defSpec{name: "flow", steps: steps(
				`{"id":"pick","type":"switch","cases":[{"when":"facts.tier == 'gold'","next":"done"}],"default":"done"}`,
				endStep("done", "finished"),
			)})
			id := e.start("flow", "k")
			e.mustPump(id)
			r := e.run(id)
			term := e.terminal(id)
			if r.State != stateFailed || *r.TerminalName != TerminalFailed || term.StepID != StepRun || term.Error == nil {
				t.Fatalf("run = %s %v at %s, want failed %s", r.State, *r.TerminalName, term.StepID, TerminalFailed)
			}
			if !strings.HasPrefix(*term.Error, "redacted: ") || !containsAny(*term.Error, c.want) {
				t.Errorf("reason = %q, want a redacted error with one of %q", *term.Error, c.want)
			}
			if n := e.count(`SELECT count(*) FROM {s}domain_events WHERE type = 'x'`); n != 0 {
				t.Errorf("the resolver wrote %d rows", n)
			}
		})
	}
}

func containsAny(s string, subs []string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func TestPumpFailsARunItCannotWalk(t *testing.T) {
	t.Run("definition no longer compiles", func(t *testing.T) {
		e := newEnv(t)
		e.publish(defSpec{name: "flow"})
		id := e.start("flow", "k")
		e.planner.fail = errors.New("action enrol is not registered")
		e.mustPump(id)
		r := e.run(id)
		if term := e.terminal(id); r.State != stateFailed || *r.TerminalName != TerminalFailed ||
			!strings.Contains(*term.Error, "not registered") {
			t.Fatalf("run = %s %v, error %v", r.State, *r.TerminalName, term.Error)
		}
	})

	t.Run("loop without an action or wait", func(t *testing.T) {
		e := newEnv(t)
		e.publish(defSpec{name: "flow", steps: steps(
			`{"id":"a","type":"switch","cases":[{"when":"false","next":"done"}],"default":"b"}`,
			`{"id":"b","type":"switch","cases":[{"when":"event.key == 'out'","next":"done"}],"default":"a"}`,
			endStep("done", "finished"),
		)})
		id := e.start("flow", "k")
		e.mustPump(id)
		if term := e.terminal(id); e.run(id).State != stateFailed || term.StepID != "a" ||
			!strings.Contains(*term.Error, "reached twice") {
			t.Fatalf("terminal = %s %v, want a failure at a", term.StepID, term.Error)
		}
		if got := e.kinds(id); !reflect.DeepEqual(got, []string{"a:branched", "b:branched", "a:terminal"}) {
			t.Errorf("steps = %v", got)
		}
	})

	t.Run("missing run", func(t *testing.T) {
		e := newEnv(t)
		if res := e.pump(ids.NewV4()); res != "done" {
			t.Fatalf("pump of a missing run = %s", res)
		}
	})
}

// TestPumpFinishAndCancelPreviousTakeLocksInOneOrder races the pump that
// ends a run, completing or failing it after recording a walked step,
// with the route of a newer trigger that cancels the run. Each round holds
// the key's serial lock while both queue on it, the pump first in some
// rounds and the route first in others, then releases it. Both take the
// serial lock before the run row, so neither deadlocks, whichever goes
// first.
func TestPumpFinishAndCancelPreviousTakeLocksInOneOrder(t *testing.T) {
	e := newEnv(t)
	e.planner.action("noop", testAction{execute: (&recorder{output: map[string]any{}}).execute})
	e.publish(defSpec{name: "flow", policy: model.PolicyCancelPrevious, serialKey: "event.key", steps: steps(
		`{"id":"pick","type":"switch","cases":[{"when":"event.key.startsWith('f')","next":"bad"}],"default":"done"}`,
		`{"id":"bad","type":"action","action":"noop","when":"int(event.key) > 0","next":"done"}`,
		endStep("done", "finished"),
	)})
	ctx := context.Background()
	const rounds = 200
	for i := range rounds {
		failing, routeFirst := i%2 == 1, i%4 >= 2
		key := fmt.Sprintf("e%d", i)
		if failing {
			key = fmt.Sprintf("f%d", i) // fails on bad's condition
		}
		first := e.start("flow", key)
		newer := e.emit(key)

		release := e.holdSerial("flow", key)
		var done sync.WaitGroup
		var pumped string
		var routed error
		ends := []func(){
			func() { pumped = e.pump(first) },
			func() { routed = e.in.RouteEvent(ctx, newer) },
		}
		if routeFirst {
			ends[0], ends[1] = ends[1], ends[0]
		}
		for n, end := range ends {
			done.Go(end)
			e.awaitLockWaiters(n + 1)
		}
		release()
		done.Wait()

		if pumped != "done" || routed != nil {
			t.Fatalf("round %d: pump %s, route %v", i, pumped, routed)
		}
		r := e.run(first)
		want := stateCompleted
		switch {
		case routeFirst:
			want = stateCancelled
			if n := e.countKind(first, stepStalePump); n != 1 {
				t.Fatalf("round %d: %d stale_pump records after the cancel; steps %v", i, n, e.kinds(first))
			}
		case failing:
			want = stateFailed
		}
		if r.State != want {
			t.Fatalf("round %d: first run %s, want %s", i, r.State, want)
		}
		if !e.runExists(RunID("flow", newer)) {
			t.Fatalf("round %d: the newer trigger started no run", i)
		}
	}
}
