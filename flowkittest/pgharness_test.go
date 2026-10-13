package flowkittest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/g0-up/flowkit"
	"github.com/g0-up/flowkit/internal/testpg"
	"github.com/g0-up/flowkit/migrate"
)

// schedulerTick outlasts one tick of River's scheduler, which promotes
// due jobs every 5s: a job that stays queued past it was not due.
const schedulerTick = 6 * time.Second

// pgStudentRegistry registers the student events and actions.
func pgStudentRegistry(actions ...flowkit.ActionDef) *flowkit.Registry {
	reg := flowkit.NewRegistry()
	reg.MustAddEvent(flowkit.EventDef{
		Type: "student.registered",
		PayloadSchema: json.RawMessage(`{"type":"object","required":["student_id","email","vip"],"properties":{
			"student_id":{"type":"string"},"email":{"type":"string"},"vip":{"type":"boolean"}}}`),
		EntityKind:    "student",
		EntityIDField: "student_id",
	})
	reg.MustAddEvent(flowkit.EventDef{
		Type:          "student.logged_in",
		PayloadSchema: json.RawMessage(`{"type":"object","required":["student_id"],"properties":{"student_id":{"type":"string"}}}`),
		EntityKind:    "student",
		EntityIDField: "student_id",
	})
	for _, a := range actions {
		reg.MustAddAction(a)
	}
	return reg
}

// pgAction is an action counting its calls, failing the first failures
// of them.
type pgAction struct {
	mu       sync.Mutex
	calls    int
	failures int
}

func (a *pgAction) def(name string, idempotent bool, retry flowkit.RetryPolicy) flowkit.ActionDef {
	return flowkit.ActionDef{
		Name:         name,
		ParamsSchema: json.RawMessage(`{"type":"object"}`),
		Idempotent:   idempotent,
		Retry:        retry,
		Execute: func(context.Context, flowkit.ActionInput) (flowkit.ActionResult, error) {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.calls++
			if a.calls <= a.failures {
				return flowkit.ActionResult{}, errors.New("mail server unavailable")
			}
			return flowkit.ActionResult{}, nil
		},
	}
}

func (a *pgAction) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

func pgRegistered(studentID string, vip bool) flowkit.Event {
	return flowkit.Event{Type: "student.registered", Payload: map[string]any{
		"student_id": studentID, "email": studentID + "@example.com", "vip": vip,
	}}
}

// pgOnlyRun returns the id of the only run of definition def.
func pgOnlyRun(t *testing.T, runs *flowkit.Runs, def string) string {
	t.Helper()
	list, err := runs.List(context.Background(), flowkit.RunFilter{DefName: def})
	if err != nil {
		t.Fatalf("List(%s): %v", def, err)
	}
	if len(list) != 1 {
		t.Fatalf("List(%s) = %d runs, want 1: %+v", def, len(list), list)
	}
	return list[0].RunID
}

// pgClaimed reports whether the trace of runID has a claimed record.
func pgClaimed(t *testing.T, runs *flowkit.Runs, runID string) bool {
	t.Helper()
	tr, err := runs.Trace(context.Background(), runID)
	if err != nil {
		t.Fatalf("Trace(%s): %v", runID, err)
	}
	for _, s := range tr.Steps {
		if s.Kind == "claimed" {
			return true
		}
	}
	return false
}

const nudgeBody = `{
	"metamodel_version": 1,
	"trigger": {"event": "student.registered"},
	"steps": [
		{"id": "first_login", "type": "wait_for_event", "event": "student.logged_in",
		 "timeout": "72h", "on_timeout": "nudge", "next": "active"},
		{"id": "nudge", "type": "action", "action": "send_email",
		 "params": {"to": {"$expr": "event.email"}}, "next": "nudged"},
		{"id": "active", "type": "end", "name": "active"},
		{"id": "nudged", "type": "end", "name": "nudged"}
	]
}`

func TestAdvanceFiresAWaitTimeoutOnlyOnceDue(t *testing.T) {
	t.Parallel()
	email := &pgAction{}
	h := NewPGHarness(t, pgStudentRegistry(email.def("send_email", true, flowkit.RetryPolicy{})))
	runs := h.Engine().Runs()
	h.Publish("nudge", json.RawMessage(nudgeBody))
	h.Emit(pgRegistered("s-1", false))
	h.Advance(0)
	runID := pgOnlyRun(t, runs, "nudge")
	if v := h.Run(runID); v.State != "waiting" || v.WaitStep != "first_login" {
		t.Fatalf("after the event Run = %+v, want waiting at first_login", v)
	}

	h.Advance(71 * time.Hour)
	time.Sleep(schedulerTick)
	h.WaitIdle(0)
	if v := h.Run(runID); v.State != "waiting" || v.WaitStep != "first_login" {
		t.Fatalf("an hour before the timeout Run = %+v, want waiting at first_login", v)
	}
	if pgClaimed(t, runs, runID) || email.count() != 0 {
		t.Fatalf("the timeout fired an hour early: claimed %v, %d emails", pgClaimed(t, runs, runID), email.count())
	}

	h.Advance(time.Hour)
	if v := h.Run(runID); v.State != "completed" || v.TerminalName != "nudged" || v.WaitStep != "" {
		t.Fatalf("after the timeout Run = %+v, want completed nudged", v)
	}
	if !pgClaimed(t, runs, runID) {
		t.Error("the timeout did not record its claim of the wait")
	}
	if email.count() != 1 {
		t.Errorf("send_email ran %d times, want 1", email.count())
	}
	AssertTrace(t, runs, runID, StepExecutedTimes("nudge", 1), RunTerminal("nudged"))
	// A loose check only: each of these jumps outlasts River's leader TTL,
	// so the leader steps down and its successor restarts the periodic
	// enqueuer, which hides a schedule that catches up on missed passes.
	// TestJanitorScheduleSkipsMissedPasses in internal/sched guards that.
	if n := h.countJobs("flowkit.janitor"); n > 10 {
		t.Errorf("%d janitor jobs after three days, want a few, not one per 30s jumped", n)
	}
}

// countJobs counts the River jobs of kind.
func (h *PGHarness) countJobs(kind string) int {
	h.t.Helper()
	var n int
	if err := h.pool.QueryRow(context.Background(), "SELECT count(*) FROM "+h.table("river_job")+" WHERE kind = $1", kind).Scan(&n); err != nil {
		h.t.Fatalf("count %s jobs: %v", kind, err)
	}
	return n
}

const sendBody = `{
	"metamodel_version": 1,
	"trigger": {"event": "student.registered"},
	"steps": [
		{"id": "welcome", "type": "action", "action": "send_email",
		 "params": {"to": {"$expr": "event.email"}}, "next": "sent"},
		{"id": "sent", "type": "end", "name": "sent"}
	]
}`

func TestAdvanceRunsARetryOnlyOnceItsBackoffElapsed(t *testing.T) {
	t.Parallel()
	// The retry comes 9m after the failure, plus up to 10% of jitter.
	email := &pgAction{failures: 1}
	h := NewPGHarness(t, pgStudentRegistry(email.def("send_email", true,
		flowkit.RetryPolicy{MaxAttempts: 3, BaseBackoff: 9 * time.Minute, MaxBackoff: 9 * time.Minute})))
	runs := h.Engine().Runs()
	h.Publish("send", json.RawMessage(sendBody))
	h.Emit(pgRegistered("s-1", false))
	h.Advance(0)
	runID := pgOnlyRun(t, runs, "send")
	if n := email.count(); n != 1 {
		t.Fatalf("send_email ran %d times after the event, want 1", n)
	}

	h.Advance(8 * time.Minute)
	time.Sleep(schedulerTick)
	h.WaitIdle(0)
	if n := email.count(); n != 1 {
		t.Fatalf("send_email ran %d times before its backoff elapsed, want 1", n)
	}
	if v := h.Run(runID); v.State != "running" {
		t.Fatalf("before the retry Run = %+v, want running", v)
	}

	h.Advance(2 * time.Minute)
	if n := email.count(); n != 2 {
		t.Fatalf("send_email ran %d times after its backoff, want 2", n)
	}
	if v := h.Run(runID); v.State != "completed" || v.TerminalName != "sent" {
		t.Fatalf("after the retry Run = %+v, want completed sent", v)
	}
	AssertTrace(t, runs, runID, StepExecutedTimes("welcome", 1), RunTerminal("sent"))
}

func TestRunShowsFactsAndTheWaitStep(t *testing.T) {
	t.Parallel()
	email := &pgAction{}
	start := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	h := NewPGHarness(t, pgStudentRegistry(email.def("send_email", true, flowkit.RetryPolicy{})),
		HarnessStart(start))
	if got := h.Clock().Now(); !got.Equal(start) {
		t.Fatalf("Clock().Now() = %s, want the HarnessStart time %s", got, start)
	}
	h.Publish("nudge", json.RawMessage(nudgeBody))
	h.Emit(pgRegistered("s-1", false))
	h.WaitIdle(0)
	runID := pgOnlyRun(t, h.Engine().Runs(), "nudge")
	if got := pgStartedAt(t, h.Engine().Runs(), runID); !got.Equal(start) {
		t.Errorf("run started at %s, want the HarnessStart time %s", got, start)
	}
	v := h.Run(runID)
	if v.State != "waiting" || v.WaitStep != "first_login" || v.TerminalName != "" {
		t.Fatalf("Run = %+v, want waiting at first_login", v)
	}
	if v.Facts == nil {
		t.Errorf("Run.Facts = nil, want the run's facts object")
	}

	h.Emit(flowkit.Event{Type: "student.logged_in", Payload: map[string]any{"student_id": "s-1"}})
	h.WaitIdle(0)
	if v := h.Run(runID); v.State != "completed" || v.TerminalName != "active" || v.WaitStep != "" {
		t.Fatalf("after the login Run = %+v, want completed active", v)
	}
	if email.count() != 0 {
		t.Errorf("send_email ran %d times, want 0", email.count())
	}
}

// deadLetterBody sends a dead letter of each kind a run can produce.
const deadLetterBody = `{
	"metamodel_version": 1,
	"trigger": {"event": "student.registered"},
	"steps": [
		{"id": "call", "type": "action", "action": %q, "next": "done"},
		{"id": "done", "type": "end", "name": "done"}
	]
}`

// The trace shows a dead letter without naming it, so DeadLetter derives
// it; check the derivation against the dead letters the Engine stores.
// pgStartedAt returns when run runID started.
func pgStartedAt(t *testing.T, runs *flowkit.Runs, runID string) time.Time {
	t.Helper()
	list, err := runs.List(context.Background(), flowkit.RunFilter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, r := range list {
		if r.RunID == runID {
			return r.StartedAt
		}
	}
	t.Fatalf("run %s is not listed: %+v", runID, list)
	return time.Time{}
}

func TestHarnessConfigKeepsTheHarnessPoolSchemaClockAndRoles(t *testing.T) {
	t.Parallel()
	email := &pgAction{failures: 1}
	reg := pgStudentRegistry(email.def("send_email", true,
		flowkit.RetryPolicy{MaxAttempts: 2, BaseBackoff: time.Minute, MaxBackoff: time.Minute}))
	other := NewVirtualClock(time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC))
	start := time.Date(2030, 6, 1, 0, 0, 0, 0, time.UTC)
	h := NewPGHarness(t, reg, HarnessStart(start), HarnessConfig(flowkit.Config{
		Schema: "elsewhere",
		Clock:  other,
		Roles:  []flowkit.Role{flowkit.RoleWorker},
		// The cfg fields the harness leaves alone reach the Engine.
		RedactError: func(error) string { return "redacted" },
		Workers:     flowkit.WorkerConfig{MaxWorkers: 20},
	}))
	if h.Clock() == other || !h.Clock().Now().Equal(start) {
		t.Fatalf("Clock() = %s, want the harness clock at %s", h.Clock().Now(), start)
	}
	if h.schema == "elsewhere" {
		t.Fatal("the harness runs in the Schema of HarnessConfig")
	}
	if got, want := h.pool.Config().MaxConns, int32(20+harnessExtraConns); got != want {
		t.Errorf("pool MaxConns = %d, want %d for 20 workers", got, want)
	}
	h.Publish("send", json.RawMessage(sendBody))
	h.Emit(pgRegistered("s-1", false))
	h.Advance(0)
	runID := pgOnlyRun(t, h.Engine().Runs(), "send")
	// The retry becomes due on the harness clock and only a janitor leads
	// River's scheduler, so it runs only with the harness roles and clock.
	h.Advance(2 * time.Minute)
	if n := email.count(); n != 2 {
		t.Fatalf("send_email ran %d times, want 2: the retry did not run", n)
	}
	if got := pgStartedAt(t, h.Engine().Runs(), runID); !got.Equal(start) {
		t.Errorf("run started at %s, want the harness clock's %s", got, start)
	}
	if got := other.Now(); !got.Equal(time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("the Clock of HarnessConfig moved to %s", got)
	}
	tr, err := h.Engine().Runs().Trace(context.Background(), runID)
	if err != nil {
		t.Fatalf("Trace: %v", err)
	}
	redacted := false
	for _, s := range tr.Steps {
		if s.Kind == "attempt_failed" {
			redacted = s.Error == "redacted"
		}
	}
	if !redacted {
		t.Errorf("the failed attempt does not show the RedactError text:\n%s", formatTrace(tr))
	}
	AssertTrace(t, h.Engine().Runs(), runID, StepExecutedTimes("welcome", 1), RunTerminal("sent"))
}

func TestDeadLetterAgreesWithTheStoredDeadLetters(t *testing.T) {
	t.Parallel()
	exhaust, doubt, mismatch := &pgAction{failures: 10}, &pgAction{failures: 10}, &pgAction{}
	mismatchDef := mismatch.def("bad_output", true, flowkit.RetryPolicy{MaxAttempts: 1})
	mismatchDef.OutputFacts = []flowkit.OutputFact{{Name: "student.tagged", Type: flowkit.FactBool}}
	h := NewPGHarness(t, pgStudentRegistry(
		exhaust.def("exhaust", true, flowkit.RetryPolicy{MaxAttempts: 1}),
		doubt.def("doubt", false, flowkit.RetryPolicy{}),
		mismatchDef,
	))
	runs := h.Engine().Runs()
	for _, action := range []string{"exhaust", "doubt", "bad_output"} {
		h.Publish(action, json.RawMessage(fmt.Sprintf(deadLetterBody, action)))
	}
	h.Emit(pgRegistered("s-1", false))
	h.Advance(0)

	for _, tc := range []struct {
		def, stored  string
		match, other Matcher
	}{
		{"exhaust", "exhausted", DeadLetter("exhausted"), DeadLetter("effect_in_doubt")},
		{"doubt", "effect_in_doubt", DeadLetter("effect_in_doubt"), DeadLetter("exhausted")},
		{"bad_output", "", RunFailed("$action_failed"), DeadLetter("exhausted")},
	} {
		runID := pgOnlyRun(t, runs, tc.def)
		if got := h.storedDeadLetters(runID); got != tc.stored {
			t.Fatalf("%s: stored dead letters %q, want %q", tc.def, got, tc.stored)
		}
		tr, err := runs.Trace(context.Background(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if got := tc.match.check(tr); got != "" {
			t.Errorf("%s: %s does not match: %s\n%s", tc.def, tc.match.desc, got, formatTrace(tr))
		}
		if got := tc.other.check(tr); got == "" {
			t.Errorf("%s: %s matches\n%s", tc.def, tc.other.desc, formatTrace(tr))
		}
		if tc.def == "bad_output" && DeadLetter("effect_in_doubt").check(tr) == "" {
			t.Errorf("%s: dead letter effect_in_doubt matches\n%s", tc.def, formatTrace(tr))
		}
	}
}

// storedDeadLetters lists the kinds of the dead letters of run runID.
func (h *PGHarness) storedDeadLetters(runID string) string {
	h.t.Helper()
	var kinds string
	err := h.pool.QueryRow(context.Background(),
		"SELECT coalesce(string_agg(kind, ',' ORDER BY id), '') FROM "+h.table("dead_letters")+" WHERE run_id::text = $1",
		runID).Scan(&kinds)
	if err != nil {
		h.t.Fatalf("read dead letters of run %s: %v", runID, err)
	}
	return kinds
}

func (h *PGHarness) table(name string) string {
	return pgx.Identifier{h.schema, name}.Sanitize()
}

// stopTB records the first Fatalf and stops the call that made it.
type stopTB struct {
	testing.TB
	msg string
}

type stopSignal struct{}

func (f *stopTB) Helper() {}

func (f *stopTB) Fatalf(format string, args ...any) {
	f.msg = fmt.Sprintf(format, args...)
	panic(stopSignal{})
}

// fatalOf runs fn and returns the message it passed to Fatalf, or "" when
// it did not call it.
func (f *stopTB) fatalOf(fn func()) (msg string) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(stopSignal); !ok {
				panic(r)
			}
			msg = f.msg
		}
	}()
	fn()
	return ""
}

func TestWaitIdleReportsTheDueJobsAndAMissingLeader(t *testing.T) {
	t.Parallel()
	pool := testpg.Pool(t)
	schema := testpg.Schema(t, pool)
	if err := migrate.Run(context.Background(), pool, schema, migrate.Options{}); err != nil {
		t.Fatal(err)
	}
	ftb := &stopTB{TB: t}
	clock := NewVirtualClock(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	// No Engine runs: nothing works the jobs and no leader is elected.
	h := &PGHarness{t: ftb, clock: clock, pool: pool, schema: schema}
	insert := "INSERT INTO " + h.table("river_job") + " (args, kind, max_attempts, queue, state, scheduled_at) VALUES ('{}', $1, 1, $2, 'available', $3)"
	for _, job := range []struct {
		kind, queue string
		at          time.Time
	}{
		{"pump_run", "flowkit", clock.Now()},
		{"wait_timeout", "flowkit", clock.Now().Add(time.Hour)},
		{"other_app_job", "default", clock.Now()},
	} {
		if _, err := pool.Exec(context.Background(), insert, job.kind, job.queue, job.at); err != nil {
			t.Fatal(err)
		}
	}

	start := time.Now()
	msg := ftb.fatalOf(func() { h.WaitIdle(200 * time.Millisecond) })
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("WaitIdle(200ms) took %s", elapsed)
	}
	for _, want := range []string{"WaitIdle", "pump_run: available", "no River leader is elected"} {
		if !strings.Contains(msg, want) {
			t.Errorf("WaitIdle failure does not mention %q:\n%s", want, msg)
		}
	}
	for _, notDue := range []string{"wait_timeout", "other_app_job"} {
		if strings.Contains(msg, notDue) {
			t.Errorf("WaitIdle failure lists %s, which is not due or not flowkit's:\n%s", notDue, msg)
		}
	}

	if msg := ftb.fatalOf(func() { h.Advance(-time.Second) }); !strings.Contains(msg, "only moves forward") {
		t.Errorf("Advance(-1s) failure = %q, want a refusal", msg)
	}
	if msg := ftb.fatalOf(func() { h.Run("00000000-0000-0000-0000-000000000000") }); !strings.Contains(msg, "does not exist") {
		t.Errorf("Run of a missing run failure = %q", msg)
	}
}
