package flowkittest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/g0-up/flowkit"
	"github.com/jackc/pgx/v5"
)

// realAction is an action whose registered Execute fails the test: the Sim
// must run the action's stub instead.
func realAction(t *testing.T, name string, idempotent bool, retry flowkit.RetryPolicy) flowkit.ActionDef {
	return flowkit.ActionDef{
		Name:         name,
		ParamsSchema: json.RawMessage(`{"type":"object"}`),
		Idempotent:   idempotent,
		Retry:        retry,
		Execute: func(context.Context, flowkit.ActionInput) (flowkit.ActionResult, error) {
			t.Fatalf("the Sim ran the registered Execute of %s", name)
			return flowkit.ActionResult{}, nil
		},
	}
}

// stubRecorder is a stub that records its calls and fails the first ones
// with the errors in fails.
type stubRecorder struct {
	mu    sync.Mutex
	fails []error
	calls []flowkit.ActionInput
}

func (r *stubRecorder) stub(_ context.Context, in flowkit.ActionInput) (flowkit.ActionResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, in)
	if len(r.fails) > 0 {
		err := r.fails[0]
		r.fails = r.fails[1:]
		return flowkit.ActionResult{}, err
	}
	return flowkit.ActionResult{}, nil
}

func (r *stubRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

// newSimGolden runs the onboarding sample on a Sim whose actions would fail
// the test if their registered Execute ran.
func newSimGolden(t *testing.T) goldenEnv {
	s := NewSim(t, pgStudentRegistry(
		realAction(t, "send_email", true, flowkit.RetryPolicy{}),
		realAction(t, "notify_slack", true, flowkit.RetryPolicy{}),
	))
	ok := (&stubRecorder{}).stub
	s.StubAction("send_email", ok)
	s.StubAction("notify_slack", ok)
	return s
}

func TestSimRunsStubsInsteadOfTheRegisteredExecute(t *testing.T) {
	t.Parallel()
	// A short claim timeout bounds the wait should a registered Execute
	// run and stop its goroutine with t.Fatal.
	s := NewSim(t, pgStudentRegistry(realAction(t, "send_email", true, flowkit.RetryPolicy{})),
		SimConfig(flowkit.Config{ClaimTimeout: 5 * time.Second}),
		SimStart(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)))
	if got := s.Clock().Now(); !got.Equal(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Fatalf("Clock().Now() = %s, want the start option", got)
	}
	email := &stubRecorder{}
	s.StubAction("send_email", email.stub)
	s.Publish("send", json.RawMessage(sendBody))
	s.Emit(pgRegistered("s-1", false))
	if n := email.count(); n != 0 {
		t.Fatalf("the stub ran %d times before Advance", n)
	}
	s.Advance(0)

	runID := pgOnlyRun(t, s.Runs(), "send")
	if n := email.count(); n != 1 {
		t.Fatalf("the stub ran %d times, want 1", n)
	}
	in := email.calls[0]
	if in.RunID != runID || in.StepID != "welcome" || in.Params["to"] != "s-1@example.com" ||
		in.Scope.EntityKind != "student" || in.Scope.EntityID != "s-1" || !strings.HasPrefix(in.IdempotencyKey, runID+":welcome:") {
		t.Errorf("the stub got %+v", in)
	}
	AssertTrace(t, s.Runs(), runID, StepExecutedTimes("welcome", 1), RunTerminal("sent"))
}

func TestSimFailsTheTestOnAnActionWithoutAStub(t *testing.T) {
	t.Parallel()
	tb := &stopTB{TB: t}
	var s *Sim
	msg := tb.fatalOf(func() {
		s = NewSim(tb, pgStudentRegistry(
			realAction(t, "send_email", true, flowkit.RetryPolicy{}),
			realAction(t, "notify_slack", true, flowkit.RetryPolicy{}),
		))
		s.StubAction("notify_slack", (&stubRecorder{}).stub)
		s.Publish("onboarding", json.RawMessage(onboardingBody))
		s.Emit(pgRegistered("s-1", false))
		s.Advance(time.Hour)
	})
	if !strings.Contains(msg, `action "send_email" ran without a stub`) {
		t.Fatalf("Fatalf = %q, want it to name send_email", msg)
	}
	// Advance stopped at the failed attempt, before the clock moved.
	if s.Clock().Now().After(time.Now()) {
		t.Errorf("the clock moved on to %s after the failure", s.Clock().Now())
	}
	runs, err := s.Runs().List(context.Background(), flowkit.RunFilter{DefName: "onboarding"})
	if err != nil || len(runs) != 1 {
		t.Fatalf("List = %v, %v", runs, err)
	}
	AssertTrace(t, s.Runs(), runs[0].RunID, StepExecutedTimes("welcome", 0))
}

func TestSimStopsAtAStubThatCallsFailNow(t *testing.T) {
	t.Parallel()
	tb := &stopTB{TB: t}
	var s *Sim
	msg := tb.fatalOf(func() {
		s = NewSim(tb, pgStudentRegistry(realAction(t, "send_email", true, flowkit.RetryPolicy{})))
		// runtime.Goexit is what t.FailNow does to the stub's goroutine.
		s.StubAction("send_email", func(context.Context, flowkit.ActionInput) (flowkit.ActionResult, error) {
			runtime.Goexit()
			return flowkit.ActionResult{}, nil
		})
		s.Publish("send", json.RawMessage(sendBody))
		s.Emit(pgRegistered("s-1", false))
		s.Advance(time.Hour)
	})
	if !strings.Contains(msg, `the stub of action "send_email" stopped its goroutine`) {
		t.Fatalf("Fatalf = %q, want it to name send_email", msg)
	}
	AssertTrace(t, s.Runs(), pgOnlyRun(t, s.Runs(), "send"), StepExecutedTimes("welcome", 0))
}

func TestSimStubPanicIsAnActionPanic(t *testing.T) {
	t.Parallel()
	tb := &logTB{TB: t}
	s := NewSim(tb, pgStudentRegistry(realAction(t, "send_email", true,
		flowkit.RetryPolicy{MaxAttempts: 2, BaseBackoff: time.Minute, MaxBackoff: time.Minute})))
	calls := 0
	s.StubAction("send_email", func(context.Context, flowkit.ActionInput) (flowkit.ActionResult, error) {
		calls++
		if calls == 1 {
			panicTemplate()
		}
		return flowkit.ActionResult{}, nil
	})
	s.Publish("send", json.RawMessage(sendBody))
	s.Emit(pgRegistered("s-1", false))
	s.Advance(time.Hour)
	if calls != 2 {
		t.Fatalf("the stub ran %d times, want 2: a panic of an idempotent action is retried", calls)
	}
	AssertTrace(t, s.Runs(), pgOnlyRun(t, s.Runs(), "send"), StepExecutedTimes("welcome", 1), RunTerminal("sent"))
	// The executor sees the panic only once raised again: the log of the
	// stub's own stack is what names the line that panicked.
	if logs := tb.logged(); !strings.Contains(logs, `the stub of action "send_email" panicked: template missing`) ||
		!strings.Contains(logs, "flowkittest.panicTemplate(") {
		t.Errorf("the test log lacks the stack of the stub's panic:\n%s", logs)
	}
}

func TestSimFailsTheTestOnAStubThatOutlivesItsContext(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	tb := &stopTB{TB: t}
	var s *Sim
	msg := tb.fatalOf(func() {
		// The shortest claim timeout: the Sim waits 4s of real time, the
		// action deadline, for the stub.
		s = NewSim(tb, pgStudentRegistry(realAction(t, "send_email", true, flowkit.RetryPolicy{})),
			SimConfig(flowkit.Config{ClaimTimeout: 5 * time.Second}))
		s.StubAction("send_email", func(context.Context, flowkit.ActionInput) (flowkit.ActionResult, error) {
			<-release
			return flowkit.ActionResult{}, nil
		})
		s.Publish("send", json.RawMessage(sendBody))
		s.Emit(pgRegistered("s-1", false))
		s.Advance(time.Hour)
	})
	if !strings.Contains(msg, `the stub of action "send_email" did not return before its context ended`) {
		t.Fatalf("Fatalf = %q, want it to name send_email", msg)
	}
	AssertTrace(t, s.Runs(), pgOnlyRun(t, s.Runs(), "send"), StepExecutedTimes("welcome", 0))
}

// logTB records what the test logs.
type logTB struct {
	testing.TB
	mu   sync.Mutex
	logs []string
}

func (l *logTB) Logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logs = append(l.logs, fmt.Sprintf(format, args...))
}

func (l *logTB) logged() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.logs, "\n")
}

// panicTemplate panics in a frame of its own, for the logged stack to name.
func panicTemplate() { panic("template missing") }

func TestSimStubThatWasNotPerformedIsRetriedAfterItsBackoff(t *testing.T) {
	t.Parallel()
	// The retry comes 9m after the failure, plus up to 10% of jitter.
	s := NewSim(t, pgStudentRegistry(realAction(t, "send_email", false,
		flowkit.RetryPolicy{MaxAttempts: 3, BaseBackoff: 9 * time.Minute, MaxBackoff: 9 * time.Minute})))
	email := &stubRecorder{fails: []error{flowkit.NotPerformed(errors.New("mail server rejected the request"))}}
	s.StubAction("send_email", email.stub)
	s.Publish("send", json.RawMessage(sendBody))
	s.Emit(pgRegistered("s-1", false))
	s.Advance(0)
	runID := pgOnlyRun(t, s.Runs(), "send")

	s.Advance(8 * time.Minute)
	if n := email.count(); n != 1 {
		t.Fatalf("the stub ran %d times before its backoff elapsed, want 1", n)
	}
	s.Advance(2 * time.Minute)
	if n := email.count(); n != 2 {
		t.Fatalf("the stub ran %d times after its backoff, want 2", n)
	}
	if a, b := email.calls[0].IdempotencyKey, email.calls[1].IdempotencyKey; a != b {
		t.Errorf("the retry changed the idempotency key from %s to %s", a, b)
	}
	AssertTrace(t, s.Runs(), runID, StepExecutedTimes("welcome", 1), RunTerminal("sent"))
}

func TestSimStubFailureOfANonIdempotentActionIsInDoubt(t *testing.T) {
	t.Parallel()
	s := NewSim(t, pgStudentRegistry(realAction(t, "send_email", false, flowkit.RetryPolicy{MaxAttempts: 3})))
	email := &stubRecorder{fails: []error{errors.New("connection reset after the request was sent")}}
	s.StubAction("send_email", email.stub)
	s.Publish("send", json.RawMessage(sendBody))
	s.Emit(pgRegistered("s-1", false))
	s.Advance(0)
	s.Advance(24 * time.Hour)
	if n := email.count(); n != 1 {
		t.Fatalf("the stub ran %d times, want 1: an effect in doubt is not retried", n)
	}
	AssertTrace(t, s.Runs(), pgOnlyRun(t, s.Runs(), "send"), DeadLetter("effect_in_doubt"), StepExecutedTimes("welcome", 0))
}

func TestSimStubThatKeepsFailingExhaustsItsAttempts(t *testing.T) {
	t.Parallel()
	s := NewSim(t, pgStudentRegistry(realAction(t, "send_email", true,
		flowkit.RetryPolicy{MaxAttempts: 2, BaseBackoff: time.Minute, MaxBackoff: time.Minute})))
	down := errors.New("mail server unavailable")
	email := &stubRecorder{fails: []error{down, down, down}}
	s.StubAction("send_email", email.stub)
	s.Publish("send", json.RawMessage(sendBody))
	s.Emit(pgRegistered("s-1", false))
	s.Advance(0)
	s.Advance(time.Hour)
	if n := email.count(); n != 2 {
		t.Fatalf("the stub ran %d times, want 2", n)
	}
	AssertTrace(t, s.Runs(), pgOnlyRun(t, s.Runs(), "send"), DeadLetter("exhausted"), RunFailed("$action_failed"))
}

func TestSimWaitTimesOutOnlyOnceDue(t *testing.T) {
	t.Parallel()
	s := NewSim(t, pgStudentRegistry(realAction(t, "send_email", true, flowkit.RetryPolicy{})))
	email := &stubRecorder{}
	s.StubAction("send_email", email.stub)
	s.Publish("nudge", json.RawMessage(nudgeBody))
	s.Emit(pgRegistered("s-1", false))
	s.Advance(0)
	runID := pgOnlyRun(t, s.Runs(), "nudge")

	s.Advance(72*time.Hour - time.Second)
	AssertTrace(t, s.Runs(), runID, RunWaiting("first_login"))
	if n := email.count(); n != 0 {
		t.Fatalf("the timeout fired early: the stub ran %d times", n)
	}
	s.Advance(time.Second)
	AssertTrace(t, s.Runs(), runID, StepExecutedTimes("nudge", 1), RunTerminal("nudged"))
	if n := email.count(); n != 1 {
		t.Errorf("the stub ran %d times, want 1", n)
	}
}

func TestSimEngineHasNoJobClient(t *testing.T) {
	t.Parallel()
	s := NewSim(t, pgStudentRegistry())
	if err := s.engine.Start(context.Background()); err == nil {
		t.Error("Start of the Sim Engine succeeded")
	}
	// The Sim Engine has no pool, so it cannot join a caller's pgx.Tx.
	err := s.engine.Emit(context.Background(), struct{ pgx.Tx }{}, pgRegistered("s-1", false))
	if err == nil || !strings.Contains(err.Error(), "simulator") {
		t.Errorf("Emit on the Sim Engine = %v, want an error pointing to the Sim", err)
	}
}
