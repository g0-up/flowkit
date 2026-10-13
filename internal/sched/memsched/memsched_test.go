package memsched_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/sched"
	"github.com/g0-up/flowkit/internal/sched/memsched"
	"github.com/g0-up/flowkit/internal/store"
	"github.com/g0-up/flowkit/internal/store/memstore"
)

var t0 = time.Date(2026, 10, 12, 9, 30, 0, 0, time.UTC)

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

// call is one handler invocation.
type call struct {
	kind string
	args any
	at   time.Time
}

// handlers records every job and answers with result, which defaults to
// Done. Janitor passes are recorded and answered by result only when
// janitor is set.
type handlers struct {
	clock   *clock
	janitor bool
	result  func(c call) sched.Result
	calls   []call
}

func (h *handlers) record(kind string, args any) sched.Result {
	if kind == sched.KindJanitor && !h.janitor {
		return sched.Done()
	}
	c := call{kind: kind, args: args, at: h.clock.Now()}
	h.calls = append(h.calls, c)
	if h.result != nil {
		return h.result(c)
	}
	return sched.Done()
}

func (h *handlers) Route(_ context.Context, a sched.RouteEventArgs) sched.Result {
	return h.record(sched.KindRouteEvent, a)
}

func (h *handlers) Pump(_ context.Context, a sched.PumpRunArgs) sched.Result {
	return h.record(sched.KindPumpRun, a)
}

func (h *handlers) Timeout(_ context.Context, a sched.WaitTimeoutArgs) sched.Result {
	return h.record(sched.KindWaitTimeout, a)
}

func (h *handlers) Janitor(_ context.Context, a sched.JanitorArgs) sched.Result {
	return h.record(sched.KindJanitor, a)
}

type fixture struct {
	t     *testing.T
	ctx   context.Context
	store *memstore.Store
	clock *clock
	h     *handlers
	s     *memsched.Scheduler
}

func newFixture(t *testing.T, interrupt func() error) *fixture {
	t.Helper()
	f := &fixture{t: t, ctx: context.Background(), store: memstore.New(), clock: &clock{now: t0}}
	f.h = &handlers{clock: f.clock}
	s, err := memsched.New(memsched.Options{
		Store:     f.store,
		Clock:     f.clock,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Interrupt: interrupt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(f.h, sched.NewTiming(5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	f.s = s
	return f
}

// commit runs fn in a transaction and commits it.
func (f *fixture) commit(fn func(tx store.Tx)) {
	f.t.Helper()
	tx, err := f.store.Begin(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	fn(tx)
	if err := tx.Commit(f.ctx); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) ok(err error) {
	f.t.Helper()
	if err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) advance(d time.Duration) {
	f.t.Helper()
	f.ok(f.s.Advance(f.ctx, d))
}

func (f *fixture) wantCalls(want ...call) {
	f.t.Helper()
	got := f.h.calls
	if len(got) != len(want) {
		f.t.Fatalf("calls = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i].kind != want[i].kind || got[i].args != want[i].args || !got[i].at.Equal(want[i].at) {
			f.t.Fatalf("call %d = %+v, want %+v (all calls %+v)", i, got[i], want[i], got)
		}
	}
}

func TestJobsExistOnlyOnceCommitted(t *testing.T) {
	f := newFixture(t, nil)
	tx, err := f.store.Begin(f.ctx)
	f.ok(err)
	f.ok(f.s.EnqueueRoute(f.ctx, tx, 1))
	if n := len(f.s.Pending()); n != 0 {
		t.Fatalf("%d jobs queued before the commit", n)
	}
	f.ok(tx.Rollback(f.ctx))
	f.commit(func(tx store.Tx) { f.ok(f.s.EnqueueRoute(f.ctx, tx, 2)) })
	f.advance(0)
	f.wantCalls(call{sched.KindRouteEvent, sched.RouteEventArgs{EventID: 2}, t0})
}

func TestEnqueueRefusesAForeignTransaction(t *testing.T) {
	f := newFixture(t, nil)
	tx, err := memstore.New().Begin(f.ctx)
	f.ok(err)
	defer func() { _ = tx.Rollback(f.ctx) }()
	if err := f.s.EnqueuePump(f.ctx, tx, ids.UUID{1}); !errors.Is(err, store.ErrForeignTx) {
		t.Fatalf("EnqueuePump with a foreign transaction = %v", err)
	}
}

func TestAdvanceRunsJobsInDueOrderAtTheirTime(t *testing.T) {
	f := newFixture(t, nil)
	run := ids.UUID{7}
	timeout := sched.WaitTimeoutArgs{RunID: run, StepID: "wait", WaitSeq: 1, DueAt: t0.Add(time.Hour)}
	f.commit(func(tx store.Tx) {
		f.ok(f.s.ScheduleTimeout(f.ctx, tx, timeout))
		f.ok(f.s.SnoozePump(f.ctx, tx, run, 10*time.Minute))
		f.ok(f.s.EnqueuePumpMany(f.ctx, tx, []ids.UUID{{1}, {2}}))
		f.ok(f.s.EnqueuePumpMany(f.ctx, tx, nil))
		f.ok(f.s.EnqueueRoute(f.ctx, tx, 3))
	})
	// A job enqueued by a job runs in the same Advance when it is due.
	f.h.result = func(c call) sched.Result {
		if c.kind == sched.KindRouteEvent {
			f.commit(func(tx store.Tx) { f.ok(f.s.SnoozePump(f.ctx, tx, ids.UUID{9}, 2*time.Hour)) })
		}
		return sched.Done()
	}
	f.advance(2 * time.Hour)
	f.wantCalls(
		call{sched.KindPumpRun, sched.PumpRunArgs{RunID: ids.UUID{1}}, t0},
		call{sched.KindPumpRun, sched.PumpRunArgs{RunID: ids.UUID{2}}, t0},
		call{sched.KindRouteEvent, sched.RouteEventArgs{EventID: 3}, t0},
		call{sched.KindPumpRun, sched.PumpRunArgs{RunID: run}, t0.Add(10 * time.Minute)},
		call{sched.KindWaitTimeout, timeout, t0.Add(time.Hour)},
		call{sched.KindPumpRun, sched.PumpRunArgs{RunID: ids.UUID{9}}, t0.Add(2 * time.Hour)},
	)
	if now := f.clock.Now(); !now.Equal(t0.Add(2 * time.Hour)) {
		t.Fatalf("clock = %s after Advance", now)
	}
	if err := f.s.Advance(f.ctx, -time.Second); err == nil {
		t.Fatal("Advance moved the clock back")
	}
}

func TestScheduleTimeoutNeedsADueTime(t *testing.T) {
	f := newFixture(t, nil)
	f.commit(func(tx store.Tx) {
		if err := f.s.ScheduleTimeout(f.ctx, tx, sched.WaitTimeoutArgs{RunID: ids.UUID{1}}); err == nil {
			t.Fatal("a timeout without a due time was accepted")
		}
	})
}

func TestSnoozeRunsAgainWithoutUsingAnAttempt(t *testing.T) {
	f := newFixture(t, nil)
	snoozes := 30 // more than the attempts of a job
	f.h.result = func(c call) sched.Result {
		if snoozes > 0 {
			snoozes--
			return sched.Snooze(time.Minute)
		}
		return sched.Done()
	}
	f.commit(func(tx store.Tx) { f.ok(f.s.EnqueueRoute(f.ctx, tx, 1)) })
	f.advance(time.Hour)
	if n := len(f.h.calls); n != 31 {
		t.Fatalf("route ran %d times, want 31", n)
	}
	if last := f.h.calls[30].at; !last.Equal(t0.Add(30 * time.Minute)) {
		t.Fatalf("last run at %s, want 30 minutes in", last)
	}
	if p := f.s.Pending(); len(p) != 0 {
		t.Fatalf("jobs left: %+v", p)
	}
}

func TestFailedJobsRetryWithBackoffUntilDiscarded(t *testing.T) {
	f := newFixture(t, nil)
	panics := true
	f.h.result = func(c call) sched.Result {
		if panics {
			panics = false
			panic("boom")
		}
		return sched.Retry(errors.New("still failing"))
	}
	f.commit(func(tx store.Tx) { f.ok(f.s.EnqueuePump(f.ctx, tx, ids.UUID{1})) })
	f.advance(0)
	p := f.s.Pending()
	if len(p) != 1 || p[0].Attempt != 1 || !p[0].Due.Equal(t0.Add(time.Second)) {
		t.Fatalf("after a panic the queue is %+v, want the job due a second later on attempt 1", p)
	}
	f.advance(time.Second)
	if p := f.s.Pending(); len(p) != 1 || !p[0].Due.Equal(t0.Add(17*time.Second)) {
		t.Fatalf("after a second failure the queue is %+v, want the job due 16s later", p)
	}
	// Backoffs of attempt^4 seconds add up to about 26 days for 25 attempts.
	f.advance(60 * 24 * time.Hour)
	if n := len(f.h.calls); n != 25 {
		t.Fatalf("the job ran %d times, want 25", n)
	}
	if p := f.s.Pending(); len(p) != 0 {
		t.Fatalf("a job outlived its attempts: %+v", p)
	}
}

func TestJanitorRunsOnStartAndSkipsMissedPasses(t *testing.T) {
	f := newFixture(t, nil)
	f.h.janitor = true
	j := sched.JanitorArgs{}
	f.advance(0)
	f.advance(10 * time.Second)
	f.advance(20 * time.Second)
	f.advance(72 * time.Hour)
	f.wantCalls(
		call{sched.KindJanitor, j, t0},
		call{sched.KindJanitor, j, t0.Add(30 * time.Second)},
		call{sched.KindJanitor, j, t0.Add(30*time.Second + 72*time.Hour)},
	)
	// The pass after a jump is due an interval after it.
	f.advance(29 * time.Second)
	f.advance(time.Second)
	if n := len(f.h.calls); n != 4 {
		t.Fatalf("janitor passes = %d, want 4", n)
	}
}

func TestFailedJanitorPassIsNotRetried(t *testing.T) {
	f := newFixture(t, nil)
	f.h.janitor = true
	f.h.result = func(call) sched.Result { return sched.Retry(errors.New("janitor failed")) }
	f.advance(29 * time.Second)
	if n := len(f.h.calls); n != 1 {
		t.Fatalf("janitor passes = %d, want 1", n)
	}
	if p := f.s.Pending(); len(p) != 0 {
		t.Fatalf("a failed janitor pass was queued again: %+v", p)
	}
}

func TestJobsRunUnderTheirKindTimeout(t *testing.T) {
	f := newFixture(t, nil)
	timing := sched.NewTiming(5 * time.Minute)
	got := map[string]time.Duration{}
	record := func(ctx context.Context, kind string) sched.Result {
		dl, ok := ctx.Deadline()
		if !ok {
			t.Errorf("%s has no deadline", kind)
		}
		got[kind] = time.Until(dl)
		return sched.Done()
	}
	h := timeoutHandlers{record}
	s, err := memsched.New(memsched.Options{
		Store: f.store, Clock: f.clock, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	f.ok(err)
	f.ok(s.Start(h, timing))
	f.commit(func(tx store.Tx) {
		f.ok(s.EnqueuePump(f.ctx, tx, ids.UUID{1}))
		f.ok(s.EnqueueRoute(f.ctx, tx, 1))
	})
	f.ok(s.Advance(f.ctx, 0))
	near := func(kind string, want time.Duration) {
		if d := got[kind]; d > want || d < want-time.Minute {
			t.Errorf("%s ran with %s left, want about %s", kind, d, want)
		}
	}
	near(sched.KindPumpRun, timing.PumpJobTimeout)
	near(sched.KindRouteEvent, timing.JobTimeout)
	near(sched.KindJanitor, timing.JobTimeout)
}

type timeoutHandlers struct {
	record func(ctx context.Context, kind string) sched.Result
}

func (h timeoutHandlers) Route(ctx context.Context, _ sched.RouteEventArgs) sched.Result {
	return h.record(ctx, sched.KindRouteEvent)
}

func (h timeoutHandlers) Pump(ctx context.Context, _ sched.PumpRunArgs) sched.Result {
	return h.record(ctx, sched.KindPumpRun)
}

func (h timeoutHandlers) Timeout(ctx context.Context, _ sched.WaitTimeoutArgs) sched.Result {
	return h.record(ctx, sched.KindWaitTimeout)
}

func (h timeoutHandlers) Janitor(ctx context.Context, _ sched.JanitorArgs) sched.Result {
	return h.record(ctx, sched.KindJanitor)
}

func TestQueueFeedsTheStoreView(t *testing.T) {
	f := newFixture(t, nil)
	run := ids.UUID{4}
	f.commit(func(tx store.Tx) {
		f.ok(f.s.EnqueuePump(f.ctx, tx, run))
		f.ok(f.s.SnoozePump(f.ctx, tx, ids.UUID{5}, time.Minute))
	})
	if !f.s.LiveJob(sched.KindPumpRun, run) || !f.s.LiveJob(sched.KindPumpRun, ids.UUID{5}) {
		t.Fatal("a queued pump is not live")
	}
	if f.s.LiveJob(sched.KindWaitTimeout, run) || f.s.LiveJob(sched.KindPumpRun, ids.UUID{6}) {
		t.Fatal("a job that is not queued is live")
	}
	if n := f.s.Available(sched.Queue); n != 1 {
		t.Fatalf("available = %d, want 1: the snoozed pump is not due", n)
	}
	if n := f.s.Available("other"); n != 0 {
		t.Fatalf("available on another queue = %d", n)
	}
	// The running job is live while its handler runs.
	f.h.result = func(c call) sched.Result {
		if c.kind == sched.KindPumpRun && !f.s.LiveJob(sched.KindPumpRun, run) {
			t.Error("the running pump is not live")
		}
		return sched.Done()
	}
	f.advance(0)
	if f.s.LiveJob(sched.KindPumpRun, run) {
		t.Fatal("a finished pump is still live")
	}
}

func TestInterruptStopsAdvance(t *testing.T) {
	stop := errors.New("stop")
	var f *fixture
	f = newFixture(t, func() error {
		if len(f.h.calls) == 1 {
			return stop
		}
		return nil
	})
	f.commit(func(tx store.Tx) {
		f.ok(f.s.EnqueueRoute(f.ctx, tx, 1))
		f.ok(f.s.EnqueueRoute(f.ctx, tx, 2))
	})
	if err := f.s.Advance(f.ctx, time.Hour); !errors.Is(err, stop) {
		t.Fatalf("Advance = %v, want the interrupt", err)
	}
	if !f.clock.Now().Equal(t0) {
		t.Fatalf("the clock moved to %s past the interrupt", f.clock.Now())
	}
	// The second route and the janitor pass due on start are left.
	if n := len(f.s.Pending()); n != 2 {
		t.Fatalf("%d jobs left, want 2", n)
	}
}

func TestEndlessSnoozeIsReported(t *testing.T) {
	f := newFixture(t, nil)
	f.h.result = func(call) sched.Result { return sched.Snooze(0) }
	f.commit(func(tx store.Tx) { f.ok(f.s.EnqueueRoute(f.ctx, tx, 1)) })
	err := f.s.Advance(f.ctx, 0)
	if err == nil || !strings.Contains(err.Error(), "keeps becoming due") {
		t.Fatalf("Advance = %v", err)
	}
}

func TestStartAndRunDueGuards(t *testing.T) {
	s, err := memsched.New(memsched.Options{
		Store: memstore.New(), Clock: &clock{now: t0}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RunDue(context.Background()); err == nil {
		t.Fatal("RunDue before Start succeeded")
	}
	timing := sched.NewTiming(5 * time.Minute)
	if err := s.Start(nil, timing); err == nil {
		t.Fatal("Start(nil) succeeded")
	}
	if err := s.Start(sched.StubHandlers{}, sched.Timing{}); err == nil {
		t.Fatal("Start with zero timing succeeded")
	}
	if err := s.Start(sched.StubHandlers{}, timing); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(sched.StubHandlers{}, timing); err == nil {
		t.Fatal("a second Start succeeded")
	}
	if _, err := memsched.New(memsched.Options{}); err == nil {
		t.Fatal("New without a store succeeded")
	}
}
