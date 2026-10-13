package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/g0-up/flowkit/internal/store"
)

func TestNewValidatesOptions(t *testing.T) {
	base := func() Options {
		return Options{
			Store: (*nilStore)(nil), Clock: &fakeClock{}, ClaimTimeout: time.Minute, ActionDeadline: 48 * time.Second,
			MaxOutputBytes: 1, IsNotPerformed: isNotPerformed, RedactError: redact,
		}
	}
	if _, err := New(base()); err != nil {
		t.Fatalf("New(valid) = %v", err)
	}
	for name, mutate := range map[string]func(*Options){
		"nil store":                func(o *Options) { o.Store = nil },
		"nil clock":                func(o *Options) { o.Clock = nil },
		"nil IsNotPerformed":       func(o *Options) { o.IsNotPerformed = nil },
		"nil RedactError":          func(o *Options) { o.RedactError = nil },
		"zero claim timeout":       func(o *Options) { o.ClaimTimeout = 0 },
		"zero action deadline":     func(o *Options) { o.ActionDeadline = 0 },
		"deadline at claim expiry": func(o *Options) { o.ActionDeadline = o.ClaimTimeout },
		"zero max output":          func(o *Options) { o.MaxOutputBytes = 0 },
		"negative rate":            func(o *Options) { o.RateLimit = RateLimit{Rate: -1, Burst: 1} },
		"burst below one":          func(o *Options) { o.RateLimit = RateLimit{Rate: 1, Burst: 0.5} },
	} {
		o := base()
		mutate(&o)
		if _, err := New(o); err == nil {
			t.Errorf("New with %s succeeded", name)
		}
	}
}

// nilStore satisfies Store for tests that never reach the database.
type nilStore struct{ Store }

func TestRunRejectsInvalidRequests(t *testing.T) {
	ex, err := New(Options{
		Store: (*nilStore)(nil), Clock: &fakeClock{}, ClaimTimeout: time.Minute, ActionDeadline: time.Second,
		MaxOutputBytes: 1, IsNotPerformed: isNotPerformed, RedactError: redact,
	})
	if err != nil {
		t.Fatal(err)
	}
	exec := func(context.Context, Input) (map[string]any, error) { return nil, nil }
	valid := ExecRequest{StepID: "s", IdemKey: "k", DefName: "d", Action: Action{Name: "a", Execute: exec}}
	for name, mutate := range map[string]func(*ExecRequest){
		"empty key":     func(r *ExecRequest) { r.IdemKey = "" },
		"empty step":    func(r *ExecRequest) { r.StepID = "" },
		"empty def":     func(r *ExecRequest) { r.DefName = "" },
		"empty action":  func(r *ExecRequest) { r.Action.Name = "" },
		"no Execute fn": func(r *ExecRequest) { r.Action.Execute = nil },
	} {
		r := valid
		mutate(&r)
		if out := ex.Run(context.Background(), r); out.Kind != Errored || out.Err == nil {
			t.Errorf("Run with %s = %+v; want Errored", name, out)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if out := ex.Run(ctx, valid); out.Kind != Abandoned {
		t.Errorf("Run with a cancelled context = %s; want abandoned", out.Kind)
	}
}

func TestSuccessIsRecordedAndCached(t *testing.T) {
	e := newEnv(t)
	ex := e.executor()
	ts := time.Date(2026, 10, 12, 1, 2, 3, 400, time.UTC)
	var acquired int32
	a, calls := countingAction("payments.charge", false, func(context.Context, int64) (map[string]any, error) {
		acquired = e.pool.Stat().AcquiredConns()
		return map[string]any{"id": "ch_1", "amount": 1250, "ok": true, "rate": 0.25, "at": ts}, nil
	})
	a.OutputFacts = []OutputFact{
		{"id", FactString}, {"amount", FactInt}, {"ok", FactBool}, {"rate", FactDouble}, {"at", FactTimestamp},
	}
	want := map[string]any{"id": "ch_1", "amount": int64(1250), "ok": true, "rate": 0.25, "at": ts}

	out := ex.Run(context.Background(), e.request("k1", a))
	wantKind(t, out, Done)
	if out.Cached || out.Err != nil || !outputEqual(out.Output, want) {
		t.Fatalf("first run = %+v; want output %v", out, want)
	}
	if acquired != 0 {
		t.Fatalf("Execute ran with %d connections acquired; it must run outside any transaction", acquired)
	}
	row := e.effect("k1")
	if row.DoneAt == nil || !row.DoneAt.Equal(t0) || row.Attempt != 0 || row.LastError != nil || row.InDoubt {
		t.Fatalf("effect = %+v", row)
	}

	out = ex.Run(context.Background(), e.request("k1", a))
	wantKind(t, out, Done)
	if !out.Cached || out.Err != nil || !outputEqual(out.Output, want) {
		t.Fatalf("second run = %+v; want the cached output %v", out, want)
	}
	if calls.Load() != 1 {
		t.Fatalf("Execute ran %d times; want 1", calls.Load())
	}
}

func outputEqual(got, want map[string]any) bool {
	if len(got) != len(want) {
		return false
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			return false
		}
		if wt, ok := w.(time.Time); ok {
			if gt, ok := g.(time.Time); !ok || !gt.Equal(wt) {
				return false
			}
			continue
		}
		if g != w {
			return false
		}
	}
	return true
}

func TestNotPerformedThenSuccess(t *testing.T) {
	e := newEnv(t)
	ex := e.executor()
	a, calls := countingAction("payments.charge", false, func(_ context.Context, n int64) (map[string]any, error) {
		if n == 1 {
			return nil, fmt.Errorf("card declined before capture: %w", errNotPerformed)
		}
		return nil, nil
	})

	out := ex.Run(context.Background(), e.request("k1", a))
	wantKind(t, out, RetryLater)
	if out.Delay != DefaultBaseBackoff || !errors.Is(out.Err, errNotPerformed) {
		t.Fatalf("first run = %+v; want RetryLater after %s with the action's error", out, DefaultBaseBackoff)
	}
	row := e.effect("k1")
	if row.Attempt != 1 || row.ClaimedAt != nil || row.ClaimToken != nil || row.DoneAt != nil ||
		*row.LastError != "redacted: card declined before capture: not performed" {
		t.Fatalf("effect after the failed attempt = %+v", row)
	}
	steps := e.steps(stepAttemptFailed)
	if len(steps) != 1 || *steps[0].Attempt != 1 || *steps[0].Detail != "payments.charge" ||
		!strings.HasPrefix(*steps[0].Error, "redacted: ") || steps[0].StepID != "step" || !steps[0].At.Equal(t0) {
		t.Fatalf("attempt_failed steps = %+v", steps)
	}

	out = ex.Run(context.Background(), e.request("k1", a))
	wantKind(t, out, Done)
	if out.Cached || out.Err != nil {
		t.Fatalf("second run = %+v", out)
	}
	row = e.effect("k1")
	if row.Attempt != 1 || row.DoneAt == nil || row.LastError != nil || row.InDoubt {
		t.Fatalf("effect after success = %+v", row)
	}
	if dl := e.deadLetters(); len(dl) != 0 {
		t.Fatalf("dead letters = %+v; want none", dl)
	}
	if calls.Load() != 2 {
		t.Fatalf("Execute ran %d times; want 2", calls.Load())
	}
}

func TestUnwrappedErrorOnNonIdempotentActionGoesInDoubt(t *testing.T) {
	e := newEnv(t)
	ex := e.executor()
	a, calls := countingAction("payments.charge", false, func(context.Context, int64) (map[string]any, error) {
		return nil, errors.New("connection reset")
	})

	out := ex.Run(context.Background(), e.request("k1", a))
	wantKind(t, out, BlockedInDoubt)
	if out.Err == nil || out.Err.Error() != "action payments.charge: redacted: connection reset" {
		t.Fatalf("outcome err = %v; want the stored text", out.Err)
	}
	row := e.effect("k1")
	if !row.InDoubt || row.ClaimToken != nil || row.DoneAt != nil || row.Attempt != 0 || *row.LastError != "redacted: connection reset" {
		t.Fatalf("effect = %+v", row)
	}
	dl := e.deadLetters()
	if len(dl) != 1 || dl[0].Kind != "effect_in_doubt" || dl[0].Ref != "k1" || dl[0].RunID != e.runID ||
		dl[0].Note != "redacted: connection reset" {
		t.Fatalf("dead letters = %+v", dl)
	}
	var raw map[string]any
	if err := json.Unmarshal(dl[0].Raw, &raw); err != nil {
		t.Fatal(err)
	}
	if raw["idem_key"] != "k1" || raw["run_id"] != e.runID.String() || raw["step_id"] != "step" ||
		raw["action"] != "payments.charge" || raw["attempt"] != float64(1) {
		t.Fatalf("dead letter raw = %s", dl[0].Raw)
	}
	if steps := e.steps(stepEffectInDoubt); len(steps) != 1 || *steps[0].Attempt != 1 || *steps[0].Error != "redacted: connection reset" {
		t.Fatalf("effect_in_doubt steps = %+v", steps)
	}
	if steps := e.steps(stepAttemptFailed); len(steps) != 0 {
		t.Fatalf("attempt_failed steps = %+v; want none", steps)
	}

	out = ex.Run(context.Background(), e.request("k1", a))
	wantKind(t, out, BlockedInDoubt)
	if calls.Load() != 1 {
		t.Fatalf("Execute ran %d times; want 1, an in-doubt effect is never retried", calls.Load())
	}
	if dl := e.deadLetters(); len(dl) != 1 {
		t.Fatalf("dead letters after the second run = %d; want 1", len(dl))
	}
}

func TestIdempotentActionRetriesAnyError(t *testing.T) {
	e := newEnv(t)
	ex := e.executor()
	a, _ := countingAction("crm.update", true, func(_ context.Context, n int64) (map[string]any, error) {
		if n == 1 {
			panic("nil map")
		}
		return nil, errors.New("503")
	})
	out := ex.Run(context.Background(), e.request("k1", a))
	wantKind(t, out, RetryLater)
	if !strings.Contains(out.Err.Error(), "panicked: nil map") {
		t.Fatalf("panic outcome err = %v", out.Err)
	}
	out = ex.Run(context.Background(), e.request("k1", a))
	wantKind(t, out, RetryLater)
	if out.Delay != 2*DefaultBaseBackoff {
		t.Fatalf("second delay = %s; want %s", out.Delay, 2*DefaultBaseBackoff)
	}
	if row := e.effect("k1"); row.Attempt != 2 || row.InDoubt {
		t.Fatalf("effect = %+v", row)
	}
	if dl := e.deadLetters(); len(dl) != 0 {
		t.Fatalf("dead letters = %+v; want none", dl)
	}
}

func TestPanicOfNonIdempotentActionGoesInDoubt(t *testing.T) {
	e := newEnv(t)
	ex := e.executor()
	a, _ := countingAction("payments.charge", false, func(context.Context, int64) (map[string]any, error) {
		// Even a panic with a NotPerformed error says nothing reliable about
		// what the action did before it.
		panic(fmt.Errorf("oops: %w", errNotPerformed))
	})
	out := ex.Run(context.Background(), e.request("k1", a))
	wantKind(t, out, BlockedInDoubt)
	if row := e.effect("k1"); !row.InDoubt {
		t.Fatalf("effect = %+v", row)
	}
}

func TestFailedAfterMaxAttempts(t *testing.T) {
	e := newEnv(t)
	ex := e.executor()
	a, calls := countingAction("crm.update", true, func(context.Context, int64) (map[string]any, error) {
		return nil, errors.New("503")
	})
	a.Retry = RetryPolicy{MaxAttempts: 3, BaseBackoff: time.Second, MaxBackoff: 90 * time.Second}

	for i, wantDelay := range []time.Duration{time.Second, 2 * time.Second} {
		out := ex.Run(context.Background(), e.request("k1", a))
		wantKind(t, out, RetryLater)
		if out.Delay != wantDelay {
			t.Fatalf("attempt %d delay = %s; want %s", i+1, out.Delay, wantDelay)
		}
	}
	out := ex.Run(context.Background(), e.request("k1", a))
	wantKind(t, out, Failed)
	if out.Err == nil || !strings.Contains(out.Err.Error(), "failed 3 times") {
		t.Fatalf("Failed err = %v", out.Err)
	}
	steps := e.steps(stepAttemptFailed)
	if len(steps) != 3 {
		t.Fatalf("attempt_failed steps = %d; want 3", len(steps))
	}
	for i, s := range steps {
		if *s.Attempt != i+1 {
			t.Fatalf("step %d attempt = %d", i, *s.Attempt)
		}
	}

	// A request after exhaustion does not execute again.
	out = ex.Run(context.Background(), e.request("k1", a))
	wantKind(t, out, Failed)
	if calls.Load() != 3 {
		t.Fatalf("Execute ran %d times; want 3", calls.Load())
	}
	if row := e.effect("k1"); row.Attempt != 3 || row.ClaimedAt != nil || row.DoneAt != nil {
		t.Fatalf("effect = %+v", row)
	}
}

func TestParentCancelAbandonsWithoutWrites(t *testing.T) {
	for _, tc := range []struct {
		name       string
		idempotent bool
		honourCtx  bool
	}{
		{"non-idempotent action returning the context error", false, true},
		{"idempotent action returning the context error", true, true},
		{"action ignoring its context", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			ex := e.executor()
			ctx, cancel := context.WithCancel(context.Background())
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })
			a, _ := countingAction("payments.charge", tc.idempotent, func(actx context.Context, _ int64) (map[string]any, error) {
				cancel()
				if tc.honourCtx {
					<-actx.Done()
					return nil, actx.Err()
				}
				<-release
				return nil, nil
			})
			out := ex.Run(ctx, e.request("k1", a))
			wantKind(t, out, Abandoned)
			row := e.effect("k1")
			if row.ClaimToken == nil || row.Attempt != 0 || row.DoneAt != nil || row.LastError != nil || row.InDoubt {
				t.Fatalf("effect = %+v; want the claim as the request took it", row)
			}
			if steps := e.steps(stepAttemptFailed); len(steps) != 0 {
				t.Fatalf("steps = %+v", steps)
			}
			if dl := e.deadLetters(); len(dl) != 0 {
				t.Fatalf("dead letters = %+v", dl)
			}
		})
	}
}

// cancelingClock cancels the caller's context on the first Now after the
// action returned, so the cancel lands between the success and its record.
type cancelingClock struct {
	*fakeClock
	returned atomic.Bool
	cancel   context.CancelFunc
}

func (c *cancelingClock) Now() time.Time {
	if c.returned.Load() {
		c.cancel()
	}
	return c.fakeClock.Now()
}

func TestSuccessIsRecordedWhenParentCancelsBeforeTheRecord(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := &cancelingClock{fakeClock: e.clock, cancel: cancel}
	ex := e.executor(func(o *Options) { o.Clock = clock })
	a, _ := countingAction("payments.charge", false, func(context.Context, int64) (map[string]any, error) {
		clock.returned.Store(true)
		return nil, nil
	})
	out := ex.Run(ctx, e.request("k1", a))
	if ctx.Err() == nil {
		t.Fatal("the caller's context was not canceled before the record")
	}
	wantKind(t, out, Done)
	if row := e.effect("k1"); row.DoneAt == nil {
		t.Fatalf("effect = %+v; want it done", row)
	}
}

func TestActionDeadline(t *testing.T) {
	for _, tc := range []struct {
		name       string
		idempotent bool
		want       ExecKind
	}{
		{"idempotent", true, RetryLater},
		{"non-idempotent", false, BlockedInDoubt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			ex := e.executor(func(o *Options) { o.ActionDeadline = 50 * time.Millisecond })
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })
			a, _ := countingAction("slow", tc.idempotent, func(context.Context, int64) (map[string]any, error) {
				<-release // ignores the deadline
				return nil, nil
			})
			out := ex.Run(context.Background(), e.request("k1", a))
			wantKind(t, out, tc.want)
			if !errors.Is(out.Err, context.DeadlineExceeded) {
				t.Fatalf("err = %v; want a deadline error", out.Err)
			}
			if row := e.effect("k1"); *row.LastError != "redacted: did not return before its context ended: action deadline exceeded" {
				t.Fatalf("last_error = %q", *row.LastError)
			}
		})
	}
}

func TestInvalidOutputIsRecordedAsDoneWithError(t *testing.T) {
	e := newEnv(t)
	ex := e.executor()
	a, calls := countingAction("payments.charge", false, func(context.Context, int64) (map[string]any, error) {
		return map[string]any{"id": 7, "extra": true}, nil
	})
	a.OutputFacts = []OutputFact{{"id", FactString}, {"amount", FactInt}}

	out := ex.Run(context.Background(), e.request("k1", a))
	wantKind(t, out, Done)
	if out.Err == nil || out.Output != nil {
		t.Fatalf("outcome = %+v; want Done with an error and no output", out)
	}
	row := e.effect("k1")
	if row.DoneAt == nil || row.Result != nil || !strings.HasPrefix(*row.LastError, "redacted: output does not match") {
		t.Fatalf("effect = %+v", row)
	}
	out = ex.Run(context.Background(), e.request("k1", a))
	wantKind(t, out, Done)
	if !out.Cached || out.Err == nil || !strings.Contains(out.Err.Error(), "output does not match") {
		t.Fatalf("cached outcome = %+v", out)
	}
	if calls.Load() != 1 {
		t.Fatalf("Execute ran %d times", calls.Load())
	}
}

func TestOversizedOutputIsRejected(t *testing.T) {
	e := newEnv(t)
	ex := e.executor(func(o *Options) { o.MaxOutputBytes = 16 })
	a, _ := countingAction("big", true, func(context.Context, int64) (map[string]any, error) {
		return map[string]any{"blob": strings.Repeat("x", 32)}, nil
	})
	a.OutputFacts = []OutputFact{{"blob", FactString}}
	out := ex.Run(context.Background(), e.request("k1", a))
	wantKind(t, out, Done)
	if out.Err == nil || !strings.Contains(out.Err.Error(), "above the limit of 16") {
		t.Fatalf("outcome = %+v", out)
	}
}

func TestExistingClaim(t *testing.T) {
	e := newEnv(t)
	ex := e.executor()
	a, calls := countingAction("payments.charge", false, func(context.Context, int64) (map[string]any, error) {
		return nil, nil
	})
	e.claimAt("fresh", a.Name, t0.Add(-time.Minute))
	out := ex.Run(context.Background(), e.request("fresh", a))
	wantKind(t, out, RetryLater)
	if out.Delay != testClaimTimeout-time.Minute {
		t.Fatalf("delay = %s; want the rest of the claim timeout", out.Delay)
	}

	e.claimAt("expired", a.Name, t0.Add(-testClaimTimeout))
	out = ex.Run(context.Background(), e.request("expired", a))
	wantKind(t, out, BlockedInDoubt)

	if calls.Load() != 0 {
		t.Fatalf("Execute ran %d times; want 0", calls.Load())
	}
}

func TestExecKindString(t *testing.T) {
	for k, want := range map[ExecKind]string{
		Done: "done", RetryLater: "retry_later", Failed: "failed", BlockedInDoubt: "blocked_in_doubt",
		Abandoned: "abandoned", Errored: "errored", 0: "ExecKind(0)", 99: "ExecKind(99)",
	} {
		if got := k.String(); got != want {
			t.Errorf("ExecKind(%d).String() = %q; want %q", int(k), got, want)
		}
	}
}

func TestCallerDeadlineCutsTheActionDeadline(t *testing.T) {
	// The margin before the caller's deadline is 500ms, and an action needs
	// at least 200ms.
	const claimTimeout, actionDeadline, margin = 10 * time.Second, 2 * time.Second, 500 * time.Millisecond
	for _, tc := range []struct {
		name       string
		idempotent bool
		want       ExecKind
	}{
		{"idempotent", true, RetryLater},
		{"non-idempotent", false, BlockedInDoubt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			ex := e.executor(func(o *Options) { o.ClaimTimeout, o.ActionDeadline = claimTimeout, actionDeadline })
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })
			actionDeadlines := make(chan time.Time, 1)
			a, _ := countingAction("slow", tc.idempotent, func(actx context.Context, _ int64) (map[string]any, error) {
				d, _ := actx.Deadline()
				actionDeadlines <- d
				<-release // ignores its deadline
				return nil, nil
			})
			deadline := time.Now().Add(1500 * time.Millisecond)
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()

			out := ex.Run(ctx, e.request("k1", a))
			wantKind(t, out, tc.want)
			if ctx.Err() != nil {
				t.Fatal("the caller's context ended before Run returned")
			}
			if actionDeadlineAt := <-actionDeadlines; actionDeadlineAt.After(deadline.Add(-margin)) {
				t.Fatalf("action deadline %s is less than %s before the caller's %s", actionDeadlineAt, margin, deadline)
			}
			if !errors.Is(out.Err, context.DeadlineExceeded) {
				t.Fatalf("err = %v; want a deadline error", out.Err)
			}
			row := e.effect("k1")
			if *row.LastError != "redacted: did not return before its context ended: action deadline exceeded" {
				t.Fatalf("last_error = %q", *row.LastError)
			}
			if tc.idempotent && row.Attempt != 1 {
				t.Fatalf("attempt = %d; want 1", row.Attempt)
			}
			if dl := e.deadLetters(); tc.idempotent == (len(dl) != 0) {
				t.Fatalf("dead letters = %+v", dl)
			}
		})
	}
}

func TestTooLittleTimeBeforeTheCallerDeadline(t *testing.T) {
	e := newEnv(t)
	ex := e.executor(func(o *Options) { o.ClaimTimeout, o.ActionDeadline = 10*time.Second, 2*time.Second })
	a, calls := countingAction("payments.charge", false, func(context.Context, int64) (map[string]any, error) {
		return nil, nil
	})
	// 600ms less the 500ms margin leaves less than the 200ms an action needs.
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	out := ex.Run(ctx, e.request("k1", a))
	wantKind(t, out, RetryLater)
	if out.Delay != shortBudgetRetryDelay || out.Err != nil {
		t.Fatalf("outcome = %+v; want a short retry", out)
	}
	if calls.Load() != 0 {
		t.Fatalf("Execute ran %d times; want 0", calls.Load())
	}
	e.inTx(func(tx store.Tx) {
		if _, err := e.store.GetEffect(context.Background(), tx, "k1"); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("GetEffect = %v; want no effect claimed", err)
		}
	})
}

// lateCancelCtx reports Canceled once armed but never closes its Done
// channel, so the statement under way still commits: it stands for a context
// that ends just after the claim committed.
type lateCancelCtx struct {
	context.Context
	armed atomic.Bool
}

func (c *lateCancelCtx) Err() error {
	if c.armed.Load() {
		return context.Canceled
	}
	return nil
}

func TestCancelAfterTheClaimReleasesIt(t *testing.T) {
	e := newEnv(t)
	ctx := &lateCancelCtx{Context: context.Background()}
	h := &hookStore{PgStore: e.store, afterInsertClaim: func() { ctx.armed.Store(true) }}
	ex := e.executor(func(o *Options) { o.Store = h })
	a, calls := countingAction("payments.charge", false, func(context.Context, int64) (map[string]any, error) {
		return nil, nil
	})

	out := ex.Run(ctx, e.request("k1", a))
	wantKind(t, out, Abandoned)
	if calls.Load() != 0 {
		t.Fatalf("Execute ran %d times; want 0", calls.Load())
	}
	row := e.effect("k1")
	if row.ClaimToken != nil || row.ClaimedAt != nil || row.Attempt != 0 || row.InDoubt || row.LastError != nil {
		t.Fatalf("effect = %+v; want the claim released without an attempt", row)
	}

	wantKind(t, ex.Run(context.Background(), e.request("k1", a)), Done)
	if calls.Load() != 1 {
		t.Fatalf("Execute ran %d times; want 1", calls.Load())
	}
}
