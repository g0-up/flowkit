package executor

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/g0-up/flowkit/internal/sched"
	"github.com/g0-up/flowkit/internal/store"
)

func TestResolveExpiredClaims(t *testing.T) {
	e := newEnv(t)
	client, err := sched.New(sched.Options{
		Pool: e.pool, Store: e.store, InstanceID: "test", Timing: sched.NewTiming(testClaimTimeout),
		Logger: e.logger, Clock: e.clock,
	}, sched.StubHandlers{})
	if err != nil {
		t.Fatal(err)
	}
	ex := e.executor(func(o *Options) {
		o.Pumps = client.Scheduler()
		o.IsIdempotent = func(action string) bool { return action == "crm.update" }
	})
	old := t0.Add(-testClaimTimeout - time.Second)
	e.claimAt("idem", "crm.update", old)
	chargeToken := e.claimAt("charge", "payments.charge", old)
	e.claimAt("unknown", "gone.action", old)
	e.claimAt("fresh", "payments.charge", t0.Add(-time.Minute))

	n, err := ex.ResolveExpiredClaims(context.Background(), e.clock.Now(), 10)
	if err != nil || n != 3 {
		t.Fatalf("ResolveExpiredClaims = %d, %v; want 3", n, err)
	}
	if row := e.effect("idem"); row.ClaimedAt != nil || row.ClaimToken != nil || row.InDoubt || row.Attempt != 1 ||
		*row.LastError != "redacted: claim expired after 5m0s without a result" {
		t.Fatalf("released effect = %+v; want it released with a failed attempt", row)
	}
	if steps := e.steps(stepAttemptFailed); len(steps) != 1 || *steps[0].Attempt != 1 || *steps[0].Detail != "crm.update" {
		t.Fatalf("attempt_failed steps = %+v", steps)
	}
	if got := pumpJobs(t, e); got != 1 {
		t.Fatalf("pump jobs = %d; want 1", got)
	}
	for _, key := range []string{"charge", "unknown"} {
		row := e.effect(key)
		if !row.InDoubt || row.ClaimToken != nil || !strings.HasPrefix(*row.LastError, "redacted: ") {
			t.Fatalf("%s: effect = %+v; want it in doubt", key, row)
		}
	}
	// The pump that held the claim cannot record a result any more.
	e.inTx(func(tx store.Tx) {
		ok, err := e.store.CompleteEffect(context.Background(), tx, "charge", chargeToken, t0, json.RawMessage(`{}`), nil)
		if err != nil || ok {
			t.Fatalf("CompleteEffect with the revoked token = %v, %v; want false", ok, err)
		}
	})
	if row := e.effect("fresh"); row.ClaimToken == nil || row.InDoubt {
		t.Fatalf("fresh claim = %+v; want it untouched", row)
	}
	dl := e.deadLetters()
	if len(dl) != 2 || dl[0].Kind != "effect_in_doubt" || dl[1].Kind != "effect_in_doubt" {
		t.Fatalf("dead letters = %+v", dl)
	}
	if steps := e.steps(stepEffectInDoubt); len(steps) != 2 || *steps[0].Attempt != 1 {
		t.Fatalf("effect_in_doubt steps = %+v", steps)
	}

	// The released effect is executed by the next request; the others wait
	// for an operator.
	a, calls := countingAction("crm.update", true, func(context.Context, int64) (map[string]any, error) { return nil, nil })
	wantKind(t, ex.Run(context.Background(), e.request("idem", a)), Done)
	charge, _ := countingAction("payments.charge", false, func(context.Context, int64) (map[string]any, error) { return nil, nil })
	wantKind(t, ex.Run(context.Background(), e.request("charge", charge)), BlockedInDoubt)
	if calls.Load() != 1 {
		t.Fatalf("Execute ran %d times; want 1", calls.Load())
	}

	if n, err := ex.ResolveExpiredClaims(context.Background(), e.clock.Now(), 10); err != nil || n != 0 {
		t.Fatalf("second ResolveExpiredClaims = %d, %v; want 0", n, err)
	}
}

func TestResolveExpiredClaimsValidates(t *testing.T) {
	e := newEnv(t)
	ex := e.executor()
	if _, err := ex.ResolveExpiredClaims(context.Background(), e.clock.Now(), 10); err == nil {
		t.Fatal("ResolveExpiredClaims without a pump enqueuer succeeded")
	}
	ex = e.executor(func(o *Options) { o.Pumps = noPumps{}; o.IsIdempotent = func(string) bool { return false } })
	if _, err := ex.ResolveExpiredClaims(context.Background(), e.clock.Now(), 0); err == nil {
		t.Fatal("ResolveExpiredClaims with limit 0 succeeded")
	}
}

type noPumps struct{ PumpEnqueuer }

func TestCrashLoopStopsAfterMaxAttempts(t *testing.T) {
	e := newEnv(t)
	client, err := sched.New(sched.Options{
		Pool: e.pool, Store: e.store, InstanceID: "test", Timing: sched.NewTiming(testClaimTimeout),
		Logger: e.logger, Clock: e.clock,
	}, sched.StubHandlers{})
	if err != nil {
		t.Fatal(err)
	}
	ex := e.executor(func(o *Options) {
		o.Pumps = client.Scheduler()
		o.IsIdempotent = func(string) bool { return true }
	})
	// Each request dies while the action runs: its context ends and nothing
	// is recorded, as when the process crashes.
	var crash context.CancelFunc
	a, calls := countingAction("crm.update", true, func(ctx context.Context, _ int64) (map[string]any, error) {
		crash()
		<-ctx.Done()
		return nil, ctx.Err()
	})
	a.Retry = RetryPolicy{MaxAttempts: 3}

	for i := 1; i <= 3; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		crash = cancel
		wantKind(t, ex.Run(ctx, e.request("k1", a)), Abandoned)
		cancel()
		stale := *e.effect("k1").ClaimToken
		e.clock.Advance(testClaimTimeout + time.Second)
		if n, err := ex.ResolveExpiredClaims(context.Background(), e.clock.Now(), 10); err != nil || n != 1 {
			t.Fatalf("round %d: ResolveExpiredClaims = %d, %v; want 1", i, n, err)
		}
		if row := e.effect("k1"); row.Attempt != i || row.ClaimToken != nil {
			t.Fatalf("round %d: effect = %+v", i, row)
		}
		// The dead request's token can no longer write.
		e.inTx(func(tx store.Tx) {
			ctx := context.Background()
			if ok, err := e.store.CompleteEffect(ctx, tx, "k1", stale, t0, json.RawMessage(`{}`), nil); err != nil || ok {
				t.Fatalf("round %d: CompleteEffect with the stale token = %v, %v; want false", i, ok, err)
			}
			if _, ok, err := e.store.FailEffect(ctx, tx, "k1", stale, "late"); err != nil || ok {
				t.Fatalf("round %d: FailEffect with the stale token = %v, %v; want false", i, ok, err)
			}
			if ok, err := e.store.ReleaseEffectClaim(ctx, tx, "k1", stale); err != nil || ok {
				t.Fatalf("round %d: ReleaseEffectClaim with the stale token = %v, %v; want false", i, ok, err)
			}
		})
	}

	out := ex.Run(context.Background(), e.request("k1", a))
	wantKind(t, out, Failed)
	if out.Err == nil || !strings.Contains(out.Err.Error(), "failed 3 times") || !strings.Contains(out.Err.Error(), "claim expired") {
		t.Fatalf("err = %v", out.Err)
	}
	if calls.Load() != 3 {
		t.Fatalf("Execute ran %d times; want 3", calls.Load())
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
	if got := pumpJobs(t, e); got != 3 {
		t.Fatalf("pump jobs = %d; want 3", got)
	}
}
