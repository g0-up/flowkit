package executor

import (
	"context"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/g0-up/flowkit/internal/store"
)

func TestRateLimitAcrossConcurrentRequests(t *testing.T) {
	e := newEnv(t)
	ex := e.executor(func(o *Options) { o.RateLimit = RateLimit{Rate: 10.0 / 60, Burst: 10} })
	a, calls := countingAction("sms.send", false, func(context.Context, int64) (map[string]any, error) { return nil, nil })

	const requests = 30
	outs := make([]ExecOutcome, requests)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, key := range fmtKeys("sms", requests) {
		wg.Go(func() {
			<-start
			outs[i] = ex.Run(context.Background(), e.request(key, a))
		})
	}
	close(start)
	wg.Wait()

	done, limited := 0, 0
	for _, out := range outs {
		switch {
		case out.Kind == Done:
			done++
		case out.Kind == RetryLater && out.Delay > 0 && out.Err == nil:
			limited++
		default:
			t.Fatalf("outcome %+v", out)
		}
	}
	if done != 10 || limited != 20 || calls.Load() != 10 {
		t.Fatalf("done %d, rate limited %d, executions %d; want 10, 20, 10", done, limited, calls.Load())
	}
	if got := e.meter.count("flowkit.effect_rate_limited"); got != 20 {
		t.Fatalf("effect_rate_limited = %d; want 20", got)
	}
	if a, d := e.meter.lastAttr("flowkit.effect_rate_limited", "action"), e.meter.lastAttr("flowkit.effect_rate_limited", "def"); a != "sms.send" || d != "order_flow" {
		t.Fatalf("effect_rate_limited attributes = %q, %q", a, d)
	}

	// One token is due every 6 s.
	e.clock.Advance(6 * time.Second)
	wantKind(t, ex.Run(context.Background(), e.request("late-1", a)), Done)
	out := ex.Run(context.Background(), e.request("late-2", a))
	wantKind(t, out, RetryLater)
	if out.Delay != 6*time.Second {
		t.Fatalf("delay = %s; want 6s", out.Delay)
	}
}

func TestRateLimitIsPerDefinition(t *testing.T) {
	e := newEnv(t)
	ex := e.executor(func(o *Options) { o.RateLimit = RateLimit{Rate: 1, Burst: 1} })
	a, _ := countingAction("sms.send", false, func(context.Context, int64) (map[string]any, error) { return nil, nil })
	wantKind(t, ex.Run(context.Background(), e.request("k1", a)), Done)
	wantKind(t, ex.Run(context.Background(), e.request("k2", a)), RetryLater)
	other := e.request("k3", a)
	other.DefName = "other_flow"
	wantKind(t, ex.Run(context.Background(), other), Done)
}

func TestRateLimitTake(t *testing.T) {
	r := RateLimit{Rate: 2, Burst: 3}
	b := store.RateLimitRow{DefName: "d", Tokens: 0.5, RefilledAt: t0}

	got, allowed, wait := r.take(b, t0)
	if allowed || wait != 250*time.Millisecond || got.Tokens != 0.5 {
		t.Fatalf("take with half a token = %+v, %v, %s", got, allowed, wait)
	}
	got, allowed, _ = r.take(b, t0.Add(time.Second))
	if !allowed || got.Tokens != 1.5 || !got.RefilledAt.Equal(t0.Add(time.Second)) {
		t.Fatalf("take after 1s = %+v, %v", got, allowed)
	}
	got, allowed, _ = r.take(b, t0.Add(time.Hour))
	if !allowed || got.Tokens != 2 {
		t.Fatalf("take after an hour = %+v, %v; want the bucket capped at the burst", got, allowed)
	}
	// A clock behind the bucket adds no tokens and keeps its refill time.
	got, allowed, _ = r.take(b, t0.Add(-time.Hour))
	if allowed || got.Tokens != 0.5 || !got.RefilledAt.Equal(t0) {
		t.Fatalf("take with a clock behind = %+v, %v", got, allowed)
	}
}

func TestRateLimitValidate(t *testing.T) {
	for _, r := range []RateLimit{{}, {Rate: 0.1, Burst: 1}, {Rate: 100, Burst: 100}} {
		if err := r.validate(); err != nil {
			t.Errorf("%+v: %v", r, err)
		}
	}
	for _, r := range []RateLimit{
		{Rate: -1, Burst: 1}, {Rate: math.NaN(), Burst: 1}, {Rate: math.Inf(1), Burst: 1},
		{Rate: 1}, {Rate: 1, Burst: math.Inf(1)}, {Rate: 1, Burst: math.NaN()},
	} {
		if err := r.validate(); err == nil {
			t.Errorf("%+v: accepted", r)
		}
	}
}

func TestAllowWithoutRateLimit(t *testing.T) {
	ex, err := New(Options{
		Store: (*nilStore)(nil), Clock: &fakeClock{}, ClaimTimeout: time.Minute, ActionDeadline: time.Second,
		MaxOutputBytes: 1, IsNotPerformed: isNotPerformed, RedactError: redact,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ok, wait, err := ex.Allow(context.Background(), "d", t0); !ok || wait != 0 || err != nil {
		t.Fatalf("Allow = %v, %s, %v; want it to allow without touching the store", ok, wait, err)
	}
}

func TestRateLimitedWaitsAreSpread(t *testing.T) {
	const requests = 30
	e := newEnv(t)
	var draws atomic.Int64
	ex := e.executor(func(o *Options) {
		o.RateLimit = RateLimit{Rate: 10.0 / 60, Burst: 10}
		o.Rand = func() float64 { return float64(draws.Add(1)-1) / requests }
	})
	a, _ := countingAction("sms.send", false, func(context.Context, int64) (map[string]any, error) { return nil, nil })
	outs := make([]ExecOutcome, requests)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, key := range fmtKeys("sms", requests) {
		wg.Go(func() {
			<-start
			outs[i] = ex.Run(context.Background(), e.request(key, a))
		})
	}
	close(start)
	wg.Wait()

	var delays []time.Duration
	for _, out := range outs {
		if out.Kind == RetryLater {
			delays = append(delays, out.Delay)
		}
	}
	slices.Sort(delays)
	if len(delays) != 20 || len(slices.Compact(slices.Clone(delays))) != 20 {
		t.Fatalf("delays = %v; want 20 distinct ones", delays)
	}
	// The bucket refills in 60s; the waits cover most of it.
	if delays[0] < 6*time.Second || delays[19]-delays[0] < 30*time.Second {
		t.Fatalf("delays from %s to %s; want them spread over the refill window after the next token", delays[0], delays[19])
	}
}

func TestRateLimitRejectionWritesNothing(t *testing.T) {
	e := newEnv(t)
	ex := e.executor(func(o *Options) { o.RateLimit = RateLimit{Rate: 1, Burst: 1} })
	a, _ := countingAction("sms.send", false, func(context.Context, int64) (map[string]any, error) { return nil, nil })
	bucket := func() (tokens float64, refilledAt time.Time) {
		t.Helper()
		err := e.pool.QueryRow(context.Background(),
			"SELECT tokens, refilled_at FROM "+pgx.Identifier{e.store.Schema(), "rate_limits"}.Sanitize()+" WHERE def_name = $1",
			"order_flow").Scan(&tokens, &refilledAt)
		if err != nil {
			t.Fatal(err)
		}
		return tokens, refilledAt
	}
	wantKind(t, ex.Run(context.Background(), e.request("k1", a)), Done)
	tokens, refilledAt := bucket()

	e.clock.Advance(500 * time.Millisecond)
	out := ex.Run(context.Background(), e.request("k2", a))
	wantKind(t, out, RetryLater)
	if out.Delay != 500*time.Millisecond {
		t.Fatalf("delay = %s; want 500ms", out.Delay)
	}
	if gotTokens, gotAt := bucket(); gotTokens != tokens || !gotAt.Equal(refilledAt) {
		t.Fatalf("bucket after a rejection = %v at %s; want %v at %s", gotTokens, gotAt, tokens, refilledAt)
	}

	// The refill still counts from the stored time.
	e.clock.Advance(500 * time.Millisecond)
	wantKind(t, ex.Run(context.Background(), e.request("k2", a)), Done)
}

func TestRateLimitWaitsSaturate(t *testing.T) {
	r := RateLimit{Rate: 1e-300, Burst: 1}
	if _, allowed, wait := r.take(store.RateLimitRow{RefilledAt: t0}, t0); allowed || wait != maxDelay {
		t.Fatalf("take at a tiny rate = %v, %s; want a saturated wait", allowed, wait)
	}
	if got := r.spread(0.5); got != maxDelay {
		t.Fatalf("spread at a tiny rate = %s; want it saturated", got)
	}
	if got := (RateLimit{Rate: 2, Burst: 3}).spread(0.5); got != 750*time.Millisecond {
		t.Fatalf("spread = %s; want 750ms", got)
	}
}
