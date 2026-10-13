package executor

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/g0-up/flowkit/internal/sched"
	"github.com/g0-up/flowkit/internal/store"
)

func TestConcurrentRequestsExecuteOnce(t *testing.T) {
	const rounds, requests = 200, 8
	e := newEnv(t)
	ex := e.executor()
	var executions sync.Map // idem key → *atomic.Int64
	a := Action{
		Name: "payments.charge",
		Execute: func(_ context.Context, in Input) (map[string]any, error) {
			n, _ := executions.LoadOrStore(in.IdemKey, new(atomic.Int64))
			n.(*atomic.Int64).Add(1)
			return nil, nil
		},
	}
	for _, key := range fmtKeys("round", rounds) {
		start := make(chan struct{})
		outs := make([]ExecOutcome, requests)
		var wg sync.WaitGroup
		for i := range requests {
			wg.Go(func() {
				<-start
				outs[i] = ex.Run(context.Background(), e.request(key, a))
			})
		}
		close(start)
		wg.Wait()
		n, _ := executions.Load(key)
		if n == nil || n.(*atomic.Int64).Load() != 1 {
			t.Fatalf("%s: Execute ran %v times; want exactly 1", key, n)
		}
		fresh := 0
		for _, out := range outs {
			switch {
			case out.Kind == Done && !out.Cached:
				fresh++
			case out.Kind == Done || out.Kind == RetryLater:
			default:
				t.Fatalf("%s: outcome %s (err %v)", key, out.Kind, out.Err)
			}
		}
		if fresh != 1 {
			t.Fatalf("%s: %d fresh Done outcomes; want 1", key, fresh)
		}
	}
}

func TestStaleClaimAfterReclaimIsOrphaned(t *testing.T) {
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
	started, release := make(chan struct{}), make(chan struct{})
	a, calls := countingAction("crm.update", true, func(_ context.Context, n int64) (map[string]any, error) {
		if n == 1 {
			close(started)
			<-release
			return map[string]any{"by": "old"}, nil
		}
		return map[string]any{"by": "new"}, nil
	})
	a.OutputFacts = []OutputFact{{"by", FactString}}

	oldDone := make(chan ExecOutcome, 1)
	go func() { oldDone <- ex.Run(context.Background(), e.request("k1", a)) }()
	<-started
	oldToken := *e.effect("k1").ClaimToken

	// The old pump overruns its claim; the janitor releases it and a new pump
	// claims and completes the effect.
	e.clock.Advance(testClaimTimeout + time.Second)
	if n, err := ex.ResolveExpiredClaims(context.Background(), e.clock.Now(), 10); err != nil || n != 1 {
		t.Fatalf("ResolveExpiredClaims = %d, %v; want 1", n, err)
	}
	if got := pumpJobs(t, e); got != 1 {
		t.Fatalf("pump jobs = %d; want 1", got)
	}
	out := ex.Run(context.Background(), e.request("k1", a))
	wantKind(t, out, Done)
	if out.Output["by"] != "new" {
		t.Fatalf("new pump output = %v", out.Output)
	}
	newRow := e.effect("k1")
	if *newRow.ClaimToken == oldToken {
		t.Fatal("the new pump reused the old claim token")
	}

	close(release)
	old := <-oldDone
	wantKind(t, old, BlockedInDoubt)
	if got := e.meter.count("flowkit.effect_orphaned"); got != 1 {
		t.Fatalf("effect_orphaned = %d; want 1", got)
	}
	if got := e.meter.lastAttr("flowkit.effect_orphaned", "action"); got != "crm.update" {
		t.Fatalf("effect_orphaned action = %q", got)
	}
	row := e.effect("k1")
	if !row.DoneAt.Equal(*newRow.DoneAt) || *row.ClaimToken != *newRow.ClaimToken || row.InDoubt {
		t.Fatalf("effect = %+v; want it as the new pump left it", row)
	}
	var result map[string]string
	if err := json.Unmarshal(row.Result, &result); err != nil || result["by"] != "new" {
		t.Fatalf("result = %s, %v", row.Result, err)
	}
	if calls.Load() != 2 {
		t.Fatalf("Execute ran %d times; want 2", calls.Load())
	}
}

// pumpJobs counts the pump_run jobs enqueued for the env's run.
func pumpJobs(t *testing.T, e *env) int {
	t.Helper()
	var n int
	err := e.pool.QueryRow(context.Background(),
		"SELECT count(*) FROM "+pgx.Identifier{e.store.Schema(), "river_job"}.Sanitize()+
			" WHERE kind = $1 AND args->>'run_id' = $2", sched.KindPumpRun, e.runID.String()).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestLostClaimOnFailureIsNotRecorded(t *testing.T) {
	e := newEnv(t)
	ex := e.executor()
	for _, idempotent := range []bool{true, false} {
		key := "idempotent"
		if !idempotent {
			key = "non-idempotent"
		}
		a, _ := countingAction("a", idempotent, func(ctx context.Context, _ int64) (map[string]any, error) {
			// Another holder takes over the claim while the action runs.
			e.inTx(func(tx store.Tx) {
				row, err := e.store.GetEffect(ctx, tx, key)
				if err != nil {
					t.Fatal(err)
				}
				if ok, err := e.store.ReleaseEffectClaim(ctx, tx, key, *row.ClaimToken); err != nil || !ok {
					t.Fatal(ok, err)
				}
			})
			return nil, context.DeadlineExceeded
		})
		out := ex.Run(context.Background(), e.request(key, a))
		wantKind(t, out, BlockedInDoubt)
		row := e.effect(key)
		if row.Attempt != 0 || row.InDoubt || row.LastError != nil {
			t.Fatalf("%s: effect = %+v; want it untouched", key, row)
		}
	}
	if steps := e.steps(stepAttemptFailed); len(steps) != 0 {
		t.Fatalf("steps = %+v", steps)
	}
	if dl := e.deadLetters(); len(dl) != 0 {
		t.Fatalf("dead letters = %+v", dl)
	}
}

func TestDecide(t *testing.T) {
	ex, err := New(Options{
		Store: (*nilStore)(nil), Clock: &fakeClock{}, ClaimTimeout: testClaimTimeout, ActionDeadline: time.Second,
		MaxOutputBytes: 1, IsNotPerformed: isNotPerformed, RedactError: redact, Rand: func() float64 { return 0.5 },
	})
	if err != nil {
		t.Fatal(err)
	}
	a := Action{Name: "a", Retry: RetryPolicy{MaxAttempts: 2}, OutputFacts: []OutputFact{{"n", FactInt}}}
	at := func(d time.Duration) *time.Time { v := t0.Add(d); return &v }
	str := func(s string) *string { return &s }
	for _, tc := range []struct {
		name      string
		row       store.EffectRow
		want      ExecKind
		claimable bool
		delay     time.Duration
		errSub    string
	}{
		{name: "done", row: store.EffectRow{DoneAt: at(0), Result: json.RawMessage(`{"n": 1}`)}, want: Done},
		{name: "done with invalid output", row: store.EffectRow{Action: "a", DoneAt: at(0), LastError: str("bad")}, want: Done, errSub: "bad"},
		{name: "done with a stored output that no longer matches", row: store.EffectRow{DoneAt: at(0), Result: json.RawMessage(`{"m": 1}`)}, want: Done, errSub: "stored output"},
		{name: "in doubt", row: store.EffectRow{InDoubt: true, ClaimedAt: at(-time.Hour)}, want: BlockedInDoubt},
		{name: "released", row: store.EffectRow{Attempt: 1}, claimable: true},
		{name: "released with attempts used up", row: store.EffectRow{Action: "a", Attempt: 2, LastError: str("503")}, want: Failed, errSub: "failed 2 times: action a: 503"},
		{name: "claimed recently", row: store.EffectRow{ClaimedAt: at(-time.Minute)}, want: RetryLater, delay: (testClaimTimeout - time.Minute) * 21 / 20},
		{name: "claimed just now", row: store.EffectRow{ClaimedAt: at(0)}, want: RetryLater, delay: testClaimTimeout * 21 / 20},
		{name: "claim expired", row: store.EffectRow{ClaimedAt: at(-testClaimTimeout)}, want: BlockedInDoubt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, final := ex.decide(a, tc.row, t0)
			if final == tc.claimable {
				t.Fatalf("final = %v; want %v", final, !tc.claimable)
			}
			if tc.claimable {
				return
			}
			if out.Kind != tc.want || out.Delay != tc.delay {
				t.Fatalf("decide = %+v; want %s with delay %s", out, tc.want, tc.delay)
			}
			if (tc.errSub == "") != (out.Err == nil) || (out.Err != nil && !strings.Contains(out.Err.Error(), tc.errSub)) {
				t.Fatalf("err = %v; want one containing %q", out.Err, tc.errSub)
			}
			if tc.want == Done && !out.Cached {
				t.Fatal("a done effect must be reported as cached")
			}
		})
	}
}

func TestReclaimStopsAtMaxAttempts(t *testing.T) {
	e := newEnv(t)
	h := &hookStore{PgStore: e.store}
	ex := e.executor(func(o *Options) { o.Store = h })
	a, calls := countingAction("crm.update", true, func(context.Context, int64) (map[string]any, error) {
		return nil, errors.New("503")
	})
	a.Retry = RetryPolicy{MaxAttempts: 2}
	wantKind(t, ex.Run(context.Background(), e.request("k1", a)), RetryLater)

	// Another request records the last attempt between this request's read
	// and its reclaim.
	h.beforeReclaim = func(key string) {
		e.exec("UPDATE {schema}.executed_effects SET attempt = 2 WHERE idem_key = $1", key)
	}
	out := ex.Run(context.Background(), e.request("k1", a))
	wantKind(t, out, Failed)
	if out.Err == nil || !strings.Contains(out.Err.Error(), "failed 2 times") {
		t.Fatalf("err = %v", out.Err)
	}
	if calls.Load() != 1 {
		t.Fatalf("Execute ran %d times; want 1", calls.Load())
	}
	if row := e.effect("k1"); row.ClaimToken != nil || row.Attempt != 2 {
		t.Fatalf("effect = %+v; want it unclaimed", row)
	}
}

func TestOrphanedSuccessOfAnInDoubtEffectIsKept(t *testing.T) {
	for _, tc := range []struct {
		name       string
		output     map[string]any
		earlier    string // a result the effect already kept
		wantResult string
		wantNote   string // "" when no line is added
	}{
		{"valid output", map[string]any{"id": "ch_1"}, "", `{"id": "ch_1"}`, "its output is the effect's result"},
		{"invalid output", map[string]any{"id": 7}, "", "", "with invalid output: redacted: output does not match"},
		// The output is dropped with its note: the first one kept wins.
		{"effect with a result", map[string]any{"id": "ch_2"}, `{"id": "ch_1"}`, `{"id": "ch_1"}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
				o.IsIdempotent = func(string) bool { return false }
			})
			a, _ := countingAction("payments.charge", false, func(ctx context.Context, _ int64) (map[string]any, error) {
				// The request overruns its claim and the janitor puts the
				// effect in doubt before the action returns.
				e.clock.Advance(testClaimTimeout + time.Second)
				if n, err := ex.ResolveExpiredClaims(ctx, e.clock.Now(), 10); err != nil || n != 1 {
					t.Errorf("ResolveExpiredClaims = %d, %v; want 1", n, err)
				}
				if tc.earlier != "" {
					e.exec(`UPDATE {schema}.executed_effects SET result = $1 WHERE idem_key = 'k1'`, tc.earlier)
				}
				return tc.output, nil
			})
			a.OutputFacts = []OutputFact{{"id", FactString}}

			wantKind(t, ex.Run(context.Background(), e.request("k1", a)), BlockedInDoubt)
			if got := e.meter.count("flowkit.effect_orphaned"); got != 1 {
				t.Fatalf("effect_orphaned = %d; want 1", got)
			}
			row := e.effect("k1")
			if !row.InDoubt || row.DoneAt != nil || string(row.Result) != tc.wantResult {
				t.Fatalf("effect = %+v (result %s); want it in doubt with result %s", row, row.Result, tc.wantResult)
			}
			dl := e.deadLetters()
			if len(dl) != 1 || !strings.HasPrefix(dl[0].Note, "redacted: claim expired") ||
				strings.Contains(dl[0].Note, "\n") != (tc.wantNote != "") || !strings.Contains(dl[0].Note, tc.wantNote) {
				t.Fatalf("dead letters = %+v", dl)
			}
			wantKind(t, ex.Run(context.Background(), e.request("k1", a)), BlockedInDoubt)
		})
	}
}
