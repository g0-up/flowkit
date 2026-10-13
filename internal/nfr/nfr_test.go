package nfr_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"

	"github.com/g0-up/flowkit"
	"github.com/g0-up/flowkit/internal/dbgate"
	"github.com/g0-up/flowkit/internal/testpg"
	"github.com/g0-up/flowkit/migrate"
)

// enableVar must be "1" for the latency tests to run: each configuration
// takes about a minute of real time.
const enableVar = "FLOWKIT_NFR"

// Budgets from the design's latency goals and alert rules.
const (
	wakeLatencyP95Budget = 30 * time.Second
	timerLagBudget       = 60 * time.Second
)

const (
	// runsPerKind is how many waits are woken by an event, and how many
	// time out, per configuration.
	runsPerKind = 100
	// emitEvery spaces the events of each kind: 5 per second per kind is
	// well above the expected peak of a few thousand events an hour.
	emitEvery = 200 * time.Millisecond
	// waitTimeout is the timeout of the waits left to expire.
	waitTimeout = "10s"
	// settleTimeout bounds each wait for runs to reach a state.
	settleTimeout = 3 * time.Minute
	opTimeout     = 30 * time.Second
	// extraConns covers the LISTEN connection, Emit and the test's queries.
	extraConns = 8
)

const wakeBody = `{
	"metamodel_version": 1,
	"trigger": {"event": "student.registered"},
	"steps": [
		{"id": "first_login", "type": "wait_for_event", "event": "student.logged_in",
		 "timeout": "72h", "on_timeout": "timed_out", "next": "woken"},
		{"id": "woken", "type": "end", "name": "woken"},
		{"id": "timed_out", "type": "end", "name": "timed_out"}
	]
}`

var timerBody = fmt.Sprintf(`{
	"metamodel_version": 1,
	"trigger": {"event": "lead.created"},
	"steps": [
		{"id": "reply", "type": "wait_for_event", "event": "lead.replied",
		 "timeout": %q, "on_timeout": "timed_out", "next": "replied"},
		{"id": "replied", "type": "end", "name": "replied"},
		{"id": "timed_out", "type": "end", "name": "timed_out"}
	]
}`, waitTimeout)

func registry() *flowkit.Registry {
	reg := flowkit.NewRegistry()
	for _, e := range []struct{ typ, kind, field string }{
		{"student.registered", "student", "student_id"},
		{"student.logged_in", "student", "student_id"},
		{"lead.created", "lead", "lead_id"},
		{"lead.replied", "lead", "lead_id"},
	} {
		reg.MustAddEvent(flowkit.EventDef{
			Type: e.typ,
			PayloadSchema: json.RawMessage(fmt.Sprintf(
				`{"type":"object","required":[%q],"properties":{%q:{"type":"string"}}}`, e.field, e.field)),
			EntityKind:    e.kind,
			EntityIDField: e.field,
		})
	}
	return reg
}

// samples records every value of the engine's histograms, so percentiles
// are exact rather than bucket estimates.
type samples struct {
	mu     sync.Mutex
	values map[string][]float64
}

func (s *samples) add(name string, v float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[name] = append(s.values[name], v)
}

func (s *samples) get(name string) []float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.values[name])
}

type sampleProvider struct {
	metricnoop.MeterProvider
	s *samples
}

func (p sampleProvider) Meter(string, ...metric.MeterOption) metric.Meter {
	return sampleMeter{s: p.s}
}

type sampleMeter struct {
	metricnoop.Meter
	s *samples
}

func (m sampleMeter) Float64Histogram(name string, _ ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	return sampleHistogram{name: name, s: m.s}, nil
}

type sampleHistogram struct {
	metricnoop.Float64Histogram
	name string
	s    *samples
}

func (h sampleHistogram) Record(_ context.Context, v float64, _ ...metric.RecordOption) {
	h.s.add(h.name, v)
}

// stats summarises latency samples in seconds.
type stats struct {
	n             int
	p50, p95, max time.Duration
}

func summarise(values []float64) stats {
	slices.Sort(values)
	st := stats{n: len(values)}
	if len(values) == 0 {
		return st
	}
	// Nearest-rank percentile.
	rank := func(p float64) time.Duration {
		i := int(math.Ceil(p*float64(len(values)))) - 1
		return time.Duration(values[max(i, 0)] * float64(time.Second))
	}
	st.p50, st.p95, st.max = rank(0.50), rank(0.95), rank(1)
	return st
}

func (s stats) String() string {
	return fmt.Sprintf("n=%d p50=%s p95=%s max=%s", s.n,
		s.p50.Round(time.Millisecond), s.p95.Round(time.Millisecond), s.max.Round(time.Millisecond))
}

// TestLatencyBudgets runs a light, steady load on a real Engine with the
// system clock, once with LISTEN/NOTIFY (the default) and once with PollOnly,
// and checks wake latency (event stored to wait resumed) and timer lag (wait
// timeout due to handled) against the budgets.
func TestLatencyBudgets(t *testing.T) {
	if os.Getenv(enableVar) != "1" {
		t.Skipf("set %s=1 to run the latency checks (about two minutes)", enableVar)
	}
	for _, c := range []struct {
		name    string
		workers flowkit.WorkerConfig
	}{
		{"default", flowkit.WorkerConfig{}},
		{"poll_only", flowkit.WorkerConfig{PollOnly: true}},
	} {
		t.Run(c.name, func(t *testing.T) {
			wake, lag := measure(t, c.workers)
			t.Logf("wake_latency %s", wake)
			t.Logf("timer_lag    %s", lag)
			if wake.n < runsPerKind || lag.n < runsPerKind {
				t.Fatalf("recorded %d wake latencies and %d timer lags, want at least %d each", wake.n, lag.n, runsPerKind)
			}
			if wake.p95 > wakeLatencyP95Budget {
				t.Errorf("wake latency p95 %s exceeds %s", wake.p95, wakeLatencyP95Budget)
			}
			if lag.p95 > timerLagBudget || lag.max > timerLagBudget {
				t.Errorf("timer lag p95 %s / max %s exceeds %s", lag.p95, lag.max, timerLagBudget)
			}
		})
	}
}

// measure starts an Engine with workers on a fresh schema, drives the load
// and returns the wake latency and timer lag samples it recorded.
func measure(t *testing.T, workers flowkit.WorkerConfig) (wake, lag stats) {
	poolCfg, err := pgxpool.ParseConfig(testpg.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	maxWorkers := workers.MaxWorkers
	if maxWorkers == 0 {
		maxWorkers = 4
	}
	poolCfg.MaxConns = int32(maxWorkers + extraConns)
	pool := testpg.PoolWithConfig(t, poolCfg)
	schema := testpg.Schema(t, pool)
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	if err := migrate.Run(ctx, pool, schema, migrate.Options{}); err != nil {
		t.Fatal(err)
	}

	rec := &samples{values: map[string][]float64{}}
	engine, err := flowkit.New(flowkit.Config{
		Pool:       pool,
		Schema:     schema,
		Logger:     slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelWarn})),
		Meter:      sampleProvider{s: rec},
		InstanceID: "nfr",
		Workers:    workers,
	}, registry())
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(func() {
		defer stop()
		ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
		defer cancel()
		if err := engine.Close(ctx); err != nil {
			t.Errorf("close engine: %v", err)
		}
		dbgate.Check(t, pool, schema)
	})
	publish(t, engine, "nfr_wake", wakeBody)
	publish(t, engine, "nfr_timer", timerBody)
	if err := engine.Start(runCtx); err != nil {
		t.Fatal(err)
	}

	for i := range runsPerKind {
		emit(t, pool, engine, flowkit.Event{Type: "student.registered", Payload: map[string]any{"student_id": fmt.Sprintf("s-%d", i)}})
	}
	waitForRuns(t, engine, "nfr_wake", "waiting", "")

	tick := time.NewTicker(emitEvery)
	defer tick.Stop()
	for i := range runsPerKind {
		<-tick.C
		emit(t, pool, engine, flowkit.Event{Type: "lead.created", Payload: map[string]any{"lead_id": fmt.Sprintf("l-%d", i)}})
		emit(t, pool, engine, flowkit.Event{Type: "student.logged_in", Payload: map[string]any{"student_id": fmt.Sprintf("s-%d", i)}})
	}
	waitForRuns(t, engine, "nfr_wake", "completed", "woken")
	waitForRuns(t, engine, "nfr_timer", "completed", "timed_out")

	return summarise(rec.get("flowkit.wake_latency")), summarise(rec.get("flowkit.timer_lag"))
}

func publish(t *testing.T, engine *flowkit.Engine, name, body string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	defs := engine.Definitions()
	id, err := defs.SaveDraft(ctx, name, json.RawMessage(body))
	if err != nil {
		t.Fatalf("save draft %s: %v", name, err)
	}
	if _, err := defs.Publish(ctx, id, flowkit.PublishOpts{Actor: "nfr"}); err != nil {
		t.Fatalf("publish %s: %v", name, err)
	}
}

func emit(t *testing.T, pool *pgxpool.Pool, engine *flowkit.Engine, evt flowkit.Event) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	if err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error { return engine.Emit(ctx, tx, evt) }); err != nil {
		t.Fatalf("emit %s: %v", evt.Type, err)
	}
}

// waitForRuns polls until all runsPerKind runs of def are in state, and, when
// terminal is set, ended at that end step.
func waitForRuns(t *testing.T, engine *flowkit.Engine, def, state, terminal string) {
	t.Helper()
	deadline := time.Now().Add(settleTimeout)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
		list, err := engine.Runs().List(ctx, flowkit.RunFilter{DefName: def, Limit: 1000})
		cancel()
		if err != nil {
			t.Fatalf("list runs of %s: %v", def, err)
		}
		done := 0
		for _, r := range list {
			if r.State == state {
				if terminal != "" && r.TerminalName != terminal {
					t.Fatalf("run %s of %s ended at %q, want %q", r.RunID, def, r.TerminalName, terminal)
				}
				done++
			}
		}
		if done == runsPerKind {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after %s, %d of %d runs of %s are %s (%d runs listed)", settleTimeout, done, runsPerKind, def, state, len(list))
		}
		time.Sleep(250 * time.Millisecond)
	}
}
