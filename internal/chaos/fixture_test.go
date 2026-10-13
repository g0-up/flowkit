package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/g0-up/flowkit"
	"github.com/g0-up/flowkit/internal/dbgate"
	"github.com/g0-up/flowkit/internal/testhooks"
	"github.com/g0-up/flowkit/internal/testpg"
	"github.com/g0-up/flowkit/migrate"
)

const (
	// claimTimeout is the smallest flowkit accepts: actions time out after
	// 4s and the janitor treats a run as stuck after 10s.
	claimTimeout = 5 * time.Second
	// rescueAfter and jobTimeout replace River's one-hour rescue delay and
	// one-minute job timeout, so the jobs of a killed process are retried
	// within a janitor interval.
	rescueAfter = 30 * time.Second
	jobTimeout  = 10 * time.Second
	// pollInterval is how often engines poll for jobs.
	pollInterval = 100 * time.Millisecond
	// opTimeout bounds one database call of the test.
	opTimeout = 10 * time.Second
	// settleTimeout bounds the wait for a restarted instance to recover:
	// several janitor passes, 30s apart, and River rescues.
	settleTimeout = 4 * time.Minute
	// settlePoll is how often a recovery condition is checked.
	settlePoll = 250 * time.Millisecond
	// fixtureConns are the connections of the test process's pool: its
	// queries plus a recovery engine of recoveryWorkers.
	fixtureConns    = 8
	recoveryWorkers = 2
)

// TestMain shortens the job timing of every engine the tests and their
// child processes build, before any of them exists.
func TestMain(m *testing.M) {
	testhooks.Timing = func(time.Duration) (time.Duration, time.Duration) { return rescueAfter, jobTimeout }
	os.Exit(m.Run())
}

// Event types. Each flow has its own trigger; every flow then waits for
// evWake of its run's key.
const (
	evWake       = "chaos.wake"
	payloadOfKey = `{"type":"object","required":["key"],"properties":{"key":{"type":"string"}}}`
	// exhaustAttempts is the MaxAttempts of the action that is killed
	// until its attempts are used up.
	exhaustAttempts = 3
)

// flow is a published definition: its action, then a wait for evWake of
// the run's key, ending "got", or "late" after an hour.
type flow struct{ name, trigger, action string }

var (
	// flowOnce runs a non-idempotent action that writes the sink once.
	flowOnce = flow{"chaos_once", "chaos.once", "chaos.sink_once"}
	// flowRetry runs an idempotent action that writes the sink once.
	flowRetry = flow{"chaos_retry", "chaos.retry", "chaos.sink_retry"}
	// flowExhaust runs an idempotent action of exhaustAttempts attempts.
	flowExhaust = flow{"chaos_exhaust", "chaos.exhaust", "chaos.sink_exhaust"}
	// flowStallOnce runs a non-idempotent action that writes the sink, then
	// overruns its deadline.
	flowStallOnce = flow{"chaos_stall_once", "chaos.stall_once", "chaos.stall_once"}
	// flowStallRetry runs an idempotent action that writes the sink, then
	// overruns its deadline on its first execution only.
	flowStallRetry = flow{"chaos_stall_retry", "chaos.stall_retry", "chaos.stall_retry"}

	flows = []flow{flowOnce, flowRetry, flowExhaust, flowStallOnce, flowStallRetry}
)

func (f flow) body() json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"metamodel_version":1,"trigger":{"event":%q},"steps":[
		{"id":"act","type":"action","action":%q,"next":"wait"},
		{"id":"wait","type":"wait_for_event","event":%q,"match":"event.key == scope.entity_id",
		 "timeout":"1h","on_timeout":"late","next":"got"},
		{"id":"got","type":"end","name":"got"},
		{"id":"late","type":"end","name":"late"}]}`, f.trigger, f.action, evWake))
}

// newRegistry registers the chaos events and the actions writing to sink.
func newRegistry(sink *Sink) *flowkit.Registry {
	reg := flowkit.NewRegistry()
	reg.MustAddEvent(flowkit.EventDef{
		Type: evWake, PayloadSchema: json.RawMessage(payloadOfKey), EntityKind: "chaos", EntityIDField: "key",
	})
	for _, f := range flows {
		reg.MustAddEvent(flowkit.EventDef{
			Type: f.trigger, PayloadSchema: json.RawMessage(payloadOfKey), EntityKind: "chaos", EntityIDField: "key",
		})
	}
	write := func(ctx context.Context, in flowkit.ActionInput) (flowkit.ActionResult, error) {
		_, err := sink.Record(ctx, in.IdempotencyKey)
		return flowkit.ActionResult{}, err
	}
	// stall writes the sink whatever the action's deadline, then, when
	// stalls says so for the write's count, blocks until the deadline.
	stall := func(stalls func(n int) bool) func(context.Context, flowkit.ActionInput) (flowkit.ActionResult, error) {
		return func(ctx context.Context, in flowkit.ActionInput) (flowkit.ActionResult, error) {
			n, err := sink.Record(context.WithoutCancel(ctx), in.IdempotencyKey)
			if err != nil || !stalls(n) {
				return flowkit.ActionResult{}, err
			}
			<-ctx.Done()
			return flowkit.ActionResult{}, ctx.Err()
		}
	}
	params := json.RawMessage(`{"type":"object"}`)
	quick := flowkit.RetryPolicy{BaseBackoff: time.Second, MaxBackoff: 2 * time.Second}
	exhaust := quick
	exhaust.MaxAttempts = exhaustAttempts
	for _, a := range []flowkit.ActionDef{
		{Name: flowOnce.action, Execute: write},
		{Name: flowRetry.action, Idempotent: true, Retry: quick, Execute: write},
		{Name: flowExhaust.action, Idempotent: true, Retry: exhaust, Execute: write},
		{Name: flowStallOnce.action, Execute: stall(func(int) bool { return true })},
		{Name: flowStallRetry.action, Idempotent: true, Retry: quick, Execute: stall(func(n int) bool { return n == 1 })},
	} {
		a.ParamsSchema = params
		reg.MustAddAction(a)
	}
	return reg
}

// newPool returns a pool of maxConns connections to the test database,
// closed when the test ends.
func newPool(t testing.TB, maxConns int32) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(testpg.DSN(t))
	if err != nil {
		t.Fatalf("parse %s: %v", testpg.EnvVar, err)
	}
	cfg.MaxConns = maxConns
	return testpg.PoolWithConfig(t, cfg)
}

// newEngine builds an Engine of the chaos registry on schema, logging
// warnings to log. It is not started.
func newEngine(t testing.TB, pool *pgxpool.Pool, schema string, sink *Sink, log io.Writer, workers int, roles ...flowkit.Role) *flowkit.Engine {
	t.Helper()
	e, err := flowkit.New(flowkit.Config{
		Pool:         pool,
		Schema:       schema,
		Logger:       slog.New(slog.NewTextHandler(log, &slog.HandlerOptions{Level: slog.LevelWarn})),
		InstanceID:   "chaos-" + strconv.Itoa(os.Getpid()),
		Roles:        roles,
		Workers:      flowkit.WorkerConfig{MaxWorkers: workers, PollInterval: pollInterval},
		ClaimTimeout: claimTimeout,
	}, newRegistry(sink))
	if err != nil {
		t.Fatalf("flowkit.New: %v", err)
	}
	return e
}

// fixture is a migrated flowkit schema with the chaos flows published and a
// sink in a schema of its own. When the test ends, the flowkit schema must
// still hold exactly the objects its migrations created; then both schemas
// are dropped.
type fixture struct {
	t          *testing.T
	pool       *pgxpool.Pool
	schema     string
	sinkSchema string
	sink       *Sink
	// client emits events and publishes definitions; it is never started,
	// so it runs no job.
	client *flowkit.Engine
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	pool := newPool(t, fixtureConns)
	f := &fixture{t: t, pool: pool, schema: testpg.Schema(t, pool), sinkSchema: testpg.Schema(t, pool)}
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	if err := migrate.Run(ctx, pool, f.schema, migrate.Options{}); err != nil {
		t.Fatalf("migrate %s: %v", f.schema, err)
	}
	// Cleanups run last in, first out: the gate runs after every engine
	// started later has closed, and before the schema is dropped.
	t.Cleanup(func() { dbgate.Check(t, pool, f.schema) })
	f.sink = NewSink(pool, f.sinkSchema)
	if err := f.sink.Create(ctx); err != nil {
		t.Fatal(err)
	}
	f.client = newEngine(t, pool, f.schema, f.sink, t.Output(), 1, flowkit.RoleWorker)
	defs := f.client.Definitions()
	for _, fl := range flows {
		id, err := defs.SaveDraft(ctx, fl.name, fl.body())
		if err != nil {
			t.Fatalf("save draft of %s: %v", fl.name, err)
		}
		if _, err := defs.Publish(ctx, id, flowkit.PublishOpts{Actor: "chaos"}); err != nil {
			t.Fatalf("publish %s: %v", fl.name, err)
		}
	}
	return f
}

// start builds and starts an Engine on the fixture's schema, closed when
// the test ends.
func (f *fixture) start(workers int, roles ...flowkit.Role) {
	f.t.Helper()
	e := newEngine(f.t, f.pool, f.schema, f.sink, f.t.Output(), workers, roles...)
	ctx, stop := context.WithCancel(context.Background())
	f.t.Cleanup(func() {
		defer stop()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := e.Close(ctx); err != nil {
			f.t.Errorf("close engine: %v", err)
		}
	})
	if err := e.Start(ctx); err != nil {
		f.t.Fatalf("start engine: %v", err)
	}
}

// emit emits one event of each type for key in a single transaction.
func (f *fixture) emit(key string, types ...string) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	err := pgx.BeginFunc(ctx, f.pool, func(tx pgx.Tx) error {
		for _, typ := range types {
			if err := f.client.Emit(ctx, tx, flowkit.Event{Type: typ, Payload: map[string]any{"key": key}}); err != nil {
				return fmt.Errorf("emit %s: %w", typ, err)
			}
		}
		return nil
	})
	if err != nil {
		f.t.Fatalf("emit for %s: %v", key, err)
	}
}

// sql qualifies query's tables: {s} stands for the flowkit schema and
// {sink} for the sink table.
func (f *fixture) sql(query string) string {
	return strings.NewReplacer(
		"{s}", pgx.Identifier{f.schema}.Sanitize()+".",
		"{sink}", pgx.Identifier{f.sinkSchema, "chaos_sink"}.Sanitize(),
	).Replace(query)
}

// count runs a count(*) query.
func (f *fixture) count(query string, args ...any) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	var n int
	if err := f.pool.QueryRow(ctx, f.sql(query), args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("%s: %w", query, err)
	}
	return n, nil
}

// mustCount is count failing the test on an error.
func (f *fixture) mustCount(query string, args ...any) int {
	f.t.Helper()
	n, err := f.count(query, args...)
	if err != nil {
		f.t.Fatal(err)
	}
	return n
}

// lines returns the rows of query, each row's columns joined by spaces.
func (f *fixture) lines(query string, args ...any) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	rows, err := f.pool.Query(ctx, f.sql(query), args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", query, err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (string, error) {
		vals, err := r.Values()
		if err != nil {
			return "", err
		}
		parts := make([]string, len(vals))
		for i, v := range vals {
			parts[i] = fmt.Sprint(v)
		}
		return strings.Join(parts, " "), nil
	})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", query, err)
	}
	return out, nil
}

// mustLines is lines failing the test on an error.
func (f *fixture) mustLines(query string, args ...any) []string {
	f.t.Helper()
	out, err := f.lines(query, args...)
	if err != nil {
		f.t.Fatal(err)
	}
	return out
}

// eventually polls cond until it returns nil, and fails the test with its
// last error when that takes longer than timeout.
func eventually(t *testing.T, timeout time.Duration, cond func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	tick := time.NewTicker(settlePoll)
	defer tick.Stop()
	for {
		err := cond()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("not settled after %s: %v", timeout, err)
		}
		<-tick.C
	}
}
