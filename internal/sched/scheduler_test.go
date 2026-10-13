package sched

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/migrate"
	"github.com/g0-up/flowkit/internal/store"
	"github.com/g0-up/flowkit/internal/testpg"
)

const stopTimeout = 15 * time.Second

func testLogger(t testing.TB) *slog.Logger {
	return slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// migratedSchema returns a pool and a fresh schema migrated by flowkit, so
// River's tables are in it.
func migratedSchema(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	pool, schema := testpg.New(t)
	if err := migrate.Run(context.Background(), pool, schema, migrate.Options{}); err != nil {
		t.Fatalf("migrate %s: %v", schema, err)
	}
	return pool, schema
}

func pgOptions(t *testing.T, pool *pgxpool.Pool, schema string, worker, janitor bool) Options {
	st, err := store.NewPgStore(pool, schema)
	if err != nil {
		t.Fatal(err)
	}
	return Options{
		Pool:         pool,
		Store:        st,
		InstanceID:   "test",
		Worker:       worker,
		MaxWorkers:   2,
		Janitor:      janitor,
		PollInterval: 200 * time.Millisecond,
		Timing:       NewTiming(5 * time.Minute),
		Logger:       testLogger(t),
		Clock:        clockFunc(time.Now),
	}
}

// newTestClient builds a client and stops it when the test ends. The
// cleanup runs before the schema is dropped.
func newTestClient(t *testing.T, o Options, h Handlers) *Client {
	t.Helper()
	c, err := New(o, h)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
		defer cancel()
		if err := c.Stop(ctx); err != nil {
			t.Errorf("stop client: %v", err)
		}
	})
	return c
}

// funcHandlers runs the given function per kind, completing the job when a
// function is nil.
type funcHandlers struct {
	route   func(context.Context, RouteEventArgs) Result
	pump    func(context.Context, PumpRunArgs) Result
	timeout func(context.Context, WaitTimeoutArgs) Result
	janitor func(context.Context, JanitorArgs) Result
}

func (h funcHandlers) Route(ctx context.Context, a RouteEventArgs) Result {
	if h.route == nil {
		return Done()
	}
	return h.route(ctx, a)
}

func (h funcHandlers) Pump(ctx context.Context, a PumpRunArgs) Result {
	if h.pump == nil {
		return Done()
	}
	return h.pump(ctx, a)
}

func (h funcHandlers) Timeout(ctx context.Context, a WaitTimeoutArgs) Result {
	if h.timeout == nil {
		return Done()
	}
	return h.timeout(ctx, a)
}

func (h funcHandlers) Janitor(ctx context.Context, a JanitorArgs) Result {
	if h.janitor == nil {
		return Done()
	}
	return h.janitor(ctx, a)
}

// recorder notes every job a client works, by kind.
type recorder struct {
	mu    sync.Mutex
	kinds []string
}

func (r *recorder) add(kind string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.kinds = append(r.kinds, kind)
}

func (r *recorder) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.kinds)
}

func (r *recorder) handlers() Handlers {
	return funcHandlers{
		route:   func(context.Context, RouteEventArgs) Result { r.add(KindRouteEvent); return Done() },
		pump:    func(context.Context, PumpRunArgs) Result { r.add(KindPumpRun); return Done() },
		timeout: func(context.Context, WaitTimeoutArgs) Result { r.add(KindWaitTimeout); return Done() },
		janitor: func(context.Context, JanitorArgs) Result { r.add(KindJanitor); return Done() },
	}
}

// commitTx runs fn in a transaction of st and commits it.
func commitTx(t *testing.T, pool *pgxpool.Pool, st *store.PgStore, fn func(store.Tx)) {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	fn(st.WrapTx(tx))
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

type jobRow struct {
	ID          int64
	Kind        string
	Queue       string
	State       string
	Attempt     int
	ScheduledAt time.Time
}

func jobs(t *testing.T, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, schema string) []jobRow {
	t.Helper()
	rows, err := q.Query(context.Background(),
		"SELECT id, kind, queue, state::text, attempt, scheduled_at FROM "+pgx.Identifier{schema, "river_job"}.Sanitize()+" ORDER BY id")
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByPos[jobRow])
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	return out
}

func leaderID(t *testing.T, pool *pgxpool.Pool, schema string) string {
	t.Helper()
	var id string
	err := pool.QueryRow(context.Background(),
		"SELECT leader_id FROM "+pgx.Identifier{schema, "river_leader"}.Sanitize()).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return ""
	}
	if err != nil {
		t.Fatalf("read leader: %v", err)
	}
	return id
}

func eventually(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for: %s", timeout, msg)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// otherArgs is a kind no flowkit client registers.
type otherArgs struct{}

func (otherArgs) Kind() string { return "flowkit_test.other" }

func TestEveryRoleRegistersEveryKind(t *testing.T) {
	pool, schema := migratedSchema(t)
	roles := []struct {
		name            string
		worker, janitor bool
	}{
		{"worker and janitor", true, true},
		{"worker only", true, false},
		{"janitor only", false, true},
	}
	for _, role := range roles {
		t.Run(role.name, func(t *testing.T) {
			c := newTestClient(t, pgOptions(t, pool, schema, role.worker, role.janitor), StubHandlers{})
			s := c.Scheduler()
			ctx := context.Background()
			ptx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = ptx.Rollback(ctx) }()
			tx := s.store.WrapTx(ptx)
			// River refuses to insert a kind its client has no worker for.
			if err := s.insert(ctx, tx, otherArgs{}, nil); err == nil {
				t.Fatal("inserting an unregistered kind succeeded; the check below proves nothing")
			}
			// The failed insert was rejected before reaching the database.
			if err := s.EnqueueRoute(ctx, tx, 1); err != nil {
				t.Errorf("route_event: %v", err)
			}
			if err := s.EnqueuePump(ctx, tx, ids.NewV4()); err != nil {
				t.Errorf("pump_run: %v", err)
			}
			if err := s.ScheduleTimeout(ctx, tx, WaitTimeoutArgs{RunID: ids.NewV4(), StepID: "s", WaitSeq: 1, DueAt: time.Now().Add(time.Hour)}); err != nil {
				t.Errorf("wait_timeout: %v", err)
			}
			if err := s.insert(ctx, tx, JanitorArgs{}, nil); err != nil {
				t.Errorf("janitor: %v", err)
			}
		})
	}
}

func TestSchedulerInserts(t *testing.T) {
	pool, schema := migratedSchema(t)
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	o := pgOptions(t, pool, schema, true, true)
	o.Clock = clockFunc(func() time.Time { return now })
	s := newTestClient(t, o, StubHandlers{}).Scheduler()
	ctx := context.Background()
	runID := ids.NewV4()
	due := now.Add(3 * time.Hour)
	commitTx(t, pool, s.store, func(tx store.Tx) {
		for _, err := range []error{
			s.EnqueueRoute(ctx, tx, 42),
			s.EnqueuePump(ctx, tx, runID),
			s.SnoozePump(ctx, tx, runID, 90*time.Second),
			s.ScheduleTimeout(ctx, tx, WaitTimeoutArgs{RunID: runID, StepID: "approve", WaitSeq: 2, DueAt: due}),
		} {
			if err != nil {
				t.Fatal(err)
			}
		}
	})
	got := jobs(t, pool, schema)
	if len(got) != 4 {
		t.Fatalf("jobs = %+v, want 4", got)
	}
	wantKinds := []string{KindRouteEvent, KindPumpRun, KindPumpRun, KindWaitTimeout}
	for i, j := range got {
		if j.Kind != wantKinds[i] || j.Queue != Queue {
			t.Errorf("job %d = %s on %s, want %s on %s", i, j.Kind, j.Queue, wantKinds[i], Queue)
		}
	}
	if got[0].State != "available" || got[1].State != "available" {
		t.Errorf("immediate jobs are %s and %s, want available", got[0].State, got[1].State)
	}
	if !got[2].ScheduledAt.Equal(now.Add(90*time.Second)) || got[2].State != "scheduled" {
		t.Errorf("snoozed pump: %s at %s, want scheduled at %s", got[2].State, got[2].ScheduledAt, now.Add(90*time.Second))
	}
	if !got[3].ScheduledAt.Equal(due) || got[3].State != "scheduled" {
		t.Errorf("wait timeout: %s at %s, want scheduled at %s", got[3].State, got[3].ScheduledAt, due)
	}

	var args string
	err := pool.QueryRow(ctx, "SELECT args::text FROM "+pgx.Identifier{schema, "river_job"}.Sanitize()+" WHERE id = $1", got[3].ID).Scan(&args)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"run_id": "` + runID.String() + `"`, `"step_id": "approve"`, `"wait_seq": 2`, `"due_at": "2030-01-02T06:04:05Z"`} {
		if !strings.Contains(args, want) {
			t.Errorf("wait timeout args %s lack %s", args, want)
		}
	}

	commitTx(t, pool, s.store, func(tx store.Tx) {
		if err := s.ScheduleTimeout(ctx, tx, WaitTimeoutArgs{RunID: runID}); err == nil {
			t.Error("a wait timeout without a due time was accepted")
		}
	})
}

func TestEnqueuePumpMany(t *testing.T) {
	pool, schema := migratedSchema(t)
	s := newTestClient(t, pgOptions(t, pool, schema, true, true), StubHandlers{}).Scheduler()
	ctx := context.Background()
	runIDs := []ids.UUID{ids.NewV4(), ids.NewV4(), ids.NewV4()}
	commitTx(t, pool, s.store, func(tx store.Tx) {
		if err := s.EnqueuePumpMany(ctx, tx, nil); err != nil {
			t.Fatal(err)
		}
		if err := s.EnqueuePumpMany(ctx, tx, runIDs); err != nil {
			t.Fatal(err)
		}
	})
	got := jobs(t, pool, schema)
	if len(got) != len(runIDs) {
		t.Fatalf("jobs = %+v, want %d", got, len(runIDs))
	}
	seen := map[string]bool{}
	for _, j := range got {
		if j.Kind != KindPumpRun || j.Queue != Queue || j.State != "available" {
			t.Errorf("job = %+v, want an available %s on %s", j, KindPumpRun, Queue)
		}
		var args string
		if err := pool.QueryRow(ctx, "SELECT args->>'run_id' FROM "+pgx.Identifier{schema, "river_job"}.Sanitize()+" WHERE id = $1", j.ID).Scan(&args); err != nil {
			t.Fatal(err)
		}
		seen[args] = true
	}
	for _, id := range runIDs {
		if !seen[id.String()] {
			t.Errorf("no pump for run %s", id)
		}
	}
	if err := s.EnqueuePumpMany(ctx, nil, runIDs); !errors.Is(err, store.ErrForeignTx) {
		t.Errorf("EnqueuePumpMany with a nil Tx: %v", err)
	}
}

func TestSchedulerRejectsForeignTx(t *testing.T) {
	pool, schema := migratedSchema(t)
	s := newTestClient(t, pgOptions(t, pool, schema, true, true), StubHandlers{}).Scheduler()
	other, err := store.NewPgStore(pool, testpg.Schema(t, pool))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ptx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ptx.Rollback(ctx) }()
	for name, tx := range map[string]store.Tx{"another store": other.WrapTx(ptx), "nil": nil} {
		if err := s.EnqueueRoute(ctx, tx, 1); !errors.Is(err, store.ErrForeignTx) {
			t.Errorf("EnqueueRoute with a Tx of %s: %v", name, err)
		}
	}
	if got := jobs(t, ptx, schema); len(got) != 0 {
		t.Fatalf("a foreign Tx enqueued %+v", got)
	}
}

func TestWorkerOnlyClientNeverLeads(t *testing.T) {
	pool, schema := migratedSchema(t)
	var rec recorder
	c := newTestClient(t, pgOptions(t, pool, schema, true, false), rec.handlers())
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	commitTx(t, pool, c.sched.store, func(tx store.Tx) {
		if err := c.Scheduler().EnqueueRoute(context.Background(), tx, 1); err != nil {
			t.Fatal(err)
		}
	})
	eventually(t, 10*time.Second, "worker-only client works a route job", func() bool { return len(rec.seen()) == 1 })
	// A client that elects starts trying at once; give it a few rounds.
	for range 20 {
		if id := leaderID(t, pool, schema); id != "" {
			t.Fatalf("worker-only client %s became leader", id)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestJanitorOnlyClientLeadsAndWorksNothing(t *testing.T) {
	pool, schema := migratedSchema(t)
	var rec recorder
	c := newTestClient(t, pgOptions(t, pool, schema, false, true), rec.handlers())
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("janitor-only client does not start: %v", err)
	}
	id := c.river.ID()
	eventually(t, 15*time.Second, "janitor-only client leads", func() bool { return leaderID(t, pool, schema) == id })

	commitTx(t, pool, c.sched.store, func(tx store.Tx) {
		s := c.Scheduler()
		ctx := context.Background()
		if err := s.EnqueueRoute(ctx, tx, 1); err != nil {
			t.Fatal(err)
		}
		args := WaitTimeoutArgs{RunID: ids.NewV4(), StepID: "s", WaitSeq: 1, DueAt: time.Now().Add(time.Second)}
		if err := s.ScheduleTimeout(ctx, tx, args); err != nil {
			t.Fatal(err)
		}
	})
	// The leader's scheduler makes the timeout available, and its periodic
	// job enqueues a janitor pass on start...
	eventually(t, 20*time.Second, "leader promotes the scheduled timeout and enqueues the janitor", func() bool {
		kinds := map[string]string{}
		for _, j := range jobs(t, pool, schema) {
			kinds[j.Kind] = j.State
		}
		return len(kinds) == 3 && kinds[KindWaitTimeout] == "available" && kinds[KindJanitor] == "available"
	})
	// ...but the janitor-only client works none of the jobs.
	time.Sleep(2 * time.Second)
	for _, j := range jobs(t, pool, schema) {
		if j.State != "available" || j.Attempt != 0 {
			t.Errorf("job %s is %s after %d attempts, want available and untouched", j.Kind, j.State, j.Attempt)
		}
	}
	if seen := rec.seen(); len(seen) != 0 {
		t.Fatalf("janitor-only client worked %v", seen)
	}
}

func TestStopCancelsJobsWhenCtxEnds(t *testing.T) {
	pool, schema := migratedSchema(t)
	started := make(chan struct{})
	cancelled := make(chan struct{})
	var once sync.Once
	h := funcHandlers{route: func(ctx context.Context, _ RouteEventArgs) Result {
		once.Do(func() { close(started) })
		<-ctx.Done()
		close(cancelled)
		return Retry(ctx.Err())
	}}
	// The cleanup only matters when the test fails before its own Stop.
	c := newTestClient(t, pgOptions(t, pool, schema, true, false), h)
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	commitTx(t, pool, c.sched.store, func(tx store.Tx) {
		if err := c.Scheduler().EnqueueRoute(context.Background(), tx, 1); err != nil {
			t.Fatal(err)
		}
	})
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("job never started")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := c.Stop(ctx)
	if err == nil || !strings.Contains(err.Error(), "graceful stop cut short") {
		t.Fatalf("Stop = %v, want a cut-short error", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Stop error %v does not keep the context error", err)
	}
	select {
	case <-cancelled:
	default:
		t.Fatal("Stop returned before the running job saw its cancellation")
	}
}

func TestStopIdleAndUnstartedClients(t *testing.T) {
	pool, schema := migratedSchema(t)
	ctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	defer cancel()
	unstarted, err := New(pgOptions(t, pool, schema, true, true), StubHandlers{})
	if err != nil {
		t.Fatal(err)
	}
	if err := unstarted.Stop(ctx); err != nil {
		t.Errorf("Stop of a client never started: %v", err)
	}
	idle, err := New(pgOptions(t, pool, schema, true, true), StubHandlers{})
	if err != nil {
		t.Fatal(err)
	}
	if err := idle.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := idle.Stop(ctx); err != nil {
		t.Errorf("Stop of an idle client: %v", err)
	}
}

// indexFiles maps each index of River's tables in schema to its file node,
// which REINDEX CONCURRENTLY changes, and reports leftover reindex artifacts.
func indexFiles(t *testing.T, pool *pgxpool.Pool, schema string) (files map[string]uint32, artifacts []string) {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT c.relname, c.relfilenode::bigint
		  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = $1 AND c.relkind = 'i' AND c.relname LIKE 'river\_%'`, schema)
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}
	files = map[string]uint32{}
	for rows.Next() {
		var name string
		var node int64
		if err := rows.Scan(&name, &node); err != nil {
			t.Fatal(err)
		}
		files[name] = uint32(node)
		if strings.Contains(name, "_ccnew") || strings.Contains(name, "_ccold") {
			artifacts = append(artifacts, name)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return files, artifacts
}

// startReindexingLeader starts a janitor client on schema whose reindexer
// runs every few hundred milliseconds, with indexNames as configured by
// riverConfig unless overridden.
func startReindexingLeader(t *testing.T, pool *pgxpool.Pool, schema string, override func(*river.Config)) string {
	t.Helper()
	o := pgOptions(t, pool, schema, false, true)
	workers, err := newWorkers(StubHandlers{}, o.Timing)
	if err != nil {
		t.Fatal(err)
	}
	cfg := riverConfig(o, workers)
	cfg.ReindexerSchedule = river.PeriodicInterval(300 * time.Millisecond)
	if override != nil {
		override(cfg)
	}
	rc, err := river.NewClient(riverpgxv5.New(pool), cfg)
	if err != nil {
		t.Fatal(err)
	}
	c := &Client{river: rc}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
		defer cancel()
		if err := c.Stop(ctx); err != nil {
			t.Errorf("stop client: %v", err)
		}
	})
	if err := rc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, 15*time.Second, "client leads", func() bool { return leaderID(t, pool, schema) == rc.ID() })
	return rc.ID()
}

func TestReindexerIsDisabled(t *testing.T) {
	t.Run("control: River's default list reindexes", func(t *testing.T) {
		pool, schema := migratedSchema(t)
		before, _ := indexFiles(t, pool, schema)
		startReindexingLeader(t, pool, schema, func(cfg *river.Config) { cfg.ReindexerIndexNames = nil })
		eventually(t, 20*time.Second, "an index is rebuilt", func() bool {
			after, _ := indexFiles(t, pool, schema)
			for name, node := range before {
				if after[name] != node {
					return true
				}
			}
			return false
		})
	})
	t.Run("flowkit's configuration reindexes nothing", func(t *testing.T) {
		pool, schema := migratedSchema(t)
		before, _ := indexFiles(t, pool, schema)
		startReindexingLeader(t, pool, schema, nil)
		// Stagger (up to 1s) plus many 300ms rounds.
		time.Sleep(4 * time.Second)
		after, artifacts := indexFiles(t, pool, schema)
		if len(artifacts) > 0 {
			t.Errorf("reindex artifacts left: %v", artifacts)
		}
		for name, node := range before {
			if after[name] != node {
				t.Errorf("index %s was rebuilt", name)
			}
		}
	})
}
