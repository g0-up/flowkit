package flowkittest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/g0-up/flowkit"
	"github.com/g0-up/flowkit/internal/sched"
	"github.com/g0-up/flowkit/internal/testpg"
	"github.com/g0-up/flowkit/migrate"
)

const (
	// defaultIdleTimeout bounds Advance. River's scheduler promotes due jobs
	// on a fixed 5s tick, and a leader that steps down after a large jump
	// of the clock is re-elected about 5s later, so it must stay well above
	// 10s on a loaded test machine.
	defaultIdleTimeout = 30 * time.Second
	// idlePoll is how often WaitIdle reads the job table.
	idlePoll = 50 * time.Millisecond
	// setupTimeout bounds migrating, starting and stopping the Engine.
	setupTimeout = 30 * time.Second
	// opTimeout bounds one database call the harness makes for the test.
	opTimeout = 10 * time.Second
	// harnessExtraConns are the pool connections beyond the workers: River's
	// listener and leader election, and the harness's own queries.
	harnessExtraConns = 12
	// harnessWorkers is the default Config.Workers.MaxWorkers of the harness
	// Engine, and harnessPoll its default Config.Workers.PollInterval.
	harnessWorkers = 4
	harnessPoll    = 100 * time.Millisecond
	// maxHarnessWorkers is flowkit's limit on Config.Workers.MaxWorkers.
	maxHarnessWorkers = 10000
)

// PGHarness runs a real Engine on Postgres with a VirtualClock, for tests
// that need flowkit's exact semantics. Each harness gets its own randomly
// named schema on the database named by FLOWKIT_TEST_DATABASE_URL, migrated
// with migrate.Run and dropped when the test ends. Without the variable
// the test is skipped, unless the CI variable is set, which fails it.
//
// The Engine runs with RoleWorker and RoleJanitor, so it works jobs and
// leads River, whose scheduler makes timers due. River follows the
// harness clock: the clock does not move by itself, and a timer, a retry
// backoff or any other delayed job becomes due only when Advance moves the
// clock past it.
//
// The methods report failures through the test and stop it with
// t.Fatalf, so call them from the goroutine running the test.
type PGHarness struct {
	t      testing.TB
	engine *flowkit.Engine
	clock  *VirtualClock
	pool   *pgxpool.Pool
	schema string
}

// HarnessOption configures NewPGHarness.
type HarnessOption func(*harnessConfig)

type harnessConfig struct {
	start time.Time
	cfg   flowkit.Config
}

// HarnessStart starts the harness clock at start instead of the current
// time.
func HarnessStart(start time.Time) HarnessOption {
	return func(c *harnessConfig) { c.start = start }
}

// HarnessConfig configures the harness Engine with cfg, defaulted as
// flowkit.New defaults it. Pool, Schema, Clock and Roles are not used: the
// Engine runs on the harness pool and schema with the harness clock, as
// RoleWorker and RoleJanitor. InstanceID defaults to "flowkittest", Logger
// to Warn and above written to the test output, Workers.MaxWorkers to 4
// and Workers.PollInterval to 100ms. The pool gets enough connections for
// Workers.MaxWorkers.
func HarnessConfig(cfg flowkit.Config) HarnessOption {
	return func(c *harnessConfig) { c.cfg = cfg }
}

// NewPGHarness creates a schema, migrates it, builds an Engine for reg on
// it with a VirtualClock and starts the Engine. The Engine is closed and
// the schema dropped when the test ends.
func NewPGHarness(t testing.TB, reg *flowkit.Registry, opts ...HarnessOption) *PGHarness {
	t.Helper()
	hc := harnessConfig{start: time.Now().UTC().Truncate(time.Second)}
	for _, opt := range opts {
		if opt != nil {
			opt(&hc)
		}
	}
	cfg := hc.cfg
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelWarn}))
	}
	if cfg.InstanceID == "" {
		cfg.InstanceID = "flowkittest"
	}
	if cfg.Workers.MaxWorkers == 0 {
		cfg.Workers.MaxWorkers = harnessWorkers
	}
	if cfg.Workers.PollInterval == 0 {
		cfg.Workers.PollInterval = harnessPoll
	}

	poolCfg, err := pgxpool.ParseConfig(testpg.DSN(t))
	if err != nil {
		t.Fatalf("flowkittest: parse %s: %v", testpg.EnvVar, err)
	}
	// flowkit.New rejects a MaxWorkers out of range; the clamp only keeps
	// the pool valid until then.
	poolCfg.MaxConns = int32(min(max(cfg.Workers.MaxWorkers, 0), maxHarnessWorkers)) + harnessExtraConns
	pool := testpg.PoolWithConfig(t, poolCfg)
	schema := testpg.Schema(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), setupTimeout)
	defer cancel()
	if err := migrate.Run(ctx, pool, schema, migrate.Options{}); err != nil {
		t.Fatalf("flowkittest: migrate schema %s: %v", schema, err)
	}
	clock := NewVirtualClock(hc.start)
	cfg.Pool, cfg.Schema, cfg.Clock = pool, schema, clock
	cfg.Roles = []flowkit.Role{flowkit.RoleWorker, flowkit.RoleJanitor}
	engine, err := flowkit.New(cfg, reg)
	if err != nil {
		t.Fatalf("flowkittest: New: %v", err)
	}
	runCtx, stop := context.WithCancel(context.Background())
	// Cleanups run last in, first out: the Engine stops before the schema
	// is dropped and the pool closed.
	t.Cleanup(func() {
		defer stop()
		ctx, cancel := context.WithTimeout(context.Background(), setupTimeout)
		defer cancel()
		if err := engine.Close(ctx); err != nil {
			t.Errorf("flowkittest: close engine: %v", err)
		}
	})
	if err := engine.Start(runCtx); err != nil {
		t.Fatalf("flowkittest: start engine: %v", err)
	}
	return &PGHarness{t: t, engine: engine, clock: clock, pool: pool, schema: schema}
}

// Engine returns the harness Engine.
func (h *PGHarness) Engine() *flowkit.Engine { return h.engine }

// Clock returns the harness clock. Prefer Advance to moving it directly:
// Advance also waits for the jobs that became due.
func (h *PGHarness) Clock() *VirtualClock { return h.clock }

// Emit emits evt in a transaction of its own and commits it. Any error,
// ErrDuplicateEvent included, fails the test; call Engine().Emit to test
// the error itself. Emit does not wait for the event to be routed; use
// WaitIdle or Advance.
func (h *PGHarness) Emit(evt flowkit.Event) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	err := pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		return h.engine.Emit(ctx, tx, evt)
	})
	if err != nil {
		h.t.Fatalf("flowkittest: emit %s: %v", evt.Type, err)
	}
}

// Publish saves body as a draft of definition name and publishes it,
// returning the new version.
func (h *PGHarness) Publish(name string, body json.RawMessage) flowkit.Version {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	defs := h.engine.Definitions()
	id, err := defs.SaveDraft(ctx, name, body)
	if err != nil {
		h.t.Fatalf("flowkittest: save draft of %s: %v", name, err)
	}
	v, err := defs.Publish(ctx, id, flowkit.PublishOpts{Actor: "flowkittest"})
	if err != nil {
		h.t.Fatalf("flowkittest: publish %s: %v", name, err)
	}
	return v
}

// Advance moves the clock forward by d and waits, as WaitIdle does for up
// to 30s, until every job due by the new time has run. Expect it to take
// about 5s whenever a timer or delayed job becomes due: River's scheduler
// promotes due jobs on a fixed 5s tick. A large jump also makes the River
// leader step down and get re-elected, logging a warning.
//
// Advance does not wait for a periodic janitor pass: River enqueues one
// on a real-time timer or when a leader is elected, so a pass can run
// after Advance returns, between the test's assertions. A test of the
// janitor's effects must poll for them.
func (h *PGHarness) Advance(d time.Duration) {
	h.t.Helper()
	if d < 0 {
		h.t.Fatalf("flowkittest: Advance(%s): the clock only moves forward", d)
	}
	h.clock.Advance(d)
	h.waitIdle("Advance("+d.String()+")", defaultIdleTimeout)
}

// WaitIdle waits until no flowkit job is due by the harness clock: none is
// running, and none waiting to run has a scheduled time at or before the
// clock. Jobs due later stay queued. It fails the test after timeout,
// listing the jobs still due and saying whether a River leader is elected;
// a timeout of zero or less selects 30s. Like Advance, it does not wait for
// a periodic janitor pass River has not enqueued yet.
func (h *PGHarness) WaitIdle(timeout time.Duration) {
	h.t.Helper()
	if timeout <= 0 {
		timeout = defaultIdleTimeout
	}
	h.waitIdle("WaitIdle", timeout)
}

func (h *PGHarness) waitIdle(op string, timeout time.Duration) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		jobs, err := h.dueJobs()
		if err != nil {
			h.t.Fatalf("flowkittest: %s: %v", op, err)
		}
		if len(jobs) == 0 {
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("flowkittest: %s: jobs still due after %s at virtual time %s:\n%s%s",
				op, timeout, h.clock.Now().Format(time.RFC3339), formatJobs(jobs), h.leaderStatus())
		}
		time.Sleep(idlePoll)
	}
}

// dueJob is a flowkit job WaitIdle is waiting for.
type dueJob struct {
	id          int64
	kind, state string
	attempt     int
	scheduledAt time.Time
	lastError   string
}

// dueJobs lists the flowkit jobs running, or waiting to run with a
// scheduled time at or before the clock. A job River promoted ahead of
// its time is not due yet: River fetches it only once the clock reaches
// its scheduled time.
func (h *PGHarness) dueJobs() ([]dueJob, error) {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	rows, err := h.pool.Query(ctx, `
		SELECT id, kind, state::text, attempt, scheduled_at, coalesce(errors[array_upper(errors, 1)]->>'error', '')
		FROM `+pgx.Identifier{h.schema, "river_job"}.Sanitize()+`
		WHERE queue = $1
		  AND (state = 'running' OR (state IN ('available', 'pending', 'retryable', 'scheduled') AND scheduled_at <= $2))
		ORDER BY id`, sched.Queue, h.clock.Now())
	if err != nil {
		return nil, fmt.Errorf("read River jobs: %w", err)
	}
	jobs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (dueJob, error) {
		var j dueJob
		err := row.Scan(&j.id, &j.kind, &j.state, &j.attempt, &j.scheduledAt, &j.lastError)
		return j, err
	})
	if err != nil {
		return nil, fmt.Errorf("read River jobs: %w", err)
	}
	return jobs, nil
}

func formatJobs(jobs []dueJob) string {
	var b strings.Builder
	for _, j := range jobs {
		fmt.Fprintf(&b, "  job %d %s: %s, attempt %d, scheduled %s", j.id, j.kind, j.state, j.attempt, j.scheduledAt.UTC().Format(time.RFC3339))
		if j.lastError != "" {
			fmt.Fprintf(&b, ", last error: %s", j.lastError)
		}
		b.WriteByte('\n')
	}
	return b.String()
}

// leaderStatus says whether a River leader is elected. Without one no
// scheduled job is ever promoted.
func (h *PGHarness) leaderStatus() string {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	var leader string
	err := h.pool.QueryRow(ctx, "SELECT leader_id FROM "+pgx.Identifier{h.schema, "river_leader"}.Sanitize()).Scan(&leader)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return "no River leader is elected: only the leader promotes scheduled jobs, so timers and retries cannot become due " +
			"(a leader that stepped down after a clock jump is normally re-elected within about 5s)"
	case err != nil:
		return "the River leader could not be read: " + err.Error()
	}
	return "River leader: " + leader
}

// RunView is a run as it stands now, for quick assertions.
type RunView struct {
	// State is running, waiting, completed, failed or cancelled.
	State string
	// TerminalName is the name the run ended with; empty while it runs.
	TerminalName string
	// WaitStep is the step the run waits at; empty unless State is
	// waiting.
	WaitStep string
	// Facts are the run's facts by name. JSON numbers are json.Number.
	Facts map[string]any
}

// Run returns the current state of run runID. A run that does not exist
// fails the test.
func (h *PGHarness) Run(runID string) RunView {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	var (
		v              RunView
		terminal, wait *string
		facts          []byte
	)
	err := h.pool.QueryRow(ctx,
		"SELECT state, terminal_name, wait_step, facts FROM "+pgx.Identifier{h.schema, "runs"}.Sanitize()+" WHERE id::text = $1",
		runID).Scan(&v.State, &terminal, &wait, &facts)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		h.t.Fatalf("flowkittest: run %s does not exist", runID)
	case err != nil:
		h.t.Fatalf("flowkittest: read run %s: %v", runID, err)
	}
	if terminal != nil {
		v.TerminalName = *terminal
	}
	if wait != nil && v.State == "waiting" {
		v.WaitStep = *wait
	}
	dec := json.NewDecoder(bytes.NewReader(facts))
	dec.UseNumber()
	if err := dec.Decode(&v.Facts); err != nil {
		h.t.Fatalf("flowkittest: decode facts of run %s: %v", runID, err)
	}
	return v
}
