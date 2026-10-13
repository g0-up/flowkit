package flowkittest

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/g0-up/flowkit"
	"github.com/g0-up/flowkit/internal/sched/memsched"
	"github.com/g0-up/flowkit/internal/simhook"
	"github.com/g0-up/flowkit/internal/store"
	"github.com/g0-up/flowkit/internal/store/memstore"
)

// Sim runs a real Engine in memory, for fast tests of definition logic
// without a database. The interpreter and the executor are flowkit's own;
// the store keeps flowkit's tables in memory with the constraints the
// Engine relies on, and an in-memory queue replaces River.
//
// Actions never run their registered Execute. Each runs the stub given to
// StubAction instead, through the real executor: claims, retries and their
// backoff, NotPerformed and in-doubt effects behave as on Postgres. An
// action that runs without a stub, or whose stub calls t.Fatal or does not
// return before its context ends, fails the test, naming the action.
//
// The Engine stays inside the Sim: Emit emits, Advance runs the jobs, and
// Runs reads the runs. Time moves only through Advance, which runs every
// job due on the way at its due time. Fact resolvers cannot query a
// database: every query of their querier returns an error.
//
// The methods report failures through the test and stop it with t.Fatalf,
// so call them from the goroutine running the test.
type Sim struct {
	t      testing.TB
	engine *flowkit.Engine
	clock  *VirtualClock
	store  *memstore.Store
	jobs   *memsched.Scheduler
	emit   func(ctx context.Context, tx store.Tx, evt any) error
	stubs  *stubs
}

// SimOption configures NewSim.
type SimOption func(*simConfig)

type simConfig struct {
	start time.Time
	cfg   flowkit.Config
}

// SimStart starts the Sim clock at start instead of the current time.
func SimStart(start time.Time) SimOption {
	return func(c *simConfig) { c.start = start }
}

// SimConfig configures the Engine of the Sim with cfg, defaulted as flowkit.New
// defaults it. Pool, Schema, Clock and Roles are not used, and
// InstanceID defaults to "flowkittest".
func SimConfig(cfg flowkit.Config) SimOption {
	return func(c *simConfig) { c.cfg = cfg }
}

// NewSim builds an Engine for reg in memory with a VirtualClock. Stub the
// actions the test runs with StubAction. The Engine is closed when the
// test ends.
func NewSim(t testing.TB, reg *flowkit.Registry, opts ...SimOption) *Sim {
	t.Helper()
	sc := simConfig{start: time.Now().UTC().Truncate(time.Second)}
	for _, opt := range opts {
		if opt != nil {
			opt(&sc)
		}
	}
	cfg := sc.cfg
	clock := NewVirtualClock(sc.start)
	cfg.Pool, cfg.Clock, cfg.Roles = nil, clock, nil
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelWarn}))
	}
	if cfg.InstanceID == "" {
		cfg.InstanceID = "flowkittest"
	}

	st := memstore.New()
	stubs := newStubs(t.Logf)
	jobs, err := memsched.New(memsched.Options{
		Store:     st,
		Clock:     clock,
		Logger:    cfg.Logger,
		Interrupt: stubs.failure,
	})
	if err != nil {
		t.Fatalf("flowkittest: NewSim: %v", err)
	}
	built, err := simhook.NewEngine(simhook.Options{
		Config:   cfg,
		Registry: reg,
		Store:    st,
		Jobs:     jobs,
		Execute:  stubs.execute,
	})
	if err != nil {
		t.Fatalf("flowkittest: NewSim: %v", err)
	}
	engine, ok := built.Engine.(*flowkit.Engine)
	if !ok {
		t.Fatalf("flowkittest: NewSim: the hook built a %T, want *flowkit.Engine", built.Engine)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), setupTimeout)
		defer cancel()
		if err := engine.Close(ctx); err != nil {
			t.Errorf("flowkittest: close sim engine: %v", err)
		}
	})
	if err := jobs.Start(built.Handlers, built.Timing); err != nil {
		t.Fatalf("flowkittest: NewSim: %v", err)
	}
	return &Sim{t: t, engine: engine, clock: clock, store: st, jobs: jobs, emit: built.Emit, stubs: stubs}
}

// StubAction makes action run fn in place of its Execute from now on. A
// later call for the same action replaces the stub.
func (s *Sim) StubAction(action string, fn StubFunc) {
	s.t.Helper()
	if fn == nil {
		s.t.Fatalf("flowkittest: StubAction(%q): nil stub", action)
	}
	s.stubs.set(action, fn)
}

// Clock returns the Sim clock. Prefer Advance to moving it directly:
// Advance also runs the jobs that became due.
func (s *Sim) Clock() *VirtualClock { return s.clock }

// Runs returns the run API of the Engine of the Sim, for AssertTrace and the
// other matchers.
func (s *Sim) Runs() *flowkit.Runs { return s.engine.Runs() }

// Publish saves body as a draft of definition name and publishes it,
// returning the new version.
func (s *Sim) Publish(name string, body json.RawMessage) flowkit.Version {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	defs := s.engine.Definitions()
	id, err := defs.SaveDraft(ctx, name, body)
	if err != nil {
		s.t.Fatalf("flowkittest: save draft of %s: %v", name, err)
	}
	v, err := defs.Publish(ctx, id, flowkit.PublishOpts{Actor: "flowkittest"})
	if err != nil {
		s.t.Fatalf("flowkittest: publish %s: %v", name, err)
	}
	return v
}

// Emit emits evt in a transaction of its own and commits it. Any error,
// flowkit.ErrDuplicateEvent included, fails the test. Emit does not route
// the event; Advance does, Advance(0) without moving the clock.
func (s *Sim) Emit(evt flowkit.Event) {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	tx, err := s.store.Begin(ctx)
	if err != nil {
		s.t.Fatalf("flowkittest: emit %s: %v", evt.Type, err)
	}
	if err := s.emit(ctx, tx, evt); err != nil {
		_ = tx.Rollback(ctx)
		s.t.Fatalf("flowkittest: emit %s: %v", evt.Type, err)
	}
	if err := tx.Commit(ctx); err != nil {
		s.t.Fatalf("flowkittest: emit %s: commit: %v", evt.Type, err)
	}
}

// Advance moves the clock forward by d, running every job on the way when
// it becomes due: routes, pumps, wait timeouts, action retries and janitor
// passes. Advance(0) runs what is due now. It fails the test as soon as an
// action runs without a stub, or a stub calls t.Fatal or outlives its
// context.
func (s *Sim) Advance(d time.Duration) {
	s.t.Helper()
	if d < 0 {
		s.t.Fatalf("flowkittest: Advance(%s): the clock only moves forward", d)
	}
	if err := s.jobs.Advance(s.t.Context(), d); err != nil {
		s.t.Fatalf("flowkittest: Advance(%s) at virtual time %s: %v", d, s.clock.Now().Format(time.RFC3339), err)
	}
}
