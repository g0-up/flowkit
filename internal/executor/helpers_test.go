package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/migrate"
	"github.com/g0-up/flowkit/internal/store"
	"github.com/g0-up/flowkit/internal/testpg"
)

var t0 = time.Date(2026, 10, 12, 9, 30, 0, 0, time.UTC)

const testClaimTimeout = 5 * time.Minute

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// errNotPerformed stands in for flowkit.NotPerformed.
var errNotPerformed = errors.New("not performed")

func isNotPerformed(err error) bool { return errors.Is(err, errNotPerformed) }

func redact(err error) string { return "redacted: " + err.Error() }

// countingMeter counts what its Int64Counters add, by instrument name, and
// keeps the attributes of each counter's last addition.
type countingMeter struct {
	noop.Meter
	mu     sync.Mutex
	counts map[string]*atomic.Int64
	attrs  map[string]attribute.Set
}

type countingCounter struct {
	noop.Int64Counter
	m    *countingMeter
	name string
}

func (c countingCounter) Add(_ context.Context, v int64, opts ...metric.AddOption) {
	c.m.counter(c.name).Add(v)
	c.m.mu.Lock()
	defer c.m.mu.Unlock()
	if c.m.attrs == nil {
		c.m.attrs = map[string]attribute.Set{}
	}
	c.m.attrs[c.name] = metric.NewAddConfig(opts).Attributes()
}

func (m *countingMeter) Int64Counter(name string, _ ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	return countingCounter{m: m, name: name}, nil
}

// lastAttr is the value of attribute key on counter name's last addition.
func (m *countingMeter) lastAttr(name string, key attribute.Key) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	set := m.attrs[name]
	v, _ := set.Value(key)
	return v.AsString()
}

func (m *countingMeter) counter(name string) *atomic.Int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.counts == nil {
		m.counts = map[string]*atomic.Int64{}
	}
	if m.counts[name] == nil {
		m.counts[name] = new(atomic.Int64)
	}
	return m.counts[name]
}

func (m *countingMeter) count(name string) int64 { return m.counter(name).Load() }

// env is a migrated schema with one run.
type env struct {
	t      *testing.T
	pool   *pgxpool.Pool
	store  *store.PgStore
	clock  *fakeClock
	meter  *countingMeter
	runID  ids.UUID
	logger *slog.Logger
}

func newEnv(t *testing.T) *env {
	t.Helper()
	pool, schema := testpg.New(t)
	ctx := context.Background()
	if err := migrate.Run(ctx, pool, schema, migrate.Options{}); err != nil {
		t.Fatalf("migrate %s: %v", schema, err)
	}
	st, err := store.NewPgStore(pool, schema)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{
		t: t, pool: pool, store: st, clock: &fakeClock{now: t0}, meter: &countingMeter{},
		logger: slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelError + 1})),
	}
	e.runID = e.newRun()
	return e
}

func (e *env) newRun() ids.UUID {
	e.t.Helper()
	ctx := context.Background()
	id := ids.NewV4()
	e.inTx(func(tx store.Tx) {
		evID, _, err := e.store.InsertEvent(ctx, tx, store.EventRow{
			Type: "order.created", Payload: json.RawMessage(`{}`), OccurredAt: t0, InsertedAt: t0,
		})
		if err != nil {
			e.t.Fatal(err)
		}
		err = e.store.InsertRun(ctx, tx, store.RunRow{
			ID: id, DefName: "order_flow", DefVersion: 1, MetamodelVersion: 1, LibVersion: "v0.0.0",
			Definition: json.RawMessage(`{}`), TriggerEventID: evID, State: "running",
			Frames: json.RawMessage(`[]`), Facts: json.RawMessage(`{}`), Rev: 1, CreatedAt: t0, UpdatedAt: t0,
		})
		if err != nil {
			e.t.Fatal(err)
		}
	})
	return id
}

func (e *env) inTx(fn func(store.Tx)) {
	e.t.Helper()
	ctx := context.Background()
	tx, err := e.store.Begin(ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	fn(tx)
	if err := tx.Commit(ctx); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) options() Options {
	return Options{
		Store:          e.store,
		Clock:          e.clock,
		ClaimTimeout:   testClaimTimeout,
		ActionDeadline: testClaimTimeout * 8 / 10,
		MaxOutputBytes: 1024,
		IsNotPerformed: isNotPerformed,
		RedactError:    redact,
		Rand:           func() float64 { return 0 },
		Logger:         e.logger,
		Meter:          e.meter,
	}
}

func (e *env) executor(mutate ...func(*Options)) *Executor {
	e.t.Helper()
	o := e.options()
	for _, m := range mutate {
		m(&o)
	}
	ex, err := New(o)
	if err != nil {
		e.t.Fatal(err)
	}
	return ex
}

func (e *env) effect(key string) store.EffectRow {
	e.t.Helper()
	var row store.EffectRow
	e.inTx(func(tx store.Tx) {
		var err error
		if row, err = e.store.GetEffect(context.Background(), tx, key); err != nil {
			e.t.Fatalf("get effect %s: %v", key, err)
		}
	})
	return row
}

func (e *env) steps(kind string) []store.StepRow {
	e.t.Helper()
	var out []store.StepRow
	e.inTx(func(tx store.Tx) {
		all, err := e.store.ListSteps(context.Background(), tx, e.runID)
		if err != nil {
			e.t.Fatal(err)
		}
		for _, s := range all {
			if s.Kind == kind {
				out = append(out, s)
			}
		}
	})
	return out
}

type deadLetter struct {
	Kind, Ref, Note string
	RunID           ids.UUID
	Raw             json.RawMessage
}

func (e *env) deadLetters() []deadLetter {
	e.t.Helper()
	rows, err := e.pool.Query(context.Background(),
		"SELECT kind, ref, coalesce(note, ''), run_id, raw FROM "+pgx.Identifier{e.store.Schema(), "dead_letters"}.Sanitize()+" ORDER BY id")
	if err != nil {
		e.t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (deadLetter, error) {
		var d deadLetter
		err := r.Scan(&d.Kind, &d.Ref, &d.Note, &d.RunID, &d.Raw)
		return d, err
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return out
}

// claimAt claims key for a fresh token at at, as an earlier request would.
func (e *env) claimAt(key, action string, at time.Time) ids.UUID {
	e.t.Helper()
	tok := ids.NewV4()
	e.inTx(func(tx store.Tx) {
		ok, err := e.store.InsertEffectClaim(context.Background(), tx, store.EffectClaim{
			IdemKey: key, RunID: e.runID, StepID: "step", Action: action, Token: tok, At: at,
		})
		if err != nil || !ok {
			e.t.Fatalf("claim %s: %v, %v", key, ok, err)
		}
	})
	return tok
}

// countingAction returns an action whose Execute calls fn and counts calls.
func countingAction(name string, idempotent bool, fn func(ctx context.Context, n int64) (map[string]any, error)) (Action, *atomic.Int64) {
	calls := new(atomic.Int64)
	return Action{
		Name:       name,
		Idempotent: idempotent,
		Execute: func(ctx context.Context, _ Input) (map[string]any, error) {
			return fn(ctx, calls.Add(1))
		},
	}, calls
}

func (e *env) request(key string, a Action) ExecRequest {
	return ExecRequest{RunID: e.runID, StepID: "step", IdemKey: key, DefName: "order_flow", Action: a}
}

func wantKind(t *testing.T, got ExecOutcome, want ExecKind) {
	t.Helper()
	if got.Kind != want {
		t.Fatalf("outcome = %s (err %v); want %s", got.Kind, got.Err, want)
	}
}

func fmtKeys(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s-%d", prefix, i)
	}
	return out
}

// hookStore lets a test step in before some store calls.
type hookStore struct {
	*store.PgStore
	// afterInsertClaim runs after a first claim on an effect was inserted.
	afterInsertClaim func()
	// beforeReclaim runs before ReclaimEffect.
	beforeReclaim func(key string)
	// beforeComplete runs before the nth CompleteEffect; a non-nil error is
	// returned instead of completing the effect.
	beforeComplete func(n int64, key string, token ids.UUID) error
	completes      atomic.Int64
}

func (h *hookStore) InsertEffectClaim(ctx context.Context, tx store.Tx, c store.EffectClaim) (bool, error) {
	ok, err := h.PgStore.InsertEffectClaim(ctx, tx, c)
	if ok && err == nil && h.afterInsertClaim != nil {
		h.afterInsertClaim()
	}
	return ok, err
}

func (h *hookStore) ReclaimEffect(ctx context.Context, tx store.Tx, key string, token ids.UUID, at time.Time, maxAttempts int) (bool, error) {
	if h.beforeReclaim != nil {
		h.beforeReclaim(key)
	}
	return h.PgStore.ReclaimEffect(ctx, tx, key, token, at, maxAttempts)
}

func (h *hookStore) CompleteEffect(ctx context.Context, tx store.Tx, key string, token ids.UUID, at time.Time, result json.RawMessage, lastError *string) (bool, error) {
	n := h.completes.Add(1)
	if h.beforeComplete != nil {
		if err := h.beforeComplete(n, key, token); err != nil {
			return false, err
		}
	}
	return h.PgStore.CompleteEffect(ctx, tx, key, token, at, result, lastError)
}

// exec runs sql on the env's schema; {schema} in sql is replaced by the quoted
// schema name.
func (e *env) exec(sql string, args ...any) {
	e.t.Helper()
	sql = strings.ReplaceAll(sql, "{schema}", pgx.Identifier{e.store.Schema()}.Sanitize())
	if _, err := e.pool.Exec(context.Background(), sql, args...); err != nil {
		e.t.Fatal(err)
	}
}
