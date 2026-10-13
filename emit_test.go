package flowkit_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/g0-up/flowkit"
	"github.com/g0-up/flowkit/flowkittest"
	"github.com/g0-up/flowkit/internal/testpg"
	"github.com/g0-up/flowkit/migrate"
)

var t0 = time.Date(2026, 10, 12, 9, 30, 0, 0, time.UTC)

func testLogger(t testing.TB) *slog.Logger {
	return slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// migratedSchema returns a pool and a fresh schema migrated for flowkit.
func migratedSchema(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	pool, schema := testpg.New(t)
	if err := migrateOn(pool, schema); err != nil {
		t.Fatal(err)
	}
	return pool, schema
}

func migrateOn(pool *pgxpool.Pool, schema string) error {
	return migrate.Run(context.Background(), pool, schema, migrate.Options{})
}

// newEngine builds an Engine on a fresh migrated schema, closed when the
// test ends, before the schema is dropped.
func newEngine(t *testing.T, reg *flowkit.Registry, mutate func(*flowkit.Config)) (*flowkit.Engine, *pgxpool.Pool, string, *flowkittest.VirtualClock) {
	t.Helper()
	pool, schema := migratedSchema(t)
	clock := flowkittest.NewVirtualClock(t0)
	cfg := flowkit.Config{Pool: pool, Schema: schema, Clock: clock, Logger: testLogger(t), InstanceID: "test"}
	if mutate != nil {
		mutate(&cfg)
	}
	e, err := flowkit.New(cfg, reg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := e.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return e, pool, schema, clock
}

func emitRegistry() *flowkit.Registry {
	r := flowkit.NewRegistry()
	r.MustAddEvent(flowkit.EventDef{
		Type: "order.paid",
		PayloadSchema: json.RawMessage(`{
			"type": "object",
			"required": ["order_id", "amount"],
			"properties": {
				"order_id": {"type": "string"},
				"amount": {"type": "integer", "minimum": 0}
			}
		}`),
		DedupKeyField: "order_id",
	})
	r.MustAddEvent(flowkit.EventDef{
		Type:          "invoice.issued",
		PayloadSchema: json.RawMessage(`{"type": "object", "properties": {"number": {"type": "integer"}}}`),
		DedupKeyField: "number",
	})
	r.MustAddEvent(flowkit.EventDef{
		Type:          "note.added",
		PayloadSchema: json.RawMessage(`{"type": "object"}`),
	})
	r.MustAddEvent(flowkit.EventDef{
		Type: "student.enrolled",
		PayloadSchema: json.RawMessage(`{
			"type": "object",
			"properties": {"student_id": {"type": "string"}, "school_id": {"type": "string"}}
		}`),
		EntityKind:    "student",
		EntityIDField: "student_id",
		ScopeFields:   map[string]string{"school": "school_id"},
	})
	return r
}

func table(schema, name string) string { return pgx.Identifier{schema, name}.Sanitize() }

type storedEvent struct {
	ID         int64
	Type       string
	DedupKey   *string
	Payload    string
	OccurredAt time.Time
	InsertedAt time.Time
}

func storedEvents(t *testing.T, pool *pgxpool.Pool, schema string) []storedEvent {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		"SELECT id, type, dedup_key, payload::text, occurred_at, inserted_at FROM "+table(schema, "domain_events")+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	evs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[storedEvent])
	if err != nil {
		t.Fatal(err)
	}
	return evs
}

type storedJob struct {
	Kind    string
	Queue   string
	State   string
	EventID int64
}

func storedJobs(t *testing.T, pool *pgxpool.Pool, schema string) []storedJob {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		"SELECT kind, queue, state::text, coalesce((args->>'event_id')::bigint, 0) FROM "+table(schema, "river_job")+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[storedJob])
	if err != nil {
		t.Fatal(err)
	}
	return jobs
}

// businessTable creates a table standing in for the caller's own data, in a
// schema of its own.
func businessTable(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	name := table(testpg.Schema(t, pool), "notes")
	if _, err := pool.Exec(context.Background(), "CREATE TABLE "+name+" (body text NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	return name
}

func countRows(t *testing.T, pool *pgxpool.Pool, name string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM "+name).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// emitCommitted emits evt in its own transaction and commits it.
func emitCommitted(t *testing.T, e *flowkit.Engine, pool *pgxpool.Pool, evt flowkit.Event) error {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	emitErr := e.Emit(ctx, tx, evt)
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit after Emit returned %v: %v", emitErr, err)
	}
	return emitErr
}

func TestEmitRollbackLeavesNothing(t *testing.T) {
	e, pool, schema, _ := newEngine(t, emitRegistry(), nil)
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Emit(ctx, tx, flowkit.Event{Type: "order.paid", Payload: map[string]any{"order_id": "o-1", "amount": 5}}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if evs, jobs := storedEvents(t, pool, schema), storedJobs(t, pool, schema); len(evs) != 0 || len(jobs) != 0 {
		t.Fatalf("after rollback: events %+v, jobs %+v", evs, jobs)
	}
}

func TestEmitCommitStoresEventAndRouteJob(t *testing.T) {
	e, pool, schema, clock := newEngine(t, emitRegistry(), nil)
	if err := emitCommitted(t, e, pool, flowkit.Event{Type: "order.paid", Payload: map[string]any{"order_id": "o-1", "amount": 5}}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	happened := t0.Add(-time.Hour)
	if err := emitCommitted(t, e, pool, flowkit.Event{Type: "note.added", Payload: map[string]any{"text": "hi"}, OccurredAt: happened}); err != nil {
		t.Fatal(err)
	}

	evs := storedEvents(t, pool, schema)
	if len(evs) != 2 {
		t.Fatalf("events = %+v, want 2", evs)
	}
	first, second := evs[0], evs[1]
	if first.Type != "order.paid" || first.DedupKey == nil || *first.DedupKey != "o-1" {
		t.Errorf("first event = %+v, want order.paid with dedup key o-1", first)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(first.Payload), &payload); err != nil || payload["order_id"] != "o-1" || payload["amount"] != 5.0 {
		t.Errorf("payload = %s (%v)", first.Payload, err)
	}
	if !first.OccurredAt.Equal(t0) || !first.InsertedAt.Equal(t0) {
		t.Errorf("first event times %s / %s, want both from the flowkit clock %s", first.OccurredAt, first.InsertedAt, t0)
	}
	if second.DedupKey != nil {
		t.Errorf("event without a dedup key stored key %q", *second.DedupKey)
	}
	if !second.OccurredAt.Equal(happened) || !second.InsertedAt.Equal(t0.Add(time.Minute)) {
		t.Errorf("second event times %s / %s, want %s / %s", second.OccurredAt, second.InsertedAt, happened, t0.Add(time.Minute))
	}

	jobs := storedJobs(t, pool, schema)
	if len(jobs) != 2 {
		t.Fatalf("jobs = %+v, want 2", jobs)
	}
	for i, j := range jobs {
		if j.Kind != "flowkit.route_event" || j.Queue != "flowkit" || j.State != "available" || j.EventID != evs[i].ID {
			t.Errorf("job %d = %+v, want an available flowkit.route_event on queue flowkit for event %d", i, j, evs[i].ID)
		}
	}
}

func TestEmitDuplicateKeepsTheCallerTransaction(t *testing.T) {
	e, pool, schema, _ := newEngine(t, emitRegistry(), nil)
	notes := businessTable(t, pool)
	evt := flowkit.Event{Type: "order.paid", Payload: map[string]any{"order_id": "o-1", "amount": 5}}
	if err := emitCommitted(t, e, pool, evt); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	evt.Payload["amount"] = 7 // same key, different payload: still a duplicate
	if err := e.Emit(ctx, tx, evt); !errors.Is(err, flowkit.ErrDuplicateEvent) {
		t.Fatalf("second Emit = %v, want ErrDuplicateEvent", err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO "+notes+" (body) VALUES ('paid twice')"); err != nil {
		t.Fatalf("transaction unusable after ErrDuplicateEvent: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit after ErrDuplicateEvent: %v", err)
	}
	if n := countRows(t, pool, notes); n != 1 {
		t.Fatalf("business rows = %d, want 1", n)
	}
	if evs, jobs := storedEvents(t, pool, schema), storedJobs(t, pool, schema); len(evs) != 1 || len(jobs) != 1 {
		t.Fatalf("after duplicate: %d events, %d jobs, want 1 and 1", len(evs), len(jobs))
	}
}

func TestEmitDedupKeyPrecedence(t *testing.T) {
	e, pool, schema, _ := newEngine(t, emitRegistry(), nil)
	steps := []struct {
		name    string
		evt     flowkit.Event
		wantDup bool
	}{
		{"explicit key wins over the field", flowkit.Event{Type: "order.paid", DedupKey: "k-1", Payload: map[string]any{"order_id": "o-1", "amount": 1}}, false},
		{"field value is a different key", flowkit.Event{Type: "order.paid", Payload: map[string]any{"order_id": "o-1", "amount": 1}}, false},
		{"explicit key repeated", flowkit.Event{Type: "order.paid", DedupKey: "k-1", Payload: map[string]any{"order_id": "o-2", "amount": 1}}, true},
		{"explicit key equal to a field key", flowkit.Event{Type: "order.paid", DedupKey: "o-1", Payload: map[string]any{"order_id": "o-3", "amount": 1}}, true},
		{"numeric field", flowkit.Event{Type: "invoice.issued", Payload: map[string]any{"number": int64(9007199254740993)}}, false},
		{"numeric field repeated", flowkit.Event{Type: "invoice.issued", Payload: map[string]any{"number": int64(9007199254740993)}}, true},
		{"same key on another type", flowkit.Event{Type: "invoice.issued", DedupKey: "k-1"}, false},
		{"field absent: no dedup", flowkit.Event{Type: "invoice.issued"}, false},
		{"field absent again: no dedup", flowkit.Event{Type: "invoice.issued"}, false},
		{"no field configured: no dedup", flowkit.Event{Type: "note.added", Payload: map[string]any{"text": "x"}}, false},
		{"no field configured again", flowkit.Event{Type: "note.added", Payload: map[string]any{"text": "x"}}, false},
	}
	for _, s := range steps {
		err := emitCommitted(t, e, pool, s.evt)
		switch {
		case s.wantDup && !errors.Is(err, flowkit.ErrDuplicateEvent):
			t.Fatalf("%s: Emit = %v, want ErrDuplicateEvent", s.name, err)
		case !s.wantDup && err != nil:
			t.Fatalf("%s: Emit = %v", s.name, err)
		}
	}
	var keys []string
	for _, ev := range storedEvents(t, pool, schema) {
		k := "<nil>"
		if ev.DedupKey != nil {
			k = *ev.DedupKey
		}
		keys = append(keys, ev.Type+"="+k)
	}
	want := []string{
		"order.paid=k-1", "order.paid=o-1", "invoice.issued=9007199254740993", "invoice.issued=k-1",
		"invoice.issued=<nil>", "invoice.issued=<nil>", "note.added=<nil>", "note.added=<nil>",
	}
	if strings.Join(keys, " ") != strings.Join(want, " ") {
		t.Fatalf("stored keys\n got %v\nwant %v", keys, want)
	}
}

func TestEmitIntegerDedupKeysAreCanonical(t *testing.T) {
	e, pool, schema, _ := newEngine(t, emitRegistry(), nil)
	steps := []struct {
		number  any
		wantDup bool
	}{
		{json.Number("100"), false},
		{json.Number("1e2"), true},
		{json.Number("1E+2"), true},
		{json.Number("100.0"), true},
		{int64(100), true},
		{json.Number("-0"), false},
		{0, true},
		{json.Number("12345678901234567890123e2"), false},
		{json.Number("1234567890123456789012300"), true},
	}
	for _, s := range steps {
		err := emitCommitted(t, e, pool, flowkit.Event{Type: "invoice.issued", Payload: map[string]any{"number": s.number}})
		switch {
		case s.wantDup && !errors.Is(err, flowkit.ErrDuplicateEvent):
			t.Fatalf("number %v: Emit = %v, want ErrDuplicateEvent", s.number, err)
		case !s.wantDup && err != nil:
			t.Fatalf("number %v: Emit = %v", s.number, err)
		}
	}
	var keys []string
	for _, ev := range storedEvents(t, pool, schema) {
		keys = append(keys, *ev.DedupKey)
	}
	if want := []string{"100", "0", "1234567890123456789012300"}; strings.Join(keys, " ") != strings.Join(want, " ") {
		t.Fatalf("stored keys %v, want %v", keys, want)
	}
}

// pgCode returns the SQLSTATE of err, or "".
func pgCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func TestEmitConcurrentDuplicateUnderRepeatableRead(t *testing.T) {
	e, pool, _, _ := newEngine(t, emitRegistry(), nil)
	ctx := context.Background()
	evt := flowkit.Event{Type: "order.paid", Payload: map[string]any{"order_id": "o-1", "amount": 1}}
	rr := pgx.TxOptions{IsoLevel: pgx.RepeatableRead}

	tx, err := pool.BeginTx(ctx, rr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SELECT 1"); err != nil { // takes the snapshot
		t.Fatal(err)
	}
	if err := emitCommitted(t, e, pool, evt); err != nil {
		t.Fatal(err)
	}
	if err := e.Emit(ctx, tx, evt); pgCode(err) != "40001" {
		t.Fatalf("Emit of a duplicate committed after the snapshot = %v, want a serialization failure", err)
	}
	_ = tx.Rollback(ctx)

	retry, err := pool.BeginTx(ctx, rr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = retry.Rollback(ctx) }()
	if err := e.Emit(ctx, retry, evt); !errors.Is(err, flowkit.ErrDuplicateEvent) {
		t.Fatalf("retried Emit = %v, want ErrDuplicateEvent", err)
	}
	if err := retry.Commit(ctx); err != nil {
		t.Fatalf("commit after ErrDuplicateEvent: %v", err)
	}
}

func TestEmitEnqueueFailureIsNotAValidationError(t *testing.T) {
	e, pool, schema, _ := newEngine(t, emitRegistry(), nil)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, "DROP TABLE "+table(schema, "river_job")+" CASCADE"); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	err = e.Emit(ctx, tx, flowkit.Event{Type: "note.added"})
	var verrs flowkit.ValidationErrors
	if err == nil || errors.As(err, &verrs) || errors.Is(err, flowkit.ErrDuplicateEvent) {
		t.Fatalf("Emit without a job table = %v, want a database error", err)
	}
	// The failed insert aborted the caller's transaction, as Emit documents.
	if _, err := tx.Exec(ctx, "SELECT 1"); pgCode(err) != "25P02" {
		t.Fatalf("statement after the failed Emit: %v, want the transaction aborted", err)
	}
}

// rejectedEmit is an Emit that must fail without touching the transaction.
type rejectedEmit struct {
	name  string
	evt   flowkit.Event
	check func(error) bool // nil: Emit gets a nil transaction
}

func isValidation(paths ...string) func(error) bool {
	return func(err error) bool {
		var verrs flowkit.ValidationErrors
		if !errors.As(err, &verrs) || len(verrs) != len(paths) {
			return false
		}
		for i, v := range verrs {
			if v.Path != paths[i] || v.Msg == "" {
				return false
			}
		}
		return true
	}
}

func nested(depth int) map[string]any {
	m := map[string]any{"leaf": true}
	for range depth - 1 {
		m = map[string]any{"n": m}
	}
	return m
}

func TestEmitRejectsBadInputWithoutTouchingTheTransaction(t *testing.T) {
	e, pool, schema, _ := newEngine(t, emitRegistry(), func(c *flowkit.Config) { c.MaxPayloadBytes = 4096 })
	notes := businessTable(t, pool)
	cases := []rejectedEmit{
		{name: "unknown type", evt: flowkit.Event{Type: "order.shipped"}, check: func(err error) bool { return errors.Is(err, flowkit.ErrUnknownEvent) }},
		{name: "missing required", evt: flowkit.Event{Type: "order.paid", Payload: map[string]any{}}, check: isValidation("/amount", "/order_id")},
		{name: "wrong types", evt: flowkit.Event{Type: "order.paid", Payload: map[string]any{"order_id": 1, "amount": -1}}, check: isValidation("/amount", "/order_id")},
		{name: "oversize payload", evt: flowkit.Event{Type: "note.added", Payload: map[string]any{"text": strings.Repeat("x", 5000)}}, check: isValidation("")},
		{name: "too deep", evt: flowkit.Event{Type: "note.added", Payload: nested(33)}, check: isValidation("")},
		{name: "not encodable", evt: flowkit.Event{Type: "note.added", Payload: map[string]any{"x": math.Inf(1)}}, check: isValidation("")},
		{name: "long dedup key", evt: flowkit.Event{Type: "note.added", DedupKey: strings.Repeat("k", 513)}, check: isValidation("")},
		{name: "long dedup field", evt: flowkit.Event{Type: "order.paid", Payload: map[string]any{"order_id": strings.Repeat("k", 513), "amount": 1}}, check: isValidation("/order_id")},
		{name: "entity id missing", evt: flowkit.Event{Type: "student.enrolled", Payload: map[string]any{"school_id": "s-1"}}, check: isValidation("/student_id")},
		{name: "entity id empty", evt: flowkit.Event{Type: "student.enrolled", Payload: map[string]any{"student_id": ""}}, check: isValidation("/student_id")},
		{name: "entity id null", evt: flowkit.Event{Type: "student.enrolled", Payload: map[string]any{"student_id": nil}}, check: isValidation("/student_id")},
		{name: "NUL in a payload string", evt: flowkit.Event{Type: "note.added", Payload: map[string]any{"text": "a\x00b"}}, check: isValidation("/text")},
		{name: "NUL in a nested key", evt: flowkit.Event{Type: "note.added", Payload: map[string]any{"meta": map[string]any{"k\x00": 1}}}, check: isValidation("/meta/k\x00")},
		{name: "NUL in an array", evt: flowkit.Event{Type: "note.added", Payload: map[string]any{"tags": []any{"ok", "x\x00"}}}, check: isValidation("/tags/1")},
		{name: "invalid UTF-8 in a payload string", evt: flowkit.Event{Type: "note.added", Payload: map[string]any{"text": "bad \xff"}}, check: isValidation("")},
		{name: "invalid UTF-8 in a payload key", evt: flowkit.Event{Type: "note.added", Payload: map[string]any{"\xff": 1}}, check: isValidation("")},
		{name: "invalid UTF-8 in raw JSON", evt: flowkit.Event{Type: "note.added", Payload: map[string]any{"raw": json.RawMessage("\"\xff\"")}}, check: isValidation("")},
		{name: "lone surrogate escape in raw JSON", evt: flowkit.Event{Type: "note.added", Payload: map[string]any{"raw": json.RawMessage(`"\ud800"`)}}, check: isValidation("")},
		{name: "NUL in the dedup key", evt: flowkit.Event{Type: "note.added", DedupKey: "k\x00"}, check: isValidation("")},
		{name: "invalid UTF-8 in the dedup key", evt: flowkit.Event{Type: "note.added", DedupKey: "\xff"}, check: isValidation("")},
		{name: "NUL in the dedup field", evt: flowkit.Event{Type: "order.paid", Payload: map[string]any{"order_id": "o\x00", "amount": 1}}, check: isValidation("/order_id")},
		{name: "NUL in the entity id", evt: flowkit.Event{Type: "student.enrolled", Payload: map[string]any{"student_id": "s\x00"}}, check: isValidation("/student_id")},
		{name: "NUL in a scope value", evt: flowkit.Event{Type: "student.enrolled", Payload: map[string]any{"student_id": "s-1", "school_id": "x\x00"}}, check: isValidation("/school_id")},
		{name: "invalid UTF-8 in the entity id", evt: flowkit.Event{Type: "student.enrolled", Payload: map[string]any{"student_id": "\xff"}}, check: isValidation("")},
		{name: "integer key too large", evt: flowkit.Event{Type: "invoice.issued", Payload: map[string]any{"number": json.Number("1e1000")}}, check: isValidation("/number")},
		{name: "integer key too long", evt: flowkit.Event{Type: "invoice.issued", Payload: map[string]any{"number": json.Number("1e600")}}, check: isValidation("/number")},
		{name: "number past float64", evt: flowkit.Event{Type: "note.added", Payload: map[string]any{"x": json.Number("1e131000")}}, check: isValidation("/x")},
		{name: "number past jsonb", evt: flowkit.Event{Type: "note.added", Payload: map[string]any{"x": json.Number("1e131072")}}, check: isValidation("/x")},
		{name: "number underflowing float64", evt: flowkit.Event{Type: "note.added", Payload: map[string]any{"x": json.Number("1e-1000000000")}}, check: isValidation("/x")},
		{name: "zero with a huge exponent", evt: flowkit.Event{Type: "note.added", Payload: map[string]any{"x": json.Number("0e-999999")}}, check: isValidation("/x")},
		{name: "number out of range in raw JSON", evt: flowkit.Event{Type: "note.added", Payload: map[string]any{"raw": json.RawMessage(`[1, 1e309]`)}}, check: isValidation("/raw/1")},
		{name: "nil transaction", evt: flowkit.Event{Type: "note.added"}},
	}

	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, c := range cases {
		var err error
		if c.check == nil {
			err = e.Emit(ctx, nil, c.evt)
			if err == nil {
				t.Errorf("%s: Emit succeeded", c.name)
			}
			continue
		}
		err = e.Emit(ctx, tx, c.evt)
		if err == nil || !c.check(err) {
			t.Errorf("%s: Emit = %v", c.name, err)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO "+notes+" (body) VALUES ($1)", c.name); err != nil {
			t.Fatalf("%s: transaction unusable afterwards: %v", c.name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if n := countRows(t, pool, notes); n != len(cases)-1 {
		t.Fatalf("business rows = %d, want %d", n, len(cases)-1)
	}
	if evs, jobs := storedEvents(t, pool, schema), storedJobs(t, pool, schema); len(evs) != 0 || len(jobs) != 0 {
		t.Fatalf("rejected emits wrote %d events and %d jobs", len(evs), len(jobs))
	}
}

func TestEmitAcceptsLimitsExactly(t *testing.T) {
	e, pool, schema, _ := newEngine(t, emitRegistry(), nil)
	accepted := []flowkit.Event{
		{Type: "note.added", Payload: nested(32)},
		{Type: "note.added", DedupKey: strings.Repeat("k", 512)},
		{Type: "student.enrolled", Payload: map[string]any{"student_id": "st-1", "school_id": "sc-1"}},
		{Type: "note.added"}, // nil payload is stored as {}
		{Type: "note.added", Payload: map[string]any{
			"big":  json.Number("9007199254740993"),
			"max":  json.Number("1e308"),
			"tiny": json.Number("5e-324"),
			"zero": json.Number("0e1000"),
		}},
	}
	for i, evt := range accepted {
		if err := emitCommitted(t, e, pool, evt); err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
	}
	evs := storedEvents(t, pool, schema)
	if len(evs) != len(accepted) || evs[3].Payload != "{}" {
		t.Fatalf("stored %+v", evs)
	}
}

func TestEmitPayloadAtTheSizeLimit(t *testing.T) {
	const limit = 64
	e, pool, _, _ := newEngine(t, emitRegistry(), func(c *flowkit.Config) { c.MaxPayloadBytes = limit })
	// {"text":"..."} is 11 bytes of framing.
	exact := map[string]any{"text": strings.Repeat("x", limit-11)}
	if err := emitCommitted(t, e, pool, flowkit.Event{Type: "note.added", Payload: exact}); err != nil {
		t.Fatalf("payload of exactly %d bytes: %v", limit, err)
	}
	over := map[string]any{"text": strings.Repeat("x", limit-10)}
	if err := emitCommitted(t, e, pool, flowkit.Event{Type: "note.added", Payload: over}); !isValidation("")(err) {
		t.Fatalf("payload of %d bytes: %v", limit+1, err)
	}
}
