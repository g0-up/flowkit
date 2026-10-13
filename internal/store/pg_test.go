package store

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/migrate"
	"github.com/g0-up/flowkit/internal/testpg"
)

// t0 is a fixed instant with microsecond precision, as timestamptz stores.
var t0 = time.Date(2026, 10, 12, 9, 30, 0, 123456000, time.UTC)

// newStore migrates a fresh schema and returns a store over it.
func newStore(t *testing.T) (*PgStore, *pgxpool.Pool) {
	t.Helper()
	pool := testpg.Pool(t)
	return migratedStore(t, pool), pool
}

func migratedStore(t *testing.T, pool *pgxpool.Pool) *PgStore {
	t.Helper()
	schema := testpg.Schema(t, pool)
	if err := migrate.Run(context.Background(), pool, schema, migrate.Options{}); err != nil {
		t.Fatalf("migrate %s: %v", schema, err)
	}
	s, err := NewPgStore(pool, schema)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// inTx runs fn in a store transaction and commits it.
func inTx(t *testing.T, s Store, fn func(Tx)) {
	t.Helper()
	ctx := context.Background()
	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	fn(tx)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func ptr[T any](v T) *T { return &v }

func jsonEqual(t *testing.T, field string, got, want json.RawMessage) {
	t.Helper()
	if want == nil || got == nil {
		if (want == nil) != (got == nil) {
			t.Errorf("%s = %s, want %s", field, got, want)
		}
		return
	}
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("%s: %v", field, err)
	}
	if err := json.Unmarshal(want, &w); err != nil {
		t.Fatalf("%s: %v", field, err)
	}
	if !reflect.DeepEqual(g, w) {
		t.Errorf("%s = %s, want %s", field, got, want)
	}
}

func insertEvent(t *testing.T, s Store, tx Tx, typ string) int64 {
	t.Helper()
	id, inserted, err := s.InsertEvent(context.Background(), tx, EventRow{
		Type: typ, Payload: json.RawMessage(`{}`), OccurredAt: t0, InsertedAt: t0,
	})
	if err != nil || !inserted {
		t.Fatalf("insert event: %d %v %v", id, inserted, err)
	}
	return id
}

func newRun(eventID int64) RunRow {
	return RunRow{
		ID:               ids.NewV4(),
		DefName:          "order_flow",
		DefVersion:       1,
		MetamodelVersion: 1,
		LibVersion:       "v0.0.0",
		Definition:       json.RawMessage(`{"steps":[]}`),
		TriggerEventID:   eventID,
		State:            "running",
		Frames:           json.RawMessage(`[]`),
		Facts:            json.RawMessage(`{}`),
		Rev:              1,
		CreatedAt:        t0,
		UpdatedAt:        t0,
	}
}

func TestNewPgStoreValidatesInput(t *testing.T) {
	if _, err := NewPgStore(nil, "Bad-Name"); err == nil {
		t.Fatal("invalid schema name accepted")
	}
	if _, err := NewPgStore(nil, "flowkit"); err == nil {
		t.Fatal("nil pool accepted")
	}
}

func TestEvents(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	inTx(t, s, func(tx Tx) {
		e := EventRow{
			Type:       "order.created",
			DedupKey:   ptr("o-1"),
			Payload:    json.RawMessage(`{"order_id": "o-1", "total": 12}`),
			OccurredAt: t0,
			InsertedAt: t0.Add(time.Second),
		}
		id, inserted, err := s.InsertEvent(ctx, tx, e)
		if err != nil || !inserted {
			t.Fatalf("first insert: %d %v %v", id, inserted, err)
		}
		dup, inserted, err := s.InsertEvent(ctx, tx, e)
		if err != nil || inserted || dup != id {
			t.Fatalf("duplicate insert = %d %v %v, want %d false nil", dup, inserted, err, id)
		}
		other, inserted, err := s.InsertEvent(ctx, tx, EventRow{
			Type: "order.paid", DedupKey: ptr("o-1"), Payload: json.RawMessage(`{}`), OccurredAt: t0, InsertedAt: t0,
		})
		if err != nil || !inserted || other == id {
			t.Fatalf("same key, other type = %d %v %v", other, inserted, err)
		}
		// Events without a dedup key never deduplicate.
		a := insertEvent(t, s, tx, "tick")
		b := insertEvent(t, s, tx, "tick")
		if a == b {
			t.Fatal("events without dedup key share an id")
		}

		got, err := s.GetEvent(ctx, tx, id)
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != id || got.Type != e.Type || *got.DedupKey != "o-1" ||
			!got.OccurredAt.Equal(e.OccurredAt) || !got.InsertedAt.Equal(e.InsertedAt) {
			t.Fatalf("GetEvent = %+v", got)
		}
		jsonEqual(t, "payload", got.Payload, e.Payload)
		if got, err := s.GetEvent(ctx, tx, a); err != nil || got.DedupKey != nil {
			t.Fatalf("GetEvent without key = %+v, %v", got, err)
		}
		if _, err := s.GetEvent(ctx, tx, math.MaxInt64); !errors.Is(err, ErrNotFound) {
			t.Fatalf("GetEvent of missing id: %v", err)
		}
	})
}

func TestDefinitions(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	d := DefinitionRow{
		Name:              "order_flow",
		Enabled:           true,
		ConcurrencyPolicy: "concurrent",
		CreatedAt:         t0,
		UpdatedAt:         t0,
	}
	inTx(t, s, func(tx Tx) {
		ok, err := s.InsertDefinitionIfAbsent(ctx, tx, d)
		if err != nil || !ok {
			t.Fatalf("insert = %v %v", ok, err)
		}
		ok, err = s.InsertDefinitionIfAbsent(ctx, tx, d)
		if err != nil || ok {
			t.Fatalf("second insert = %v %v, want false nil", ok, err)
		}
		got, err := s.GetDefinition(ctx, tx, d.Name)
		if err != nil {
			t.Fatal(err)
		}
		if got.CurrentVersion != nil || got.TriggerEventType != nil || !got.Enabled || !got.CreatedAt.Equal(t0) {
			t.Fatalf("GetDefinition = %+v", got)
		}

		pub := d
		pub.CurrentVersion = ptr(3)
		pub.TriggerEventType = ptr("order.created")
		pub.ConcurrencyPolicy = "dedup_window"
		pub.SerialKeyCEL = ptr("event.order_id")
		pub.DedupWindowSeconds = ptr(60)
		pub.UpdatedAt = t0.Add(time.Minute)
		if err := s.UpdateDefinitionPublished(ctx, tx, pub); err != nil {
			t.Fatal(err)
		}
		locked, err := s.LockDefinition(ctx, tx, d.Name)
		if err != nil {
			t.Fatal(err)
		}
		if *locked.CurrentVersion != 3 || *locked.TriggerEventType != "order.created" ||
			locked.ConcurrencyPolicy != "dedup_window" || *locked.SerialKeyCEL != "event.order_id" ||
			*locked.DedupWindowSeconds != 60 || !locked.UpdatedAt.Equal(pub.UpdatedAt) || !locked.CreatedAt.Equal(t0) {
			t.Fatalf("after publish = %+v", locked)
		}

		if err := s.SetDefinitionEnabled(ctx, tx, d.Name, false, t0.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		got, err = s.GetDefinition(ctx, tx, d.Name)
		if err != nil || got.Enabled || !got.UpdatedAt.Equal(t0.Add(time.Hour)) {
			t.Fatalf("after disable = %+v, %v", got, err)
		}

		missing := d
		missing.Name = "nope"
		missing.CurrentVersion = ptr(1)
		for name, err := range map[string]error{
			"get":     func() error { _, err := s.GetDefinition(ctx, tx, "nope"); return err }(),
			"lock":    func() error { _, err := s.LockDefinition(ctx, tx, "nope"); return err }(),
			"publish": s.UpdateDefinitionPublished(ctx, tx, missing),
			"enable":  s.SetDefinitionEnabled(ctx, tx, "nope", true, t0),
		} {
			if !errors.Is(err, ErrNotFound) {
				t.Errorf("%s of missing definition: %v", name, err)
			}
		}
		if err := s.UpdateDefinitionPublished(ctx, tx, d); err == nil {
			t.Error("publish without current version accepted")
		}
	})
}

func TestDraftsAndVersions(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	inTx(t, s, func(tx Tx) {
		draft := DraftRow{
			ID:               ids.NewV4(),
			Name:             "order_flow",
			Body:             json.RawMessage(`{"name":"order_flow"}`),
			MetamodelVersion: 1,
			CreatedAt:        t0,
		}
		if err := s.InsertDraft(ctx, tx, draft); err != nil {
			t.Fatal(err)
		}
		got, err := s.GetDraft(ctx, tx, draft.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != draft.ID || got.Name != draft.Name || got.MetamodelVersion != 1 || got.CreatedBy != nil || !got.CreatedAt.Equal(t0) {
			t.Fatalf("GetDraft = %+v", got)
		}
		jsonEqual(t, "draft body", got.Body, draft.Body)
		if _, err := s.GetDraft(ctx, tx, ids.NewV4()); !errors.Is(err, ErrNotFound) {
			t.Fatalf("GetDraft of missing id: %v", err)
		}

		if _, err := s.InsertDefinitionIfAbsent(ctx, tx, DefinitionRow{
			Name: "order_flow", Enabled: true, ConcurrencyPolicy: "serial", CreatedAt: t0, UpdatedAt: t0,
		}); err != nil {
			t.Fatal(err)
		}
		v := DefinitionVersionRow{
			Name:             "order_flow",
			Version:          1,
			Body:             json.RawMessage(`{"name":"order_flow","v":1}`),
			MetamodelVersion: 1,
			Checksum:         "sha256:abc",
			PublishedBy:      ptr("alice"),
			Note:             ptr("first"),
			PublishedAt:      t0,
		}
		if err := s.InsertDefinitionVersion(ctx, tx, v); err != nil {
			t.Fatal(err)
		}
		gv, err := s.GetDefinitionVersion(ctx, tx, "order_flow", 1)
		if err != nil {
			t.Fatal(err)
		}
		if gv.Version != 1 || gv.Checksum != v.Checksum || *gv.PublishedBy != "alice" || *gv.Note != "first" || !gv.PublishedAt.Equal(t0) {
			t.Fatalf("GetDefinitionVersion = %+v", gv)
		}
		jsonEqual(t, "version body", gv.Body, v.Body)
		if _, err := s.GetDefinitionVersion(ctx, tx, "order_flow", 2); !errors.Is(err, ErrNotFound) {
			t.Fatalf("GetDefinitionVersion of missing version: %v", err)
		}
		if _, err := s.GetDefinitionVersion(ctx, tx, "order_flow", math.MaxInt32+1); err == nil || errors.Is(err, ErrNotFound) {
			t.Fatalf("out-of-range version: %v", err)
		}
	})
}

func TestComponentRefs(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	send := ComponentRef{Kind: "action", Ref: "send_email"}
	inTx(t, s, func(tx Tx) {
		if err := s.InsertComponentRefs(ctx, tx, "b_flow", 2, []ComponentRef{send, send, {Kind: "event", Ref: "order.created"}}); err != nil {
			t.Fatal(err)
		}
		if err := s.InsertComponentRefs(ctx, tx, "a_flow", 1, []ComponentRef{send}); err != nil {
			t.Fatal(err)
		}
		if err := s.InsertComponentRefs(ctx, tx, "b_flow", 1, []ComponentRef{send}); err != nil {
			t.Fatal(err)
		}
		// Inserting the same refs again is a no-op.
		if err := s.InsertComponentRefs(ctx, tx, "a_flow", 1, []ComponentRef{send}); err != nil {
			t.Fatal(err)
		}
		if err := s.InsertComponentRefs(ctx, tx, "a_flow", 1, nil); err != nil {
			t.Fatal(err)
		}
		users, err := s.ListComponentUsers(ctx, tx, send)
		if err != nil {
			t.Fatal(err)
		}
		want := []DefinitionRef{{"a_flow", 1}, {"b_flow", 1}, {"b_flow", 2}}
		if !reflect.DeepEqual(users, want) {
			t.Fatalf("ListComponentUsers = %v, want %v", users, want)
		}
		none, err := s.ListComponentUsers(ctx, tx, ComponentRef{Kind: "fact", Ref: "unused"})
		if err != nil || len(none) != 0 {
			t.Fatalf("unused component = %v, %v", none, err)
		}
	})

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := s.InsertComponentRefs(ctx, tx, "c_flow", 1, []ComponentRef{{Kind: "widget", Ref: "x"}}); err == nil {
		t.Fatal("unknown component kind accepted")
	}
}

func TestRunsAndSteps(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	var run RunRow
	inTx(t, s, func(tx Tx) {
		run = newRun(insertEvent(t, s, tx, "order.created"))
		run.EntityKind = ptr("order")
		run.EntityID = ptr("o-1")
		run.ScopeExtra = json.RawMessage(`{"region":"eu"}`)
		run.SerialKey = ptr("o-1")
		run.Exclusive = true
		run.WaitStep = ptr("await_payment")
		run.WaitSeq = 2
		run.LookbackPending = true
		if err := s.InsertRun(ctx, tx, run); err != nil {
			t.Fatal(err)
		}
		got, err := s.GetRun(ctx, tx, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		jsonEqual(t, "definition", got.Definition, run.Definition)
		jsonEqual(t, "scope_extra", got.ScopeExtra, run.ScopeExtra)
		jsonEqual(t, "frames", got.Frames, run.Frames)
		jsonEqual(t, "facts", got.Facts, run.Facts)
		got.Definition, got.ScopeExtra, got.Frames, got.Facts = run.Definition, run.ScopeExtra, run.Frames, run.Facts
		if !got.CreatedAt.Equal(run.CreatedAt) || !got.UpdatedAt.Equal(run.UpdatedAt) {
			t.Fatalf("timestamps = %v %v", got.CreatedAt, got.UpdatedAt)
		}
		got.CreatedAt, got.UpdatedAt = run.CreatedAt, run.UpdatedAt
		if !reflect.DeepEqual(got, run) {
			t.Fatalf("GetRun = %+v\nwant     %+v", got, run)
		}
		if _, err := s.GetRun(ctx, tx, ids.NewV4()); !errors.Is(err, ErrNotFound) {
			t.Fatalf("GetRun of missing id: %v", err)
		}

		plain := newRun(run.TriggerEventID)
		if err := s.InsertRun(ctx, tx, plain); err != nil {
			t.Fatal(err)
		}
		if got, err := s.GetRun(ctx, tx, plain.ID); err != nil || got.ScopeExtra != nil || got.EntityKind != nil {
			t.Fatalf("run without optional fields = %+v, %v", got, err)
		}
	})

	inTx(t, s, func(tx Tx) {
		for i, st := range []StepRow{
			{RunID: run.ID, At: t0, StepID: "$start", Kind: "started"},
			{RunID: run.ID, Seq: 99, At: t0.Add(time.Second), StepID: "charge", Kind: "attempt_failed",
				Detail: ptr("timeout"), Snapshot: json.RawMessage(`{"n":1}`), Attempt: ptr(2), Error: ptr("deadline")},
		} {
			seq, err := s.AppendStep(ctx, tx, st)
			if err != nil {
				t.Fatal(err)
			}
			if seq != i+1 {
				t.Fatalf("step %d got seq %d", i, seq)
			}
		}
		next, err := s.NextStepSeq(ctx, tx, run.ID)
		if err != nil || next != 3 {
			t.Fatalf("NextStepSeq = %d, %v; want 3", next, err)
		}
		steps, err := s.ListSteps(ctx, tx, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(steps) != 2 || steps[0].Seq != 1 || steps[1].Seq != 2 {
			t.Fatalf("ListSteps = %+v", steps)
		}
		if st := steps[0]; st.Detail != nil || st.Snapshot != nil || st.Attempt != nil || st.Error != nil || !st.At.Equal(t0) {
			t.Fatalf("first step = %+v", st)
		}
		if st := steps[1]; st.StepID != "charge" || st.Kind != "attempt_failed" || *st.Detail != "timeout" ||
			*st.Attempt != 2 || *st.Error != "deadline" {
			t.Fatalf("second step = %+v", st)
		}
		jsonEqual(t, "snapshot", steps[1].Snapshot, json.RawMessage(`{"n":1}`))

		if _, err := s.NextStepSeq(ctx, tx, ids.NewV4()); !errors.Is(err, ErrNotFound) {
			t.Fatalf("NextStepSeq of missing run: %v", err)
		}
	})

	// Recording steps leaves the optimistic run version alone.
	var rev int64
	var stepSeq int
	err := pool.QueryRow(ctx, migrate.RenderSchema("SELECT rev, step_seq FROM "+migrate.SchemaToken+"runs WHERE id = $1", s.Schema()), run.ID).Scan(&rev, &stepSeq)
	if err != nil {
		t.Fatal(err)
	}
	if rev != run.Rev || stepSeq != 3 {
		t.Fatalf("rev %d step_seq %d, want %d and 3", rev, stepSeq, run.Rev)
	}
}

func TestGetEffect(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	var run RunRow
	inTx(t, s, func(tx Tx) {
		run = newRun(insertEvent(t, s, tx, "order.created"))
		if err := s.InsertRun(ctx, tx, run); err != nil {
			t.Fatal(err)
		}
	})
	token := ids.NewV4()
	_, err := pool.Exec(ctx, migrate.RenderSchema(`INSERT INTO `+migrate.SchemaToken+`executed_effects
		(idem_key, run_id, step_id, action, attempt, claimed_at, claim_token, in_doubt)
		VALUES ($1, $2, 'charge', 'payments.charge', 1, $3, $4, false)`, s.Schema()),
		"k1", run.ID, t0, token)
	if err != nil {
		t.Fatal(err)
	}
	inTx(t, s, func(tx Tx) {
		got, err := s.GetEffect(ctx, tx, "k1")
		if err != nil {
			t.Fatal(err)
		}
		if got.RunID != run.ID || got.StepID != "charge" || got.Action != "payments.charge" || got.Attempt != 1 ||
			!got.ClaimedAt.Equal(t0) || *got.ClaimToken != token || got.DoneAt != nil || got.Result != nil ||
			got.LastError != nil || got.InDoubt {
			t.Fatalf("GetEffect = %+v", got)
		}
		if _, err := s.GetEffect(ctx, tx, "missing"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("GetEffect of missing key: %v", err)
		}
	})
}

func TestDeadLetters(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	d := DeadLetterRow{
		Kind:      "unroutable_event",
		Ref:       "42",
		EventID:   ptr(int64(42)),
		Raw:       json.RawMessage(`{"type":"x"}`),
		Note:      ptr("no definition"),
		CreatedAt: t0,
	}
	var id int64
	inTx(t, s, func(tx Tx) {
		var inserted bool
		var err error
		id, inserted, err = s.InsertDeadLetter(ctx, tx, d)
		if err != nil || !inserted {
			t.Fatalf("insert = %d %v %v", id, inserted, err)
		}
		dup, inserted, err := s.InsertDeadLetter(ctx, tx, d)
		if err != nil || inserted || dup != id {
			t.Fatalf("duplicate open insert = %d %v %v, want %d false nil", dup, inserted, err, id)
		}
		got, err := s.GetDeadLetter(ctx, tx, id)
		if err != nil {
			t.Fatal(err)
		}
		if got.Kind != d.Kind || got.Ref != d.Ref || got.RunID != nil || *got.EventID != 42 || *got.Note != "no definition" ||
			!got.CreatedAt.Equal(t0) || got.ResolvedAt != nil || got.Resolution != nil {
			t.Fatalf("GetDeadLetter = %+v", got)
		}
		jsonEqual(t, "raw", got.Raw, d.Raw)
		if _, err := s.GetDeadLetter(ctx, tx, id+1000); !errors.Is(err, ErrNotFound) {
			t.Fatalf("GetDeadLetter of missing id: %v", err)
		}
	})

	// Once resolved, the same (kind, ref) may be dead-lettered again.
	_, err := pool.Exec(ctx, migrate.RenderSchema(`UPDATE `+migrate.SchemaToken+`dead_letters
		SET resolved_at = $2, resolution = 'skip' WHERE id = $1`, s.Schema()), id, t0)
	if err != nil {
		t.Fatal(err)
	}
	inTx(t, s, func(tx Tx) {
		again, inserted, err := s.InsertDeadLetter(ctx, tx, d)
		if err != nil || !inserted || again == id {
			t.Fatalf("insert after resolve = %d %v %v", again, inserted, err)
		}
	})
}

// beforeQuery is a pgx tracer that runs fn once, on the calling connection's
// goroutine, just before the first query whose SQL contains marker.
type beforeQuery struct {
	marker string
	once   sync.Once
	fn     func()
}

func (b *beforeQuery) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, b.marker) {
		b.once.Do(b.fn)
	}
	return ctx
}

func (b *beforeQuery) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// tracedStore returns a store over the schema of base whose connections run
// tracer, so a test can act between two statements of one store call.
func tracedStore(t *testing.T, base *PgStore, tracer pgx.QueryTracer) *PgStore {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(testpg.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Tracer = tracer
	s, err := NewPgStore(testpg.PoolWithConfig(t, cfg), base.Schema())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestInsertEventRetriesWhenTheDedupHolderVanishes(t *testing.T) {
	base, pool := newStore(t)
	ctx := context.Background()
	e := EventRow{Type: "order.created", DedupKey: ptr("k1"), Payload: json.RawMessage(`{}`), OccurredAt: t0, InsertedAt: t0}
	var first int64
	inTx(t, base, func(tx Tx) {
		var err error
		if first, _, err = base.InsertEvent(ctx, tx, e); err != nil {
			t.Fatal(err)
		}
	})
	// Delete the holder after the conflicting insert, before the lookup.
	s := tracedStore(t, base, &beforeQuery{marker: "GetEventIDByDedupKey", fn: func() {
		if _, err := pool.Exec(ctx, migrate.RenderSchema("DELETE FROM "+migrate.SchemaToken+"domain_events WHERE id = $1", base.Schema()), first); err != nil {
			t.Errorf("delete holder: %v", err)
		}
	}})
	inTx(t, s, func(tx Tx) {
		id, inserted, err := s.InsertEvent(ctx, tx, e)
		if err != nil || !inserted || id == first {
			t.Fatalf("insert after the holder vanished = %d %v %v, want a new event", id, inserted, err)
		}
	})
}

func TestInsertDeadLetterRetriesWhenTheOpenOneIsResolved(t *testing.T) {
	base, pool := newStore(t)
	ctx := context.Background()
	d := DeadLetterRow{Kind: "exhausted", Ref: "r1", Raw: json.RawMessage(`{}`), CreatedAt: t0}
	var first int64
	inTx(t, base, func(tx Tx) {
		var err error
		if first, _, err = base.InsertDeadLetter(ctx, tx, d); err != nil {
			t.Fatal(err)
		}
	})
	// Resolve the open dead letter after the conflicting insert, before the
	// lookup.
	s := tracedStore(t, base, &beforeQuery{marker: "GetOpenDeadLetterID", fn: func() {
		_, err := pool.Exec(ctx, migrate.RenderSchema(`UPDATE `+migrate.SchemaToken+`dead_letters
			SET resolved_at = $2, resolution = 'skip' WHERE id = $1`, base.Schema()), first, t0)
		if err != nil {
			t.Errorf("resolve open dead letter: %v", err)
		}
	}})
	inTx(t, s, func(tx Tx) {
		id, inserted, err := s.InsertDeadLetter(ctx, tx, d)
		if err != nil || !inserted || id == first {
			t.Fatalf("insert after the open dead letter was resolved = %d %v %v, want a new dead letter", id, inserted, err)
		}
	})
}

func TestBorrowedTx(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	ptx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ptx.Rollback(ctx) }()

	tx := s.WrapTx(ptx)
	if got, err := s.PgxTx(tx); err != nil || got != ptx {
		t.Fatal("PgxTx does not return the wrapped transaction")
	}
	id := insertEvent(t, s, tx, "order.created")
	if err := tx.Commit(ctx); !errors.Is(err, ErrBorrowedTx) {
		t.Fatalf("Commit of borrowed tx: %v", err)
	}
	if err := tx.Rollback(ctx); !errors.Is(err, ErrBorrowedTx) {
		t.Fatalf("Rollback of borrowed tx: %v", err)
	}
	// The caller's transaction is still open and sees the row.
	if _, err := s.GetEvent(ctx, tx, id); err != nil {
		t.Fatal(err)
	}
	if err := ptx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	inTx(t, s, func(tx Tx) {
		if _, err := s.GetEvent(ctx, tx, id); !errors.Is(err, ErrNotFound) {
			t.Fatalf("event survived the caller's rollback: %v", err)
		}
	})
}

type foreignTx struct{}

func (foreignTx) Commit(context.Context) error   { return nil }
func (foreignTx) Rollback(context.Context) error { return nil }

func TestForeignTxIsRejected(t *testing.T) {
	s, err := NewPgStore(&pgxpool.Pool{}, "flowkit")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := s.PgxTx(foreignTx{}); !errors.Is(err, ErrForeignTx) {
		t.Fatalf("PgxTx of a foreign Tx: %v", err)
	}
	if _, err := s.PgxTx(nil); !errors.Is(err, ErrForeignTx) {
		t.Fatalf("PgxTx of nil: %v", err)
	}
	if _, _, err := s.InsertEvent(ctx, foreignTx{}, EventRow{}); !errors.Is(err, ErrForeignTx) {
		t.Fatalf("InsertEvent: %v", err)
	}
	if _, err := s.GetRun(ctx, nil, ids.UUID{}); !errors.Is(err, ErrForeignTx) {
		t.Fatalf("GetRun with nil tx: %v", err)
	}
}

func TestTxOfAnotherStoreIsRejected(t *testing.T) {
	a, pool := newStore(t)
	b := migratedStore(t, pool)
	ctx := context.Background()
	ptx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ptx.Rollback(ctx) }()
	e := EventRow{Type: "order.created", Payload: json.RawMessage(`{}`), OccurredAt: t0, InsertedAt: t0}
	if _, _, err := b.InsertEvent(ctx, a.WrapTx(ptx), e); !errors.Is(err, ErrForeignTx) {
		t.Fatalf("InsertEvent with a wrapped Tx of another store: %v", err)
	}
	if _, err := b.PgxTx(a.WrapTx(ptx)); !errors.Is(err, ErrForeignTx) {
		t.Fatalf("PgxTx with a wrapped Tx of another store: %v", err)
	}
	inTx(t, a, func(tx Tx) {
		if _, _, err := b.InsertEvent(ctx, tx, e); !errors.Is(err, ErrForeignTx) {
			t.Fatalf("InsertEvent with a Tx begun by another store: %v", err)
		}
	})
}

func TestInt4Conversion(t *testing.T) {
	if n, err := toInt32("v", math.MaxInt32); err != nil || n != math.MaxInt32 {
		t.Fatalf("toInt32(MaxInt32) = %d, %v", n, err)
	}
	if _, err := toInt32("v", math.MaxInt32+1); err == nil {
		t.Fatal("toInt32 accepted a value above int4")
	}
	if _, err := toInt32Ptr("v", ptr(math.MinInt32-1)); err == nil {
		t.Fatal("toInt32Ptr accepted a value below int4")
	}
	if n, err := toInt32Ptr("v", nil); n != nil || err != nil {
		t.Fatalf("toInt32Ptr(nil) = %v, %v", n, err)
	}
	if intPtr(nil) != nil || *intPtr(ptr(int32(7))) != 7 {
		t.Fatal("intPtr")
	}
}

// TestStoresOnTwoSchemasAreIndependent runs the same queries through two
// stores on one pool concurrently; each sees only its own schema.
func TestStoresOnTwoSchemasAreIndependent(t *testing.T) {
	pool := testpg.Pool(t)
	stores := []*PgStore{migratedStore(t, pool), migratedStore(t, pool)}
	ctx := context.Background()
	const perStore = 20
	var wg sync.WaitGroup
	for i, s := range stores {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range perStore + i {
				tx, err := s.Begin(ctx)
				if err != nil {
					t.Error(err)
					return
				}
				_, _, err = s.InsertEvent(ctx, tx, EventRow{
					Type: "tick", Payload: json.RawMessage(`{}`), OccurredAt: t0.Add(time.Duration(n)), InsertedAt: t0,
				})
				if err == nil {
					err = tx.Commit(ctx)
				}
				if err != nil {
					_ = tx.Rollback(ctx)
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	for i, s := range stores {
		var n int
		err := pool.QueryRow(ctx, migrate.RenderSchema("SELECT count(*) FROM "+migrate.SchemaToken+"domain_events", s.Schema())).Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		if n != perStore+i {
			t.Errorf("schema %s holds %d events, want %d", s.Schema(), n, perStore+i)
		}
	}
}
