package memstore_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/g0-up/flowkit/internal/executor"
	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/migrate"
	"github.com/g0-up/flowkit/internal/store"
	"github.com/g0-up/flowkit/internal/store/memstore"
	"github.com/g0-up/flowkit/internal/testpg"
)

// The conformance suite runs every case on Postgres and on memstore, so the
// simulator's store is held to the semantics of the real one. The Postgres
// half skips when no test database is configured.

// t0 is a fixed instant with microsecond precision, as timestamptz stores.
var t0 = time.Date(2026, 10, 12, 9, 30, 0, 123456000, time.UTC)

// fullStore is what flowkit needs of a store: the interpreter, janitor and
// inspection methods and the executor's.
type fullStore interface {
	store.Store
	executor.Store
}

type conformanceCase struct {
	name string
	run  func(h *harness)
}

func TestConformance(t *testing.T) {
	t.Run("memstore", func(t *testing.T) {
		runConformance(t, true, func(*testing.T) fullStore { return memstore.New() })
	})
	t.Run("postgres", func(t *testing.T) {
		pool := testpg.Pool(t)
		// Sequential: a parallel migration per case would exhaust the pool.
		runConformance(t, false, func(t *testing.T) fullStore {
			schema := testpg.Schema(t, pool)
			if err := migrate.Run(context.Background(), pool, schema, migrate.Options{}); err != nil {
				t.Fatalf("migrate %s: %v", schema, err)
			}
			s, err := store.NewPgStore(pool, schema)
			if err != nil {
				t.Fatal(err)
			}
			return s
		})
	})
}

func runConformance(t *testing.T, parallel bool, open func(*testing.T) fullStore) {
	for _, c := range conformanceCases {
		t.Run(c.name, func(t *testing.T) {
			if parallel {
				t.Parallel()
			}
			c.run(&harness{T: t, s: open(t), ctx: context.Background()})
		})
	}
}

type harness struct {
	*testing.T
	s    fullStore
	ctx  context.Context
	last store.Tx
}

// tx runs fn in a transaction and commits it.
func (h *harness) tx(fn func(tx store.Tx)) {
	h.Helper()
	tx, err := h.s.Begin(h.ctx)
	if err != nil {
		h.Fatal(err)
	}
	defer func() { _ = tx.Rollback(h.ctx) }()
	fn(tx)
	if err := tx.Commit(h.ctx); err != nil {
		h.Fatalf("commit: %v", err)
	}
}

// begin opens a transaction for a statement expected to fail. It rolls back
// the transaction the previous begin opened, since memstore runs one
// transaction at a time.
func (h *harness) begin() store.Tx {
	h.Helper()
	if h.last != nil {
		_ = h.last.Rollback(h.ctx)
	}
	tx, err := h.s.Begin(h.ctx)
	if err != nil {
		h.Fatal(err)
	}
	h.last = tx
	h.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	return tx
}

func (h *harness) ok(err error) {
	h.Helper()
	if err != nil {
		h.Fatal(err)
	}
}

func (h *harness) event(tx store.Tx, typ string, at time.Time) int64 {
	h.Helper()
	id, inserted, err := h.s.InsertEvent(h.ctx, tx, store.EventRow{
		Type: typ, Payload: json.RawMessage(`{"n":1}`), OccurredAt: at, InsertedAt: at,
	})
	if err != nil || !inserted {
		h.Fatalf("insert event: %d %v %v", id, inserted, err)
	}
	return id
}

// definition inserts an enabled, published definition triggered by
// order.placed.
func (h *harness) definition(tx store.Tx, name string) {
	h.Helper()
	ok, err := h.s.InsertDefinitionIfAbsent(h.ctx, tx, store.DefinitionRow{
		Name: name, Enabled: true, CurrentVersion: ptr(1), TriggerEventType: ptr("order.placed"),
		ConcurrencyPolicy: "serial", CreatedAt: t0, UpdatedAt: t0,
	})
	if err != nil || !ok {
		h.Fatalf("insert definition %s: %v %v", name, ok, err)
	}
}

func newRun(eventID int64) store.RunRow {
	return store.RunRow{
		ID: ids.NewV4(), DefName: "order_flow", DefVersion: 1, MetamodelVersion: 1, LibVersion: "v0.0.0",
		Definition: json.RawMessage(`{"steps":[]}`), TriggerEventID: eventID, State: "running",
		Frames: json.RawMessage(`[]`), Facts: json.RawMessage(`{}`), Rev: 1, CreatedAt: t0, UpdatedAt: t0,
	}
}

func (h *harness) run(tx store.Tx, eventID int64, edit func(*store.RunRow)) store.RunRow {
	h.Helper()
	r := newRun(eventID)
	if edit != nil {
		edit(&r)
	}
	h.ok(h.s.InsertRun(h.ctx, tx, r))
	return r
}

func (h *harness) getRun(tx store.Tx, id ids.UUID) store.RunRow {
	h.Helper()
	r, err := h.s.GetRun(h.ctx, tx, id)
	h.ok(err)
	return r
}

func (h *harness) wantCode(err error, code string) {
	h.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != code {
		h.Fatalf("err = %v, want SQLSTATE %s", err, code)
	}
}

func (h *harness) wantNotFound(err error) {
	h.Helper()
	if !errors.Is(err, store.ErrNotFound) {
		h.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func (h *harness) wantBool(what string, got bool, err error, want bool) {
	h.Helper()
	h.ok(err)
	if got != want {
		h.Fatalf("%s = %v, want %v", what, got, want)
	}
}

func (h *harness) wantEqual(what string, got, want any) {
	h.Helper()
	if !reflect.DeepEqual(got, want) {
		h.Fatalf("%s = %+v, want %+v", what, got, want)
	}
}

// wantIDs compares lists of run ids, treating nil and empty alike.
func (h *harness) wantIDs(what string, got []ids.UUID, err error, want ...ids.UUID) {
	h.Helper()
	h.ok(err)
	if len(got) != len(want) || (len(got) > 0 && !reflect.DeepEqual(got, want)) {
		h.Fatalf("%s = %v, want %v", what, got, want)
	}
}

func jsonEqual(a, b json.RawMessage) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	var x, y any
	return json.Unmarshal(a, &x) == nil && json.Unmarshal(b, &y) == nil && reflect.DeepEqual(x, y)
}

func ptr[T any](v T) *T { return &v }

// sortedIDs returns n fresh ids in ascending order.
func sortedIDs(n int) []ids.UUID {
	out := make([]ids.UUID, n)
	for i := range out {
		out[i] = ids.NewV4()
	}
	slices.SortFunc(out, func(a, b ids.UUID) int { return bytes.Compare(a[:], b[:]) })
	return out
}

var conformanceCases = []conformanceCase{
	{"transaction rollback discards every write", func(h *harness) {
		var evID int64
		var run store.RunRow
		h.tx(func(tx store.Tx) {
			evID = h.event(tx, "order.placed", t0)
			run = h.run(tx, evID, nil)
		})
		tx := h.begin()
		h.event(tx, "order.placed", t0)
		h.definition(tx, "order_flow")
		_, err := h.s.UpdateRun(h.ctx, tx, store.RunUpdate{ID: run.ID, Rev: 1, Frames: json.RawMessage(`[1]`), Facts: json.RawMessage(`{}`), At: t0.Add(time.Second)})
		h.ok(err)
		_, err = h.s.AppendStep(h.ctx, tx, store.StepRow{RunID: run.ID, At: t0, StepID: "a", Kind: "action"})
		h.ok(err)
		h.ok(tx.Rollback(h.ctx))

		h.tx(func(tx store.Tx) {
			_, err := h.s.GetEvent(h.ctx, tx, evID+1)
			h.wantNotFound(err)
			_, err = h.s.GetDefinition(h.ctx, tx, "order_flow")
			h.wantNotFound(err)
			got := h.getRun(tx, run.ID)
			if got.Rev != 1 || got.StepSeq != 0 || !jsonEqual(got.Frames, json.RawMessage(`[]`)) {
				h.Fatalf("run after rollback = rev %d, step_seq %d, frames %s", got.Rev, got.StepSeq, got.Frames)
			}
			steps, err := h.s.ListSteps(h.ctx, tx, run.ID)
			h.ok(err)
			h.wantEqual("steps", len(steps), 0)
		})
	}},
	{"a failed statement aborts the transaction", func(h *harness) {
		tx := h.begin()
		err := h.s.InsertRun(h.ctx, tx, newRun(424242))
		h.wantCode(err, "23503")
		_, err = h.s.GetDefinition(h.ctx, tx, "order_flow")
		h.wantCode(err, "25P02")
		if err := tx.Commit(h.ctx); !errors.Is(err, pgx.ErrTxCommitRollback) {
			h.Fatalf("commit of an aborted transaction = %v, want ErrTxCommitRollback", err)
		}
	}},
	{"a closed transaction refuses statements", func(h *harness) {
		tx := h.begin()
		h.ok(tx.Commit(h.ctx))
		_, err := h.s.GetDefinition(h.ctx, tx, "order_flow")
		if !errors.Is(err, pgx.ErrTxClosed) {
			h.Fatalf("statement after commit = %v, want ErrTxClosed", err)
		}
		if err := tx.Rollback(h.ctx); !errors.Is(err, pgx.ErrTxClosed) {
			h.Fatalf("rollback after commit = %v, want ErrTxClosed", err)
		}
	}},
	{"a transaction of another store is refused", func(h *harness) {
		other := memstore.New()
		tx, err := other.Begin(h.ctx)
		h.ok(err)
		defer func() { _ = tx.Rollback(h.ctx) }()
		_, err = h.s.GetDefinition(h.ctx, tx, "order_flow")
		if !errors.Is(err, store.ErrForeignTx) {
			h.Fatalf("err = %v, want ErrForeignTx", err)
		}
	}},
	{"events dedup on type and dedup key", func(h *harness) {
		h.tx(func(tx store.Tx) {
			row := store.EventRow{Type: "order.placed", DedupKey: ptr("k1"), Payload: json.RawMessage(`{"a": 1}`), OccurredAt: t0, InsertedAt: t0}
			id, inserted, err := h.s.InsertEvent(h.ctx, tx, row)
			h.wantBool("first insert", inserted, err, true)
			again, inserted, err := h.s.InsertEvent(h.ctx, tx, row)
			h.wantBool("duplicate insert", inserted, err, false)
			h.wantEqual("duplicate id", again, id)
			row.Type = "order.paid"
			other, inserted, err := h.s.InsertEvent(h.ctx, tx, row)
			h.wantBool("same key, other type", inserted, err, true)
			if other == id {
				h.Fatal("another type reused the id")
			}
			row.DedupKey = nil
			_, inserted, err = h.s.InsertEvent(h.ctx, tx, row)
			h.wantBool("no key", inserted, err, true)
			_, inserted, err = h.s.InsertEvent(h.ctx, tx, row)
			h.wantBool("no key again", inserted, err, true)

			got, err := h.s.GetEvent(h.ctx, tx, id)
			h.ok(err)
			if got.Type != "order.placed" || *got.DedupKey != "k1" || !jsonEqual(got.Payload, json.RawMessage(`{"a":1}`)) || !got.InsertedAt.Equal(t0) {
				h.Fatalf("event = %+v", got)
			}
			_, err = h.s.GetEvent(h.ctx, tx, id+100)
			h.wantNotFound(err)
		})
		tx := h.begin()
		_, _, err := h.s.InsertEvent(h.ctx, tx, store.EventRow{Type: "x", OccurredAt: t0, InsertedAt: t0})
		h.wantCode(err, "23502")
	}},
	{"definitions", func(h *harness) {
		h.tx(func(tx store.Tx) {
			h.definition(tx, "b_flow")
			ok, err := h.s.InsertDefinitionIfAbsent(h.ctx, tx, store.DefinitionRow{Name: "b_flow", ConcurrencyPolicy: "concurrent", CreatedAt: t0, UpdatedAt: t0})
			h.wantBool("second insert", ok, err, false)
			ok, err = h.s.InsertDefinitionIfAbsent(h.ctx, tx, store.DefinitionRow{Name: "a_flow", ConcurrencyPolicy: "concurrent", CreatedAt: t0, UpdatedAt: t0})
			h.wantBool("unpublished insert", ok, err, true)
			h.definition(tx, "c_flow")
			h.ok(h.s.SetDefinitionEnabled(h.ctx, tx, "c_flow", false, t0.Add(time.Minute)))

			got, err := h.s.ListTriggeredDefinitions(h.ctx, tx, "order.placed")
			h.ok(err)
			if len(got) != 1 || got[0].Name != "b_flow" {
				h.Fatalf("triggered = %+v, want b_flow only", got)
			}
			h.ok(h.s.UpdateDefinitionPublished(h.ctx, tx, store.DefinitionRow{
				Name: "a_flow", CurrentVersion: ptr(2), TriggerEventType: ptr("order.placed"),
				ConcurrencyPolicy: "dedup_window", DedupWindowSeconds: ptr(60), UpdatedAt: t0.Add(time.Hour),
			}))
			d, err := h.s.LockDefinition(h.ctx, tx, "a_flow")
			h.ok(err)
			if *d.CurrentVersion != 2 || d.ConcurrencyPolicy != "dedup_window" || *d.DedupWindowSeconds != 60 || !d.UpdatedAt.Equal(t0.Add(time.Hour)) || d.Enabled {
				h.Fatalf("published definition = %+v", d)
			}
			got, err = h.s.ListTriggeredDefinitions(h.ctx, tx, "order.placed")
			h.ok(err)
			if len(got) != 1 {
				h.Fatalf("a disabled definition is triggered: %+v", got)
			}
			h.ok(h.s.SetDefinitionEnabled(h.ctx, tx, "a_flow", true, t0))
			got, err = h.s.ListTriggeredDefinitions(h.ctx, tx, "order.placed")
			h.ok(err)
			if len(got) != 2 || got[0].Name != "a_flow" || got[1].Name != "b_flow" {
				h.Fatalf("triggered = %+v, want a_flow, b_flow", got)
			}
			if err := h.s.UpdateDefinitionPublished(h.ctx, tx, store.DefinitionRow{Name: "a_flow", ConcurrencyPolicy: "serial"}); err == nil {
				h.Fatal("publishing without a current version succeeded")
			}
			h.wantNotFound(h.s.UpdateDefinitionPublished(h.ctx, tx, store.DefinitionRow{Name: "nope", CurrentVersion: ptr(1), ConcurrencyPolicy: "serial"}))
			h.wantNotFound(h.s.SetDefinitionEnabled(h.ctx, tx, "nope", true, t0))
			_, err = h.s.GetDefinition(h.ctx, tx, "nope")
			h.wantNotFound(err)
		})
		tx := h.begin()
		_, err := h.s.InsertDefinitionIfAbsent(h.ctx, tx, store.DefinitionRow{Name: "bad", ConcurrencyPolicy: "sometimes", CreatedAt: t0, UpdatedAt: t0})
		h.wantCode(err, "23514")
	}},
	{"drafts, versions and component refs", func(h *harness) {
		draftID := ids.NewV4()
		h.tx(func(tx store.Tx) {
			h.ok(h.s.InsertDraft(h.ctx, tx, store.DraftRow{ID: draftID, Name: "order_flow", Body: json.RawMessage(`{"x":1}`), MetamodelVersion: 1, CreatedAt: t0}))
			d, err := h.s.GetDraft(h.ctx, tx, draftID)
			h.ok(err)
			if d.Name != "order_flow" || !jsonEqual(d.Body, json.RawMessage(`{"x":1}`)) || d.CreatedBy != nil {
				h.Fatalf("draft = %+v", d)
			}
			_, err = h.s.GetDraft(h.ctx, tx, ids.NewV4())
			h.wantNotFound(err)

			h.definition(tx, "order_flow")
			v := store.DefinitionVersionRow{Name: "order_flow", Version: 1, Body: json.RawMessage(`{}`), MetamodelVersion: 1, Checksum: "c1", Note: ptr("first"), PublishedAt: t0}
			h.ok(h.s.InsertDefinitionVersion(h.ctx, tx, v))
			got, err := h.s.GetDefinitionVersion(h.ctx, tx, "order_flow", 1)
			h.ok(err)
			if got.Checksum != "c1" || *got.Note != "first" || got.PublishedBy != nil {
				h.Fatalf("version = %+v", got)
			}
			_, err = h.s.GetDefinitionVersion(h.ctx, tx, "order_flow", 2)
			h.wantNotFound(err)

			h.definition(tx, "a_flow")
			h.ok(h.s.InsertComponentRefs(h.ctx, tx, "order_flow", 1, nil))
			h.ok(h.s.InsertComponentRefs(h.ctx, tx, "order_flow", 1, []store.ComponentRef{{Kind: "action", Ref: "send"}, {Kind: "event", Ref: "order.placed"}}))
			h.ok(h.s.InsertComponentRefs(h.ctx, tx, "a_flow", 3, []store.ComponentRef{{Kind: "action", Ref: "send"}}))
			h.ok(h.s.InsertComponentRefs(h.ctx, tx, "order_flow", 2, []store.ComponentRef{{Kind: "action", Ref: "send"}}))
			users, err := h.s.ListComponentUsers(h.ctx, tx, store.ComponentRef{Kind: "action", Ref: "send"})
			h.ok(err)
			h.wantEqual("users", users, []store.DefinitionRef{{Name: "a_flow", Version: 3}, {Name: "order_flow", Version: 1}, {Name: "order_flow", Version: 2}})
			users, err = h.s.ListComponentUsers(h.ctx, tx, store.ComponentRef{Kind: "fact", Ref: "send"})
			h.ok(err)
			h.wantEqual("users of an unused ref", len(users), 0)
		})
		tx := h.begin()
		err := h.s.InsertDefinitionVersion(h.ctx, tx, store.DefinitionVersionRow{Name: "order_flow", Version: 1, Body: json.RawMessage(`{}`), PublishedAt: t0})
		h.wantCode(err, "23505")
		tx = h.begin()
		err = h.s.InsertDefinitionVersion(h.ctx, tx, store.DefinitionVersionRow{Name: "missing", Version: 1, Body: json.RawMessage(`{}`), PublishedAt: t0})
		h.wantCode(err, "23503")
		tx = h.begin()
		err = h.s.InsertComponentRefs(h.ctx, tx, "order_flow", 1, []store.ComponentRef{{Kind: "widget", Ref: "w"}})
		h.wantCode(err, "23514")
	}},
	{"run rev checks", func(h *harness) {
		var run store.RunRow
		h.tx(func(tx store.Tx) {
			run = h.run(tx, h.event(tx, "order.placed", t0), nil)
			u := store.RunUpdate{ID: run.ID, Rev: 1, Frames: json.RawMessage(`[1]`), Facts: json.RawMessage(`{"f":1}`), At: t0.Add(time.Second)}
			ok, err := h.s.UpdateRun(h.ctx, tx, u)
			h.wantBool("update at rev 1", ok, err, true)
			ok, err = h.s.UpdateRun(h.ctx, tx, u)
			h.wantBool("stale update", ok, err, false)
			got := h.getRun(tx, run.ID)
			if got.Rev != 2 || !jsonEqual(got.Facts, json.RawMessage(`{"f":1}`)) || !got.UpdatedAt.Equal(t0.Add(time.Second)) {
				h.Fatalf("run after update = %+v", got)
			}

			_, ok, err = h.s.EnterWait(h.ctx, tx, store.RunUpdate{ID: run.ID, Rev: 1, Frames: json.RawMessage(`[]`), Facts: json.RawMessage(`{}`), At: t0}, "w")
			h.wantBool("stale enter wait", ok, err, false)
			seq, ok, err := h.s.EnterWait(h.ctx, tx, store.RunUpdate{ID: run.ID, Rev: 2, Frames: json.RawMessage(`[2]`), Facts: json.RawMessage(`{}`), At: t0.Add(2 * time.Second)}, "w")
			h.wantBool("enter wait", ok, err, true)
			h.wantEqual("wait seq", seq, 1)
			got = h.getRun(tx, run.ID)
			if got.State != "waiting" || *got.WaitStep != "w" || got.WaitSeq != 1 || !got.LookbackPending || got.Rev != 3 {
				h.Fatalf("waiting run = %+v", got)
			}
			ok, err = h.s.UpdateRun(h.ctx, tx, store.RunUpdate{ID: run.ID, Rev: 3, Frames: json.RawMessage(`[]`), Facts: json.RawMessage(`{}`), At: t0})
			h.wantBool("update of a waiting run", ok, err, false)

			ok, err = h.s.ClearLookback(h.ctx, tx, run.ID, 2)
			h.wantBool("clear look-back of another wait", ok, err, false)
			ok, err = h.s.ClearLookback(h.ctx, tx, run.ID, 1)
			h.wantBool("clear look-back", ok, err, true)
			ok, err = h.s.ClearLookback(h.ctx, tx, run.ID, 1)
			h.wantBool("clear look-back twice", ok, err, false)

			claim := store.ClaimRow{ID: run.ID, Rev: 3, WaitStep: "w", WaitSeq: 1, Frames: json.RawMessage(`[3]`), At: t0.Add(3 * time.Second)}
			for _, bad := range []store.ClaimRow{
				{ID: run.ID, Rev: 2, WaitStep: "w", WaitSeq: 1, Frames: claim.Frames, At: claim.At},
				{ID: run.ID, Rev: 3, WaitStep: "v", WaitSeq: 1, Frames: claim.Frames, At: claim.At},
				{ID: run.ID, Rev: 3, WaitStep: "w", WaitSeq: 2, Frames: claim.Frames, At: claim.At},
			} {
				ok, err = h.s.ClaimRun(h.ctx, tx, bad)
				h.wantBool("mismatched claim", ok, err, false)
			}
			ok, err = h.s.ClaimRun(h.ctx, tx, claim)
			h.wantBool("claim", ok, err, true)
			ok, err = h.s.ClaimRun(h.ctx, tx, claim)
			h.wantBool("second claim", ok, err, false)
			got = h.getRun(tx, run.ID)
			if got.State != "running" || got.WaitStep != nil || got.WaitSeq != 1 || got.Rev != 4 || !jsonEqual(got.Frames, json.RawMessage(`[3]`)) {
				h.Fatalf("claimed run = %+v", got)
			}

			ok, err = h.s.FinishRun(h.ctx, tx, store.FinishRunRow{ID: run.ID, Rev: 3, State: "completed", TerminalName: "done", At: t0})
			h.wantBool("stale finish", ok, err, false)
			ok, err = h.s.FinishRun(h.ctx, tx, store.FinishRunRow{ID: run.ID, Rev: 4, State: "completed", TerminalName: "done", At: t0.Add(4 * time.Second)})
			h.wantBool("finish", ok, err, true)
			got = h.getRun(tx, run.ID)
			if !got.Terminal || got.State != "completed" || *got.TerminalName != "done" || got.Rev != 5 {
				h.Fatalf("finished run = %+v", got)
			}
			ok, err = h.s.FinishRun(h.ctx, tx, store.FinishRunRow{ID: run.ID, Rev: 5, State: "failed", TerminalName: "x", At: t0})
			h.wantBool("finish a terminal run", ok, err, false)
			ok, err = h.s.UpdateRun(h.ctx, tx, store.RunUpdate{ID: ids.NewV4(), Rev: 1, Frames: json.RawMessage(`[]`), Facts: json.RawMessage(`{}`), At: t0})
			h.wantBool("update a missing run", ok, err, false)
		})
		tx := h.begin()
		_, err := h.s.FinishRun(h.ctx, tx, store.FinishRunRow{ID: run.ID, Rev: 1, State: "completed", TerminalName: "x", At: t0})
		h.ok(err)
		r := newRun(1)
		r.State = "dreaming"
		h.wantCode(h.s.InsertRun(h.ctx, tx, r), "23514")
	}},
	{"one active exclusive run per serial key", func(h *harness) {
		h.tx(func(tx store.Tx) {
			ev := h.event(tx, "order.placed", t0)
			exclusive := func(key *string) func(*store.RunRow) {
				return func(r *store.RunRow) { r.SerialKey, r.Exclusive = key, true }
			}
			first := h.run(tx, ev, exclusive(ptr("k")))
			h.run(tx, ev, exclusive(ptr("other")))
			h.run(tx, ev, func(r *store.RunRow) { r.SerialKey = ptr("k") })
			h.run(tx, ev, exclusive(nil))
			h.run(tx, ev, exclusive(nil))
			h.run(tx, ev, func(r *store.RunRow) {
				r.DefName, r.SerialKey, r.Exclusive = "other_flow", ptr("k"), true
			})

			got, err := h.s.LockActiveExclusiveRun(h.ctx, tx, "order_flow", "k")
			h.ok(err)
			h.wantEqual("active exclusive run", got.ID, first.ID)
			_, err = h.s.LockActiveExclusiveRun(h.ctx, tx, "order_flow", "none")
			h.wantNotFound(err)

			ok, err := h.s.FinishRun(h.ctx, tx, store.FinishRunRow{ID: first.ID, Rev: 1, State: "cancelled", TerminalName: "cancelled", At: t0})
			h.wantBool("finish", ok, err, true)
			h.run(tx, ev, exclusive(ptr("k")))
		})
		tx := h.begin()
		h.wantCode(h.s.InsertRun(h.ctx, tx, func() store.RunRow {
			r := newRun(1)
			r.SerialKey, r.Exclusive = ptr("k"), true
			return r
		}()), "23505")
	}},
	{"steps", func(h *harness) {
		h.tx(func(tx store.Tx) {
			run := h.run(tx, h.event(tx, "order.placed", t0), nil)
			seq, err := h.s.AppendStep(h.ctx, tx, store.StepRow{RunID: run.ID, At: t0, StepID: "a", Kind: "action", Detail: ptr("ok"), Snapshot: json.RawMessage(`{"s":1}`), Attempt: ptr(1)})
			h.ok(err)
			h.wantEqual("first seq", seq, 1)
			next, err := h.s.NextStepSeq(h.ctx, tx, run.ID)
			h.ok(err)
			h.wantEqual("reserved seq", next, 2)
			seq, err = h.s.AppendStep(h.ctx, tx, store.StepRow{RunID: run.ID, At: t0.Add(time.Second), StepID: "b", Kind: "skipped", Error: ptr("e")})
			h.ok(err)
			h.wantEqual("seq after a reserved one", seq, 3)
			steps, err := h.s.ListSteps(h.ctx, tx, run.ID)
			h.ok(err)
			if len(steps) != 2 || steps[0].Seq != 1 || steps[1].Seq != 3 || steps[0].StepID != "a" || *steps[0].Attempt != 1 ||
				!jsonEqual(steps[0].Snapshot, json.RawMessage(`{"s":1}`)) || steps[1].Snapshot != nil || *steps[1].Error != "e" {
				h.Fatalf("steps = %+v", steps)
			}
			h.wantEqual("step_seq", h.getRun(tx, run.ID).StepSeq, 3)
			ok, err := h.s.StepRecorded(h.ctx, tx, run.ID, "a", "action", "ok")
			h.wantBool("recorded step", ok, err, true)
			ok, err = h.s.StepRecorded(h.ctx, tx, run.ID, "b", "skipped", "")
			h.wantBool("step without detail", ok, err, false)
			_, err = h.s.NextStepSeq(h.ctx, tx, ids.NewV4())
			h.wantNotFound(err)
		})
	}},
	{"subscriptions page in run id order", func(h *harness) {
		runIDs := sortedIDs(4)
		h.tx(func(tx store.Tx) {
			ev := h.event(tx, "order.placed", t0)
			paid := h.event(tx, "order.paid", t0)
			for i, id := range runIDs {
				h.run(tx, ev, func(r *store.RunRow) { r.ID = id })
				seq, ok, err := h.s.EnterWait(h.ctx, tx, store.RunUpdate{ID: id, Rev: 1, Frames: json.RawMessage(`[]`), Facts: json.RawMessage(`{}`), At: t0}, "w")
				h.wantBool("enter wait", ok, err, true)
				typ := "order.paid"
				if i == 3 {
					typ = "order.shipped"
				}
				h.ok(h.s.InsertSubscription(h.ctx, tx, store.SubscriptionRow{RunID: id, WaitSeq: seq, StepID: "w", EventType: typ, MatchCEL: "true", CreatedAt: t0}))
			}
			// The second run already consumed the event.
			h.ok(h.s.InsertConsumedEvent(h.ctx, tx, store.ConsumedEventRow{RunID: runIDs[1], EventID: paid, StepID: "w", At: t0}))

			page, err := h.s.ListEventSubscriptions(h.ctx, tx, store.SubscriptionQuery{EventType: "order.paid", EventID: paid, Limit: 1})
			h.ok(err)
			if len(page) != 1 || page[0].Sub.RunID != runIDs[0] || page[0].Run.ID != runIDs[0] || page[0].Run.State != "waiting" ||
				page[0].Run.WaitSeq != 1 || *page[0].Run.WaitStep != "w" || page[0].Run.Rev != 2 || page[0].Sub.MatchCEL != "" || page[0].Run.Facts != nil {
				h.Fatalf("first page = %+v", page)
			}
			page, err = h.s.ListEventSubscriptions(h.ctx, tx, store.SubscriptionQuery{EventType: "order.paid", EventID: paid, AfterRunID: runIDs[0], Limit: 10})
			h.ok(err)
			if len(page) != 1 || page[0].Run.ID != runIDs[2] {
				h.Fatalf("second page = %+v, want the third run only", page)
			}

			// A run resumed from the wait no longer matches its subscription.
			ok, err := h.s.ClaimRun(h.ctx, tx, store.ClaimRow{ID: runIDs[2], Rev: 2, WaitStep: "w", WaitSeq: 1, Frames: json.RawMessage(`[]`), At: t0})
			h.wantBool("claim", ok, err, true)
			page, err = h.s.ListEventSubscriptions(h.ctx, tx, store.SubscriptionQuery{EventType: "order.paid", EventID: paid, AfterRunID: runIDs[0], Limit: 10})
			h.ok(err)
			h.wantEqual("page after the claim", len(page), 0)

			h.ok(h.s.DeleteSubscription(h.ctx, tx, runIDs[0], 1))
			h.ok(h.s.DeleteRunSubscriptions(h.ctx, tx, runIDs[3]))
			page, err = h.s.ListEventSubscriptions(h.ctx, tx, store.SubscriptionQuery{EventType: "order.shipped", EventID: paid, Limit: 10})
			h.ok(err)
			h.wantEqual("deleted subscriptions", len(page), 0)
			page, err = h.s.ListEventSubscriptions(h.ctx, tx, store.SubscriptionQuery{EventType: "order.paid", EventID: paid, Limit: 10})
			h.ok(err)
			h.wantEqual("deleted subscriptions", len(page), 0)
		})
		tx := h.begin()
		err := h.s.InsertSubscription(h.ctx, tx, store.SubscriptionRow{RunID: runIDs[1], WaitSeq: 1, StepID: "w", EventType: "x", CreatedAt: t0})
		h.wantCode(err, "23505")
		tx = h.begin()
		err = h.s.InsertSubscription(h.ctx, tx, store.SubscriptionRow{RunID: ids.NewV4(), WaitSeq: 1, StepID: "w", EventType: "x", CreatedAt: t0})
		h.wantCode(err, "23503")
	}},
	{"look-back pages in (inserted_at, id) order", func(h *harness) {
		h.tx(func(tx store.Tx) {
			trigger := h.event(tx, "order.placed", t0)
			run := h.run(tx, trigger, nil)
			a := h.event(tx, "order.paid", t0.Add(2*time.Second))
			b := h.event(tx, "order.paid", t0.Add(time.Second))
			c := h.event(tx, "order.paid", t0.Add(time.Second))
			h.event(tx, "order.shipped", t0.Add(time.Second))
			d := h.event(tx, "order.paid", t0.Add(3*time.Second))
			h.ok(h.s.InsertConsumedEvent(h.ctx, tx, store.ConsumedEventRow{RunID: run.ID, EventID: a, StepID: "w", At: t0}))

			eventIDs := func(rows []store.EventRow) []int64 {
				out := []int64{}
				for _, r := range rows {
					out = append(out, r.ID)
				}
				return out
			}
			page, err := h.s.LookbackPage(h.ctx, tx, store.LookbackQuery{RunID: run.ID, EventType: "order.paid", AfterAt: t0, Limit: 2})
			h.ok(err)
			h.wantEqual("first page", eventIDs(page), []int64{b, c})
			if !jsonEqual(page[0].Payload, json.RawMessage(`{"n":1}`)) || !page[0].InsertedAt.Equal(t0.Add(time.Second)) {
				h.Fatalf("look-back row = %+v", page[0])
			}
			page, err = h.s.LookbackPage(h.ctx, tx, store.LookbackQuery{RunID: run.ID, EventType: "order.paid", AfterAt: page[0].InsertedAt, AfterID: b, Limit: 10})
			h.ok(err)
			h.wantEqual("page after the tie", eventIDs(page), []int64{c, d})
			page, err = h.s.LookbackPage(h.ctx, tx, store.LookbackQuery{RunID: run.ID, EventType: "order.paid", AfterAt: t0.Add(3 * time.Second), AfterID: d, Limit: 10})
			h.ok(err)
			h.wantEqual("last page", len(page), 0)
		})
		tx := h.begin()
		_, err := h.s.LookbackPage(h.ctx, tx, store.LookbackQuery{EventType: "order.paid", Limit: -1})
		h.wantCode(err, "2201W")
	}},
	{"consumed events and the run queue", func(h *harness) {
		h.tx(func(tx store.Tx) {
			ev := h.event(tx, "order.placed", t0)
			run := h.run(tx, ev, nil)
			c := store.ConsumedEventRow{RunID: run.ID, EventID: ev, StepID: "w", At: t0}
			ok, err := h.s.InsertConsumedEventIfAbsent(h.ctx, tx, c)
			h.wantBool("first consume", ok, err, true)
			ok, err = h.s.InsertConsumedEventIfAbsent(h.ctx, tx, c)
			h.wantBool("second consume", ok, err, false)

			e1 := h.event(tx, "order.placed", t0.Add(2*time.Second))
			e2 := h.event(tx, "order.placed", t0.Add(time.Second))
			e3 := h.event(tx, "order.placed", t0.Add(time.Second))
			for _, q := range []store.RunQueueRow{
				{DefName: "order_flow", SerialKey: "k", TriggerEventID: e1, EventAt: t0.Add(2 * time.Second), CreatedAt: t0},
				{DefName: "order_flow", SerialKey: "k", TriggerEventID: e3, EventAt: t0.Add(time.Second), CreatedAt: t0},
				{DefName: "order_flow", SerialKey: "k", TriggerEventID: e2, EventAt: t0.Add(time.Second), CreatedAt: t0},
			} {
				ok, err = h.s.InsertRunQueue(h.ctx, tx, q)
				h.wantBool("queue", ok, err, true)
			}
			ok, err = h.s.InsertRunQueue(h.ctx, tx, store.RunQueueRow{DefName: "order_flow", SerialKey: "k", TriggerEventID: e1, EventAt: t0, CreatedAt: t0})
			h.wantBool("queue twice", ok, err, false)
			ok, err = h.s.QueuedEventExists(h.ctx, tx, "order_flow", e1)
			h.wantBool("queued", ok, err, true)
			ok, err = h.s.QueuedEventExists(h.ctx, tx, "other_flow", e1)
			h.wantBool("queued for another definition", ok, err, false)
			ok, err = h.s.RunQueueNonEmpty(h.ctx, tx, "order_flow", "k")
			h.wantBool("non-empty", ok, err, true)

			var popped []int64
			for {
				head, err := h.s.PeekRunQueue(h.ctx, tx, "order_flow", "k")
				if errors.Is(err, store.ErrNotFound) {
					break
				}
				h.ok(err)
				popped = append(popped, head.TriggerEventID)
				ok, err := h.s.DeleteRunQueue(h.ctx, tx, head)
				h.wantBool("delete head", ok, err, true)
				ok, err = h.s.DeleteRunQueue(h.ctx, tx, head)
				h.wantBool("delete head twice", ok, err, false)
			}
			h.wantEqual("queue order", popped, []int64{e2, e3, e1})
			ok, err = h.s.RunQueueNonEmpty(h.ctx, tx, "order_flow", "k")
			h.wantBool("empty", ok, err, false)
			h.ok(h.s.LockSerial(h.ctx, tx, "order_flow", "k"))
			h.ok(h.s.LockSerial(h.ctx, tx, "order_flow", "k"))
		})
		tx := h.begin()
		err := h.s.InsertConsumedEvent(h.ctx, tx, store.ConsumedEventRow{RunID: ids.NewV4(), EventID: 1, StepID: "w", At: t0})
		h.wantCode(err, "23503")
		tx = h.begin()
		_, err = h.s.InsertRunQueue(h.ctx, tx, store.RunQueueRow{DefName: "order_flow", SerialKey: "k", TriggerEventID: 9999, EventAt: t0, CreatedAt: t0})
		h.wantCode(err, "23503")
	}},
	{"consumed events key on run and event", func(h *harness) {
		var c store.ConsumedEventRow
		h.tx(func(tx store.Tx) {
			ev := h.event(tx, "order.placed", t0)
			c = store.ConsumedEventRow{RunID: h.run(tx, ev, nil).ID, EventID: ev, StepID: "w", At: t0}
			h.ok(h.s.InsertConsumedEvent(h.ctx, tx, c))
		})
		tx := h.begin()
		c.StepID = "other"
		h.wantCode(h.s.InsertConsumedEvent(h.ctx, tx, c), "23505")
	}},
	{"serial key run history", func(h *harness) {
		h.tx(func(tx store.Tx) {
			early := h.event(tx, "order.placed", t0)
			tied := h.event(tx, "order.placed", t0)
			h.run(tx, tied, func(r *store.RunRow) {
				r.SerialKey, r.CreatedAt = ptr("k"), t0.Add(time.Minute)
			})
			ok, err := h.s.NewerRunExists(h.ctx, tx, "order_flow", "k", t0, early)
			h.wantBool("run of a later event with the same time", ok, err, true)
			ok, err = h.s.NewerRunExists(h.ctx, tx, "order_flow", "k", t0, tied)
			h.wantBool("run of the same event", ok, err, false)
			ok, err = h.s.NewerRunExists(h.ctx, tx, "order_flow", "other", t0.Add(-time.Hour), 0)
			h.wantBool("another key", ok, err, false)

			ok, err = h.s.RecentRunExists(h.ctx, tx, "order_flow", "k", t0.Add(59*time.Second))
			h.wantBool("recent run", ok, err, true)
			ok, err = h.s.RecentRunExists(h.ctx, tx, "order_flow", "k", t0.Add(time.Minute))
			h.wantBool("run created exactly at since", ok, err, false)
		})
	}},
	{"runs by definition", func(h *harness) {
		h.tx(func(tx store.Tx) {
			ev := h.event(tx, "order.placed", t0)
			a := h.run(tx, ev, nil)
			b := h.run(tx, ev, func(r *store.RunRow) { r.DefVersion = 2 })
			h.run(tx, ev, func(r *store.RunRow) { r.DefVersion = 2; r.State = "waiting" })
			h.run(tx, ev, func(r *store.RunRow) { r.State, r.Terminal = "completed", true })
			h.run(tx, ev, func(r *store.RunRow) { r.DefName = "other_flow" })
			got, err := h.s.ListRunningRunIDs(h.ctx, tx, "order_flow")
			want := []ids.UUID{a.ID, b.ID}
			slices.SortFunc(want, func(x, y ids.UUID) int { return bytes.Compare(x[:], y[:]) })
			slices.SortFunc(got, func(x, y ids.UUID) int { return bytes.Compare(x[:], y[:]) })
			h.wantIDs("running runs", got, err, want...)
			pinned, err := h.s.CountPinnedRuns(h.ctx, tx, "order_flow")
			h.ok(err)
			h.wantEqual("pinned", pinned, map[int]int{1: 1, 2: 2})
			ok, err := h.s.RunExists(h.ctx, tx, a.ID)
			h.wantBool("run exists", ok, err, true)
			ok, err = h.s.RunExists(h.ctx, tx, ids.NewV4())
			h.wantBool("missing run exists", ok, err, false)
			_, err = h.s.GetRun(h.ctx, tx, ids.NewV4())
			h.wantNotFound(err)
		})
	}},
	{"effect lifecycle", func(h *harness) {
		tok, other := ids.NewV4(), ids.NewV4()
		h.tx(func(tx store.Tx) {
			run := h.run(tx, h.event(tx, "order.placed", t0), nil)
			claim := store.EffectClaim{IdemKey: "k1", RunID: run.ID, StepID: "s", Action: "send", Token: tok, At: t0}
			ok, err := h.s.InsertEffectClaim(h.ctx, tx, claim)
			h.wantBool("claim", ok, err, true)
			ok, err = h.s.InsertEffectClaim(h.ctx, tx, claim)
			h.wantBool("claim twice", ok, err, false)
			e, err := h.s.GetEffect(h.ctx, tx, "k1")
			h.ok(err)
			if e.Attempt != 0 || *e.ClaimToken != tok || !e.ClaimedAt.Equal(t0) || e.DoneAt != nil || e.InDoubt || e.Result != nil {
				h.Fatalf("claimed effect = %+v", e)
			}

			ok, err = h.s.CompleteEffect(h.ctx, tx, "k1", other, t0, json.RawMessage(`{}`), nil)
			h.wantBool("complete with another token", ok, err, false)
			ok, err = h.s.ReclaimEffect(h.ctx, tx, "k1", other, t0, 5)
			h.wantBool("reclaim a claimed effect", ok, err, false)
			attempt, ok, err := h.s.FailEffect(h.ctx, tx, "k1", tok, "boom")
			h.wantBool("fail", ok, err, true)
			h.wantEqual("attempt", attempt, 1)
			_, ok, err = h.s.FailEffect(h.ctx, tx, "k1", tok, "boom")
			h.wantBool("fail a released effect", ok, err, false)
			e, err = h.s.GetEffect(h.ctx, tx, "k1")
			h.ok(err)
			if e.ClaimToken != nil || e.ClaimedAt != nil || *e.LastError != "boom" {
				h.Fatalf("failed effect = %+v", e)
			}
			ok, err = h.s.ReclaimEffect(h.ctx, tx, "k1", other, t0.Add(time.Second), 1)
			h.wantBool("reclaim past max attempts", ok, err, false)
			ok, err = h.s.ReclaimEffect(h.ctx, tx, "k1", other, t0.Add(time.Second), 2)
			h.wantBool("reclaim", ok, err, true)
			ok, err = h.s.ReleaseEffectClaim(h.ctx, tx, "k1", tok)
			h.wantBool("release with the old token", ok, err, false)
			ok, err = h.s.ReleaseEffectClaim(h.ctx, tx, "k1", other)
			h.wantBool("release", ok, err, true)
			ok, err = h.s.ReclaimEffect(h.ctx, tx, "k1", tok, t0.Add(2*time.Second), 2)
			h.wantBool("reclaim after release", ok, err, true)
			ok, err = h.s.CompleteEffect(h.ctx, tx, "k1", tok, t0.Add(3*time.Second), json.RawMessage(`{"id":7}`), ptr("late"))
			h.wantBool("complete", ok, err, true)
			e, err = h.s.GetEffect(h.ctx, tx, "k1")
			h.ok(err)
			if !e.DoneAt.Equal(t0.Add(3*time.Second)) || !jsonEqual(e.Result, json.RawMessage(`{"id":7}`)) || *e.LastError != "late" || e.Attempt != 1 {
				h.Fatalf("completed effect = %+v", e)
			}
			ok, err = h.s.CompleteEffect(h.ctx, tx, "k1", tok, t0, nil, nil)
			h.wantBool("complete twice", ok, err, false)
			_, err = h.s.GetEffect(h.ctx, tx, "missing")
			h.wantNotFound(err)
		})
		tx := h.begin()
		_, err := h.s.InsertEffectClaim(h.ctx, tx, store.EffectClaim{IdemKey: "k2", RunID: ids.NewV4(), StepID: "s", Action: "a", Token: tok, At: t0})
		h.wantCode(err, "23503")
	}},
	{"in-doubt effects", func(h *harness) {
		tok := ids.NewV4()
		h.tx(func(tx store.Tx) {
			run := h.run(tx, h.event(tx, "order.placed", t0), nil)
			for _, k := range []string{"k1", "k2", "k3"} {
				ok, err := h.s.InsertEffectClaim(h.ctx, tx, store.EffectClaim{IdemKey: k, RunID: run.ID, StepID: "s", Action: "send", Token: tok, At: t0})
				h.wantBool("claim", ok, err, true)
				attempt, ok, err := h.s.MarkEffectInDoubt(h.ctx, tx, k, tok, "lost")
				h.wantBool("mark in doubt", ok, err, true)
				h.wantEqual("attempt", attempt, 0)
			}
			_, ok, err := h.s.MarkEffectInDoubt(h.ctx, tx, "k1", tok, "lost")
			h.wantBool("mark in doubt twice", ok, err, false)
			ok, err = h.s.CompleteEffect(h.ctx, tx, "k1", tok, t0, nil, nil)
			h.wantBool("complete an in-doubt effect", ok, err, false)
			ok, err = h.s.ReclaimEffect(h.ctx, tx, "k1", tok, t0, 9)
			h.wantBool("reclaim an in-doubt effect", ok, err, false)
			e, err := h.s.GetEffect(h.ctx, tx, "k1")
			h.ok(err)
			if !e.InDoubt || e.ClaimToken != nil || e.ClaimedAt == nil || *e.LastError != "lost" {
				h.Fatalf("in-doubt effect = %+v", e)
			}

			ok, err = h.s.SetInDoubtEffectResult(h.ctx, tx, "k1", json.RawMessage(`{"late":true}`))
			h.wantBool("keep a late result", ok, err, true)
			ok, err = h.s.SetInDoubtEffectResult(h.ctx, tx, "k1", json.RawMessage(`{"late":2}`))
			h.wantBool("keep a second late result", ok, err, false)
			ok, err = h.s.RedriveInDoubtEffect(h.ctx, tx, "k1")
			h.wantBool("redrive an effect with a late result", ok, err, false)
			ok, err = h.s.MarkInDoubtEffectDone(h.ctx, tx, "k1", t0.Add(time.Minute), json.RawMessage(`{"late":true}`))
			h.wantBool("mark done", ok, err, true)
			ok, err = h.s.MarkInDoubtEffectDone(h.ctx, tx, "k1", t0, nil)
			h.wantBool("mark done twice", ok, err, false)

			ok, err = h.s.RedriveInDoubtEffect(h.ctx, tx, "k2")
			h.wantBool("redrive", ok, err, true)
			e, err = h.s.GetEffect(h.ctx, tx, "k2")
			h.ok(err)
			if e.InDoubt || e.ClaimedAt != nil || e.ClaimToken != nil || e.DoneAt != nil {
				h.Fatalf("redriven effect = %+v", e)
			}
			ok, err = h.s.RedriveInDoubtEffect(h.ctx, tx, "k2")
			h.wantBool("redrive twice", ok, err, false)
			ok, err = h.s.SetInDoubtEffectResult(h.ctx, tx, "k2", json.RawMessage(`{}`))
			h.wantBool("late result of a redriven effect", ok, err, false)

			g, err := h.s.ReadGauges(h.ctx, tx, "default")
			h.ok(err)
			h.wantEqual("in-doubt gauge", g.InDoubtOpen, 1)
		})
	}},
	{"expired claims", func(h *harness) {
		h.tx(func(tx store.Tx) {
			run := h.run(tx, h.event(tx, "order.placed", t0), nil)
			claim := func(k string, at time.Time) ids.UUID {
				tok := ids.NewV4()
				ok, err := h.s.InsertEffectClaim(h.ctx, tx, store.EffectClaim{IdemKey: k, RunID: run.ID, StepID: "s", Action: "a", Token: tok, At: at})
				h.wantBool("claim "+k, ok, err, true)
				return tok
			}
			claim("b", t0)
			claim("a", t0)
			claim("c", t0.Add(-time.Second))
			claim("fresh", t0.Add(time.Minute))
			done := claim("done", t0)
			ok, err := h.s.CompleteEffect(h.ctx, tx, "done", done, t0, nil, nil)
			h.wantBool("complete", ok, err, true)
			doubt := claim("doubt", t0)
			_, ok, err = h.s.MarkEffectInDoubt(h.ctx, tx, "doubt", doubt, "x")
			h.wantBool("in doubt", ok, err, true)
			released := claim("released", t0)
			ok, err = h.s.ReleaseEffectClaim(h.ctx, tx, "released", released)
			h.wantBool("release", ok, err, true)

			got, err := h.s.ListExpiredClaims(h.ctx, tx, t0.Add(time.Second), 3)
			h.ok(err)
			var keys []string
			for _, e := range got {
				keys = append(keys, e.IdemKey)
			}
			h.wantEqual("expired claims", keys, []string{"c", "a", "b"})
			got, err = h.s.ListExpiredClaims(h.ctx, tx, t0, 10)
			h.ok(err)
			if len(got) != 1 || got[0].IdemKey != "c" {
				h.Fatalf("claims before t0 = %+v", got)
			}
		})
	}},
	{"dead letters", func(h *harness) {
		h.tx(func(tx store.Tx) {
			d := store.DeadLetterRow{Kind: "effect_in_doubt", Ref: "k1", Raw: json.RawMessage(`{"r":1}`), CreatedAt: t0}
			id, inserted, err := h.s.InsertDeadLetter(h.ctx, tx, d)
			h.wantBool("insert", inserted, err, true)
			again, inserted, err := h.s.InsertDeadLetter(h.ctx, tx, d)
			h.wantBool("insert an open duplicate", inserted, err, false)
			h.wantEqual("open duplicate id", again, id)
			d2 := d
			d2.Kind = "exhausted"
			other, inserted, err := h.s.InsertDeadLetter(h.ctx, tx, d2)
			h.wantBool("same ref, other kind", inserted, err, true)

			ok, err := h.s.AppendInDoubtEffectNote(h.ctx, tx, "k1", "first")
			h.wantBool("note", ok, err, true)
			ok, err = h.s.AppendInDoubtEffectNote(h.ctx, tx, "k1", "second")
			h.wantBool("second note", ok, err, true)
			ok, err = h.s.AppendInDoubtEffectNote(h.ctx, tx, "missing", "x")
			h.wantBool("note without dead letter", ok, err, false)
			got, err := h.s.GetDeadLetter(h.ctx, tx, id)
			h.ok(err)
			if got.Kind != "effect_in_doubt" || got.Ref != "k1" || *got.Note != "first\nsecond" || got.ResolvedAt != nil || !jsonEqual(got.Raw, d.Raw) {
				h.Fatalf("dead letter = %+v", got)
			}

			claimed, ok, err := h.s.ClaimDeadLetter(h.ctx, tx, store.DeadLetterClaim{ID: id, Resolution: "skip", ResolvedBy: ptr("ops"), Reason: ptr("why"), At: t0.Add(time.Hour)})
			h.wantBool("claim", ok, err, true)
			if !claimed.ResolvedAt.Equal(t0.Add(time.Hour)) || *claimed.Resolution != "skip" || *claimed.ResolvedBy != "ops" || *claimed.ResolveReason != "why" || claimed.ID != id {
				h.Fatalf("claimed dead letter = %+v", claimed)
			}
			_, ok, err = h.s.ClaimDeadLetter(h.ctx, tx, store.DeadLetterClaim{ID: id, Resolution: "skip", At: t0})
			h.wantBool("claim twice", ok, err, false)
			_, ok, err = h.s.ClaimDeadLetter(h.ctx, tx, store.DeadLetterClaim{ID: id + 1000, Resolution: "skip", At: t0})
			h.wantBool("claim a missing dead letter", ok, err, false)
			ok, err = h.s.AppendInDoubtEffectNote(h.ctx, tx, "k1", "third")
			h.wantBool("note on a resolved dead letter", ok, err, false)

			reopened, inserted, err := h.s.InsertDeadLetter(h.ctx, tx, d)
			h.wantBool("insert after resolution", inserted, err, true)
			if reopened == id || reopened == other {
				h.Fatal("a new dead letter reused an id")
			}
			_, err = h.s.GetDeadLetter(h.ctx, tx, id+1000)
			h.wantNotFound(err)

			g, err := h.s.ReadGauges(h.ctx, tx, "default")
			h.ok(err)
			h.wantEqual("open dead letters", g.OpenDeadLetters, map[string]int{"effect_in_doubt": 1, "exhausted": 1})
			h.wantEqual("queue depth", g.QueueDepth, 0)
			h.wantEqual("leader", g.LeaderID, "")
		})
		tx := h.begin()
		_, _, err := h.s.InsertDeadLetter(h.ctx, tx, store.DeadLetterRow{Kind: "mystery", Ref: "x", Raw: json.RawMessage(`{}`), CreatedAt: t0})
		h.wantCode(err, "23514")
		tx = h.begin()
		_, _, err = h.s.InsertDeadLetter(h.ctx, tx, store.DeadLetterRow{Kind: "exhausted", Ref: "x", CreatedAt: t0})
		h.wantCode(err, "23502")
	}},
	{"rate limits", func(h *harness) {
		h.tx(func(tx store.Tx) {
			_, err := h.s.LockRateLimit(h.ctx, tx, "order_flow")
			h.wantNotFound(err)
			h.ok(h.s.UpdateRateLimit(h.ctx, tx, store.RateLimitRow{DefName: "order_flow", Tokens: 1, RefilledAt: t0}))
			h.ok(h.s.InsertRateLimit(h.ctx, tx, store.RateLimitRow{DefName: "order_flow", Tokens: 5, RefilledAt: t0}))
			h.ok(h.s.InsertRateLimit(h.ctx, tx, store.RateLimitRow{DefName: "order_flow", Tokens: 9, RefilledAt: t0.Add(time.Hour)}))
			r, err := h.s.LockRateLimit(h.ctx, tx, "order_flow")
			h.ok(err)
			if r.DefName != "order_flow" || r.Tokens != 5 || !r.RefilledAt.Equal(t0) {
				h.Fatalf("bucket = %+v, want the first insert", r)
			}
			h.ok(h.s.UpdateRateLimit(h.ctx, tx, store.RateLimitRow{DefName: "order_flow", Tokens: 2.5, RefilledAt: t0.Add(time.Second)}))
			r, err = h.s.LockRateLimit(h.ctx, tx, "order_flow")
			h.ok(err)
			if r.Tokens != 2.5 || !r.RefilledAt.Equal(t0.Add(time.Second)) {
				h.Fatalf("updated bucket = %+v", r)
			}
		})
	}},
	{"janitor finds stale look-backs, stuck runs and orphaned queues", func(h *harness) {
		h.tx(func(tx store.Tx) {
			h.definition(tx, "order_flow")
			ev := h.event(tx, "order.placed", t0)
			at := func(d time.Duration) func(*store.RunRow) {
				return func(r *store.RunRow) { r.UpdatedAt = t0.Add(d) }
			}
			old := h.run(tx, ev, at(-time.Hour))
			older := h.run(tx, ev, at(-2*time.Hour))
			h.run(tx, ev, at(time.Hour))
			inDoubt := h.run(tx, ev, at(-time.Hour))
			claimed := h.run(tx, ev, at(-time.Hour))
			staleClaim := h.run(tx, ev, at(-3*time.Hour))
			h.run(tx, ev, func(r *store.RunRow) { r.UpdatedAt, r.State = t0.Add(-time.Hour), "waiting" })
			h.run(tx, ev, func(r *store.RunRow) { r.DefName, r.UpdatedAt = "missing_flow", t0.Add(-time.Hour) })

			tok := ids.NewV4()
			for _, c := range []struct {
				run store.RunRow
				at  time.Time
			}{{inDoubt, t0}, {claimed, t0.Add(-time.Minute)}, {staleClaim, t0.Add(-time.Hour)}} {
				ok, err := h.s.InsertEffectClaim(h.ctx, tx, store.EffectClaim{IdemKey: c.run.ID.String(), RunID: c.run.ID, StepID: "s", Action: "a", Token: tok, At: c.at})
				h.wantBool("claim", ok, err, true)
			}
			_, ok, err := h.s.MarkEffectInDoubt(h.ctx, tx, inDoubt.ID.String(), tok, "x")
			h.wantBool("in doubt", ok, err, true)

			q := store.StuckRunQuery{UpdatedBefore: t0, ClaimedAfter: t0.Add(-time.Minute), PumpKind: "flowkit.pump_run", Limit: 10}
			got, err := h.s.ListStuckRuns(h.ctx, tx, q)
			h.wantIDs("stuck runs", got, err, staleClaim.ID, older.ID, old.ID)
			q.Limit = 1
			got, err = h.s.ListStuckRuns(h.ctx, tx, q)
			h.wantIDs("first stuck run", got, err, staleClaim.ID)
			h.ok(h.s.SetDefinitionEnabled(h.ctx, tx, "order_flow", false, t0))
			got, err = h.s.ListStuckRuns(h.ctx, tx, q)
			h.wantIDs("stuck runs of a disabled definition", got, err)
			h.ok(h.s.SetDefinitionEnabled(h.ctx, tx, "order_flow", true, t0))

			wait := func(d time.Duration) store.RunRow {
				r := h.run(tx, ev, at(d))
				_, ok, err := h.s.EnterWait(h.ctx, tx, store.RunUpdate{ID: r.ID, Rev: 1, Frames: json.RawMessage(`[]`), Facts: json.RawMessage(`{}`), At: t0.Add(d)}, "w")
				h.wantBool("enter wait", ok, err, true)
				return r
			}
			w1, w2, w3 := wait(-time.Minute), wait(-2*time.Minute), wait(-3*time.Minute)
			wait(time.Minute)
			ok, err = h.s.ClearLookback(h.ctx, tx, w3.ID, 1)
			h.wantBool("clear look-back", ok, err, true)
			got, err = h.s.ListStaleLookbacks(h.ctx, tx, t0, 10)
			h.wantIDs("stale look-backs", got, err, w2.ID, w1.ID)
			got, err = h.s.ListStaleLookbacks(h.ctx, tx, t0, 1)
			h.wantIDs("first stale look-back", got, err, w2.ID)

			h.definition(tx, "a_flow")
			ok, err = h.s.InsertDefinitionIfAbsent(h.ctx, tx, store.DefinitionRow{Name: "draft_flow", ConcurrencyPolicy: "serial", CreatedAt: t0, UpdatedAt: t0})
			h.wantBool("unpublished definition", ok, err, true)
			for _, q := range []store.RunQueueRow{
				{DefName: "order_flow", SerialKey: "held"}, {DefName: "order_flow", SerialKey: "free"},
				{DefName: "order_flow", SerialKey: "free"}, {DefName: "a_flow", SerialKey: "z"},
				{DefName: "draft_flow", SerialKey: "k"}, {DefName: "missing_flow", SerialKey: "k"},
			} {
				q.TriggerEventID, q.EventAt, q.CreatedAt = h.event(tx, "order.placed", t0), t0, t0
				ok, err := h.s.InsertRunQueue(h.ctx, tx, q)
				h.wantBool("queue", ok, err, true)
			}
			h.run(tx, ev, func(r *store.RunRow) { r.SerialKey, r.Exclusive, r.State = ptr("held"), true, "waiting" })
			orphans, err := h.s.ListOrphanedQueues(h.ctx, tx, 10)
			h.ok(err)
			h.wantEqual("orphaned queues", orphans, []store.SerialKeyRef{{DefName: "a_flow", SerialKey: "z"}, {DefName: "order_flow", SerialKey: "free"}})
			orphans, err = h.s.ListOrphanedQueues(h.ctx, tx, 1)
			h.ok(err)
			h.wantEqual("first orphaned queue", orphans, []store.SerialKeyRef{{DefName: "a_flow", SerialKey: "z"}})
		})
	}},
	{"retention summarises runs and deletes expired rows", func(h *harness) {
		h.tx(func(tx store.Tx) {
			h.definition(tx, "order_flow")
			ev := h.event(tx, "order.placed", t0.Add(-48*time.Hour))
			finished := func(updated time.Duration) store.RunRow {
				return h.run(tx, ev, func(r *store.RunRow) {
					r.State, r.Terminal, r.TerminalName = "completed", true, ptr("done")
					r.CreatedAt, r.UpdatedAt, r.EntityKind, r.EntityID = t0.Add(-24*time.Hour), t0.Add(updated), ptr("order"), ptr("o1")
				})
			}
			old := finished(-2 * time.Hour)
			older := finished(-3 * time.Hour)
			recent := finished(time.Hour)
			live := h.run(tx, ev, func(r *store.RunRow) { r.UpdatedAt = t0.Add(-5 * time.Hour) })
			_, err := h.s.AppendStep(h.ctx, tx, store.StepRow{RunID: old.ID, At: t0, StepID: "a", Kind: "action"})
			h.ok(err)
			consumed := h.event(tx, "order.paid", t0.Add(-48*time.Hour))
			h.ok(h.s.InsertConsumedEvent(h.ctx, tx, store.ConsumedEventRow{RunID: old.ID, EventID: consumed, StepID: "w", At: t0}))
			h.ok(h.s.InsertSubscription(h.ctx, tx, store.SubscriptionRow{RunID: old.ID, WaitSeq: 1, StepID: "w", EventType: "order.paid", CreatedAt: t0}))
			ok, err := h.s.InsertEffectClaim(h.ctx, tx, store.EffectClaim{IdemKey: "e", RunID: old.ID, StepID: "s", Action: "a", Token: ids.NewV4(), At: t0})
			h.wantBool("claim", ok, err, true)

			n, err := h.s.SummarizeRuns(h.ctx, tx, t0, 1)
			h.ok(err)
			h.wantEqual("first summarised batch", n, 1)
			ok, err = h.s.RunExists(h.ctx, tx, older.ID)
			h.wantBool("oldest run after summarising", ok, err, false)
			n, err = h.s.SummarizeRuns(h.ctx, tx, t0, 10)
			h.ok(err)
			h.wantEqual("second summarised batch", n, 1)
			n, err = h.s.SummarizeRuns(h.ctx, tx, t0, 10)
			h.ok(err)
			h.wantEqual("nothing left to summarise", n, 0)
			for _, id := range []ids.UUID{recent.ID, live.ID} {
				ok, err = h.s.RunExists(h.ctx, tx, id)
				h.wantBool("kept run", ok, err, true)
			}
			steps, err := h.s.ListSteps(h.ctx, tx, old.ID)
			h.ok(err)
			h.wantEqual("steps of a summarised run", len(steps), 0)
			_, err = h.s.GetEffect(h.ctx, tx, "e")
			h.wantNotFound(err)

			rows, err := h.s.ListRuns(h.ctx, tx, store.RunListQuery{States: []string{"completed"}, Limit: 10})
			h.ok(err)
			if len(rows) != 3 {
				h.Fatalf("completed runs = %+v", rows)
			}
			var summary store.RunSummaryRow
			for _, r := range rows {
				if r.RunID == old.ID {
					summary = r
				}
			}
			want := store.RunSummaryRow{
				RunID: old.ID, DefName: "order_flow", DefVersion: 1, State: "completed", TerminalName: ptr("done"),
				StepCount: 1, TriggerEventType: "order.placed", EntityKind: ptr("order"), EntityID: ptr("o1"),
				StartedAt: t0.Add(-24 * time.Hour), EndedAt: ptr(t0.Add(-2 * time.Hour)),
			}
			if !summary.StartedAt.Equal(want.StartedAt) || !summary.EndedAt.Equal(*want.EndedAt) {
				h.Fatalf("summary times = %v, %v", summary.StartedAt, summary.EndedAt)
			}
			summary.StartedAt, summary.EndedAt = want.StartedAt, want.EndedAt
			h.wantEqual("summary", summary, want)

			n, err = h.s.DeleteExpiredSummaries(h.ctx, tx, t0.Add(-2*time.Hour), 10)
			h.ok(err)
			h.wantEqual("expired summaries", n, 1)
			n, err = h.s.DeleteExpiredSummaries(h.ctx, tx, t0, 10)
			h.ok(err)
			h.wantEqual("remaining expired summaries", n, 1)

			// Only the consumed event is free now: the queued event and the
			// runs' trigger stay.
			queued := h.event(tx, "order.placed", t0.Add(-48*time.Hour))
			ok, err = h.s.InsertRunQueue(h.ctx, tx, store.RunQueueRow{DefName: "order_flow", SerialKey: "k", TriggerEventID: queued, EventAt: t0, CreatedAt: t0})
			h.wantBool("queue", ok, err, true)
			h.event(tx, "order.placed", t0)
			n, err = h.s.DeleteExpiredEvents(h.ctx, tx, t0, 10)
			h.ok(err)
			h.wantEqual("expired events", n, 1)
			_, err = h.s.GetEvent(h.ctx, tx, consumed)
			h.wantNotFound(err)

			for i, at := range []time.Duration{-time.Hour, -2 * time.Hour, time.Hour} {
				id, _, err := h.s.InsertDeadLetter(h.ctx, tx, store.DeadLetterRow{Kind: "exhausted", Ref: string(rune('a' + i)), Raw: json.RawMessage(`{}`), CreatedAt: t0})
				h.ok(err)
				_, ok, err := h.s.ClaimDeadLetter(h.ctx, tx, store.DeadLetterClaim{ID: id, Resolution: "skip", At: t0.Add(at)})
				h.wantBool("resolve", ok, err, true)
			}
			_, _, err = h.s.InsertDeadLetter(h.ctx, tx, store.DeadLetterRow{Kind: "exhausted", Ref: "open", Raw: json.RawMessage(`{}`), CreatedAt: t0.Add(-time.Hour)})
			h.ok(err)
			n, err = h.s.DeleteResolvedDeadLetters(h.ctx, tx, t0, 1)
			h.ok(err)
			h.wantEqual("first resolved batch", n, 1)
			n, err = h.s.DeleteResolvedDeadLetters(h.ctx, tx, t0, 10)
			h.ok(err)
			h.wantEqual("second resolved batch", n, 1)
			g, err := h.s.ReadGauges(h.ctx, tx, "default")
			h.ok(err)
			h.wantEqual("open dead letters", g.OpenDeadLetters, map[string]int{"exhausted": 1})
		})
	}},
	{"run list filters and keyset pages", func(h *harness) {
		runIDs := sortedIDs(5)
		h.tx(func(tx store.Tx) {
			h.definition(tx, "order_flow")
			placed := h.event(tx, "order.placed", t0)
			paid := h.event(tx, "order.paid", t0)
			mk := func(i int, edit func(*store.RunRow)) {
				h.run(tx, placed, func(r *store.RunRow) {
					r.ID = runIDs[i]
					edit(r)
				})
			}
			// started: 0 and 1 tie at t0, 2 at t0+1s, 3 at t0+2s, 4 at t0-1s.
			mk(0, func(r *store.RunRow) { r.EntityKind, r.EntityID = ptr("order"), ptr("o1") })
			mk(1, func(r *store.RunRow) {
				r.State, r.Terminal, r.TerminalName, r.UpdatedAt = "failed", true, ptr("boom"), t0.Add(time.Hour)
			})
			mk(2, func(r *store.RunRow) { r.CreatedAt, r.DefName = t0.Add(time.Second), "other_flow" })
			mk(3, func(r *store.RunRow) { r.CreatedAt, r.TriggerEventID, r.State = t0.Add(2*time.Second), paid, "waiting" })
			mk(4, func(r *store.RunRow) {
				r.CreatedAt, r.UpdatedAt, r.State, r.Terminal, r.EntityKind = t0.Add(-time.Second), t0.Add(-time.Second), "completed", true, ptr("order")
			})
			n, err := h.s.SummarizeRuns(h.ctx, tx, t0, 10)
			h.ok(err)
			h.wantEqual("summarised", n, 1)

			list := func(q store.RunListQuery) []ids.UUID {
				h.Helper()
				rows, err := h.s.ListRuns(h.ctx, tx, q)
				h.ok(err)
				out := []ids.UUID{}
				for _, r := range rows {
					out = append(out, r.RunID)
				}
				return out
			}
			all := list(store.RunListQuery{Limit: 10})
			h.wantEqual("all runs", all, []ids.UUID{runIDs[3], runIDs[2], runIDs[1], runIDs[0], runIDs[4]})
			page := list(store.RunListQuery{Limit: 3})
			h.wantEqual("first page", page, all[:3])
			page = list(store.RunListQuery{Limit: 3, AfterAt: ptr(t0), AfterID: ptr(runIDs[1])})
			h.wantEqual("page after a tie", page, []ids.UUID{runIDs[0], runIDs[4]})
			page = list(store.RunListQuery{Limit: 10, AfterAt: ptr(t0)})
			h.wantEqual("page after a time without id", page, []ids.UUID{runIDs[4]})

			h.wantEqual("by definition", list(store.RunListQuery{DefName: ptr("other_flow"), Limit: 10}), []ids.UUID{runIDs[2]})
			h.wantEqual("by states", list(store.RunListQuery{States: []string{"failed", "waiting"}, Limit: 10}), []ids.UUID{runIDs[3], runIDs[1]})
			h.wantEqual("by entity kind", list(store.RunListQuery{EntityKind: ptr("order"), Limit: 10}), []ids.UUID{runIDs[0], runIDs[4]})
			h.wantEqual("by entity", list(store.RunListQuery{EntityKind: ptr("order"), EntityID: ptr("o1"), Limit: 10}), []ids.UUID{runIDs[0]})
			h.wantEqual("by start window", list(store.RunListQuery{StartedFrom: ptr(t0), StartedBefore: ptr(t0.Add(2 * time.Second)), Limit: 10}),
				[]ids.UUID{runIDs[2], runIDs[1], runIDs[0]})
			h.wantEqual("empty page", list(store.RunListQuery{Limit: 0}), []ids.UUID{})

			rows, err := h.s.ListRuns(h.ctx, tx, store.RunListQuery{Limit: 10})
			h.ok(err)
			byID := map[ids.UUID]store.RunSummaryRow{}
			for _, r := range rows {
				byID[r.RunID] = r
			}
			if r := byID[runIDs[3]]; r.TriggerEventType != "order.paid" || r.EndedAt != nil || r.State != "waiting" {
				h.Fatalf("live run row = %+v", r)
			}
			if r := byID[runIDs[1]]; r.EndedAt == nil || !r.EndedAt.Equal(t0.Add(time.Hour)) || *r.TerminalName != "boom" {
				h.Fatalf("terminal live run row = %+v", r)
			}
			if r := byID[runIDs[4]]; r.EndedAt == nil || !r.EndedAt.Equal(t0.Add(-time.Second)) || r.TriggerEventType != "order.placed" {
				h.Fatalf("summarised run row = %+v", r)
			}
		})
		tx := h.begin()
		_, err := h.s.ListRuns(h.ctx, tx, store.RunListQuery{Limit: -1})
		h.wantCode(err, "2201W")
	}},
}
