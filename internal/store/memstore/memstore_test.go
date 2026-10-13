package memstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/store"
	"github.com/g0-up/flowkit/internal/store/memstore"
)

func newHarness(t *testing.T) (*harness, *memstore.Store) {
	s := memstore.New()
	return &harness{T: t, s: s, ctx: context.Background()}, s
}

func TestAfterCommitRunsOnlyOnCommit(t *testing.T) {
	h, s := newHarness(t)
	var ran []string
	tx := h.begin()
	h.ok(s.AfterCommit(tx, func() { ran = append(ran, "rolled back") }))
	h.ok(tx.Rollback(h.ctx))
	h.tx(func(tx store.Tx) {
		h.ok(s.AfterCommit(tx, func() {
			// The store accepts the next transaction before the hooks run.
			next, err := s.Begin(h.ctx)
			if err != nil {
				t.Error(err)
				return
			}
			_ = next.Rollback(h.ctx)
			ran = append(ran, "first")
		}))
		h.ok(s.AfterCommit(tx, func() { ran = append(ran, "second") }))
	})
	h.wantEqual("hooks", ran, []string{"first", "second"})

	if err := s.AfterCommit(tx, func() {}); !errors.Is(err, pgx.ErrTxClosed) {
		t.Fatalf("AfterCommit on a closed transaction = %v", err)
	}
	other, err := memstore.New().Begin(h.ctx)
	h.ok(err)
	if err := s.AfterCommit(other, func() {}); !errors.Is(err, store.ErrForeignTx) {
		t.Fatalf("AfterCommit on a foreign transaction = %v", err)
	}
}

func TestAbortedCommitSkipsHooks(t *testing.T) {
	h, s := newHarness(t)
	tx := h.begin()
	ran := false
	h.ok(s.AfterCommit(tx, func() { ran = true }))
	h.wantCode(s.InsertRun(h.ctx, tx, newRun(1)), "23503")
	if err := tx.Commit(h.ctx); !errors.Is(err, pgx.ErrTxCommitRollback) {
		t.Fatalf("commit = %v", err)
	}
	if ran {
		t.Fatal("a hook of an aborted transaction ran")
	}
}

func TestBeginWaitsForTheOpenTransaction(t *testing.T) {
	h, s := newHarness(t)
	tx := h.begin()
	h.event(tx, "order.placed", t0)

	ctx, cancel := context.WithTimeout(h.ctx, 20*time.Millisecond)
	defer cancel()
	if _, err := s.Begin(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Begin while a transaction is open = %v, want a deadline error", err)
	}

	done := make(chan store.Tx)
	go func() {
		next, err := s.Begin(h.ctx)
		if err != nil {
			t.Error(err)
		}
		done <- next
	}()
	select {
	case <-done:
		t.Fatal("Begin returned while a transaction is open")
	case <-time.After(10 * time.Millisecond):
	}
	h.ok(tx.Commit(h.ctx))
	next := <-done
	// The waiting transaction sees the committed write.
	_, err := s.GetEvent(h.ctx, next, 1)
	h.ok(err)
	h.ok(next.Rollback(h.ctx))
}

func TestCancelledContextAbortsTheTransaction(t *testing.T) {
	h, s := newHarness(t)
	ctx, cancel := context.WithCancel(h.ctx)
	tx, err := s.Begin(ctx)
	h.ok(err)
	h.event(tx, "order.placed", t0)
	cancel()
	if _, err := s.GetEvent(ctx, tx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("statement after cancel = %v", err)
	}
	if err := tx.Commit(h.ctx); !errors.Is(err, pgx.ErrTxCommitRollback) {
		t.Fatalf("commit after cancel = %v", err)
	}
	h.tx(func(tx store.Tx) {
		_, err := s.GetEvent(h.ctx, tx, 1)
		h.wantNotFound(err)
	})
}

func TestRowsAreCopies(t *testing.T) {
	h, s := newHarness(t)
	payload := json.RawMessage(`{"a":1}`)
	h.tx(func(tx store.Tx) {
		_, _, err := s.InsertEvent(h.ctx, tx, store.EventRow{Type: "x", Payload: payload, OccurredAt: t0, InsertedAt: t0})
		h.ok(err)
	})
	payload[2] = 'b'
	h.tx(func(tx store.Tx) {
		e, err := s.GetEvent(h.ctx, tx, 1)
		h.ok(err)
		if string(e.Payload) != `{"a":1}` {
			t.Fatalf("stored payload changed with the caller's slice: %s", e.Payload)
		}
		e.Payload[2] = 'c'
		again, err := s.GetEvent(h.ctx, tx, 1)
		h.ok(err)
		if string(again.Payload) != `{"a":1}` {
			t.Fatalf("stored payload changed with a returned slice: %s", again.Payload)
		}
	})
}

func TestTimestampsHaveMicrosecondPrecision(t *testing.T) {
	h, s := newHarness(t)
	h.tx(func(tx store.Tx) {
		_, _, err := s.InsertEvent(h.ctx, tx, store.EventRow{Type: "x", Payload: json.RawMessage(`{}`), OccurredAt: t0.Add(999), InsertedAt: t0.Add(999)})
		h.ok(err)
		e, err := s.GetEvent(h.ctx, tx, 1)
		h.ok(err)
		if !e.InsertedAt.Equal(t0) {
			t.Fatalf("inserted_at = %v, want %v", e.InsertedAt, t0)
		}
	})
}

type fakeJobs struct {
	live      map[ids.UUID]bool
	available int
}

func (j fakeJobs) LiveJob(kind string, runID ids.UUID) bool {
	return kind == "flowkit.pump_run" && j.live[runID]
}

func (j fakeJobs) Available(queue string) int {
	if queue != "flowkit" {
		return 0
	}
	return j.available
}

func TestJobsFeedStuckRunsAndGauges(t *testing.T) {
	h, s := newHarness(t)
	var pumped, stuck store.RunRow
	h.tx(func(tx store.Tx) {
		h.definition(tx, "order_flow")
		ev := h.event(tx, "order.placed", t0)
		pumped = h.run(tx, ev, nil)
		stuck = h.run(tx, ev, nil)
	})
	s.SetJobs(fakeJobs{live: map[ids.UUID]bool{pumped.ID: true}, available: 3})
	h.tx(func(tx store.Tx) {
		got, err := s.ListStuckRuns(h.ctx, tx, store.StuckRunQuery{UpdatedBefore: t0.Add(time.Second), ClaimedAfter: t0, PumpKind: "flowkit.pump_run", Limit: 10})
		h.wantIDs("stuck runs", got, err, stuck.ID)
		g, err := s.ReadGauges(h.ctx, tx, "flowkit")
		h.ok(err)
		h.wantEqual("queue depth", g.QueueDepth, 3)
	})
}

func TestReadOnlyHasNoDatabase(t *testing.T) {
	s := memstore.New()
	ctx := context.Background()
	if err := s.ReadOnly(ctx, 0, func(context.Context, store.ReadQuerier) error { return nil }); err == nil {
		t.Fatal("a zero timeout was accepted")
	}
	err := s.ReadOnly(ctx, time.Second, func(ctx context.Context, q store.ReadQuerier) error {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("the context has no deadline")
		}
		if _, err := q.Query(ctx, "SELECT 1"); !errors.Is(err, memstore.ErrNoDatabase) {
			t.Errorf("Query = %v", err)
		}
		var n int
		return q.QueryRow(ctx, "SELECT 1").Scan(&n)
	})
	if !errors.Is(err, memstore.ErrNoDatabase) {
		t.Fatalf("ReadOnly = %v", err)
	}
}

func TestLimitOutsideInt4IsRefused(t *testing.T) {
	h, s := newHarness(t)
	h.tx(func(tx store.Tx) {
		if _, err := s.ListStaleLookbacks(h.ctx, tx, t0, 1<<40); err == nil {
			t.Fatal("a limit outside int4 was accepted")
		}
		// Like the Postgres store, the refusal does not abort the transaction.
		_, err := s.ListStaleLookbacks(h.ctx, tx, t0, 1)
		h.ok(err)
	})
}
