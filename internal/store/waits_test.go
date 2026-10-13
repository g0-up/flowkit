package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/g0-up/flowkit/internal/ids"
)

func TestRunWaitLifecycle(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	frames := json.RawMessage(`{"stack":[{"step":"w"}],"visits":{"w":1}}`)
	inTx(t, s, func(tx Tx) {
		run := newRun(insertEvent(t, s, tx, "order.created"))
		if err := s.InsertRun(ctx, tx, run); err != nil {
			t.Fatal(err)
		}
		get := func() RunRow {
			t.Helper()
			r, err := s.GetRun(ctx, tx, run.ID)
			if err != nil {
				t.Fatal(err)
			}
			return r
		}

		update := RunUpdate{ID: run.ID, Rev: 7, Frames: frames, Facts: json.RawMessage(`{"n":1}`), At: t0.Add(time.Second)}
		if ok, err := s.UpdateRun(ctx, tx, update); err != nil || ok {
			t.Fatalf("UpdateRun with a stale rev = %v, %v", ok, err)
		}
		update.Rev = 1
		if ok, err := s.UpdateRun(ctx, tx, update); err != nil || !ok {
			t.Fatalf("UpdateRun = %v, %v", ok, err)
		}
		r := get()
		jsonEqual(t, "facts", r.Facts, update.Facts)
		if r.Rev != 2 || !r.UpdatedAt.Equal(update.At) {
			t.Fatalf("after UpdateRun rev %d updated_at %v", r.Rev, r.UpdatedAt)
		}
		if _, err := s.AppendStep(ctx, tx, StepRow{RunID: run.ID, At: t0, StepID: "a", Kind: "executed"}); err != nil {
			t.Fatal(err)
		}
		if r := get(); r.Rev != 2 {
			t.Fatalf("AppendStep moved rev to %d", r.Rev)
		}

		enter := RunUpdate{ID: run.ID, Rev: 1, Frames: frames, Facts: json.RawMessage(`{}`), At: t0.Add(2 * time.Second)}
		if _, ok, err := s.EnterWait(ctx, tx, enter, "w"); err != nil || ok {
			t.Fatalf("EnterWait with a stale rev = %v, %v", ok, err)
		}
		enter.Rev = 2
		seq, ok, err := s.EnterWait(ctx, tx, enter, "w")
		if err != nil || !ok || seq != 1 {
			t.Fatalf("EnterWait = %d %v %v", seq, ok, err)
		}
		r = get()
		if r.State != "waiting" || *r.WaitStep != "w" || r.WaitSeq != 1 || !r.LookbackPending || r.Rev != 3 {
			t.Fatalf("waiting run = %+v", r)
		}
		enter.Rev = 3
		if _, ok, err := s.EnterWait(ctx, tx, enter, "w"); err != nil || ok {
			t.Fatalf("EnterWait of a waiting run = %v, %v", ok, err)
		}

		if ok, err := s.ClearLookback(ctx, tx, run.ID, 2); err != nil || ok {
			t.Fatalf("ClearLookback of another sequence = %v, %v", ok, err)
		}
		if ok, err := s.ClearLookback(ctx, tx, run.ID, 1); err != nil || !ok {
			t.Fatalf("ClearLookback = %v, %v", ok, err)
		}
		if r := get(); r.LookbackPending || r.Rev != 3 {
			t.Fatalf("after ClearLookback pending %v rev %d, want cleared and rev kept", r.LookbackPending, r.Rev)
		}

		claim := ClaimRow{ID: run.ID, Rev: 3, WaitStep: "w", WaitSeq: 1, Frames: json.RawMessage(`{"stack":[{"step":"next"}],"visits":{"w":1}}`), At: t0.Add(3 * time.Second)}
		for _, stale := range []ClaimRow{
			func(c ClaimRow) ClaimRow { c.Rev = 2; return c }(claim),
			func(c ClaimRow) ClaimRow { c.WaitSeq = 2; return c }(claim),
			func(c ClaimRow) ClaimRow { c.WaitStep = "other"; return c }(claim),
		} {
			if ok, err := s.ClaimRun(ctx, tx, stale); err != nil || ok {
				t.Fatalf("ClaimRun %+v = %v, %v; want refused", stale, ok, err)
			}
		}
		if ok, err := s.ClaimRun(ctx, tx, claim); err != nil || !ok {
			t.Fatalf("ClaimRun = %v, %v", ok, err)
		}
		r = get()
		jsonEqual(t, "frames", r.Frames, claim.Frames)
		if r.State != "running" || r.WaitStep != nil || r.LookbackPending || r.WaitSeq != 1 || r.Rev != 4 {
			t.Fatalf("claimed run = %+v", r)
		}
		if ok, err := s.ClaimRun(ctx, tx, ClaimRow{ID: run.ID, Rev: 4, WaitStep: "w", WaitSeq: 1, Frames: claim.Frames, At: t0}); err != nil || ok {
			t.Fatalf("second ClaimRun = %v, %v", ok, err)
		}
		if ok, err := s.ClearLookback(ctx, tx, run.ID, 1); err != nil || ok {
			t.Fatalf("ClearLookback of a running run = %v, %v", ok, err)
		}
	})
}

func TestSubscriptionsAndConsumedEvents(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	inTx(t, s, func(tx Tx) {
		trigger := insertEvent(t, s, tx, "order.created")
		first, second := newRun(trigger), newRun(trigger)
		if bytes.Compare(first.ID[:], second.ID[:]) > 0 {
			first, second = second, first
		}
		other := newRun(trigger)
		for _, r := range []*RunRow{&first, &second, &other} {
			r.State, r.WaitStep, r.WaitSeq = "waiting", ptr("w"), 1
			r.EntityKind, r.EntityID, r.ScopeExtra = ptr("order"), ptr("o-1"), json.RawMessage(`{"region": "vn"}`)
			r.SerialKey, r.Exclusive = ptr("o-1"), false
			if err := s.InsertRun(ctx, tx, *r); err != nil {
				t.Fatal(err)
			}
		}
		for _, r := range []RunRow{first, second} {
			if err := s.InsertSubscription(ctx, tx, SubscriptionRow{
				RunID: r.ID, WaitSeq: 1, StepID: "w", EventType: "order.paid", MatchCEL: "event.ok", CreatedAt: t0,
			}); err != nil {
				t.Fatal(err)
			}
		}
		// A subscription of another sequence than the run waits on.
		if err := s.InsertSubscription(ctx, tx, SubscriptionRow{
			RunID: other.ID, WaitSeq: 2, StepID: "w", EventType: "order.paid", CreatedAt: t0,
		}); err != nil {
			t.Fatal(err)
		}
		ev := insertEvent(t, s, tx, "order.paid")
		list := func(eventType string, eventID int64, after ids.UUID, limit int) []WaitingRun {
			t.Helper()
			got, err := s.ListEventSubscriptions(ctx, tx, SubscriptionQuery{EventType: eventType, EventID: eventID, AfterRunID: after, Limit: limit})
			if err != nil {
				t.Fatal(err)
			}
			return got
		}

		// Pages follow the run id.
		var seen []ids.UUID
		for after := (ids.UUID{}); ; {
			page := list("order.paid", ev, after, 1)
			if len(page) == 0 {
				break
			}
			seen = append(seen, page[0].Sub.RunID)
			after = page[0].Sub.RunID
		}
		if !reflect.DeepEqual(seen, []ids.UUID{first.ID, second.ID}) {
			t.Fatalf("pages = %v, want %v then %v", seen, first.ID, second.ID)
		}
		got := list("order.paid", ev, ids.UUID{}, 10)
		if len(got) != 2 {
			t.Fatalf("subscriptions = %+v, want the open waits only", got)
		}
		want := WaitingRun{
			Sub: SubscriptionRow{RunID: first.ID, WaitSeq: 1, StepID: "w", EventType: "order.paid", CreatedAt: t0},
			Run: RunRow{
				ID: first.ID, DefName: first.DefName, DefVersion: first.DefVersion, TriggerEventID: trigger,
				State: "waiting", EntityKind: first.EntityKind, EntityID: first.EntityID, ScopeExtra: first.ScopeExtra,
				SerialKey: first.SerialKey, WaitStep: ptr("w"), WaitSeq: 1, Frames: first.Frames, Rev: first.Rev, CreatedAt: t0,
			},
		}
		g := got[0]
		g.Sub.CreatedAt, g.Run.CreatedAt = g.Sub.CreatedAt.UTC(), g.Run.CreatedAt.UTC()
		if !reflect.DeepEqual(g, want) {
			t.Fatalf("subscription =\n%+v\nwant (no match, definition or facts)\n%+v", g, want)
		}
		if got := list("order.shipped", ev, ids.UUID{}, 10); len(got) != 0 {
			t.Fatalf("subscriptions of another type = %+v", got)
		}

		c := ConsumedEventRow{RunID: first.ID, EventID: ev, StepID: "w", At: t0}
		if fresh, err := s.InsertConsumedEventIfAbsent(ctx, tx, c); err != nil || !fresh {
			t.Fatalf("InsertConsumedEventIfAbsent = %v, %v", fresh, err)
		}
		if fresh, err := s.InsertConsumedEventIfAbsent(ctx, tx, c); err != nil || fresh {
			t.Fatalf("second InsertConsumedEventIfAbsent = %v, %v", fresh, err)
		}
		if got := list("order.paid", ev, ids.UUID{}, 10); len(got) != 1 || got[0].Sub.RunID != second.ID {
			t.Fatalf("subscriptions of an event one run consumed = %+v", got)
		}

		if err := s.DeleteSubscription(ctx, tx, second.ID, 1); err != nil {
			t.Fatal(err)
		}
		if got := list("order.paid", ev, ids.UUID{}, 10); len(got) != 0 {
			t.Fatalf("subscriptions after the delete = %+v", got)
		}
	})

	// A failed query aborts its transaction, so this one gets its own.
	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := s.ListEventSubscriptions(ctx, tx, SubscriptionQuery{EventType: "order.paid", Limit: -1}); err == nil {
		t.Fatal("ListEventSubscriptions accepted a negative limit")
	}
}

func TestLookbackPage(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	inTx(t, s, func(tx Tx) {
		run := newRun(insertEvent(t, s, tx, "order.created"))
		if err := s.InsertRun(ctx, tx, run); err != nil {
			t.Fatal(err)
		}
		// Five order.paid events, two of them stored at one instant, and
		// one of another type in between.
		var paid []int64
		for i, at := range []time.Duration{0, time.Second, time.Second, 2 * time.Second, 3 * time.Second} {
			if i == 2 {
				insertEvent(t, s, tx, "order.shipped")
			}
			id, _, err := s.InsertEvent(ctx, tx, EventRow{
				Type: "order.paid", Payload: json.RawMessage(`{}`), OccurredAt: t0, InsertedAt: t0.Add(at),
			})
			if err != nil {
				t.Fatal(err)
			}
			paid = append(paid, id)
		}
		if _, err := s.InsertConsumedEventIfAbsent(ctx, tx, ConsumedEventRow{RunID: run.ID, EventID: paid[3], StepID: "w", At: t0}); err != nil {
			t.Fatal(err)
		}

		var seen []int64
		q := LookbackQuery{RunID: run.ID, EventType: "order.paid", AfterAt: t0.Add(-time.Second), Limit: 2}
		for {
			page, err := s.LookbackPage(ctx, tx, q)
			if err != nil {
				t.Fatal(err)
			}
			for _, ev := range page {
				if ev.Type != "order.paid" {
					t.Fatalf("page has a %s event", ev.Type)
				}
				seen = append(seen, ev.ID)
				q.AfterAt, q.AfterID = ev.InsertedAt, ev.ID
			}
			if len(page) < q.Limit {
				break
			}
		}
		want := []int64{paid[0], paid[1], paid[2], paid[4]}
		if len(seen) != len(want) {
			t.Fatalf("look-back read %v, want %v without the consumed event", seen, want)
		}
		for i := range want {
			if seen[i] != want[i] {
				t.Fatalf("look-back read %v, want %v in (inserted_at, id) order", seen, want)
			}
		}

		// The cursor is exclusive: from the second event, the third is next.
		page, err := s.LookbackPage(ctx, tx, LookbackQuery{
			RunID: run.ID, EventType: "order.paid", AfterAt: t0.Add(time.Second), AfterID: paid[1], Limit: 10,
		})
		if err != nil || len(page) != 2 || page[0].ID != paid[2] {
			t.Fatalf("page after the second event = %+v, %v", page, err)
		}
	})

	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := s.LookbackPage(ctx, tx, LookbackQuery{RunID: ids.NewV4(), EventType: "x", Limit: -1}); err == nil {
		t.Fatal("LookbackPage accepted a negative limit")
	}
}

func TestReadOnly(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	var n int
	err := s.ReadOnly(ctx, time.Second, func(ctx context.Context, q ReadQuerier) error {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("the callback's context has no deadline")
		}
		return q.QueryRow(ctx, `SELECT 41 + 1`).Scan(&n)
	})
	if err != nil || n != 42 {
		t.Fatalf("read = %d, %v", n, err)
	}

	err = s.ReadOnly(ctx, time.Second, func(ctx context.Context, q ReadQuerier) error {
		rows, err := q.Query(ctx, `INSERT INTO "`+s.Schema()+`".serial_locks (def_name, serial_key) VALUES ('d', 'k')`)
		if err != nil {
			return err
		}
		rows.Close()
		return rows.Err()
	})
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "25006" {
		t.Fatalf("write in a read-only transaction = %v, want SQLSTATE 25006", err)
	}

	start := time.Now()
	err = s.ReadOnly(ctx, 100*time.Millisecond, func(ctx context.Context, q ReadQuerier) error {
		return q.QueryRow(ctx, `SELECT pg_sleep(5)`).Scan(new(string))
	})
	if err == nil || time.Since(start) > 4*time.Second {
		t.Fatalf("slow read = %v after %v, want stopped at the timeout", err, time.Since(start))
	}

	called := false
	err = s.ReadOnly(ctx, 0, func(context.Context, ReadQuerier) error { called = true; return nil })
	if err == nil || called || !strings.Contains(err.Error(), "must be positive") {
		t.Fatalf("zero timeout = %v (called %v)", err, called)
	}

	want := errors.New("resolver failed")
	if err := s.ReadOnly(ctx, time.Second, func(context.Context, ReadQuerier) error { return want }); !errors.Is(err, want) {
		t.Fatalf("callback error = %v, want it returned", err)
	}
}

func TestStepRecorded(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	inTx(t, s, func(tx Tx) {
		run := newRun(insertEvent(t, s, tx, "order.created"))
		if err := s.InsertRun(ctx, tx, run); err != nil {
			t.Fatal(err)
		}
		if _, err := s.AppendStep(ctx, tx, StepRow{RunID: run.ID, At: t0, StepID: "w", Kind: "match_error", Detail: ptr("event 7")}); err != nil {
			t.Fatal(err)
		}
		for _, c := range []struct {
			stepID, kind, detail string
			want                 bool
		}{
			{"w", "match_error", "event 7", true},
			{"w", "match_error", "event 8", false},
			{"v", "match_error", "event 7", false},
			{"w", "claim_lost", "event 7", false},
		} {
			if got, err := s.StepRecorded(ctx, tx, run.ID, c.stepID, c.kind, c.detail); err != nil || got != c.want {
				t.Errorf("StepRecorded(%s, %s, %s) = %v, %v; want %v", c.stepID, c.kind, c.detail, got, err, c.want)
			}
		}
		if got, err := s.StepRecorded(ctx, tx, ids.NewV4(), "w", "match_error", "event 7"); err != nil || got {
			t.Errorf("StepRecorded of another run = %v, %v", got, err)
		}
	})
}
