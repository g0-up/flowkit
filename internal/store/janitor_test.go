package store

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/migrate"
)

// execSQL runs sql, written with migrate.SchemaToken, against s's schema.
func execSQL(t *testing.T, pool *pgxpool.Pool, s *PgStore, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), migrate.RenderSchema(sql, s.Schema()), args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func countRows(t *testing.T, pool *pgxpool.Pool, s *PgStore, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), migrate.RenderSchema(sql, s.Schema()), args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

func insertDefinition(t *testing.T, s *PgStore, tx Tx, name string, enabled bool) {
	t.Helper()
	if _, err := s.InsertDefinitionIfAbsent(context.Background(), tx, DefinitionRow{
		Name: name, Enabled: enabled, CurrentVersion: ptr(1), ConcurrencyPolicy: "serial", CreatedAt: t0, UpdatedAt: t0,
	}); err != nil {
		t.Fatal(err)
	}
}

func insertRuns(t *testing.T, s *PgStore, tx Tx, runs ...RunRow) {
	t.Helper()
	for _, r := range runs {
		if err := s.InsertRun(context.Background(), tx, r); err != nil {
			t.Fatal(err)
		}
	}
}

func sortedIDs(in []ids.UUID) []string {
	out := make([]string, len(in))
	for i, id := range in {
		out[i] = id.String()
	}
	slices.Sort(out)
	return out
}

func wantIDs(t *testing.T, what string, got []ids.UUID, want ...RunRow) {
	t.Helper()
	w := make([]ids.UUID, len(want))
	for i, r := range want {
		w[i] = r.ID
	}
	if g, w := sortedIDs(got), sortedIDs(w); !slices.Equal(g, w) {
		t.Errorf("%s = %v, want %v", what, g, w)
	}
}

func TestListStaleLookbacks(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	cutoff := t0.Add(time.Hour)
	inTx(t, s, func(tx Tx) {
		ev := insertEvent(t, s, tx, "order.created")
		waiting := func(updated time.Time, pending bool) RunRow {
			r := newRun(ev)
			r.State, r.WaitStep, r.WaitSeq, r.LookbackPending, r.UpdatedAt = "waiting", ptr("w"), 1, pending, updated
			return r
		}
		stale := waiting(t0, true)
		atCutoff := waiting(cutoff, true)
		finished := waiting(t0, false)
		running := newRun(ev)
		insertRuns(t, s, tx, stale, atCutoff, finished, running)

		got, err := s.ListStaleLookbacks(ctx, tx, cutoff, 10)
		if err != nil {
			t.Fatal(err)
		}
		wantIDs(t, "ListStaleLookbacks", got, stale)
	})
}

func TestListOrphanedQueues(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	inTx(t, s, func(tx Tx) {
		insertDefinition(t, s, tx, "enrol", true)
		insertDefinition(t, s, tx, "paused", false)
		ev := insertEvent(t, s, tx, "order.created")
		queue := func(def, key string) {
			if _, err := s.InsertRunQueue(ctx, tx, RunQueueRow{DefName: def, SerialKey: key, TriggerEventID: ev, EventAt: t0, CreatedAt: t0}); err != nil {
				t.Fatal(err)
			}
		}
		exclusive := func(def, key string, terminal bool) RunRow {
			r := newRun(ev)
			r.DefName, r.SerialKey, r.Exclusive = def, ptr(key), true
			if terminal {
				r.State, r.Terminal, r.TerminalName = "completed", true, ptr("done")
			}
			return r
		}
		// k1 has an active run that promotes its queue when it ends; k2's run
		// ended without promoting; k3 never had a run.
		insertRuns(t, s, tx, exclusive("enrol", "k1", false), exclusive("enrol", "k2", true))
		queue("enrol", "k1")
		queue("enrol", "k2")
		queue("enrol", "k3")
		queue("paused", "k1")

		got, err := s.ListOrphanedQueues(ctx, tx, 10)
		if err != nil {
			t.Fatal(err)
		}
		want := []SerialKeyRef{{"enrol", "k2"}, {"enrol", "k3"}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("ListOrphanedQueues = %v, want %v", got, want)
		}
		if got, err := s.ListOrphanedQueues(ctx, tx, 1); err != nil || len(got) != 1 {
			t.Fatalf("ListOrphanedQueues with limit 1 = %v, %v", got, err)
		}
	})
}

func TestListStuckRuns(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	updatedBefore := t0.Add(time.Hour)
	claimedAfter := t0.Add(50 * time.Minute)
	var stuck, expiredClaim, pumped, finishedPump, inDoubt, claimed, recent, disabled, waiting RunRow
	inTx(t, s, func(tx Tx) {
		insertDefinition(t, s, tx, "order_flow", true)
		insertDefinition(t, s, tx, "paused", false)
		ev := insertEvent(t, s, tx, "order.created")
		stuck, expiredClaim, pumped, finishedPump = newRun(ev), newRun(ev), newRun(ev), newRun(ev)
		inDoubt, claimed, recent, disabled, waiting = newRun(ev), newRun(ev), newRun(ev), newRun(ev), newRun(ev)
		recent.UpdatedAt = updatedBefore
		disabled.DefName = "paused"
		waiting.State, waiting.WaitStep, waiting.WaitSeq = "waiting", ptr("w"), 1
		insertRuns(t, s, tx, stuck, expiredClaim, pumped, finishedPump, inDoubt, claimed, recent, disabled, waiting)

		claim := func(key string, run RunRow, at time.Time) ids.UUID {
			tok := ids.NewV4()
			if ok, err := s.InsertEffectClaim(ctx, tx, EffectClaim{IdemKey: key, RunID: run.ID, StepID: "s", Action: "a", Token: tok, At: at}); err != nil || !ok {
				t.Fatal(ok, err)
			}
			return tok
		}
		claim("expired", expiredClaim, claimedAfter.Add(-time.Second))
		claim("live", claimed, claimedAfter)
		tok := claim("doubt", inDoubt, t0)
		if _, ok, err := s.MarkEffectInDoubt(ctx, tx, "doubt", tok, "timeout"); err != nil || !ok {
			t.Fatal(ok, err)
		}
		// A completed effect does not hold the run.
		done := claim("done", stuck, claimedAfter)
		if ok, err := s.CompleteEffect(ctx, tx, "done", done, t0, json.RawMessage(`{}`), nil); err != nil || !ok {
			t.Fatal(ok, err)
		}
	})
	pumpJob := func(run RunRow, state string) {
		finalized := any(nil)
		if state == "completed" {
			finalized = t0
		}
		execSQL(t, pool, s, `INSERT INTO `+migrate.SchemaToken+`river_job (kind, queue, state, args, max_attempts, finalized_at)
			VALUES ('flowkit.pump_run', 'flowkit', $1, jsonb_build_object('run_id', $2::text), 25, $3)`, state, run.ID.String(), finalized)
	}
	// A snoozed pump is scheduled and still live; a completed one is not.
	pumpJob(pumped, "scheduled")
	pumpJob(finishedPump, "completed")

	inTx(t, s, func(tx Tx) {
		got, err := s.ListStuckRuns(ctx, tx, StuckRunQuery{
			UpdatedBefore: updatedBefore, ClaimedAfter: claimedAfter, PumpKind: "flowkit.pump_run", Limit: 10,
		})
		if err != nil {
			t.Fatal(err)
		}
		wantIDs(t, "ListStuckRuns", got, stuck, expiredClaim, finishedPump)
	})
}

func TestSummarizeRuns(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	cutoff := t0.Add(90 * 24 * time.Hour)
	var old, atCutoff, live RunRow
	inTx(t, s, func(tx Tx) {
		insertDefinition(t, s, tx, "order_flow", true)
		ev := insertEvent(t, s, tx, "order.created")
		old, atCutoff, live = newRun(ev), newRun(ev), newRun(ev)
		old.State, old.Terminal, old.TerminalName = "completed", true, ptr("done")
		// The step appended below makes the step count 4.
		old.EntityKind, old.EntityID, old.StepSeq = ptr("order"), ptr("o-1"), 3
		old.CreatedAt, old.UpdatedAt = t0.Add(-time.Hour), t0
		atCutoff.State, atCutoff.Terminal, atCutoff.UpdatedAt = "failed", true, cutoff
		insertRuns(t, s, tx, old, atCutoff, live)
		for _, r := range []RunRow{old, live} {
			if _, err := s.AppendStep(ctx, tx, StepRow{RunID: r.ID, At: t0, StepID: "s", Kind: "entered"}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.InsertEffectClaim(ctx, tx, EffectClaim{IdemKey: r.ID.String(), RunID: r.ID, StepID: "s", Action: "a", Token: ids.NewV4(), At: t0}); err != nil {
				t.Fatal(err)
			}
			if err := s.InsertConsumedEvent(ctx, tx, ConsumedEventRow{RunID: r.ID, EventID: ev, StepID: "w", At: t0}); err != nil {
				t.Fatal(err)
			}
			if err := s.InsertSubscription(ctx, tx, SubscriptionRow{RunID: r.ID, WaitSeq: 1, StepID: "w", EventType: "order.paid", CreatedAt: t0}); err != nil {
				t.Fatal(err)
			}
		}
	})

	inTx(t, s, func(tx Tx) {
		n, err := s.SummarizeRuns(ctx, tx, cutoff, 10)
		if err != nil || n != 1 {
			t.Fatalf("SummarizeRuns = %d, %v; want 1", n, err)
		}
		if _, err := s.GetRun(ctx, tx, old.ID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("summarised run is still readable: %v", err)
		}
		for _, r := range []RunRow{atCutoff, live} {
			if _, err := s.GetRun(ctx, tx, r.ID); err != nil {
				t.Fatalf("run %s: %v", r.ID, err)
			}
		}
	})
	for _, table := range []string{"run_steps", "consumed_events", "subscriptions", "executed_effects"} {
		sql := `SELECT count(*) FROM ` + migrate.SchemaToken + table + ` WHERE run_id = $1`
		if n := countRows(t, pool, s, sql, old.ID); n != 0 {
			t.Errorf("%s keeps %d rows of the summarised run", table, n)
		}
		if n := countRows(t, pool, s, sql, live.ID); n != 1 {
			t.Errorf("%s has %d rows of the live run, want 1", table, n)
		}
	}

	inTx(t, s, func(tx Tx) {
		got, err := s.ListRuns(ctx, tx, RunListQuery{EntityKind: ptr("order"), Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		want := RunSummaryRow{
			RunID: old.ID, DefName: "order_flow", DefVersion: 1, State: "completed", TerminalName: ptr("done"),
			StepCount: 4, TriggerEventType: "order.created", EntityKind: ptr("order"), EntityID: ptr("o-1"),
			StartedAt: old.CreatedAt, EndedAt: ptr(t0),
		}
		if len(got) != 1 || !summaryEqual(got[0], want) {
			t.Fatalf("ListRuns of the summarised run = %+v, want %+v", got, want)
		}

		// Summaries are kept until Retention.Run.
		if n, err := s.DeleteExpiredSummaries(ctx, tx, t0, 10); err != nil || n != 0 {
			t.Fatalf("DeleteExpiredSummaries at the end time = %d, %v; want 0", n, err)
		}
		if n, err := s.DeleteExpiredSummaries(ctx, tx, t0.Add(time.Microsecond), 10); err != nil || n != 1 {
			t.Fatalf("DeleteExpiredSummaries = %d, %v; want 1", n, err)
		}
	})
}

func summaryEqual(a, b RunSummaryRow) bool {
	if !a.StartedAt.Equal(b.StartedAt) || (a.EndedAt == nil) != (b.EndedAt == nil) ||
		(a.EndedAt != nil && !a.EndedAt.Equal(*b.EndedAt)) {
		return false
	}
	a.StartedAt, a.EndedAt = b.StartedAt, b.EndedAt
	return reflect.DeepEqual(a, b)
}

func TestDeleteExpiredEvents(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	cutoff := t0.Add(time.Hour)
	inTx(t, s, func(tx Tx) {
		event := func(at time.Time) int64 {
			id, _, err := s.InsertEvent(ctx, tx, EventRow{Type: "order.created", Payload: json.RawMessage(`{}`), OccurredAt: at, InsertedAt: at})
			if err != nil {
				t.Fatal(err)
			}
			return id
		}
		trigger, consumed, queued, free, fresh := event(t0), event(t0), event(t0), event(t0), event(cutoff)
		run := newRun(trigger)
		insertRuns(t, s, tx, run)
		if err := s.InsertConsumedEvent(ctx, tx, ConsumedEventRow{RunID: run.ID, EventID: consumed, StepID: "w", At: t0}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.InsertRunQueue(ctx, tx, RunQueueRow{DefName: "d", SerialKey: "k", TriggerEventID: queued, EventAt: t0, CreatedAt: t0}); err != nil {
			t.Fatal(err)
		}

		n, err := s.DeleteExpiredEvents(ctx, tx, cutoff, 10)
		if err != nil || n != 1 {
			t.Fatalf("DeleteExpiredEvents = %d, %v; want 1", n, err)
		}
		if _, err := s.GetEvent(ctx, tx, free); !errors.Is(err, ErrNotFound) {
			t.Fatalf("unreferenced expired event: %v", err)
		}
		for _, id := range []int64{trigger, consumed, queued, fresh} {
			if _, err := s.GetEvent(ctx, tx, id); err != nil {
				t.Fatalf("event %d: %v", id, err)
			}
		}
	})
}

func TestDeleteResolvedDeadLetters(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	cutoff := t0.Add(time.Hour)
	inTx(t, s, func(tx Tx) {
		letter := func(ref string) int64 {
			id, _, err := s.InsertDeadLetter(ctx, tx, DeadLetterRow{Kind: "unroutable_event", Ref: ref, Raw: json.RawMessage(`{}`), CreatedAt: t0})
			if err != nil {
				t.Fatal(err)
			}
			return id
		}
		open, old, recent := letter("1"), letter("2"), letter("3")
		for id, at := range map[int64]time.Time{old: t0, recent: cutoff} {
			if _, ok, err := s.ClaimDeadLetter(ctx, tx, DeadLetterClaim{ID: id, Resolution: "skip", At: at}); err != nil || !ok {
				t.Fatal(ok, err)
			}
		}

		n, err := s.DeleteResolvedDeadLetters(ctx, tx, cutoff, 10)
		if err != nil || n != 1 {
			t.Fatalf("DeleteResolvedDeadLetters = %d, %v; want 1", n, err)
		}
		if _, err := s.GetDeadLetter(ctx, tx, old); !errors.Is(err, ErrNotFound) {
			t.Fatalf("expired resolved dead letter: %v", err)
		}
		for _, id := range []int64{open, recent} {
			if _, err := s.GetDeadLetter(ctx, tx, id); err != nil {
				t.Fatalf("dead letter %d: %v", id, err)
			}
		}
		// An open dead letter is never deleted, however old.
		if n, err := s.DeleteResolvedDeadLetters(ctx, tx, t0.Add(1000*time.Hour), 10); err != nil || n != 1 {
			t.Fatalf("DeleteResolvedDeadLetters later = %d, %v; want 1", n, err)
		}
		if _, err := s.GetDeadLetter(ctx, tx, open); err != nil {
			t.Fatalf("open dead letter: %v", err)
		}
	})
}

func TestClaimDeadLetter(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	inTx(t, s, func(tx Tx) {
		id, _, err := s.InsertDeadLetter(ctx, tx, DeadLetterRow{
			Kind: "unroutable_event", Ref: "7", EventID: ptr(int64(7)), Raw: json.RawMessage(`{}`), CreatedAt: t0,
		})
		if err != nil {
			t.Fatal(err)
		}
		at := t0.Add(time.Minute)
		got, ok, err := s.ClaimDeadLetter(ctx, tx, DeadLetterClaim{ID: id, Resolution: "redrive", ResolvedBy: ptr("ops"), Reason: ptr("fixed"), At: at})
		if err != nil || !ok {
			t.Fatalf("ClaimDeadLetter = %v, %v", ok, err)
		}
		if got.ID != id || got.Kind != "unroutable_event" || *got.EventID != 7 || !got.ResolvedAt.Equal(at) ||
			*got.Resolution != "redrive" || *got.ResolvedBy != "ops" || *got.ResolveReason != "fixed" {
			t.Fatalf("claimed dead letter = %+v", got)
		}
		if _, ok, err := s.ClaimDeadLetter(ctx, tx, DeadLetterClaim{ID: id, Resolution: "skip", At: at}); err != nil || ok {
			t.Fatalf("second ClaimDeadLetter = %v, %v; want false", ok, err)
		}
		if _, ok, err := s.ClaimDeadLetter(ctx, tx, DeadLetterClaim{ID: id + 100, Resolution: "skip", At: at}); err != nil || ok {
			t.Fatalf("ClaimDeadLetter of a missing id = %v, %v; want false", ok, err)
		}
	})
}

func TestResolveInDoubtEffects(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	run := insertTestRun(t, s)
	at := t0.Add(time.Minute)
	inTx(t, s, func(tx Tx) {
		for _, key := range []string{"redrive", "kept", "done", "claimed"} {
			tok := ids.NewV4()
			if _, err := s.InsertEffectClaim(ctx, tx, EffectClaim{IdemKey: key, RunID: run.ID, StepID: "s", Action: "a", Token: tok, At: t0}); err != nil {
				t.Fatal(err)
			}
			if key == "claimed" {
				continue
			}
			if _, ok, err := s.MarkEffectInDoubt(ctx, tx, key, tok, "timeout"); err != nil || !ok {
				t.Fatal(ok, err)
			}
			if key == "redrive" {
				continue
			}
			// A late result of the first attempt.
			if _, err := s.SetInDoubtEffectResult(ctx, tx, key, json.RawMessage(`{"late":true}`)); err != nil {
				t.Fatal(err)
			}
		}
		// An effect that kept a late success is not run again.
		for _, key := range []string{"claimed", "missing", "kept"} {
			if ok, err := s.RedriveInDoubtEffect(ctx, tx, key); err != nil || ok {
				t.Fatalf("RedriveInDoubtEffect(%s) = %v, %v; want false", key, ok, err)
			}
			if key == "kept" {
				continue
			}
			if ok, err := s.MarkInDoubtEffectDone(ctx, tx, key, at, json.RawMessage(`{}`)); err != nil || ok {
				t.Fatalf("MarkInDoubtEffectDone(%s) = %v, %v; want false", key, ok, err)
			}
		}
		if ok, err := s.RedriveInDoubtEffect(ctx, tx, "redrive"); err != nil || !ok {
			t.Fatalf("RedriveInDoubtEffect = %v, %v", ok, err)
		}
		if ok, err := s.RedriveInDoubtEffect(ctx, tx, "redrive"); err != nil || ok {
			t.Fatalf("second RedriveInDoubtEffect = %v, %v; want false", ok, err)
		}
		if ok, err := s.MarkInDoubtEffectDone(ctx, tx, "done", at, json.RawMessage(`{"n":1}`)); err != nil || !ok {
			t.Fatalf("MarkInDoubtEffectDone = %v, %v", ok, err)
		}
		if ok, err := s.RedriveInDoubtEffect(ctx, tx, "done"); err != nil || ok {
			t.Fatalf("RedriveInDoubtEffect of a done effect = %v, %v; want false", ok, err)
		}
	})
	// The redriven effect runs again from a clean slate: no claim, no result.
	if got := getEffect(t, s, "redrive"); got.InDoubt || got.ClaimedAt != nil || got.ClaimToken != nil || got.DoneAt != nil || got.Result != nil {
		t.Fatalf("redriven effect = %+v (result %s)", got, got.Result)
	}
	if got := getEffect(t, s, "kept"); !got.InDoubt || got.DoneAt != nil {
		t.Fatalf("effect with a late result = %+v, want it still in doubt", got)
	} else {
		jsonEqual(t, "late result", got.Result, json.RawMessage(`{"late":true}`))
	}
	// The effect marked done keeps its in-doubt flag as a record; done_at
	// ends it.
	got := getEffect(t, s, "done")
	if got.DoneAt == nil || !got.DoneAt.Equal(at) {
		t.Fatalf("effect marked done = %+v", got)
	}
	jsonEqual(t, "result", got.Result, json.RawMessage(`{"n":1}`))
}

func TestListRuns(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	var runs []RunRow // newest first
	inTx(t, s, func(tx Tx) {
		insertDefinition(t, s, tx, "order_flow", true)
		ev := insertEvent(t, s, tx, "order.created")
		for i := range 5 {
			r := newRun(ev)
			// Two runs share each start time but the last, so the run id
			// breaks ties.
			r.CreatedAt = t0.Add(time.Duration(i/2) * time.Minute)
			if i%2 == 1 {
				r.State, r.Terminal, r.TerminalName = "failed", true, ptr("$failed")
				r.UpdatedAt = r.CreatedAt.Add(time.Second)
			}
			if i == 4 {
				r.DefName, r.EntityKind, r.EntityID = "refund", ptr("order"), ptr("o-9")
			}
			runs = append(runs, r)
		}
		insertRuns(t, s, tx, runs...)
	})
	slices.SortFunc(runs, func(a, b RunRow) int {
		if c := b.CreatedAt.Compare(a.CreatedAt); c != 0 {
			return c
		}
		return -slices.Compare(a.ID[:], b.ID[:])
	})
	// Summarising a run keeps it in the list at the same place.
	inTx(t, s, func(tx Tx) {
		if n, err := s.SummarizeRuns(ctx, tx, t0.Add(time.Hour), 1); err != nil || n != 1 {
			t.Fatalf("SummarizeRuns = %d, %v", n, err)
		}
	})

	list := func(q RunListQuery) []ids.UUID {
		t.Helper()
		var out []ids.UUID
		inTx(t, s, func(tx Tx) {
			rows, err := s.ListRuns(ctx, tx, q)
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range rows {
				out = append(out, r.RunID)
			}
		})
		return out
	}
	var paged []ids.UUID
	q := RunListQuery{Limit: 2}
	for {
		page := list(q)
		paged = append(paged, page...)
		if len(page) < q.Limit {
			break
		}
		var last RunRow
		for _, r := range runs {
			if r.ID == page[len(page)-1] {
				last = r
			}
		}
		q.AfterAt, q.AfterID = &last.CreatedAt, &last.ID
	}
	want := make([]ids.UUID, len(runs))
	for i, r := range runs {
		want[i] = r.ID
	}
	if !slices.Equal(paged, want) {
		t.Fatalf("pages = %v, want %v", paged, want)
	}

	if got := list(RunListQuery{States: []string{"failed"}, Limit: 10}); len(got) != 2 {
		t.Errorf("failed runs = %d, want 2", len(got))
	}
	if got := list(RunListQuery{DefName: ptr("refund"), Limit: 10}); len(got) != 1 {
		t.Errorf("runs of refund = %d, want 1", len(got))
	}
	if got := list(RunListQuery{EntityKind: ptr("order"), EntityID: ptr("o-9"), Limit: 10}); len(got) != 1 {
		t.Errorf("runs of order o-9 = %d, want 1", len(got))
	}
	from, before := t0.Add(time.Minute), t0.Add(2*time.Minute)
	if got := list(RunListQuery{StartedFrom: &from, StartedBefore: &before, Limit: 10}); len(got) != 2 {
		t.Errorf("runs started in [1m, 2m) = %d, want 2", len(got))
	}
}

func TestReadGauges(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	inTx(t, s, func(tx Tx) {
		ev := insertEvent(t, s, tx, "order.created")
		live, ended := newRun(ev), newRun(ev)
		ended.State, ended.Terminal = "cancelled", true
		insertRuns(t, s, tx, live, ended)
		for _, r := range []RunRow{live, ended} {
			tok := ids.NewV4()
			if _, err := s.InsertEffectClaim(ctx, tx, EffectClaim{IdemKey: r.ID.String(), RunID: r.ID, StepID: "s", Action: "a", Token: tok, At: t0}); err != nil {
				t.Fatal(err)
			}
			if _, ok, err := s.MarkEffectInDoubt(ctx, tx, r.ID.String(), tok, "timeout"); err != nil || !ok {
				t.Fatal(ok, err)
			}
		}
		for i, kind := range []string{"exhausted", "exhausted", "unroutable_event", "schema_mismatch"} {
			id, _, err := s.InsertDeadLetter(ctx, tx, DeadLetterRow{Kind: kind, Ref: string(rune('a' + i)), Raw: json.RawMessage(`{}`), CreatedAt: t0})
			if err != nil {
				t.Fatal(err)
			}
			if kind == "schema_mismatch" {
				if _, _, err := s.ClaimDeadLetter(ctx, tx, DeadLetterClaim{ID: id, Resolution: "skip", At: t0}); err != nil {
					t.Fatal(err)
				}
			}
		}
	})
	for _, state := range []string{"available", "available", "scheduled"} {
		execSQL(t, pool, s, `INSERT INTO `+migrate.SchemaToken+`river_job (kind, queue, state, args, max_attempts)
			VALUES ('flowkit.pump_run', 'flowkit', $1, '{}', 25)`, state)
	}
	execSQL(t, pool, s, `INSERT INTO `+migrate.SchemaToken+`river_job (kind, queue, state, args, max_attempts)
		VALUES ('other', 'other', 'available', '{}', 25)`)

	read := func() Gauges {
		var g Gauges
		inTx(t, s, func(tx Tx) {
			var err error
			if g, err = s.ReadGauges(ctx, tx, "flowkit"); err != nil {
				t.Fatal(err)
			}
		})
		return g
	}
	want := Gauges{OpenDeadLetters: map[string]int{"exhausted": 2, "unroutable_event": 1}, InDoubtOpen: 1, QueueDepth: 2}
	if got := read(); !reflect.DeepEqual(got, want) {
		t.Fatalf("ReadGauges = %+v, want %+v", got, want)
	}
	execSQL(t, pool, s, `INSERT INTO `+migrate.SchemaToken+`river_leader (leader_id, elected_at, expires_at)
		VALUES ('instance-1', $1, $1)`, t0)
	if got := read(); got.LeaderID != "instance-1" {
		t.Fatalf("leader = %q, want instance-1", got.LeaderID)
	}
}
