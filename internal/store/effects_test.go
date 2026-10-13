package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/g0-up/flowkit/internal/ids"
)

func insertTestRun(t *testing.T, s *PgStore) RunRow {
	t.Helper()
	var run RunRow
	inTx(t, s, func(tx Tx) {
		run = newRun(insertEvent(t, s, tx, "order.created"))
		if err := s.InsertRun(context.Background(), tx, run); err != nil {
			t.Fatal(err)
		}
	})
	return run
}

func getEffect(t *testing.T, s *PgStore, key string) EffectRow {
	t.Helper()
	var row EffectRow
	inTx(t, s, func(tx Tx) {
		var err error
		if row, err = s.GetEffect(context.Background(), tx, key); err != nil {
			t.Fatal(err)
		}
	})
	return row
}

func TestEffectClaimLifecycle(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	run := insertTestRun(t, s)
	tok := ids.NewV4()
	claim := EffectClaim{IdemKey: "k1", RunID: run.ID, StepID: "charge", Action: "payments.charge", Token: tok, At: t0}

	inTx(t, s, func(tx Tx) {
		if ok, err := s.InsertEffectClaim(ctx, tx, claim); err != nil || !ok {
			t.Fatalf("first InsertEffectClaim = %v, %v", ok, err)
		}
		if ok, err := s.InsertEffectClaim(ctx, tx, claim); err != nil || ok {
			t.Fatalf("second InsertEffectClaim = %v, %v; want false", ok, err)
		}
	})
	got := getEffect(t, s, "k1")
	if got.Attempt != 0 || !got.ClaimedAt.Equal(t0) || *got.ClaimToken != tok || got.DoneAt != nil || got.InDoubt {
		t.Fatalf("claimed effect = %+v", got)
	}

	other := ids.NewV4()
	inTx(t, s, func(tx Tx) {
		if ok, err := s.ReclaimEffect(ctx, tx, "k1", other, t0, 5); err != nil || ok {
			t.Fatalf("ReclaimEffect of a held claim = %v, %v; want false", ok, err)
		}
		if _, ok, err := s.FailEffect(ctx, tx, "k1", other, "boom"); err != nil || ok {
			t.Fatalf("FailEffect with a foreign token = %v, %v; want false", ok, err)
		}
		attempt, ok, err := s.FailEffect(ctx, tx, "k1", tok, "boom")
		if err != nil || !ok || attempt != 1 {
			t.Fatalf("FailEffect = %d, %v, %v; want 1, true", attempt, ok, err)
		}
	})
	got = getEffect(t, s, "k1")
	if got.Attempt != 1 || got.ClaimedAt != nil || got.ClaimToken != nil || *got.LastError != "boom" {
		t.Fatalf("failed effect = %+v", got)
	}

	t1 := t0.Add(time.Minute)
	inTx(t, s, func(tx Tx) {
		if ok, err := s.CompleteEffect(ctx, tx, "k1", tok, t1, json.RawMessage(`{}`), nil); err != nil || ok {
			t.Fatalf("CompleteEffect after the claim was released = %v, %v; want false", ok, err)
		}
		if ok, err := s.ReclaimEffect(ctx, tx, "k1", other, t1, 1); err != nil || ok {
			t.Fatalf("ReclaimEffect with its attempts used up = %v, %v; want false", ok, err)
		}
		if ok, err := s.ReclaimEffect(ctx, tx, "k1", other, t1, 5); err != nil || !ok {
			t.Fatalf("ReclaimEffect of a released effect = %v, %v", ok, err)
		}
		if ok, err := s.CompleteEffect(ctx, tx, "k1", other, t1, json.RawMessage(`{"id": "ch_1"}`), nil); err != nil || !ok {
			t.Fatalf("CompleteEffect = %v, %v", ok, err)
		}
		if ok, err := s.CompleteEffect(ctx, tx, "k1", other, t1, json.RawMessage(`{}`), nil); err != nil || ok {
			t.Fatalf("second CompleteEffect = %v, %v; want false", ok, err)
		}
		if ok, err := s.ReleaseEffectClaim(ctx, tx, "k1", other); err != nil || ok {
			t.Fatalf("ReleaseEffectClaim of a done effect = %v, %v; want false", ok, err)
		}
	})
	got = getEffect(t, s, "k1")
	if got.Attempt != 1 || !got.DoneAt.Equal(t1) || !got.ClaimedAt.Equal(t1) || got.LastError != nil {
		t.Fatalf("done effect = %+v", got)
	}
	jsonEqual(t, "result", got.Result, json.RawMessage(`{"id": "ch_1"}`))
}

func TestCompleteEffectStoresNullResultWithError(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	run := insertTestRun(t, s)
	tok := ids.NewV4()
	inTx(t, s, func(tx Tx) {
		if _, err := s.InsertEffectClaim(ctx, tx, EffectClaim{IdemKey: "k1", RunID: run.ID, StepID: "s", Action: "a", Token: tok, At: t0}); err != nil {
			t.Fatal(err)
		}
		if ok, err := s.CompleteEffect(ctx, tx, "k1", tok, t0, nil, ptr("bad output")); err != nil || !ok {
			t.Fatalf("CompleteEffect = %v, %v", ok, err)
		}
	})
	got := getEffect(t, s, "k1")
	if got.DoneAt == nil || got.Result != nil || *got.LastError != "bad output" {
		t.Fatalf("effect = %+v", got)
	}
}

func TestMarkEffectInDoubtRevokesTheClaim(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	run := insertTestRun(t, s)
	tok := ids.NewV4()
	inTx(t, s, func(tx Tx) {
		if _, err := s.InsertEffectClaim(ctx, tx, EffectClaim{IdemKey: "k1", RunID: run.ID, StepID: "s", Action: "a", Token: tok, At: t0}); err != nil {
			t.Fatal(err)
		}
		attempt, ok, err := s.MarkEffectInDoubt(ctx, tx, "k1", tok, "timeout")
		if err != nil || !ok || attempt != 0 {
			t.Fatalf("MarkEffectInDoubt = %d, %v, %v; want 0, true", attempt, ok, err)
		}
		if ok, err := s.CompleteEffect(ctx, tx, "k1", tok, t0, json.RawMessage(`{}`), nil); err != nil || ok {
			t.Fatalf("CompleteEffect of an in-doubt effect = %v, %v; want false", ok, err)
		}
		if _, ok, err := s.MarkEffectInDoubt(ctx, tx, "k1", tok, "again"); err != nil || ok {
			t.Fatalf("second MarkEffectInDoubt = %v, %v; want false", ok, err)
		}
		if ok, err := s.ReclaimEffect(ctx, tx, "k1", ids.NewV4(), t0, 5); err != nil || ok {
			t.Fatalf("ReclaimEffect of an in-doubt effect = %v, %v; want false", ok, err)
		}
	})
	got := getEffect(t, s, "k1")
	if !got.InDoubt || got.ClaimToken != nil || got.DoneAt != nil || *got.LastError != "timeout" {
		t.Fatalf("in-doubt effect = %+v", got)
	}
}

func TestListExpiredClaims(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	run := insertTestRun(t, s)
	claim := func(tx Tx, key string, at time.Time) ids.UUID {
		tok := ids.NewV4()
		if ok, err := s.InsertEffectClaim(ctx, tx, EffectClaim{IdemKey: key, RunID: run.ID, StepID: key, Action: "a", Token: tok, At: at}); err != nil || !ok {
			t.Fatalf("claim %s: %v, %v", key, ok, err)
		}
		return tok
	}
	inTx(t, s, func(tx Tx) {
		claim(tx, "old2", t0.Add(-2*time.Minute))
		claim(tx, "old1", t0.Add(-3*time.Minute))
		claim(tx, "old3", t0.Add(-time.Minute))
		claim(tx, "fresh", t0)
		done := claim(tx, "done", t0.Add(-time.Hour))
		if ok, err := s.CompleteEffect(ctx, tx, "done", done, t0, json.RawMessage(`{}`), nil); err != nil || !ok {
			t.Fatal(ok, err)
		}
		doubt := claim(tx, "doubt", t0.Add(-time.Hour))
		if _, ok, err := s.MarkEffectInDoubt(ctx, tx, "doubt", doubt, "x"); err != nil || !ok {
			t.Fatal(ok, err)
		}
		released := claim(tx, "released", t0.Add(-time.Hour))
		if ok, err := s.ReleaseEffectClaim(ctx, tx, "released", released); err != nil || !ok {
			t.Fatal(ok, err)
		}
		// A claim time without a token holder is no claim to resolve; it must
		// not take a place at the head of every batch.
		claim(tx, "no-token", t0.Add(-time.Hour))
		ptx, err := s.PgxTx(tx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ptx.Exec(ctx, "UPDATE "+pgx.Identifier{s.Schema(), "executed_effects"}.Sanitize()+
			" SET claim_token = NULL WHERE idem_key = 'no-token'"); err != nil {
			t.Fatal(err)
		}
	})
	inTx(t, s, func(tx Tx) {
		rows, err := s.ListExpiredClaims(ctx, tx, t0.Add(-30*time.Second), 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 2 || rows[0].IdemKey != "old1" || rows[1].IdemKey != "old2" {
			t.Fatalf("ListExpiredClaims limit 2 = %+v", rows)
		}
		rows, err = s.ListExpiredClaims(ctx, tx, t0.Add(-30*time.Second), 10)
		if err != nil {
			t.Fatal(err)
		}
		var keys []string
		for _, r := range rows {
			keys = append(keys, r.IdemKey)
		}
		if len(keys) != 3 || keys[2] != "old3" {
			t.Fatalf("ListExpiredClaims = %v; want old1, old2, old3", keys)
		}
	})
}

func TestInDoubtEffectResultAndNote(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	run := insertTestRun(t, s)
	tok := ids.NewV4()
	var dlID int64
	inTx(t, s, func(tx Tx) {
		for key, token := range map[string]ids.UUID{"k1": tok, "claimed": ids.NewV4()} {
			if _, err := s.InsertEffectClaim(ctx, tx, EffectClaim{IdemKey: key, RunID: run.ID, StepID: "s", Action: "a", Token: token, At: t0}); err != nil {
				t.Fatal(err)
			}
		}
		if ok, err := s.SetInDoubtEffectResult(ctx, tx, "k1", json.RawMessage(`{"n": 1}`)); err != nil || ok {
			t.Fatalf("SetInDoubtEffectResult before the effect is in doubt = %v, %v; want false", ok, err)
		}
		if ok, err := s.AppendInDoubtEffectNote(ctx, tx, "k1", "late"); err != nil || ok {
			t.Fatalf("AppendInDoubtEffectNote without a dead letter = %v, %v; want false", ok, err)
		}
		if _, ok, err := s.MarkEffectInDoubt(ctx, tx, "k1", tok, "timeout"); err != nil || !ok {
			t.Fatal(ok, err)
		}
		note := "timeout"
		id, ok, err := s.InsertDeadLetter(ctx, tx, DeadLetterRow{
			Kind: "effect_in_doubt", Ref: "k1", RunID: &run.ID, Raw: json.RawMessage(`{}`), Note: &note, CreatedAt: t0,
		})
		if err != nil || !ok {
			t.Fatal(ok, err)
		}
		dlID = id

		if ok, err := s.SetInDoubtEffectResult(ctx, tx, "k1", json.RawMessage(`{"n": 1}`)); err != nil || !ok {
			t.Fatalf("SetInDoubtEffectResult = %v, %v; want true", ok, err)
		}
		if ok, err := s.SetInDoubtEffectResult(ctx, tx, "k1", json.RawMessage(`{"n": 2}`)); err != nil || ok {
			t.Fatalf("second SetInDoubtEffectResult = %v, %v; want false, the first result wins", ok, err)
		}
		if ok, err := s.SetInDoubtEffectResult(ctx, tx, "claimed", json.RawMessage(`{}`)); err != nil || ok {
			t.Fatalf("SetInDoubtEffectResult of a claimed effect = %v, %v; want false", ok, err)
		}
		if ok, err := s.AppendInDoubtEffectNote(ctx, tx, "k1", "late success"); err != nil || !ok {
			t.Fatalf("AppendInDoubtEffectNote = %v, %v; want true", ok, err)
		}
	})
	got := getEffect(t, s, "k1")
	if !got.InDoubt || got.DoneAt != nil || string(got.Result) != `{"n": 1}` {
		t.Fatalf("effect = %+v (result %s); want it in doubt with the first result", got, got.Result)
	}
	inTx(t, s, func(tx Tx) {
		dl, err := s.GetDeadLetter(ctx, tx, dlID)
		if err != nil {
			t.Fatal(err)
		}
		if dl.Note == nil || *dl.Note != "timeout\nlate success" {
			t.Fatalf("dead letter note = %v", dl.Note)
		}
	})
}
