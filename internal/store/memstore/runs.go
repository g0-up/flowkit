package memstore

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"slices"
	"time"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/store"
)

// runStates are the values the runs CHECK constraint allows, and
// terminalStates those it requires terminal to be set for.
var (
	runStates      = []string{"running", "waiting", "completed", "failed", "cancelled"}
	terminalStates = []string{"completed", "failed", "cancelled"}
)

// checkRun applies the NOT NULL, jsonb and CHECK constraints of runs.
func checkRun(r store.RunRow) error {
	for _, c := range []struct {
		name    string
		v       json.RawMessage
		notNull bool
	}{{"definition", r.Definition, true}, {"frames", r.Frames, true}, {"facts", r.Facts, true}, {"scope_extra", r.ScopeExtra, false}} {
		if err := jsonCol(c.name, c.v, c.notNull); err != nil {
			return err
		}
	}
	if !slices.Contains(runStates, r.State) {
		return checkErr("runs_state_check")
	}
	if r.Terminal != slices.Contains(terminalStates, r.State) {
		return checkErr("runs_check")
	}
	return nil
}

func cmpUUID(a, b ids.UUID) int { return bytes.Compare(a[:], b[:]) }

func runRow(r store.RunRow) store.RunRow {
	r.Definition = cpJSON(r.Definition)
	r.TerminalName = cp(r.TerminalName)
	r.EntityKind = cp(r.EntityKind)
	r.EntityID = cp(r.EntityID)
	r.ScopeExtra = cpJSON(r.ScopeExtra)
	r.SerialKey = cp(r.SerialKey)
	r.WaitStep = cp(r.WaitStep)
	r.Frames = cpJSON(r.Frames)
	r.Facts = cpJSON(r.Facts)
	r.CreatedAt = ts(r.CreatedAt)
	r.UpdatedAt = ts(r.UpdatedAt)
	return r
}

// activeExclusive reports whether r is in the runs_serial_active index.
func activeExclusive(r store.RunRow) bool { return r.Exclusive && !r.Terminal }

// sameKey reports whether r runs under (defName, serialKey). A NULL serial
// key equals nothing, as in SQL.
func sameKey(r store.RunRow, defName, serialKey string) bool {
	return r.DefName == defName && r.SerialKey != nil && *r.SerialKey == serialKey
}

func (s *Store) InsertRun(ctx context.Context, tx store.Tx, r store.RunRow) error {
	t, err := s.use(ctx, tx)
	if err != nil {
		return err
	}
	if err := checkRun(r); err != nil {
		return t.fail("insert run", err)
	}
	if _, ok := t.t.runs[r.ID]; ok {
		return t.fail("insert run", uniqueErr("runs_pkey"))
	}
	if activeExclusive(r) && r.SerialKey != nil {
		for _, o := range t.t.runs {
			if activeExclusive(o) && sameKey(o, r.DefName, *r.SerialKey) {
				return t.fail("insert run", uniqueErr("runs_serial_active"))
			}
		}
	}
	if _, ok := t.t.events[r.TriggerEventID]; !ok {
		return t.fail("insert run", foreignKeyErr("runs_trigger_event_id_fkey"))
	}
	write(t, tabRuns, &t.t.runs)[r.ID] = runRow(r)
	return nil
}

func (s *Store) GetRun(ctx context.Context, tx store.Tx, id ids.UUID) (store.RunRow, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return store.RunRow{}, err
	}
	r, ok := t.t.runs[id]
	if !ok {
		return store.RunRow{}, notFound("get run")
	}
	return runRow(r), nil
}

func (s *Store) RunExists(ctx context.Context, tx store.Tx, id ids.UUID) (bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return false, err
	}
	_, ok := t.t.runs[id]
	return ok, nil
}

func (s *Store) ListRunningRunIDs(ctx context.Context, tx store.Tx, defName string) ([]ids.UUID, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return nil, err
	}
	out := []ids.UUID{}
	for _, r := range t.t.runs {
		if r.DefName == defName && r.State == "running" && !r.Terminal {
			out = append(out, r.ID)
		}
	}
	slices.SortFunc(out, cmpUUID)
	return out, nil
}

func (s *Store) CountPinnedRuns(ctx context.Context, tx store.Tx, defName string) (map[int]int, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return nil, err
	}
	out := map[int]int{}
	for _, r := range t.t.runs {
		if r.DefName == defName && !r.Terminal {
			out[r.DefVersion]++
		}
	}
	return out, nil
}

// updateRun replaces the run with the result of fn when it exists and
// match accepts it. fn returns the new row; its constraints are checked.
func (t *memTx) updateRun(op string, id ids.UUID, match func(store.RunRow) bool, fn func(*store.RunRow)) (bool, error) {
	r, ok := t.t.runs[id]
	if !ok || !match(r) {
		return false, nil
	}
	fn(&r)
	if err := checkRun(r); err != nil {
		return false, t.fail(op, err)
	}
	write(t, tabRuns, &t.t.runs)[id] = runRow(r)
	return true, nil
}

func (s *Store) NextStepSeq(ctx context.Context, tx store.Tx, runID ids.UUID) (int, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return 0, err
	}
	var seq int
	ok, err := t.updateRun("next step seq", runID, func(store.RunRow) bool { return true }, func(r *store.RunRow) {
		r.StepSeq++
		seq = r.StepSeq
	})
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, notFound("next step seq")
	}
	return seq, nil
}

func (s *Store) AppendStep(ctx context.Context, tx store.Tx, st store.StepRow) (int, error) {
	seq, err := s.NextStepSeq(ctx, tx, st.RunID)
	if err != nil {
		return 0, err
	}
	t, err := s.use(ctx, tx)
	if err != nil {
		return 0, err
	}
	if err := jsonCol("snapshot", st.Snapshot, false); err != nil {
		return 0, t.fail("append step", err)
	}
	st.Seq = seq
	m := write(t, tabSteps, &t.t.steps)
	// Clip forces append to copy, so the committed slice is never written.
	m[st.RunID] = append(slices.Clip(m[st.RunID]), stepRow(st))
	return seq, nil
}

func stepRow(st store.StepRow) store.StepRow {
	st.At = ts(st.At)
	st.Detail = cp(st.Detail)
	st.Snapshot = cpJSON(st.Snapshot)
	st.Attempt = cp(st.Attempt)
	st.Error = cp(st.Error)
	return st
}

func (s *Store) ListSteps(ctx context.Context, tx store.Tx, runID ids.UUID) ([]store.StepRow, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return nil, err
	}
	steps := t.t.steps[runID]
	out := make([]store.StepRow, len(steps))
	for i, st := range steps {
		out[i] = stepRow(st)
	}
	return out, nil
}

func (s *Store) StepRecorded(ctx context.Context, tx store.Tx, runID ids.UUID, stepID, kind, detail string) (bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return false, err
	}
	for _, st := range t.t.steps[runID] {
		if st.StepID == stepID && st.Kind == kind && st.Detail != nil && *st.Detail == detail {
			return true, nil
		}
	}
	return false, nil
}

func (s *Store) FinishRun(ctx context.Context, tx store.Tx, f store.FinishRunRow) (bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return false, err
	}
	return t.updateRun("finish run", f.ID, func(r store.RunRow) bool {
		return r.Rev == f.Rev && !r.Terminal
	}, func(r *store.RunRow) {
		name := f.TerminalName
		r.State, r.Terminal, r.TerminalName = f.State, true, &name
		r.WaitStep, r.LookbackPending = nil, false
		r.Rev++
		r.UpdatedAt = f.At
	})
}

func (s *Store) UpdateRun(ctx context.Context, tx store.Tx, u store.RunUpdate) (bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return false, err
	}
	return t.updateRun("update run", u.ID, func(r store.RunRow) bool {
		return r.Rev == u.Rev && r.State == "running"
	}, func(r *store.RunRow) {
		r.Frames, r.Facts = u.Frames, u.Facts
		r.Rev++
		r.UpdatedAt = u.At
	})
}

func (s *Store) EnterWait(ctx context.Context, tx store.Tx, u store.RunUpdate, waitStep string) (int, bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return 0, false, err
	}
	var seq int
	ok, err := t.updateRun("enter wait", u.ID, func(r store.RunRow) bool {
		return r.Rev == u.Rev && r.State == "running"
	}, func(r *store.RunRow) {
		r.State, r.WaitStep = "waiting", &waitStep
		r.WaitSeq++
		seq = r.WaitSeq
		r.LookbackPending = true
		r.Frames, r.Facts = u.Frames, u.Facts
		r.Rev++
		r.UpdatedAt = u.At
	})
	if err != nil || !ok {
		return 0, false, err
	}
	return seq, true, nil
}

func (s *Store) ClearLookback(ctx context.Context, tx store.Tx, runID ids.UUID, waitSeq int) (bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return false, err
	}
	return t.updateRun("clear look-back", runID, func(r store.RunRow) bool {
		return r.State == "waiting" && r.WaitSeq == waitSeq && r.LookbackPending
	}, func(r *store.RunRow) {
		r.LookbackPending = false
	})
}

func (s *Store) ClaimRun(ctx context.Context, tx store.Tx, c store.ClaimRow) (bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return false, err
	}
	return t.updateRun("claim run", c.ID, func(r store.RunRow) bool {
		return r.State == "waiting" && r.WaitStep != nil && *r.WaitStep == c.WaitStep && r.WaitSeq == c.WaitSeq && r.Rev == c.Rev
	}, func(r *store.RunRow) {
		r.State, r.WaitStep, r.LookbackPending = "running", nil, false
		r.Frames = c.Frames
		r.Rev++
		r.UpdatedAt = c.At
	})
}

func (s *Store) InsertSubscription(ctx context.Context, tx store.Tx, sub store.SubscriptionRow) error {
	t, err := s.use(ctx, tx)
	if err != nil {
		return err
	}
	key := subKey{runID: sub.RunID, waitSeq: sub.WaitSeq}
	if _, ok := t.t.subs[key]; ok {
		return t.fail("insert subscription", uniqueErr("subscriptions_pkey"))
	}
	if _, ok := t.t.runs[sub.RunID]; !ok {
		return t.fail("insert subscription", foreignKeyErr("subscriptions_run_id_fkey"))
	}
	sub.CreatedAt = ts(sub.CreatedAt)
	write(t, tabSubs, &t.t.subs)[key] = sub
	return nil
}

func (s *Store) DeleteSubscription(ctx context.Context, tx store.Tx, runID ids.UUID, waitSeq int) error {
	t, err := s.use(ctx, tx)
	if err != nil {
		return err
	}
	key := subKey{runID: runID, waitSeq: waitSeq}
	if _, ok := t.t.subs[key]; ok {
		delete(write(t, tabSubs, &t.t.subs), key)
	}
	return nil
}

func (s *Store) DeleteRunSubscriptions(ctx context.Context, tx store.Tx, runID ids.UUID) error {
	t, err := s.use(ctx, tx)
	if err != nil {
		return err
	}
	t.deleteSubsOf(map[ids.UUID]bool{runID: true})
	return nil
}

func (t *memTx) deleteSubsOf(runs map[ids.UUID]bool) {
	for k := range t.t.subs {
		if runs[k.runID] {
			delete(write(t, tabSubs, &t.t.subs), k)
		}
	}
}

// afterCursor reports whether (at, id) comes after (afterAt, afterID) in
// (inserted_at, id) order.
func afterCursor(at time.Time, id int64, afterAt time.Time, afterID int64) bool {
	return at.After(afterAt) || (at.Equal(afterAt) && id > afterID)
}

func cmpEvents(a, b store.EventRow) int {
	return cmp.Or(a.InsertedAt.Compare(b.InsertedAt), cmp.Compare(a.ID, b.ID))
}

func (s *Store) LookbackPage(ctx context.Context, tx store.Tx, p store.LookbackQuery) ([]store.EventRow, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := t.limit("look-back page", p.Limit); err != nil {
		return nil, err
	}
	afterAt := ts(p.AfterAt)
	out := []store.EventRow{}
	for _, e := range t.t.events {
		if e.Type != p.EventType || !afterCursor(e.InsertedAt, e.ID, afterAt, p.AfterID) {
			continue
		}
		if _, consumed := t.t.consumed[consumedKey{runID: p.RunID, eventID: e.ID}]; consumed {
			continue
		}
		out = append(out, e)
	}
	slices.SortFunc(out, cmpEvents)
	out = out[:min(len(out), p.Limit)]
	for i := range out {
		out[i] = cloneEvent(out[i])
	}
	return out, nil
}

func (s *Store) ListEventSubscriptions(ctx context.Context, tx store.Tx, p store.SubscriptionQuery) ([]store.WaitingRun, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := t.limit("list event subscriptions", p.Limit); err != nil {
		return nil, err
	}
	out := []store.WaitingRun{}
	for _, sub := range t.t.subs {
		if sub.EventType != p.EventType || cmpUUID(sub.RunID, p.AfterRunID) <= 0 {
			continue
		}
		r, ok := t.t.runs[sub.RunID]
		if !ok || r.State != "waiting" || r.WaitSeq != sub.WaitSeq || r.WaitStep == nil || *r.WaitStep != sub.StepID {
			continue
		}
		if _, consumed := t.t.consumed[consumedKey{runID: r.ID, eventID: p.EventID}]; consumed {
			continue
		}
		r = runRow(r)
		out = append(out, store.WaitingRun{
			Sub: store.SubscriptionRow{
				RunID: sub.RunID, WaitSeq: sub.WaitSeq, StepID: sub.StepID, EventType: sub.EventType, CreatedAt: sub.CreatedAt,
			},
			Run: store.RunRow{
				ID: r.ID, DefName: r.DefName, DefVersion: r.DefVersion, TriggerEventID: r.TriggerEventID,
				State: r.State, EntityKind: r.EntityKind, EntityID: r.EntityID, ScopeExtra: r.ScopeExtra,
				SerialKey: r.SerialKey, Exclusive: r.Exclusive, WaitStep: r.WaitStep, WaitSeq: sub.WaitSeq,
				Frames: r.Frames, Rev: r.Rev, CreatedAt: r.CreatedAt,
			},
		})
	}
	slices.SortFunc(out, func(a, b store.WaitingRun) int { return cmpUUID(a.Sub.RunID, b.Sub.RunID) })
	return out[:min(len(out), p.Limit)], nil
}
