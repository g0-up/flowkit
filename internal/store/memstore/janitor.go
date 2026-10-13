package memstore

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/store"
)

// oldestRuns returns the ids of up to limit runs that keep accepts, in
// (updated_at, id) order.
func (t *memTx) oldestRuns(limit int, keep func(store.RunRow) bool) []ids.UUID {
	var rows []store.RunRow
	for _, r := range t.t.runs {
		if keep(r) {
			rows = append(rows, r)
		}
	}
	slices.SortFunc(rows, func(a, b store.RunRow) int {
		return cmp.Or(a.UpdatedAt.Compare(b.UpdatedAt), cmpUUID(a.ID, b.ID))
	})
	out := []ids.UUID{}
	for _, r := range rows[:min(len(rows), limit)] {
		out = append(out, r.ID)
	}
	return out
}

func (s *Store) ListStaleLookbacks(ctx context.Context, tx store.Tx, before time.Time, limit int) ([]ids.UUID, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := t.limit("list stale look-backs", limit); err != nil {
		return nil, err
	}
	before = ts(before)
	return t.oldestRuns(limit, func(r store.RunRow) bool {
		return r.LookbackPending && r.State == "waiting" && r.UpdatedAt.Before(before)
	}), nil
}

// ListStuckRuns reads the live jobs from the job queue given to SetJobs.
func (s *Store) ListStuckRuns(ctx context.Context, tx store.Tx, q store.StuckRunQuery) ([]ids.UUID, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := t.limit("list stuck runs", q.Limit); err != nil {
		return nil, err
	}
	updatedBefore, claimedAfter := ts(q.UpdatedBefore), ts(q.ClaimedAfter)
	busy := map[ids.UUID]bool{}
	for _, e := range t.t.effects {
		if e.DoneAt == nil && (e.InDoubt || (e.ClaimToken != nil && e.ClaimedAt != nil && !e.ClaimedAt.Before(claimedAfter))) {
			busy[e.RunID] = true
		}
	}
	jobs := s.jobView()
	return t.oldestRuns(q.Limit, func(r store.RunRow) bool {
		d, ok := t.t.definitions[r.DefName]
		return r.State == "running" && !r.Terminal && r.UpdatedAt.Before(updatedBefore) &&
			ok && d.Enabled && !busy[r.ID] && (jobs == nil || !jobs.LiveJob(q.PumpKind, r.ID))
	}), nil
}

func (s *Store) ListOrphanedQueues(ctx context.Context, tx store.Tx, limit int) ([]store.SerialKeyRef, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := t.limit("list orphaned queues", limit); err != nil {
		return nil, err
	}
	held := map[store.SerialKeyRef]bool{}
	for _, r := range t.t.runs {
		if activeExclusive(r) && r.SerialKey != nil {
			held[store.SerialKeyRef{DefName: r.DefName, SerialKey: *r.SerialKey}] = true
		}
	}
	seen := map[store.SerialKeyRef]bool{}
	out := []store.SerialKeyRef{}
	for k := range t.t.runQueue {
		key := store.SerialKeyRef{DefName: k.defName, SerialKey: k.serialKey}
		d, ok := t.t.definitions[k.defName]
		if seen[key] || held[key] || !ok || !d.Enabled || d.CurrentVersion == nil {
			continue
		}
		seen[key] = true
		out = append(out, key)
	}
	slices.SortFunc(out, func(a, b store.SerialKeyRef) int {
		return cmp.Or(cmp.Compare(a.DefName, b.DefName), cmp.Compare(a.SerialKey, b.SerialKey))
	})
	return out[:min(len(out), limit)], nil
}

// SummarizeRuns moves up to limit terminal runs that ended before
// endedBefore to the summaries and deletes every other row of theirs.
func (s *Store) SummarizeRuns(ctx context.Context, tx store.Tx, endedBefore time.Time, limit int) (int, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return 0, err
	}
	if err := t.limit("lock expired runs", limit); err != nil {
		return 0, err
	}
	endedBefore = ts(endedBefore)
	runIDs := t.oldestRuns(limit, func(r store.RunRow) bool {
		return r.Terminal && r.UpdatedAt.Before(endedBefore)
	})
	if len(runIDs) == 0 {
		return 0, nil
	}
	gone := map[ids.UUID]bool{}
	for _, id := range runIDs {
		gone[id] = true
		r := t.t.runs[id]
		e, ok := t.t.events[r.TriggerEventID]
		if _, dup := t.t.summaries[id]; !ok || dup {
			continue
		}
		write(t, tabSummaries, &t.t.summaries)[id] = store.RunSummaryRow{
			RunID: r.ID, DefName: r.DefName, DefVersion: r.DefVersion, State: r.State,
			TerminalName: cp(r.TerminalName), StepCount: r.StepSeq, TriggerEventType: e.Type,
			EntityKind: cp(r.EntityKind), EntityID: cp(r.EntityID), StartedAt: r.CreatedAt,
			EndedAt: tsPtr(&r.UpdatedAt),
		}
	}
	steps := write(t, tabSteps, &t.t.steps)
	for id := range gone {
		delete(steps, id)
	}
	deleteWhere(t, tabConsumed, &t.t.consumed, func(k consumedKey) bool { return gone[k.runID] })
	t.deleteSubsOf(gone)
	deleteWhere(t, tabEffects, &t.t.effects, func(k string) bool { return gone[t.t.effects[k].RunID] })
	deleteWhere(t, tabRuns, &t.t.runs, func(id ids.UUID) bool { return gone[id] })
	return len(runIDs), nil
}

// deleteWhere deletes the rows of table m whose key matches.
func deleteWhere[K comparable, V any](t *memTx, table int, m *map[K]V, match func(K) bool) {
	var keys []K
	for k := range *m {
		if match(k) {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return
	}
	w := write(t, table, m)
	for _, k := range keys {
		delete(w, k)
	}
}

func (s *Store) DeleteExpiredSummaries(ctx context.Context, tx store.Tx, endedBefore time.Time, limit int) (int, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return 0, err
	}
	if err := t.limit("delete expired summaries", limit); err != nil {
		return 0, err
	}
	endedBefore = ts(endedBefore)
	var rows []store.RunSummaryRow
	for _, r := range t.t.summaries {
		if _, ok := t.t.definitions[r.DefName]; ok && r.EndedAt.Before(endedBefore) {
			rows = append(rows, r)
		}
	}
	// Postgres picks the rows in index order; any order meets the contract.
	slices.SortFunc(rows, func(a, b store.RunSummaryRow) int {
		return cmp.Or(cmp.Compare(a.DefName, b.DefName), a.EndedAt.Compare(*b.EndedAt), cmpUUID(a.RunID, b.RunID))
	})
	rows = rows[:min(len(rows), limit)]
	if len(rows) == 0 {
		return 0, nil
	}
	w := write(t, tabSummaries, &t.t.summaries)
	for _, r := range rows {
		delete(w, r.RunID)
	}
	return len(rows), nil
}

func (s *Store) DeleteExpiredEvents(ctx context.Context, tx store.Tx, insertedBefore time.Time, limit int) (int, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return 0, err
	}
	if err := t.limit("delete expired events", limit); err != nil {
		return 0, err
	}
	insertedBefore = ts(insertedBefore)
	used := map[int64]bool{}
	for _, r := range t.t.runs {
		used[r.TriggerEventID] = true
	}
	for k := range t.t.consumed {
		used[k.eventID] = true
	}
	for k := range t.t.runQueue {
		used[k.eventID] = true
	}
	var rows []store.EventRow
	for _, e := range t.t.events {
		if e.InsertedAt.Before(insertedBefore) && !used[e.ID] {
			rows = append(rows, e)
		}
	}
	slices.SortFunc(rows, cmpEvents)
	rows = rows[:min(len(rows), limit)]
	if len(rows) == 0 {
		return 0, nil
	}
	w := write(t, tabEvents, &t.t.events)
	for _, e := range rows {
		delete(w, e.ID)
	}
	return len(rows), nil
}

func (s *Store) DeleteResolvedDeadLetters(ctx context.Context, tx store.Tx, resolvedBefore time.Time, limit int) (int, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return 0, err
	}
	if err := t.limit("delete resolved dead letters", limit); err != nil {
		return 0, err
	}
	resolvedBefore = ts(resolvedBefore)
	var rows []store.DeadLetterRow
	for _, d := range t.t.deadLetters {
		if d.ResolvedAt != nil && d.ResolvedAt.Before(resolvedBefore) {
			rows = append(rows, d)
		}
	}
	slices.SortFunc(rows, func(a, b store.DeadLetterRow) int {
		return cmp.Or(a.ResolvedAt.Compare(*b.ResolvedAt), cmp.Compare(a.ID, b.ID))
	})
	rows = rows[:min(len(rows), limit)]
	if len(rows) == 0 {
		return 0, nil
	}
	w := write(t, tabDeadLetters, &t.t.deadLetters)
	for _, d := range rows {
		delete(w, d.ID)
	}
	return len(rows), nil
}

func (s *Store) ListRuns(ctx context.Context, tx store.Tx, q store.RunListQuery) ([]store.RunSummaryRow, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := t.limit("list runs", q.Limit); err != nil {
		return nil, err
	}
	all := make([]store.RunSummaryRow, 0, len(t.t.runs)+len(t.t.summaries))
	for _, r := range t.t.runs {
		row := store.RunSummaryRow{
			RunID: r.ID, DefName: r.DefName, DefVersion: r.DefVersion, State: r.State,
			TerminalName: cp(r.TerminalName), StepCount: r.StepSeq,
			TriggerEventType: t.t.events[r.TriggerEventID].Type,
			EntityKind:       cp(r.EntityKind), EntityID: cp(r.EntityID), StartedAt: r.CreatedAt,
		}
		if r.Terminal {
			row.EndedAt = tsPtr(&r.UpdatedAt)
		}
		all = append(all, row)
	}
	for _, r := range t.t.summaries {
		r.TerminalName, r.EntityKind, r.EntityID, r.EndedAt = cp(r.TerminalName), cp(r.EntityKind), cp(r.EntityID), tsPtr(r.EndedAt)
		all = append(all, r)
	}
	out := []store.RunSummaryRow{}
	for _, r := range all {
		if listed(r, q) {
			out = append(out, r)
		}
	}
	slices.SortFunc(out, func(a, b store.RunSummaryRow) int {
		return cmp.Or(b.StartedAt.Compare(a.StartedAt), cmpUUID(b.RunID, a.RunID))
	})
	return out[:min(len(out), q.Limit)], nil
}

// listed reports whether ListRuns returns r for q.
func listed(r store.RunSummaryRow, q store.RunListQuery) bool {
	switch {
	case q.DefName != nil && r.DefName != *q.DefName,
		len(q.States) > 0 && !slices.Contains(q.States, r.State),
		q.EntityKind != nil && (r.EntityKind == nil || *r.EntityKind != *q.EntityKind),
		q.EntityID != nil && (r.EntityID == nil || *r.EntityID != *q.EntityID),
		q.StartedFrom != nil && r.StartedAt.Before(ts(*q.StartedFrom)),
		q.StartedBefore != nil && !r.StartedAt.Before(ts(*q.StartedBefore)):
		return false
	}
	if q.AfterAt == nil {
		return true
	}
	// (started_at, run_id) < (after_at, after_id): a NULL after_id makes
	// the comparison of an equal started_at NULL, which filters the row.
	at := ts(*q.AfterAt)
	if q.AfterID == nil {
		return r.StartedAt.Before(at)
	}
	return r.StartedAt.Before(at) || (r.StartedAt.Equal(at) && cmpUUID(r.RunID, *q.AfterID) < 0)
}

// ReadGauges reads the queue depth from the job queue given to SetJobs. The
// simulator has no leader election, so LeaderID is empty.
func (s *Store) ReadGauges(ctx context.Context, tx store.Tx, queue string) (store.Gauges, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return store.Gauges{}, err
	}
	g := store.Gauges{OpenDeadLetters: map[string]int{}}
	for _, d := range t.t.deadLetters {
		if d.ResolvedAt == nil {
			g.OpenDeadLetters[d.Kind]++
		}
	}
	for _, e := range t.t.effects {
		if r, ok := t.t.runs[e.RunID]; ok && e.InDoubt && e.DoneAt == nil && !r.Terminal {
			g.InDoubtOpen++
		}
	}
	if jobs := s.jobView(); jobs != nil {
		g.QueueDepth = jobs.Available(queue)
	}
	return g, nil
}
