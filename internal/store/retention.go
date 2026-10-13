package store

import (
	"context"
	"time"

	"github.com/g0-up/flowkit/internal/store/db"
)

// SummarizeRuns moves up to limit terminal runs that ended before
// endedBefore to run_summaries and deletes every other row of theirs:
// steps, consumed events, subscriptions, effects and the run itself. It
// returns how many runs it summarised. Runs another transaction holds are
// skipped.
func (s *PgStore) SummarizeRuns(ctx context.Context, tx Tx, endedBefore time.Time, limit int) (int, error) {
	q, err := s.q(tx)
	if err != nil {
		return 0, err
	}
	n, err := toInt32("limit", limit)
	if err != nil {
		return 0, err
	}
	runIDs, err := q.LockExpiredTerminalRuns(ctx, db.LockExpiredTerminalRunsParams{EndedBefore: endedBefore, MaxRows: n})
	if err != nil {
		return 0, wrapErr("lock expired runs", err)
	}
	if len(runIDs) == 0 {
		return 0, nil
	}
	if err := q.InsertRunSummaries(ctx, runIDs); err != nil {
		return 0, wrapErr("insert run summaries", err)
	}
	if err := q.DeleteRunStepsOf(ctx, runIDs); err != nil {
		return 0, wrapErr("delete run steps", err)
	}
	if err := q.DeleteConsumedEventsOf(ctx, runIDs); err != nil {
		return 0, wrapErr("delete consumed events", err)
	}
	if err := q.DeleteSubscriptionsOf(ctx, runIDs); err != nil {
		return 0, wrapErr("delete subscriptions", err)
	}
	if err := q.DeleteEffectsOf(ctx, runIDs); err != nil {
		return 0, wrapErr("delete effects", err)
	}
	deleted, err := q.DeleteTerminalRuns(ctx, runIDs)
	if err != nil {
		return 0, wrapErr("delete runs", err)
	}
	return int(deleted), nil
}

// DeleteExpiredSummaries deletes up to limit run summaries of runs that
// ended before endedBefore.
func (s *PgStore) DeleteExpiredSummaries(ctx context.Context, tx Tx, endedBefore time.Time, limit int) (int, error) {
	q, err := s.q(tx)
	if err != nil {
		return 0, err
	}
	n, err := toInt32("limit", limit)
	if err != nil {
		return 0, err
	}
	deleted, err := q.DeleteExpiredSummaries(ctx, db.DeleteExpiredSummariesParams{EndedBefore: endedBefore, MaxRows: n})
	if err != nil {
		return 0, wrapErr("delete expired summaries", err)
	}
	return int(deleted), nil
}

// DeleteExpiredEvents deletes up to limit events stored before
// insertedBefore that no run, consumed event or queued trigger references.
func (s *PgStore) DeleteExpiredEvents(ctx context.Context, tx Tx, insertedBefore time.Time, limit int) (int, error) {
	q, err := s.q(tx)
	if err != nil {
		return 0, err
	}
	n, err := toInt32("limit", limit)
	if err != nil {
		return 0, err
	}
	deleted, err := q.DeleteExpiredEvents(ctx, db.DeleteExpiredEventsParams{InsertedBefore: insertedBefore, MaxRows: n})
	if err != nil {
		return 0, wrapErr("delete expired events", err)
	}
	return int(deleted), nil
}

// DeleteResolvedDeadLetters deletes up to limit dead letters resolved
// before resolvedBefore. Open dead letters are never deleted.
func (s *PgStore) DeleteResolvedDeadLetters(ctx context.Context, tx Tx, resolvedBefore time.Time, limit int) (int, error) {
	q, err := s.q(tx)
	if err != nil {
		return 0, err
	}
	n, err := toInt32("limit", limit)
	if err != nil {
		return 0, err
	}
	deleted, err := q.DeleteResolvedDeadLetters(ctx, db.DeleteResolvedDeadLettersParams{ResolvedBefore: &resolvedBefore, MaxRows: n})
	if err != nil {
		return 0, wrapErr("delete resolved dead letters", err)
	}
	return int(deleted), nil
}
