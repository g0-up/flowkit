package store

import (
	"context"
	"time"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/store/db"
)

// SerialKeyRef names the run queue of one serial key.
type SerialKeyRef struct {
	DefName   string
	SerialKey string
}

// StuckRunQuery selects running runs that lost their pump.
type StuckRunQuery struct {
	// UpdatedBefore: the run has not been written since.
	UpdatedBefore time.Time
	// ClaimedAfter: an effect of the run claimed since then is still
	// running, so the run is not stuck.
	ClaimedAfter time.Time
	// PumpKind is the job kind of a pump; a run with a live pump job is
	// not stuck.
	PumpKind string
	Limit    int
}

func (s *PgStore) ListStaleLookbacks(ctx context.Context, tx Tx, before time.Time, limit int) ([]ids.UUID, error) {
	q, err := s.q(tx)
	if err != nil {
		return nil, err
	}
	n, err := toInt32("limit", limit)
	if err != nil {
		return nil, err
	}
	out, err := q.ListStaleLookbacks(ctx, db.ListStaleLookbacksParams{UpdatedBefore: before, MaxRows: n})
	if err != nil {
		return nil, wrapErr("list stale look-backs", err)
	}
	return out, nil
}

func (s *PgStore) ListOrphanedQueues(ctx context.Context, tx Tx, limit int) ([]SerialKeyRef, error) {
	q, err := s.q(tx)
	if err != nil {
		return nil, err
	}
	n, err := toInt32("limit", limit)
	if err != nil {
		return nil, err
	}
	rows, err := q.ListOrphanedQueues(ctx, n)
	if err != nil {
		return nil, wrapErr("list orphaned queues", err)
	}
	out := make([]SerialKeyRef, len(rows))
	for i, r := range rows {
		out[i] = SerialKeyRef{DefName: r.DefName, SerialKey: r.SerialKey}
	}
	return out, nil
}

// listStuckRuns reads River's job table, which sqlc does not know, so it is
// written here. A job is live until it completes, is cancelled or is
// discarded.
const listStuckRuns = `SELECT r.id
FROM /* TEMPLATE: schema */runs r
JOIN /* TEMPLATE: schema */definitions d ON d.name = r.def_name
WHERE r.state = 'running' AND NOT r.terminal AND r.updated_at < $1 AND d.enabled
  AND NOT EXISTS (
    SELECT 1 FROM /* TEMPLATE: schema */executed_effects e
    WHERE e.run_id = r.id AND e.done_at IS NULL
      AND (e.in_doubt OR (e.claim_token IS NOT NULL AND e.claimed_at >= $2))
  )
  AND NOT EXISTS (
    SELECT 1 FROM /* TEMPLATE: schema */river_job j
    WHERE j.kind = $3 AND j.state IN ('available', 'pending', 'retryable', 'running', 'scheduled')
      AND j.args->>'run_id' = r.id::text
  )
ORDER BY r.updated_at, r.id
LIMIT $4`

// ListStuckRuns returns up to q.Limit running runs of enabled definitions
// not written since q.UpdatedBefore that have no live pump job, no effect
// in doubt and no effect claimed since q.ClaimedAfter, oldest first.
func (s *PgStore) ListStuckRuns(ctx context.Context, tx Tx, q StuckRunQuery) ([]ids.UUID, error) {
	d, err := s.dbtx(tx)
	if err != nil {
		return nil, err
	}
	rows, err := d.Query(ctx, listStuckRuns, q.UpdatedBefore, q.ClaimedAfter, q.PumpKind, q.Limit)
	if err != nil {
		return nil, wrapErr("list stuck runs", err)
	}
	defer rows.Close()
	var out []ids.UUID
	for rows.Next() {
		var id ids.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, wrapErr("list stuck runs", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapErr("list stuck runs", err)
	}
	return out, nil
}
