package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/store/db"
)

// RunListQuery is one keyset page of runs, newest first. Nil and empty
// fields do not filter.
type RunListQuery struct {
	DefName       *string
	States        []string
	EntityKind    *string
	EntityID      *string
	StartedFrom   *time.Time // inclusive
	StartedBefore *time.Time // exclusive
	// AfterAt and AfterID are the (StartedAt, RunID) of the last run of
	// the previous page; both nil on the first page.
	AfterAt *time.Time
	AfterID *ids.UUID
	Limit   int
}

// RunSummaryRow is a run as listed: a live run or a run_summaries row.
type RunSummaryRow struct {
	RunID            ids.UUID
	DefName          string
	DefVersion       int
	State            string
	TerminalName     *string
	StepCount        int
	TriggerEventType string
	EntityKind       *string
	EntityID         *string
	StartedAt        time.Time
	EndedAt          *time.Time // nil while the run is not terminal
}

// ListRuns returns one page of live and summarised runs in (StartedAt,
// RunID) descending order.
func (s *PgStore) ListRuns(ctx context.Context, tx Tx, p RunListQuery) ([]RunSummaryRow, error) {
	q, err := s.q(tx)
	if err != nil {
		return nil, err
	}
	n, err := toInt32("limit", p.Limit)
	if err != nil {
		return nil, err
	}
	states := p.States
	if states == nil {
		states = []string{}
	}
	rows, err := q.ListRuns(ctx, db.ListRunsParams{
		DefName:       p.DefName,
		States:        states,
		EntityKind:    p.EntityKind,
		EntityID:      p.EntityID,
		StartedFrom:   p.StartedFrom,
		StartedBefore: p.StartedBefore,
		AfterAt:       p.AfterAt,
		AfterID:       p.AfterID,
		MaxRows:       n,
	})
	if err != nil {
		return nil, wrapErr("list runs", err)
	}
	out := make([]RunSummaryRow, len(rows))
	for i, r := range rows {
		out[i] = RunSummaryRow{
			RunID:            r.RunID,
			DefName:          r.DefName,
			DefVersion:       int(r.DefVersion),
			State:            r.State,
			TerminalName:     r.TerminalName,
			StepCount:        int(r.StepCount),
			TriggerEventType: r.TriggerEventType,
			EntityKind:       r.EntityKind,
			EntityID:         r.EntityID,
			StartedAt:        r.StartedAt,
		}
		if r.Terminal {
			at := r.LastAt
			out[i].EndedAt = &at
		}
	}
	return out, nil
}

// Gauges are the instance-wide figures the metrics report.
type Gauges struct {
	// OpenDeadLetters counts unresolved dead letters by kind.
	OpenDeadLetters map[string]int
	// InDoubtOpen counts unresolved in-doubt effects of non-terminal runs.
	InDoubtOpen int
	// QueueDepth counts available jobs of queue.
	QueueDepth int
	// LeaderID is the client id in River's leader row; empty when there
	// is none. A leader that died keeps its row until the next election,
	// but reports no metrics either.
	LeaderID string
}

const (
	countOpenDeadLetters = `SELECT kind, count(*) FROM /* TEMPLATE: schema */dead_letters
WHERE resolved_at IS NULL GROUP BY kind`
	countInDoubtOpen = `SELECT count(*) FROM /* TEMPLATE: schema */executed_effects e
JOIN /* TEMPLATE: schema */runs r ON r.id = e.run_id
WHERE e.in_doubt AND e.done_at IS NULL AND NOT r.terminal`
	countAvailableJobs = `SELECT count(*) FROM /* TEMPLATE: schema */river_job
WHERE queue = $1 AND state = 'available'`
	// River keeps at most one row, written by the elected leader.
	readLeader = `SELECT leader_id FROM /* TEMPLATE: schema */river_leader`
)

// ReadGauges reads the gauges for jobs of queue.
func (s *PgStore) ReadGauges(ctx context.Context, tx Tx, queue string) (Gauges, error) {
	d, err := s.dbtx(tx)
	if err != nil {
		return Gauges{}, err
	}
	g := Gauges{OpenDeadLetters: map[string]int{}}
	rows, err := d.Query(ctx, countOpenDeadLetters)
	if err != nil {
		return Gauges{}, wrapErr("count open dead letters", err)
	}
	for rows.Next() {
		var kind string
		var n int
		if err := rows.Scan(&kind, &n); err != nil {
			rows.Close()
			return Gauges{}, wrapErr("count open dead letters", err)
		}
		g.OpenDeadLetters[kind] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Gauges{}, wrapErr("count open dead letters", err)
	}
	if err := d.QueryRow(ctx, countInDoubtOpen).Scan(&g.InDoubtOpen); err != nil {
		return Gauges{}, wrapErr("count in-doubt effects", err)
	}
	if err := d.QueryRow(ctx, countAvailableJobs, queue).Scan(&g.QueueDepth); err != nil {
		return Gauges{}, wrapErr("count available jobs", err)
	}
	err = d.QueryRow(ctx, readLeader).Scan(&g.LeaderID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return Gauges{}, wrapErr("read leader", err)
	}
	return g, nil
}
