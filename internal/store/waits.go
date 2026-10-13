package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/store/db"
)

func (s *PgStore) UpdateRun(ctx context.Context, tx Tx, u RunUpdate) (bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return false, err
	}
	n, err := q.UpdateRun(ctx, db.UpdateRunParams{
		ID: u.ID, Rev: u.Rev, Frames: u.Frames, Facts: u.Facts, UpdatedAt: u.At,
	})
	if err != nil {
		return false, wrapErr("update run", err)
	}
	return n == 1, nil
}

func (s *PgStore) EnterWait(ctx context.Context, tx Tx, u RunUpdate, waitStep string) (int, bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return 0, false, err
	}
	seq, err := q.EnterWait(ctx, db.EnterWaitParams{
		ID: u.ID, Rev: u.Rev, WaitStep: &waitStep, Frames: u.Frames, Facts: u.Facts, UpdatedAt: u.At,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, wrapErr("enter wait", err)
	}
	return int(seq), true, nil
}

func (s *PgStore) ClearLookback(ctx context.Context, tx Tx, runID ids.UUID, waitSeq int) (bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return false, err
	}
	seq, err := toInt32("wait_seq", waitSeq)
	if err != nil {
		return false, err
	}
	n, err := q.ClearLookback(ctx, db.ClearLookbackParams{ID: runID, WaitSeq: seq})
	if err != nil {
		return false, wrapErr("clear look-back", err)
	}
	return n == 1, nil
}

func (s *PgStore) ClaimRun(ctx context.Context, tx Tx, c ClaimRow) (bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return false, err
	}
	seq, err := toInt32("wait_seq", c.WaitSeq)
	if err != nil {
		return false, err
	}
	n, err := q.ClaimRun(ctx, db.ClaimRunParams{
		ID: c.ID, Rev: c.Rev, WaitStep: &c.WaitStep, WaitSeq: seq, Frames: c.Frames, UpdatedAt: c.At,
	})
	if err != nil {
		return false, wrapErr("claim run", err)
	}
	return n == 1, nil
}

func (s *PgStore) InsertSubscription(ctx context.Context, tx Tx, sub SubscriptionRow) error {
	q, err := s.q(tx)
	if err != nil {
		return err
	}
	seq, err := toInt32("wait_seq", sub.WaitSeq)
	if err != nil {
		return err
	}
	err = q.InsertSubscription(ctx, db.InsertSubscriptionParams{
		RunID: sub.RunID, WaitSeq: seq, StepID: sub.StepID, EventType: sub.EventType,
		MatchCel: sub.MatchCEL, CreatedAt: sub.CreatedAt,
	})
	if err != nil {
		return wrapErr("insert subscription", err)
	}
	return nil
}

func (s *PgStore) DeleteSubscription(ctx context.Context, tx Tx, runID ids.UUID, waitSeq int) error {
	q, err := s.q(tx)
	if err != nil {
		return err
	}
	seq, err := toInt32("wait_seq", waitSeq)
	if err != nil {
		return err
	}
	if err := q.DeleteSubscription(ctx, db.DeleteSubscriptionParams{RunID: runID, WaitSeq: seq}); err != nil {
		return wrapErr("delete subscription", err)
	}
	return nil
}

func (s *PgStore) InsertConsumedEventIfAbsent(ctx context.Context, tx Tx, c ConsumedEventRow) (bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return false, err
	}
	n, err := q.InsertConsumedEventIfAbsent(ctx, db.InsertConsumedEventIfAbsentParams{
		RunID: c.RunID, EventID: c.EventID, StepID: c.StepID, At: c.At,
	})
	if err != nil {
		return false, wrapErr("insert consumed event", err)
	}
	return n == 1, nil
}

func (s *PgStore) LookbackPage(ctx context.Context, tx Tx, p LookbackQuery) ([]EventRow, error) {
	q, err := s.q(tx)
	if err != nil {
		return nil, err
	}
	limit, err := toInt32("page size", p.Limit)
	if err != nil {
		return nil, err
	}
	rows, err := q.LookbackPage(ctx, db.LookbackPageParams{
		EventType: p.EventType, AfterAt: p.AfterAt, AfterID: p.AfterID, RunID: p.RunID, PageSize: limit,
	})
	if err != nil {
		return nil, wrapErr("look-back page", err)
	}
	out := make([]EventRow, len(rows))
	for i, r := range rows {
		out[i] = EventRow{
			ID: r.ID, Type: r.Type, DedupKey: r.DedupKey, Payload: r.Payload,
			OccurredAt: r.OccurredAt, InsertedAt: r.InsertedAt,
		}
	}
	return out, nil
}

func (s *PgStore) ListEventSubscriptions(ctx context.Context, tx Tx, p SubscriptionQuery) ([]WaitingRun, error) {
	q, err := s.q(tx)
	if err != nil {
		return nil, err
	}
	limit, err := toInt32("page size", p.Limit)
	if err != nil {
		return nil, err
	}
	rows, err := q.ListEventSubscriptions(ctx, db.ListEventSubscriptionsParams{
		EventType: p.EventType, AfterRunID: p.AfterRunID, EventID: p.EventID, PageSize: limit,
	})
	if err != nil {
		return nil, wrapErr("list event subscriptions", err)
	}
	out := make([]WaitingRun, len(rows))
	for i, r := range rows {
		out[i] = WaitingRun{
			Sub: SubscriptionRow{
				RunID: r.RunID, WaitSeq: int(r.WaitSeq), StepID: r.StepID, EventType: r.EventType, CreatedAt: r.SubscribedAt,
			},
			Run: RunRow{
				ID: r.RunID, DefName: r.DefName, DefVersion: int(r.DefVersion), TriggerEventID: r.TriggerEventID,
				State: r.State, EntityKind: r.EntityKind, EntityID: r.EntityID, ScopeExtra: r.ScopeExtra,
				SerialKey: r.SerialKey, Exclusive: r.Exclusive, WaitStep: r.WaitStep, WaitSeq: int(r.WaitSeq),
				Frames: r.Frames, Rev: r.Rev, CreatedAt: r.CreatedAt,
			},
		}
	}
	return out, nil
}

// readQuerier exposes only the reading half of a read-only transaction.
type readQuerier struct{ tx pgx.Tx }

func (r readQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return r.tx.Query(ctx, sql, args...)
}

func (r readQuerier) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return r.tx.QueryRow(ctx, sql, args...)
}

func (s *PgStore) ReadOnly(ctx context.Context, timeout time.Duration, fn func(ctx context.Context, q ReadQuerier) error) error {
	if timeout <= 0 {
		return fmt.Errorf("flowkit store: read-only timeout %v must be positive", timeout)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return fmt.Errorf("flowkit store: begin read-only: %w", err)
	}
	// The transaction only reads, so it always ends in a rollback.
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	ms := max(timeout.Milliseconds(), 1)
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL statement_timeout = %d", ms)); err != nil {
		return fmt.Errorf("flowkit store: set statement timeout: %w", err)
	}
	return fn(ctx, readQuerier{tx: tx})
}
