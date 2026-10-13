package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/g0-up/flowkit/internal/store/db"
)

func (s *PgStore) InsertDeadLetter(ctx context.Context, tx Tx, d DeadLetterRow) (int64, bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return 0, false, err
	}
	// The open dead letter can be resolved between the insert and the
	// lookup; then (kind, ref) is free and one more insert normally takes it.
	for attempt := 0; ; attempt++ {
		id, err := q.InsertDeadLetter(ctx, db.InsertDeadLetterParams{
			Kind:      d.Kind,
			Ref:       d.Ref,
			RunID:     d.RunID,
			EventID:   d.EventID,
			Raw:       d.Raw,
			Note:      d.Note,
			CreatedAt: d.CreatedAt,
		})
		if err == nil {
			return id, true, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return 0, false, wrapErr("insert dead letter", err)
		}
		// An open dead letter already holds (kind, ref).
		id, err = q.GetOpenDeadLetterID(ctx, db.GetOpenDeadLetterIDParams{Kind: d.Kind, Ref: d.Ref})
		if err == nil {
			return id, false, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) || attempt >= insertOrGetRetries {
			return 0, false, wrapErr("find open dead letter", err)
		}
	}
}

func (s *PgStore) GetDeadLetter(ctx context.Context, tx Tx, id int64) (DeadLetterRow, error) {
	q, err := s.q(tx)
	if err != nil {
		return DeadLetterRow{}, err
	}
	r, err := q.GetDeadLetter(ctx, id)
	if err != nil {
		return DeadLetterRow{}, wrapErr("get dead letter", err)
	}
	return deadLetterRow(r), nil
}

// DeadLetterClaim resolves one open dead letter.
type DeadLetterClaim struct {
	ID         int64
	Resolution string // "redrive", "mark_done" or "skip"
	ResolvedBy *string
	Reason     *string
	At         time.Time
}

func (s *PgStore) ClaimDeadLetter(ctx context.Context, tx Tx, c DeadLetterClaim) (DeadLetterRow, bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return DeadLetterRow{}, false, err
	}
	r, err := q.ClaimDeadLetter(ctx, db.ClaimDeadLetterParams{
		ID:            c.ID,
		ResolvedAt:    &c.At,
		Resolution:    &c.Resolution,
		ResolvedBy:    c.ResolvedBy,
		ResolveReason: c.Reason,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return DeadLetterRow{}, false, nil
	}
	if err != nil {
		return DeadLetterRow{}, false, wrapErr("claim dead letter", err)
	}
	return deadLetterRow(r), true, nil
}

func deadLetterRow(r db.DeadLetter) DeadLetterRow {
	return DeadLetterRow{
		ID:            r.ID,
		Kind:          r.Kind,
		Ref:           r.Ref,
		RunID:         r.RunID,
		EventID:       r.EventID,
		Raw:           r.Raw,
		Note:          r.Note,
		CreatedAt:     r.CreatedAt,
		ResolvedAt:    r.ResolvedAt,
		Resolution:    r.Resolution,
		ResolvedBy:    r.ResolvedBy,
		ResolveReason: r.ResolveReason,
	}
}
