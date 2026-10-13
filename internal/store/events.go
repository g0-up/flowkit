package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/g0-up/flowkit/internal/store/db"
)

func (s *PgStore) InsertEvent(ctx context.Context, tx Tx, e EventRow) (int64, bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return 0, false, err
	}
	// The event holding the dedup key can be deleted between the insert and
	// the lookup; then the key is free and one more insert normally takes it.
	for attempt := 0; ; attempt++ {
		id, err := q.InsertEvent(ctx, db.InsertEventParams{
			Type:       e.Type,
			DedupKey:   e.DedupKey,
			Payload:    e.Payload,
			OccurredAt: e.OccurredAt,
			InsertedAt: e.InsertedAt,
		})
		if err == nil {
			return id, true, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) || e.DedupKey == nil {
			return 0, false, wrapErr("insert event", err)
		}
		// The dedup key is taken: return the event that holds it.
		id, err = q.GetEventIDByDedupKey(ctx, db.GetEventIDByDedupKeyParams{Type: e.Type, DedupKey: e.DedupKey})
		if err == nil {
			return id, false, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) || attempt >= insertOrGetRetries {
			return 0, false, wrapErr("find deduplicated event", err)
		}
	}
}

func (s *PgStore) GetEvent(ctx context.Context, tx Tx, id int64) (EventRow, error) {
	q, err := s.q(tx)
	if err != nil {
		return EventRow{}, err
	}
	r, err := q.GetEvent(ctx, id)
	if err != nil {
		return EventRow{}, wrapErr("get event", err)
	}
	return EventRow{
		ID:         r.ID,
		Type:       r.Type,
		DedupKey:   r.DedupKey,
		Payload:    r.Payload,
		OccurredAt: r.OccurredAt,
		InsertedAt: r.InsertedAt,
	}, nil
}
