package store

import (
	"context"
	"time"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/store/db"
)

func (s *PgStore) ListTriggeredDefinitions(ctx context.Context, tx Tx, eventType string) ([]DefinitionRow, error) {
	q, err := s.q(tx)
	if err != nil {
		return nil, err
	}
	rows, err := q.ListTriggeredDefinitions(ctx, &eventType)
	if err != nil {
		return nil, wrapErr("list triggered definitions", err)
	}
	out := make([]DefinitionRow, len(rows))
	for i, r := range rows {
		out[i] = definitionRow(r)
	}
	return out, nil
}

func (s *PgStore) RunExists(ctx context.Context, tx Tx, id ids.UUID) (bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return false, err
	}
	ok, err := q.RunExists(ctx, id)
	if err != nil {
		return false, wrapErr("check run exists", err)
	}
	return ok, nil
}

func (s *PgStore) QueuedEventExists(ctx context.Context, tx Tx, defName string, eventID int64) (bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return false, err
	}
	ok, err := q.QueuedEventExists(ctx, db.QueuedEventExistsParams{DefName: defName, TriggerEventID: eventID})
	if err != nil {
		return false, wrapErr("check queued event", err)
	}
	return ok, nil
}

func (s *PgStore) InsertConsumedEvent(ctx context.Context, tx Tx, c ConsumedEventRow) error {
	q, err := s.q(tx)
	if err != nil {
		return err
	}
	err = q.InsertConsumedEvent(ctx, db.InsertConsumedEventParams{
		RunID: c.RunID, EventID: c.EventID, StepID: c.StepID, At: c.At,
	})
	if err != nil {
		return wrapErr("insert consumed event", err)
	}
	return nil
}

func (s *PgStore) LockSerial(ctx context.Context, tx Tx, defName, serialKey string) error {
	q, err := s.q(tx)
	if err != nil {
		return err
	}
	if _, err := q.LockSerial(ctx, db.LockSerialParams{DefName: defName, SerialKey: serialKey}); err != nil {
		return wrapErr("lock serial key", err)
	}
	return nil
}

func (s *PgStore) LockActiveExclusiveRun(ctx context.Context, tx Tx, defName, serialKey string) (RunRow, error) {
	q, err := s.q(tx)
	if err != nil {
		return RunRow{}, err
	}
	r, err := q.LockActiveExclusiveRun(ctx, db.LockActiveExclusiveRunParams{DefName: defName, SerialKey: &serialKey})
	if err != nil {
		return RunRow{}, wrapErr("lock active exclusive run", err)
	}
	return runRow(r), nil
}

func (s *PgStore) RunQueueNonEmpty(ctx context.Context, tx Tx, defName, serialKey string) (bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return false, err
	}
	ok, err := q.RunQueueNonEmpty(ctx, db.RunQueueNonEmptyParams{DefName: defName, SerialKey: serialKey})
	if err != nil {
		return false, wrapErr("check run queue", err)
	}
	return ok, nil
}

func (s *PgStore) RecentRunExists(ctx context.Context, tx Tx, defName, serialKey string, since time.Time) (bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return false, err
	}
	ok, err := q.RecentRunExists(ctx, db.RecentRunExistsParams{DefName: defName, SerialKey: &serialKey, Since: since})
	if err != nil {
		return false, wrapErr("check recent run", err)
	}
	return ok, nil
}

func (s *PgStore) NewerRunExists(ctx context.Context, tx Tx, defName, serialKey string, eventAt time.Time, eventID int64) (bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return false, err
	}
	ok, err := q.NewerRunExists(ctx, db.NewerRunExistsParams{
		DefName: defName, SerialKey: &serialKey, EventAt: eventAt, EventID: eventID,
	})
	if err != nil {
		return false, wrapErr("check newer run", err)
	}
	return ok, nil
}

func (s *PgStore) InsertRunQueue(ctx context.Context, tx Tx, r RunQueueRow) (bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return false, err
	}
	n, err := q.InsertRunQueue(ctx, db.InsertRunQueueParams{
		DefName: r.DefName, SerialKey: r.SerialKey, TriggerEventID: r.TriggerEventID, EventAt: r.EventAt, CreatedAt: r.CreatedAt,
	})
	if err != nil {
		return false, wrapErr("insert run queue", err)
	}
	return n == 1, nil
}

func (s *PgStore) PeekRunQueue(ctx context.Context, tx Tx, defName, serialKey string) (RunQueueRow, error) {
	q, err := s.q(tx)
	if err != nil {
		return RunQueueRow{}, err
	}
	r, err := q.PeekRunQueue(ctx, db.PeekRunQueueParams{DefName: defName, SerialKey: serialKey})
	if err != nil {
		return RunQueueRow{}, wrapErr("peek run queue", err)
	}
	return RunQueueRow{
		DefName: r.DefName, SerialKey: r.SerialKey, TriggerEventID: r.TriggerEventID, EventAt: r.EventAt, CreatedAt: r.CreatedAt,
	}, nil
}

func (s *PgStore) DeleteRunQueue(ctx context.Context, tx Tx, r RunQueueRow) (bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return false, err
	}
	n, err := q.DeleteRunQueue(ctx, db.DeleteRunQueueParams{
		DefName: r.DefName, SerialKey: r.SerialKey, TriggerEventID: r.TriggerEventID,
	})
	if err != nil {
		return false, wrapErr("delete run queue", err)
	}
	return n == 1, nil
}
