package memstore

import (
	"cmp"
	"context"
	"time"

	"github.com/g0-up/flowkit/internal/store"
)

func (s *Store) QueuedEventExists(ctx context.Context, tx store.Tx, defName string, eventID int64) (bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return false, err
	}
	for k := range t.t.runQueue {
		if k.defName == defName && k.eventID == eventID {
			return true, nil
		}
	}
	return false, nil
}

func (s *Store) InsertConsumedEvent(ctx context.Context, tx store.Tx, c store.ConsumedEventRow) error {
	t, err := s.use(ctx, tx)
	if err != nil {
		return err
	}
	if _, ok := t.t.consumed[consumedKey{runID: c.RunID, eventID: c.EventID}]; ok {
		return t.fail("insert consumed event", uniqueErr("consumed_events_pkey"))
	}
	return t.insertConsumed("insert consumed event", c)
}

func (s *Store) InsertConsumedEventIfAbsent(ctx context.Context, tx store.Tx, c store.ConsumedEventRow) (bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return false, err
	}
	if _, ok := t.t.consumed[consumedKey{runID: c.RunID, eventID: c.EventID}]; ok {
		return false, nil
	}
	if err := t.insertConsumed("insert consumed event", c); err != nil {
		return false, err
	}
	return true, nil
}

func (t *memTx) insertConsumed(op string, c store.ConsumedEventRow) error {
	if _, ok := t.t.runs[c.RunID]; !ok {
		return t.fail(op, foreignKeyErr("consumed_events_run_id_fkey"))
	}
	if _, ok := t.t.events[c.EventID]; !ok {
		return t.fail(op, foreignKeyErr("consumed_events_event_id_fkey"))
	}
	c.At = ts(c.At)
	write(t, tabConsumed, &t.t.consumed)[consumedKey{runID: c.RunID, eventID: c.EventID}] = c
	return nil
}

// LockSerial records the gate row; the transaction already excludes every
// other one.
func (s *Store) LockSerial(ctx context.Context, tx store.Tx, defName, serialKey string) error {
	t, err := s.use(ctx, tx)
	if err != nil {
		return err
	}
	key := store.SerialKeyRef{DefName: defName, SerialKey: serialKey}
	if _, ok := t.t.serialLocks[key]; !ok {
		write(t, tabSerialLocks, &t.t.serialLocks)[key] = struct{}{}
	}
	return nil
}

func (s *Store) LockActiveExclusiveRun(ctx context.Context, tx store.Tx, defName, serialKey string) (store.RunRow, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return store.RunRow{}, err
	}
	for _, r := range t.t.runs {
		if activeExclusive(r) && sameKey(r, defName, serialKey) {
			return runRow(r), nil
		}
	}
	return store.RunRow{}, notFound("lock active exclusive run")
}

func (s *Store) RunQueueNonEmpty(ctx context.Context, tx store.Tx, defName, serialKey string) (bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return false, err
	}
	for k := range t.t.runQueue {
		if k.defName == defName && k.serialKey == serialKey {
			return true, nil
		}
	}
	return false, nil
}

func (s *Store) RecentRunExists(ctx context.Context, tx store.Tx, defName, serialKey string, since time.Time) (bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return false, err
	}
	since = ts(since)
	for _, r := range t.t.runs {
		if sameKey(r, defName, serialKey) && r.CreatedAt.After(since) {
			return true, nil
		}
	}
	return false, nil
}

func (s *Store) NewerRunExists(ctx context.Context, tx store.Tx, defName, serialKey string, eventAt time.Time, eventID int64) (bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return false, err
	}
	eventAt = ts(eventAt)
	for _, r := range t.t.runs {
		if !sameKey(r, defName, serialKey) {
			continue
		}
		if e, ok := t.t.events[r.TriggerEventID]; ok && afterCursor(e.InsertedAt, e.ID, eventAt, eventID) {
			return true, nil
		}
	}
	return false, nil
}

func (s *Store) InsertRunQueue(ctx context.Context, tx store.Tx, r store.RunQueueRow) (bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return false, err
	}
	key := queueKey{defName: r.DefName, serialKey: r.SerialKey, eventID: r.TriggerEventID}
	if _, ok := t.t.runQueue[key]; ok {
		return false, nil
	}
	if _, ok := t.t.events[r.TriggerEventID]; !ok {
		return false, t.fail("insert run queue", foreignKeyErr("run_queue_trigger_event_id_fkey"))
	}
	r.EventAt, r.CreatedAt = ts(r.EventAt), ts(r.CreatedAt)
	write(t, tabRunQueue, &t.t.runQueue)[key] = r
	return true, nil
}

func (s *Store) PeekRunQueue(ctx context.Context, tx store.Tx, defName, serialKey string) (store.RunQueueRow, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return store.RunQueueRow{}, err
	}
	var head store.RunQueueRow
	found := false
	for k, r := range t.t.runQueue {
		if k.defName != defName || k.serialKey != serialKey {
			continue
		}
		if !found || cmp.Or(r.EventAt.Compare(head.EventAt), cmp.Compare(r.TriggerEventID, head.TriggerEventID)) < 0 {
			head, found = r, true
		}
	}
	if !found {
		return store.RunQueueRow{}, notFound("peek run queue")
	}
	return head, nil
}

func (s *Store) DeleteRunQueue(ctx context.Context, tx store.Tx, r store.RunQueueRow) (bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return false, err
	}
	key := queueKey{defName: r.DefName, serialKey: r.SerialKey, eventID: r.TriggerEventID}
	if _, ok := t.t.runQueue[key]; !ok {
		return false, nil
	}
	delete(write(t, tabRunQueue, &t.t.runQueue), key)
	return true, nil
}
