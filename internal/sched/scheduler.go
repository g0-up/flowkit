package sched

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/store"
)

// hardStopTimeout bounds how long Stop waits for cancelled jobs to return
// once a graceful stop ran out of time.
const hardStopTimeout = 10 * time.Second

// Client is flowkit's River client with its worker bundle.
type Client struct {
	river *river.Client[pgx.Tx]
	sched *Scheduler
}

// New builds a client that is not started. Jobs can be enqueued before Start.
func New(o Options, h Handlers) (*Client, error) {
	if h == nil {
		return nil, errors.New("flowkit sched: nil handlers")
	}
	if err := o.validate(); err != nil {
		return nil, err
	}
	workers, err := newWorkers(h, o.Timing)
	if err != nil {
		return nil, err
	}
	rc, err := river.NewClient(riverpgxv5.New(o.Pool), riverConfig(o, workers))
	if err != nil {
		return nil, fmt.Errorf("flowkit sched: create River client: %w", err)
	}
	return &Client{river: rc, sched: &Scheduler{client: rc, store: o.Store, clock: o.Clock}}, nil
}

// ID returns the client's River ID, which River records as leader_id
// while this client leads.
func (c *Client) ID() string { return c.river.ID() }

// Scheduler returns the enqueue side of the client.
func (c *Client) Scheduler() *Scheduler { return c.sched }

// Start starts fetching and working jobs. Cancelling ctx stops the client
// and cancels running jobs.
func (c *Client) Start(ctx context.Context) error {
	if err := c.river.Start(ctx); err != nil {
		return fmt.Errorf("flowkit sched: start: %w", err)
	}
	return nil
}

// Stop stops fetching and waits for running jobs to finish. When ctx ends
// first, it cancels the running jobs and waits up to hardStopTimeout for
// them to return; it then reports that the graceful stop was cut short.
func (c *Client) Stop(ctx context.Context) error {
	err := c.river.Stop(ctx)
	if err == nil {
		return nil
	}
	if ctx.Err() == nil {
		return fmt.Errorf("flowkit sched: stop: %w", err)
	}
	hctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), hardStopTimeout)
	defer cancel()
	if herr := c.river.StopAndCancel(hctx); herr != nil {
		return fmt.Errorf("flowkit sched: stop: running jobs did not return after cancellation: %w", errors.Join(err, herr))
	}
	return fmt.Errorf("flowkit sched: stop: graceful stop cut short, running jobs were cancelled: %w", err)
}

// Scheduler enqueues flowkit jobs inside store transactions, so a job exists
// exactly when the transaction that wants it commits. It accepts only
// transactions of the Engine's own store: a Tx of another store, even on the
// same pool, gets store.ErrForeignTx, so no job lands in another schema.
//
// An error from River's insert can leave the transaction aborted; the
// caller must roll it back.
type Scheduler struct {
	client *river.Client[pgx.Tx]
	store  *store.PgStore
	clock  Clock
}

func (s *Scheduler) insert(ctx context.Context, tx store.Tx, args river.JobArgs, opts *river.InsertOpts) error {
	ptx, err := s.store.PgxTx(tx)
	if err != nil {
		return fmt.Errorf("flowkit sched: enqueue %s: %w", args.Kind(), err)
	}
	if _, err := s.client.InsertTx(ctx, ptx, args, opts); err != nil {
		return fmt.Errorf("flowkit sched: enqueue %s: %w", args.Kind(), err)
	}
	return nil
}

// EnqueueRoute enqueues routing of a stored event.
func (s *Scheduler) EnqueueRoute(ctx context.Context, tx store.Tx, eventID int64) error {
	return s.insert(ctx, tx, RouteEventArgs{EventID: eventID}, nil)
}

// EnqueuePump enqueues an immediate pump of a run.
func (s *Scheduler) EnqueuePump(ctx context.Context, tx store.Tx, runID ids.UUID) error {
	return s.insert(ctx, tx, PumpRunArgs{RunID: runID}, nil)
}

// EnqueuePumpMany enqueues an immediate pump of each run in one insert.
func (s *Scheduler) EnqueuePumpMany(ctx context.Context, tx store.Tx, runIDs []ids.UUID) error {
	if len(runIDs) == 0 {
		return nil
	}
	ptx, err := s.store.PgxTx(tx)
	if err != nil {
		return fmt.Errorf("flowkit sched: enqueue %s: %w", KindPumpRun, err)
	}
	params := make([]river.InsertManyParams, len(runIDs))
	for i, id := range runIDs {
		params[i] = river.InsertManyParams{Args: PumpRunArgs{RunID: id}}
	}
	if _, err := s.client.InsertManyTx(ctx, ptx, params); err != nil {
		return fmt.Errorf("flowkit sched: enqueue %s: %w", KindPumpRun, err)
	}
	return nil
}

// SnoozePump enqueues a pump of a run that becomes due after d, measured on
// the flowkit clock.
func (s *Scheduler) SnoozePump(ctx context.Context, tx store.Tx, runID ids.UUID, d time.Duration) error {
	return s.insert(ctx, tx, PumpRunArgs{RunID: runID}, &river.InsertOpts{ScheduledAt: s.clock.Now().Add(d)})
}

// ScheduleTimeout enqueues the timeout of a wait, due at args.DueAt.
func (s *Scheduler) ScheduleTimeout(ctx context.Context, tx store.Tx, args WaitTimeoutArgs) error {
	if args.DueAt.IsZero() {
		return errors.New("flowkit sched: wait timeout without a due time")
	}
	return s.insert(ctx, tx, args, &river.InsertOpts{ScheduledAt: args.DueAt})
}
