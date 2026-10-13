package sched

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"github.com/g0-up/flowkit/internal/store"
	"github.com/g0-up/flowkit/internal/testhooks"
)

// Clock is the flowkit clock, Config.Clock of the Engine.
type Clock interface {
	Now() time.Time
}

// Options configures a Client.
type Options struct {
	Pool *pgxpool.Pool
	// Store is the Engine's store. The client uses its schema, and the
	// Scheduler accepts only transactions this store began or wrapped.
	Store *store.PgStore
	// InstanceID names the instance; the River client ID is derived from it
	// with a random suffix, so engines sharing an InstanceID never share a
	// leader-election identity.
	InstanceID string
	// Worker subscribes the client to the flowkit queue with MaxWorkers
	// workers. Without it the client subscribes to IdleQueue only and never
	// processes a flowkit job.
	Worker     bool
	MaxWorkers int
	// Janitor makes the client take part in leader election. Without it
	// leader election is disabled and the client runs no periodic job.
	Janitor      bool
	PollInterval time.Duration // zero keeps River's default
	PollOnly     bool
	Timing       Timing
	Logger       *slog.Logger
	// Clock is the flowkit clock. It sets the scheduled time of delayed
	// jobs and is handed to testhooks.RiverTime.
	Clock Clock
}

func (o Options) validate() error {
	switch {
	case o.Pool == nil:
		return errors.New("flowkit sched: nil pool")
	case o.Store == nil:
		return errors.New("flowkit sched: nil store")
	case o.Clock == nil:
		return errors.New("flowkit sched: nil clock")
	case o.Logger == nil:
		return errors.New("flowkit sched: nil logger")
	case o.Worker && o.MaxWorkers < 1:
		return fmt.Errorf("flowkit sched: MaxWorkers %d must be at least 1", o.MaxWorkers)
	}
	return o.Timing.Validate()
}

// newWorkers returns the worker bundle. Every client registers every kind,
// whatever its roles: the leader's rescuer discards a job whose kind its
// bundle lacks, and any janitor can become leader.
func newWorkers(h Handlers, t Timing) (*river.Workers, error) {
	ws := river.NewWorkers()
	errs := []error{
		river.AddWorkerSafely(ws, &worker[RouteEventArgs]{timeout: t.JobTimeout, handle: h.Route}),
		river.AddWorkerSafely(ws, &worker[PumpRunArgs]{timeout: t.PumpJobTimeout, handle: h.Pump}),
		river.AddWorkerSafely(ws, &worker[WaitTimeoutArgs]{timeout: t.JobTimeout, handle: h.Timeout}),
		river.AddWorkerSafely(ws, &worker[JanitorArgs]{timeout: t.JobTimeout, handle: h.Janitor}),
	}
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("flowkit sched: register workers: %w", err)
	}
	return ws, nil
}

// riverConfig maps the options, roles included, to a River configuration.
func riverConfig(o Options, workers *river.Workers) *river.Config {
	queues := map[string]river.QueueConfig{IdleQueue: {MaxWorkers: 1}}
	if o.Worker {
		queues = map[string]river.QueueConfig{Queue: {MaxWorkers: o.MaxWorkers}}
	}
	// River's clock is the hook's time generator when it supplies one.
	var tg rivertype.TimeGenerator
	if h := testhooks.RiverTime; h != nil {
		tg = h(o.Clock)
	}
	riverNow := time.Now
	if tg != nil {
		riverNow = tg.Now
	}
	cfg := &river.Config{
		ID:     clientID(o.InstanceID),
		Schema: o.Store.Schema(),
		Logger: o.Logger,
		Queues: queues,
		// Only janitors elect a leader. The leader alone runs the scheduler,
		// the rescuer and the periodic jobs.
		LeaderElectionDisabled: !o.Janitor,
		PeriodicJobs:           periodicJobs(o.Janitor, riverNow),
		JobTimeout:             o.Timing.JobTimeout,
		RescueStuckJobsAfter:   o.Timing.RescueStuckJobsAfter,
		PollOnly:               o.PollOnly,
		FetchPollInterval:      o.PollInterval,
		// A non-nil empty list disables the leader's daily
		// REINDEX CONCURRENTLY of River's tables; nil would select River's
		// default index list.
		ReindexerIndexNames: []string{},
		Workers:             workers,
	}
	if tg != nil {
		cfg.Test.Time = tg
	}
	return cfg
}

// maxClientIDLen is River's limit on a client ID.
const maxClientIDLen = 100

var unsafeIDChars = regexp.MustCompile(`[^A-Za-z0-9_.:\-]+`)

// clientID derives a River client ID from the instance ID: unsafe characters
// replaced, cut to fit River's limit, and made unique with a random suffix.
func clientID(instanceID string) string {
	var b [6]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never returns an error
	suffix := "_" + hex.EncodeToString(b[:])
	id := unsafeIDChars.ReplaceAllString(instanceID, "_")
	if id == "" {
		id = "flowkit"
	}
	if limit := maxClientIDLen - len(suffix); len(id) > limit {
		id = id[:limit]
	}
	return id + suffix
}
