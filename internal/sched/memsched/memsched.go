// Package memsched runs flowkit's jobs in memory on a virtual clock, in
// place of River, for the simulator. It enqueues through memstore
// transactions, so a job exists only once its transaction commits, and it
// runs a job only when Advance or RunDue is called: the clock never moves by
// itself.
//
// It follows what the Engine relies on from River: jobs run in order of due
// time, then of enqueue; a snoozed job runs again after its delay without
// using up an attempt; a failed or panicking job is retried with River's
// default backoff of attempt^4 seconds until its attempts run out; each job
// runs under its kind's timeout on the system clock; and the janitor is a
// periodic job that runs on start and every sched.JanitorInterval after,
// with missed passes skipped.
package memsched

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/sched"
	"github.com/g0-up/flowkit/internal/store"
	"github.com/g0-up/flowkit/internal/store/memstore"
)

const (
	// maxAttempts is River's default number of attempts of a job.
	maxAttempts = 25
	// maxJobsPerRun bounds the jobs one RunDue runs, so a job that keeps
	// snoozing for zero time ends the test instead of hanging it.
	maxJobsPerRun = 100_000
)

// Clock is the virtual clock the scheduler reads and moves.
type Clock interface {
	Now() time.Time
	Set(t time.Time)
}

// Options configures a Scheduler.
type Options struct {
	// Store is the store whose transactions carry enqueued jobs. Required.
	Store *memstore.Store
	// Clock is the virtual clock. Required.
	Clock Clock
	// Logger receives retries and discarded jobs. Required.
	Logger *slog.Logger
	// Interrupt, when set, is called after every job. A non-nil error stops
	// RunDue and Advance, which return it.
	Interrupt func() error
}

// Scheduler queues and runs flowkit jobs. Its enqueue methods implement the
// interfaces the interpreter and the executor enqueue through, and it
// implements memstore.Jobs so the janitor and the gauges see its queue.
type Scheduler struct {
	o Options

	mu      sync.Mutex
	h       sched.Handlers
	timing  sched.Timing
	seq     int64
	pending []*job
	running *job
	// nextJanitor is when the next janitor pass is due; zero before Start.
	nextJanitor time.Time
}

// job is a queued job. args is one of the sched args types.
type job struct {
	seq         int64
	kind        string
	args        any
	due         time.Time
	attempt     int // attempts made so far
	maxAttempts int
}

// New returns a Scheduler that runs no job until Start.
func New(o Options) (*Scheduler, error) {
	switch {
	case o.Store == nil:
		return nil, errors.New("flowkit memsched: nil store")
	case o.Clock == nil:
		return nil, errors.New("flowkit memsched: nil clock")
	case o.Logger == nil:
		return nil, errors.New("flowkit memsched: nil logger")
	}
	return &Scheduler{o: o}, nil
}

// Start hands jobs to h from now on, each under its kind's timeout from
// timing, and makes the first janitor pass due now. It sets the store's
// Jobs to s.
func (s *Scheduler) Start(h sched.Handlers, timing sched.Timing) error {
	if h == nil {
		return errors.New("flowkit memsched: nil handlers")
	}
	if err := timing.Validate(); err != nil {
		return fmt.Errorf("flowkit memsched: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.h != nil {
		return errors.New("flowkit memsched: already started")
	}
	s.h, s.timing = h, timing
	s.nextJanitor = s.o.Clock.Now()
	s.o.Store.SetJobs(s)
	return nil
}

// EnqueueRoute enqueues routing of a stored event when tx commits.
func (s *Scheduler) EnqueueRoute(_ context.Context, tx store.Tx, eventID int64) error {
	return s.enqueue(tx, sched.KindRouteEvent, sched.RouteEventArgs{EventID: eventID}, time.Time{})
}

// EnqueuePump enqueues an immediate pump of a run when tx commits.
func (s *Scheduler) EnqueuePump(_ context.Context, tx store.Tx, runID ids.UUID) error {
	return s.enqueue(tx, sched.KindPumpRun, sched.PumpRunArgs{RunID: runID}, time.Time{})
}

// EnqueuePumpMany enqueues an immediate pump of each run when tx commits.
func (s *Scheduler) EnqueuePumpMany(_ context.Context, tx store.Tx, runIDs []ids.UUID) error {
	for _, id := range runIDs {
		if err := s.enqueue(tx, sched.KindPumpRun, sched.PumpRunArgs{RunID: id}, time.Time{}); err != nil {
			return err
		}
	}
	return nil
}

// SnoozePump enqueues a pump of a run that becomes due after d, measured on
// the virtual clock.
func (s *Scheduler) SnoozePump(_ context.Context, tx store.Tx, runID ids.UUID, d time.Duration) error {
	return s.enqueue(tx, sched.KindPumpRun, sched.PumpRunArgs{RunID: runID}, s.o.Clock.Now().Add(d))
}

// ScheduleTimeout enqueues the timeout of a wait, due at args.DueAt.
func (s *Scheduler) ScheduleTimeout(_ context.Context, tx store.Tx, args sched.WaitTimeoutArgs) error {
	if args.DueAt.IsZero() {
		return errors.New("flowkit memsched: wait timeout without a due time")
	}
	return s.enqueue(tx, sched.KindWaitTimeout, args, args.DueAt)
}

// enqueue queues a job of kind when tx commits. A zero due makes it due at
// the commit's virtual time.
func (s *Scheduler) enqueue(tx store.Tx, kind string, args any, due time.Time) error {
	err := s.o.Store.AfterCommit(tx, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		at := due
		if at.IsZero() {
			at = s.o.Clock.Now()
		}
		s.push(&job{kind: kind, args: args, due: at, maxAttempts: maxAttempts})
	})
	if err != nil {
		return fmt.Errorf("flowkit memsched: enqueue %s: %w", kind, err)
	}
	return nil
}

// push queues j as the latest job. s.mu must be held.
func (s *Scheduler) push(j *job) {
	s.seq++
	j.seq = s.seq
	s.pending = append(s.pending, j)
}

// Advance moves the clock forward by d and runs every job due on the way:
// it steps the clock to the due time of each queued job up to now+d, runs
// what is due there, and ends at now+d with the jobs due then run. Jobs the
// run enqueues for a time within the window run too.
func (s *Scheduler) Advance(ctx context.Context, d time.Duration) error {
	if d < 0 {
		return fmt.Errorf("flowkit memsched: Advance(%s): the clock only moves forward", d)
	}
	target := s.o.Clock.Now().Add(d)
	for {
		if err := s.RunDue(ctx); err != nil {
			return err
		}
		next, ok := s.nextDue()
		if !ok || next.After(target) {
			break
		}
		if next.After(s.o.Clock.Now()) {
			s.o.Clock.Set(next)
		}
	}
	if target.After(s.o.Clock.Now()) {
		s.o.Clock.Set(target)
	}
	return s.RunDue(ctx)
}

// nextDue returns the earliest due time of a queued job. The janitor is not
// a step of its own: like River's periodic job on a clock that jumps, it
// runs once at each step that passes its due time.
func (s *Scheduler) nextDue() (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var next time.Time
	for _, j := range s.pending {
		if next.IsZero() || j.due.Before(next) {
			next = j.due
		}
	}
	return next, !next.IsZero()
}

// RunDue runs, one at a time, every job due by the clock, including those
// the jobs it runs make due, without moving the clock. It returns early
// with the error of Interrupt or of ctx.
func (s *Scheduler) RunDue(ctx context.Context) error {
	s.mu.Lock()
	started := s.h != nil
	s.mu.Unlock()
	if !started {
		return errors.New("flowkit memsched: not started")
	}
	for n := 0; ; n++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("flowkit memsched: %w", err)
		}
		if n == maxJobsPerRun {
			return fmt.Errorf("flowkit memsched: %d jobs ran without the queue draining at %s; a job keeps becoming due",
				n, s.o.Clock.Now().Format(time.RFC3339Nano))
		}
		j := s.take()
		if j == nil {
			return nil
		}
		s.work(ctx, j)
		if s.o.Interrupt != nil {
			if err := s.o.Interrupt(); err != nil {
				return err
			}
		}
	}
}

// take removes the next due job from the queue and marks it running, or
// returns nil when none is due. It enqueues the janitor pass that is due.
func (s *Scheduler) take() *job {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.o.Clock.Now()
	if !s.nextJanitor.After(now) {
		// A failed pass is not retried: the next one redoes every step.
		s.push(&job{kind: sched.KindJanitor, args: sched.JanitorArgs{}, due: s.nextJanitor, maxAttempts: 1})
		s.nextJanitor = s.nextJanitor.Add(sched.JanitorInterval)
		if s.nextJanitor.Before(now) {
			s.nextJanitor = now.Add(sched.JanitorInterval)
		}
	}
	best := -1
	for i, j := range s.pending {
		if j.due.After(now) {
			continue
		}
		if best < 0 || j.due.Before(s.pending[best].due) ||
			(j.due.Equal(s.pending[best].due) && j.seq < s.pending[best].seq) {
			best = i
		}
	}
	if best < 0 {
		return nil
	}
	j := s.pending[best]
	s.pending = slices.Delete(s.pending, best, best+1)
	s.running = j
	return j
}

// work runs j and queues it again when it snoozes or fails with attempts
// left.
func (s *Scheduler) work(ctx context.Context, j *job) {
	timeout := s.timing.JobTimeout
	if j.kind == sched.KindPumpRun {
		timeout = s.timing.PumpJobTimeout
	}
	jctx, cancel := context.WithTimeout(ctx, timeout)
	res, panicked := s.call(jctx, j)
	cancel()

	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = nil
	now := s.o.Clock.Now()
	var cause error
	switch {
	case panicked != nil:
		cause = panicked
	case res.Err() != nil:
		cause = res.Err()
	default:
		if delay, snoozed := res.SnoozeDelay(); snoozed {
			j.due = now.Add(delay)
			s.push(j)
		}
		return
	}
	j.attempt++
	if j.attempt >= j.maxAttempts {
		s.o.Logger.Error("flowkit memsched: job discarded after its last attempt",
			"kind", j.kind, "args", fmt.Sprintf("%+v", j.args), "attempts", j.attempt, "error", cause)
		return
	}
	j.due = now.Add(retryDelay(j.attempt))
	s.o.Logger.Warn("flowkit memsched: job failed; retrying",
		"kind", j.kind, "args", fmt.Sprintf("%+v", j.args), "attempt", j.attempt, "retry_at", j.due, "error", cause)
	s.push(j)
}

// call hands j to its handler, turning a panic into an error.
func (s *Scheduler) call(ctx context.Context, j *job) (res sched.Result, panicked error) {
	defer func() {
		if v := recover(); v != nil {
			panicked = fmt.Errorf("flowkit memsched: %s job panicked: %v", j.kind, v)
		}
	}()
	switch args := j.args.(type) {
	case sched.RouteEventArgs:
		return s.h.Route(ctx, args), nil
	case sched.PumpRunArgs:
		return s.h.Pump(ctx, args), nil
	case sched.WaitTimeoutArgs:
		return s.h.Timeout(ctx, args), nil
	case sched.JanitorArgs:
		return s.h.Janitor(ctx, args), nil
	default:
		return sched.Retry(fmt.Errorf("flowkit memsched: unknown job args %T", j.args)), nil
	}
}

// retryDelay is River's default backoff after attempt failed attempts,
// without its jitter: attempt^4 seconds.
func retryDelay(attempt int) time.Duration {
	a := time.Duration(attempt)
	return a * a * a * a * time.Second
}

// LiveJob reports whether a job of kind for run runID is queued or
// running.
func (s *Scheduler) LiveJob(kind string, runID ids.UUID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	is := func(j *job) bool {
		if j == nil || j.kind != kind {
			return false
		}
		switch args := j.args.(type) {
		case sched.PumpRunArgs:
			return args.RunID == runID
		case sched.WaitTimeoutArgs:
			return args.RunID == runID
		}
		return false
	}
	return is(s.running) || slices.ContainsFunc(s.pending, is)
}

// Available returns how many jobs of queue are due and waiting to run.
func (s *Scheduler) Available(queue string) int {
	if queue != sched.Queue {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.o.Clock.Now()
	n := 0
	for _, j := range s.pending {
		if !j.due.After(now) {
			n++
		}
	}
	return n
}

// Job describes a queued job, for tests and failure reports.
type Job struct {
	Kind    string
	Args    any
	Due     time.Time
	Attempt int
}

// Pending lists the queued jobs in the order they would run.
func (s *Scheduler) Pending() []Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	js := slices.Clone(s.pending)
	slices.SortFunc(js, func(a, b *job) int {
		if c := a.due.Compare(b.due); c != 0 {
			return c
		}
		return int(a.seq - b.seq)
	})
	out := make([]Job, len(js))
	for i, j := range js {
		out[i] = Job{Kind: j.kind, Args: j.args, Due: j.due, Attempt: j.attempt}
	}
	return out
}
