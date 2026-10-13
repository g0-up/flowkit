package sched

import (
	"context"
	"errors"
	"time"

	"github.com/riverqueue/river"
)

// Handlers does the work behind each job kind. The interpreter implements it
// without importing River; the workers in this package adapt it.
type Handlers interface {
	Route(ctx context.Context, args RouteEventArgs) Result
	Pump(ctx context.Context, args PumpRunArgs) Result
	Timeout(ctx context.Context, args WaitTimeoutArgs) Result
	Janitor(ctx context.Context, args JanitorArgs) Result
}

type resultKind int

const (
	resultDone resultKind = iota
	resultSnooze
	resultRetry
)

// Result is how a handler ends a job. The zero Result is Done.
type Result struct {
	kind  resultKind
	delay time.Duration
	err   error
}

// Done completes the job.
func Done() Result { return Result{kind: resultDone} }

// Snooze puts the job back to run again after d without using up an attempt.
func Snooze(d time.Duration) Result { return Result{kind: resultSnooze, delay: d} }

// Retry fails this attempt; River retries the job with backoff until its
// attempts run out.
func Retry(err error) Result {
	if err == nil {
		err = errRetryWithoutCause
	}
	return Result{kind: resultRetry, err: err}
}

var errRetryWithoutCause = errors.New("flowkit: job retry requested")

// String describes r: "done", "snooze <delay>" or "retry: <cause>".
func (r Result) String() string {
	switch r.kind {
	case resultSnooze:
		return "snooze " + r.delay.String()
	case resultRetry:
		return "retry: " + r.err.Error()
	default:
		return "done"
	}
}

// Err returns the cause of a Retry, and nil for Done and Snooze.
func (r Result) Err() error {
	if r.kind == resultRetry {
		return r.err
	}
	return nil
}

// SnoozeDelay returns the delay of a Snooze and true, and false for Done and
// Retry.
func (r Result) SnoozeDelay() (time.Duration, bool) {
	return r.delay, r.kind == resultSnooze
}

// riverError maps r to what a River worker returns. It is the only place
// that knows River's encoding of these outcomes.
func (r Result) riverError() error {
	switch r.kind {
	case resultSnooze:
		return river.JobSnooze(r.delay)
	case resultRetry:
		return r.err
	default:
		return nil
	}
}

// errNotImplemented is returned by handlers whose work is not wired yet.
var errNotImplemented = errors.New("flowkit: job handler not implemented")

// StubHandlers fails every job with a retryable "not implemented" error. It
// lets jobs be enqueued and inspected before the real handlers exist.
type StubHandlers struct{}

// Route fails the job.
func (StubHandlers) Route(context.Context, RouteEventArgs) Result { return Retry(errNotImplemented) }

// Pump fails the job.
func (StubHandlers) Pump(context.Context, PumpRunArgs) Result { return Retry(errNotImplemented) }

// Timeout fails the job.
func (StubHandlers) Timeout(context.Context, WaitTimeoutArgs) Result {
	return Retry(errNotImplemented)
}

// Janitor fails the job.
func (StubHandlers) Janitor(context.Context, JanitorArgs) Result { return Retry(errNotImplemented) }

// worker is the thin River worker of one job kind: it applies the kind's
// timeout and hands the args to a handler.
type worker[T river.JobArgs] struct {
	river.WorkerDefaults[T]
	timeout time.Duration
	handle  func(context.Context, T) Result
}

func (w *worker[T]) Timeout(*river.Job[T]) time.Duration { return w.timeout }

func (w *worker[T]) Work(ctx context.Context, job *river.Job[T]) error {
	return w.handle(ctx, job.Args).riverError()
}
