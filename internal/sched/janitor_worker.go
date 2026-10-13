package sched

import (
	"time"

	"github.com/riverqueue/river"
)

// JanitorInterval is how often the leader enqueues a janitor pass.
const JanitorInterval = 30 * time.Second

// periodicJobs returns the periodic jobs of a client: the janitor's when it
// has the janitor role, none otherwise. River runs them on the leader only
// and refuses them when leader election is disabled. now is River's clock.
func periodicJobs(janitor bool, now func() time.Time) []*river.PeriodicJob {
	if !janitor {
		return nil
	}
	return []*river.PeriodicJob{river.NewPeriodicJob(
		janitorSchedule{now: now},
		func() (river.JobArgs, *river.InsertOpts) {
			// A failed pass is not retried: the next one comes within
			// JanitorInterval and redoes every step.
			return JanitorArgs{}, &river.InsertOpts{Queue: Queue, MaxAttempts: 1}
		},
		&river.PeriodicJobOpts{ID: KindJanitor, RunOnStart: true},
	)}
}

// janitorSchedule makes a janitor pass due every JanitorInterval. When the
// clock jumps or the leader stalls past several intervals, River enqueues
// every missed pass of a plain interval schedule back to back; one pass
// covers them all, so janitorSchedule skips the rest and resumes an interval
// after now.
type janitorSchedule struct{ now func() time.Time }

func (s janitorSchedule) Next(prev time.Time) time.Time {
	next := prev.Add(JanitorInterval)
	if now := s.now(); next.Before(now) {
		return now.Add(JanitorInterval)
	}
	return next
}
