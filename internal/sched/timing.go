package sched

import (
	"fmt"
	"time"
)

const (
	// MinClaimTimeout is the smallest ClaimTimeout flowkit accepts.
	MinClaimTimeout = 5 * time.Second
	// RescueStuckJobsAfter is when River's rescuer retries a job still
	// running. flowkit always sets it, because setting JobTimeout alone makes
	// River default it to one hour plus JobTimeout.
	RescueStuckJobsAfter = time.Hour
	// JobTimeout bounds route_event, wait_timeout and janitor jobs.
	JobTimeout = time.Minute
)

// Timing is the chain of durations derived from ClaimTimeout. Every step must
// expire before the next one, so an action always times out before its job,
// and its job before River rescues it:
//
//	ActionDeadline < PumpJobTimeout < ClaimTimeout < StuckRunAfter < RescueStuckJobsAfter
type Timing struct {
	ActionDeadline       time.Duration // context deadline of an action's Execute
	PumpJobTimeout       time.Duration // River Worker.Timeout of pump_run
	ClaimTimeout         time.Duration
	StuckRunAfter        time.Duration // when the janitor treats a run as stuck
	RescueStuckJobsAfter time.Duration
	JobTimeout           time.Duration // River Config.JobTimeout for the other kinds
}

// NewTiming derives the chain from ClaimTimeout.
func NewTiming(claimTimeout time.Duration) Timing {
	return Timing{
		ActionDeadline:       claimTimeout * 8 / 10,
		PumpJobTimeout:       claimTimeout * 9 / 10,
		ClaimTimeout:         claimTimeout,
		StuckRunAfter:        2 * claimTimeout,
		RescueStuckJobsAfter: RescueStuckJobsAfter,
		JobTimeout:           JobTimeout,
	}
}

// Validate checks the chain. River does not compare per-kind worker timeouts
// with the rescue delay, so this is the only guard against River rescuing a
// pump that is still running.
func (t Timing) Validate() error {
	switch {
	case t.ClaimTimeout < MinClaimTimeout:
		return fmt.Errorf("claim timeout %s is below the minimum %s", t.ClaimTimeout, MinClaimTimeout)
	case t.StuckRunAfter >= t.RescueStuckJobsAfter:
		return fmt.Errorf("stuck-run threshold %s (2 × claim timeout %s) must be below the job rescue delay %s",
			t.StuckRunAfter, t.ClaimTimeout, t.RescueStuckJobsAfter)
	case t.PumpJobTimeout >= t.RescueStuckJobsAfter:
		return fmt.Errorf("pump job timeout %s must be below the job rescue delay %s", t.PumpJobTimeout, t.RescueStuckJobsAfter)
	case !(0 < t.ActionDeadline && t.ActionDeadline < t.PumpJobTimeout &&
		t.PumpJobTimeout < t.ClaimTimeout && t.ClaimTimeout < t.StuckRunAfter):
		return fmt.Errorf("timing chain out of order: action %s, pump job %s, claim %s, stuck run %s",
			t.ActionDeadline, t.PumpJobTimeout, t.ClaimTimeout, t.StuckRunAfter)
	case t.JobTimeout <= 0 || t.JobTimeout >= t.RescueStuckJobsAfter:
		return fmt.Errorf("job timeout %s must be positive and below the job rescue delay %s", t.JobTimeout, t.RescueStuckJobsAfter)
	}
	return nil
}
