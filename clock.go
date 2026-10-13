package flowkit

import "time"

// Clock is the only source of time flowkit uses for rows it writes and for
// deadlines it compares against: event timestamps, claims, wait deadlines and
// retention cut-offs. flowkit never reads the database clock. It has no Sleep
// or After: timers live in the database, not in goroutines. Tests substitute
// flowkittest.VirtualClock.
type Clock interface {
	Now() time.Time
}

// realClock is the default Clock: the host's wall clock.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
