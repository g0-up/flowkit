package flowkittest

import (
	"sync"
	"time"
)

// VirtualClock is a flowkit.Clock that moves only when told to. Use it as
// Config.Clock to control the timestamps flowkit writes and the deadlines it
// compares against. An Engine using it runs River on it too, so a timer or
// retry becomes due once the clock passes it; PGHarness.Advance moves the
// clock and waits for those jobs to run. It is safe for concurrent use.
type VirtualClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewVirtualClock returns a clock reading start.
func NewVirtualClock(start time.Time) *VirtualClock {
	return &VirtualClock{now: start}
}

// Now returns the current virtual time.
func (c *VirtualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d. A negative d moves it back.
func (c *VirtualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Set moves the clock to t.
func (c *VirtualClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}
