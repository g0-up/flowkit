package flowkittest

import (
	"time"

	"github.com/riverqueue/river/rivertype"

	"github.com/g0-up/flowkit/internal/testhooks"
)

// An Engine whose Config.Clock is a *VirtualClock runs River on the same
// virtual time: River stamps jobs with it and makes scheduled and snoozed
// jobs due by it. Any other clock leaves River on the system clock, so a
// real-time Engine in the same test binary is unaffected.
func init() {
	testhooks.RiverTime = func(clock any) rivertype.TimeGenerator {
		if c, ok := clock.(*VirtualClock); ok && c != nil {
			return riverTime{c}
		}
		return nil
	}
}

// riverTime reads a VirtualClock as River's time generator.
type riverTime struct{ c *VirtualClock }

func (r riverTime) Now() time.Time { return r.c.Now() }

// NowOrNil always reports the virtual time, so River writes it to the
// database and compares due times with it instead of the database clock.
func (r riverTime) NowOrNil() *time.Time {
	now := r.c.Now()
	return &now
}
