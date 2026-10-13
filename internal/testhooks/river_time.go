package testhooks

import "github.com/riverqueue/river/rivertype"

// RiverTime, when set, maps the Engine's clock (the Config.Clock value) to
// the time generator River uses for job timestamps and for deciding which
// scheduled jobs are due. Returning nil keeps River on the system clock.
//
// It is nil in production. Only test support code (flowkittest) assigns it,
// before creating an Engine, so a virtual clock also drives River.
var RiverTime func(clock any) rivertype.TimeGenerator
