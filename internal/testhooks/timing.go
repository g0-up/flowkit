package testhooks

import "time"

// Timing, when set, returns the job rescue delay and the job timeout an
// Engine uses for the given ClaimTimeout, in place of the fixed production
// values. A crash test shortens them so River rescues a killed process's jobs
// in seconds; Config validation still checks the shortened chain.
//
// It is nil in production. Tests assign it before creating an Engine.
var Timing func(claimTimeout time.Duration) (rescueStuckJobsAfter, jobTimeout time.Duration)
