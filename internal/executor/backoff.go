package executor

import (
	"math"
	"time"
)

// Defaults for zero RetryPolicy fields.
const (
	DefaultMaxAttempts = 5
	DefaultBaseBackoff = 10 * time.Second
	DefaultMaxBackoff  = 10 * time.Minute
)

// jitterFraction is the largest share of a delay added as random jitter, so
// retries that became due together spread out.
const jitterFraction = 0.1

// maxDelay is the longest delay the executor reports; longer ones saturate.
const maxDelay = time.Duration(math.MaxInt64)

// withDefaults fills zero (or negative) fields. A defaulted MaxBackoff never
// falls below BaseBackoff.
func (p RetryPolicy) withDefaults() RetryPolicy {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = DefaultMaxAttempts
	}
	if p.BaseBackoff <= 0 {
		p.BaseBackoff = DefaultBaseBackoff
	}
	if p.MaxBackoff <= 0 {
		p.MaxBackoff = max(DefaultMaxBackoff, p.BaseBackoff)
	}
	return p
}

// backoff is the delay before the next attempt after failures failed ones:
// min(MaxBackoff, BaseBackoff × 2^(failures−1)), without jitter. p must have
// its defaults applied.
func backoff(p RetryPolicy, failures int) time.Duration {
	d := p.BaseBackoff
	for i := 1; i < failures && d < p.MaxBackoff; i++ {
		if d > p.MaxBackoff/2 {
			return p.MaxBackoff
		}
		d *= 2
	}
	return min(d, p.MaxBackoff)
}

// withJitter adds up to jitterFraction of d, drawn from rnd, which returns
// values in [0, 1). The sum saturates at maxDelay.
func withJitter(d time.Duration, rnd func() float64) time.Duration {
	return addDelay(d, scaleDelay(d, rnd()*jitterFraction))
}

// scaleDelay is d × f for f ≥ 0, rounded down and saturating at maxDelay.
func scaleDelay(d time.Duration, f float64) time.Duration {
	return saturate(float64(d) * f)
}

// ceilDuration converts seconds to a duration, rounding up and saturating at
// maxDelay.
func ceilDuration(seconds float64) time.Duration {
	return saturate(math.Ceil(seconds * float64(time.Second)))
}

// saturate converts nanoseconds to a duration in [0, maxDelay]; NaN gives
// maxDelay.
func saturate(ns float64) time.Duration {
	switch {
	case ns <= 0:
		return 0
	case !(ns < float64(maxDelay)):
		return maxDelay
	}
	return time.Duration(ns)
}

// addDelay is a + b for non-negative delays, saturating at maxDelay.
func addDelay(a, b time.Duration) time.Duration {
	if a > maxDelay-b {
		return maxDelay
	}
	return a + b
}
