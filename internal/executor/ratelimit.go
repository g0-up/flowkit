package executor

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/g0-up/flowkit/internal/store"
)

// RateLimit is a token bucket per definition, shared by every instance
// through the rate_limits table. Each claim attempt that passes the fast
// read takes one token.
type RateLimit struct {
	// Rate is the refill rate in tokens per second; zero disables the limit.
	Rate float64
	// Burst is the bucket's capacity, at least 1 when Rate is set. A new
	// bucket starts full.
	Burst float64
}

func (r RateLimit) enabled() bool { return r.Rate > 0 }

func (r RateLimit) validate() error {
	switch {
	case math.IsNaN(r.Rate) || math.IsInf(r.Rate, 0) || r.Rate < 0:
		return fmt.Errorf("flowkit executor: rate limit %v must be a finite non-negative number", r.Rate)
	case r.Rate == 0:
		return nil
	case math.IsNaN(r.Burst) || math.IsInf(r.Burst, 0) || r.Burst < 1:
		return fmt.Errorf("flowkit executor: rate limit burst %v must be a finite number of at least 1", r.Burst)
	}
	return nil
}

// Allow takes a token from defName's bucket at now. Without a rate limit it
// always allows. When the bucket is empty it returns a positive wait: the time
// until the next token plus a random share of the time the whole bucket takes
// to refill (Burst/Rate), so requests turned away together come back spread
// out rather than all at the next token. A rejection writes nothing.
//
// Run takes the token before it claims the effect, so a request that then
// finds the effect claimed by another one has spent its token.
func (e *Executor) Allow(ctx context.Context, defName string, now time.Time) (ok bool, wait time.Duration, err error) {
	if !e.limit.enabled() {
		return true, 0, nil
	}
	if defName == "" {
		return false, 0, errors.New("flowkit executor: empty definition name")
	}
	err = e.inTx(ctx, func(tx store.Tx) error {
		if err := e.store.InsertRateLimit(ctx, tx, store.RateLimitRow{DefName: defName, Tokens: e.limit.Burst, RefilledAt: now}); err != nil {
			return err
		}
		b, err := e.store.LockRateLimit(ctx, tx, defName)
		if err != nil {
			return err
		}
		if b, ok, wait = e.limit.take(b, now); !ok {
			// The refill is linear and a bucket below one token is below its
			// burst, so the next request computes the same tokens from the
			// stored row: there is nothing to write.
			return nil
		}
		return e.store.UpdateRateLimit(ctx, tx, b)
	})
	if err != nil {
		return false, 0, err
	}
	if ok {
		return true, 0, nil
	}
	return false, max(addDelay(wait, e.limit.spread(e.rand())), time.Nanosecond), nil
}

// take refills b up to now and takes one token if there is one; otherwise it
// returns how long until there is. A clock behind the bucket's refill time
// adds nothing.
func (r RateLimit) take(b store.RateLimitRow, now time.Time) (_ store.RateLimitRow, allowed bool, wait time.Duration) {
	if elapsed := now.Sub(b.RefilledAt); elapsed > 0 {
		b.Tokens = math.Min(r.Burst, b.Tokens+elapsed.Seconds()*r.Rate)
		b.RefilledAt = now
	}
	if b.Tokens >= 1 {
		b.Tokens--
		return b, true, 0
	}
	return b, false, ceilDuration((1 - b.Tokens) / r.Rate)
}

// spread is the share f, in [0, 1), of the time the bucket takes to refill
// completely.
func (r RateLimit) spread(f float64) time.Duration {
	return ceilDuration(f * r.Burst / r.Rate)
}
