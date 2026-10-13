package executor

import (
	"math"
	"testing"
	"time"
)

func TestRetryPolicyDefaults(t *testing.T) {
	if got := (RetryPolicy{}).withDefaults(); got != (RetryPolicy{DefaultMaxAttempts, DefaultBaseBackoff, DefaultMaxBackoff}) {
		t.Fatalf("zero policy = %+v", got)
	}
	if got := (RetryPolicy{BaseBackoff: time.Hour}).withDefaults(); got.MaxBackoff != time.Hour {
		t.Fatalf("defaulted MaxBackoff = %s; want it raised to BaseBackoff", got.MaxBackoff)
	}
	p := RetryPolicy{MaxAttempts: 2, BaseBackoff: time.Second, MaxBackoff: time.Minute}
	if got := p.withDefaults(); got != p {
		t.Fatalf("set policy changed to %+v", got)
	}
}

func TestBackoff(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 100, BaseBackoff: time.Second, MaxBackoff: 10 * time.Second}
	for failures, want := range map[int]time.Duration{
		0: time.Second, 1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 4: 8 * time.Second,
		5: 10 * time.Second, 60: 10 * time.Second, math.MaxInt: 10 * time.Second,
	} {
		if got := backoff(p, failures); got != want {
			t.Errorf("backoff after %d failures = %s; want %s", failures, got, want)
		}
	}
	huge := RetryPolicy{BaseBackoff: time.Duration(math.MaxInt64 / 3), MaxBackoff: time.Duration(math.MaxInt64)}
	if got := backoff(huge, 10); got <= 0 {
		t.Fatalf("backoff overflowed to %s", got)
	}
}

func TestWithJitter(t *testing.T) {
	if got := withJitter(10*time.Second, func() float64 { return 0 }); got != 10*time.Second {
		t.Fatalf("no jitter = %s", got)
	}
	if got := withJitter(10*time.Second, func() float64 { return 0.5 }); got != 10500*time.Millisecond {
		t.Fatalf("half jitter = %s", got)
	}
}

func TestDelaysSaturate(t *testing.T) {
	huge := RetryPolicy{MaxAttempts: 100, BaseBackoff: maxDelay / 3, MaxBackoff: maxDelay}
	if got := withJitter(backoff(huge, 10), func() float64 { return 0.99 }); got != maxDelay {
		t.Fatalf("jittered huge backoff = %s; want %s", got, maxDelay)
	}
	if got := withJitter(maxDelay/2, func() float64 { return 0.99 }); got < maxDelay/2 {
		t.Fatalf("jitter shortened a delay to %s", got)
	}
	for seconds, want := range map[float64]time.Duration{
		1e30: maxDelay, math.Inf(1): maxDelay, math.NaN(): maxDelay,
		-1: 0, 0: 0, 1e-12: 1, 1.5: 1500 * time.Millisecond,
	} {
		if got := ceilDuration(seconds); got != want {
			t.Errorf("ceilDuration(%v) = %s; want %s", seconds, got, want)
		}
	}
	if got := addDelay(maxDelay-1, 2); got != maxDelay {
		t.Fatalf("addDelay overflowed to %s", got)
	}
}
