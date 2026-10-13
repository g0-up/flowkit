package flowkittest_test

import (
	"sync"
	"testing"
	"time"

	"github.com/g0-up/flowkit"
	"github.com/g0-up/flowkit/flowkittest"
)

var _ flowkit.Clock = (*flowkittest.VirtualClock)(nil)

func TestVirtualClock(t *testing.T) {
	start := time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)
	c := flowkittest.NewVirtualClock(start)
	if got := c.Now(); !got.Equal(start) {
		t.Fatalf("Now = %v, want %v", got, start)
	}
	c.Advance(90 * time.Second)
	if got, want := c.Now(), start.Add(90*time.Second); !got.Equal(want) {
		t.Fatalf("after Advance, Now = %v, want %v", got, want)
	}
	c.Advance(-30 * time.Second)
	if got, want := c.Now(), start.Add(time.Minute); !got.Equal(want) {
		t.Fatalf("after negative Advance, Now = %v, want %v", got, want)
	}
	later := start.Add(48 * time.Hour)
	c.Set(later)
	if got := c.Now(); !got.Equal(later) {
		t.Fatalf("after Set, Now = %v, want %v", got, later)
	}
}

func TestVirtualClockIsSafeForConcurrentUse(t *testing.T) {
	start := time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)
	c := flowkittest.NewVirtualClock(start)
	const goroutines, steps = 8, 1000
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			prev := c.Now()
			for range steps {
				c.Advance(time.Millisecond)
				// Only forward moves happen, so no reader sees time go back.
				now := c.Now()
				if now.Before(prev) {
					t.Errorf("clock went back from %v to %v", prev, now)
					return
				}
				prev = now
			}
		}()
	}
	wg.Wait()
	if got, want := c.Now(), start.Add(goroutines*steps*time.Millisecond); !got.Equal(want) {
		t.Fatalf("after concurrent Advance, Now = %v, want %v", got, want)
	}
}
