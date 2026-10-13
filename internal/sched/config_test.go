package sched

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"github.com/g0-up/flowkit/internal/store"
	"github.com/g0-up/flowkit/internal/testhooks"
)

func TestNewTiming(t *testing.T) {
	got := NewTiming(5 * time.Minute)
	want := Timing{
		ActionDeadline:       4 * time.Minute,
		PumpJobTimeout:       4*time.Minute + 30*time.Second,
		ClaimTimeout:         5 * time.Minute,
		StuckRunAfter:        10 * time.Minute,
		RescueStuckJobsAfter: time.Hour,
		JobTimeout:           time.Minute,
	}
	if got != want {
		t.Fatalf("NewTiming(5m) = %+v, want %+v", got, want)
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("default chain rejected: %v", err)
	}
}

func TestTimingValidate(t *testing.T) {
	derived := func(ct time.Duration) func(*Timing) {
		return func(tm *Timing) { *tm = NewTiming(ct) }
	}
	tests := []struct {
		name   string
		mutate func(*Timing)
		want   string // "" accepts
	}{
		{"minimum claim timeout", derived(MinClaimTimeout), ""},
		{"largest claim timeout", derived(30*time.Minute - time.Second), ""},
		{"claim timeout below minimum", derived(MinClaimTimeout - time.Millisecond), "below the minimum"},
		{"zero claim timeout", derived(0), "below the minimum"},
		{"stuck run reaches rescue", derived(30 * time.Minute), "job rescue delay"},
		{"stuck run beyond rescue", derived(2 * time.Hour), "job rescue delay"},
		{"action deadline after pump timeout", func(tm *Timing) { tm.ActionDeadline = tm.PumpJobTimeout }, "out of order"},
		{"zero action deadline", func(tm *Timing) { tm.ActionDeadline = 0 }, "out of order"},
		{"pump timeout after claim", func(tm *Timing) { tm.PumpJobTimeout = tm.ClaimTimeout }, "out of order"},
		{"claim after stuck run", func(tm *Timing) { tm.StuckRunAfter = tm.ClaimTimeout }, "out of order"},
		{"pump timeout reaches rescue", func(tm *Timing) {
			tm.RescueStuckJobsAfter = tm.PumpJobTimeout
			tm.StuckRunAfter = tm.PumpJobTimeout - 1
		}, "job rescue delay"},
		{"zero job timeout", func(tm *Timing) { tm.JobTimeout = 0 }, "job timeout"},
		{"job timeout reaches rescue", func(tm *Timing) { tm.JobTimeout = tm.RescueStuckJobsAfter }, "job timeout"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tm := NewTiming(5 * time.Minute)
			tt.mutate(&tm)
			err := tm.Validate()
			switch {
			case tt.want == "" && err != nil:
				t.Fatalf("rejected %+v: %v", tm, err)
			case tt.want != "" && err == nil:
				t.Fatalf("accepted %+v", tm)
			case tt.want != "" && !strings.Contains(err.Error(), tt.want):
				t.Fatalf("error %q does not mention %q", err, tt.want)
			}
		})
	}
}

// placeholderPool satisfies Options.validate without a database; nothing in
// these tests connects through it.
var placeholderPool = &pgxpool.Pool{}

// clockFunc adapts a function to Clock.
type clockFunc func() time.Time

func (f clockFunc) Now() time.Time { return f() }

func unitOptions(t *testing.T, worker, janitor bool) Options {
	st, err := store.NewPgStore(placeholderPool, "flowkit")
	if err != nil {
		t.Fatal(err)
	}
	return Options{
		Pool:       placeholderPool,
		Store:      st,
		InstanceID: "host-1",
		Worker:     worker,
		MaxWorkers: 7,
		Janitor:    janitor,
		Timing:     NewTiming(5 * time.Minute),
		Logger:     slog.New(slog.DiscardHandler),
		Clock:      clockFunc(time.Now),
	}
}

func TestOptionsValidate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Options)
		want   string
	}{
		{"nil pool", func(o *Options) { o.Pool = nil }, "nil pool"},
		{"nil store", func(o *Options) { o.Store = nil }, "nil store"},
		{"nil clock", func(o *Options) { o.Clock = nil }, "nil clock"},
		{"nil logger", func(o *Options) { o.Logger = nil }, "nil logger"},
		{"worker without workers", func(o *Options) { o.MaxWorkers = 0 }, "MaxWorkers"},
		{"broken timing", func(o *Options) { o.Timing = NewTiming(time.Second) }, "claim timeout"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := unitOptions(t, true, true)
			tt.mutate(&o)
			err := o.validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("validate() = %v, want an error mentioning %q", err, tt.want)
			}
		})
	}
	o := unitOptions(t, false, true)
	o.MaxWorkers = 0
	if err := o.validate(); err != nil {
		t.Fatalf("janitor-only options without MaxWorkers rejected: %v", err)
	}
}

func TestRiverConfigByRole(t *testing.T) {
	tests := []struct {
		name            string
		worker, janitor bool
		queue           string
		maxWorkers      int
	}{
		{"worker and janitor", true, true, Queue, 7},
		{"worker only", true, false, Queue, 7},
		{"janitor only", false, true, IdleQueue, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := unitOptions(t, tt.worker, tt.janitor)
			o.PollInterval = 250 * time.Millisecond
			o.PollOnly = true
			workers, err := newWorkers(StubHandlers{}, o.Timing)
			if err != nil {
				t.Fatal(err)
			}
			cfg := riverConfig(o, workers)
			if len(cfg.Queues) != 1 || cfg.Queues[tt.queue].MaxWorkers != tt.maxWorkers {
				t.Errorf("Queues = %+v, want only %s with %d workers", cfg.Queues, tt.queue, tt.maxWorkers)
			}
			if cfg.LeaderElectionDisabled != !tt.janitor {
				t.Errorf("LeaderElectionDisabled = %v with janitor %v", cfg.LeaderElectionDisabled, tt.janitor)
			}
			// Only a janitor client registers the janitor's periodic job.
			if want := map[bool]int{true: 1, false: 0}[tt.janitor]; len(cfg.PeriodicJobs) != want {
				t.Errorf("PeriodicJobs = %v, want %d", cfg.PeriodicJobs, want)
			}
			if cfg.ReindexerIndexNames == nil || len(cfg.ReindexerIndexNames) != 0 {
				t.Errorf("ReindexerIndexNames = %#v, want a non-nil empty list", cfg.ReindexerIndexNames)
			}
			if cfg.Test.Time != nil {
				t.Errorf("Test.Time = %v without a test hook", cfg.Test.Time)
			}
			if cfg.Schema != "flowkit" || cfg.Workers != workers {
				t.Errorf("Schema %q / Workers %p, want flowkit / %p", cfg.Schema, cfg.Workers, workers)
			}
			if cfg.JobTimeout != time.Minute || cfg.RescueStuckJobsAfter != time.Hour {
				t.Errorf("JobTimeout %s, RescueStuckJobsAfter %s, want 1m and 1h", cfg.JobTimeout, cfg.RescueStuckJobsAfter)
			}
			if !cfg.PollOnly || cfg.FetchPollInterval != 250*time.Millisecond {
				t.Errorf("PollOnly %v, FetchPollInterval %s", cfg.PollOnly, cfg.FetchPollInterval)
			}
			if !strings.HasPrefix(cfg.ID, "host-1_") {
				t.Errorf("ID = %q, want it derived from the instance ID", cfg.ID)
			}
		})
	}
}

// stubTime is a River time generator stuck at one instant.
type stubTime struct{ t time.Time }

func (s stubTime) Now() time.Time       { return s.t }
func (s stubTime) NowOrNil() *time.Time { return &s.t }

func TestRiverTimeHook(t *testing.T) {
	prev := testhooks.RiverTime
	t.Cleanup(func() { testhooks.RiverTime = prev })
	workers, err := newWorkers(StubHandlers{}, NewTiming(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	o := unitOptions(t, true, true)
	stub := stubTime{t: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}

	var got any
	testhooks.RiverTime = func(clock any) rivertype.TimeGenerator {
		got = clock
		return stub
	}
	if cfg := riverConfig(o, workers); cfg.Test.Time != stub {
		t.Errorf("Test.Time = %v, want the hook's generator", cfg.Test.Time)
	}
	// The hook gets the clock value itself, so it can recognise a test clock.
	if c, ok := got.(clockFunc); !ok || c == nil {
		t.Errorf("hook got clock %T, want the Options clock", got)
	}

	testhooks.RiverTime = func(any) rivertype.TimeGenerator { return nil }
	if cfg := riverConfig(o, workers); cfg.Test.Time != nil {
		t.Errorf("Test.Time = %v when the hook declines", cfg.Test.Time)
	}
}

func TestWorkerTimeouts(t *testing.T) {
	tm := NewTiming(time.Minute)
	if got := (&worker[PumpRunArgs]{timeout: tm.PumpJobTimeout}).Timeout(nil); got != 54*time.Second {
		t.Fatalf("pump timeout = %s, want 0.9 × claim timeout", got)
	}
}

func TestClientID(t *testing.T) {
	tests := []struct {
		instance, prefix string
	}{
		{"host-1", "host-1_"},
		{"web 1/pod#2", "web_1_pod_2_"},
		{"", "flowkit_"},
		{"ñandú:9", "_and_:9_"},
		{strings.Repeat("a", 200), strings.Repeat("a", 87) + "_"},
	}
	for _, tt := range tests {
		id := clientID(tt.instance)
		if !strings.HasPrefix(id, tt.prefix) {
			t.Errorf("clientID(%q) = %q, want prefix %q", tt.instance, id, tt.prefix)
		}
		if len(id) > maxClientIDLen {
			t.Errorf("clientID(%q) has %d bytes, more than %d", tt.instance, len(id), maxClientIDLen)
		}
		if unsafeIDChars.MatchString(id) {
			t.Errorf("clientID(%q) = %q has unsafe characters", tt.instance, id)
		}
	}
	if a, b := clientID("host"), clientID("host"); a == b {
		t.Fatalf("two clients of one instance share ID %q", a)
	}
}

func TestResultMapsToRiver(t *testing.T) {
	cause := errors.New("boom")
	if err := Done().riverError(); err != nil {
		t.Errorf("Done maps to %v", err)
	}
	if err := (Result{}).riverError(); err != nil {
		t.Errorf("zero Result maps to %v", err)
	}
	if err := Retry(cause).riverError(); !errors.Is(err, cause) {
		t.Errorf("Retry maps to %v, want the cause", err)
	}
	if err := Retry(nil).riverError(); err == nil {
		t.Error("Retry(nil) maps to nil, which River treats as success")
	}
	var snooze *river.JobSnoozeError
	if err := Snooze(3 * time.Second).riverError(); !errors.As(err, &snooze) || snooze.Duration != 3*time.Second {
		t.Errorf("Snooze maps to %v, want a 3s JobSnoozeError", err)
	}
	for _, c := range []struct {
		r    Result
		want string
	}{{Done(), "done"}, {Result{}, "done"}, {Snooze(3 * time.Second), "snooze 3s"}, {Retry(cause), "retry: boom"}} {
		if got, want := c.r.String(), c.want; got != want {
			t.Errorf("String = %q, want %q", got, want)
		}
	}
	if Done().Err() != nil || Snooze(time.Second).Err() != nil || !errors.Is(Retry(cause).Err(), cause) || Retry(nil).Err() == nil {
		t.Error("Err is not the cause of a Retry and nil otherwise")
	}
	if d, ok := Snooze(3 * time.Second).SnoozeDelay(); !ok || d != 3*time.Second {
		t.Errorf("Snooze(3s).SnoozeDelay() = %s, %v, want 3s, true", d, ok)
	}
	for _, r := range []Result{Done(), Retry(cause)} {
		if _, ok := r.SnoozeDelay(); ok {
			t.Errorf("%s.SnoozeDelay() reports a snooze", r)
		}
	}
	w := &worker[RouteEventArgs]{handle: func(_ context.Context, args RouteEventArgs) Result {
		if args.EventID != 9 {
			return Retry(errors.New("wrong args"))
		}
		return Done()
	}}
	if err := w.Work(context.Background(), &river.Job[RouteEventArgs]{Args: RouteEventArgs{EventID: 9}}); err != nil {
		t.Errorf("worker passes args through: %v", err)
	}
}

func TestStubHandlersRetry(t *testing.T) {
	var h Handlers = StubHandlers{}
	ctx := context.Background()
	for name, r := range map[string]Result{
		"route":   h.Route(ctx, RouteEventArgs{}),
		"pump":    h.Pump(ctx, PumpRunArgs{}),
		"timeout": h.Timeout(ctx, WaitTimeoutArgs{}),
		"janitor": h.Janitor(ctx, JanitorArgs{}),
	} {
		if err := r.riverError(); !errors.Is(err, errNotImplemented) {
			t.Errorf("%s stub returns %v", name, err)
		}
	}
}

func TestJanitorScheduleSkipsMissedPasses(t *testing.T) {
	start := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	now := start
	s := janitorSchedule{now: func() time.Time { return now }}
	if got := s.Next(start); !got.Equal(start.Add(JanitorInterval)) {
		t.Errorf("Next on time = %s, want an interval later", got)
	}
	// The next pass may still be due a little before now: River enqueues it
	// with a small margin.
	now = start.Add(JanitorInterval - time.Second)
	if got := s.Next(start); !got.Equal(start.Add(JanitorInterval)) {
		t.Errorf("Next just before due = %s, want an interval after the last pass", got)
	}
	// Three days later the overdue pass runs once and the next comes an
	// interval after now, not 30s after the missed one.
	now = start.Add(72 * time.Hour)
	if got := s.Next(start); !got.Equal(now.Add(JanitorInterval)) {
		t.Errorf("Next after a jump = %s, want an interval after now", got)
	}
}
