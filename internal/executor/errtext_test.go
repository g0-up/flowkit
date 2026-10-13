package executor

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/g0-up/flowkit/internal/errtext"
	"github.com/g0-up/flowkit/internal/sched"
)

func TestStoredErrorWrapsTheRawError(t *testing.T) {
	err := storedError("a", "redacted", errNotPerformed)
	if err.Error() != "action a: redacted" || !errors.Is(err, errNotPerformed) {
		t.Fatalf("storedError = %v; want the stored text wrapping the raw error", err)
	}
	if err := storedError("a", "redacted", nil); errors.Unwrap(err) != nil {
		t.Fatalf("stored error read back wraps %v", errors.Unwrap(err))
	}
}

// unsafeRedact keeps NUL and invalid UTF-8, which Postgres text rejects, and
// makes the text longer than the executor stores.
func unsafeRedact(err error) string {
	return err.Error() + " \x00 \xff " + strings.Repeat("é", errtext.MaxBytes)
}

func wantStorable(t *testing.T, what, s string) {
	t.Helper()
	if !utf8.ValidString(s) || strings.ContainsRune(s, 0) || len(s) > errtext.MaxBytes {
		t.Fatalf("%s is not storable text: %d bytes, valid %v", what, len(s), utf8.ValidString(s))
	}
}

func TestUnstorableRedactedTextIsSanitized(t *testing.T) {
	e := newEnv(t)
	client, err := sched.New(sched.Options{
		Pool: e.pool, Store: e.store, InstanceID: "test", Timing: sched.NewTiming(testClaimTimeout),
		Logger: e.logger, Clock: e.clock,
	}, sched.StubHandlers{})
	if err != nil {
		t.Fatal(err)
	}
	ex := e.executor(func(o *Options) {
		o.RedactError = unsafeRedact
		o.Pumps = client.Scheduler()
		o.IsIdempotent = func(string) bool { return false }
	})

	// Every attempt is recorded, so the retries end.
	retried, calls := countingAction("crm.update", true, func(context.Context, int64) (map[string]any, error) {
		return nil, errors.New("bad\x00reply")
	})
	retried.Retry = RetryPolicy{MaxAttempts: 3, BaseBackoff: time.Second}
	for _, want := range []ExecKind{RetryLater, RetryLater, Failed} {
		out := ex.Run(context.Background(), e.request("retried", retried))
		wantKind(t, out, want)
		if s := out.Err.Error(); !utf8.ValidString(s) || strings.ContainsRune(s, 0) {
			t.Fatalf("outcome err = %q; want the sanitized text", s)
		}
	}
	if calls.Load() != 3 {
		t.Fatalf("Execute ran %d times; want 3", calls.Load())
	}
	row := e.effect("retried")
	if row.Attempt != 3 {
		t.Fatalf("attempt = %d; want 3", row.Attempt)
	}
	wantStorable(t, "last_error", *row.LastError)
	for _, s := range e.steps(stepAttemptFailed) {
		wantStorable(t, "step error", *s.Error)
	}

	charge, _ := countingAction("payments.charge", false, func(context.Context, int64) (map[string]any, error) {
		panic("bad\x00\xffpanic")
	})
	wantKind(t, ex.Run(context.Background(), e.request("charge", charge)), BlockedInDoubt)

	e.claimAt("expired", "payments.charge", t0.Add(-testClaimTimeout-time.Second))
	if n, err := ex.ResolveExpiredClaims(context.Background(), e.clock.Now(), 10); err != nil || n != 1 {
		t.Fatalf("ResolveExpiredClaims = %d, %v; want 1", n, err)
	}
	for _, key := range []string{"charge", "expired"} {
		if row := e.effect(key); !row.InDoubt {
			t.Fatalf("%s: effect = %+v; want it in doubt", key, row)
		} else {
			wantStorable(t, key+" last_error", *row.LastError)
		}
	}
	dl := e.deadLetters()
	if len(dl) != 2 {
		t.Fatalf("dead letters = %d; want 2", len(dl))
	}
	for _, d := range dl {
		wantStorable(t, "dead letter note", d.Note)
	}
	for _, s := range e.steps(stepEffectInDoubt) {
		wantStorable(t, "in-doubt step error", *s.Error)
	}
}

// lockedBuffer is a bytes.Buffer safe for concurrent writers.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestRawErrorsAreNeverLoggedOrReported(t *testing.T) {
	const secret = "token=s3cret"
	e := newEnv(t)
	var logs lockedBuffer
	ex := e.executor(func(o *Options) {
		o.RedactError = func(error) string { return "hidden" }
		o.Logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	})
	for _, tc := range []struct {
		key        string
		idempotent bool
		fail       func() error
	}{
		{"error", false, func() error { return errors.New(secret) }},
		{"panic", true, func() error { panic(secret) }},
	} {
		a, _ := countingAction("a", tc.idempotent, func(context.Context, int64) (map[string]any, error) {
			return nil, tc.fail()
		})
		out := ex.Run(context.Background(), e.request(tc.key, a))
		if out.Err == nil || out.Err.Error() != "action a: hidden" {
			t.Fatalf("%s: outcome err = %v; want the redacted text", tc.key, out.Err)
		}
	}
	if strings.Contains(logs.String(), "s3cret") {
		t.Fatalf("logs hold the raw error:\n%s", logs.String())
	}
	if !strings.Contains(logs.String(), "hidden") {
		t.Fatalf("logs lack the redacted error:\n%s", logs.String())
	}
}
