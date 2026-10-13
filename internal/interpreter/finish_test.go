package interpreter

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/g0-up/flowkit/internal/errtext"
	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/interpreter/model"
	"github.com/g0-up/flowkit/internal/store"
)

// queuedPair starts a serial run for one event and queues a second one
// behind it.
func (e *env) queuedPair() (first, second ids.UUID) {
	e.t.Helper()
	e.publish(defSpec{name: "enrol", policy: model.PolicySerial, serialKey: "event.key"})
	a, b := e.emit("k"), e.emit("k")
	e.route(a)
	e.route(b)
	return RunID("enrol", a), RunID("enrol", b)
}

func (e *env) finishWith(r store.RunRow, f Finish) error {
	return e.in.inTx(context.Background(), func(tx store.Tx) error {
		return e.in.FinishRun(context.Background(), tx, r, f)
	})
}

func TestFinishRunEndsTheRunAndPromotesTheQueue(t *testing.T) {
	e := newEnv(t)
	first, second := e.queuedPair()
	ctx := context.Background()
	if _, err := e.pool.Exec(ctx, e.sql(`UPDATE {s}runs SET wait_step = 'w' WHERE id = $1`), first); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, e.sql(`INSERT INTO {s}subscriptions (run_id, wait_seq, step_id, event_type, match_cel, created_at)
		VALUES ($1, 1, 'w', 'order.refunded', 'true', $2)`), first, t0); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(time.Second)

	reason := "redacted: boom"
	if err := e.finishWith(e.run(first), Finish{State: stateFailed, Name: "$failed", StepID: "w", Error: &reason}); err != nil {
		t.Fatal(err)
	}

	r := e.run(first)
	switch {
	case r.State != stateFailed || !r.Terminal || r.TerminalName == nil || *r.TerminalName != "$failed":
		t.Errorf("run = %s terminal %v name %v", r.State, r.Terminal, r.TerminalName)
	case r.WaitStep != nil || r.Rev != 2 || !r.UpdatedAt.Equal(e.clock.Now()):
		t.Errorf("run wait %v rev %d updated %v", r.WaitStep, r.Rev, r.UpdatedAt)
	}
	if n := e.count(`SELECT count(*) FROM {s}subscriptions WHERE run_id = $1`, first); n != 0 {
		t.Errorf("subscriptions left = %d", n)
	}
	var steps []store.StepRow
	e.inTx(func(tx store.Tx) {
		var err error
		if steps, err = e.store.ListSteps(ctx, tx, first); err != nil {
			t.Fatal(err)
		}
	})
	if len(steps) == 0 {
		t.Fatal("no terminal step recorded")
	}
	last := steps[len(steps)-1]
	if last.Kind != stepTerminal || last.StepID != "w" || last.Detail == nil || *last.Detail != "$failed" ||
		last.Error == nil || *last.Error != reason {
		t.Errorf("terminal step = %+v", last)
	}
	if p := e.run(second); p.State != stateRunning || !p.Exclusive {
		t.Errorf("queued run = %s exclusive %v, want promoted", p.State, p.Exclusive)
	}
	if n := e.count(`SELECT count(*) FROM {s}serial_locks`); n != 1 {
		t.Errorf("serial lock rows = %d, want 1", n)
	}
}

func TestFinishRunStoresASanitizedError(t *testing.T) {
	e := newEnv(t)
	first, _ := e.queuedPair()
	reason := "a\x00b\xff" + strings.Repeat("é", errtext.MaxBytes)
	if err := e.finishWith(e.run(first), Finish{State: stateFailed, Name: "$failed", StepID: StepRun, Error: &reason}); err != nil {
		t.Fatal(err)
	}
	var stored string
	if err := e.pool.QueryRow(context.Background(),
		e.sql(`SELECT error FROM {s}run_steps WHERE run_id = $1 AND kind = 'terminal'`), first).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if want := errtext.Sanitize(reason); stored != want || !strings.HasPrefix(stored, "a\uFFFDb\uFFFD") {
		t.Errorf("stored error = %d bytes %q..., want the sanitized reason", len(stored), stored[:min(len(stored), 16)])
	}
}

func TestFinishRunOfAStaleRunChangesNothing(t *testing.T) {
	e := newEnv(t)
	first, second := e.queuedPair()
	stale := e.run(first)
	if _, err := e.pool.Exec(context.Background(), e.sql(`UPDATE {s}runs SET rev = rev + 1 WHERE id = $1`), first); err != nil {
		t.Fatal(err)
	}
	err := e.finishWith(stale, Finish{State: stateCompleted, Name: "done", StepID: "done"})
	if !errors.Is(err, ErrStale) {
		t.Fatalf("FinishRun = %v, want ErrStale", err)
	}
	if r := e.run(first); r.Terminal {
		t.Error("stale finish ended the run")
	}
	if e.runExists(second) {
		t.Error("stale finish promoted the queue")
	}
	if n := e.count(`SELECT count(*) FROM {s}run_queue`); n != 1 {
		t.Errorf("queued = %d, want 1", n)
	}
}

func TestFinishRunRejectsABadTransition(t *testing.T) {
	e := newEnv(t)
	first, _ := e.queuedPair()
	r := e.run(first)
	for _, f := range []Finish{
		{State: stateRunning, Name: "done", StepID: "done"},
		{State: "paused", Name: "done", StepID: "done"},
		{State: stateCompleted, StepID: "done"},
		{State: stateCompleted, Name: "done"},
	} {
		if err := e.finishWith(r, f); err == nil {
			t.Errorf("FinishRun(%+v) = nil, want an error", f)
		}
	}
	if r := e.run(first); r.Terminal || r.Rev != 1 {
		t.Errorf("rejected finish changed the run: terminal %v rev %d", r.Terminal, r.Rev)
	}
}

func TestFinishRunTwiceIsStale(t *testing.T) {
	e := newEnv(t)
	first, _ := e.queuedPair()
	r := e.run(first)
	if err := e.finishWith(r, Finish{State: stateCancelled, Name: "$cancelled", StepID: StepRun}); err != nil {
		t.Fatal(err)
	}
	if err := e.finishWith(r, Finish{State: stateCompleted, Name: "done", StepID: "done"}); !errors.Is(err, ErrStale) {
		t.Fatalf("second FinishRun = %v, want ErrStale", err)
	}
	if got := e.run(first); got.State != stateCancelled {
		t.Errorf("state = %s, want cancelled", got.State)
	}
}
