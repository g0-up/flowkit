package flowkittest

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"

	"github.com/g0-up/flowkit"
)

// StubFunc stands in for an action's Execute in a Sim. It receives what
// Execute would, and its result and error go through the executor as
// Execute's would: an error wrapped with flowkit.NotPerformed is retried,
// any other error of a non-idempotent action leaves the effect in doubt.
//
// A stub runs on a goroutine of its own, not the test's. It may call
// t.Fatal or t.FailNow: the Sim then stops at once and fails the test,
// naming the action. A stub that panics has its stack logged to the test,
// and the panic goes through the executor as Execute's would.
//
// A stub must return before ctx ends, at the action deadline in real
// time: the Sim waits for it until then, and then fails the test, naming
// the action. A stub that blocks should therefore watch ctx.Done.
type StubFunc func(ctx context.Context, in flowkit.ActionInput) (flowkit.ActionResult, error)

// stubs holds the stubs of a Sim and the first failure to stop it with:
// an action that ran without a stub, or a stub that stopped its goroutine
// or outlived its context. logf logs to the test.
type stubs struct {
	mu     sync.Mutex
	fns    map[string]StubFunc
	failed error
	logf   func(format string, args ...any)
}

func newStubs(logf func(format string, args ...any)) *stubs {
	return &stubs{fns: map[string]StubFunc{}, logf: logf}
}

func (s *stubs) set(action string, fn StubFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fns[action] = fn
}

// execute runs the stub of action. An action without a stub, or whose
// stub stops its goroutine, is recorded for the Sim to fail the test with,
// and fails as not performed, so it has no effect to resolve. A stub still
// running when ctx ends is recorded too, and fails as Execute would: its
// effect may yet happen. A stub's panic is logged with the stub's stack,
// which the executor cannot see, and raised again for the executor to
// handle as Execute's.
func (s *stubs) execute(ctx context.Context, action string, in flowkit.ActionInput) (flowkit.ActionResult, error) {
	s.mu.Lock()
	fn := s.fns[action]
	s.mu.Unlock()
	if fn == nil {
		return flowkit.ActionResult{}, flowkit.NotPerformed(s.fail(
			fmt.Errorf("action %q ran without a stub; call StubAction(%q, fn) before it runs", action, action)))
	}
	r := runStub(ctx, fn, in)
	switch {
	case r.outlived:
		return flowkit.ActionResult{}, s.fail(
			fmt.Errorf("the stub of action %q did not return before its context ended: %w", action, context.Cause(ctx)))
	case r.panicked:
		s.logf("flowkittest: the stub of action %q panicked: %v\n%s", action, r.panic, r.stack)
		panic(r.panic)
	case r.stopped:
		return flowkit.ActionResult{}, flowkit.NotPerformed(s.fail(
			fmt.Errorf("the stub of action %q stopped its goroutine, by t.Fatal or t.FailNow", action)))
	}
	return r.out, r.err
}

// stubResult is how a stub ended: it returned out and err, panicked with
// panic at stack, stopped its goroutine with runtime.Goexit, or outlived
// its context.
type stubResult struct {
	out      flowkit.ActionResult
	err      error
	panicked bool
	panic    any
	stack    []byte
	stopped  bool
	outlived bool
}

// runStub runs fn on a goroutine of its own, so that t.FailNow in fn,
// which stops the goroutine calling it, does not stop the executor's. It
// waits for fn as the executor waits for Execute: until ctx ends, and no
// longer, leaving a stub that ignores ctx running.
func runStub(ctx context.Context, fn StubFunc, in flowkit.ActionInput) stubResult {
	done := make(chan stubResult, 1)
	go func() {
		returned := false
		defer func() {
			if returned {
				return
			}
			if v := recover(); v != nil {
				done <- stubResult{panicked: true, panic: v, stack: debug.Stack()}
				return
			}
			done <- stubResult{stopped: true}
		}()
		out, err := fn(ctx, in)
		returned = true
		done <- stubResult{out: out, err: err}
	}()
	select {
	case r := <-done:
		return r
	case <-ctx.Done():
		select {
		case r := <-done:
			return r
		default:
			return stubResult{outlived: true}
		}
	}
}

// fail records err as the failure of the Sim unless one is recorded
// already, and returns err.
func (s *stubs) fail(err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed == nil {
		s.failed = err
	}
	return err
}

// failure returns the first failure recorded.
func (s *stubs) failure() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failed
}
