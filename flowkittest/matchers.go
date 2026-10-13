package flowkittest

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/g0-up/flowkit"
)

// Step kinds and terminal names the matchers read in a trace.
const (
	kindSkipped       = "skipped"
	kindExecuted      = "executed"
	kindWaiting       = "waiting"
	kindTerminal      = "terminal"
	kindAttemptFailed = "attempt_failed"
	kindEffectInDoubt = "effect_in_doubt"
	stateWaiting      = "waiting"
	stateFailed       = "failed"
	actionFailed      = "$action_failed"
)

// Dead letter kinds a run can have.
const (
	deadLetterInDoubt   = "effect_in_doubt"
	deadLetterExhausted = "exhausted"
)

// traceTimeout bounds the read of a trace.
const traceTimeout = 10 * time.Second

// Matcher checks one property of a run's trace. Build matchers with the
// functions of this package and check them with AssertTrace.
type Matcher struct {
	// desc says what the matcher expects.
	desc string
	// check returns "" when the trace matches, and what it found instead
	// otherwise.
	check func(tr flowkit.RunTrace) string
}

// traceSource reads run traces. *flowkit.Runs is one, on Postgres or on
// the simulator's store.
type traceSource interface {
	Trace(ctx context.Context, runID string) (flowkit.RunTrace, error)
}

// AssertTrace reads the trace of run runID from runs and reports through
// t.Errorf every matcher it does not satisfy, with the trace itself. It
// stops the test when the trace cannot be read.
//
// Pass PGHarness.Engine().Runs() or Sim.Runs(); the matchers see the same
// flowkit.RunTrace from both.
func AssertTrace(t testing.TB, runs *flowkit.Runs, runID string, ms ...Matcher) {
	t.Helper()
	if runs == nil {
		t.Fatalf("flowkittest: AssertTrace: nil Runs")
	}
	assertTrace(t, runs, runID, ms)
}

func assertTrace(t testing.TB, src traceSource, runID string, ms []Matcher) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), traceTimeout)
	defer cancel()
	tr, err := src.Trace(ctx, runID)
	if err != nil {
		t.Fatalf("flowkittest: AssertTrace: trace of run %s: %v", runID, err)
	}
	if msg := mismatches(tr, ms); msg != "" {
		t.Errorf("%s", msg)
	}
}

// mismatches returns the failure report of the matchers tr does not
// satisfy, or "" when it satisfies all of them.
func mismatches(tr flowkit.RunTrace, ms []Matcher) string {
	var b strings.Builder
	for _, m := range ms {
		if m.check == nil {
			b.WriteString("  - a zero Matcher: build matchers with the functions of flowkittest\n")
			continue
		}
		if got := m.check(tr); got != "" {
			fmt.Fprintf(&b, "  - want: %s\n    got:  %s\n", m.desc, got)
		}
	}
	if b.Len() == 0 {
		return ""
	}
	return fmt.Sprintf("flowkittest: trace of run %s does not match:\n%strace:\n%s", tr.Run.RunID, b.String(), formatTrace(tr))
}

// formatTrace renders a trace one record per line.
func formatTrace(tr flowkit.RunTrace) string {
	var b strings.Builder
	r := tr.Run
	fmt.Fprintf(&b, "  run %s %s v%d: %s", r.DefName, r.RunID, r.DefVersion, r.State)
	if r.TerminalName != "" {
		fmt.Fprintf(&b, " %s", r.TerminalName)
	}
	fmt.Fprintf(&b, ", trigger %s\n", r.TriggerEventType)
	for _, s := range tr.Steps {
		line := fmt.Sprintf("  %3d %-16s %-16s", s.Seq, s.StepID, s.Kind)
		if s.Detail != "" {
			line += fmt.Sprintf(" %q", s.Detail)
		}
		if s.Attempt != 0 {
			line += fmt.Sprintf(" attempt %d", s.Attempt)
		}
		if s.Error != "" {
			line += fmt.Sprintf(" error %q", s.Error)
		}
		b.WriteString(strings.TrimRight(line, " "))
		b.WriteByte('\n')
	}
	return b.String()
}

// StepSkipped matches a run that skipped step stepID for reason: the trace
// has a skipped record of the step whose detail starts with reason. A step
// is skipped because its when expression is false, with reason "when". An
// empty reason matches any skip.
func StepSkipped(stepID, reason string) Matcher {
	return Matcher{
		desc: fmt.Sprintf("step %q skipped (%s)", stepID, reason),
		check: func(tr flowkit.RunTrace) string {
			for _, s := range tr.Steps {
				if s.StepID == stepID && s.Kind == kindSkipped && strings.HasPrefix(s.Detail, reason) {
					return ""
				}
			}
			return stepRecords(tr, stepID)
		},
	}
}

// StepExecuted matches a run that executed the action of step stepID at
// least once.
func StepExecuted(stepID string) Matcher {
	return Matcher{
		desc: fmt.Sprintf("step %q executed", stepID),
		check: func(tr flowkit.RunTrace) string {
			if countKind(tr, stepID, kindExecuted) > 0 {
				return ""
			}
			return stepRecords(tr, stepID)
		},
	}
}

// StepExecutedTimes matches a run that executed the action of step stepID
// exactly n times; n may be 0. A retried action counts once: an execution
// is a successful one.
func StepExecutedTimes(stepID string, n int) Matcher {
	return Matcher{
		desc: fmt.Sprintf("step %q executed %d times", stepID, n),
		check: func(tr flowkit.RunTrace) string {
			if got := countKind(tr, stepID, kindExecuted); got != n {
				return fmt.Sprintf("executed %d times; %s", got, stepRecords(tr, stepID))
			}
			return ""
		},
	}
}

// RunTerminal matches a run that ended with terminal name name: the name
// of the end step it reached, or a reserved name such as $action_failed.
func RunTerminal(name string) Matcher {
	return Matcher{
		desc: fmt.Sprintf("run ended %q", name),
		check: func(tr flowkit.RunTrace) string {
			if tr.Run.TerminalName == name {
				return ""
			}
			return runState(tr)
		},
	}
}

// RunWaiting matches a run that waits at the wait_for_event step stepID.
func RunWaiting(stepID string) Matcher {
	return Matcher{
		desc: fmt.Sprintf("run waiting at step %q", stepID),
		check: func(tr flowkit.RunTrace) string {
			if tr.Run.State != stateWaiting {
				return runState(tr)
			}
			if s, ok := lastOfKind(tr, kindWaiting); ok && s.StepID == stepID {
				return ""
			}
			return "run waiting; " + kindRecords(tr, kindWaiting)
		},
	}
}

// RunFailed matches a failed run whose terminal name is reason, such as
// $action_failed or $failed, or whose recorded error contains reason. An
// empty reason matches any failed run.
func RunFailed(reason string) Matcher {
	return Matcher{
		desc: fmt.Sprintf("run failed (%s)", reason),
		check: func(tr flowkit.RunTrace) string {
			if tr.Run.State != stateFailed {
				return runState(tr)
			}
			if tr.Run.TerminalName == reason {
				return ""
			}
			if s, ok := lastOfKind(tr, kindTerminal); ok && strings.Contains(s.Error, reason) {
				return ""
			}
			return runState(tr)
		},
	}
}

// DeadLetter matches a run that produced a dead letter of kind:
// effect_in_doubt, an action that may or may not have taken effect, or
// exhausted, an action that failed on every attempt. The other kinds,
// unroutable_event and schema_mismatch, belong to an event, not a run, and
// never match.
//
// The trace shows these dead letters: an effect_in_doubt record, and a run
// failed $action_failed right after the last failed attempt of the step,
// with the error that counts the attempts.
func DeadLetter(kind string) Matcher {
	return Matcher{
		desc: fmt.Sprintf("dead letter %s", kind),
		check: func(tr flowkit.RunTrace) string {
			switch kind {
			case deadLetterInDoubt:
				if _, ok := lastOfKind(tr, kindEffectInDoubt); ok {
					return ""
				}
				return "no effect_in_doubt record; " + runState(tr)
			case deadLetterExhausted:
				if exhausted(tr) {
					return ""
				}
				return "no action failed on its last attempt; " + runState(tr)
			default:
				return fmt.Sprintf("a run has no dead letter of kind %q; want %s or %s", kind, deadLetterInDoubt, deadLetterExhausted)
			}
		},
	}
}

// exhausted reports whether the run failed because an action used up its
// attempts: the terminal record follows a failed attempt of the same step,
// and its error is the executor's "action <name> failed <n> times".
func exhausted(tr flowkit.RunTrace) bool {
	if tr.Run.TerminalName != actionFailed {
		return false
	}
	for i := len(tr.Steps) - 1; i > 0; i-- {
		end, last := tr.Steps[i], tr.Steps[i-1]
		if end.Kind != kindTerminal {
			continue
		}
		return last.Kind == kindAttemptFailed && last.StepID == end.StepID &&
			strings.HasPrefix(end.Error, "action "+last.Detail+" failed "+strconv.Itoa(last.Attempt)+" times")
	}
	return false
}

func countKind(tr flowkit.RunTrace, stepID, kind string) int {
	n := 0
	for _, s := range tr.Steps {
		if s.StepID == stepID && s.Kind == kind {
			n++
		}
	}
	return n
}

func lastOfKind(tr flowkit.RunTrace, kind string) (flowkit.StepRecord, bool) {
	for i := len(tr.Steps) - 1; i >= 0; i-- {
		if tr.Steps[i].Kind == kind {
			return tr.Steps[i], true
		}
	}
	return flowkit.StepRecord{}, false
}

// stepRecords lists the record kinds of step stepID.
func stepRecords(tr flowkit.RunTrace, stepID string) string {
	var kinds []string
	for _, s := range tr.Steps {
		if s.StepID == stepID {
			k := s.Kind
			if s.Detail != "" {
				k += " " + strconv.Quote(s.Detail)
			}
			kinds = append(kinds, k)
		}
	}
	if len(kinds) == 0 {
		return fmt.Sprintf("step %q has no record", stepID)
	}
	return fmt.Sprintf("step %q records: %s", stepID, strings.Join(kinds, ", "))
}

// kindRecords lists the steps with a record of kind.
func kindRecords(tr flowkit.RunTrace, kind string) string {
	var steps []string
	for _, s := range tr.Steps {
		if s.Kind == kind {
			steps = append(steps, strconv.Quote(s.StepID))
		}
	}
	if len(steps) == 0 {
		return "no " + kind + " record"
	}
	return kind + " records at " + strings.Join(steps, ", ")
}

// runState describes the run's state and terminal name and error.
func runState(tr flowkit.RunTrace) string {
	s := "run " + tr.Run.State
	if tr.Run.TerminalName != "" {
		s += " " + strconv.Quote(tr.Run.TerminalName)
	}
	if end, ok := lastOfKind(tr, kindTerminal); ok && end.Error != "" {
		s += " with error " + strconv.Quote(end.Error)
	}
	return s
}
