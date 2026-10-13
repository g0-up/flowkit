package flowkittest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/g0-up/flowkit"
)

// trStep builds a trace record.
func trStep(stepID, kind, detail string) flowkit.StepRecord {
	return flowkit.StepRecord{StepID: stepID, Kind: kind, Detail: detail}
}

// trOf numbers steps into the trace of a run in state with terminal name
// terminal.
func trOf(state, terminal string, steps ...flowkit.StepRecord) flowkit.RunTrace {
	for i := range steps {
		steps[i].Seq = i + 1
	}
	return flowkit.RunTrace{
		Run:   flowkit.RunSummary{RunID: "r-1", DefName: "onboarding", DefVersion: 1, State: state, TerminalName: terminal, TriggerEventType: "student.registered"},
		Steps: steps,
	}
}

// trFailed is an attempt_failed record of an action at its attempt.
func trFailed(stepID, action string, attempt int, err string) flowkit.StepRecord {
	s := trStep(stepID, kindAttemptFailed, action)
	s.Attempt, s.Error = attempt, err
	return s
}

// trEnded is the terminal record of a run that ended name with err.
func trEnded(stepID, name, err string) flowkit.StepRecord {
	s := trStep(stepID, kindTerminal, name)
	s.Error = err
	return s
}

var (
	// skippedVIP skips vip-slack and completes onboarded.
	skippedVIP = trOf("completed", "onboarded",
		trStep("welcome", "entered", ""),
		trStep("welcome", kindExecuted, "send_email"),
		trStep("first_login", kindWaiting, ""),
		trStep("first_login", "claimed", "timeout"),
		trStep("vip-slack", kindSkipped, "when is false"),
		trEnded("done", "onboarded", ""),
	)
	// waitingLogin waits at first_login.
	waitingLogin = trOf("waiting", "",
		trStep("welcome", kindExecuted, "send_email"),
		trStep("first_login", kindWaiting, ""),
	)
	// exhaustedEmail used up the attempts of send_email at welcome.
	exhaustedEmail = trOf("failed", actionFailed,
		trFailed("welcome", "send_email", 1, "action send_email: timeout"),
		trFailed("welcome", "send_email", 2, "action send_email: timeout"),
		trEnded("welcome", actionFailed, "action send_email failed 2 times: action send_email: timeout"),
	)
	// inDoubtEmail lost track of whether send_email took effect.
	inDoubtEmail = trOf("failed", actionFailed,
		trStep("welcome", kindEffectInDoubt, "send_email"),
		trEnded("welcome", actionFailed, "action send_email: effect in doubt"),
	)
	// badOutput failed on output that does not match the action's facts.
	badOutput = trOf("failed", actionFailed,
		trEnded("welcome", actionFailed, `action send_email: output does not match OutputFacts: "sent" is missing`),
	)
	// retriedEmail failed once, then executed.
	retriedEmail = trOf("completed", "onboarded",
		trFailed("welcome", "send_email", 1, "action send_email: timeout"),
		trStep("welcome", kindExecuted, "send_email"),
		trEnded("done", "onboarded", ""),
	)
)

func TestMatchers(t *testing.T) {
	for _, tc := range []struct {
		name  string
		m     Matcher
		match []flowkit.RunTrace
		miss  []flowkit.RunTrace
	}{
		{"StepSkipped when", StepSkipped("vip-slack", "when"), []flowkit.RunTrace{skippedVIP}, []flowkit.RunTrace{waitingLogin}},
		{"StepSkipped any reason", StepSkipped("vip-slack", ""), []flowkit.RunTrace{skippedVIP}, []flowkit.RunTrace{waitingLogin}},
		{"StepSkipped other reason", StepSkipped("vip-slack", "deadline"), nil, []flowkit.RunTrace{skippedVIP}},
		{"StepSkipped other step", StepSkipped("welcome", "when"), nil, []flowkit.RunTrace{skippedVIP}},
		{"StepExecuted", StepExecuted("welcome"), []flowkit.RunTrace{skippedVIP, waitingLogin, retriedEmail}, []flowkit.RunTrace{exhaustedEmail, inDoubtEmail}},
		{"StepExecuted skipped step", StepExecuted("vip-slack"), nil, []flowkit.RunTrace{skippedVIP}},
		{"StepExecutedTimes 1", StepExecutedTimes("welcome", 1), []flowkit.RunTrace{skippedVIP, retriedEmail}, []flowkit.RunTrace{exhaustedEmail}},
		{"StepExecutedTimes 0", StepExecutedTimes("welcome", 0), []flowkit.RunTrace{exhaustedEmail, badOutput}, []flowkit.RunTrace{skippedVIP}},
		{"StepExecutedTimes 2", StepExecutedTimes("welcome", 2), nil, []flowkit.RunTrace{skippedVIP, retriedEmail}},
		{"RunTerminal", RunTerminal("onboarded"), []flowkit.RunTrace{skippedVIP, retriedEmail}, []flowkit.RunTrace{waitingLogin, exhaustedEmail}},
		{"RunTerminal reserved", RunTerminal(actionFailed), []flowkit.RunTrace{exhaustedEmail, badOutput}, []flowkit.RunTrace{skippedVIP}},
		{"RunWaiting", RunWaiting("first_login"), []flowkit.RunTrace{waitingLogin}, []flowkit.RunTrace{skippedVIP}},
		{"RunWaiting other step", RunWaiting("welcome"), nil, []flowkit.RunTrace{waitingLogin}},
		{"RunFailed terminal name", RunFailed(actionFailed), []flowkit.RunTrace{exhaustedEmail, inDoubtEmail, badOutput}, []flowkit.RunTrace{skippedVIP, waitingLogin}},
		{"RunFailed error", RunFailed("does not match OutputFacts"), []flowkit.RunTrace{badOutput}, []flowkit.RunTrace{exhaustedEmail}},
		{"RunFailed any", RunFailed(""), []flowkit.RunTrace{badOutput}, []flowkit.RunTrace{retriedEmail}},
		{"DeadLetter exhausted", DeadLetter("exhausted"), []flowkit.RunTrace{exhaustedEmail}, []flowkit.RunTrace{inDoubtEmail, badOutput, retriedEmail, skippedVIP}},
		{"DeadLetter effect_in_doubt", DeadLetter("effect_in_doubt"), []flowkit.RunTrace{inDoubtEmail}, []flowkit.RunTrace{exhaustedEmail, badOutput, skippedVIP}},
		{"DeadLetter of an event", DeadLetter("unroutable_event"), nil, []flowkit.RunTrace{exhaustedEmail, inDoubtEmail}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, tr := range tc.match {
				if got := tc.m.check(tr); got != "" {
					t.Errorf("%s does not match, got %s:\n%s", tc.m.desc, got, formatTrace(tr))
				}
			}
			for _, tr := range tc.miss {
				if got := tc.m.check(tr); got == "" {
					t.Errorf("%s matches:\n%s", tc.m.desc, formatTrace(tr))
				}
			}
		})
	}
}

func TestDeadLetterExhaustedNeedsTheLastAttemptBeforeTheEnd(t *testing.T) {
	for name, tr := range map[string]flowkit.RunTrace{
		"another step failed last": trOf("failed", actionFailed,
			trFailed("notify", "notify_slack", 1, "x"),
			trEnded("welcome", actionFailed, "action send_email failed 1 times: x"),
		),
		"attempt count differs": trOf("failed", actionFailed,
			trFailed("welcome", "send_email", 1, "x"),
			trEnded("welcome", actionFailed, "action send_email failed 3 times: x"),
		),
		"no failed attempt": trOf("failed", actionFailed,
			trEnded("welcome", actionFailed, "action send_email failed 1 times: x"),
		),
	} {
		if DeadLetter("exhausted").check(tr) == "" {
			t.Errorf("%s: DeadLetter(exhausted) matches:\n%s", name, formatTrace(tr))
		}
	}
	// A claim lost after the run ended does not hide the dead letter.
	late := exhaustedEmail
	late.Steps = append(append([]flowkit.StepRecord(nil), late.Steps...), trStep("first_login", "claim_lost", ""))
	if got := DeadLetter("exhausted").check(late); got != "" {
		t.Errorf("DeadLetter(exhausted) does not match after a later record: %s", got)
	}
}

// fakeTraces serves one trace, or an error.
type fakeTraces struct {
	tr  flowkit.RunTrace
	err error
}

func (f fakeTraces) Trace(_ context.Context, runID string) (flowkit.RunTrace, error) {
	if f.err != nil {
		return flowkit.RunTrace{}, f.err
	}
	if runID != f.tr.Run.RunID {
		return flowkit.RunTrace{}, fmt.Errorf("run %s not found", runID)
	}
	return f.tr, nil
}

// recordTB records Errorf and, like stopTB, Fatalf.
type recordTB struct {
	stopTB
	errs []string
}

func (r *recordTB) Errorf(format string, args ...any) {
	r.errs = append(r.errs, fmt.Sprintf(format, args...))
}

func TestAssertTracePassesWhenEveryMatcherMatches(t *testing.T) {
	rtb := &recordTB{stopTB: stopTB{TB: t}}
	fatal := rtb.fatalOf(func() {
		assertTrace(rtb, fakeTraces{tr: skippedVIP}, "r-1", []Matcher{
			StepSkipped("vip-slack", "when"), StepExecutedTimes("welcome", 1), RunTerminal("onboarded"),
		})
	})
	if fatal != "" || len(rtb.errs) != 0 {
		t.Errorf("AssertTrace failed: fatal %q, errors %q", fatal, rtb.errs)
	}
}

func TestAssertTraceReportsEachMismatchWithTheTrace(t *testing.T) {
	rtb := &recordTB{stopTB: stopTB{TB: t}}
	rtb.fatalOf(func() {
		assertTrace(rtb, fakeTraces{tr: skippedVIP}, "r-1", []Matcher{
			StepSkipped("vip-slack", "when"), StepExecuted("vip-slack"), RunTerminal("nudged"), {},
		})
	})
	if len(rtb.errs) != 1 {
		t.Fatalf("AssertTrace reported %d errors, want 1: %q", len(rtb.errs), rtb.errs)
	}
	got := rtb.errs[0]
	want := `flowkittest: trace of run r-1 does not match:
  - want: step "vip-slack" executed
    got:  step "vip-slack" records: skipped "when is false"
  - want: run ended "nudged"
    got:  run completed "onboarded"
  - a zero Matcher: build matchers with the functions of flowkittest
trace:
  run onboarding r-1 v1: completed onboarded, trigger student.registered
    1 welcome          entered
    2 welcome          executed         "send_email"
    3 first_login      waiting
    4 first_login      claimed          "timeout"
    5 vip-slack        skipped          "when is false"
    6 done             terminal         "onboarded"
`
	if got != want {
		t.Errorf("AssertTrace report:\n%s\nwant:\n%s", got, want)
	}
}

func TestAssertTraceStopsWhenTheTraceCannotBeRead(t *testing.T) {
	rtb := &recordTB{stopTB: stopTB{TB: t}}
	fatal := rtb.fatalOf(func() {
		assertTrace(rtb, fakeTraces{err: errors.New("connection refused")}, "r-1", []Matcher{RunTerminal("onboarded")})
	})
	if !strings.Contains(fatal, "trace of run r-1: connection refused") {
		t.Errorf("AssertTrace fatal = %q, want the read error", fatal)
	}
	fatal = rtb.fatalOf(func() { AssertTrace(rtb, nil, "r-1", RunTerminal("onboarded")) })
	if !strings.Contains(fatal, "nil Runs") {
		t.Errorf("AssertTrace(nil) fatal = %q", fatal)
	}
}

func TestFormatTraceShowsAttemptsAndErrors(t *testing.T) {
	got := formatTrace(exhaustedEmail)
	for _, want := range []string{
		"run onboarding r-1 v1: failed $action_failed, trigger student.registered\n",
		`attempt_failed   "send_email" attempt 2 error "action send_email: timeout"`,
		`terminal         "$action_failed" error "action send_email failed 2 times: action send_email: timeout"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("formatTrace does not contain %q:\n%s", want, got)
		}
	}
}
