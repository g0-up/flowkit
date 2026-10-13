package executor

import (
	"strconv"
	"time"
)

// ExecKind says how an execution request ended and what the caller does
// next.
type ExecKind int

const (
	// Done: the effect is recorded as done. Output holds the action's output;
	// Cached reports that it was recorded by an earlier request. A non-nil
	// Err means the output does not match the action's OutputFacts: the
	// effect happened, but the run cannot continue and must fail.
	Done ExecKind = iota + 1
	// RetryLater: request the same effect again after Delay. Err, when set,
	// is the error of the attempt that failed.
	RetryLater
	// Failed: the action failed and its attempts are used up. Err is the last
	// error.
	Failed
	// BlockedInDoubt: the effect cannot proceed until the janitor or an
	// operator decides; whoever does re-enqueues the run. When the action
	// succeeded after the effect went in doubt, its output is kept on the
	// effect and noted on its dead letter for whoever resolves it.
	BlockedInDoubt
	// Abandoned: the caller's context ended. When it ended while the action
	// ran, nothing was written after the claim and the janitor handles the
	// effect once the claim expires; when it ended between the claim and
	// Execute, the claim was released without counting an attempt.
	Abandoned
	// Errored: the executor's own bookkeeping failed, for example a database
	// error. Err says why. Any claim taken stays for the janitor; retry the
	// request with backoff.
	Errored
)

var kindNames = [...]string{
	Done:           "done",
	RetryLater:     "retry_later",
	Failed:         "failed",
	BlockedInDoubt: "blocked_in_doubt",
	Abandoned:      "abandoned",
	Errored:        "errored",
}

func (k ExecKind) String() string {
	if k > 0 && int(k) < len(kindNames) {
		return kindNames[k]
	}
	return "ExecKind(" + strconv.Itoa(int(k)) + ")"
}

// ExecOutcome is the result of Executor.Run.
type ExecOutcome struct {
	Kind ExecKind
	// Output is set with Done when the output is valid. Values are
	// normalised by fact type: bool, int64, float64, string, time.Time.
	Output map[string]any
	Cached bool
	Delay  time.Duration // with RetryLater; always positive
	// Err reports an action's failure (with RetryLater, Failed,
	// BlockedInDoubt, Abandoned, or Done with invalid output) as "action
	// <name>: " followed by the redacted text the executor stored, whether
	// the failure just happened or was read back, so it is safe to log and
	// store as it is. A fresh one also wraps the action's raw error for
	// errors.Is and errors.As; do not log what it unwraps to. Failed prefixes
	// it with how many attempts failed. With Errored, Abandoned before
	// Execute started, or an effect orphaned by a revoked claim, Err is the
	// executor's own error, such as a database or context error, and carries
	// no action error.
	Err error
}
