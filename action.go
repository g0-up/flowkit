package flowkit

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// ActionDef registers an action that workflow steps can call. Name,
// ParamsSchema and Execute are required.
type ActionDef struct {
	// Name identifies the action in definitions, e.g. "email.send".
	Name string
	// ParamsSchema is the JSON Schema the resolved step params must satisfy.
	// Use {"type": "object"} for an action that takes any params.
	ParamsSchema json.RawMessage
	// Retry bounds how often a failed execution is retried.
	Retry RetryPolicy
	// Idempotent states that executing the action twice with the same
	// IdempotencyKey has the effect of executing it once. It decides what
	// flowkit does after an error or an in-doubt execution: an idempotent
	// action is retried; a non-idempotent action is retried only when its
	// error is wrapped with NotPerformed, and otherwise goes to a dead letter.
	Idempotent bool
	// OutputFacts declares the keys of ActionResult.Output. Each becomes a
	// fact the steps after the action can read.
	OutputFacts []OutputFact
	// Execute performs the action. Its context carries a deadline shorter
	// than the claim timeout.
	Execute func(ctx context.Context, in ActionInput) (ActionResult, error)
}

// OutputFact is a fact an action produces.
type OutputFact struct {
	// Name is the fact name later steps read, as facts.<Name>.
	Name string
	Type FactType
}

// RetryPolicy bounds the retries of an action. Zero values select flowkit's
// defaults.
type RetryPolicy struct {
	// MaxAttempts is the number of executions, the first included.
	MaxAttempts int
	// BaseBackoff is the delay before the first retry; later retries back
	// off exponentially up to MaxBackoff.
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

// ActionInput is what Execute receives.
type ActionInput struct {
	// Params are the step params, validated against ParamsSchema with every
	// expression resolved.
	Params map[string]any
	// IdempotencyKey is stable across retries of the same step visit:
	// runID:stepID:visit, where visit counts how often the run entered the
	// step. Pass it to external systems that deduplicate requests.
	IdempotencyKey string
	RunID          string
	StepID         string
	// Scope is the entity scope of the run, taken from its trigger event. It
	// never comes from definition expressions.
	Scope Scope
}

// ActionResult is what Execute returns on success.
type ActionResult struct {
	// Output must match the action's OutputFacts; it is merged into the
	// run's facts.
	Output map[string]any
}

// NotPerformed wraps err to assert that the action certainly had no external
// effect, for example because the request was rejected before it was sent. A
// non-idempotent action is retried only when its error is wrapped with
// NotPerformed; any other error, a panic or a missed deadline leaves the
// execution in doubt and sends it to a dead letter. NotPerformed(nil) is nil.
func NotPerformed(err error) error {
	if err == nil {
		return nil
	}
	return &notPerformedError{err: err}
}

type notPerformedError struct{ err error }

func (e *notPerformedError) Error() string { return "not performed: " + e.err.Error() }
func (e *notPerformedError) Unwrap() error { return e.err }

// isNotPerformed reports whether err, or any error it wraps, came from
// NotPerformed.
func isNotPerformed(err error) bool {
	var np *notPerformedError
	return errors.As(err, &np)
}
