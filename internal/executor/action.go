package executor

import (
	"context"
	"time"
)

// Action is what the executor needs to know about a registered action. Its
// RetryPolicy, OutputFact and Scope mirror the public flowkit types field
// for field, so the engine converts them directly.
type Action struct {
	Name string
	// Idempotent selects how a failed execution is handled: an idempotent
	// action is retried after any error, a non-idempotent one only after an
	// error IsNotPerformed recognises; otherwise the effect goes in doubt.
	Idempotent  bool
	Retry       RetryPolicy
	OutputFacts []OutputFact
	// Execute performs the action. Its context carries the action deadline.
	Execute func(ctx context.Context, in Input) (map[string]any, error)
}

// RetryPolicy bounds the retries of an action. Zero fields select the
// defaults (DefaultMaxAttempts, DefaultBaseBackoff, DefaultMaxBackoff).
type RetryPolicy struct {
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

// FactType is the type of an output fact. Its values equal those of
// flowkit.FactType.
type FactType int

// Output fact types.
const (
	FactBool FactType = iota + 1
	FactInt
	FactDouble
	FactString
	FactTimestamp
)

// OutputFact is a key an action's output must have, with its type.
type OutputFact struct {
	Name string
	Type FactType
}

// Scope is the entity scope of a run.
type Scope struct {
	EntityKind string
	EntityID   string
	Extra      map[string]string
}

// Input is what Action.Execute receives.
type Input struct {
	Params  map[string]any
	IdemKey string
	RunID   string
	StepID  string
	Scope   Scope
}
