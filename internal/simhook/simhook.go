// Package simhook lets flowkittest build an Engine on an in-memory store
// and scheduler without a public constructor. The flowkit package assigns
// NewEngine when it is initialised; only flowkittest calls it.
//
// The flowkit types cross the hook as any, because flowkit imports this
// package: Options and Engine document the type each field holds.
package simhook

import (
	"context"

	"github.com/g0-up/flowkit/internal/executor"
	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/sched"
	"github.com/g0-up/flowkit/internal/store"
)

// Store is the persistence of an in-memory Engine.
type Store interface {
	store.Store
	executor.Store
}

// Jobs enqueues jobs in a store transaction, visible once it commits.
type Jobs interface {
	EnqueueRoute(ctx context.Context, tx store.Tx, eventID int64) error
	EnqueuePump(ctx context.Context, tx store.Tx, runID ids.UUID) error
	EnqueuePumpMany(ctx context.Context, tx store.Tx, runIDs []ids.UUID) error
	ScheduleTimeout(ctx context.Context, tx store.Tx, args sched.WaitTimeoutArgs) error
}

// Options configures an in-memory Engine.
type Options struct {
	// Config is a flowkit.Config. Its defaults apply; Pool and Schema are
	// not used.
	Config any
	// Registry is a *flowkit.Registry, copied as flowkit.New copies it.
	Registry any
	Store    Store
	Jobs     Jobs
	// Execute runs every action in place of its registered Execute, which
	// the Engine never calls. It is a
	// func(ctx context.Context, action string, in flowkit.ActionInput) (flowkit.ActionResult, error)
	// and is required.
	Execute any
}

// Engine is an in-memory Engine with what the simulator drives it by.
type Engine struct {
	// Engine is the *flowkit.Engine.
	Engine any
	// Handlers run the Engine's jobs; hand them to the scheduler with
	// Timing, the job timeouts derived from the Engine's ClaimTimeout.
	Handlers sched.Handlers
	Timing   sched.Timing
	// Emit emits evt, a flowkit.Event, in tx as Engine.Emit does.
	Emit func(ctx context.Context, tx store.Tx, evt any) error
}

// NewEngine builds an in-memory Engine. The flowkit package sets it.
var NewEngine func(Options) (Engine, error)
