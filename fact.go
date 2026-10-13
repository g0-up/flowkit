package flowkit

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// FactType is the type of a fact as definition expressions see it.
type FactType int

// Fact types. The zero FactType is invalid.
const (
	FactBool FactType = iota + 1
	FactInt
	FactDouble
	FactString
	FactTimestamp
)

func (t FactType) valid() bool { return t >= FactBool && t <= FactTimestamp }

// Scope identifies the entity a run is about. It is computed from the run's
// trigger event (EventDef.EntityKind, EntityIDField and ScopeFields), stored
// with the run, and never taken from definition expressions.
type Scope struct {
	EntityKind string
	EntityID   string
	// Extra holds the EventDef.ScopeFields values, keyed like ScopeFields.
	Extra map[string]string
}

// Querier is the read-only database access a fact resolver gets. Resolvers
// run in a READ ONLY transaction whose statement_timeout is
// Config.FactTimeout.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// FactDef registers a fact that definitions can read but no event or action
// supplies, such as an aggregate over domain tables. All fields are required.
type FactDef struct {
	// Name is the fact name expressions read, e.g. "customer.revenue_90d".
	Name string
	Type FactType
	// AvailableOn lists the trigger event types the fact can be resolved
	// for: an exact type ("order.paid"), a prefix pattern ("order.*"), or
	// "*" for every event. Each pattern must match a registered event.
	AvailableOn []string
	// Resolve computes the fact for a run's scope. It must return a value
	// of Type.
	Resolve func(ctx context.Context, q Querier, s Scope) (any, error)
}
