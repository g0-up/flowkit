// Package flowkit is an embeddable, Postgres-backed workflow engine.
//
// Domain code registers events, actions and facts, publishes workflow
// definitions, and emits events inside its own database transactions; flowkit
// routes those events to runs and executes actions with claim-then-execute
// semantics. Background work is scheduled through River in a configurable
// Postgres schema.
package flowkit
