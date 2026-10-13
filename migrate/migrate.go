package migrate

import (
	"context"
	"slices"

	"github.com/jackc/pgx/v5/pgxpool"

	imigrate "github.com/g0-up/flowkit/internal/migrate"
)

// Errors Run returns wrapped, so callers can test them with errors.Is.
var (
	// ErrConfirmationRequired means migrations are pending on a schema that
	// already holds flowkit data and Options.ConfirmNonEmpty is false. Back
	// the schema up, then run again with ConfirmNonEmpty set.
	ErrConfirmationRequired = imigrate.ErrConfirmationRequired

	// ErrForeignSchema means the schema has objects but no flowkit_meta
	// table, so it does not belong to flowkit. Run changes nothing in it.
	ErrForeignSchema = imigrate.ErrForeignSchema
)

// Options tunes Run.
type Options struct {
	// ConfirmNonEmpty allows migrations to run on a schema that already holds
	// flowkit data. Set it only after backing the schema up
	// (pg_dump -n <schema>).
	ConfirmNonEmpty bool
}

// State is the migration state of a schema.
type State struct {
	SchemaVersion int      // flowkit migrations applied
	MinLibSchema  int      // oldest library schema that can run on it (flowkit_meta.min_lib_schema)
	RiverVersion  int      // River migrations applied
	Pending       []string // migrations not applied yet, River first, e.g. "river:006_..." and "flowkit:0001_init"
}

// Run brings schema up to date: it creates the schema if needed, applies
// River's migrations and then flowkit's, each in its own transaction, while
// holding a row lock in flowkit_meta so concurrent runners apply each
// migration once. A schema that has objects but no flowkit_meta table is
// refused with ErrForeignSchema. When migrations are pending on a schema that
// already holds flowkit data, Run refuses with ErrConfirmationRequired unless
// opts.ConfirmNonEmpty is set.
//
// The schema name must be a lowercase identifier of at most 46 bytes, not
// public, information_schema or pg_*; it is checked before any statement is
// sent. pool needs at least two connections, one per concurrent runner plus
// one.
func Run(ctx context.Context, pool *pgxpool.Pool, schema string, opts Options) error {
	return imigrate.Run(ctx, pool, schema, imigrate.Options{ConfirmNonEmpty: opts.ConfirmNonEmpty})
}

// Status reads the migration state of schema without changing anything. A
// schema that does not exist reports every migration as pending.
func Status(ctx context.Context, pool *pgxpool.Pool, schema string) (State, error) {
	st, err := imigrate.Status(ctx, pool, schema)
	if err != nil {
		return State{}, err
	}
	return State{
		SchemaVersion: st.SchemaVersion,
		MinLibSchema:  st.MinLibSchema,
		RiverVersion:  st.RiverVersion,
		Pending:       slices.Clone(st.Pending),
	}, nil
}
