// Package dbgate lets Postgres-backed tests assert that a flowkit schema still
// holds exactly the objects its migrations create, after the test has driven
// the engine against it.
package dbgate

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/g0-up/flowkit/internal/migrate"
)

// checkTimeout bounds the catalog reads of one Check.
const checkTimeout = 30 * time.Second

// Check fails t unless schema is migrated to this build and its objects match
// the golden listing in internal/migrate/testdata/db-objects.golden: nothing
// created at runtime, no function, procedure, trigger, rule, column default
// or custom type from flowkit, and River's objects exactly the signed
// allowlist.
//
// It reports with t.Errorf rather than stopping the test, so it is safe to
// call from t.Cleanup.
func Check(t testing.TB, pool *pgxpool.Pool, schema string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), checkTimeout)
	defer cancel()
	err := migrate.Check(ctx, pool, schema)
	if err == nil {
		return
	}
	var drift *migrate.DriftError
	if errors.As(err, &drift) {
		t.Errorf("schema %s fails the database object gate:\n  %s", schema, strings.Join(drift.Problems, "\n  "))
		return
	}
	t.Errorf("schema %s: database object gate: %v", schema, err)
}
