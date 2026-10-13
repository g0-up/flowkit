package dbgate_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/g0-up/flowkit/internal/dbgate"
	"github.com/g0-up/flowkit/internal/migrate"
	"github.com/g0-up/flowkit/internal/testpg"
)

// recorder captures the failures Check reports instead of failing the test.
type recorder struct {
	testing.TB
	errors []string
}

func (r *recorder) Helper() {}

func (r *recorder) Errorf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}

func TestCheckPassesOnMigratedSchema(t *testing.T) {
	pool, schema := testpg.New(t)
	if err := migrate.Run(context.Background(), pool, schema, migrate.Options{}); err != nil {
		t.Fatal(err)
	}
	rec := &recorder{TB: t}
	dbgate.Check(rec, pool, schema)
	if len(rec.errors) > 0 {
		t.Fatalf("fresh migrated schema fails the gate: %v", rec.errors)
	}
}

func TestCheckFailsOnExtraObjects(t *testing.T) {
	cases := []struct {
		name   string
		ddl    string
		expect string
	}{
		{"function", "CREATE FUNCTION %s.fk_extra() RETURNS integer LANGUAGE sql AS 'SELECT 1'", "object not on the River allowlist: function fk_extra()"},
		{"trigger", "CREATE TRIGGER fk_extra BEFORE UPDATE ON %s.runs FOR EACH ROW EXECUTE FUNCTION suppress_redundant_updates_trigger()", "object not on the River allowlist: trigger runs.fk_extra"},
		{"enum", "CREATE TYPE %s.fk_extra AS ENUM ('a')", "object not on the River allowlist: enum fk_extra"},
		{"table", "CREATE TABLE %s.fk_extra (id bigint)", "unexpected table fk_extra"},
		{"index", "CREATE INDEX fk_extra ON %s.runs (id)", "unexpected index fk_extra"},
		{"column default", "ALTER TABLE %s.runs ALTER COLUMN entity_kind SET DEFAULT 'x'", "object not on the River allowlist: column_default runs.entity_kind"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pool, schema := testpg.New(t)
			ctx := context.Background()
			if err := migrate.Run(ctx, pool, schema, migrate.Options{}); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, fmt.Sprintf(c.ddl, pgx.Identifier{schema}.Sanitize())); err != nil {
				t.Fatal(err)
			}
			rec := &recorder{TB: t}
			dbgate.Check(rec, pool, schema)
			if len(rec.errors) != 1 || !strings.Contains(rec.errors[0], c.expect) {
				t.Fatalf("gate errors %q, want one containing %q", rec.errors, c.expect)
			}
		})
	}
}

func TestCheckFailsOnUnmigratedSchema(t *testing.T) {
	pool, schema := testpg.New(t)
	rec := &recorder{TB: t}
	dbgate.Check(rec, pool, schema)
	if len(rec.errors) != 1 || !strings.Contains(rec.errors[0], "not migrated") {
		t.Fatalf("gate errors %q, want one saying the schema is not migrated", rec.errors)
	}
}
