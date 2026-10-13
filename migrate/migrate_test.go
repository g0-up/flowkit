package migrate_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/g0-up/flowkit/internal/testpg"
	"github.com/g0-up/flowkit/migrate"
)

func TestRunAndStatus(t *testing.T) {
	pool := testpg.Pool(t)
	ctx := context.Background()
	schema, err := testpg.NewSchemaName()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Errorf("drop schema %s: %v", schema, err)
		}
	})

	before, err := migrate.Status(ctx, pool, schema)
	if err != nil {
		t.Fatal(err)
	}
	if before.SchemaVersion != 0 || before.RiverVersion != 0 || len(before.Pending) == 0 {
		t.Fatalf("status of a new schema = %+v", before)
	}
	if !strings.HasPrefix(before.Pending[0], "river:") || before.Pending[len(before.Pending)-1] != "flowkit:0001_init" {
		t.Fatalf("pending = %v, want River first and flowkit:0001_init last", before.Pending)
	}

	for range 2 {
		if err := migrate.Run(ctx, pool, schema, migrate.Options{}); err != nil {
			t.Fatal(err)
		}
	}
	after, err := migrate.Status(ctx, pool, schema)
	if err != nil {
		t.Fatal(err)
	}
	if after.SchemaVersion < 1 || after.MinLibSchema < 1 || after.RiverVersion == 0 || len(after.Pending) != 0 {
		t.Fatalf("status after Run = %+v", after)
	}
}

func TestRunRejectsInvalidSchemaNames(t *testing.T) {
	// The name is checked before the pool is used, so a nil pool is fine.
	for _, name := range []string{"", "public", "pg_flowkit", "Flowkit", "flow-kit", strings.Repeat("a", 47)} {
		if err := migrate.Run(context.Background(), nil, name, migrate.Options{}); err == nil {
			t.Errorf("Run accepted schema %q", name)
		}
		if _, err := migrate.Status(context.Background(), nil, name); err == nil {
			t.Errorf("Status accepted schema %q", name)
		}
	}
}

func TestRunRefusesAForeignSchema(t *testing.T) {
	pool, schema := testpg.New(t)
	ctx := context.Background()
	table := pgx.Identifier{schema, "app_table"}.Sanitize()
	if _, err := pool.Exec(ctx, "CREATE TABLE "+table+" (id int)"); err != nil {
		t.Fatal(err)
	}
	err := migrate.Run(ctx, pool, schema, migrate.Options{ConfirmNonEmpty: true})
	if !errors.Is(err, migrate.ErrForeignSchema) {
		t.Fatalf("Run on a schema without flowkit_meta = %v, want ErrForeignSchema", err)
	}
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", pgx.Identifier{schema, "flowkit_meta"}.Sanitize()).Scan(&exists); err != nil || exists {
		t.Fatalf("Run changed the foreign schema (flowkit_meta exists=%v, err=%v)", exists, err)
	}
}

func TestRunAsksForConfirmationOnASchemaWithData(t *testing.T) {
	pool, schema := testpg.New(t)
	ctx := context.Background()
	if err := migrate.Run(ctx, pool, schema, migrate.Options{}); err != nil {
		t.Fatal(err)
	}
	// Give the schema flowkit data and a pending migration: forgetting the
	// last River migration makes it pending again.
	_, err := pool.Exec(ctx, "INSERT INTO "+pgx.Identifier{schema, "definitions"}.Sanitize()+
		" (name, enabled, concurrency_policy, created_at, updated_at) VALUES ('d', true, 'concurrent', $1, $1)", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	riverMigration := pgx.Identifier{schema, "river_migration"}.Sanitize()
	_, err = pool.Exec(ctx, "DELETE FROM "+riverMigration+" WHERE version = (SELECT max(version) FROM "+riverMigration+")")
	if err != nil {
		t.Fatal(err)
	}
	err = migrate.Run(ctx, pool, schema, migrate.Options{})
	if !errors.Is(err, migrate.ErrConfirmationRequired) {
		t.Fatalf("Run with pending migrations on a schema with data = %v, want ErrConfirmationRequired", err)
	}
	if errors.Is(err, migrate.ErrForeignSchema) {
		t.Fatalf("Run error %v matches both sentinels", err)
	}
}
