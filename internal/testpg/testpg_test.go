package testpg

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestNewSchemaNameIsValidAndUnique(t *testing.T) {
	seen := make(map[string]bool)
	for range 1000 {
		name, err := NewSchemaName()
		if err != nil {
			t.Fatal(err)
		}
		if !ValidSchemaName(name) {
			t.Fatalf("generated name %q is not valid", name)
		}
		if seen[name] {
			t.Fatalf("generated duplicate name %q", name)
		}
		seen[name] = true
	}
}

func TestValidSchemaName(t *testing.T) {
	cases := map[string]bool{
		"flowkit":               true,
		"_private":              true,
		"a1_b2":                 true,
		"public":                false,
		"pg_temp":               false,
		"Flowkit":               false,
		"1flowkit":              false,
		"flow-kit":              false,
		"":                      false,
		strings.Repeat("a", 46): true,
		strings.Repeat("a", 47): false,
	}
	for name, want := range cases {
		if got := ValidSchemaName(name); got != want {
			t.Errorf("ValidSchemaName(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestSchemaIsCreatedAndDropped(t *testing.T) {
	pool := Pool(t)
	ctx := context.Background()

	name, drop, err := CreateSchema(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if !schemaExists(t, pool, name) {
		t.Fatalf("schema %s missing after create", name)
	}
	if _, err := pool.Exec(ctx, "CREATE TABLE "+name+".probe (id int)"); err != nil {
		t.Fatal(err)
	}
	if err := drop(ctx); err != nil {
		t.Fatal(err)
	}
	if schemaExists(t, pool, name) {
		t.Fatalf("schema %s still present after drop", name)
	}
}

func schemaExists(t *testing.T, pool *pgxpool.Pool, name string) bool {
	t.Helper()
	var exists bool
	err := pool.QueryRow(context.Background(),
		"SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)", name).Scan(&exists)
	if err != nil {
		t.Fatal(err)
	}
	return exists
}
