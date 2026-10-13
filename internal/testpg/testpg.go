// Package testpg gives Postgres-backed tests an isolated, randomly named schema
// on the database named by FLOWKIT_TEST_DATABASE_URL, and drops it afterwards.
//
// A dedicated variable is used instead of DATABASE_URL so tests never create or
// drop schemas on a developer's real database by accident.
package testpg

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EnvVar names the environment variable holding the test database DSN.
const EnvVar = "FLOWKIT_TEST_DATABASE_URL"

// schemaPrefix marks schemas created by tests so leftovers are easy to spot.
const schemaPrefix = "fkt_"

// cleanupTimeout bounds schema drops and pool shutdown during test cleanup.
const cleanupTimeout = 30 * time.Second

// validSchemaName mirrors the schema-name rule flowkit enforces for its own
// configuration: lowercase identifier, at most 46 bytes.
var validSchemaName = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,45}$`)

// DSN returns the test database DSN. It skips the test when the variable is
// unset, except under CI where a missing database is a configuration error.
func DSN(t testing.TB) string {
	t.Helper()
	dsn := os.Getenv(EnvVar)
	if dsn != "" {
		return dsn
	}
	if os.Getenv("CI") != "" {
		t.Fatalf("%s must be set in CI", EnvVar)
	}
	t.Skipf("%s not set; skipping Postgres-backed test", EnvVar)
	return ""
}

// Pool opens a pool on the test database and closes it when the test ends.
func Pool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(DSN(t))
	if err != nil {
		t.Fatalf("parse %s: %v", EnvVar, err)
	}
	return PoolWithConfig(t, cfg)
}

// PoolWithConfig opens a pool from cfg, verifies connectivity, and closes it
// when the test ends.
func PoolWithConfig(t testing.TB, cfg *pgxpool.Config) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("open test pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping test database: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// Schema creates a fresh schema on pool and drops it, with everything in it,
// when the test ends.
func Schema(t testing.TB, pool *pgxpool.Pool) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	name, drop, err := CreateSchema(ctx, pool)
	if err != nil {
		t.Fatalf("create test schema: %v", err)
	}
	t.Cleanup(func() {
		// The test context is already cancelled when cleanups run.
		ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if err := drop(ctx); err != nil {
			t.Errorf("drop test schema %s: %v", name, err)
		}
	})
	return name
}

// New opens a pool on the test database and creates a fresh schema on it.
func New(t testing.TB) (*pgxpool.Pool, string) {
	t.Helper()
	pool := Pool(t)
	return pool, Schema(t, pool)
}

// CreateSchema creates a randomly named schema and returns its name with a
// function that drops it. Callers outside a testing.TB own the drop.
func CreateSchema(ctx context.Context, pool *pgxpool.Pool) (string, func(context.Context) error, error) {
	name, err := NewSchemaName()
	if err != nil {
		return "", nil, err
	}
	ident := pgx.Identifier{name}.Sanitize()
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+ident); err != nil {
		return "", nil, fmt.Errorf("create schema %s: %w", name, err)
	}
	drop := func(ctx context.Context) error {
		if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+ident+" CASCADE"); err != nil {
			return fmt.Errorf("drop schema %s: %w", name, err)
		}
		return nil
	}
	return name, drop, nil
}

// NewSchemaName returns a random schema name that satisfies flowkit's schema
// name rule and cannot collide with pg_* or public.
func NewSchemaName() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate schema name: %w", err)
	}
	name := schemaPrefix + hex.EncodeToString(b[:])
	if !ValidSchemaName(name) {
		return "", fmt.Errorf("generated invalid schema name %q", name)
	}
	return name, nil
}

// ValidSchemaName reports whether name is acceptable as a flowkit schema.
func ValidSchemaName(name string) bool {
	return validSchemaName.MatchString(name) && !strings.HasPrefix(name, "pg_") && name != "public"
}
