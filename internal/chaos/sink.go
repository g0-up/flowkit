// Package chaos kills flowkit processes at their crash points and checks
// what a restarted instance recovers. The tests run only against Postgres;
// this file holds the external system the test actions write to.
package chaos

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Sink is the external effect of the test actions: a chaos_sink table that
// counts, per idempotency key, how often an action wrote to it. It lives in
// a schema of its own, outside the flowkit schema under test, and every
// write commits on its own connection, outside any flowkit transaction, so
// an effect persists exactly as a real external call would.
type Sink struct {
	pool  *pgxpool.Pool
	table string
}

// NewSink returns the sink in schema, reached through pool.
func NewSink(pool *pgxpool.Pool, schema string) *Sink {
	return &Sink{pool: pool, table: pgx.Identifier{schema, "chaos_sink"}.Sanitize()}
}

// Create creates the sink table.
func (s *Sink) Create(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, "CREATE TABLE "+s.table+" (idem_key text PRIMARY KEY, n int NOT NULL)"); err != nil {
		return fmt.Errorf("chaos: create sink: %w", err)
	}
	return nil
}

// Record counts one write for key and returns the count.
func (s *Sink) Record(ctx context.Context, key string) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, "INSERT INTO "+s.table+" AS s (idem_key, n) VALUES ($1, 1)"+
		" ON CONFLICT (idem_key) DO UPDATE SET n = s.n + 1 RETURNING n", key).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("chaos: record %s: %w", key, err)
	}
	return n, nil
}

// Counts returns the count of every key written.
func (s *Sink) Counts(ctx context.Context) (map[string]int, error) {
	rows, err := s.pool.Query(ctx, "SELECT idem_key, n FROM "+s.table)
	if err != nil {
		return nil, fmt.Errorf("chaos: read sink: %w", err)
	}
	out := map[string]int{}
	var (
		key string
		n   int
	)
	if _, err := pgx.ForEachRow(rows, []any{&key, &n}, func() error {
		out[key] = n
		return nil
	}); err != nil {
		return nil, fmt.Errorf("chaos: read sink: %w", err)
	}
	return out, nil
}
