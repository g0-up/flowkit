package store

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/g0-up/flowkit/internal/migrate"
	"github.com/g0-up/flowkit/internal/store/db"
)

// sqlRenderer replaces the schema token in SQL text with one schema, caching
// the result per input string. The cache grows with the number of distinct
// statements, which is the fixed set of generated queries.
type sqlRenderer struct {
	schema string
	cache  sync.Map // original SQL -> rendered SQL
}

func newSQLRenderer(schema string) *sqlRenderer {
	return &sqlRenderer{schema: schema}
}

func (r *sqlRenderer) render(sql string) string {
	if v, ok := r.cache.Load(sql); ok {
		return v.(string)
	}
	v, _ := r.cache.LoadOrStore(sql, migrate.RenderSchema(sql, r.schema))
	return v.(string)
}

// schemaDBTX implements the sqlc DBTX interface over a pool, connection or
// transaction, rendering the schema token in every statement. It never
// changes session state such as search_path, so it can wrap a transaction
// the caller owns.
type schemaDBTX struct {
	inner db.DBTX
	r     *sqlRenderer
}

var _ db.DBTX = schemaDBTX{}

func (d schemaDBTX) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return d.inner.Exec(ctx, d.r.render(sql), args...)
}

func (d schemaDBTX) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return d.inner.Query(ctx, d.r.render(sql), args...)
}

func (d schemaDBTX) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return d.inner.QueryRow(ctx, d.r.render(sql), args...)
}
