package store

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/g0-up/flowkit/internal/migrate"
	"github.com/g0-up/flowkit/internal/store/db"
)

// PgStore is the Postgres Store. All SQL is qualified with the schema given
// to NewPgStore.
type PgStore struct {
	pool   *pgxpool.Pool
	schema string
	r      *sqlRenderer
}

var _ Store = (*PgStore)(nil)

// NewPgStore returns a store over pool for schema. The schema name is
// validated here; whether it is migrated is checked by migrate.Check.
func NewPgStore(pool *pgxpool.Pool, schema string) (*PgStore, error) {
	if err := migrate.ValidateSchemaName(schema); err != nil {
		return nil, err
	}
	if pool == nil {
		return nil, errors.New("flowkit store: nil pool")
	}
	return &PgStore{pool: pool, schema: schema, r: newSQLRenderer(schema)}, nil
}

// Schema returns the schema the store reads and writes.
func (s *PgStore) Schema() string { return s.schema }

// pgTx is the Tx of PgStore.
type pgTx struct {
	tx       pgx.Tx
	owner    *PgStore // the store that began or wrapped tx
	borrowed bool     // owned by the caller; Commit and Rollback are refused
}

func (t *pgTx) Commit(ctx context.Context) error {
	if t.borrowed {
		return ErrBorrowedTx
	}
	return t.tx.Commit(ctx)
}

func (t *pgTx) Rollback(ctx context.Context) error {
	if t.borrowed {
		return ErrBorrowedTx
	}
	return t.tx.Rollback(ctx)
}

// Begin starts a transaction on the pool.
func (s *PgStore) Begin(ctx context.Context) (Tx, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("flowkit store: begin: %w", err)
	}
	return &pgTx{tx: tx, owner: s}, nil
}

// WrapTx lets store methods run inside a transaction the caller owns, such as
// the one passed to Emit. The returned Tx refuses Commit and Rollback.
func (s *PgStore) WrapTx(tx pgx.Tx) Tx {
	return &pgTx{tx: tx, owner: s, borrowed: true}
}

// PgxTx returns the pgx transaction behind tx, for APIs that need it
// directly, such as River's InsertTx. Like every store method it refuses,
// with ErrForeignTx, a Tx this store did not begin or wrap.
func (s *PgStore) PgxTx(tx Tx) (pgx.Tx, error) {
	t, ok := tx.(*pgTx)
	if !ok || t == nil || t.tx == nil || t.owner != s {
		return nil, ErrForeignTx
	}
	return t.tx, nil
}

// q returns generated queries bound to tx and this store's schema. A Tx
// from another store is refused even when both share a pool, so a caller
// cannot mix schemas within one store call.
func (s *PgStore) q(tx Tx) (*db.Queries, error) {
	ptx, err := s.PgxTx(tx)
	if err != nil {
		return nil, err
	}
	return db.New(schemaDBTX{inner: ptx, r: s.r}), nil
}

// dbtx returns tx with this store's schema rendered into every statement,
// for SQL written by hand, such as queries of River's tables.
func (s *PgStore) dbtx(tx Tx) (schemaDBTX, error) {
	ptx, err := s.PgxTx(tx)
	if err != nil {
		return schemaDBTX{}, err
	}
	return schemaDBTX{inner: ptx, r: s.r}, nil
}

// insertOrGetRetries is how many extra inserts an insert-or-get makes when
// the conflicting row is gone by the time it is looked up.
const insertOrGetRetries = 1

// wrapErr maps pgx.ErrNoRows to ErrNotFound and adds context to the rest.
func wrapErr(op string, err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("flowkit store: %s: %w", op, ErrNotFound)
	}
	return fmt.Errorf("flowkit store: %s: %w", op, err)
}

// toInt32 converts an int for an int4 column, rejecting values that do not
// fit instead of wrapping them.
func toInt32(field string, v int) (int32, error) {
	if v < math.MinInt32 || v > math.MaxInt32 {
		return 0, fmt.Errorf("flowkit store: %s %d out of int4 range", field, v)
	}
	return int32(v), nil
}

func toInt32Ptr(field string, v *int) (*int32, error) {
	if v == nil {
		return nil, nil
	}
	n, err := toInt32(field, *v)
	if err != nil {
		return nil, err
	}
	return &n, nil
}

func intPtr(v *int32) *int {
	if v == nil {
		return nil
	}
	n := int(*v)
	return &n
}
