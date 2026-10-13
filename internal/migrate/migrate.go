package migrate

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"math"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

//go:embed sql/*.sql
var sqlFS embed.FS

// metaDDL creates the bookkeeping table. The runner issues it before taking the
// migration lock, because the lock is a row in this table; 0001_init.sql
// repeats it verbatim so sqlc sees the table.
const metaDDL = "CREATE TABLE IF NOT EXISTS " + SchemaToken + "flowkit_meta (k text PRIMARY KEY, v text NOT NULL)"

// Keys of flowkit_meta.
const (
	metaSchemaVersion = "schema_version"
	metaMinLibSchema  = "min_lib_schema"
	metaMigrateLock   = "migrate_lock"
)

// defaultLockTimeout bounds how long Run waits for another runner that holds
// the migration lock, and how long a migration statement waits for a table
// lock.
const defaultLockTimeout = 10 * time.Minute

// createRaceRetries is how many times schema and meta-table creation is
// retried when a concurrent runner wins the race (PostgreSQL bug #9956: CREATE
// ... IF NOT EXISTS can still fail with a unique violation).
const createRaceRetries = 2

var (
	// ErrConfirmationRequired is returned by Run when migrations are pending
	// on a schema that already holds flowkit data and the caller has not
	// confirmed that a backup exists.
	ErrConfirmationRequired = errors.New("flowkit migrate: schema holds data; confirmation required")

	// ErrForeignSchema is returned by Run when the target schema already has
	// objects but no flowkit_meta table, so it does not belong to flowkit.
	ErrForeignSchema = errors.New("flowkit migrate: schema has objects but is not a flowkit schema")
)

// Options tunes Run.
type Options struct {
	// ConfirmNonEmpty allows migrations to run on a schema that already holds
	// flowkit data. The operator sets it after taking a backup.
	ConfirmNonEmpty bool
	// Logger receives River's migration log lines. Nil discards them.
	Logger *slog.Logger
	// LockTimeout bounds the wait for a concurrent runner and, inside each
	// migration, the wait for any table lock. Zero means ten minutes.
	LockTimeout time.Duration
}

// migration is one embedded flowkit SQL file.
type migration struct {
	Version      int
	Name         string // file name without extension, e.g. 0001_init
	MinLibSchema int
	SQL          string // still contains SchemaToken
}

var (
	migrationFileName = regexp.MustCompile(`^(\d{4})_([a-z0-9_]+)\.sql$`)
	minLibSchemaLine  = regexp.MustCompile(`^--\s*min_lib_schema:\s*(\d+)\s*$`)
)

// loadMigrations parses the embedded SQL files once.
var loadMigrations = sync.OnceValues(func() ([]migration, error) {
	return parseMigrations(sqlFS, "sql")
})

// parseMigrations reads NNNN_name.sql files from dir. Versions must run 1..n
// without gaps, and every file must declare min_lib_schema in its leading
// comment block, between 1 and its own version.
func parseMigrations(fsys fs.FS, dir string) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("flowkit migrate: read migrations: %w", err)
	}
	var out []migration
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := migrationFileName.FindStringSubmatch(e.Name())
		if m == nil {
			return nil, fmt.Errorf("flowkit migrate: unexpected file %s in migrations", e.Name())
		}
		version, _ := strconv.Atoi(m[1])
		body, err := fs.ReadFile(fsys, path.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("flowkit migrate: read %s: %w", e.Name(), err)
		}
		minLib, err := headerMinLibSchema(string(body))
		if err != nil {
			return nil, fmt.Errorf("flowkit migrate: %s: %w", e.Name(), err)
		}
		if minLib < 1 || minLib > version {
			return nil, fmt.Errorf("flowkit migrate: %s: min_lib_schema %d outside 1..%d", e.Name(), minLib, version)
		}
		out = append(out, migration{
			Version:      version,
			Name:         strings.TrimSuffix(e.Name(), ".sql"),
			MinLibSchema: minLib,
			SQL:          string(body),
		})
	}
	// fs.ReadDir returns entries sorted by name, so versions are ascending.
	for i, m := range out {
		if m.Version != i+1 {
			return nil, fmt.Errorf("flowkit migrate: migration %s out of sequence, want version %d", m.Name, i+1)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("flowkit migrate: no migrations embedded")
	}
	return out, nil
}

// headerMinLibSchema reads "-- min_lib_schema: N" from the comment block at
// the top of a migration file.
func headerMinLibSchema(sql string) (int, error) {
	for line := range strings.Lines(sql) {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "--") {
			break
		}
		if m := minLibSchemaLine.FindStringSubmatch(line); m != nil {
			return strconv.Atoi(m[1])
		}
	}
	return 0, errors.New("missing \"-- min_lib_schema: N\" header")
}

// LibSchema is the highest flowkit migration this build embeds: the schema
// version it is written against.
func LibSchema() (int, error) {
	migs, err := loadMigrations()
	if err != nil {
		return 0, err
	}
	return migs[len(migs)-1].Version, nil
}

// Run brings schema up to date: it creates the schema and flowkit_meta if
// needed, takes the migration lock (a row lock on flowkit_meta held by a
// transaction on its own connection), applies River's migrations into the same
// schema, then applies each pending flowkit migration in its own transaction.
// Concurrent runners wait for the lock; the loser finds nothing left to do.
//
// The pool needs at least two connections: one holds the lock while the
// migrations run on another. A runner waiting for the lock also holds a
// connection, so concurrent runners sharing one pool need more connections
// than runners.
func Run(ctx context.Context, pool *pgxpool.Pool, schema string, opts Options) error {
	if err := ValidateSchemaName(schema); err != nil {
		return err
	}
	if pool == nil {
		return errors.New("flowkit migrate: nil pool")
	}
	if n := pool.Config().MaxConns; n < 2 {
		return fmt.Errorf("flowkit migrate: pool MaxConns is %d, need at least 2", n)
	}
	migs, err := loadMigrations()
	if err != nil {
		return err
	}
	return run(ctx, pool, schema, opts, migs)
}

// run is Run with the migration list as a parameter.
func run(ctx context.Context, pool *pgxpool.Pool, schema string, opts Options, migs []migration) error {
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	lockTimeout := opts.LockTimeout
	if lockTimeout <= 0 {
		lockTimeout = defaultLockTimeout
	}

	if err := ensureMeta(ctx, pool, schema); err != nil {
		return err
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("flowkit migrate: acquire lock connection: %w", err)
	}
	defer conn.Release()
	lockTx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("flowkit migrate: begin lock transaction: %w", err)
	}
	// Rollback after Commit is a no-op; on early return it releases the lock.
	defer func() { _ = lockTx.Rollback(context.WithoutCancel(ctx)) }()
	if err := takeLock(ctx, lockTx, schema, lockTimeout); err != nil {
		return err
	}

	m, err := readMeta(ctx, pool, schema)
	if err != nil {
		return err
	}
	riverMigrator, err := newRiverMigrator(pool, schema, logger)
	if err != nil {
		return err
	}
	riverPending, _, err := riverPendingVersions(ctx, riverMigrator)
	if err != nil {
		return err
	}
	pending := pendingMigrations(migs, m.SchemaVersion)
	if len(pending) == 0 && len(riverPending) == 0 {
		if err := lockTx.Commit(ctx); err != nil {
			return fmt.Errorf("flowkit migrate: release migration lock on schema %q: %w", schema, err)
		}
		return nil
	}
	if !opts.ConfirmNonEmpty && m.SchemaVersion > 0 {
		hasData, err := schemaHasData(ctx, pool, schema)
		if err != nil {
			return err
		}
		if hasData {
			return fmt.Errorf("%w: schema %q has pending migrations and holds data; back it up (pg_dump -n %s) and confirm", ErrConfirmationRequired, schema, schema)
		}
	}

	if len(riverPending) > 0 {
		if _, err := riverMigrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
			return fmt.Errorf("flowkit migrate: River migrations in schema %q: %w", schema, err)
		}
	}
	minLib := m.MinLibSchema
	for _, mig := range pending {
		minLib = max(minLib, mig.MinLibSchema)
		if err := applyMigration(ctx, pool, schema, mig, minLib, lockTimeout); err != nil {
			return err
		}
	}
	if err := lockTx.Commit(ctx); err != nil {
		// Every migration committed on its own connection; only the lock
		// release failed, and the server drops the lock with the connection.
		return fmt.Errorf("flowkit migrate: migrations applied to schema %q, but releasing the migration lock failed (the lock ends with its connection; no action needed): %w", schema, err)
	}
	return nil
}

// ensureMeta creates schema and flowkit_meta when missing, retrying the
// create-if-not-exists race a concurrent runner can trigger. It refuses a
// schema that has objects but no flowkit_meta.
func ensureMeta(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	for attempt := 0; ; attempt++ {
		err := ensureMetaOnce(ctx, pool, schema)
		if err == nil || attempt >= createRaceRetries || !isCreateRace(err) {
			return err
		}
	}
}

func ensureMetaOnce(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	var schemaExists, hasObjects, hasMeta bool
	err := pool.QueryRow(ctx, `
		SELECT n.oid IS NOT NULL,
		       EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.relnamespace = n.oid)
		         OR EXISTS (SELECT 1 FROM pg_catalog.pg_proc p WHERE p.pronamespace = n.oid)
		         OR EXISTS (SELECT 1 FROM pg_catalog.pg_type t WHERE t.typnamespace = n.oid),
		       EXISTS (SELECT 1 FROM pg_catalog.pg_class c
		               WHERE c.relnamespace = n.oid AND c.relname = 'flowkit_meta' AND c.relkind = 'r')
		FROM (SELECT $1::text AS name) s
		LEFT JOIN pg_catalog.pg_namespace n ON n.nspname = s.name`, schema).Scan(&schemaExists, &hasObjects, &hasMeta)
	if err != nil {
		return fmt.Errorf("flowkit migrate: inspect schema %q: %w", schema, err)
	}
	if hasObjects && !hasMeta {
		return fmt.Errorf("%w: %q", ErrForeignSchema, schema)
	}
	// Creating only when absent lets a role without CREATE on the database
	// migrate into a schema an administrator created for it.
	if !schemaExists {
		if _, err := pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgx.Identifier{schema}.Sanitize()); err != nil {
			return fmt.Errorf("flowkit migrate: create schema %q: %w", schema, err)
		}
	}
	if !hasMeta {
		if _, err := pool.Exec(ctx, RenderSchema(metaDDL, schema)); err != nil {
			return fmt.Errorf("flowkit migrate: create flowkit_meta in %q: %w", schema, err)
		}
	}
	return nil
}

// isCreateRace reports the errors a concurrent CREATE ... IF NOT EXISTS can
// raise: unique_violation, duplicate_schema, duplicate_table, and
// duplicate_object (the table's row type colliding in pg_type).
func isCreateRace(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	switch pgErr.Code {
	case "23505", "42P06", "42P07", "42710":
		return true
	}
	return false
}

// takeLock locks the migrate_lock row of flowkit_meta inside tx. Another
// runner blocks here until tx ends; if this process dies, its connection
// closes and the lock is released.
//
// The lock is held on a connection that idles while migrations run on other
// connections of the same pool. Two consequences follow:
//   - A migration that needs a lock conflicting with the held row lock (ALTER,
//     DROP or TRUNCATE of flowkit_meta, or a write to the migrate_lock row)
//     would wait on its own runner, which the server cannot detect as a
//     deadlock. Migrations never touch flowkit_meta that way, a test enforces
//     it, and applyMigration sets lock_timeout so any such wait ends in an
//     error instead of a hang.
//   - If only the lock connection drops mid-run, the server releases the lock
//     and a second runner may start while this one is still applying a
//     migration. The second runner reads the version committed so far, so it
//     may run the in-flight migration again; its statements wait on the first
//     runner's uncommitted changes and then fail on the objects that already
//     exist, and the first runner fails at the final lock commit. No schema
//     is half-applied because each migration is one transaction, but both
//     runners may report errors. This case is accepted, not prevented.
func takeLock(ctx context.Context, tx pgx.Tx, schema string, timeout time.Duration) error {
	stmts := []string{
		setLockTimeout(timeout),
		// The lock transaction idles while migrations run on other
		// connections; server-side timeouts must not end it.
		"SET LOCAL idle_in_transaction_session_timeout = 0",
		"SET LOCAL statement_timeout = 0",
	}
	for _, s := range stmts {
		if _, err := tx.Exec(ctx, s); err != nil {
			return fmt.Errorf("flowkit migrate: prepare lock transaction: %w", err)
		}
	}
	_, err := tx.Exec(ctx, RenderSchema(
		"INSERT INTO "+SchemaToken+"flowkit_meta (k, v) VALUES ($1, '') ON CONFLICT (k) DO NOTHING", schema),
		metaMigrateLock)
	if err == nil {
		var v string
		err = tx.QueryRow(ctx, RenderSchema(
			"SELECT v FROM "+SchemaToken+"flowkit_meta WHERE k = $1 FOR UPDATE", schema),
			metaMigrateLock).Scan(&v)
	}
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "55P03" {
			return fmt.Errorf("flowkit migrate: another runner holds the migration lock on schema %q (waited %s): %w", schema, timeout, err)
		}
		return fmt.Errorf("flowkit migrate: take migration lock on schema %q: %w", schema, err)
	}
	return nil
}

// setLockTimeout returns the SET LOCAL lock_timeout statement for d. The
// setting is an int4 of milliseconds where 0 disables the timeout, so d is
// clamped to [1ms, MaxInt32 ms].
func setLockTimeout(d time.Duration) string {
	ms := min(max(d.Milliseconds(), 1), math.MaxInt32)
	return fmt.Sprintf("SET LOCAL lock_timeout = %d", ms)
}

// metaState is what flowkit_meta records about a schema.
type metaState struct {
	Exists        bool // flowkit_meta table is present
	SchemaVersion int
	MinLibSchema  int
}

// queryer is the read surface shared by pools, connections and transactions.
type queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// readMeta reads schema_version and min_lib_schema. Missing keys read as 0; a
// missing table reads as Exists false.
func readMeta(ctx context.Context, q queryer, schema string) (metaState, error) {
	var st metaState
	err := q.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
		               JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		               WHERE n.nspname = $1 AND c.relname = 'flowkit_meta' AND c.relkind = 'r')`,
		schema).Scan(&st.Exists)
	if err != nil {
		return st, fmt.Errorf("flowkit migrate: inspect schema %q: %w", schema, err)
	}
	if !st.Exists {
		return st, nil
	}
	rows, err := q.Query(ctx, RenderSchema("SELECT k, v FROM "+SchemaToken+"flowkit_meta WHERE k = ANY($1)", schema),
		[]string{metaSchemaVersion, metaMinLibSchema})
	if err != nil {
		return st, fmt.Errorf("flowkit migrate: read flowkit_meta in %q: %w", schema, err)
	}
	kv, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) ([2]string, error) {
		var p [2]string
		err := r.Scan(&p[0], &p[1])
		return p, err
	})
	if err != nil {
		return st, fmt.Errorf("flowkit migrate: read flowkit_meta in %q: %w", schema, err)
	}
	for _, p := range kv {
		n, err := strconv.Atoi(p[1])
		if err != nil || n < 0 {
			return st, fmt.Errorf("flowkit migrate: flowkit_meta %s in %q is %q, want a non-negative integer", p[0], schema, p[1])
		}
		switch p[0] {
		case metaSchemaVersion:
			st.SchemaVersion = n
		case metaMinLibSchema:
			st.MinLibSchema = n
		}
	}
	return st, nil
}

// pendingMigrations returns the migrations above version.
func pendingMigrations(migs []migration, version int) []migration {
	var out []migration
	for _, m := range migs {
		if m.Version > version {
			out = append(out, m)
		}
	}
	return out
}

// schemaHasData reports whether any table holding user-visible flowkit state
// has a row. Only valid once migration 0001 is applied.
func schemaHasData(ctx context.Context, q queryer, schema string) (bool, error) {
	var has bool
	err := q.QueryRow(ctx, RenderSchema(`
		SELECT EXISTS (SELECT 1 FROM `+SchemaToken+`domain_events)
		    OR EXISTS (SELECT 1 FROM `+SchemaToken+`definitions)
		    OR EXISTS (SELECT 1 FROM `+SchemaToken+`drafts)
		    OR EXISTS (SELECT 1 FROM `+SchemaToken+`run_summaries)
		    OR EXISTS (SELECT 1 FROM `+SchemaToken+`dead_letters)`, schema)).Scan(&has)
	if err != nil {
		return false, fmt.Errorf("flowkit migrate: check for data in %q: %w", schema, err)
	}
	return has, nil
}

// applyMigration runs one flowkit migration and records it in flowkit_meta in
// the same transaction. lockTimeout bounds every lock wait inside it.
func applyMigration(ctx context.Context, pool *pgxpool.Pool, schema string, mig migration, minLib int, lockTimeout time.Duration) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("flowkit migrate: begin %s: %w", mig.Name, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, setLockTimeout(lockTimeout)); err != nil {
		return fmt.Errorf("flowkit migrate: begin %s: %w", mig.Name, err)
	}
	// Without arguments pgx uses the simple protocol, which accepts a file
	// of several statements.
	if _, err := tx.Exec(ctx, RenderSchema(mig.SQL, schema)); err != nil {
		return fmt.Errorf("flowkit migrate: apply %s to schema %q: %w", mig.Name, schema, err)
	}
	_, err = tx.Exec(ctx, RenderSchema(
		"INSERT INTO "+SchemaToken+"flowkit_meta (k, v) VALUES ($1, $2), ($3, $4) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v", schema),
		metaSchemaVersion, strconv.Itoa(mig.Version), metaMinLibSchema, strconv.Itoa(minLib))
	if err != nil {
		return fmt.Errorf("flowkit migrate: record %s in schema %q: %w", mig.Name, schema, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("flowkit migrate: commit %s to schema %q: %w", mig.Name, schema, err)
	}
	return nil
}

func newRiverMigrator(pool *pgxpool.Pool, schema string, logger *slog.Logger) (*rivermigrate.Migrator[pgx.Tx], error) {
	m, err := rivermigrate.New(riverpgxv5.New(pool), &rivermigrate.Config{Schema: schema, Logger: logger})
	if err != nil {
		return nil, fmt.Errorf("flowkit migrate: River migrator: %w", err)
	}
	return m, nil
}

// riverPendingVersions lists River migrations not yet applied to the schema,
// and the highest applied version.
func riverPendingVersions(ctx context.Context, m *rivermigrate.Migrator[pgx.Tx]) (pending []rivermigrate.Migration, applied int, err error) {
	existing, err := m.ExistingVersions(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("flowkit migrate: read River migrations: %w", err)
	}
	done := make(map[int]bool, len(existing))
	for _, v := range existing {
		done[v.Version] = true
		applied = max(applied, v.Version)
	}
	for _, v := range m.AllVersions() {
		if !done[v.Version] {
			pending = append(pending, v)
		}
	}
	return pending, applied, nil
}

// State describes how far a schema is migrated.
type State struct {
	SchemaVersion int
	MinLibSchema  int
	RiverVersion  int      // highest applied River migration, 0 if none
	Pending       []string // "flowkit:0001_init", "river:001_create_river_migration", ...
}

// Status reads the migration state of schema without changing anything. A
// schema that does not exist yet reports every migration as pending.
func Status(ctx context.Context, pool *pgxpool.Pool, schema string) (State, error) {
	var st State
	if err := ValidateSchemaName(schema); err != nil {
		return st, err
	}
	if pool == nil {
		return st, errors.New("flowkit migrate: nil pool")
	}
	migs, err := loadMigrations()
	if err != nil {
		return st, err
	}
	m, err := readMeta(ctx, pool, schema)
	if err != nil {
		return st, err
	}
	st.SchemaVersion, st.MinLibSchema = m.SchemaVersion, m.MinLibSchema

	riverMigrator, err := newRiverMigrator(pool, schema, slog.New(slog.DiscardHandler))
	if err != nil {
		return st, err
	}
	riverPending, riverApplied, err := riverPendingVersions(ctx, riverMigrator)
	if err != nil {
		return st, err
	}
	st.RiverVersion = riverApplied
	for _, v := range riverPending {
		st.Pending = append(st.Pending, fmt.Sprintf("river:%03d_%s", v.Version, strings.ReplaceAll(v.Name, " ", "_")))
	}
	for _, mig := range pendingMigrations(migs, m.SchemaVersion) {
		st.Pending = append(st.Pending, "flowkit:"+mig.Name)
	}
	return st, nil
}
