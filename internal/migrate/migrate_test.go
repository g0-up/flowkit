package migrate

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/g0-up/flowkit/internal/testpg"
)

// unusedSchema returns a valid schema name that does not exist yet and drops
// it, if created, when the test ends.
func unusedSchema(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	name, err := testpg.NewSchemaName()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+pgx.Identifier{name}.Sanitize()+" CASCADE"); err != nil {
			t.Errorf("drop schema %s: %v", name, err)
		}
	})
	return name
}

// withExtra returns the embedded migrations followed by extra files, given as
// name to SQL.
func withExtra(t *testing.T, extra map[string]string) []migration {
	t.Helper()
	fsys := fstest.MapFS{}
	embedded, err := fs.ReadDir(sqlFS, "sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range embedded {
		data, err := fs.ReadFile(sqlFS, "sql/"+e.Name())
		if err != nil {
			t.Fatal(err)
		}
		fsys["sql/"+e.Name()] = &fstest.MapFile{Data: data}
	}
	for name, sql := range extra {
		fsys["sql/"+name] = &fstest.MapFile{Data: []byte(sql)}
	}
	migs, err := parseMigrations(fsys, "sql")
	if err != nil {
		t.Fatal(err)
	}
	return migs
}

func metaValue(t *testing.T, pool *pgxpool.Pool, schema, key string) string {
	t.Helper()
	var v string
	err := pool.QueryRow(context.Background(),
		RenderSchema("SELECT v FROM "+SchemaToken+"flowkit_meta WHERE k = $1", schema), key).Scan(&v)
	if err != nil {
		t.Fatalf("read flowkit_meta %s: %v", key, err)
	}
	return v
}

func mustStatus(t *testing.T, pool *pgxpool.Pool, schema string) State {
	t.Helper()
	st, err := Status(context.Background(), pool, schema)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func riverMigrationCount(t *testing.T, pool *pgxpool.Pool, schema string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		"SELECT count(*) FROM "+pgx.Identifier{schema, "river_migration"}.Sanitize()).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestStatusOfMissingSchemaListsEverything(t *testing.T) {
	pool := testpg.Pool(t)
	schema := unusedSchema(t, pool)
	st := mustStatus(t, pool, schema)
	if st.SchemaVersion != 0 || st.MinLibSchema != 0 || st.RiverVersion != 0 {
		t.Fatalf("state = %+v, want zero versions", st)
	}
	if !slices.Contains(st.Pending, "flowkit:0001_init") || !slices.Contains(st.Pending, "river:001_create_river_migration") {
		t.Fatalf("pending = %v", st.Pending)
	}
	var exists bool
	if err := pool.QueryRow(context.Background(), "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)", schema).Scan(&exists); err != nil || exists {
		t.Fatalf("Status created the schema (exists=%v, err=%v)", exists, err)
	}
}

func TestRunTwiceIsNoop(t *testing.T) {
	pool := testpg.Pool(t)
	schema := unusedSchema(t, pool)
	ctx := context.Background()
	if err := Run(ctx, pool, schema, Options{}); err != nil {
		t.Fatal(err)
	}
	st := mustStatus(t, pool, schema)
	lib, _ := LibSchema()
	if st.SchemaVersion != lib || st.MinLibSchema != 1 || st.RiverVersion == 0 || len(st.Pending) != 0 {
		t.Fatalf("after Run: %+v", st)
	}
	if err := Check(ctx, pool, schema); err != nil {
		t.Fatalf("Check after Run: %v", err)
	}
	before, err := dumpObjects(ctx, pool, schema)
	if err != nil {
		t.Fatal(err)
	}
	riverBefore := riverMigrationCount(t, pool, schema)

	if err := Run(ctx, pool, schema, Options{}); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	after, err := dumpObjects(ctx, pool, schema)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(before, after) || riverMigrationCount(t, pool, schema) != riverBefore {
		t.Fatal("second Run changed the schema")
	}
	if st2 := mustStatus(t, pool, schema); st2.SchemaVersion != st.SchemaVersion || st2.RiverVersion != st.RiverVersion {
		t.Fatalf("second Run changed versions: %+v -> %+v", st, st2)
	}
}

func TestConcurrentRunsApplyOnce(t *testing.T) {
	pool := testpg.Pool(t)
	schema := unusedSchema(t, pool)
	ctx := context.Background()
	// One pool per runner, as with separate processes: a waiting runner holds
	// a connection, so runners sharing one pool need more connections than
	// runners.
	const runners = 4
	pools := make([]*pgxpool.Pool, runners)
	for i := range pools {
		pools[i] = testpg.Pool(t)
	}
	var wg sync.WaitGroup
	errs := make([]error, runners)
	for i := range runners {
		wg.Go(func() { errs[i] = Run(ctx, pools[i], schema, Options{}) })
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("runner %d: %v", i, err)
		}
	}
	if err := Check(ctx, pool, schema); err != nil {
		t.Fatalf("Check after concurrent runs: %v", err)
	}
	if st := mustStatus(t, pool, schema); len(st.Pending) != 0 {
		t.Fatalf("pending after concurrent runs: %v", st.Pending)
	}
	// River records each version once; a double apply would have failed on
	// its primary key or re-created objects.
	var dup int
	if err := pool.QueryRow(ctx, "SELECT count(*) - count(DISTINCT (line, version)) FROM "+
		pgx.Identifier{schema, "river_migration"}.Sanitize()).Scan(&dup); err != nil || dup != 0 {
		t.Fatalf("duplicate River migration rows: %d, %v", dup, err)
	}
}

func TestRunWaitsForTheMigrationLock(t *testing.T) {
	pool := testpg.Pool(t)
	schema := unusedSchema(t, pool)
	ctx := context.Background()
	if err := Run(ctx, pool, schema, Options{}); err != nil {
		t.Fatal(err)
	}
	// A pending migration gives the waiting runner work to do.
	migs := withExtra(t, map[string]string{
		"0002_extra.sql": "-- min_lib_schema: 1\nCREATE TABLE " + SchemaToken + "extra (id int PRIMARY KEY);\n",
	})

	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback(ctx) }()
	if err := takeLock(ctx, holder, schema, time.Second); err != nil {
		t.Fatal(err)
	}

	err = run(ctx, pool, schema, Options{LockTimeout: 200 * time.Millisecond}, migs)
	if err == nil || !strings.Contains(err.Error(), "holds the migration lock") {
		t.Fatalf("Run with the lock held elsewhere = %v, want lock timeout", err)
	}

	done := make(chan error, 1)
	go func() { done <- run(ctx, pool, schema, Options{}, migs) }()
	waitForBlockedBackend(t, pool, holder.Conn().PgConn().PID(), done)
	if v := metaValue(t, pool, schema, metaSchemaVersion); v != "1" {
		t.Fatalf("schema_version changed to %s while the lock was held", v)
	}
	if err := holder.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after the lock was released: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not finish after the lock was released")
	}
	if v := metaValue(t, pool, schema, metaSchemaVersion); v != "2" {
		t.Fatalf("schema_version = %s, want 2", v)
	}
}

// waitForBlockedBackend waits until the server reports a backend blocked by
// the backend holderPID. It fails the test if done delivers first, since the
// waiting runner must not finish while the holder keeps its lock.
func waitForBlockedBackend(t *testing.T, pool *pgxpool.Pool, holderPID uint32, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var blocked bool
		err := pool.QueryRow(context.Background(), `
			SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity
			               WHERE $1::int = ANY (pg_catalog.pg_blocking_pids(pid)))`, int32(holderPID)).Scan(&blocked)
		if err != nil {
			t.Fatal(err)
		}
		if blocked {
			return
		}
		select {
		case err := <-done:
			t.Fatalf("Run finished while the lock was held: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("no backend waited for the migration lock")
		}
	}
}

func TestMigrationLockWaitsAreBounded(t *testing.T) {
	pool := testpg.Pool(t)
	schema := unusedSchema(t, pool)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := Run(ctx, pool, schema, Options{}); err != nil {
		t.Fatal(err)
	}
	// ALTER TABLE needs a lock that conflicts with the runner's own row lock
	// on flowkit_meta; without a lock timeout this migration would wait for
	// its own runner forever.
	migs := withExtra(t, map[string]string{
		"0002_extra.sql": "-- min_lib_schema: 1\nALTER TABLE " + SchemaToken + "flowkit_meta ADD COLUMN note text;\n",
	})
	err := run(ctx, pool, schema, Options{LockTimeout: 200 * time.Millisecond}, migs)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" || !strings.Contains(err.Error(), "0002_extra") {
		t.Fatalf("run = %v, want a lock timeout in 0002_extra", err)
	}
	if v := metaValue(t, pool, schema, metaSchemaVersion); v != "1" {
		t.Fatalf("schema_version = %s after the failed migration, want 1", v)
	}
}

func TestSetLockTimeoutClampsToTheSettingRange(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{500 * time.Microsecond, "SET LOCAL lock_timeout = 1"},
		{2 * time.Second, "SET LOCAL lock_timeout = 2000"},
		{1000 * time.Hour, "SET LOCAL lock_timeout = 2147483647"},
	}
	for _, c := range cases {
		if got := setLockTimeout(c.d); got != c.want {
			t.Errorf("setLockTimeout(%s) = %q, want %q", c.d, got, c.want)
		}
	}
}

func TestSchemasAreIndependent(t *testing.T) {
	pool := testpg.Pool(t)
	a, b := unusedSchema(t, pool), unusedSchema(t, pool)
	ctx := context.Background()
	var wg sync.WaitGroup
	var errA, errB error
	wg.Go(func() { errA = Run(ctx, pool, a, Options{}) })
	wg.Go(func() { errB = Run(ctx, pool, b, Options{}) })
	wg.Wait()
	if errA != nil || errB != nil {
		t.Fatalf("parallel runs: %v, %v", errA, errB)
	}
	at := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, RenderSchema(`INSERT INTO `+SchemaToken+`domain_events (type, payload, occurred_at, inserted_at)
		VALUES ('x', '{}', $1, $1)`, a), at); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := pool.QueryRow(ctx, RenderSchema("SELECT count(*) FROM "+SchemaToken+"domain_events", b)).Scan(&n); err != nil || n != 0 {
		t.Fatalf("schema %s sees %d events (err %v)", b, n, err)
	}
	for _, s := range []string{a, b} {
		if err := Check(ctx, pool, s); err != nil {
			t.Fatalf("Check %s: %v", s, err)
		}
	}
}

func TestDropSchemaRemovesEverything(t *testing.T) {
	pool := testpg.Pool(t)
	schema := unusedSchema(t, pool)
	ctx := context.Background()
	if err := Run(ctx, pool, schema, Options{}); err != nil {
		t.Fatal(err)
	}
	var oid uint32
	if err := pool.QueryRow(ctx, "SELECT oid FROM pg_namespace WHERE nspname = $1", schema).Scan(&oid); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
		t.Fatal(err)
	}
	var left int
	err := pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM pg_class WHERE relnamespace = $1)
		     + (SELECT count(*) FROM pg_proc WHERE pronamespace = $1)
		     + (SELECT count(*) FROM pg_type WHERE typnamespace = $1)`,
		oid).Scan(&left)
	if err != nil || left != 0 {
		t.Fatalf("%d objects left after DROP SCHEMA (err %v)", left, err)
	}
	// Nothing was installed outside the schema: migrating again from scratch
	// succeeds and yields the same objects.
	if err := Run(ctx, pool, schema, Options{}); err != nil {
		t.Fatalf("Run after drop: %v", err)
	}
	if err := Check(ctx, pool, schema); err != nil {
		t.Fatalf("Check after re-run: %v", err)
	}
}

func TestRunRejectsForeignSchema(t *testing.T) {
	pool, schema := testpg.New(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, "CREATE TABLE "+pgx.Identifier{schema, "orders"}.Sanitize()+" (id int)"); err != nil {
		t.Fatal(err)
	}
	if err := Run(ctx, pool, schema, Options{}); !errors.Is(err, ErrForeignSchema) {
		t.Fatalf("Run on a foreign schema = %v, want ErrForeignSchema", err)
	}
	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM pg_class WHERE relnamespace = $1::regnamespace", schema).Scan(&n); err != nil || n != 1 {
		t.Fatalf("Run touched the foreign schema: %d relations (err %v)", n, err)
	}
}

func TestRunRejectsBadInputBeforeTheDatabase(t *testing.T) {
	ctx := context.Background()
	// A nil pool would panic if anything reached the database.
	for _, name := range []string{"Flow-Kit", "public", "pg_x", strings.Repeat("a", 47), "Flowkit"} {
		if err := Run(ctx, nil, name, Options{}); err == nil || !strings.Contains(err.Error(), "schema name") {
			t.Errorf("Run(%q) = %v, want schema name error", name, err)
		}
		if _, err := Status(ctx, nil, name); err == nil {
			t.Errorf("Status(%q) succeeded", name)
		}
		if err := Check(ctx, nil, name); err == nil {
			t.Errorf("Check(%q) succeeded", name)
		}
	}

	cfg, err := pgxpool.ParseConfig(testpg.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 1
	small := testpg.PoolWithConfig(t, cfg)
	if err := Run(ctx, small, "flowkit_small_pool", Options{}); err == nil || !strings.Contains(err.Error(), "MaxConns") {
		t.Fatalf("Run with MaxConns=1 = %v, want MaxConns error", err)
	}
}

func TestConfirmNonEmpty(t *testing.T) {
	pool := testpg.Pool(t)
	schema := unusedSchema(t, pool)
	ctx := context.Background()
	if err := Run(ctx, pool, schema, Options{}); err != nil {
		t.Fatal(err)
	}
	migs := withExtra(t, map[string]string{
		"0002_extra.sql": "-- min_lib_schema: 1\nCREATE TABLE " + SchemaToken + "extra (id int PRIMARY KEY);\n",
	})
	at := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, RenderSchema(`INSERT INTO `+SchemaToken+`definitions
		(name, enabled, concurrency_policy, created_at, updated_at) VALUES ('d', true, 'concurrent', $1, $1)`, schema), at); err != nil {
		t.Fatal(err)
	}

	err := run(ctx, pool, schema, Options{}, migs)
	if !errors.Is(err, ErrConfirmationRequired) || !strings.Contains(err.Error(), "pg_dump -n "+schema) {
		t.Fatalf("Run on a schema with data = %v, want ErrConfirmationRequired with backup hint", err)
	}
	if v := metaValue(t, pool, schema, metaSchemaVersion); v != "1" {
		t.Fatalf("schema_version = %s after refused run", v)
	}
	if err := run(ctx, pool, schema, Options{ConfirmNonEmpty: true}, migs); err != nil {
		t.Fatalf("confirmed Run: %v", err)
	}
	if v := metaValue(t, pool, schema, metaSchemaVersion); v != "2" {
		t.Fatalf("schema_version = %s, want 2", v)
	}
	// Nothing pending: no confirmation needed even with data.
	if err := run(ctx, pool, schema, Options{}, migs); err != nil {
		t.Fatalf("no-op Run on a schema with data: %v", err)
	}
}

func TestMinLibSchemaAndRollingUpgrade(t *testing.T) {
	pool := testpg.Pool(t)
	schema := unusedSchema(t, pool)
	ctx := context.Background()

	// An additive migration keeps min_lib_schema, so this build (which only
	// knows 0001) still runs against the newer schema.
	additive := withExtra(t, map[string]string{
		"0002_add_table.sql": "-- flowkit migration 0002.\n-- min_lib_schema: 1\nCREATE TABLE " + SchemaToken + "extra (id int PRIMARY KEY);\n",
	})
	if err := run(ctx, pool, schema, Options{}, additive); err != nil {
		t.Fatal(err)
	}
	if v := metaValue(t, pool, schema, metaMinLibSchema); v != "1" {
		t.Fatalf("min_lib_schema = %s, want 1", v)
	}
	if err := Check(ctx, pool, schema); err != nil {
		t.Fatalf("Check on a schema migrated additively by a newer library: %v", err)
	}

	// A breaking migration raises min_lib_schema; this build must refuse it,
	// and migrating cannot help.
	breaking := withExtra(t, map[string]string{
		"0002_add_table.sql":   "-- min_lib_schema: 1\nCREATE TABLE " + SchemaToken + "extra (id int PRIMARY KEY);\n",
		"0003_break_reads.sql": "-- min_lib_schema: 3\nALTER TABLE " + SchemaToken + "extra ADD COLUMN v text;\n",
	})
	if err := run(ctx, pool, schema, Options{}, breaking); err != nil {
		t.Fatal(err)
	}
	if v := metaValue(t, pool, schema, metaMinLibSchema); v != "3" {
		t.Fatalf("min_lib_schema = %s, want 3", v)
	}
	var drift *DriftError
	if err := Check(ctx, pool, schema); !errors.As(err, &drift) || drift.NeedsMigrate || !strings.Contains(err.Error(), ">= 3") {
		t.Fatalf("Check with min_lib_schema above the library = %v, want DriftError without NeedsMigrate", err)
	}
	// Re-running an older migration list never lowers min_lib_schema.
	if err := run(ctx, pool, schema, Options{}, additive); err != nil {
		t.Fatal(err)
	}
	if v := metaValue(t, pool, schema, metaMinLibSchema); v != "3" {
		t.Fatalf("min_lib_schema = %s after older runner, want 3", v)
	}
}

func TestCheckReportsDrift(t *testing.T) {
	ctx := context.Background()
	pool := testpg.Pool(t)
	exec := func(t *testing.T, sql string) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	migrated := func(t *testing.T) string {
		t.Helper()
		schema := unusedSchema(t, pool)
		if err := Run(ctx, pool, schema, Options{}); err != nil {
			t.Fatal(err)
		}
		return schema
	}
	expectDrift := func(t *testing.T, schema string, needsMigrate bool, contains string) {
		t.Helper()
		err := Check(ctx, pool, schema)
		var drift *DriftError
		if !errors.As(err, &drift) {
			t.Fatalf("Check = %v, want *DriftError", err)
		}
		if drift.NeedsMigrate != needsMigrate || !strings.Contains(err.Error(), contains) {
			t.Fatalf("Check = %v (NeedsMigrate %v), want NeedsMigrate %v mentioning %q", err, drift.NeedsMigrate, needsMigrate, contains)
		}
	}

	t.Run("missing schema", func(t *testing.T) {
		expectDrift(t, unusedSchema(t, pool), true, "not migrated")
	})
	t.Run("empty schema", func(t *testing.T) {
		expectDrift(t, testpg.Schema(t, pool), true, "not migrated")
	})
	t.Run("schema version behind", func(t *testing.T) {
		s := migrated(t)
		exec(t, RenderSchema("UPDATE "+SchemaToken+"flowkit_meta SET v = '0' WHERE k = 'schema_version'", s))
		expectDrift(t, s, true, "below library schema")
	})
	t.Run("River migration missing", func(t *testing.T) {
		s := migrated(t)
		exec(t, "DELETE FROM "+pgx.Identifier{s, "river_migration"}.Sanitize()+" WHERE version = (SELECT max(version) FROM "+
			pgx.Identifier{s, "river_migration"}.Sanitize()+")")
		expectDrift(t, s, true, "River")
	})
	t.Run("function added", func(t *testing.T) {
		s := migrated(t)
		exec(t, "CREATE FUNCTION "+pgx.Identifier{s, "sneaky"}.Sanitize()+"() RETURNS int LANGUAGE sql AS 'SELECT 1'")
		expectDrift(t, s, false, "not on the River allowlist: function sneaky()")
	})
	t.Run("table added", func(t *testing.T) {
		s := migrated(t)
		exec(t, "CREATE TABLE "+pgx.Identifier{s, "stray"}.Sanitize()+" (id int)")
		expectDrift(t, s, false, "unexpected table stray")
	})
	t.Run("index dropped", func(t *testing.T) {
		s := migrated(t)
		exec(t, "DROP INDEX "+pgx.Identifier{s, "runs_entity"}.Sanitize())
		expectDrift(t, s, false, "missing index runs_entity")
	})
	t.Run("corrupt meta", func(t *testing.T) {
		s := migrated(t)
		exec(t, RenderSchema("UPDATE "+SchemaToken+"flowkit_meta SET v = 'x' WHERE k = 'schema_version'", s))
		err := Check(ctx, pool, s)
		var drift *DriftError
		if err == nil || errors.As(err, &drift) {
			t.Fatalf("Check with unreadable meta = %v, want plain error", err)
		}
	})
}

func TestParseMigrations(t *testing.T) {
	file := func(sql string) *fstest.MapFile { return &fstest.MapFile{Data: []byte(sql)} }
	ok := fstest.MapFS{
		"sql/0001_a.sql": file("-- first\n-- min_lib_schema: 1\nSELECT 1;\n"),
		"sql/0002_b.sql": file("\n-- min_lib_schema:2\nSELECT 2;\n"),
	}
	migs, err := parseMigrations(ok, "sql")
	if err != nil {
		t.Fatal(err)
	}
	if len(migs) != 2 || migs[0].Name != "0001_a" || migs[1].Version != 2 || migs[1].MinLibSchema != 2 {
		t.Fatalf("parsed %+v", migs)
	}

	bad := map[string]fstest.MapFS{
		"no header":             {"sql/0001_a.sql": file("SELECT 1;\n-- min_lib_schema: 1\n")},
		"gap":                   {"sql/0001_a.sql": file("-- min_lib_schema: 1\n"), "sql/0003_c.sql": file("-- min_lib_schema: 1\n")},
		"not starting at one":   {"sql/0002_b.sql": file("-- min_lib_schema: 1\n")},
		"min above own version": {"sql/0001_a.sql": file("-- min_lib_schema: 2\n")},
		"min zero":              {"sql/0001_a.sql": file("-- min_lib_schema: 0\n")},
		"bad file name":         {"sql/0001-a.sql": file("-- min_lib_schema: 1\n")},
		"empty":                 {"sql/.keep": file("")},
	}
	for name, fsys := range bad {
		if _, err := parseMigrations(fsys, "sql"); err == nil {
			t.Errorf("%s: parseMigrations succeeded", name)
		}
	}
}

func TestEmbeddedMigrationsParse(t *testing.T) {
	migs, err := loadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	lib, err := LibSchema()
	if err != nil || lib != len(migs) || migs[0].Name != "0001_init" {
		t.Fatalf("LibSchema = %d, %v; migrations %d", lib, err, len(migs))
	}
	// The runner creates flowkit_meta itself; 0001 must repeat the identical
	// statement so sqlc and the runner agree on the table.
	if !strings.Contains(migs[0].SQL, metaDDL+";") {
		t.Fatalf("0001_init.sql does not contain %q", metaDDL)
	}
}

func TestIsCreateRace(t *testing.T) {
	for code, want := range map[string]bool{
		"23505": true,  // unique_violation
		"42P06": true,  // duplicate_schema
		"42P07": true,  // duplicate_table
		"42710": true,  // duplicate_object: row type of a concurrently created table
		"42501": false, // insufficient_privilege
	} {
		err := fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: code})
		if got := isCreateRace(err); got != want {
			t.Errorf("isCreateRace(%s) = %v, want %v", code, got, want)
		}
	}
	if isCreateRace(errors.New("plain")) {
		t.Error("isCreateRace(non-pg error) = true")
	}
}
