package store

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/g0-up/flowkit/internal/migrate"
)

// recordingDBTX captures the SQL it receives.
type recordingDBTX struct {
	mu  sync.Mutex
	sql []string
}

func (r *recordingDBTX) record(sql string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sql = append(r.sql, sql)
}

func (r *recordingDBTX) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	r.record(sql)
	return pgconn.CommandTag{}, nil
}

func (r *recordingDBTX) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	r.record(sql)
	return nil, nil
}

func (r *recordingDBTX) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	r.record(sql)
	return nil
}

func TestSchemaDBTXRendersEveryCall(t *testing.T) {
	const in = "SELECT 1 FROM " + migrate.SchemaToken + "runs r JOIN " + migrate.SchemaToken + "run_steps s ON true"
	rec := &recordingDBTX{}
	d := schemaDBTX{inner: rec, r: newSQLRenderer("tenant_2")}
	ctx := context.Background()
	if _, err := d.Exec(ctx, in); err != nil {
		t.Fatal(err)
	}
	_, _ = d.Query(ctx, in)
	_ = d.QueryRow(ctx, in)

	expected := `SELECT 1 FROM "tenant_2".runs r JOIN "tenant_2".run_steps s ON true`
	if len(rec.sql) != 3 {
		t.Fatalf("got %d calls, want 3", len(rec.sql))
	}
	for i, got := range rec.sql {
		if got != expected {
			t.Errorf("call %d: got %q, want %q", i, got, expected)
		}
	}
}

func TestSQLRendererCachesBySourceText(t *testing.T) {
	r := newSQLRenderer("flow_kit_9")
	const in = "SELECT * FROM " + migrate.SchemaToken + "definitions"
	first := r.render(in)
	if first != `SELECT * FROM "flow_kit_9".definitions` {
		t.Fatalf("render = %q", first)
	}
	if strings.Contains(first, migrate.SchemaToken) {
		t.Fatalf("token left in %q", first)
	}
	cached, ok := r.cache.Load(in)
	if !ok || cached.(string) != first {
		t.Fatalf("cache entry = %v, %v", cached, ok)
	}
	if again := r.render(in); again != first {
		t.Fatalf("second render = %q, want %q", again, first)
	}
	n := 0
	r.cache.Range(func(_, _ any) bool { n++; return true })
	if n != 1 {
		t.Fatalf("cache holds %d entries, want 1", n)
	}
}

func TestSQLRendererIsSafeForConcurrentUse(t *testing.T) {
	r := newSQLRenderer("s1")
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			in := "SELECT " + strings.Repeat("1", i%4+1) + " FROM " + migrate.SchemaToken + "runs"
			if got := r.render(in); strings.Contains(got, migrate.SchemaToken) {
				t.Errorf("token left in %q", got)
			}
		}()
	}
	wg.Wait()
}
