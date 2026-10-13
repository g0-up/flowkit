package store

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/g0-up/flowkit/internal/migrate"
)

// queryRecorder is a pgx tracer that keeps the SQL and arguments of the
// first query whose SQL contains marker.
type queryRecorder struct {
	marker string
	mu     sync.Mutex
	sql    string
	args   []any
}

func (r *queryRecorder) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sql == "" && strings.Contains(data.SQL, r.marker) {
		r.sql, r.args = data.SQL, data.Args
	}
	return ctx
}

func (r *queryRecorder) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// planIndexes returns the indexes a JSON plan scans.
func planIndexes(node map[string]any, into map[string]bool) {
	if name, ok := node["Index Name"].(string); ok {
		into[name] = true
	}
	children, _ := node["Plans"].([]any)
	for _, c := range children {
		if m, ok := c.(map[string]any); ok {
			planIndexes(m, into)
		}
	}
}

// TestQueuedEventExistsUsesItsIndex checks the plan of the store's own
// statement on a run queue of many keys: the lookup by definition and
// event must not scan the queue.
func TestQueuedEventExistsUsesItsIndex(t *testing.T) {
	base, pool := newStore(t)
	ctx := context.Background()
	q := func(sql string) string { return migrate.RenderSchema(sql, base.Schema()) }
	if _, err := pool.Exec(ctx, q(`INSERT INTO `+migrate.SchemaToken+`domain_events (type, payload, occurred_at, inserted_at)
		SELECT 'order.paid', '{}', $1, $1 FROM generate_series(1, 4000)`), t0); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, q(`INSERT INTO `+migrate.SchemaToken+`run_queue (def_name, serial_key, trigger_event_id, event_at, created_at)
		SELECT CASE WHEN id % 2 = 0 THEN 'enrol' ELSE 'refund' END, 'k' || (id % 500), id, $1, $1
		FROM `+migrate.SchemaToken+`domain_events`), t0); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, q(`ANALYZE `+migrate.SchemaToken+`run_queue`)); err != nil {
		t.Fatal(err)
	}

	rec := &queryRecorder{marker: "QueuedEventExists"}
	s := tracedStore(t, base, rec)
	inTx(t, s, func(tx Tx) {
		if ok, err := s.QueuedEventExists(ctx, tx, "enrol", 2000); err != nil || !ok {
			t.Fatalf("QueuedEventExists = %v, %v; want true", ok, err)
		}
	})
	if rec.sql == "" {
		t.Fatal("QueuedEventExists ran no traced query")
	}
	var raw []byte
	if err := pool.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+rec.sql, rec.args...).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var plans []struct {
		Plan map[string]any `json:"Plan"`
	}
	if err := json.Unmarshal(raw, &plans); err != nil || len(plans) != 1 {
		t.Fatalf("decode plan %s: %v", raw, err)
	}
	used := map[string]bool{}
	planIndexes(plans[0].Plan, used)
	if !used["rq_event"] || strings.Contains(string(raw), `"Seq Scan"`) {
		t.Errorf("QueuedEventExists plan does not look the event up by rq_event:\n%s", raw)
	}
}
