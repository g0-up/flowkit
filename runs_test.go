package flowkit_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/g0-up/flowkit"
	"github.com/g0-up/flowkit/flowkittest"
)

// followBody notes a paid order, waits for its delivery, checks the note
// and ends delivered.
const followBody = `{
	"metamodel_version": 1,
	"trigger": {"event": "order.paid"},
	"concurrency": {"policy": "serial", "serial_key": "event.customer_id"},
	"steps": [
		{"id": "note", "type": "action", "action": "note_order",
		 "params": {"order_id": {"$expr": "event.order_id"}}, "next": "delivered"},
		{"id": "delivered", "type": "wait_for_event", "event": "shipment.delivered",
		 "match": "event.order_id == 'o-1'", "timeout": "1h", "on_timeout": "late", "next": "check"},
		{"id": "check", "type": "switch", "cases": [{"when": "facts.order.noted", "next": "done"}], "default": "late"},
		{"id": "done", "type": "end", "name": "delivered"},
		{"id": "late", "type": "end", "name": "late"}
	]
}`

// noteAction is the note_order action, counting its calls. While fail is
// set it fails without saying whether it took effect.
type noteAction struct {
	mu    sync.Mutex
	calls int
	fail  bool
}

func (a *noteAction) def(idempotent bool) flowkit.ActionDef {
	return flowkit.ActionDef{
		Name:         "note_order",
		ParamsSchema: json.RawMessage(`{"type":"object","required":["order_id"],"properties":{"order_id":{"type":"string"}}}`),
		OutputFacts:  []flowkit.OutputFact{{Name: "order.noted", Type: flowkit.FactBool}},
		Idempotent:   idempotent,
		Execute: func(context.Context, flowkit.ActionInput) (flowkit.ActionResult, error) {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.calls++
			if a.fail {
				return flowkit.ActionResult{}, errors.New("connection reset")
			}
			return flowkit.ActionResult{Output: map[string]any{"order.noted": true}}, nil
		},
	}
}

func (a *noteAction) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

func (a *noteAction) setFail(fail bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.fail = fail
}

var paidOrder = flowkit.Event{Type: "order.paid", Payload: map[string]any{
	"order_id": "o-1", "customer_id": "c-1", "email": "c1@example.com", "total": 10,
	"items": []any{map[string]any{"sku": "s", "qty": 1}},
}}

// workerConfig makes an Engine work jobs, polling fast.
func workerConfig(c *flowkit.Config) {
	c.Roles = []flowkit.Role{flowkit.RoleWorker}
	c.Workers.PollInterval = 100 * time.Millisecond
}

// awaitState waits until the only run of follow is in state.
func awaitState(t *testing.T, pool *pgxpool.Pool, schema, want string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var states []string
		rows, err := pool.Query(ctx, "SELECT state FROM "+table(schema, "runs")+" WHERE def_name = 'follow'")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				t.Fatal(err)
			}
			states = append(states, s)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if len(states) == 1 && states[0] == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("runs of follow are %v, want one %s", states, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// awaitCount waits until query counts want.
func awaitCount(t *testing.T, pool *pgxpool.Pool, query string, want int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var n int
		if err := pool.QueryRow(context.Background(), query).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s = %d, want %d", query, n, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// runFollow publishes follow on e, which must work jobs, and drives one
// run of it to completed. It returns the run's id.
func runFollow(t *testing.T, e *flowkit.Engine, pool *pgxpool.Pool, schema string) string {
	t.Helper()
	ctx := context.Background()
	publish(t, e.Definitions(), "follow", json.RawMessage(followBody))
	if err := emitCommitted(t, e, pool, paidOrder); err != nil {
		t.Fatal(err)
	}
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	awaitState(t, pool, schema, "waiting")
	if err := emitCommitted(t, e, pool, flowkit.Event{Type: "shipment.delivered", Payload: map[string]any{"order_id": "o-1"}}); err != nil {
		t.Fatal(err)
	}
	awaitState(t, pool, schema, "completed")
	// Jobs still queued may record more steps; the wait's timeout stays
	// snoozed until its due time.
	awaitCount(t, pool, "SELECT count(*) FROM "+table(schema, "river_job")+" WHERE state IN ('available', 'running', 'retryable')", 0)
	runs, err := e.Runs().List(ctx, flowkit.RunFilter{DefName: "follow"})
	if err != nil || len(runs) != 1 {
		t.Fatalf("List = %v, %v; want the run", runs, err)
	}
	return runs[0].RunID
}

// collect reads every metric of reader by name.
func collect(t *testing.T, reader sdkmetric.Reader) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	out := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

// gauge returns the value of an int64 gauge at the attributes kvs.
func gauge(t *testing.T, m metricdata.Metrics, kvs ...attribute.KeyValue) (int64, bool) {
	t.Helper()
	g, ok := m.Data.(metricdata.Gauge[int64])
	if !ok {
		t.Fatalf("%s is %T, want an int64 gauge", m.Name, m.Data)
	}
	want := attribute.NewSet(kvs...)
	for _, p := range g.DataPoints {
		if p.Attributes.Equals(&want) {
			return p.Value, true
		}
	}
	return 0, false
}

func TestTraceShowsTheStepsOfARunInOrder(t *testing.T) {
	note := &noteAction{}
	reg := sampleRegistry()
	reg.MustAddAction(note.def(true))
	spans := tracetest.NewSpanRecorder()
	reader := sdkmetric.NewManualReader()
	e, pool, schema, _ := newEngine(t, reg, func(c *flowkit.Config) {
		workerConfig(c)
		c.Tracer = sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans))
		c.Meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	})
	id := runFollow(t, e, pool, schema)

	tr, err := e.Runs().Trace(context.Background(), id)
	if err != nil {
		t.Fatalf("Trace: %v", err)
	}
	run := tr.Run
	if run.RunID != id || run.DefName != "follow" || run.DefVersion != 1 || run.State != "completed" ||
		run.TerminalName != "delivered" || run.TriggerEventType != "order.paid" || run.EndedAt.IsZero() ||
		run.StartedAt.IsZero() || run.StepCount != len(tr.Steps) {
		t.Errorf("run = %+v", run)
	}
	if tr.MetamodelVersion != 1 || tr.LibVersion == "" || tr.TriggerEventID == 0 ||
		tr.TriggerEvent.Type != "order.paid" || tr.TriggerEvent.Payload["order_id"] != "o-1" ||
		tr.TriggerEvent.Payload["total"] != json.Number("10") {
		t.Errorf("trace = metamodel %d lib %q event %d %+v", tr.MetamodelVersion, tr.LibVersion, tr.TriggerEventID, tr.TriggerEvent)
	}
	var kinds []string
	for i, s := range tr.Steps {
		kinds = append(kinds, s.StepID+":"+s.Kind)
		if s.Seq != i+1 {
			t.Errorf("step %d has seq %d", i, s.Seq)
		}
		if i > 0 && s.At.Before(tr.Steps[i-1].At) {
			t.Errorf("step %d recorded before step %d", i, i-1)
		}
	}
	want := []string{"note:entered", "note:executed", "delivered:waiting", "delivered:claimed", "check:branched", "done:terminal"}
	if !slices.Equal(kinds, want) {
		t.Fatalf("steps = %v, want %v", kinds, want)
	}
	snap := func(i int) map[string]any { return tr.Steps[i].Snapshot }
	if key, _ := snap(1)["idem_key"].(string); !strings.HasSuffix(key, ":note:1") || snap(1)["next"] != "delivered" {
		t.Errorf("executed snapshot = %v", snap(1))
	}
	if _, ok := snap(3)["event_id"].(json.Number); !ok || snap(3)["next"] != "check" {
		t.Errorf("claimed snapshot = %v", snap(3))
	}
	if snap(4)["next"] != "done" {
		t.Errorf("branched snapshot = %v", snap(4))
	}

	names := map[string]bool{}
	for _, s := range spans.Ended() {
		names[s.Name()] = true
		if s.Name() == "flowkit.run" {
			attrs := attribute.NewSet(s.Attributes()...)
			if v, _ := attrs.Value("def_name"); v.AsString() != "follow" {
				t.Errorf("flowkit.run span attributes = %v", s.Attributes())
			}
			for _, k := range []attribute.Key{"def_version", "metamodel_version", "lib_version", "instance_id"} {
				if !attrs.HasValue(k) {
					t.Errorf("flowkit.run span lacks %s", k)
				}
			}
		}
	}
	if !names["flowkit.run"] || !names["flowkit.step"] {
		t.Errorf("spans = %v, want flowkit.run and flowkit.step", names)
	}

	metrics := collect(t, reader)
	for _, name := range []string{
		"flowkit.events_emitted", "flowkit.routes", "flowkit.runs_started", "flowkit.runs_terminal",
		"flowkit.effect_duration", "flowkit.effects_inflight", "flowkit.wake_latency",
		"flowkit.dead_letters_open", "flowkit.in_doubt_open", "flowkit.queue_depth", "flowkit.scheduler_leader",
	} {
		if _, ok := metrics[name]; !ok {
			t.Errorf("metric %s was not reported", name)
		}
	}
	if sum, ok := metrics["flowkit.events_emitted"].Data.(metricdata.Sum[int64]); !ok || len(sum.DataPoints) != 2 {
		t.Errorf("events_emitted = %+v, want one series per event type", metrics["flowkit.events_emitted"].Data)
	}
	for _, kind := range []string{"effect_in_doubt", "exhausted", "schema_mismatch", "unroutable_event"} {
		if v, ok := gauge(t, metrics["flowkit.dead_letters_open"], attribute.String("kind", kind)); !ok || v != 0 {
			t.Errorf("dead_letters_open{kind=%s} = %d (reported %v), want 0", kind, v, ok)
		}
	}
	if v, ok := gauge(t, metrics["flowkit.scheduler_leader"], attribute.String("instance", "test")); !ok || v != 0 {
		t.Errorf("scheduler_leader of a worker-only instance = %d (reported %v), want 0", v, ok)
	}
}

func TestRetentionKeepsTheSummaryOfARunListed(t *testing.T) {
	note := &noteAction{}
	reg := sampleRegistry()
	reg.MustAddAction(note.def(true))
	e, pool, schema, clock := newEngine(t, reg, workerConfig)
	id := runFollow(t, e, pool, schema)
	ctx := context.Background()

	// The wait's timeout is never cancelled: once a leader schedules it, it
	// records claim_lost on the finished run. Let it land before reading
	// the live summary, under a leader that stops before the retention.
	clock.Advance(2 * time.Hour)
	first := newEngineOn(t, pool, schema, clock, reg, func(c *flowkit.Config) {
		c.Roles = []flowkit.Role{flowkit.RoleJanitor}
		c.InstanceID = "first"
	})
	if err := first.Start(ctx); err != nil {
		t.Fatal(err)
	}
	awaitCount(t, pool, "SELECT count(*) FROM "+table(schema, "run_steps")+" WHERE kind = 'claim_lost'", 1)
	awaitCount(t, pool, "SELECT count(*) FROM "+table(schema, "river_job")+" WHERE state NOT IN ('completed', 'cancelled', 'discarded')", 0)
	before, err := e.Runs().List(ctx, flowkit.RunFilter{DefName: "follow"})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// A janitor instance started 91 days later passes on start.
	clock.Advance(91 * 24 * time.Hour)
	reader := sdkmetric.NewManualReader()
	janitor := newEngineOn(t, pool, schema, clock, reg, func(c *flowkit.Config) {
		c.Roles = []flowkit.Role{flowkit.RoleJanitor}
		c.InstanceID = "janitor"
		c.Meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	})
	if err := janitor.Start(ctx); err != nil {
		t.Fatal(err)
	}
	awaitCount(t, pool, "SELECT count(*) FROM "+table(schema, "runs"), 0)

	for _, name := range []string{"run_steps", "consumed_events", "subscriptions", "executed_effects"} {
		if n := countRows(t, pool, table(schema, name)); n != 0 {
			t.Errorf("%s keeps %d rows", name, n)
		}
	}
	if _, err := e.Runs().Trace(ctx, id); !errors.Is(err, flowkit.ErrNotFound) {
		t.Fatalf("Trace of a summarised run = %v, want ErrNotFound", err)
	}
	after, err := e.Runs().List(ctx, flowkit.RunFilter{DefName: "follow", States: []string{"completed"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 1 || after[0] != before[0] {
		t.Fatalf("List after retention = %+v, want the same summary as before %+v", after, before)
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		if v, _ := gauge(t, collect(t, reader)["flowkit.scheduler_leader"], attribute.String("instance", "janitor")); v == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the janitor instance never reported itself the scheduler leader")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// newEngineOn builds an Engine on an existing schema, closed when the test
// ends.
func newEngineOn(t *testing.T, pool *pgxpool.Pool, schema string, clock *flowkittest.VirtualClock, reg *flowkit.Registry, mutate func(*flowkit.Config)) *flowkit.Engine {
	t.Helper()
	cfg := flowkit.Config{Pool: pool, Schema: schema, Clock: clock, Logger: testLogger(t), InstanceID: "test"}
	mutate(&cfg)
	e, err := flowkit.New(cfg, reg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := e.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return e
}

// openDeadLetter returns the id of the only open dead letter of kind.
func openDeadLetter(t *testing.T, pool *pgxpool.Pool, schema, kind string) int64 {
	t.Helper()
	var id int64
	err := pool.QueryRow(context.Background(),
		"SELECT id FROM "+table(schema, "dead_letters")+" WHERE kind = $1 AND resolved_at IS NULL", kind).Scan(&id)
	if err != nil {
		t.Fatalf("open %s dead letter: %v", kind, err)
	}
	return id
}

// inDoubtFollow starts follow with a note_order that is not idempotent and
// fails, and waits until the run's effect is in doubt. Then note_order
// succeeds. It returns the run and the dead letter.
func inDoubtFollow(t *testing.T, note *noteAction) (*flowkit.Engine, *pgxpool.Pool, string, string, int64) {
	t.Helper()
	reg := sampleRegistry()
	reg.MustAddAction(note.def(false))
	note.setFail(true)
	e, pool, schema, _ := newEngine(t, reg, workerConfig)
	publish(t, e.Definitions(), "follow", json.RawMessage(followBody))
	if err := emitCommitted(t, e, pool, paidOrder); err != nil {
		t.Fatal(err)
	}
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	awaitCount(t, pool, "SELECT count(*) FROM "+table(schema, "dead_letters")+" WHERE kind = 'effect_in_doubt'", 1)
	note.setFail(false)
	runs, err := e.Runs().List(context.Background(), flowkit.RunFilter{DefName: "follow"})
	if err != nil || len(runs) != 1 || runs[0].State != "running" {
		t.Fatalf("List = %+v, %v; want the running run", runs, err)
	}
	return e, pool, schema, runs[0].RunID, openDeadLetter(t, pool, schema, "effect_in_doubt")
}

func TestConcurrentRedrivesThroughRunsExecuteOnce(t *testing.T) {
	note := &noteAction{}
	e, pool, schema, _, dl := inDoubtFollow(t, note)
	ctx := context.Background()

	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Go(func() {
			errs[i] = e.Runs().ResolveDeadLetter(ctx, dl, flowkit.Resolution{Kind: flowkit.ResolveRedrive, Actor: "ops", Reason: "provider says not sent"})
		})
	}
	wg.Wait()
	var won, lost int
	for _, err := range errs {
		switch {
		case err == nil:
			won++
		case errors.Is(err, flowkit.ErrAlreadyResolved):
			lost++
		default:
			t.Fatalf("redrive = %v", err)
		}
	}
	if won != 1 || lost != 1 {
		t.Fatalf("redrives: %d won, %d already resolved", won, lost)
	}
	awaitState(t, pool, schema, "waiting")
	if n := note.count(); n != 2 {
		t.Fatalf("note_order ran %d times, want the failed try and one redriven run", n)
	}
}

func TestResolveDeadLetterThroughRuns(t *testing.T) {
	note := &noteAction{}
	e, pool, schema, runID, dl := inDoubtFollow(t, note)
	ctx := context.Background()
	runs := e.Runs()

	err := runs.ResolveDeadLetter(ctx, dl, flowkit.Resolution{Kind: flowkit.ResolveMarkDone, Output: map[string]any{"order.noted": "yes"}})
	if verrs := validationErrors(t, err); len(verrs) != 1 || verrs[0].Path != "/Output" {
		t.Fatalf("mark done with a bad output = %v, want a ValidationError at /Output", err)
	}
	if err := runs.ResolveDeadLetter(ctx, dl, flowkit.Resolution{}); validationErrors(t, err)[0].Path != "/Kind" {
		t.Fatalf("resolve without a kind = %v, want a ValidationError at /Kind", err)
	}
	if err := runs.ResolveDeadLetter(ctx, dl+1000, flowkit.Resolution{Kind: flowkit.ResolveSkip}); !errors.Is(err, flowkit.ErrNotFound) {
		t.Fatalf("resolve of a missing dead letter = %v, want ErrNotFound", err)
	}

	if err := runs.ResolveDeadLetter(ctx, dl, flowkit.Resolution{
		Kind: flowkit.ResolveMarkDone, Actor: "ops", Reason: "noted by hand", Output: map[string]any{"order.noted": true},
	}); err != nil {
		t.Fatalf("mark done = %v", err)
	}
	awaitState(t, pool, schema, "waiting")
	if n := note.count(); n != 1 {
		t.Fatalf("note_order ran %d times after mark done, want only the failed try", n)
	}
	if err := runs.ResolveDeadLetter(ctx, dl, flowkit.Resolution{Kind: flowkit.ResolveSkip}); !errors.Is(err, flowkit.ErrAlreadyResolved) {
		t.Fatalf("second resolve = %v, want ErrAlreadyResolved", err)
	}

	if err := runs.Cancel(ctx, runID, "order refunded"); err != nil {
		t.Fatalf("Cancel = %v", err)
	}
	tr, err := runs.Trace(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	last := tr.Steps[len(tr.Steps)-1]
	if tr.Run.State != "cancelled" || tr.Run.TerminalName != "$cancelled" || last.Kind != "terminal" || last.Error != "order refunded" {
		t.Fatalf("cancelled run = %+v, last step %+v", tr.Run, last)
	}
	if err := runs.Cancel(ctx, runID, "again"); err != nil {
		t.Fatalf("Cancel of a cancelled run = %v, want nil", err)
	}
}

func TestRunsRejectUnknownRunsAndBadFilters(t *testing.T) {
	e, _, _, _ := newEngine(t, sampleRegistry(), nil)
	ctx := context.Background()
	runs := e.Runs()
	for _, id := range []string{"not-a-uuid", "6f1c1f9e-3c1a-5b8e-9d2f-1a2b3c4d5e6f"} {
		if _, err := runs.Trace(ctx, id); !errors.Is(err, flowkit.ErrNotFound) {
			t.Errorf("Trace(%q) = %v, want ErrNotFound", id, err)
		}
		if err := runs.Cancel(ctx, id, ""); !errors.Is(err, flowkit.ErrNotFound) {
			t.Errorf("Cancel(%q) = %v, want ErrNotFound", id, err)
		}
	}
	for _, c := range []struct {
		filter flowkit.RunFilter
		path   string
	}{
		{flowkit.RunFilter{States: []string{"completed", "done"}}, "/States/1"},
		{flowkit.RunFilter{Limit: -1}, "/Limit"},
		{flowkit.RunFilter{Limit: 1001}, "/Limit"},
		{flowkit.RunFilter{After: "not a cursor"}, "/After"},
		{flowkit.RunFilter{After: "MTIzLm5vdC1hLXV1aWQ"}, "/After"},
	} {
		_, err := runs.List(ctx, c.filter)
		if verrs := validationErrors(t, err); len(verrs) != 1 || verrs[0].Path != c.path {
			t.Errorf("List(%+v) = %v, want a ValidationError at %s", c.filter, err, c.path)
		}
	}
	if got, err := runs.List(ctx, flowkit.RunFilter{Limit: 1000}); err != nil || len(got) != 0 {
		t.Errorf("List of no runs = %v, %v", got, err)
	}
}

func TestListPagesWithItsCursor(t *testing.T) {
	note := &noteAction{}
	reg := sampleRegistry()
	reg.MustAddAction(note.def(true))
	e, pool, schema, clock := newEngine(t, reg, nil)
	ctx := context.Background()
	publish(t, e.Definitions(), "follow", json.RawMessage(followBody))
	// Three customers, one run each, routed by a worker started later.
	for _, c := range []string{"c-1", "c-2", "c-3"} {
		evt := paidOrder
		evt.Payload = map[string]any{
			"order_id": "o-" + c, "customer_id": c, "email": "c@example.com", "total": 1,
			"items": []any{map[string]any{"sku": "s", "qty": 1}},
		}
		if err := emitCommitted(t, e, pool, evt); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Second)
	}
	worker := newEngineOn(t, pool, schema, clock, reg, workerConfig)
	if err := worker.Start(ctx); err != nil {
		t.Fatal(err)
	}
	awaitCount(t, pool, "SELECT count(*) FROM "+table(schema, "runs")+" WHERE state = 'waiting'", 3)

	var seen []string
	f := flowkit.RunFilter{DefName: "follow", Limit: 2}
	for {
		page, err := e.Runs().List(ctx, f)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range page {
			seen = append(seen, r.RunID)
		}
		if len(page) < f.Limit {
			break
		}
		f.After = page[len(page)-1].Cursor
	}
	if len(seen) != 3 || len(slices.Compact(slices.Sorted(slices.Values(seen)))) != 3 {
		t.Fatalf("pages listed %v, want each of the three runs once", seen)
	}
	if got, err := e.Runs().List(ctx, flowkit.RunFilter{EntityKind: "order", EntityID: "o-c-2"}); err != nil || len(got) != 1 {
		t.Fatalf("List by entity = %+v, %v; want the run of o-c-2", got, err)
	}
	if got, err := e.Runs().List(ctx, flowkit.RunFilter{States: []string{"completed"}}); err != nil || len(got) != 0 {
		t.Fatalf("List of completed runs = %+v, %v; want none", got, err)
	}
}
