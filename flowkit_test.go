package flowkit_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/g0-up/flowkit"
	"github.com/g0-up/flowkit/internal/testpg"
)

func TestNewRejectsSchemaDrift(t *testing.T) {
	tests := []struct {
		name    string
		migrate bool
		tamper  string // SQL run on the schema; %s is the schema
		hint    string
	}{
		{name: "not migrated", hint: "flowkit migrate --schema"},
		{name: "behind the library", migrate: true, tamper: `UPDATE %s.flowkit_meta SET v = '0' WHERE k = 'schema_version'`, hint: "flowkit migrate --schema"},
		{name: "needs a newer library", migrate: true, tamper: `UPDATE %s.flowkit_meta SET v = '999' WHERE k = 'min_lib_schema'`, hint: "upgrade flowkit"},
		{name: "foreign table", migrate: true, tamper: `CREATE TABLE %s.extra (id int)`, hint: "upgrade flowkit or restore the schema"},
		{name: "foreign function", migrate: true, tamper: `CREATE FUNCTION %s.f() RETURNS int LANGUAGE sql AS 'SELECT 1'`, hint: "restore the schema"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var pool *pgxpool.Pool
			var schema string
			if tt.migrate {
				pool, schema = migratedSchema(t)
			} else {
				pool, schema = testpg.New(t)
			}
			if tt.tamper != "" {
				if _, err := pool.Exec(context.Background(), strings.ReplaceAll(tt.tamper, "%s", `"`+schema+`"`)); err != nil {
					t.Fatal(err)
				}
			}
			e, err := flowkit.New(flowkit.Config{Pool: pool, Schema: schema, Logger: testLogger(t)}, flowkit.NewRegistry())
			if !errors.Is(err, flowkit.ErrSchemaDrift) {
				t.Fatalf("New = %v, %v; want ErrSchemaDrift", e, err)
			}
			if !strings.Contains(err.Error(), tt.hint) {
				t.Errorf("error %q does not say %q", err, tt.hint)
			}
		})
	}
}

func TestNewOnAMissingSchemaIsDrift(t *testing.T) {
	pool := testpg.Pool(t)
	name, err := testpg.NewSchemaName()
	if err != nil {
		t.Fatal(err)
	}
	_, err = flowkit.New(flowkit.Config{Pool: pool, Schema: name, Logger: testLogger(t)}, flowkit.NewRegistry())
	if !errors.Is(err, flowkit.ErrSchemaDrift) {
		t.Fatalf("New on a schema that does not exist = %v, want ErrSchemaDrift", err)
	}
}

func TestNewDoesNotMigrate(t *testing.T) {
	pool, schema := testpg.New(t)
	if _, err := flowkit.New(flowkit.Config{Pool: pool, Schema: schema, Logger: testLogger(t)}, flowkit.NewRegistry()); err == nil {
		t.Fatal("New succeeded on an empty schema")
	}
	var n int
	err := pool.QueryRow(context.Background(),
		"SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1", schema).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("New created %d objects in the schema", n)
	}
}

// syncBuffer is a log destination safe for River's goroutines.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestStartWorksTheFlowkitQueueAndClose(t *testing.T) {
	var logs syncBuffer
	e, pool, schema, _ := newEngine(t, emitRegistry(), func(c *flowkit.Config) {
		c.Roles = []flowkit.Role{flowkit.RoleWorker}
		c.Workers.PollInterval = 200 * time.Millisecond
		c.Logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	})
	ctx := context.Background()
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "no janitor role") {
		t.Errorf("worker-only Start did not warn about the missing janitor; logs:\n%s", logs.String())
	}
	if err := emitCommitted(t, e, pool, flowkit.Event{Type: "note.added"}); err != nil {
		t.Fatal(err)
	}
	// No definition is triggered by the event, so routing completes the job.
	deadline := time.Now().Add(15 * time.Second)
	for {
		var state, errs string
		err := pool.QueryRow(ctx, "SELECT state::text, coalesce(errors::text, '') FROM "+table(schema, "river_job")).Scan(&state, &errs)
		if err != nil {
			t.Fatal(err)
		}
		if state == "completed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("route job not worked: state %s, errors %s", state, errs)
		}
		time.Sleep(50 * time.Millisecond)
	}
	closeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := e.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Emit keeps working on a closed Engine: it only writes in the caller's
	// transaction.
	if err := emitCommitted(t, e, pool, flowkit.Event{Type: "note.added"}); err != nil {
		t.Fatalf("Emit after Close: %v", err)
	}
}

func TestStartJanitorOnly(t *testing.T) {
	var logs syncBuffer
	e, _, _, _ := newEngine(t, emitRegistry(), func(c *flowkit.Config) {
		c.Roles = []flowkit.Role{flowkit.RoleJanitor}
		c.Logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	})
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("janitor-only Start: %v", err)
	}
	if out := logs.String(); strings.Contains(out, "no janitor role") || !strings.Contains(out, "engine started") {
		t.Errorf("janitor-only Start logs:\n%s", out)
	}
}

func TestCloseWithoutStart(t *testing.T) {
	e, _, _, _ := newEngine(t, emitRegistry(), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := e.Close(ctx); err != nil {
		t.Fatalf("Close of an Engine never started: %v", err)
	}
}

func TestEnginesOnTwoSchemasAreIndependent(t *testing.T) {
	a, pool, schemaA, _ := newEngine(t, emitRegistry(), nil)
	schemaB := testpg.Schema(t, pool)
	if err := migrateOn(pool, schemaB); err != nil {
		t.Fatal(err)
	}
	b, err := flowkit.New(flowkit.Config{Pool: pool, Schema: schemaB, Logger: testLogger(t)}, emitRegistry())
	if err != nil {
		t.Fatal(err)
	}
	evt := flowkit.Event{Type: "order.paid", Payload: map[string]any{"order_id": "o-1", "amount": 1}}
	if err := emitCommitted(t, a, pool, evt); err != nil {
		t.Fatal(err)
	}
	if err := emitCommitted(t, b, pool, evt); err != nil {
		t.Fatalf("same key on another schema: %v", err)
	}
	if len(storedEvents(t, pool, schemaA)) != 1 || len(storedEvents(t, pool, schemaB)) != 1 {
		t.Fatal("events did not land one per schema")
	}
}

func TestStartRoutesEmittedEventsToRuns(t *testing.T) {
	e, pool, schema, _ := newEngine(t, sampleRegistry(), func(c *flowkit.Config) {
		c.Roles = []flowkit.Role{flowkit.RoleWorker}
		c.Workers.PollInterval = 200 * time.Millisecond
	})
	ctx := context.Background()
	// Serial by customer: the second order queues behind the first.
	publish(t, e.Definitions(), "fulfillment", readTestdata(t, "valid/sellkit-fulfillment.json"))
	for _, order := range []string{"o-1", "o-2"} {
		if err := emitCommitted(t, e, pool, flowkit.Event{Type: "order.paid", Payload: map[string]any{
			"order_id": order, "customer_id": "c-1", "email": "c1@example.com", "total": 10,
			"items": []any{map[string]any{"sku": "s", "qty": 1}},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		if err := e.Close(closeCtx); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	count := func(query string) int {
		var n int
		if err := pool.QueryRow(ctx, query).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	deadline := time.Now().Add(15 * time.Second)
	for count("SELECT count(*) FROM "+table(schema, "run_queue")) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("routing did not queue the second order")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if n := count("SELECT count(*) FROM " + table(schema, "runs") + " WHERE def_name = 'fulfillment' AND serial_key = 'c-1' AND exclusive"); n != 1 {
		t.Fatalf("runs = %d, want 1", n)
	}
	if jobs := pumpJobs(t, pool, schema); len(jobs) != 1 {
		t.Fatalf("pump jobs = %v, want the run's first pump", jobs)
	}
}

func TestStartRunsADefinitionThroughAnActionAndAWait(t *testing.T) {
	var (
		mu   sync.Mutex
		sent []flowkit.ActionInput
	)
	reg := sampleRegistry()
	reg.MustAddAction(flowkit.ActionDef{
		Name:         "note_order",
		ParamsSchema: json.RawMessage(`{"type":"object","required":["order_id"],"properties":{"order_id":{"type":"string"}}}`),
		OutputFacts:  []flowkit.OutputFact{{Name: "order.noted", Type: flowkit.FactBool}},
		Idempotent:   true,
		Execute: func(_ context.Context, in flowkit.ActionInput) (flowkit.ActionResult, error) {
			mu.Lock()
			defer mu.Unlock()
			sent = append(sent, in)
			return flowkit.ActionResult{Output: map[string]any{"order.noted": true}}, nil
		},
	})
	e, pool, schema, _ := newEngine(t, reg, func(c *flowkit.Config) {
		c.Roles = []flowkit.Role{flowkit.RoleWorker}
		c.Workers.PollInterval = 100 * time.Millisecond
	})
	ctx := context.Background()
	publish(t, e.Definitions(), "follow", json.RawMessage(`{
		"metamodel_version": 1,
		"trigger": {"event": "order.paid"},
		"steps": [
			{"id": "note", "type": "action", "action": "note_order",
			 "params": {"order_id": {"$expr": "event.order_id"}}, "next": "delivered"},
			{"id": "delivered", "type": "wait_for_event", "event": "shipment.delivered",
			 "match": "event.order_id == 'o-1'", "timeout": "1h", "on_timeout": "late", "next": "check"},
			{"id": "check", "type": "switch", "cases": [{"when": "facts.order.noted", "next": "done"}], "default": "late"},
			{"id": "done", "type": "end", "name": "delivered"},
			{"id": "late", "type": "end", "name": "late"}
		]
	}`))
	if err := emitCommitted(t, e, pool, flowkit.Event{Type: "order.paid", Payload: map[string]any{
		"order_id": "o-1", "customer_id": "c-1", "email": "c1@example.com", "total": 10,
		"items": []any{map[string]any{"sku": "s", "qty": 1}},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}

	state := func() string {
		var s string
		err := pool.QueryRow(ctx, "SELECT state FROM "+table(schema, "runs")+" WHERE def_name = 'follow'").Scan(&s)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	await := func(want string) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for {
			var n int
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table(schema, "runs")+" WHERE def_name = 'follow'").Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n == 1 && state() == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("run did not reach %s", want)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	await("waiting")
	if err := emitCommitted(t, e, pool, flowkit.Event{Type: "shipment.delivered", Payload: map[string]any{"order_id": "o-1"}}); err != nil {
		t.Fatal(err)
	}
	await("completed")

	var name string
	if err := pool.QueryRow(ctx, "SELECT terminal_name FROM "+table(schema, "runs")+" WHERE def_name = 'follow'").Scan(&name); err != nil || name != "delivered" {
		t.Fatalf("terminal name = %q, %v", name, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sent) != 1 || sent[0].Params["order_id"] != "o-1" || !strings.HasSuffix(sent[0].IdempotencyKey, ":note:1") ||
		sent[0].Scope.EntityKind != "order" || sent[0].Scope.EntityID != "o-1" {
		t.Fatalf("note_order calls = %+v, want one for o-1", sent)
	}
}
