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

	"github.com/g0-up/flowkit"
	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/sched"
)

func saveDraft(t *testing.T, defs *flowkit.Definitions, name string, body json.RawMessage) flowkit.DraftID {
	t.Helper()
	id, err := defs.SaveDraft(context.Background(), name, body)
	if err != nil {
		t.Fatalf("SaveDraft %s: %v", name, err)
	}
	return id
}

func publish(t *testing.T, defs *flowkit.Definitions, name string, body json.RawMessage) flowkit.Version {
	t.Helper()
	v, err := defs.Publish(context.Background(), saveDraft(t, defs, name, body), flowkit.PublishOpts{})
	if err != nil {
		t.Fatalf("Publish %s: %v", name, err)
	}
	return v
}

// routeRow is what routing reads from definitions.
type routeRow struct {
	Enabled            bool
	CurrentVersion     *int
	TriggerEventType   *string
	ConcurrencyPolicy  string
	SerialKeyCEL       *string
	DedupWindowSeconds *int
}

func readRoute(t *testing.T, pool *pgxpool.Pool, schema, name string) routeRow {
	t.Helper()
	var r routeRow
	err := pool.QueryRow(context.Background(),
		`SELECT enabled, current_version, trigger_event_type, concurrency_policy, serial_key_cel, dedup_window_seconds
		FROM `+table(schema, "definitions")+` WHERE name = $1`, name).
		Scan(&r.Enabled, &r.CurrentVersion, &r.TriggerEventType, &r.ConcurrencyPolicy, &r.SerialKeyCEL, &r.DedupWindowSeconds)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func deref[T any](p *T) any {
	if p == nil {
		return nil
	}
	return *p
}

func TestPublishWritesVersionsAndRouteColumns(t *testing.T) {
	e, pool, schema, clock := newEngine(t, sampleRegistry(), nil)
	defs := e.Definitions()
	ctx := context.Background()
	sellkit := readTestdata(t, "valid/sellkit-fulfillment.json")
	lms := readTestdata(t, "valid/lms-onboarding.json")

	id := saveDraft(t, defs, "onboarding", sellkit)
	v1, err := defs.Publish(ctx, id, flowkit.PublishOpts{Actor: "alice", Note: "first cut"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if v1 != 1 {
		t.Fatalf("first version %d, want 1", v1)
	}
	r := readRoute(t, pool, schema, "onboarding")
	if !r.Enabled || deref(r.CurrentVersion) != 1 || deref(r.TriggerEventType) != "order.paid" ||
		r.ConcurrencyPolicy != "serial" || deref(r.SerialKeyCEL) != "event.customer_id" || r.DedupWindowSeconds != nil {
		t.Fatalf("route columns after v1: enabled %v version %v trigger %v policy %q key %v window %v",
			r.Enabled, deref(r.CurrentVersion), deref(r.TriggerEventType), r.ConcurrencyPolicy, deref(r.SerialKeyCEL), deref(r.DedupWindowSeconds))
	}

	clock.Advance(time.Hour)
	if v2 := publish(t, defs, "onboarding", lms); v2 != 2 {
		t.Fatalf("second version %d, want 2", v2)
	}
	r = readRoute(t, pool, schema, "onboarding")
	if !r.Enabled || deref(r.CurrentVersion) != 2 || deref(r.TriggerEventType) != "student.enrolled" ||
		r.ConcurrencyPolicy != "dedup_window" || deref(r.SerialKeyCEL) != "event.student_id + '/' + event.course_id" ||
		deref(r.DedupWindowSeconds) != 3600 {
		t.Fatalf("route columns after v2: enabled %v version %v trigger %v policy %q key %v window %v",
			r.Enabled, deref(r.CurrentVersion), deref(r.TriggerEventType), r.ConcurrencyPolicy, deref(r.SerialKeyCEL), deref(r.DedupWindowSeconds))
	}

	got, err := defs.Get(ctx, "onboarding", 1)
	if err != nil {
		t.Fatalf("Get v1: %v", err)
	}
	if got.Name != "onboarding" || got.Version != 1 || got.MetamodelVersion != 1 || got.PublishedBy != "alice" ||
		got.Note != "first cut" || !got.PublishedAt.Equal(t0) || len(got.Checksum) != 64 {
		t.Fatalf("Get v1 = %+v", got)
	}
	if !jsonEqual(t, got.Body, sellkit) {
		t.Fatalf("v1 body %s is not the published body", got.Body)
	}
	got2, err := defs.Get(ctx, "onboarding", 2)
	if err != nil {
		t.Fatalf("Get v2: %v", err)
	}
	if got2.PublishedBy != "" || got2.Note != "" || !got2.PublishedAt.Equal(t0.Add(time.Hour)) || !jsonEqual(t, got2.Body, lms) {
		t.Fatalf("Get v2 = %+v", got2)
	}
	if got2.Checksum == got.Checksum {
		t.Fatal("different bodies have the same checksum")
	}

	for _, v := range []int{0, 3} {
		if _, err := defs.Get(ctx, "onboarding", v); !errors.Is(err, flowkit.ErrNotFound) {
			t.Fatalf("Get version %d: %v, want ErrNotFound", v, err)
		}
	}
	if _, err := defs.Get(ctx, "nope", 1); !errors.Is(err, flowkit.ErrNotFound) {
		t.Fatalf("Get unknown definition: %v, want ErrNotFound", err)
	}
}

func jsonEqual(t *testing.T, a, b json.RawMessage) bool {
	t.Helper()
	var x, y any
	if err := json.Unmarshal(a, &x); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &y); err != nil {
		t.Fatal(err)
	}
	ax, _ := json.Marshal(x)
	by, _ := json.Marshal(y)
	return string(ax) == string(by)
}

func TestConcurrentPublishesGetDistinctVersions(t *testing.T) {
	e, _, _, _ := newEngine(t, sampleRegistry(), nil)
	defs := e.Definitions()
	body := readTestdata(t, "valid/sellkit-fulfillment.json")
	drafts := []flowkit.DraftID{saveDraft(t, defs, "fulfillment", body), saveDraft(t, defs, "fulfillment", body)}

	versions := make([]int, len(drafts))
	errs := make([]error, len(drafts))
	var wg sync.WaitGroup
	for i, id := range drafts {
		wg.Go(func() {
			v, err := defs.Publish(context.Background(), id, flowkit.PublishOpts{})
			versions[i], errs[i] = int(v), err
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		t.Fatal(err)
	}
	slices.Sort(versions)
	if !slices.Equal(versions, []int{1, 2}) {
		t.Fatalf("versions %v, want [1 2]", versions)
	}
}

func TestChecksumIgnoresFormatting(t *testing.T) {
	e, _, _, _ := newEngine(t, sampleRegistry(), nil)
	defs := e.Definitions()
	ctx := context.Background()
	body := json.RawMessage(`{
		"metamodel_version": 1,
		"trigger": {"event": "order.paid"},
		"steps": [
			{"id": "escalate", "type": "action", "action": "open_ticket",
			 "params": {"order_id": {"$expr": "event.order_id"}, "reason": "damaged", "priority": 2}, "next": "done"},
			{"id": "done", "type": "end", "name": "done"}
		]
	}`)
	reordered := json.RawMessage(`{"steps":[{"next":"done","params":{"priority":2.0,"reason":"damaged",` +
		`"order_id":{"$expr":"event.order_id"}},"action":"open_ticket","type":"action","id":"escalate"},` +
		`{"name":"done","type":"end","id":"done"}],"trigger":{"event":"order.paid"},"metamodel_version":1}`)
	changed := json.RawMessage(`{"steps":[{"next":"done","params":{"priority":3,"reason":"damaged",` +
		`"order_id":{"$expr":"event.order_id"}},"action":"open_ticket","type":"action","id":"escalate"},` +
		`{"name":"done","type":"end","id":"done"}],"trigger":{"event":"order.paid"},"metamodel_version":1}`)

	checksum := func(name string, body json.RawMessage) string {
		t.Helper()
		publish(t, defs, name, body)
		v, err := defs.Get(ctx, name, 1)
		if err != nil {
			t.Fatal(err)
		}
		return v.Checksum
	}
	a, b, c := checksum("a", body), checksum("b", reordered), checksum("c", changed)
	if a != b {
		t.Fatalf("checksums differ for the same definition: %s and %s", a, b)
	}
	if a == c {
		t.Fatal("checksum unchanged by a changed param")
	}
}

func TestSaveDraftRejects(t *testing.T) {
	e, pool, schema, _ := newEngine(t, sampleRegistry(), nil)
	defs := e.Definitions()
	ctx := context.Background()
	valid := readTestdata(t, "valid/sellkit-fulfillment.json")

	_, err := defs.SaveDraft(ctx, "broken", readTestdata(t, "invalid/has-guard-missing.json"))
	if verrs := validationErrors(t, err); verrs[0].Path != "/steps/0/params/country/$expr" {
		t.Fatalf("SaveDraft errors %v", verrs)
	}
	outOfRange := string(readTestdata(t, "invalid/number-out-of-range.json"))
	for _, n := range []string{"1e131072", "1e131000", "1e-1000000000"} {
		_, err := defs.SaveDraft(ctx, "refund", json.RawMessage(strings.Replace(outOfRange, "1e131072", n, 1)))
		if verrs := validationErrors(t, err); verrs[0].Path != "/steps/0/params/amount" {
			t.Fatalf("SaveDraft amount %s: %v", n, verrs)
		}
	}
	for _, name := range []string{"", string(make([]byte, 256)), "bad\x00name", "\xff"} {
		if _, err := defs.SaveDraft(ctx, name, valid); err == nil {
			t.Fatalf("SaveDraft accepted name %q", name)
		}
	}
	if n := countRows(t, pool, table(schema, "drafts")); n != 0 {
		t.Fatalf("%d drafts stored, want 0", n)
	}
	if err := defs.Validate(valid); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if err := defs.Validate(readTestdata(t, "invalid/step-unreachable.json")); len(validationErrors(t, err)) != 1 {
		t.Fatalf("Validate: %v", err)
	}
}

func TestPublishRejects(t *testing.T) {
	e, pool, schema, _ := newEngine(t, sampleRegistry(), nil)
	defs := e.Definitions()
	ctx := context.Background()

	for _, id := range []flowkit.DraftID{"not-a-uuid", flowkit.DraftID(ids.NewV4().String())} {
		if _, err := defs.Publish(ctx, id, flowkit.PublishOpts{}); !errors.Is(err, flowkit.ErrNotFound) {
			t.Fatalf("Publish %q: %v, want ErrNotFound", id, err)
		}
	}
	id := saveDraft(t, defs, "fulfillment", readTestdata(t, "valid/sellkit-fulfillment.json"))
	if _, err := defs.Publish(ctx, id, flowkit.PublishOpts{Actor: "bad\x00actor"}); err == nil {
		t.Fatal("Publish accepted an actor with U+0000")
	}

	// An Engine whose registry dropped an action the draft uses refuses to
	// publish it.
	other := flowkit.NewRegistry()
	other.MustAddEvent(flowkit.EventDef{Type: "order.paid", PayloadSchema: json.RawMessage(`{"type": "object"}`)})
	e2, err := flowkit.New(flowkit.Config{Pool: pool, Schema: schema, Logger: testLogger(t), InstanceID: "other"}, other)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := e2.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	_, err = e2.Definitions().Publish(ctx, id, flowkit.PublishOpts{})
	if verrs := validationErrors(t, err); !slices.ContainsFunc(verrs, func(v *flowkit.ValidationError) bool {
		return v.Path == "/steps/0/action"
	}) {
		t.Fatalf("Publish errors %v", verrs)
	}
	if n := countRows(t, pool, table(schema, "definitions")); n != 0 {
		t.Fatalf("%d definitions after failed publishes, want 0", n)
	}
}

func TestRefs(t *testing.T) {
	e, _, _, _ := newEngine(t, sampleRegistry(), nil)
	defs := e.Definitions()
	ctx := context.Background()
	publish(t, defs, "fulfillment", readTestdata(t, "valid/sellkit-fulfillment.json"))
	publish(t, defs, "onboarding", readTestdata(t, "valid/lms-onboarding.json"))
	publish(t, defs, "onboarding", readTestdata(t, "valid/lms-onboarding.json"))

	tests := []struct {
		ref  flowkit.ComponentRef
		want []flowkit.DefinitionRef
	}{
		{flowkit.ComponentRef{Kind: flowkit.ComponentAction, Ref: "send_email"},
			[]flowkit.DefinitionRef{{Name: "fulfillment", Version: 1}, {Name: "onboarding", Version: 1}, {Name: "onboarding", Version: 2}}},
		{flowkit.ComponentRef{Kind: flowkit.ComponentEvent, Ref: "order.paid"}, []flowkit.DefinitionRef{{Name: "fulfillment", Version: 1}}},
		{flowkit.ComponentRef{Kind: flowkit.ComponentEvent, Ref: "lesson.completed"},
			[]flowkit.DefinitionRef{{Name: "onboarding", Version: 1}, {Name: "onboarding", Version: 2}}},
		{flowkit.ComponentRef{Kind: flowkit.ComponentFact, Ref: "customer.lifetime_value"}, []flowkit.DefinitionRef{{Name: "fulfillment", Version: 1}}},
		{flowkit.ComponentRef{Kind: flowkit.ComponentFact, Ref: "customer.is_vip"}, []flowkit.DefinitionRef{{Name: "fulfillment", Version: 1}}},
		{flowkit.ComponentRef{Kind: flowkit.ComponentFact, Ref: "student.prior_courses"},
			[]flowkit.DefinitionRef{{Name: "onboarding", Version: 1}, {Name: "onboarding", Version: 2}}},
		// Output facts belong to their action.
		{flowkit.ComponentRef{Kind: flowkit.ComponentFact, Ref: "shipment.tracking"}, []flowkit.DefinitionRef{}},
		{flowkit.ComponentRef{Kind: flowkit.ComponentAction, Ref: "unused"}, []flowkit.DefinitionRef{}},
	}
	for _, tt := range tests {
		got, err := defs.Refs(ctx, tt.ref)
		if err != nil {
			t.Fatalf("Refs %v: %v", tt.ref, err)
		}
		if !slices.Equal(got, tt.want) {
			t.Errorf("Refs %v = %v, want %v", tt.ref, got, tt.want)
		}
	}
	if _, err := defs.Refs(ctx, flowkit.ComponentRef{Kind: "step", Ref: "x"}); err == nil {
		t.Fatal("Refs accepted an unknown kind")
	}
}

// insertRun stores a run of a published definition in the given state.
func insertRun(t *testing.T, pool *pgxpool.Pool, schema, def string, version int, state string, eventID int64) ids.UUID {
	t.Helper()
	id := ids.NewV4()
	terminal := state != "running" && state != "waiting"
	_, err := pool.Exec(context.Background(), `INSERT INTO `+table(schema, "runs")+`
		(id, def_name, def_version, metamodel_version, lib_version, definition, trigger_event_id, state, terminal,
		 exclusive, wait_seq, lookback_pending, frames, facts, rev, step_seq, created_at, updated_at)
		VALUES ($1, $2, $3, 1, 'test', '{}', $4, $5, $6, false, 0, false, '[]', '{}', 0, 0, $7, $7)`,
		id, def, version, eventID, state, terminal, t0)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func pumpJobs(t *testing.T, pool *pgxpool.Pool, schema string) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		"SELECT args->>'run_id' FROM "+table(schema, "river_job")+" WHERE kind = $1 ORDER BY args->>'run_id'", sched.KindPumpRun)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestEnableResumesRunningRunsAndPinnedRuns(t *testing.T) {
	e, pool, schema, _ := newEngine(t, sampleRegistry(), nil)
	defs := e.Definitions()
	ctx := context.Background()
	body := readTestdata(t, "valid/sellkit-fulfillment.json")
	publish(t, defs, "fulfillment", body)
	publish(t, defs, "fulfillment", body)
	publish(t, defs, "other", body)

	var eventID int64
	err := pool.QueryRow(ctx, `INSERT INTO `+table(schema, "domain_events")+
		` (type, payload, occurred_at, inserted_at) VALUES ('order.paid', '{}', $1, $1) RETURNING id`, t0).Scan(&eventID)
	if err != nil {
		t.Fatal(err)
	}
	running := []ids.UUID{
		insertRun(t, pool, schema, "fulfillment", 1, "running", eventID),
		insertRun(t, pool, schema, "fulfillment", 2, "running", eventID),
	}
	insertRun(t, pool, schema, "fulfillment", 1, "waiting", eventID)
	insertRun(t, pool, schema, "fulfillment", 1, "completed", eventID)
	insertRun(t, pool, schema, "fulfillment", 2, "failed", eventID)
	insertRun(t, pool, schema, "other", 1, "running", eventID)

	pinned, err := defs.PinnedRuns(ctx, "fulfillment")
	if err != nil {
		t.Fatalf("PinnedRuns: %v", err)
	}
	if len(pinned) != 2 || pinned[1] != 2 || pinned[2] != 1 {
		t.Fatalf("PinnedRuns = %v, want map[1:2 2:1]", pinned)
	}
	if pinned, err := defs.PinnedRuns(ctx, "nope"); err != nil || len(pinned) != 0 {
		t.Fatalf("PinnedRuns of an unknown definition = %v, %v", pinned, err)
	}

	if err := defs.Disable(ctx, "fulfillment"); err != nil {
		t.Fatalf("Disable: %v", err)
	}
	if readRoute(t, pool, schema, "fulfillment").Enabled {
		t.Fatal("definition still enabled after Disable")
	}
	if jobs := pumpJobs(t, pool, schema); len(jobs) != 0 {
		t.Fatalf("Disable enqueued pumps %v", jobs)
	}

	if err := defs.Enable(ctx, "fulfillment"); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	if !readRoute(t, pool, schema, "fulfillment").Enabled {
		t.Fatal("definition disabled after Enable")
	}
	want := []string{running[0].String(), running[1].String()}
	slices.Sort(want)
	if jobs := pumpJobs(t, pool, schema); !slices.Equal(jobs, want) {
		t.Fatalf("Enable enqueued pumps of %v, want the running runs %v", jobs, want)
	}

	for _, op := range []func(context.Context, string) error{defs.Enable, defs.Disable} {
		if err := op(ctx, "nope"); !errors.Is(err, flowkit.ErrNotFound) {
			t.Fatalf("lifecycle change of an unknown definition: %v, want ErrNotFound", err)
		}
	}
}

func TestDefinitionLookupsRefuseImpossibleNames(t *testing.T) {
	e, _, _, _ := newEngine(t, sampleRegistry(), nil)
	defs := e.Definitions()
	ctx := context.Background()
	for _, name := range []string{"", "bad\x00name", "\xff", strings.Repeat("n", 256)} {
		_, getErr := defs.Get(ctx, name, 1)
		_, pinnedErr := defs.PinnedRuns(ctx, name)
		for op, err := range map[string]error{
			"Get":        getErr,
			"Enable":     defs.Enable(ctx, name),
			"Disable":    defs.Disable(ctx, name),
			"PinnedRuns": pinnedErr,
		} {
			if !errors.Is(err, flowkit.ErrNotFound) {
				t.Errorf("%s(%q) = %v, want ErrNotFound", op, name, err)
			}
		}
	}
}
