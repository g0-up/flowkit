package interpreter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/g0-up/flowkit/internal/executor"
	"github.com/g0-up/flowkit/internal/expr"
	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/interpreter/model"
	"github.com/g0-up/flowkit/internal/migrate"
	"github.com/g0-up/flowkit/internal/sched"
	"github.com/g0-up/flowkit/internal/schema"
	"github.com/g0-up/flowkit/internal/store"
	"github.com/g0-up/flowkit/internal/testpg"
)

var t0 = time.Date(2026, 10, 12, 9, 30, 0, 0, time.UTC)

const (
	testEvent = "order.paid"
	// testWaitEvent is waited for and triggers nothing.
	testWaitEvent = "order.shipped"
	testSchema    = `{"type":"object","properties":{"key":{"type":"string"},"student":{"type":"integer"},"branch":{"type":"string"}},"required":["key"]}`
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// fakeEvents types testEvent and testWaitEvent payloads with testSchema
// and scopes them by the student and branch fields. A payload with
// "misfit":true, or of any other type, does not fit.
type fakeEvents struct {
	shape *expr.Shape
}

func newFakeEvents(t *testing.T) *fakeEvents {
	t.Helper()
	s, err := expr.NewShape(json.RawMessage(testSchema))
	if err != nil {
		t.Fatal(err)
	}
	return &fakeEvents{shape: s}
}

func (f *fakeEvents) Check(eventType string, payload json.RawMessage) (EventInfo, error) {
	if eventType != testEvent && eventType != testWaitEvent {
		return EventInfo{}, fmt.Errorf("event %q is not registered", eventType)
	}
	var p struct {
		Student *json.Number `json:"student"`
		Branch  string       `json:"branch"`
		Misfit  bool         `json:"misfit"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return EventInfo{}, err
	}
	if p.Misfit {
		return EventInfo{}, errors.New("payload does not fit the registered schema")
	}
	info := EventInfo{Shape: f.shape}
	if p.Student != nil {
		info.Scope.EntityKind, info.Scope.EntityID = "student", p.Student.String()
	}
	if p.Branch != "" {
		info.Scope.Extra = map[string]string{"branch": p.Branch}
	}
	return info, nil
}

// fakePumps counts the pumps enqueued per run and the routes enqueued per
// event, including those of transactions that rolled back.
type fakePumps struct {
	mu     sync.Mutex
	runs   map[ids.UUID]int
	events map[int64]int
	fail   error
}

func (p *fakePumps) EnqueuePump(_ context.Context, _ store.Tx, runID ids.UUID) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail != nil {
		return p.fail
	}
	if p.runs == nil {
		p.runs = map[ids.UUID]int{}
	}
	p.runs[runID]++
	return nil
}

func (p *fakePumps) count(runID ids.UUID) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.runs[runID]
}

func (p *fakePumps) EnqueueRoute(_ context.Context, _ store.Tx, eventID int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail != nil {
		return p.fail
	}
	if p.events == nil {
		p.events = map[int64]int{}
	}
	p.events[eventID]++
	return nil
}

func (p *fakePumps) routed(eventID int64) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.events[eventID]
}

// outcomeMeter counts what its Int64Counters add, by instrument name,
// definition and outcome, and by instrument name and attribute set.
type outcomeMeter struct {
	noop.Meter
	mu      sync.Mutex
	counts  map[[3]string]int64
	byAttrs map[string]int64
}

type outcomeCounter struct {
	noop.Int64Counter
	m    *outcomeMeter
	name string
}

func (c outcomeCounter) Add(_ context.Context, v int64, opts ...metric.AddOption) {
	set := metric.NewAddConfig(opts).Attributes()
	def, _ := set.Value("def")
	outcome, _ := set.Value(attribute.Key("outcome"))
	c.m.mu.Lock()
	defer c.m.mu.Unlock()
	if c.m.counts == nil {
		c.m.counts = map[[3]string]int64{}
	}
	c.m.counts[[3]string{c.name, def.AsString(), outcome.AsString()}] += v
	if c.m.byAttrs == nil {
		c.m.byAttrs = map[string]int64{}
	}
	c.m.byAttrs[attrsKey(c.name, set)] += v
}

func attrsKey(name string, set attribute.Set) string {
	return name + " " + set.Encoded(attribute.DefaultEncoder())
}

// added is what instrument name added with exactly the attributes kvs.
func (m *outcomeMeter) added(name string, kvs ...attribute.KeyValue) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.byAttrs[attrsKey(name, attribute.NewSet(kvs...))]
}

func (m *outcomeMeter) Int64Counter(name string, _ ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	return outcomeCounter{m: m, name: name}, nil
}

// count is the flowkit.routes count of def with outcome.
func (m *outcomeMeter) count(def, outcome string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts[[3]string{"flowkit.routes", def, outcome}]
}

// total is the count of instrument name for def.
func (m *outcomeMeter) total(name, def string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for k, v := range m.counts {
		if k[0] == name && k[1] == def {
			n += v
		}
	}
	return n
}

// fakeTimers keeps the wait timeouts scheduled, including those of
// transactions that rolled back.
type fakeTimers struct {
	mu   sync.Mutex
	args []sched.WaitTimeoutArgs
}

func (f *fakeTimers) ScheduleTimeout(_ context.Context, _ store.Tx, args sched.WaitTimeoutArgs) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.args = append(f.args, args)
	return nil
}

// last returns the last timeout scheduled for run.
func (f *fakeTimers) last(t *testing.T, runID ids.UUID) sched.WaitTimeoutArgs {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.args) - 1; i >= 0; i-- {
		if f.args[i].RunID == runID {
			return f.args[i]
		}
	}
	t.Fatalf("no timeout scheduled for run %s", runID)
	return sched.WaitTimeoutArgs{}
}

// testAction is an action the test planner offers.
type testAction struct {
	// params is the params schema; empty means {"type":"object"}.
	params string
	output []executor.OutputFact
	// notIdempotent makes a failed effect go in doubt.
	notIdempotent bool
	retry         executor.RetryPolicy
	execute       func(ctx context.Context, in executor.Input) (map[string]any, error)
}

// testFact is a registered fact the test planner offers.
type testFact struct {
	name    string
	typ     expr.Type
	resolve func(ctx context.Context, q store.ReadQuerier, s expr.Scope) (any, error)
}

// testPlanner compiles definition bodies as the engine does, with one
// environment for every step expression: the trigger typed by testSchema,
// and as facts the planner's facts and the output facts of its actions.
// Waits are typed by testSchema too.
type testPlanner struct {
	mu      sync.Mutex
	actions map[string]testAction
	facts   []testFact
	fail    error
}

func (p *testPlanner) action(name string, a testAction) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.actions == nil {
		p.actions = map[string]testAction{}
	}
	p.actions[name] = a
}

// idempotent reports whether the action name is idempotent, as the
// engine's registry does for the executor.
func (p *testPlanner) idempotent(name string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	a, ok := p.actions[name]
	return ok && !a.notIdempotent
}

func (p *testPlanner) fact(f testFact) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.facts = append(p.facts, f)
}

func (p *testPlanner) Plan(body json.RawMessage) (*Plan, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail != nil {
		return nil, p.fail
	}
	def, vs := model.Parse(body)
	if len(vs) > 0 {
		return nil, fmt.Errorf("definition: %v", vs)
	}
	mm, err := expr.Metamodel(def.MetamodelVersion)
	if err != nil {
		return nil, err
	}
	shape, err := expr.NewShape(json.RawMessage(testSchema))
	if err != nil {
		return nil, err
	}
	var decl []expr.Fact
	plan := &Plan{Def: def, TriggerShape: shape, Steps: make([]StepPlan, len(def.Steps))}
	for _, f := range p.facts {
		decl = append(decl, expr.Fact{Name: f.name, Type: f.typ})
		plan.Facts = append(plan.Facts, FactSource{Name: f.name, Resolve: f.resolve})
	}
	actions := map[string]*ActionPlan{}
	for _, name := range slices.Sorted(maps.Keys(p.actions)) {
		a := p.actions[name]
		for _, o := range a.output {
			decl = append(decl, expr.Fact{Name: o.Name, Type: expr.Type(o.Type)})
		}
		src := a.params
		if src == "" {
			src = `{"type":"object"}`
		}
		compiled, err := schema.Compile(json.RawMessage(src))
		if err != nil {
			return nil, err
		}
		ps, err := expr.NewShape(json.RawMessage(src))
		if err != nil {
			return nil, err
		}
		actions[name] = &ActionPlan{
			Action: executor.Action{Name: name, Idempotent: !a.notIdempotent, Retry: a.retry, OutputFacts: a.output, Execute: a.execute},
			Validate: func(doc any) error {
				if vs := compiled.Validate(doc); len(vs) > 0 {
					return fmt.Errorf("%v", vs)
				}
				return nil
			},
			ParamsShape: ps,
		}
	}
	env, err := expr.NewEnv(shape, decl, mm)
	if err != nil {
		return nil, err
	}
	waitEnv, err := expr.NewEventEnv(shape, mm)
	if err != nil {
		return nil, err
	}
	compile := func(env *expr.Env, src string, want expr.Want) (*expr.Program, error) {
		prog, errs := env.Compile(src, want)
		if len(errs) > 0 {
			return nil, fmt.Errorf("compile %q: %w", src, errors.Join(errs...))
		}
		return prog, nil
	}
	for _, s := range def.Steps {
		sp := &plan.Steps[s.Index]
		sp.Env = env
		switch s.Type {
		case model.StepAction:
			if sp.Action = actions[s.Action.Action]; sp.Action == nil {
				return nil, fmt.Errorf("action %q is not registered", s.Action.Action)
			}
			if s.Action.When != "" {
				if sp.When, err = compile(env, s.Action.When, expr.WantBool); err != nil {
					return nil, err
				}
			}
			placeholders, err := model.ParamExpressions(s.Action.Params)
			if err != nil {
				return nil, err
			}
			sp.Params = map[string]*expr.Program{}
			for _, ph := range placeholders {
				if sp.Params[ph.Pointer], err = compile(env, ph.Source, expr.WantAny); err != nil {
					return nil, err
				}
			}
		case model.StepSwitch:
			for _, c := range s.Switch.Cases {
				prog, err := compile(env, c.When, expr.WantBool)
				if err != nil {
					return nil, err
				}
				sp.Cases = append(sp.Cases, prog)
			}
		case model.StepWait:
			if s.Wait.Match != "" {
				if sp.Match, err = compile(waitEnv, s.Wait.Match, expr.WantBool); err != nil {
					return nil, err
				}
				sp.MatchShape = shape
			}
		}
	}
	return plan, nil
}

func redact(err error) string { return "redacted: " + err.Error() }

// env is a migrated schema and an Interpreter on it.
type env struct {
	t       *testing.T
	pool    *pgxpool.Pool
	store   *store.PgStore
	clock   *fakeClock
	pumps   *fakePumps
	timers  *fakeTimers
	planner *testPlanner
	meter   *outcomeMeter
	events  *fakeEvents
	logs    *logRecorder
	in      *Interpreter
}

// logRecorder is a slog.Handler that keeps the records at Warn and above,
// as their message and attributes, and prints nothing.
type logRecorder struct {
	mu    sync.Mutex
	lines []string
}

func (r *logRecorder) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelWarn }

func (r *logRecorder) Handle(_ context.Context, rec slog.Record) error {
	line := rec.Message
	rec.Attrs(func(a slog.Attr) bool {
		line += " " + a.String()
		return true
	})
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines = append(r.lines, line)
	return nil
}

func (r *logRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *logRecorder) WithGroup(string) slog.Handler      { return r }

// logged returns the kept records that contain substr.
func (r *logRecorder) logged(substr string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var got []string
	for _, l := range r.lines {
		if strings.Contains(l, substr) {
			got = append(got, l)
		}
	}
	return got
}

// Timings of the test interpreter and executor.
const (
	testFactTimeout    = 2 * time.Second
	testClaimTimeout   = 5 * time.Minute
	testActionDeadline = time.Minute
	testStuckRunAfter  = 2 * testClaimTimeout
)

// testRetention is the retention of the test interpreter: the defaults of
// flowkit.RetentionConfig.
var testRetention = Retention{
	StepDetail: 90 * 24 * time.Hour,
	Run:        2 * 365 * 24 * time.Hour,
	Event:      30 * 24 * time.Hour,
	DeadLetter: 90 * 24 * time.Hour,
}

func newEnv(t *testing.T) *env {
	t.Helper()
	pool, schema := testpg.New(t)
	ctx := context.Background()
	if err := migrate.Run(ctx, pool, schema, migrate.Options{}); err != nil {
		t.Fatalf("migrate %s: %v", schema, err)
	}
	st, err := store.NewPgStore(pool, schema)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{
		t: t, pool: pool, store: st, clock: &fakeClock{now: t0},
		pumps: &fakePumps{}, timers: &fakeTimers{}, planner: &testPlanner{}, meter: &outcomeMeter{},
		events: newFakeEvents(t), logs: &logRecorder{},
	}
	logger := slog.New(e.logs)
	exec, err := executor.New(executor.Options{
		Store: st, Pumps: e.pumps, Clock: e.clock, ClaimTimeout: testClaimTimeout, ActionDeadline: testActionDeadline,
		MaxOutputBytes: 1 << 20, IsNotPerformed: func(error) bool { return false }, RedactError: redact,
		Rand: func() float64 { return 0.5 }, Logger: logger, IsIdempotent: e.planner.idempotent,
	})
	if err != nil {
		t.Fatal(err)
	}
	e.in, err = New(Options{
		Store: st, Pumps: e.pumps, Timers: e.timers, Events: e.events, Clock: e.clock, Planner: e.planner,
		Executor: exec, FactTimeout: testFactTimeout, LibVersion: "v0.0.0-test",
		RedactError: redact, Meter: e.meter, Logger: logger, Routes: e.pumps,
		ClaimTimeout: testClaimTimeout, StuckRunAfter: testStuckRunAfter, Retention: testRetention,
	})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func (e *env) inTx(fn func(store.Tx)) {
	e.t.Helper()
	ctx := context.Background()
	tx, err := e.store.Begin(ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	fn(tx)
	if err := tx.Commit(ctx); err != nil {
		e.t.Fatal(err)
	}
}

// defSpec is a published definition triggered by testEvent.
type defSpec struct {
	name      string
	policy    string
	serialKey string // CEL
	window    int    // seconds
	disabled  bool
	// steps is the JSON array of the body's steps; empty means one end
	// step named after the version, "v<n>".
	steps string
}

// publish stores the definition at version 1, or at the next version when
// it exists.
func (e *env) publish(d defSpec) int {
	e.t.Helper()
	ctx := context.Background()
	version := 1
	e.inTx(func(tx store.Tx) {
		trigger := testEvent
		policy := d.policy
		if policy == "" {
			policy = model.PolicyConcurrent
		}
		row := store.DefinitionRow{
			Name: d.name, Enabled: !d.disabled, TriggerEventType: &trigger, ConcurrencyPolicy: policy,
			CreatedAt: e.clock.Now(), UpdatedAt: e.clock.Now(),
		}
		if d.serialKey != "" {
			row.SerialKeyCEL = &d.serialKey
		}
		if d.window > 0 {
			row.DedupWindowSeconds = &d.window
		}
		inserted, err := e.store.InsertDefinitionIfAbsent(ctx, tx, row)
		if err != nil {
			e.t.Fatal(err)
		}
		if !inserted {
			cur, err := e.store.LockDefinition(ctx, tx, d.name)
			if err != nil {
				e.t.Fatal(err)
			}
			version = *cur.CurrentVersion + 1
		}
		steps := d.steps
		if steps == "" {
			steps = fmt.Sprintf(`[{"id":"done","type":"end","name":"v%d"}]`, version)
		}
		body := fmt.Sprintf(`{"metamodel_version":1,"trigger":{"event":%q},"steps":%s}`, testEvent, steps)
		if err := e.store.InsertDefinitionVersion(ctx, tx, store.DefinitionVersionRow{
			Name: d.name, Version: version, Body: json.RawMessage(body), MetamodelVersion: 1,
			Checksum: fmt.Sprintf("c%d", version), PublishedAt: e.clock.Now(),
		}); err != nil {
			e.t.Fatal(err)
		}
		row.CurrentVersion = &version
		if err := e.store.UpdateDefinitionPublished(ctx, tx, row); err != nil {
			e.t.Fatal(err)
		}
	})
	return version
}

func (e *env) setEnabled(name string, enabled bool) {
	e.t.Helper()
	e.inTx(func(tx store.Tx) {
		if err := e.store.SetDefinitionEnabled(context.Background(), tx, name, enabled, e.clock.Now()); err != nil {
			e.t.Fatal(err)
		}
	})
}

// emit stores an event of type typ with payload and returns its id.
func (e *env) emitType(typ, payload string) int64 {
	e.t.Helper()
	var id int64
	e.inTx(func(tx store.Tx) {
		var err error
		id, _, err = e.store.InsertEvent(context.Background(), tx, store.EventRow{
			Type: typ, Payload: json.RawMessage(payload), OccurredAt: e.clock.Now(), InsertedAt: e.clock.Now(),
		})
		if err != nil {
			e.t.Fatal(err)
		}
	})
	return id
}

func (e *env) emit(key string) int64 {
	e.t.Helper()
	return e.emitType(testEvent, fmt.Sprintf(`{"key":%q}`, key))
}

func (e *env) route(eventID int64) {
	e.t.Helper()
	if err := e.in.RouteEvent(context.Background(), eventID); err != nil {
		e.t.Fatalf("route event %d: %v", eventID, err)
	}
}

func (e *env) run(id ids.UUID) store.RunRow {
	e.t.Helper()
	var r store.RunRow
	e.inTx(func(tx store.Tx) {
		var err error
		if r, err = e.store.GetRun(context.Background(), tx, id); err != nil {
			e.t.Fatalf("get run %s: %v", id, err)
		}
	})
	return r
}

func (e *env) runExists(id ids.UUID) bool {
	e.t.Helper()
	var ok bool
	e.inTx(func(tx store.Tx) {
		var err error
		if ok, err = e.store.RunExists(context.Background(), tx, id); err != nil {
			e.t.Fatal(err)
		}
	})
	return ok
}

// finish completes a run in its own transaction.
func (e *env) finish(id ids.UUID) {
	e.t.Helper()
	err := e.in.inTx(context.Background(), func(tx store.Tx) error {
		r, err := e.store.GetRun(context.Background(), tx, id)
		if err != nil {
			return err
		}
		return e.in.FinishRun(context.Background(), tx, r, Finish{State: stateCompleted, Name: "done", StepID: "done"})
	})
	if err != nil {
		e.t.Fatalf("finish run %s: %v", id, err)
	}
}

// count runs a count(*) query with the schema token replaced.
func (e *env) count(query string, args ...any) int {
	e.t.Helper()
	var n int
	if err := e.pool.QueryRow(context.Background(), e.sql(query), args...).Scan(&n); err != nil {
		e.t.Fatalf("%s: %v", query, err)
	}
	return n
}

// sql qualifies the tables of query, written with a {s} prefix.
func (e *env) sql(query string) string {
	return strings.ReplaceAll(query, "{s}", `"`+e.store.Schema()+`".`)
}

var errInjected = errors.New("injected failure")

// start routes a new trigger event of key to the definitions and returns
// the run it started for def.
func (e *env) start(def, key string) ids.UUID {
	e.t.Helper()
	return e.startWith(def, fmt.Sprintf(`{"key":%q}`, key))
}

// startWith is start with a trigger payload.
func (e *env) startWith(def, payload string) ids.UUID {
	e.t.Helper()
	eventID := e.emitType(testEvent, payload)
	e.route(eventID)
	return RunID(def, eventID)
}

// steps joins step objects into the steps array of a body.
func steps(objs ...string) string { return "[" + strings.Join(objs, ",") + "]" }

// endStep is an end step with id and name.
func endStep(id, name string) string {
	return fmt.Sprintf(`{"id":%q,"type":"end","name":%q}`, id, name)
}

// waitStep waits an hour for testWaitEvent; match is a CEL source or "".
func waitStep(id, match, next, onTimeout string) string {
	m := ""
	if match != "" {
		m = fmt.Sprintf(`"match":%q,`, match)
	}
	return fmt.Sprintf(`{"id":%q,"type":"wait_for_event","event":%q,%s"timeout":"1h","on_timeout":%q,"next":%q}`,
		id, testWaitEvent, m, onTimeout, next)
}

// emitWait stores a testWaitEvent of key and returns its id.
func (e *env) emitWait(key string) int64 {
	e.t.Helper()
	return e.emitType(testWaitEvent, fmt.Sprintf(`{"key":%q}`, key))
}

// terminal returns the run's terminal step record.
func (e *env) terminal(id ids.UUID) store.StepRow {
	e.t.Helper()
	for _, s := range e.steps(id) {
		if s.Kind == stepTerminal {
			return s
		}
	}
	e.t.Fatalf("run %s has no terminal step; steps %v", id, e.kinds(id))
	return store.StepRow{}
}

// pump runs one pump of the run and returns the job's result.
func (e *env) pump(id ids.UUID) string {
	e.t.Helper()
	return e.in.Pump(context.Background(), sched.PumpRunArgs{RunID: id}).String()
}

// mustPump pumps the run and fails the test unless the job is done.
func (e *env) mustPump(id ids.UUID) {
	e.t.Helper()
	if res := e.pump(id); res != "done" {
		e.t.Fatalf("pump run %s: %s", id, res)
	}
}

// timeout runs the wait timeout job args.
func (e *env) timeout(args sched.WaitTimeoutArgs) string {
	e.t.Helper()
	return e.in.Timeout(context.Background(), args).String()
}

// steps returns the run's step records in order.
func (e *env) steps(id ids.UUID) []store.StepRow {
	e.t.Helper()
	var out []store.StepRow
	e.inTx(func(tx store.Tx) {
		var err error
		if out, err = e.store.ListSteps(context.Background(), tx, id); err != nil {
			e.t.Fatal(err)
		}
	})
	return out
}

// kinds returns "step_id:kind" of the run's step records in order.
func (e *env) kinds(id ids.UUID) []string {
	e.t.Helper()
	var out []string
	for _, s := range e.steps(id) {
		out = append(out, s.StepID+":"+s.Kind)
	}
	return out
}

// countKind counts the run's step records of kind.
func (e *env) countKind(id ids.UUID, kind string) int {
	e.t.Helper()
	n := 0
	for _, s := range e.steps(id) {
		if s.Kind == kind {
			n++
		}
	}
	return n
}

// at returns the step the run's frame is at, and whether it is entered.
func (e *env) at(id ids.UUID) (string, bool) {
	e.t.Helper()
	f, err := decodeFrames(e.run(id).Frames)
	if err != nil {
		e.t.Fatal(err)
	}
	fr, ok := f.top()
	if !ok {
		return "", false
	}
	return fr.Step, fr.Entered
}

// holdRun locks the run's row in a transaction of its own and returns the
// function that releases it.
func (e *env) holdRun(id ids.UUID) func() {
	e.t.Helper()
	ctx := context.Background()
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, e.sql(`SELECT 1 FROM {s}runs WHERE id = $1 FOR UPDATE`), id); err != nil {
		_ = tx.Rollback(ctx)
		e.t.Fatal(err)
	}
	return func() {
		if err := tx.Rollback(ctx); err != nil {
			e.t.Errorf("release run %s: %v", id, err)
		}
	}
}

// awaitLockWaiters waits until n sessions running statements on the test
// schema wait for a lock, so a test can order the transactions it races.
func (e *env) awaitLockWaiters(n int) {
	e.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		waiting := e.count(`SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock' AND position($1 in query) > 0`,
			e.store.Schema())
		if waiting >= n {
			return
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("%d sessions wait for a lock after 10s, want %d", waiting, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// holdSerial locks the existing serial lock row of def and key in a
// transaction of its own and returns the function that releases it. Unlike
// a second LockSerial, which would make every waiter wait for the holder's
// transaction and then race, a row lock queues the waiters in order.
func (e *env) holdSerial(def, key string) func() {
	e.t.Helper()
	ctx := context.Background()
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	tag, err := tx.Exec(ctx, e.sql(`SELECT 1 FROM {s}serial_locks WHERE def_name = $1 AND serial_key = $2 FOR UPDATE`), def, key)
	if err != nil || tag.RowsAffected() != 1 {
		_ = tx.Rollback(ctx)
		e.t.Fatalf("lock serial row %s/%s: %d rows, %v", def, key, tag.RowsAffected(), err)
	}
	return func() {
		if err := tx.Rollback(ctx); err != nil {
			e.t.Errorf("release serial lock %s/%s: %v", def, key, err)
		}
	}
}
