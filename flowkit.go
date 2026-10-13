package flowkit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/g0-up/flowkit/internal/executor"
	"github.com/g0-up/flowkit/internal/expr"
	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/interpreter"
	"github.com/g0-up/flowkit/internal/migrate"
	"github.com/g0-up/flowkit/internal/sched"
	"github.com/g0-up/flowkit/internal/schema"
	"github.com/g0-up/flowkit/internal/store"
)

// Engine runs workflows for one schema. Create it with New, call Start to
// process jobs and Close to stop. Emit works whether or not the Engine is
// started. An Engine is safe for concurrent use.
type Engine struct {
	cfg   Config
	reg   *compiledRegistry
	store engineStore
	// pg and client are nil on an Engine the simulator builds in memory.
	pg      *store.PgStore
	client  *sched.Client
	jobs    jobScheduler
	interp  *interpreter.Interpreter
	metrics *engineMetrics
}

// engineStore is the persistence an Engine uses: *store.PgStore, or the
// simulator's in-memory store.
type engineStore interface {
	store.Store
	executor.Store
}

// jobScheduler enqueues jobs in a store transaction: *sched.Scheduler, or
// the simulator's in-memory scheduler.
type jobScheduler interface {
	EnqueueRoute(ctx context.Context, tx store.Tx, eventID int64) error
	EnqueuePump(ctx context.Context, tx store.Tx, runID ids.UUID) error
	EnqueuePumpMany(ctx context.Context, tx store.Tx, runIDs []ids.UUID) error
	ScheduleTimeout(ctx context.Context, tx store.Tx, args sched.WaitTimeoutArgs) error
}

// schemaCheckTimeout bounds the database checks New makes.
const schemaCheckTimeout = 30 * time.Second

// New validates cfg and reg and returns an Engine that is not started.
//
// It applies the Config defaults and rejects an invalid configuration,
// including a ClaimTimeout that breaks the timing chain. It copies reg, so
// later registrations do not reach the Engine, and checks it as a whole:
// every schema compiles, every FactDef.AvailableOn pattern matches a
// registered event, and no OutputFact reuses a FactDef name.
//
// New then checks, without changing anything, that the binary links a
// supported River version and that Config.Schema is migrated for this build
// and holds only the objects flowkit and River create. A mismatch returns an
// error matching ErrSchemaDrift; New never migrates, so run `flowkit migrate`
// (or migrate.Run) first.
func New(cfg Config, reg *Registry) (*Engine, error) {
	if reg == nil {
		return nil, errors.New("flowkit: New: nil Registry")
	}
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	compiled, err := reg.clone().compile()
	if err != nil {
		return nil, err
	}
	st, err := store.NewPgStore(cfg.Pool, cfg.Schema)
	if err != nil {
		return nil, fmt.Errorf("flowkit: New: %w", err)
	}
	if err := checkSchema(cfg); err != nil {
		return nil, err
	}
	handlers := &jobHandlers{}
	client, err := sched.New(sched.Options{
		Pool:         cfg.Pool,
		Store:        st,
		InstanceID:   cfg.InstanceID,
		Worker:       cfg.hasRole(RoleWorker),
		MaxWorkers:   cfg.Workers.MaxWorkers,
		Janitor:      cfg.hasRole(RoleJanitor),
		PollInterval: cfg.Workers.PollInterval,
		PollOnly:     cfg.Workers.PollOnly,
		Timing:       engineTiming(cfg.ClaimTimeout),
		Logger:       cfg.Logger,
		Clock:        cfg.Clock,
	}, handlers)
	if err != nil {
		return nil, fmt.Errorf("flowkit: New: %w", err)
	}
	// The executor and the interpreter enqueue through the client's
	// scheduler, so they are built after the client; no job runs before
	// Start.
	e, err := newEngine(cfg, compiled, st, client.Scheduler(), handlers)
	if err != nil {
		return nil, err
	}
	e.pg, e.client = st, client
	return e, nil
}

// newEngine builds the executor, the interpreter and the metrics of an
// Engine on st, enqueuing through jobs, and points handlers at the
// interpreter. cfg is defaulted and compiled checked.
func newEngine(cfg Config, compiled *compiledRegistry, st engineStore, jobs jobScheduler, handlers *jobHandlers) (*Engine, error) {
	plans, err := newPlanner(compiled)
	if err != nil {
		return nil, err
	}
	timing := engineTiming(cfg.ClaimTimeout)
	meter := cfg.Meter.Meter(modulePath)
	exec, err := executor.New(executor.Options{
		Store:          st,
		Pumps:          jobs,
		Clock:          cfg.Clock,
		ClaimTimeout:   cfg.ClaimTimeout,
		ActionDeadline: timing.ActionDeadline,
		MaxOutputBytes: cfg.MaxPayloadBytes,
		RateLimit:      effectRateLimit(cfg.Workers.EffectRateLimit),
		IsNotPerformed: isNotPerformed,
		RedactError:    cfg.RedactError,
		IsIdempotent: func(action string) bool {
			a, ok := compiled.reg.actions[action]
			return ok && a.Idempotent
		},
		Logger: cfg.Logger,
		Meter:  meter,
	})
	if err != nil {
		return nil, fmt.Errorf("flowkit: New: %w", err)
	}
	handlers.interp, err = interpreter.New(interpreter.Options{
		Store:          st,
		Pumps:          jobs,
		Routes:         jobs,
		Timers:         jobs,
		Events:         newEventChecker(compiled),
		Clock:          cfg.Clock,
		Planner:        plans,
		Executor:       exec,
		LookbackMargin: cfg.LookbackMargin,
		FactTimeout:    cfg.FactTimeout,
		ClaimTimeout:   cfg.ClaimTimeout,
		StuckRunAfter:  timing.StuckRunAfter,
		Retention: interpreter.Retention{
			StepDetail: cfg.Retention.StepDetail,
			Run:        cfg.Retention.Run,
			Event:      cfg.Retention.Event,
			DeadLetter: cfg.Retention.DeadLetter,
		},
		InstanceID:  cfg.InstanceID,
		LibVersion:  libVersion(),
		RedactError: cfg.RedactError,
		Logger:      cfg.Logger,
		Meter:       meter,
		Tracer:      cfg.Tracer.Tracer(modulePath),
	})
	if err != nil {
		return nil, fmt.Errorf("flowkit: New: %w", err)
	}
	e := &Engine{cfg: cfg, reg: compiled, store: st, jobs: jobs, interp: handlers.interp}
	if e.metrics, err = newEngineMetrics(meter, e); err != nil {
		return nil, fmt.Errorf("flowkit: New: %w", err)
	}
	return e, nil
}

// modulePath is flowkit's module path: its OpenTelemetry instrumentation
// scope and the module whose version runs record.
const modulePath = "github.com/g0-up/flowkit"

// libVersion is the flowkit module version linked into the binary, or
// "(devel)" when the build does not record it.
func libVersion() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "(devel)"
	}
	mod := &bi.Main
	if mod.Path != modulePath {
		mod = nil
		for _, d := range bi.Deps {
			if d.Path == modulePath {
				mod = d
				break
			}
		}
	}
	if mod == nil {
		return "(devel)"
	}
	if mod.Replace != nil {
		mod = mod.Replace
	}
	if mod.Version == "" {
		return "(devel)"
	}
	return mod.Version
}

// effectRateLimit is the executor's per-definition rate limit: rate
// executions per second, with a burst of one second's worth, at least one.
func effectRateLimit(rate float64) executor.RateLimit {
	if rate <= 0 {
		return executor.RateLimit{}
	}
	return executor.RateLimit{Rate: rate, Burst: max(1, rate)}
}

// jobHandlers hands route, pump, wait timeout and janitor jobs to the
// interpreter.
type jobHandlers struct {
	interp *interpreter.Interpreter
}

func (h *jobHandlers) Route(ctx context.Context, args sched.RouteEventArgs) sched.Result {
	return h.interp.Route(ctx, args)
}

func (h *jobHandlers) Pump(ctx context.Context, args sched.PumpRunArgs) sched.Result {
	return h.interp.Pump(ctx, args)
}

func (h *jobHandlers) Timeout(ctx context.Context, args sched.WaitTimeoutArgs) sched.Result {
	return h.interp.Timeout(ctx, args)
}

func (h *jobHandlers) Janitor(ctx context.Context, args sched.JanitorArgs) sched.Result {
	return h.interp.Janitor(ctx, args)
}

// eventChecker revalidates stored events against the registered types for
// the interpreter.
type eventChecker struct {
	reg    *compiledRegistry
	shapes map[string]*expr.Shape // nil for a schema that cannot type expressions
}

func newEventChecker(reg *compiledRegistry) *eventChecker {
	c := &eventChecker{reg: reg, shapes: make(map[string]*expr.Shape, len(reg.reg.events))}
	for name, def := range reg.reg.events {
		if s, err := expr.NewShape(def.PayloadSchema); err == nil {
			c.shapes[name] = s
		}
	}
	return c
}

// Check applies the payload schema and the scope fields of the event's
// registered type, as Emit did, to a stored payload.
func (c *eventChecker) Check(eventType string, payload json.RawMessage) (interpreter.EventInfo, error) {
	def, ok := c.reg.reg.events[eventType]
	if !ok {
		return interpreter.EventInfo{}, fmt.Errorf("%w %q", ErrUnknownEvent, eventType)
	}
	doc, err := schema.Decode(payload)
	if err != nil {
		return interpreter.EventInfo{}, fmt.Errorf("event %q: payload cannot be decoded: %w", eventType, err)
	}
	var verrs ValidationErrors
	for _, v := range c.reg.payloads[eventType].Validate(doc) {
		verrs = append(verrs, &ValidationError{Path: v.Path, Msg: v.Msg})
	}
	if len(verrs) > 0 {
		return interpreter.EventInfo{}, fmt.Errorf("event %q: payload does not match PayloadSchema: %w", eventType, verrs)
	}
	fields, _ := doc.(map[string]any)
	info := interpreter.EventInfo{Shape: c.shapes[eventType]}
	if def.EntityKind != "" {
		id, ok, err := scalarField(fields, def.EntityIDField)
		switch {
		case err != nil:
			return interpreter.EventInfo{}, fmt.Errorf("event %q: entity id %v", eventType, err)
		case !ok:
			return interpreter.EventInfo{}, fmt.Errorf("event %q: entity id of %s is missing or empty", eventType, def.EntityKind)
		}
		info.Scope.EntityKind, info.Scope.EntityID = def.EntityKind, id
	}
	for key, field := range def.ScopeFields {
		v, ok, err := scalarField(fields, field)
		if err != nil {
			return interpreter.EventInfo{}, fmt.Errorf("event %q: scope field %q %v", eventType, key, err)
		}
		if ok {
			if info.Scope.Extra == nil {
				info.Scope.Extra = map[string]string{}
			}
			info.Scope.Extra[key] = v
		}
	}
	return info, nil
}

// checkSchema maps every way the binary and the schema can disagree to
// ErrSchemaDrift, keeping the underlying error in the chain.
func checkSchema(cfg Config) error {
	if err := riverModuleError(migrate.CheckRiverModule(cfg.Logger)); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), schemaCheckTimeout)
	defer cancel()
	err := migrate.Check(ctx, cfg.Pool, cfg.Schema)
	var drift *migrate.DriftError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &drift) && drift.NeedsMigrate:
		return fmt.Errorf("%w: %w; run `flowkit migrate --schema %s`", ErrSchemaDrift, err, cfg.Schema)
	case errors.As(err, &drift):
		return fmt.Errorf("%w: %w; this flowkit build cannot use the schema, `flowkit migrate` cannot repair it: upgrade flowkit or restore the schema",
			ErrSchemaDrift, err)
	default:
		return fmt.Errorf("flowkit: New: check schema %q: %w", cfg.Schema, err)
	}
}

// riverModuleError maps a River version mismatch to ErrSchemaDrift: the
// binary cannot drive the schema River created.
func riverModuleError(err error) error {
	var rv *migrate.RiverVersionError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &rv):
		return fmt.Errorf("%w: %w", ErrSchemaDrift, err)
	default:
		return fmt.Errorf("flowkit: New: %w", err)
	}
}

// Start starts processing jobs according to Config.Roles. ctx bounds the
// Engine's life: cancelling it stops the Engine and cancels running jobs.
// Use Close for a graceful stop.
func (e *Engine) Start(ctx context.Context) error {
	if e.client == nil {
		return errNoClient
	}
	if !e.cfg.hasRole(RoleJanitor) {
		e.cfg.Logger.WarnContext(ctx, "flowkit: this instance has no janitor role; at least one running instance must have RoleJanitor or wait timeouts never fire",
			"instance", e.cfg.InstanceID, "schema", e.cfg.Schema)
	}
	if err := e.client.Start(ctx); err != nil {
		return fmt.Errorf("flowkit: %w", err)
	}
	roles := make([]string, len(e.cfg.Roles))
	for i, r := range e.cfg.Roles {
		roles[i] = roleName(r)
	}
	e.cfg.Logger.InfoContext(ctx, "flowkit: engine started", "instance", e.cfg.InstanceID, "schema", e.cfg.Schema, "roles", roles)
	return nil
}

// Close stops fetching jobs and waits for running ones to finish. When ctx
// ends first, Close cancels the running jobs, waits briefly for them to
// return, and returns an error saying the graceful stop was cut short. Close
// does not close Config.Pool.
func (e *Engine) Close(ctx context.Context) error {
	// The gauges read the database; stop them before the consumer closes
	// the pool. Unregister only fails for a registration of another meter.
	_ = e.metrics.gauges.Unregister()
	if e.client == nil {
		return nil
	}
	if err := e.client.Stop(ctx); err != nil {
		return fmt.Errorf("flowkit: close: %w", err)
	}
	return nil
}

// errNoClient is the error of the operations an Engine the simulator built
// in memory cannot do: it has no database and runs its jobs through the
// simulator.
var errNoClient = errors.New("flowkit: this Engine runs in the flowkittest simulator; use the Sim instead")
