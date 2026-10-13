package interpreter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/g0-up/flowkit/internal/errtext"
	"github.com/g0-up/flowkit/internal/executor"
	"github.com/g0-up/flowkit/internal/expr"
	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/sched"
	"github.com/g0-up/flowkit/internal/store"
)

// Run states.
const (
	stateRunning   = "running"
	stateWaiting   = "waiting"
	stateCompleted = "completed"
	stateFailed    = "failed"
	stateCancelled = "cancelled"
)

// Reserved step ids. User step ids cannot start with '$'.
const (
	// stepTrigger is the consumed_events step of a run's trigger event.
	stepTrigger = "$trigger"
	// StepRun is the step id of records about the run as a whole, such as
	// a terminal state not reached at a step.
	StepRun = "$run"
)

// TerminalCancelledByNewer is the terminal name of a run cancelled by a
// newer trigger under the cancel_previous policy.
const TerminalCancelledByNewer = "$cancelled_by_newer"

// Terminal names of runs that failed outside an end step.
const (
	// TerminalActionFailed: an action failed for good, or its output did
	// not match its OutputFacts.
	TerminalActionFailed = "$action_failed"
	// TerminalFailed: the run could not go on for another reason, such as a
	// fact resolver error, invalid resolved params or an expression error.
	TerminalFailed = "$failed"
)

// Dead letter kinds.
const (
	deadLetterUnroutable     = "unroutable_event"
	deadLetterSchemaMismatch = "schema_mismatch"
	deadLetterExhausted      = "exhausted"
	// deadLetterInDoubt is the executor's dead letter of an effect in
	// doubt, which ResolveDeadLetter resolves.
	deadLetterInDoubt = "effect_in_doubt"
)

// Default bounds of one look-back.
const (
	lookbackPageSize = 1000
	lookbackMaxRows  = 10000
)

// subscriptionPageSize is how many open waits the route of an event reads
// per transaction.
const subscriptionPageSize = 1000

// maxSerialKeyBytes caps a serial key, as maxDedupKeyBytes caps a dedup key.
const maxSerialKeyBytes = 512

// ErrStale reports that a run changed between reading it and writing it;
// the caller must roll its transaction back.
var ErrStale = errors.New("flowkit interpreter: run changed concurrently")

// PumpEnqueuer enqueues a pump of a run in tx. *sched.Scheduler implements
// it.
type PumpEnqueuer interface {
	EnqueuePump(ctx context.Context, tx store.Tx, runID ids.UUID) error
}

// Clock is the flowkit clock. Every time the interpreter writes comes from
// it.
type Clock interface {
	Now() time.Time
}

// Events checks stored events against the event types registered now.
type Events interface {
	// Check validates a stored payload against the registered type and
	// derives the run scope from it. An error means the type is not
	// registered or the payload does not fit it any more.
	Check(eventType string, payload json.RawMessage) (EventInfo, error)
}

// Executor runs the effect of an action step. *executor.Executor
// implements it.
type Executor interface {
	Run(ctx context.Context, req executor.ExecRequest) executor.ExecOutcome
	// CheckOutput checks an output given for action a as its own output
	// would be checked and returns the JSON to store.
	CheckOutput(a executor.Action, out map[string]any) (json.RawMessage, error)
	// ResolveExpiredClaims resolves up to limit effects whose claim
	// expired by now and returns how many it resolved.
	ResolveExpiredClaims(ctx context.Context, now time.Time, limit int) (int, error)
}

// RouteEnqueuer enqueues the routing of a stored event in tx.
// *sched.Scheduler implements it.
type RouteEnqueuer interface {
	EnqueueRoute(ctx context.Context, tx store.Tx, eventID int64) error
}

// Timers schedules the timeout of a wait in tx. *sched.Scheduler
// implements it.
type Timers interface {
	ScheduleTimeout(ctx context.Context, tx store.Tx, args sched.WaitTimeoutArgs) error
}

// EventInfo is what a checked event gives a route.
type EventInfo struct {
	// Shape types the payload for expressions; nil when the payload schema
	// cannot type expressions.
	Shape *expr.Shape
	Scope expr.Scope
}

// Options configures an Interpreter.
type Options struct {
	Store    store.Store
	Pumps    PumpEnqueuer
	Routes   RouteEnqueuer
	Timers   Timers
	Events   Events
	Clock    Clock
	Planner  Planner
	Executor Executor
	// LookbackMargin widens the look-back of a wait to events stored this
	// long before the run was created.
	LookbackMargin time.Duration
	// FactTimeout bounds the resolution of a pump's facts.
	FactTimeout time.Duration
	// LibVersion is recorded on every run the interpreter starts.
	LibVersion string
	// RedactError renders every error the interpreter stores.
	RedactError func(error) string
	// ClaimTimeout is the executor's claim timeout. The janitor resumes a
	// look-back unfinished for this long.
	ClaimTimeout time.Duration
	// StuckRunAfter is how long a running run goes unwritten before the
	// janitor pumps it again; it must exceed ClaimTimeout.
	StuckRunAfter time.Duration
	// Retention is how long the janitor keeps data.
	Retention Retention
	// InstanceID names this instance in spans.
	InstanceID string
	Logger     *slog.Logger // nil discards
	Meter      metric.Meter // nil selects a no-op meter
	Tracer     trace.Tracer // nil selects a no-op tracer
}

// Retention is how long the janitor keeps each kind of data. See
// flowkit.RetentionConfig.
type Retention struct {
	StepDetail time.Duration
	Run        time.Duration
	Event      time.Duration
	DeadLetter time.Duration
}

func (o Options) validate() error {
	switch {
	case o.Store == nil:
		return errors.New("flowkit interpreter: nil store")
	case o.Pumps == nil:
		return errors.New("flowkit interpreter: nil pump enqueuer")
	case o.Routes == nil:
		return errors.New("flowkit interpreter: nil route enqueuer")
	case o.Timers == nil:
		return errors.New("flowkit interpreter: nil timers")
	case o.Planner == nil:
		return errors.New("flowkit interpreter: nil planner")
	case o.Executor == nil:
		return errors.New("flowkit interpreter: nil executor")
	case o.LookbackMargin < 0:
		return fmt.Errorf("flowkit interpreter: look-back margin %s must not be negative", o.LookbackMargin)
	case o.FactTimeout <= 0:
		return fmt.Errorf("flowkit interpreter: fact timeout %s must be positive", o.FactTimeout)
	case o.Events == nil:
		return errors.New("flowkit interpreter: nil events")
	case o.Clock == nil:
		return errors.New("flowkit interpreter: nil clock")
	case o.RedactError == nil:
		return errors.New("flowkit interpreter: nil RedactError")
	case o.LibVersion == "":
		return errors.New("flowkit interpreter: empty lib version")
	case o.ClaimTimeout <= 0:
		return fmt.Errorf("flowkit interpreter: claim timeout %s must be positive", o.ClaimTimeout)
	case o.StuckRunAfter <= o.ClaimTimeout:
		return fmt.Errorf("flowkit interpreter: stuck-run threshold %s must exceed the claim timeout %s",
			o.StuckRunAfter, o.ClaimTimeout)
	case o.Retention.StepDetail <= 0 || o.Retention.Run <= 0 || o.Retention.Event <= 0 || o.Retention.DeadLetter <= 0:
		return fmt.Errorf("flowkit interpreter: retention %+v must be positive", o.Retention)
	}
	return nil
}

// Interpreter routes events to runs and moves runs between states.
type Interpreter struct {
	store          store.Store
	pumps          PumpEnqueuer
	routeJobs      RouteEnqueuer
	timers         Timers
	events         Events
	clock          Clock
	planner        Planner
	exec           Executor
	lookbackMargin time.Duration
	factTimeout    time.Duration
	libVersion     string
	redact         func(error) string
	claimTimeout   time.Duration
	stuckRunAfter  time.Duration
	retention      Retention
	instanceID     string
	log            *slog.Logger
	tracer         trace.Tracer
	routes         metric.Int64Counter
	matchErrors    metric.Int64Counter
	truncated      metric.Int64Counter
	runsStarted    metric.Int64Counter
	runsTerminal   metric.Int64Counter
	deadLetters    metric.Int64Counter
	wakeLatency    metric.Float64Histogram
	timerLag       metric.Float64Histogram

	// commitFns holds, for each transaction inTx has open, the functions
	// to call once it commits.
	commitMu  sync.Mutex
	commitFns map[store.Tx][]func()

	// Look-back bounds and the subscription page size; tests lower them.
	pageSize, maxRows int
	subsPage          int

	keysMu sync.Mutex
	keys   map[string]serialKeyProgram // by definition name

	plansMu sync.Mutex
	plans   map[planKey]*Plan

	// hooks are nil outside tests.
	hooks hooks
}

// hooks let tests stop a pump at the points where a process can die, or
// hold it there to interleave it with another.
type hooks struct {
	// afterRead runs after a pump read the run it walks.
	afterRead func(runID ids.UUID)
	// afterEnter runs after an action's entry committed; an error ends the
	// pump there.
	afterEnter func(runID ids.UUID) error
	// afterWait runs after a wait's entry committed, before its look-back;
	// an error ends the pump there.
	afterWait func(runID ids.UUID) error
}

// New returns an Interpreter.
func New(o Options) (*Interpreter, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}
	log := o.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	meter := o.Meter
	if meter == nil {
		meter = noop.NewMeterProvider().Meter("flowkit")
	}
	routes, err := meter.Int64Counter("flowkit.routes",
		metric.WithDescription("Routing decisions per definition and trigger event, by outcome."))
	if err != nil {
		return nil, err
	}
	matchErrors, err := meter.Int64Counter("flowkit.match_errors",
		metric.WithDescription("Wait match expressions that failed to evaluate, per definition."))
	if err != nil {
		return nil, err
	}
	truncated, err := meter.Int64Counter("flowkit.lookback_truncated",
		metric.WithDescription("Wait look-backs that stopped at their row limit, per definition."))
	if err != nil {
		return nil, err
	}
	runsStarted, err := meter.Int64Counter("flowkit.runs_started",
		metric.WithDescription("Runs started."))
	if err != nil {
		return nil, err
	}
	runsTerminal, err := meter.Int64Counter("flowkit.runs_terminal",
		metric.WithDescription("Runs that reached a terminal state, by state."))
	if err != nil {
		return nil, err
	}
	deadLetters, err := meter.Int64Counter("flowkit.dead_letters",
		metric.WithDescription("Dead letters written"))
	if err != nil {
		return nil, err
	}
	wakeLatency, err := meter.Float64Histogram("flowkit.wake_latency", metric.WithUnit("s"),
		metric.WithDescription("Time from storing an event to resuming the wait it satisfies."))
	if err != nil {
		return nil, err
	}
	timerLag, err := meter.Float64Histogram("flowkit.timer_lag", metric.WithUnit("s"),
		metric.WithDescription("Time from a wait timeout's due time to its handling."))
	if err != nil {
		return nil, err
	}
	tracer := o.Tracer
	if tracer == nil {
		tracer = tracenoop.NewTracerProvider().Tracer("flowkit")
	}
	return &Interpreter{
		store:          o.Store,
		pumps:          o.Pumps,
		routeJobs:      o.Routes,
		timers:         o.Timers,
		events:         o.Events,
		clock:          o.Clock,
		planner:        o.Planner,
		exec:           o.Executor,
		lookbackMargin: o.LookbackMargin,
		factTimeout:    o.FactTimeout,
		libVersion:     o.LibVersion,
		redact:         o.RedactError,
		claimTimeout:   o.ClaimTimeout,
		stuckRunAfter:  o.StuckRunAfter,
		retention:      o.Retention,
		instanceID:     o.InstanceID,
		log:            log,
		tracer:         tracer,
		routes:         routes,
		matchErrors:    matchErrors,
		truncated:      truncated,
		runsStarted:    runsStarted,
		runsTerminal:   runsTerminal,
		deadLetters:    deadLetters,
		wakeLatency:    wakeLatency,
		timerLag:       timerLag,
		commitFns:      map[store.Tx][]func(){},
		pageSize:       lookbackPageSize,
		maxRows:        lookbackMaxRows,
		subsPage:       subscriptionPageSize,
		keys:           map[string]serialKeyProgram{},
		plans:          map[planKey]*Plan{},
	}, nil
}

// inTx runs fn in a fresh transaction and commits it when fn succeeds.
// Then it calls the functions fn registered with onCommit.
func (in *Interpreter) inTx(ctx context.Context, fn func(tx store.Tx) error) error {
	tx, err := in.store.Begin(ctx)
	if err != nil {
		return err
	}
	in.commitMu.Lock()
	in.commitFns[tx] = nil
	in.commitMu.Unlock()
	if err = fn(tx); err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
	} else {
		err = tx.Commit(ctx)
	}
	in.commitMu.Lock()
	fns := in.commitFns[tx]
	delete(in.commitFns, tx)
	in.commitMu.Unlock()
	if err != nil {
		return err
	}
	for _, f := range fns {
		f()
	}
	return nil
}

// onCommit calls f once tx commits, so a metric never counts a write that
// rolled back. In a transaction inTx did not begin, which the interpreter
// cannot see commit, f runs at once.
func (in *Interpreter) onCommit(tx store.Tx, f func()) {
	in.commitMu.Lock()
	fns, ok := in.commitFns[tx]
	if ok {
		in.commitFns[tx] = append(fns, f)
	}
	in.commitMu.Unlock()
	if !ok {
		f()
	}
}

// countDeadLetter counts a dead letter of kind once tx commits.
func (in *Interpreter) countDeadLetter(ctx context.Context, tx store.Tx, kind string) {
	in.onCommit(tx, func() {
		in.deadLetters.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(attribute.String("kind", kind))))
	})
}

// noteText is the stored form of an error: redacted, then made storable by
// errtext.Sanitize.
func (in *Interpreter) noteText(err error) string {
	return errtext.Sanitize(in.redact(err))
}
