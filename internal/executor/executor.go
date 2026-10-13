package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/g0-up/flowkit/internal/errtext"
	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/store"
	"github.com/g0-up/flowkit/internal/testhooks"
)

// Store is the persistence the executor uses. *store.PgStore implements it.
type Store interface {
	Begin(ctx context.Context) (store.Tx, error)
	GetEffect(ctx context.Context, tx store.Tx, idemKey string) (store.EffectRow, error)
	InsertEffectClaim(ctx context.Context, tx store.Tx, c store.EffectClaim) (bool, error)
	ReclaimEffect(ctx context.Context, tx store.Tx, idemKey string, token ids.UUID, at time.Time, maxAttempts int) (bool, error)
	CompleteEffect(ctx context.Context, tx store.Tx, idemKey string, token ids.UUID, at time.Time, result json.RawMessage, lastError *string) (bool, error)
	FailEffect(ctx context.Context, tx store.Tx, idemKey string, token ids.UUID, lastError string) (int, bool, error)
	MarkEffectInDoubt(ctx context.Context, tx store.Tx, idemKey string, token ids.UUID, lastError string) (int, bool, error)
	ReleaseEffectClaim(ctx context.Context, tx store.Tx, idemKey string, token ids.UUID) (bool, error)
	ListExpiredClaims(ctx context.Context, tx store.Tx, before time.Time, limit int) ([]store.EffectRow, error)
	SetInDoubtEffectResult(ctx context.Context, tx store.Tx, idemKey string, result json.RawMessage) (bool, error)
	AppendInDoubtEffectNote(ctx context.Context, tx store.Tx, idemKey, note string) (bool, error)
	AppendStep(ctx context.Context, tx store.Tx, s store.StepRow) (int, error)
	InsertDeadLetter(ctx context.Context, tx store.Tx, d store.DeadLetterRow) (int64, bool, error)
	InsertRateLimit(ctx context.Context, tx store.Tx, r store.RateLimitRow) error
	LockRateLimit(ctx context.Context, tx store.Tx, defName string) (store.RateLimitRow, error)
	UpdateRateLimit(ctx context.Context, tx store.Tx, r store.RateLimitRow) error
}

// PumpEnqueuer enqueues a pump of a run in tx. *sched.Scheduler implements
// it.
type PumpEnqueuer interface {
	EnqueuePump(ctx context.Context, tx store.Tx, runID ids.UUID) error
}

// Clock is the flowkit clock. Every time the executor writes comes from it.
type Clock interface {
	Now() time.Time
}

// Step and dead-letter kinds the executor writes.
const (
	stepAttemptFailed = "attempt_failed"
	stepEffectInDoubt = "effect_in_doubt"
	deadLetterInDoubt = "effect_in_doubt"
)

const (
	// contendedRetryDelay is the base delay of a request that lost a race
	// for a claim that was free a moment earlier.
	contendedRetryDelay = time.Second
	// shortBudgetRetryDelay is the base delay of a request whose context
	// ends too soon to run the action.
	shortBudgetRetryDelay = time.Second
	// detachedWriteTimeout bounds a write that does not depend on the
	// caller's context, such as recording a successful effect.
	detachedWriteTimeout = 5 * time.Second
	// completeTries bounds the tries at recording a successful effect, which
	// pause completeRetryPause, then twice as long each time.
	completeTries      = 4
	completeRetryPause = 100 * time.Millisecond
)

// Options configures an Executor.
type Options struct {
	Store Store
	// Pumps re-enqueues runs whose expired claims ResolveExpiredClaims
	// releases. Only ResolveExpiredClaims needs it.
	Pumps PumpEnqueuer
	Clock Clock
	// ClaimTimeout is how long a claim protects an effect from other
	// requests; ActionDeadline, the context deadline of Execute, must be
	// shorter. When the caller's context has an earlier deadline, Execute
	// gets until shortly before it instead: ClaimTimeout/20, at most
	// detachedWriteTimeout, before.
	ClaimTimeout   time.Duration
	ActionDeadline time.Duration
	// MaxOutputBytes bounds the JSON encoding of an action's output.
	MaxOutputBytes int
	RateLimit      RateLimit
	// IsNotPerformed reports whether an error promises that the action had
	// no effect, which lets a non-idempotent action be retried.
	IsNotPerformed func(error) bool
	// RedactError renders every action error the executor stores or logs.
	// The executor turns invalid UTF-8 and U+0000 in its result into U+FFFD
	// and cuts it to 2 KiB, so any text can be stored.
	RedactError func(error) string
	// IsIdempotent reports whether the action with the given name is
	// idempotent; ResolveExpiredClaims needs it and treats false as not
	// idempotent.
	IsIdempotent func(action string) bool
	// Rand returns values in [0, 1) for jitter and must be safe for
	// concurrent use. Nil selects math/rand/v2.
	Rand   func() float64
	Logger *slog.Logger // nil discards
	Meter  metric.Meter // nil selects a no-op meter
}

func (o Options) validate() error {
	switch {
	case o.Store == nil:
		return errors.New("flowkit executor: nil store")
	case o.Clock == nil:
		return errors.New("flowkit executor: nil clock")
	case o.IsNotPerformed == nil:
		return errors.New("flowkit executor: nil IsNotPerformed")
	case o.RedactError == nil:
		return errors.New("flowkit executor: nil RedactError")
	case o.ClaimTimeout <= 0:
		return fmt.Errorf("flowkit executor: claim timeout %s must be positive", o.ClaimTimeout)
	case o.ActionDeadline <= 0 || o.ActionDeadline >= o.ClaimTimeout:
		return fmt.Errorf("flowkit executor: action deadline %s must be positive and below the claim timeout %s",
			o.ActionDeadline, o.ClaimTimeout)
	case o.MaxOutputBytes <= 0:
		return fmt.Errorf("flowkit executor: max output bytes %d must be positive", o.MaxOutputBytes)
	}
	return o.RateLimit.validate()
}

// Executor runs actions with claim-then-execute semantics: an effect is
// claimed in its own transaction, Execute runs outside any transaction, and
// the result is recorded only while the claim still holds.
type Executor struct {
	store          Store
	pumps          PumpEnqueuer
	clock          Clock
	claimTimeout   time.Duration
	actionDeadline time.Duration
	maxOutputBytes int
	limit          RateLimit
	isNotPerformed func(error) bool
	redact         func(error) string
	isIdempotent   func(string) bool
	rand           func() float64
	log            *slog.Logger
	// parentMargin is how long before the caller's deadline Execute must
	// end, and minActionBudget the least time worth giving Execute.
	parentMargin    time.Duration
	minActionBudget time.Duration

	duration    metric.Float64Histogram
	retries     metric.Int64Counter
	inflight    metric.Int64UpDownCounter
	orphaned    metric.Int64Counter
	rateLimited metric.Int64Counter
	deadLetters metric.Int64Counter
}

// New validates o and returns an Executor.
func New(o Options) (*Executor, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}
	e := &Executor{
		store:           o.Store,
		pumps:           o.Pumps,
		clock:           o.Clock,
		claimTimeout:    o.ClaimTimeout,
		actionDeadline:  o.ActionDeadline,
		maxOutputBytes:  o.MaxOutputBytes,
		limit:           o.RateLimit,
		isNotPerformed:  o.IsNotPerformed,
		redact:          func(err error) string { return errtext.Sanitize(o.RedactError(err)) },
		isIdempotent:    o.IsIdempotent,
		rand:            o.Rand,
		log:             o.Logger,
		parentMargin:    min(o.ClaimTimeout/20, detachedWriteTimeout),
		minActionBudget: o.ActionDeadline / 10,
	}
	if e.rand == nil {
		e.rand = rand.Float64
	}
	if e.log == nil {
		e.log = slog.New(slog.DiscardHandler)
	}
	meter := o.Meter
	if meter == nil {
		meter = noop.NewMeterProvider().Meter("flowkit")
	}
	var err error
	if e.duration, err = meter.Float64Histogram("flowkit.effect_duration", metric.WithUnit("s"),
		metric.WithDescription("Duration of action executions")); err != nil {
		return nil, fmt.Errorf("flowkit executor: %w", err)
	}
	if e.retries, err = meter.Int64Counter("flowkit.effect_retries",
		metric.WithDescription("Failed action attempts that will be retried")); err != nil {
		return nil, fmt.Errorf("flowkit executor: %w", err)
	}
	if e.inflight, err = meter.Int64UpDownCounter("flowkit.effects_inflight",
		metric.WithDescription("Actions executing in this process")); err != nil {
		return nil, fmt.Errorf("flowkit executor: %w", err)
	}
	if e.orphaned, err = meter.Int64Counter("flowkit.effect_orphaned",
		metric.WithDescription("Effects that happened after their claim was revoked")); err != nil {
		return nil, fmt.Errorf("flowkit executor: %w", err)
	}
	if e.rateLimited, err = meter.Int64Counter("flowkit.effect_rate_limited",
		metric.WithDescription("Execution requests deferred by the rate limit")); err != nil {
		return nil, fmt.Errorf("flowkit executor: %w", err)
	}
	if e.deadLetters, err = meter.Int64Counter("flowkit.dead_letters",
		metric.WithDescription("Dead letters written")); err != nil {
		return nil, fmt.Errorf("flowkit executor: %w", err)
	}
	return e, nil
}

// ExecRequest asks for one effect: the action step StepID of run RunID,
// identified by IdemKey.
type ExecRequest struct {
	RunID   ids.UUID
	StepID  string
	IdemKey string
	// DefName names the run's definition, the key of its rate limit.
	DefName string
	Action  Action
	Params  map[string]any
	Scope   Scope
}

func (r ExecRequest) validate() error {
	switch {
	case r.IdemKey == "":
		return errors.New("flowkit executor: empty idempotency key")
	case r.StepID == "":
		return errors.New("flowkit executor: empty step id")
	case r.DefName == "":
		return errors.New("flowkit executor: empty definition name")
	case r.Action.Name == "":
		return errors.New("flowkit executor: empty action name")
	case r.Action.Execute == nil:
		return fmt.Errorf("flowkit executor: action %s has no Execute", r.Action.Name)
	}
	return nil
}

// Run performs req at most once per claim and reports how it ended; see
// ExecKind. It never holds a transaction open while the action executes.
//
// Execute gets the action deadline, cut short when ctx has an earlier
// deadline so that time remains to record a failure; when too little would
// remain, Run returns RetryLater without claiming the effect.
func (e *Executor) Run(ctx context.Context, req ExecRequest) ExecOutcome {
	if err := req.validate(); err != nil {
		return ExecOutcome{Kind: Errored, Err: err}
	}
	if err := ctx.Err(); err != nil {
		return ExecOutcome{Kind: Abandoned, Err: err}
	}
	c := &call{
		ExecRequest: req,
		token:       ids.NewV4(),
		log:         e.log.With("idem_key", req.IdemKey, "run_id", req.RunID.String(), "step_id", req.StepID, "action", req.Action.Name),
		attrs: metric.WithAttributeSet(attribute.NewSet(
			attribute.String("action", req.Action.Name), attribute.String("def", req.DefName))),
	}

	// A request for a recorded or busy effect ends here, before it spends a
	// rate-limit token.
	row, found, err := e.readEffect(ctx, req.IdemKey)
	if err != nil {
		return e.storeFailure(ctx, err)
	}
	if found {
		if out, final := e.decide(req.Action, row, e.clock.Now()); final {
			return out
		}
	}
	if _, ok := e.actionBudget(ctx); !ok {
		return e.shortBudget(c)
	}

	ok, wait, err := e.Allow(ctx, req.DefName, e.clock.Now())
	if err != nil {
		return e.storeFailure(ctx, err)
	}
	if !ok {
		e.rateLimited.Add(ctx, 1, c.attrs)
		return ExecOutcome{Kind: RetryLater, Delay: wait}
	}

	if out, claimed := e.claim(ctx, c); !claimed {
		return out
	}
	// Execute has not started, so giving the claim back is safe; leaving it to
	// expire would put a non-idempotent effect in doubt that never ran.
	if err := ctx.Err(); err != nil {
		e.release(ctx, c)
		return ExecOutcome{Kind: Abandoned, Err: err}
	}
	deadline, ok := e.actionBudget(ctx)
	if !ok {
		e.release(ctx, c)
		return e.shortBudget(c)
	}

	testhooks.Reach(testhooks.PointClaimed)
	res := e.execute(ctx, c, deadline)
	testhooks.Reach(testhooks.PointExecuted)
	if res.err == nil {
		return e.complete(ctx, c, res.out)
	}
	msg := e.redact(res.err)
	if res.interrupted {
		c.log.Warn("flowkit executor: request abandoned while the action ran; the claim stays until it expires", "err", msg)
		return ExecOutcome{Kind: Abandoned, Err: storedError(req.Action.Name, msg, res.err)}
	}
	if req.Action.Idempotent || (!res.panicked && e.isNotPerformed(res.err)) {
		return e.fail(ctx, c, res.err, msg)
	}
	return e.markInDoubt(ctx, c, res.err, msg)
}

// call is one Run of a request: the request, the token it claims the effect
// with, and its log and metric attributes.
type call struct {
	ExecRequest
	token ids.UUID
	log   *slog.Logger
	attrs metric.MeasurementOption
}

// actionBudget is the deadline of an Execute starting now: the action
// deadline away, cut to parentMargin before ctx's deadline. Context deadlines
// are on the system clock, not the flowkit one. ok is false when that leaves
// less than minActionBudget.
func (e *Executor) actionBudget(ctx context.Context) (deadline time.Time, ok bool) {
	now := time.Now()
	deadline = now.Add(e.actionDeadline)
	if parent, has := ctx.Deadline(); has {
		deadline = minTime(deadline, parent.Add(-e.parentMargin))
	}
	return deadline, deadline.Sub(now) >= e.minActionBudget
}

func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

// shortBudget is the outcome of a request whose context ends too soon to run
// the action.
func (e *Executor) shortBudget(c *call) ExecOutcome {
	c.log.Debug("flowkit executor: too little time left before the caller's deadline to run the action")
	return ExecOutcome{Kind: RetryLater, Delay: withJitter(shortBudgetRetryDelay, e.rand)}
}

// release gives c's claim back before Execute started, without counting an
// attempt, so the next request can claim the effect at once.
func (e *Executor) release(ctx context.Context, c *call) {
	wctx, cancel := detached(ctx)
	defer cancel()
	var ok bool
	err := e.inTx(wctx, func(tx store.Tx) error {
		var err error
		ok, err = e.store.ReleaseEffectClaim(wctx, tx, c.IdemKey, c.token)
		return err
	})
	switch {
	case err != nil:
		c.log.Warn("flowkit executor: releasing an unused claim failed; it stays until it expires", "err", err)
	case !ok:
		c.log.Warn("flowkit executor: an unused claim was revoked before it was released")
	}
}

// errActionDeadline is the cause of an action context that reached the
// action's own deadline rather than ending with the caller's context.
var errActionDeadline error = actionDeadlineError{}

type actionDeadlineError struct{}

func (actionDeadlineError) Error() string { return "action deadline exceeded" }
func (actionDeadlineError) Unwrap() error { return context.DeadlineExceeded }

type execResult struct {
	out      map[string]any
	err      error
	panicked bool
	// interrupted reports that the caller's context ended before the action
	// deadline, which ended the action's context.
	interrupted bool
}

// execute runs the action until deadline at most. When Execute ignores its
// context, execute returns when the context ends and leaves Execute running.
func (e *Executor) execute(ctx context.Context, c *call, deadline time.Time) execResult {
	actx, cancel := context.WithDeadlineCause(ctx, deadline, errActionDeadline)
	defer cancel()
	in := Input{Params: c.Params, IdemKey: c.IdemKey, RunID: c.RunID.String(), StepID: c.StepID, Scope: c.Scope}

	e.inflight.Add(ctx, 1, c.attrs)
	defer e.inflight.Add(ctx, -1, c.attrs)
	start := e.clock.Now()
	defer func() { e.duration.Record(ctx, e.clock.Now().Sub(start).Seconds(), c.attrs) }()

	done := make(chan execResult, 1)
	go func() {
		defer func() {
			if v := recover(); v != nil {
				err := fmt.Errorf("panicked: %v", v)
				c.log.Error("flowkit executor: action panicked", "panic", e.redact(err), "stack", string(debug.Stack()))
				done <- execResult{err: err, panicked: true}
			}
		}()
		out, err := c.Action.Execute(actx, in)
		done <- execResult{out: out, err: err}
	}()
	var r execResult
	select {
	case r = <-done:
	case <-actx.Done():
		select {
		case r = <-done:
		default:
			r = execResult{err: fmt.Errorf("did not return before its context ended: %w", context.Cause(actx))}
		}
	}
	r.interrupted = actx.Err() != nil && !errors.Is(context.Cause(actx), errActionDeadline)
	return r
}

// complete records a successful execution. The write does not depend on the
// caller's context: once the effect happened, it is recorded if the claim
// still holds, even when a shutdown cancels the context meanwhile.
func (e *Executor) complete(ctx context.Context, c *call, out map[string]any) ExecOutcome {
	raw, norm, invalid := encodeOutput(c.Action.OutputFacts, out, e.maxOutputBytes)
	var stored *string
	var outErr error
	if invalid != nil {
		s := e.redact(invalid)
		raw, stored, outErr = nil, &s, storedError(c.Action.Name, s, invalid)
	}
	wctx, cancel := detached(ctx)
	defer cancel()
	ok, err := e.recordDone(wctx, c, raw, stored)
	if err != nil {
		c.log.Error("flowkit executor: action succeeded but recording it failed; the claim stays until it expires", "err", err)
		return ExecOutcome{Kind: Errored, Err: err}
	}
	if !ok {
		// A try that failed may still have committed; then the effect is done
		// under this claim and nothing was lost.
		row, found, err := e.readEffect(wctx, c.IdemKey)
		if err != nil {
			c.log.Error("flowkit executor: action succeeded but reading back its record failed", "err", err)
			return ExecOutcome{Kind: Errored, Err: err}
		}
		if !found || row.DoneAt == nil || row.ClaimToken == nil || *row.ClaimToken != c.token {
			if found && row.InDoubt && row.DoneAt == nil {
				e.keepOrphanedOutput(wctx, c, raw, stored)
			}
			return e.orphan(wctx, c)
		}
	}
	testhooks.Reach(testhooks.PointRecorded)
	return ExecOutcome{Kind: Done, Output: norm, Err: outErr}
}

// recordDone marks c's effect done. It tries again, with a growing pause,
// after a failure that may be transient, at most completeTries times in all
// and while ctx lasts.
func (e *Executor) recordDone(ctx context.Context, c *call, raw json.RawMessage, stored *string) (bool, error) {
	pause := completeRetryPause
	for try := 1; ; try++ {
		var ok bool
		err := e.inTx(ctx, func(tx store.Tx) error {
			var err error
			ok, err = e.store.CompleteEffect(ctx, tx, c.IdemKey, c.token, e.clock.Now(), raw, stored)
			return err
		})
		if err == nil || try == completeTries || !transient(err) {
			return ok, err
		}
		c.log.Warn("flowkit executor: recording a successful action failed; trying again", "err", err, "try", try)
		t := time.NewTimer(pause)
		select {
		case <-ctx.Done():
			t.Stop()
			return false, err
		case <-t.C:
		}
		pause *= 2
	}
}

// transient reports whether a failed write may succeed when tried again: a
// failure without a Postgres error (a lost connection), or a Postgres error
// of a class that does not depend on the statement: connection exception,
// transaction rollback, insufficient resources, operator intervention.
func transient(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return true
	}
	switch pgErr.Code[:min(2, len(pgErr.Code))] {
	case "08", "40", "53", "57":
		return true
	}
	return false
}

// orphan reports an effect that happened after its claim was revoked: the
// janitor counted it as a failed attempt or put it in doubt, and whoever
// resolves the effect re-enqueues the run.
func (e *Executor) orphan(ctx context.Context, c *call) ExecOutcome {
	e.orphaned.Add(ctx, 1, c.attrs)
	c.log.Error("flowkit executor: action succeeded after its claim was revoked; the effect is orphaned")
	return ExecOutcome{Kind: BlockedInDoubt, Err: errors.New("flowkit executor: claim revoked before the result was recorded")}
}

// errOrphanDropped rolls back the note of an orphaned success whose output
// the in-doubt effect no longer takes.
var errOrphanDropped = errors.New("flowkit executor: orphaned output dropped")

// keepOrphanedOutput keeps what is known of a success that happened after the
// effect went in doubt: its output, raw (nil when invalid, then stored says
// why), on the effect, which stays in doubt, and a line on its dead letter,
// so whoever resolves it sees that the action ran. The dead letter is locked
// before the effect, the order ResolveDeadLetter takes them in, so the two
// cannot deadlock. An output the effect does not take is dropped with its
// note. Failures are only logged: the outcome is the same either way.
func (e *Executor) keepOrphanedOutput(ctx context.Context, c *call, raw json.RawMessage, stored *string) {
	note := "the action succeeded after its claim was revoked; its output is the effect's result"
	if raw == nil {
		note = "the action succeeded after its claim was revoked, with invalid output: " + *stored
	}
	var noted bool
	err := e.inTx(ctx, func(tx store.Tx) (err error) {
		if noted, err = e.store.AppendInDoubtEffectNote(ctx, tx, c.IdemKey, note); err != nil || raw == nil {
			return err
		}
		kept, err := e.store.SetInDoubtEffectResult(ctx, tx, c.IdemKey, raw)
		if err == nil && !kept {
			err = errOrphanDropped
		}
		return err
	})
	switch {
	case errors.Is(err, errOrphanDropped):
		c.log.Warn("flowkit executor: the in-doubt effect changed or already has a result; the orphaned output is dropped")
	case err != nil:
		c.log.Error("flowkit executor: keeping the output of an orphaned effect failed", "err", err)
	case !noted:
		c.log.Warn("flowkit executor: the in-doubt effect has no open dead letter to note the orphaned success on")
	}
}

// fail records a failed attempt that had no effect and releases the claim.
// msg is the redacted execErr.
func (e *Executor) fail(ctx context.Context, c *call, execErr error, msg string) ExecOutcome {
	var attempt int
	var ok bool
	err := e.inTx(ctx, func(tx store.Tx) error {
		var err error
		attempt, ok, err = e.store.FailEffect(ctx, tx, c.IdemKey, c.token, msg)
		if err != nil || !ok {
			return err
		}
		_, err = e.store.AppendStep(ctx, tx, store.StepRow{
			RunID: c.RunID, At: e.clock.Now(), StepID: c.StepID, Kind: stepAttemptFailed,
			Detail: &c.Action.Name, Attempt: &attempt, Error: &msg,
		})
		return err
	})
	if err != nil {
		return e.storeFailure(ctx, err)
	}
	outErr := storedError(c.Action.Name, msg, execErr)
	if !ok {
		c.log.Warn("flowkit executor: claim revoked before a failed attempt was recorded", "err", msg)
		return ExecOutcome{Kind: BlockedInDoubt, Err: outErr}
	}
	p := c.Action.Retry.withDefaults()
	if attempt >= p.MaxAttempts {
		return ExecOutcome{Kind: Failed, Err: exhaustedError(c.Action.Name, attempt, outErr)}
	}
	e.retries.Add(ctx, 1, c.attrs)
	return ExecOutcome{Kind: RetryLater, Delay: withJitter(backoff(p, attempt), e.rand), Err: outErr}
}

// markInDoubt records a failure that may have had an effect. msg is the
// redacted execErr.
func (e *Executor) markInDoubt(ctx context.Context, c *call, execErr error, msg string) ExecOutcome {
	var ok, inserted bool
	err := e.inTx(ctx, func(tx store.Tx) error {
		attempt, marked, err := e.store.MarkEffectInDoubt(ctx, tx, c.IdemKey, c.token, msg)
		if err != nil || !marked {
			return err
		}
		ok = true
		inserted, err = e.recordInDoubt(ctx, tx, effectRef{
			IdemKey: c.IdemKey, RunID: c.RunID, StepID: c.StepID, Action: c.Action.Name, Attempt: attempt + 1,
		}, msg)
		return err
	})
	if err != nil {
		return e.storeFailure(ctx, err)
	}
	if inserted {
		e.countInDoubtDeadLetter(ctx)
	}
	if !ok {
		c.log.Warn("flowkit executor: claim revoked before an in-doubt failure was recorded", "err", msg)
	} else {
		c.log.Warn("flowkit executor: action failed and may have had an effect; it is in doubt", "err", msg)
	}
	return ExecOutcome{Kind: BlockedInDoubt, Err: storedError(c.Action.Name, msg, execErr)}
}

// effectRef identifies an effect in an in-doubt record. Attempt is the
// number of the attempt in doubt.
type effectRef struct {
	IdemKey string   `json:"idem_key"`
	RunID   ids.UUID `json:"run_id"`
	StepID  string   `json:"step_id"`
	Action  string   `json:"action"`
	Attempt int      `json:"attempt"`
}

// recordInDoubt writes the dead letter and the step of an effect that went
// in doubt. It reports whether the dead letter is new rather than an open
// one for the same effect.
func (e *Executor) recordInDoubt(ctx context.Context, tx store.Tx, ref effectRef, msg string) (bool, error) {
	raw, err := json.Marshal(ref)
	if err != nil {
		return false, fmt.Errorf("flowkit executor: encode in-doubt record: %w", err)
	}
	now := e.clock.Now()
	_, inserted, err := e.store.InsertDeadLetter(ctx, tx, store.DeadLetterRow{
		Kind: deadLetterInDoubt, Ref: ref.IdemKey, RunID: &ref.RunID, Raw: raw, Note: &msg, CreatedAt: now,
	})
	if err != nil {
		return false, err
	}
	_, err = e.store.AppendStep(ctx, tx, store.StepRow{
		RunID: ref.RunID, At: now, StepID: ref.StepID, Kind: stepEffectInDoubt,
		Detail: &ref.Action, Attempt: &ref.Attempt, Error: &msg,
	})
	return inserted, err
}

// countInDoubtDeadLetter counts a committed in-doubt dead letter.
func (e *Executor) countInDoubtDeadLetter(ctx context.Context) {
	e.deadLetters.Add(ctx, 1, metric.WithAttributes(attribute.String("kind", deadLetterInDoubt)))
}

// CheckOutput checks an output an operator gives for action a, as an
// action's own output is checked, and returns the JSON to store.
func (e *Executor) CheckOutput(a Action, out map[string]any) (json.RawMessage, error) {
	raw, _, err := encodeOutput(a.OutputFacts, out, e.maxOutputBytes)
	return raw, err
}

// storeFailure is the outcome of a failed store call: Abandoned when the
// caller's context ended (the transaction rolled back), Errored otherwise.
func (e *Executor) storeFailure(ctx context.Context, err error) ExecOutcome {
	if ctx.Err() != nil {
		return ExecOutcome{Kind: Abandoned, Err: err}
	}
	return ExecOutcome{Kind: Errored, Err: err}
}

// inTx runs fn in a transaction it commits when fn succeeds. A failed fn
// rolls back, because a failed statement leaves a Postgres transaction
// unusable.
func (e *Executor) inTx(ctx context.Context, fn func(store.Tx) error) error {
	tx, err := e.store.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		rctx, cancel := detached(ctx)
		defer cancel()
		if rerr := tx.Rollback(rctx); rerr != nil {
			e.log.Warn("flowkit executor: rollback failed", "err", rerr)
		}
		return err
	}
	return tx.Commit(ctx)
}

// detached returns a context that outlives ctx's cancellation but ends after
// detachedWriteTimeout, for writes that must happen even when the caller
// gave up.
func detached(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), detachedWriteTimeout)
}
