package flowkit

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/g0-up/flowkit/internal/migrate"
	"github.com/g0-up/flowkit/internal/sched"
	"github.com/g0-up/flowkit/internal/testhooks"
)

// Config configures an Engine. Only Pool is required; New fills every zero
// field with its default.
//
// Timestamps flowkit writes, including an event's inserted_at, come from
// Clock on the writing instance, not from the database at commit time. The
// look-back that recovers events committed while a run was being created
// therefore relies on LookbackMargin covering both the clock skew between
// instances and the longest time a caller's transaction stays open after
// Emit. Keep instance clocks synchronised with NTP.
type Config struct {
	// Pool is the connection pool flowkit uses. Required. See WorkerConfig
	// for how many connections the workers need.
	Pool *pgxpool.Pool
	// Schema is the Postgres schema holding flowkit's and River's tables.
	// Default "flowkit". It must match ^[a-z_][a-z0-9_]{0,45}$ and must not
	// be public, information_schema or pg_*.
	Schema string
	// Logger receives flowkit's logs. Default slog.Default(). Payloads are
	// never logged above Debug.
	Logger *slog.Logger
	// Tracer and Meter are OpenTelemetry providers. Default no-op.
	Tracer trace.TracerProvider
	Meter  metric.MeterProvider
	// Clock is the source of every timestamp flowkit writes. Default the
	// system clock; tests use flowkittest.VirtualClock.
	Clock Clock
	// InstanceID names this instance in spans and logs. Default
	// "<hostname>-<pid>".
	InstanceID string
	// Roles are the roles of this instance. Default {RoleWorker,
	// RoleJanitor}. At least one instance in the deployment must run
	// RoleJanitor, or timers never fire.
	Roles   []Role
	Workers WorkerConfig
	// ClaimTimeout bounds one action execution and derives the rest of the
	// timing chain: the action deadline is 0.8×, the pump job timeout 0.9×,
	// and a run counts as stuck after 2× ClaimTimeout, which must stay below
	// the one-hour rescue of stuck jobs. Default 5m; minimum 5s; must be
	// below 30m.
	ClaimTimeout time.Duration
	// LookbackMargin widens the look-back for events committed around run
	// creation. It must cover clock skew between instances plus the time a
	// caller's transaction stays open after Emit (see above). Default 1m.
	LookbackMargin time.Duration
	// FactTimeout is the statement_timeout and context deadline of a fact
	// resolver. Default 5s.
	FactTimeout time.Duration
	// MaxPayloadBytes caps the JSON size of an emitted payload and of an
	// action output. Default 256 KiB.
	MaxPayloadBytes int
	Retention       RetentionConfig
	// RedactError turns every error into the text flowkit stores (step
	// errors, dead letters). The default stores err.Error() cut to 2 KiB,
	// unchanged otherwise: consumers whose errors can carry personal data
	// must supply their own function.
	RedactError func(error) string
}

// WorkerConfig sizes job processing on an instance with RoleWorker.
//
// River holds one extra connection for LISTEN/NOTIFY outside the job
// workers, so the pool needs MaxConns of at least MaxWorkers plus the
// connections the application uses, plus one, unless PollOnly is set.
type WorkerConfig struct {
	// MaxWorkers is the number of flowkit jobs processed concurrently.
	// Default 4; at most 10000.
	MaxWorkers int
	// PollInterval is how often the queue is polled for jobs. Zero keeps
	// River's default (1s); otherwise at least 100ms.
	PollInterval time.Duration
	// PollOnly disables LISTEN/NOTIFY and relies on polling alone, freeing
	// the listener connection. Required when the pool goes through PgBouncer
	// in transaction mode.
	PollOnly bool
	// EffectRateLimit caps action executions per second for each definition.
	// Zero means unlimited.
	EffectRateLimit float64
}

// RetentionConfig bounds how long the janitor keeps data. The janitor
// deletes in batches of 500 rows, one short transaction each, with every
// cutoff taken from Config.Clock. Zero fields select the defaults; New
// rejects negative values and values below the bounds given per field.
type RetentionConfig struct {
	// StepDetail is how long a finished run keeps its detail: steps,
	// consumed events, subscriptions and effects. Then only its summary
	// remains: Runs.Trace returns ErrNotFound for it and Runs.List still
	// lists it. A summarised run no longer counts for the dedup_window and
	// cancel_previous policies, so keep StepDetail above the longest
	// dedup_window in use. Default 90 days; at least 1 day.
	StepDetail time.Duration
	// Run is how long a run summary is kept, counted from the run's end.
	// Default 2 years; at least StepDetail.
	Run time.Duration
	// Event is how long emitted events are kept; an event a run, a consumed
	// event or a queued trigger still references is kept longer. It is also
	// how long Emit deduplicates by dedup key. An event older than Event is
	// gone for look-back too: a wait entered more than Event after its run
	// was created only receives the events still kept, not every event
	// since the run began. Default 30 days; at least LookbackMargin plus
	// ClaimTimeout, which only keeps the events around a run's creation
	// for a look-back the janitor resumes after a pump lost its claim.
	Event time.Duration
	// DeadLetter is how long a resolved dead letter is kept, counted from
	// its resolution. Open dead letters are never removed. Default 90 days;
	// at least 1 hour.
	DeadLetter time.Duration
}

const (
	defaultSchema          = "flowkit"
	defaultClaimTimeout    = 5 * time.Minute
	defaultLookbackMargin  = time.Minute
	defaultFactTimeout     = 5 * time.Second
	defaultMaxPayloadBytes = 256 << 10
	defaultMaxWorkers      = 4
	maxMaxWorkers          = 10000
	minPollInterval        = 100 * time.Millisecond

	day                  = 24 * time.Hour
	defaultStepDetail    = 90 * day
	minStepDetail        = day
	minDeadLetterAge     = time.Hour
	defaultRunRetention  = 2 * 365 * day
	defaultEventKeep     = 30 * day
	defaultDeadLetterAge = 90 * day

	// redactLimit is how many bytes of an error the default RedactError keeps.
	redactLimit = 2 << 10
)

// withDefaults returns c with every zero field defaulted, or an error naming
// the first invalid field.
func (c Config) withDefaults() (Config, error) {
	if c.Pool == nil {
		return c, errors.New("flowkit: Config.Pool is required")
	}
	return c.applyDefaults()
}

// applyDefaults is withDefaults without the Pool, which the simulator's
// in-memory Engine does not use.
func (c Config) applyDefaults() (Config, error) {
	if c.Schema == "" {
		c.Schema = defaultSchema
	}
	if err := migrate.ValidateSchemaName(c.Schema); err != nil {
		return c, fmt.Errorf("flowkit: Config.Schema: %w", err)
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Tracer == nil {
		c.Tracer = tracenoop.NewTracerProvider()
	}
	if c.Meter == nil {
		c.Meter = metricnoop.NewMeterProvider()
	}
	if c.Clock == nil {
		c.Clock = realClock{}
	}
	if c.InstanceID == "" {
		c.InstanceID = defaultInstanceID()
	}
	if c.RedactError == nil {
		c.RedactError = defaultRedactError
	}

	if len(c.Roles) == 0 {
		c.Roles = []Role{RoleWorker, RoleJanitor}
	} else {
		roles := make([]Role, 0, len(c.Roles))
		for _, r := range c.Roles {
			if r != RoleWorker && r != RoleJanitor {
				return c, fmt.Errorf("flowkit: Config.Roles: unknown role %d", int(r))
			}
			if !slices.Contains(roles, r) {
				roles = append(roles, r)
			}
		}
		c.Roles = roles
	}

	var err error
	set := func(field string, v *time.Duration, def time.Duration) {
		switch {
		case err != nil:
		case *v < 0:
			err = fmt.Errorf("flowkit: Config.%s must not be negative, got %s", field, *v)
		case *v == 0:
			*v = def
		}
	}
	set("ClaimTimeout", &c.ClaimTimeout, defaultClaimTimeout)
	set("LookbackMargin", &c.LookbackMargin, defaultLookbackMargin)
	set("FactTimeout", &c.FactTimeout, defaultFactTimeout)
	set("Retention.StepDetail", &c.Retention.StepDetail, defaultStepDetail)
	set("Retention.Run", &c.Retention.Run, defaultRunRetention)
	set("Retention.Event", &c.Retention.Event, defaultEventKeep)
	set("Retention.DeadLetter", &c.Retention.DeadLetter, defaultDeadLetterAge)
	if err != nil {
		return c, err
	}
	if err := engineTiming(c.ClaimTimeout).Validate(); err != nil {
		return c, fmt.Errorf("flowkit: Config.ClaimTimeout: %w", err)
	}
	if err := c.Retention.validate(c.LookbackMargin + c.ClaimTimeout); err != nil {
		return c, err
	}

	switch {
	case c.MaxPayloadBytes < 0:
		return c, fmt.Errorf("flowkit: Config.MaxPayloadBytes must not be negative, got %d", c.MaxPayloadBytes)
	case c.MaxPayloadBytes == 0:
		c.MaxPayloadBytes = defaultMaxPayloadBytes
	}

	w := &c.Workers
	switch {
	case w.MaxWorkers < 0 || w.MaxWorkers > maxMaxWorkers:
		return c, fmt.Errorf("flowkit: Config.Workers.MaxWorkers must be between 1 and %d, got %d", maxMaxWorkers, w.MaxWorkers)
	case w.MaxWorkers == 0:
		w.MaxWorkers = defaultMaxWorkers
	}
	if w.PollInterval < 0 || (w.PollInterval > 0 && w.PollInterval < minPollInterval) {
		return c, fmt.Errorf("flowkit: Config.Workers.PollInterval must be zero or at least %s, got %s", minPollInterval, w.PollInterval)
	}
	if w.EffectRateLimit < 0 || math.IsNaN(w.EffectRateLimit) || math.IsInf(w.EffectRateLimit, 0) {
		return c, fmt.Errorf("flowkit: Config.Workers.EffectRateLimit must be zero (unlimited) or a finite positive rate, got %g", w.EffectRateLimit)
	}
	return c, nil
}

// engineTiming is the timing chain of an Engine with the given ClaimTimeout.
// A crash test may shorten its job rescue delay and job timeout through
// testhooks.Timing; production always uses the fixed values.
func engineTiming(claimTimeout time.Duration) sched.Timing {
	t := sched.NewTiming(claimTimeout)
	if h := testhooks.Timing; h != nil {
		t.RescueStuckJobsAfter, t.JobTimeout = h(claimTimeout)
	}
	return t
}

// validate checks defaulted retention against its lower bounds; minEvent
// is LookbackMargin plus ClaimTimeout.
func (r RetentionConfig) validate(minEvent time.Duration) error {
	switch {
	case r.StepDetail < minStepDetail:
		return fmt.Errorf("flowkit: Config.Retention.StepDetail must be at least %s, got %s", minStepDetail, r.StepDetail)
	case r.Run < r.StepDetail:
		return fmt.Errorf("flowkit: Config.Retention.Run must be at least Retention.StepDetail (%s), got %s", r.StepDetail, r.Run)
	case r.Event < minEvent:
		return fmt.Errorf("flowkit: Config.Retention.Event must be at least LookbackMargin + ClaimTimeout (%s), got %s", minEvent, r.Event)
	case r.DeadLetter < minDeadLetterAge:
		return fmt.Errorf("flowkit: Config.Retention.DeadLetter must be at least %s, got %s", minDeadLetterAge, r.DeadLetter)
	}
	return nil
}

func (c Config) hasRole(r Role) bool { return slices.Contains(c.Roles, r) }

func defaultInstanceID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

// defaultRedactError keeps err.Error() up to redactLimit bytes, cut at a rune
// boundary. Invalid UTF-8 and U+0000, which Postgres text cannot hold, become
// U+FFFD, so the result is never empty for a non-empty message.
func defaultRedactError(err error) string {
	if err == nil {
		return ""
	}
	s := strings.ToValidUTF8(err.Error(), "\uFFFD")
	s = strings.ReplaceAll(s, "\x00", "\uFFFD")
	if len(s) <= redactLimit {
		return s
	}
	// s is valid UTF-8, so a rune starts within the last UTFMax bytes.
	cut := redactLimit
	for cut > redactLimit-utf8.UTFMax && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}
