package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/g0-up/flowkit/internal/ids"
)

var (
	// ErrNotFound is returned when a looked-up row does not exist.
	ErrNotFound = errors.New("flowkit store: not found")
	// ErrForeignTx is returned when a Tx did not come from the store it is
	// passed to, including a Tx of another PgStore.
	ErrForeignTx = errors.New("flowkit store: transaction does not belong to this store")
	// ErrBorrowedTx is returned by Commit and Rollback on a transaction the
	// caller owns (see PgStore.WrapTx); only the caller may end it.
	ErrBorrowedTx = errors.New("flowkit store: transaction is owned by the caller")
)

// Tx is an open store transaction. Its implementation is private to each
// store; pass it back only to the store that produced it. Implementations
// must be comparable, such as pointers, and distinct per transaction:
// callers key per-transaction state, such as work to do after commit, by
// the Tx.
type Tx interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// Store is the persistence surface the interpreter, executor and scheduler
// use. Methods hold SQL and scanning only, no policy. Every method runs in
// the given transaction; a missing row yields ErrNotFound.
type Store interface {
	Begin(ctx context.Context) (Tx, error)

	// InsertEvent stores e (e.ID is ignored) and returns its id. When an event
	// with the same type and dedup key exists, it returns that event's id
	// and inserted false.
	InsertEvent(ctx context.Context, tx Tx, e EventRow) (id int64, inserted bool, err error)
	GetEvent(ctx context.Context, tx Tx, id int64) (EventRow, error)

	// InsertDefinitionIfAbsent reports whether d was inserted; false means a
	// definition with that name exists.
	InsertDefinitionIfAbsent(ctx context.Context, tx Tx, d DefinitionRow) (bool, error)
	GetDefinition(ctx context.Context, tx Tx, name string) (DefinitionRow, error)
	// LockDefinition reads a definition and locks its row until tx ends.
	LockDefinition(ctx context.Context, tx Tx, name string) (DefinitionRow, error)
	// UpdateDefinitionPublished writes the fields a publish changes:
	// CurrentVersion, trigger and concurrency settings, UpdatedAt.
	UpdateDefinitionPublished(ctx context.Context, tx Tx, d DefinitionRow) error
	SetDefinitionEnabled(ctx context.Context, tx Tx, name string, enabled bool, at time.Time) error

	InsertDraft(ctx context.Context, tx Tx, d DraftRow) error
	GetDraft(ctx context.Context, tx Tx, id ids.UUID) (DraftRow, error)

	InsertDefinitionVersion(ctx context.Context, tx Tx, v DefinitionVersionRow) error
	GetDefinitionVersion(ctx context.Context, tx Tx, name string, version int) (DefinitionVersionRow, error)
	// InsertComponentRefs records the components a definition version uses.
	// Repeated refs are stored once.
	InsertComponentRefs(ctx context.Context, tx Tx, name string, version int, refs []ComponentRef) error
	// ListComponentUsers returns the definition versions that use ref, ordered
	// by name and version.
	ListComponentUsers(ctx context.Context, tx Tx, ref ComponentRef) ([]DefinitionRef, error)

	InsertRun(ctx context.Context, tx Tx, r RunRow) error
	GetRun(ctx context.Context, tx Tx, id ids.UUID) (RunRow, error)
	// ListRunningRunIDs returns the ids of a definition's runs in state
	// running, of every version.
	ListRunningRunIDs(ctx context.Context, tx Tx, defName string) ([]ids.UUID, error)
	// CountPinnedRuns counts a definition's non-terminal runs by version.
	CountPinnedRuns(ctx context.Context, tx Tx, defName string) (map[int]int, error)
	// NextStepSeq allocates the next step sequence number of a run. It does
	// not change the run's Rev.
	NextStepSeq(ctx context.Context, tx Tx, runID ids.UUID) (int, error)
	// AppendStep stores s with the next sequence number (s.Seq is ignored)
	// and returns that number.
	AppendStep(ctx context.Context, tx Tx, s StepRow) (int, error)
	ListSteps(ctx context.Context, tx Tx, runID ids.UUID) ([]StepRow, error)
	// StepRecorded reports whether the run has a step with this step id,
	// kind and detail.
	StepRecorded(ctx context.Context, tx Tx, runID ids.UUID, stepID, kind, detail string) (bool, error)
	// FinishRun moves a non-terminal run whose rev is still f.Rev to a
	// terminal state, bumping rev. It reports false when the run moved on.
	FinishRun(ctx context.Context, tx Tx, f FinishRunRow) (bool, error)
	DeleteRunSubscriptions(ctx context.Context, tx Tx, runID ids.UUID) error
	// UpdateRun writes the frames and facts of a running run whose rev is
	// still u.Rev, bumping rev. It reports false when the run moved on.
	UpdateRun(ctx context.Context, tx Tx, u RunUpdate) (bool, error)
	// EnterWait parks a running run whose rev is still u.Rev on waitStep:
	// state waiting, the next wait sequence, lookback_pending set, rev
	// bumped. It returns the new sequence, or ok false when the run moved on.
	EnterWait(ctx context.Context, tx Tx, u RunUpdate, waitStep string) (waitSeq int, ok bool, err error)
	// ClearLookback clears lookback_pending of a run still waiting on
	// waitSeq, without bumping rev.
	ClearLookback(ctx context.Context, tx Tx, runID ids.UUID, waitSeq int) (bool, error)
	// ClaimRun resumes a run still waiting on (c.WaitStep, c.WaitSeq) with
	// rev c.Rev, bumping rev. It reports false when another claim won.
	ClaimRun(ctx context.Context, tx Tx, c ClaimRow) (bool, error)
	InsertSubscription(ctx context.Context, tx Tx, s SubscriptionRow) error
	DeleteSubscription(ctx context.Context, tx Tx, runID ids.UUID, waitSeq int) error
	// LookbackPage returns up to p.Limit events of p.EventType after the
	// cursor (p.AfterAt, p.AfterID) in (inserted_at, id) order, leaving out
	// the events the run consumed.
	LookbackPage(ctx context.Context, tx Tx, p LookbackQuery) ([]EventRow, error)
	// ListEventSubscriptions returns up to p.Limit subscriptions to
	// p.EventType after the run id p.AfterRunID, in run id order, whose run
	// still waits on them and has not consumed event p.EventID, with the
	// columns of their runs that WaitingRun lists.
	ListEventSubscriptions(ctx context.Context, tx Tx, p SubscriptionQuery) ([]WaitingRun, error)
	// ReadOnly runs fn in a read-only transaction bounded by timeout, both as
	// a statement timeout and as the deadline of the context fn gets. The
	// transaction is always rolled back.
	ReadOnly(ctx context.Context, timeout time.Duration, fn func(ctx context.Context, q ReadQuerier) error) error

	// ListTriggeredDefinitions returns the enabled, published definitions
	// triggered by eventType, ordered by name.
	ListTriggeredDefinitions(ctx context.Context, tx Tx, eventType string) ([]DefinitionRow, error)
	RunExists(ctx context.Context, tx Tx, id ids.UUID) (bool, error)
	// QueuedEventExists reports whether the event waits in the
	// definition's run queue, under any serial key.
	QueuedEventExists(ctx context.Context, tx Tx, defName string, eventID int64) (bool, error)
	InsertConsumedEvent(ctx context.Context, tx Tx, c ConsumedEventRow) error
	// InsertConsumedEventIfAbsent reports false when the run already
	// consumed the event.
	InsertConsumedEventIfAbsent(ctx context.Context, tx Tx, c ConsumedEventRow) (bool, error)
	// LockSerial takes the concurrency gate of (defName, serialKey) until tx
	// ends, creating its row on first use. Gate rows are never deleted.
	LockSerial(ctx context.Context, tx Tx, defName, serialKey string) error
	// LockActiveExclusiveRun reads and locks the non-terminal exclusive run
	// of (defName, serialKey). Take LockSerial first.
	LockActiveExclusiveRun(ctx context.Context, tx Tx, defName, serialKey string) (RunRow, error)
	RunQueueNonEmpty(ctx context.Context, tx Tx, defName, serialKey string) (bool, error)
	// RecentRunExists reports whether a run of (defName, serialKey) was
	// created after since.
	RecentRunExists(ctx context.Context, tx Tx, defName, serialKey string, since time.Time) (bool, error)
	// NewerRunExists reports whether a run of (defName, serialKey), in any
	// state, was triggered by an event emitted after the event (eventAt,
	// eventID), in (inserted_at, id) order.
	NewerRunExists(ctx context.Context, tx Tx, defName, serialKey string, eventAt time.Time, eventID int64) (bool, error)
	// InsertRunQueue queues r and reports false when it is already queued.
	InsertRunQueue(ctx context.Context, tx Tx, r RunQueueRow) (bool, error)
	// PeekRunQueue returns the head of a key's queue in emit order: oldest
	// EventAt first, ties broken by TriggerEventID.
	PeekRunQueue(ctx context.Context, tx Tx, defName, serialKey string) (RunQueueRow, error)
	DeleteRunQueue(ctx context.Context, tx Tx, r RunQueueRow) (bool, error)

	GetEffect(ctx context.Context, tx Tx, idemKey string) (EffectRow, error)

	// InsertDeadLetter stores d (d.ID and resolution fields are ignored) and
	// returns its id. When an open dead letter with the same kind and ref
	// exists, it returns that one's id and inserted false.
	InsertDeadLetter(ctx context.Context, tx Tx, d DeadLetterRow) (id int64, inserted bool, err error)
	GetDeadLetter(ctx context.Context, tx Tx, id int64) (DeadLetterRow, error)
	// ClaimDeadLetter resolves an open dead letter and returns it. It
	// reports false when the dead letter is resolved or does not exist.
	ClaimDeadLetter(ctx context.Context, tx Tx, c DeadLetterClaim) (DeadLetterRow, bool, error)
	// RedriveInDoubtEffect releases an unfinished in-doubt effect for
	// another attempt, clearing its claim. It reports false when the effect
	// is not in doubt or has kept the output of a late success.
	RedriveInDoubtEffect(ctx context.Context, tx Tx, idemKey string) (bool, error)
	// MarkInDoubtEffectDone finishes an unfinished in-doubt effect with
	// result. It reports false when the effect is not in doubt.
	MarkInDoubtEffectDone(ctx context.Context, tx Tx, idemKey string, at time.Time, result json.RawMessage) (bool, error)

	// ListStaleLookbacks returns up to limit waiting runs whose look-back
	// is unfinished and that were not written since before, oldest first.
	ListStaleLookbacks(ctx context.Context, tx Tx, before time.Time, limit int) ([]ids.UUID, error)
	// ListStuckRuns: see PgStore.ListStuckRuns.
	ListStuckRuns(ctx context.Context, tx Tx, q StuckRunQuery) ([]ids.UUID, error)
	// ListOrphanedQueues returns up to limit non-empty run queues of
	// enabled, published definitions whose key has no non-terminal
	// exclusive run.
	ListOrphanedQueues(ctx context.Context, tx Tx, limit int) ([]SerialKeyRef, error)
	SummarizeRuns(ctx context.Context, tx Tx, endedBefore time.Time, limit int) (int, error)
	DeleteExpiredSummaries(ctx context.Context, tx Tx, endedBefore time.Time, limit int) (int, error)
	DeleteExpiredEvents(ctx context.Context, tx Tx, insertedBefore time.Time, limit int) (int, error)
	DeleteResolvedDeadLetters(ctx context.Context, tx Tx, resolvedBefore time.Time, limit int) (int, error)

	ListRuns(ctx context.Context, tx Tx, q RunListQuery) ([]RunSummaryRow, error)
	ReadGauges(ctx context.Context, tx Tx, queue string) (Gauges, error)
}

// EventRow is a row of domain_events.
type EventRow struct {
	ID         int64
	Type       string
	DedupKey   *string
	Payload    json.RawMessage
	OccurredAt time.Time
	InsertedAt time.Time
}

// DefinitionRow is a row of definitions.
type DefinitionRow struct {
	Name               string
	Enabled            bool
	CurrentVersion     *int // nil until the first publish
	TriggerEventType   *string
	ConcurrencyPolicy  string
	SerialKeyCEL       *string
	DedupWindowSeconds *int
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// DraftRow is a row of drafts.
type DraftRow struct {
	ID               ids.UUID
	Name             string
	Body             json.RawMessage
	MetamodelVersion int
	CreatedBy        *string
	CreatedAt        time.Time
}

// DefinitionVersionRow is a row of definition_versions.
type DefinitionVersionRow struct {
	Name             string
	Version          int
	Body             json.RawMessage
	MetamodelVersion int
	Checksum         string
	PublishedBy      *string
	Note             *string
	PublishedAt      time.Time
}

// ComponentRef names a component a definition uses. Kind is "event",
// "action" or "fact".
type ComponentRef struct {
	Kind string
	Ref  string
}

// DefinitionRef names one published definition version.
type DefinitionRef struct {
	Name    string
	Version int
}

// RunRow is a row of runs.
type RunRow struct {
	ID               ids.UUID
	DefName          string
	DefVersion       int
	MetamodelVersion int
	LibVersion       string
	Definition       json.RawMessage
	TriggerEventID   int64
	State            string
	Terminal         bool
	TerminalName     *string
	EntityKind       *string
	EntityID         *string
	ScopeExtra       json.RawMessage // nil for SQL NULL
	SerialKey        *string
	Exclusive        bool
	WaitStep         *string
	WaitSeq          int
	LookbackPending  bool
	Frames           json.RawMessage
	Facts            json.RawMessage
	Rev              int64
	StepSeq          int
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// FinishRunRow is a terminal transition of a run.
type FinishRunRow struct {
	ID           ids.UUID
	Rev          int64 // the rev the caller read
	State        string
	TerminalName string
	At           time.Time
}

// RunUpdate is an optimistic write of a run.
type RunUpdate struct {
	ID     ids.UUID
	Rev    int64 // the rev the caller read
	Frames json.RawMessage
	Facts  json.RawMessage
	At     time.Time
}

// ClaimRow resumes a waiting run with new frames.
type ClaimRow struct {
	ID       ids.UUID
	Rev      int64 // the rev the caller read
	WaitStep string
	WaitSeq  int
	Frames   json.RawMessage
	At       time.Time
}

// SubscriptionRow is a row of subscriptions.
type SubscriptionRow struct {
	RunID     ids.UUID
	WaitSeq   int
	StepID    string
	EventType string
	MatchCEL  string // empty when the wait has no match
	CreatedAt time.Time
}

// WaitingRun is a subscription with the run that waits on it. Sub has no
// MatchCEL. Run has only ID, DefName, DefVersion, TriggerEventID, State,
// EntityKind, EntityID, ScopeExtra, SerialKey, Exclusive, WaitStep,
// WaitSeq, Frames, Rev and CreatedAt: the pinned definition and the facts
// are left out.
type WaitingRun struct {
	Sub SubscriptionRow
	Run RunRow
}

// SubscriptionQuery is one page of the subscriptions an event may satisfy.
type SubscriptionQuery struct {
	EventType  string
	EventID    int64
	AfterRunID ids.UUID // the zero UUID starts at the first run
	Limit      int
}

// LookbackQuery is one keyset page of a wait's look-back.
type LookbackQuery struct {
	RunID     ids.UUID
	EventType string
	AfterAt   time.Time
	AfterID   int64
	Limit     int
}

// ReadQuerier reads within a read-only transaction. It has no Exec. The
// read-only mode and the statement timeout are guardrails against mistakes,
// not a sandbox: a statement such as COMMIT or SET passed to Query lifts
// them, and only the context deadline still holds. Its callers are trusted
// code.
type ReadQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// ConsumedEventRow is a row of consumed_events.
type ConsumedEventRow struct {
	RunID   ids.UUID
	EventID int64
	StepID  string
	At      time.Time
}

// RunQueueRow is a row of run_queue.
type RunQueueRow struct {
	DefName        string
	SerialKey      string
	TriggerEventID int64
	EventAt        time.Time // the trigger event's inserted_at
	CreatedAt      time.Time
}

// StepRow is a row of run_steps.
type StepRow struct {
	RunID    ids.UUID
	Seq      int
	At       time.Time
	StepID   string
	Kind     string
	Detail   *string
	Snapshot json.RawMessage // nil for SQL NULL
	Attempt  *int
	Error    *string
}

// EffectRow is a row of executed_effects.
type EffectRow struct {
	IdemKey    string
	RunID      ids.UUID
	StepID     string
	Action     string
	Attempt    int
	ClaimedAt  *time.Time
	ClaimToken *ids.UUID
	DoneAt     *time.Time
	Result     json.RawMessage // nil for SQL NULL
	LastError  *string
	InDoubt    bool
}

// DeadLetterRow is a row of dead_letters.
type DeadLetterRow struct {
	ID            int64
	Kind          string
	Ref           string
	RunID         *ids.UUID
	EventID       *int64
	Raw           json.RawMessage
	Note          *string
	CreatedAt     time.Time
	ResolvedAt    *time.Time
	Resolution    *string
	ResolvedBy    *string
	ResolveReason *string
}
