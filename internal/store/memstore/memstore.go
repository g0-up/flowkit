// Package memstore is an in-memory store.Store for the simulator. It keeps
// the constraints and the query semantics flowkit relies on: optimistic rev
// checks, the unique indexes and primary keys that queries conflict on,
// foreign keys on insert, CHECK constraints, NOT NULL and valid JSON in jsonb
// columns, NULL filters, keyset ordering and LIMIT. A violated constraint
// returns a *pgconn.PgError with the SQLSTATE Postgres uses and, as Postgres
// does, aborts the transaction.
//
// One transaction runs at a time: Begin waits until the open one ends, so
// memstore never shows a race. A transaction writes to copies of the tables
// it changes; Commit publishes them and Rollback drops them. Timestamps are
// stored at microsecond precision, as timestamptz is.
//
// memstore deliberately does not check foreign keys on delete: every delete
// flowkit makes removes the referencing rows first or skips referenced rows.
package memstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/g0-up/flowkit/internal/executor"
	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/store"
)

var (
	_ store.Store    = (*Store)(nil)
	_ executor.Store = (*Store)(nil)
)

// Jobs is the view of the job queue that queries reading River's job table
// need. The simulator's scheduler implements it.
type Jobs interface {
	// LiveJob reports whether a job of kind for the run is waiting to run
	// or running.
	LiveJob(kind string, runID ids.UUID) bool
	// Available counts the jobs of queue that are due and not running.
	Available(queue string) int
}

// Store is the in-memory store. The zero value is not usable; call New.
type Store struct {
	// sem holds one token while a transaction is open.
	sem chan struct{}

	// Guarded by sem.
	data             tables
	lastEventID      int64
	lastDeadLetterID int64

	jobsMu sync.Mutex
	jobs   Jobs
}

// New returns an empty store.
func New() *Store {
	return &Store{sem: make(chan struct{}, 1), data: newTables()}
}

// SetJobs gives the store the job queue that ListStuckRuns and ReadGauges
// read. Without one, no job is live and the queue is empty.
func (s *Store) SetJobs(j Jobs) {
	s.jobsMu.Lock()
	defer s.jobsMu.Unlock()
	s.jobs = j
}

func (s *Store) jobView() Jobs {
	s.jobsMu.Lock()
	defer s.jobsMu.Unlock()
	return s.jobs
}

// tables holds every table. A transaction starts with a copy of this struct,
// whose maps it shares with the committed state until it first writes to
// one; see write.
type tables struct {
	events      map[int64]store.EventRow
	definitions map[string]store.DefinitionRow
	drafts      map[ids.UUID]store.DraftRow
	versions    map[store.DefinitionRef]store.DefinitionVersionRow
	refs        map[componentRefKey]struct{}
	runs        map[ids.UUID]store.RunRow
	steps       map[ids.UUID][]store.StepRow
	subs        map[subKey]store.SubscriptionRow
	consumed    map[consumedKey]store.ConsumedEventRow
	effects     map[string]store.EffectRow
	deadLetters map[int64]store.DeadLetterRow
	runQueue    map[queueKey]store.RunQueueRow
	serialLocks map[store.SerialKeyRef]struct{}
	rateLimits  map[string]store.RateLimitRow
	summaries   map[ids.UUID]store.RunSummaryRow
}

// Table bits of memTx.owned.
const (
	tabEvents = iota
	tabDefinitions
	tabDrafts
	tabVersions
	tabRefs
	tabRuns
	tabSteps
	tabSubs
	tabConsumed
	tabEffects
	tabDeadLetters
	tabRunQueue
	tabSerialLocks
	tabRateLimits
	tabSummaries
)

type componentRefKey struct {
	def store.DefinitionRef
	ref store.ComponentRef
}

type subKey struct {
	runID   ids.UUID
	waitSeq int
}

type consumedKey struct {
	runID   ids.UUID
	eventID int64
}

type queueKey struct {
	defName, serialKey string
	eventID            int64
}

func newTables() tables {
	return tables{
		events:      map[int64]store.EventRow{},
		definitions: map[string]store.DefinitionRow{},
		drafts:      map[ids.UUID]store.DraftRow{},
		versions:    map[store.DefinitionRef]store.DefinitionVersionRow{},
		refs:        map[componentRefKey]struct{}{},
		runs:        map[ids.UUID]store.RunRow{},
		steps:       map[ids.UUID][]store.StepRow{},
		subs:        map[subKey]store.SubscriptionRow{},
		consumed:    map[consumedKey]store.ConsumedEventRow{},
		effects:     map[string]store.EffectRow{},
		deadLetters: map[int64]store.DeadLetterRow{},
		runQueue:    map[queueKey]store.RunQueueRow{},
		serialLocks: map[store.SerialKeyRef]struct{}{},
		rateLimits:  map[string]store.RateLimitRow{},
		summaries:   map[ids.UUID]store.RunSummaryRow{},
	}
}

// memTx is the Tx of Store.
type memTx struct {
	s     *Store
	t     tables
	owned uint32 // tables t holds its own copy of
	// aborted is set by a failed statement: like Postgres, the
	// transaction then refuses every statement and Commit rolls it back.
	aborted bool
	done    bool
	after   []func()
}

// write returns the table m of t for writing, copying it on the first write
// in the transaction. Rows are values that are replaced, never changed in
// place, so a shallow copy keeps the committed state intact.
func write[K comparable, V any](t *memTx, table int, m *map[K]V) map[K]V {
	if t.owned&(1<<table) == 0 {
		*m = maps.Clone(*m)
		t.owned |= 1 << table
	}
	return *m
}

// Begin starts a transaction, waiting until the open one ends.
func (s *Store) Begin(ctx context.Context) (store.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("flowkit store: begin: %w", err)
	}
	select {
	case s.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, fmt.Errorf("flowkit store: begin: %w", ctx.Err())
	}
	return &memTx{s: s, t: s.data}, nil
}

// Commit publishes the transaction's writes and then runs the functions
// registered with AfterCommit. A transaction a failed statement aborted, or
// whose ctx has ended, is rolled back instead, as Postgres does.
func (t *memTx) Commit(ctx context.Context) error {
	if t.done {
		return pgx.ErrTxClosed
	}
	t.done = true
	if err := ctx.Err(); err != nil {
		t.s.release()
		return fmt.Errorf("flowkit store: commit: %w", err)
	}
	if t.aborted {
		t.s.release()
		return pgx.ErrTxCommitRollback
	}
	t.s.data = t.t
	after := t.after
	t.s.release()
	for _, fn := range after {
		fn()
	}
	return nil
}

// Rollback drops the transaction's writes.
func (t *memTx) Rollback(context.Context) error {
	if t.done {
		return pgx.ErrTxClosed
	}
	t.done = true
	t.s.release()
	return nil
}

func (s *Store) release() { <-s.sem }

// AfterCommit registers fn to run once tx commits, after the store accepts
// the next transaction. A rollback drops it. The simulator's scheduler uses
// it to make jobs visible on commit, as River's jobs are.
func (s *Store) AfterCommit(tx store.Tx, fn func()) error {
	t, ok := tx.(*memTx)
	if !ok || t == nil || t.s != s {
		return store.ErrForeignTx
	}
	if t.done {
		return pgx.ErrTxClosed
	}
	t.after = append(t.after, fn)
	return nil
}

// use returns tx as a transaction of s that can run a statement.
func (s *Store) use(ctx context.Context, tx store.Tx) (*memTx, error) {
	t, ok := tx.(*memTx)
	if !ok || t == nil || t.s != s {
		return nil, store.ErrForeignTx
	}
	if t.done {
		return nil, pgx.ErrTxClosed
	}
	if t.aborted {
		return nil, &pgconn.PgError{Severity: "ERROR", Code: codeTxAborted,
			Message: "current transaction is aborted, commands ignored until end of transaction block"}
	}
	if err := ctx.Err(); err != nil {
		t.aborted = true
		return nil, err
	}
	return t, nil
}

// SQLSTATE codes memstore returns.
const (
	codeNotNull      = "23502"
	codeForeignKey   = "23503"
	codeUnique       = "23505"
	codeCheck        = "23514"
	codeInvalidJSON  = "22P02"
	codeInvalidLimit = "2201W"
	codeTxAborted    = "25P02"
)

func pgErr(code, constraint, msg string) *pgconn.PgError {
	return &pgconn.PgError{Severity: "ERROR", Code: code, ConstraintName: constraint, Message: msg}
}

func uniqueErr(constraint string) error {
	return pgErr(codeUnique, constraint, fmt.Sprintf("duplicate key value violates unique constraint %q", constraint))
}

func foreignKeyErr(constraint string) error {
	return pgErr(codeForeignKey, constraint, fmt.Sprintf("insert or update violates foreign key constraint %q", constraint))
}

func checkErr(constraint string) error {
	return pgErr(codeCheck, constraint, fmt.Sprintf("new row violates check constraint %q", constraint))
}

// fail aborts t and wraps err as the store wraps statement errors.
func (t *memTx) fail(op string, err error) error {
	t.aborted = true
	return fmt.Errorf("flowkit store: %s: %w", op, err)
}

func notFound(op string) error {
	return fmt.Errorf("flowkit store: %s: %w", op, store.ErrNotFound)
}

// limit checks a LIMIT argument as Postgres and the int4 parameter do.
func (t *memTx) limit(op string, n int) error {
	if n > math.MaxInt32 || n < math.MinInt32 {
		return fmt.Errorf("flowkit store: limit %d out of int4 range", n)
	}
	if n < 0 {
		return t.fail(op, pgErr(codeInvalidLimit, "", "LIMIT must not be negative"))
	}
	return nil
}

// jsonCol checks a jsonb value; nil is SQL NULL, refused when notNull.
func jsonCol(column string, v json.RawMessage, notNull bool) error {
	if v == nil {
		if notNull {
			return pgErr(codeNotNull, "", fmt.Sprintf("null value in column %q violates not-null constraint", column))
		}
		return nil
	}
	if !json.Valid(v) {
		return pgErr(codeInvalidJSON, "", fmt.Sprintf("invalid input syntax for type json in column %q", column))
	}
	return nil
}

// ts is a timestamp as timestamptz stores it: microsecond precision, no
// monotonic reading.
func ts(t time.Time) time.Time { return t.Truncate(time.Microsecond) }

func tsPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	v := ts(*t)
	return &v
}

func cp[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

func cpJSON(v json.RawMessage) json.RawMessage {
	if v == nil {
		return nil
	}
	return append(json.RawMessage{}, v...)
}

// ReadOnly runs fn with a querier that fails every query: the simulator has
// no database for fact resolvers to read. Like the Postgres store, it bounds
// fn's context by timeout. It takes no transaction.
func (s *Store) ReadOnly(ctx context.Context, timeout time.Duration, fn func(ctx context.Context, q store.ReadQuerier) error) error {
	if timeout <= 0 {
		return fmt.Errorf("flowkit store: read-only timeout %v must be positive", timeout)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return fn(ctx, noDatabase{})
}

// ErrNoDatabase is the error of every query a fact resolver makes in the
// simulator.
var ErrNoDatabase = errors.New("flowkit simulator: there is no database to query; resolve the fact without the querier or test it on Postgres")

type noDatabase struct{}

func (noDatabase) Query(context.Context, string, ...any) (pgx.Rows, error) { return nil, ErrNoDatabase }

func (noDatabase) QueryRow(context.Context, string, ...any) pgx.Row { return errRow{} }

type errRow struct{}

func (errRow) Scan(...any) error { return ErrNoDatabase }
