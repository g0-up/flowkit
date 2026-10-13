package flowkit

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/g0-up/flowkit/internal/errtext"
	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/interpreter"
	"github.com/g0-up/flowkit/internal/store"
)

// RunSummary is a run as Runs.List and Runs.Trace show it.
type RunSummary struct {
	RunID      string
	DefName    string
	DefVersion int
	// State is running, waiting, completed, failed or cancelled.
	State string
	// TerminalName is the name of the end step the run reached, or a
	// reserved name: $action_failed, $failed, $cancelled (Runs.Cancel) or
	// $cancelled_by_newer. Empty while the run is not terminal.
	TerminalName     string
	TriggerEventType string
	StepCount        int
	StartedAt        time.Time
	EndedAt          time.Time // zero while the run is not terminal
	// Cursor is the keyset cursor of the page after this run; pass it as
	// RunFilter.After. It is opaque.
	Cursor string
}

// StepRecord is one recorded step of a run.
type StepRecord struct {
	Seq    int
	At     time.Time
	StepID string
	// Kind is what happened: entered, executed, skipped, attempt_failed,
	// effect_in_doubt, waiting, claimed, claim_lost, stale_pump,
	// match_error, terminal and others.
	Kind   string
	Detail string
	// Snapshot holds the facts and values a decision was taken on. JSON
	// numbers are json.Number, keeping their digits.
	Snapshot map[string]any
	Attempt  int
	// Error is the stored error, already passed through Config.RedactError.
	Error string
}

// RunTrace is the full detail of a run.
type RunTrace struct {
	Run              RunSummary
	MetamodelVersion int
	LibVersion       string
	TriggerEventID   int64
	// TriggerEvent is the event that started the run. Payload numbers are
	// json.Number, keeping their digits.
	TriggerEvent Event
	Steps        []StepRecord // in Seq order
}

// RunFilter selects runs for Runs.List. Zero fields do not filter.
type RunFilter struct {
	DefName string
	// States keeps runs in any of these states.
	States []string
	// EntityKind and EntityID keep runs of this scope entity.
	EntityKind, EntityID string
	// From and To bound the time the run started: From inclusive, To
	// exclusive.
	From, To time.Time
	// After is the Cursor of the last run of the previous page.
	After string
	// Limit is the page size. Default 100; at most 1000.
	Limit int
}

// ResolutionKind is what ResolveDeadLetter does with a dead letter.
type ResolutionKind int

const (
	// ResolveRedrive runs the failed work again: an effect in doubt is
	// released and its action runs again, unless the action is known to
	// have succeeded late; an unroutable or mismatched event is routed
	// again.
	ResolveRedrive ResolutionKind = iota + 1
	// ResolveMarkDone records an effect in doubt as done with
	// Resolution.Output as the action's output, and the run goes on.
	ResolveMarkDone
	// ResolveSkip only records the resolution. A run blocked by an effect
	// in doubt stays blocked until Runs.Cancel.
	ResolveSkip
)

// Resolution is an operator's decision on a dead letter.
type Resolution struct {
	Kind   ResolutionKind
	Actor  string // recorded as resolved_by
	Reason string // recorded as resolve_reason
	// Output is the action's output for ResolveMarkDone, checked against
	// the action's OutputFacts.
	Output map[string]any
}

// Page sizes of Runs.List.
const (
	defaultRunPage = 100
	maxRunPage     = 1000
)

// runStates are the states a run can be in.
var runStates = []string{"running", "waiting", "completed", "failed", "cancelled"}

// Runs reads and operates runs and dead letters. Get it from Engine.Runs.
//
// Run ids are not secret: a run id is derived from the definition name and
// the trigger event id, so it can be guessed. Every Runs operation relies
// on the application's own authorization, never on knowing an id.
type Runs struct {
	e *Engine
}

// Runs returns the run API of the Engine.
func (e *Engine) Runs() *Runs { return &Runs{e: e} }

// Trace returns a run with its trigger event and every recorded step, in
// one response: it is not paginated, so a run that loops through
// thousands of steps returns them all. A run that does not exist, or whose
// detail retention removed, returns ErrNotFound; Runs.List still shows a
// summarised run.
func (r *Runs) Trace(ctx context.Context, runID string) (RunTrace, error) {
	id, err := ids.Parse(runID)
	if err != nil {
		return RunTrace{}, fmt.Errorf("%w: run %q", ErrNotFound, runID)
	}
	var (
		run   store.RunRow
		event store.EventRow
		steps []store.StepRow
	)
	err = r.e.inTx(ctx, func(tx store.Tx) error {
		var err error
		if run, err = r.e.store.GetRun(ctx, tx, id); err != nil {
			return err
		}
		if event, err = r.e.store.GetEvent(ctx, tx, run.TriggerEventID); err != nil {
			return err
		}
		steps, err = r.e.store.ListSteps(ctx, tx, id)
		return err
	})
	switch {
	case errors.Is(err, store.ErrNotFound):
		return RunTrace{}, fmt.Errorf("%w: run %s", ErrNotFound, runID)
	case err != nil:
		return RunTrace{}, fmt.Errorf("flowkit: trace run %s: %w", runID, err)
	}
	payload, err := decodeObject(event.Payload)
	if err != nil {
		return RunTrace{}, fmt.Errorf("flowkit: trace run %s: trigger event %d: %w", runID, event.ID, err)
	}
	tr := RunTrace{
		Run: summary(store.RunSummaryRow{
			RunID: run.ID, DefName: run.DefName, DefVersion: run.DefVersion, State: run.State,
			TerminalName: run.TerminalName, StepCount: run.StepSeq, TriggerEventType: event.Type,
			StartedAt: run.CreatedAt, EndedAt: endedAt(run),
		}),
		MetamodelVersion: run.MetamodelVersion,
		LibVersion:       run.LibVersion,
		TriggerEventID:   event.ID,
		TriggerEvent: Event{
			Type: event.Type, Payload: payload, DedupKey: deref(event.DedupKey), OccurredAt: event.OccurredAt,
		},
		Steps: make([]StepRecord, len(steps)),
	}
	for i, s := range steps {
		rec := StepRecord{
			Seq: s.Seq, At: s.At, StepID: s.StepID, Kind: s.Kind, Detail: deref(s.Detail), Error: deref(s.Error),
		}
		if s.Attempt != nil {
			rec.Attempt = *s.Attempt
		}
		if len(s.Snapshot) > 0 {
			if rec.Snapshot, err = decodeObject(s.Snapshot); err != nil {
				return RunTrace{}, fmt.Errorf("flowkit: trace run %s: step %d: %w", runID, s.Seq, err)
			}
		}
		tr.Steps[i] = rec
	}
	return tr, nil
}

func endedAt(run store.RunRow) *time.Time {
	if !run.Terminal {
		return nil
	}
	at := run.UpdatedAt
	return &at
}

// List returns one page of runs, live and summarised, newest first by
// start time. Pass the Cursor of the last run as RunFilter.After for the
// next page; a page shorter than the limit is the last. An invalid filter
// returns ValidationErrors.
func (r *Runs) List(ctx context.Context, f RunFilter) ([]RunSummary, error) {
	q, err := runListQuery(f)
	if err != nil {
		return nil, err
	}
	var rows []store.RunSummaryRow
	err = r.e.inTx(ctx, func(tx store.Tx) error {
		var err error
		rows, err = r.e.store.ListRuns(ctx, tx, q)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("flowkit: list runs: %w", err)
	}
	out := make([]RunSummary, len(rows))
	for i, row := range rows {
		out[i] = summary(row)
	}
	return out, nil
}

func runListQuery(f RunFilter) (store.RunListQuery, error) {
	var verrs ValidationErrors
	q := store.RunListQuery{Limit: f.Limit}
	if f.DefName != "" {
		q.DefName = &f.DefName
	}
	for i, s := range f.States {
		if !slices.Contains(runStates, s) {
			verrs = append(verrs, &ValidationError{
				Path: "/States/" + strconv.Itoa(i), Msg: fmt.Sprintf("unknown run state %q; want one of %s", s, strings.Join(runStates, ", ")),
			})
		}
	}
	q.States = f.States
	if f.EntityKind != "" {
		q.EntityKind = &f.EntityKind
	}
	if f.EntityID != "" {
		q.EntityID = &f.EntityID
	}
	if !f.From.IsZero() {
		q.StartedFrom = &f.From
	}
	if !f.To.IsZero() {
		q.StartedBefore = &f.To
	}
	switch {
	case f.Limit < 0 || f.Limit > maxRunPage:
		verrs = append(verrs, &ValidationError{Path: "/Limit", Msg: fmt.Sprintf("must be between 1 and %d, got %d", maxRunPage, f.Limit)})
	case f.Limit == 0:
		q.Limit = defaultRunPage
	}
	if f.After != "" {
		at, id, err := decodeCursor(f.After)
		if err != nil {
			verrs = append(verrs, &ValidationError{Path: "/After", Msg: err.Error()})
		} else {
			q.AfterAt, q.AfterID = &at, &id
		}
	}
	if len(verrs) > 0 {
		return store.RunListQuery{}, verrs
	}
	return q, nil
}

func summary(row store.RunSummaryRow) RunSummary {
	s := RunSummary{
		RunID:            row.RunID.String(),
		DefName:          row.DefName,
		DefVersion:       row.DefVersion,
		State:            row.State,
		TerminalName:     deref(row.TerminalName),
		TriggerEventType: row.TriggerEventType,
		StepCount:        row.StepCount,
		StartedAt:        row.StartedAt,
		Cursor:           encodeCursor(row.StartedAt, row.RunID),
	}
	if row.EndedAt != nil {
		s.EndedAt = *row.EndedAt
	}
	return s
}

// encodeCursor encodes the keyset position (started_at, run id).
func encodeCursor(at time.Time, id ids.UUID) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(at.UnixMicro(), 10) + "." + id.String()))
}

func decodeCursor(c string) (time.Time, ids.UUID, error) {
	raw, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return time.Time{}, ids.UUID{}, errors.New("not a cursor Runs.List returned")
	}
	micros, id, ok := strings.Cut(string(raw), ".")
	if !ok {
		return time.Time{}, ids.UUID{}, errors.New("not a cursor Runs.List returned")
	}
	us, err := strconv.ParseInt(micros, 10, 64)
	if err != nil {
		return time.Time{}, ids.UUID{}, errors.New("not a cursor Runs.List returned")
	}
	u, err := ids.Parse(id)
	if err != nil {
		return time.Time{}, ids.UUID{}, errors.New("not a cursor Runs.List returned")
	}
	return time.UnixMicro(us).UTC(), u, nil
}

// decodeObject decodes a stored JSON object, keeping numbers as
// json.Number.
func decodeObject(raw json.RawMessage) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("decode stored JSON: %w", err)
	}
	return m, nil
}

// Cancel ends a run cancelled, with terminal name $cancelled and reason
// recorded on its terminal step. Its subscriptions are deleted, so a later
// timeout of its wait records claim_lost, and the next run queued under its
// serial key starts. Cancelling a terminal run does nothing and returns nil;
// a run that does not exist returns ErrNotFound. A run that keeps changing
// under Cancel returns an error after three tries.
func (r *Runs) Cancel(ctx context.Context, runID string, reason string) error {
	id, err := ids.Parse(runID)
	if err != nil {
		return fmt.Errorf("%w: run %q", ErrNotFound, runID)
	}
	err = r.e.interp.Cancel(ctx, id, errtext.Sanitize(reason))
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrNotFound):
		return fmt.Errorf("%w: run %s", ErrNotFound, runID)
	}
	return fmt.Errorf("flowkit: cancel run %s: %w", runID, err)
}

// ResolveDeadLetter resolves an open dead letter, recording res.Actor and
// res.Reason. The resolution and the work it starts commit in one
// transaction, so of concurrent resolutions of one dead letter exactly one
// takes effect and the others return ErrAlreadyResolved.
//
// What each kind of dead letter accepts:
//
//   - effect_in_doubt: ResolveRedrive releases the effect and its action
//     runs again; ResolveMarkDone stores res.Output, which must match the
//     action's OutputFacts, as the action's output, and the run goes on;
//     ResolveSkip leaves the run blocked until Cancel. An effect that kept
//     the output of a late success, shown in the dead letter's note,
//     refuses ResolveRedrive: the action ran, so mark it done. The effect
//     of a run that has ended accepts only ResolveSkip. An effect that
//     retention removed returns ErrNotFound; resolve it with ResolveSkip.
//   - exhausted: the run already failed; only ResolveSkip is accepted.
//     To run the work again, emit a new event.
//   - schema_mismatch, unroutable_event: ResolveRedrive routes the event
//     again, or returns ErrNotFound when retention removed it;
//     ResolveSkip records.
//
// A refused resolution, or an output that does not match, returns
// ValidationErrors and leaves the dead letter open. A dead letter that
// does not exist returns ErrNotFound.
func (r *Runs) ResolveDeadLetter(ctx context.Context, id int64, res Resolution) error {
	var kind string
	switch res.Kind {
	case ResolveRedrive:
		kind = interpreter.ResolveRedrive
	case ResolveMarkDone:
		kind = interpreter.ResolveMarkDone
	case ResolveSkip:
		kind = interpreter.ResolveSkip
	default:
		return ValidationErrors{{Path: "/Kind", Msg: fmt.Sprintf("unknown resolution kind %d", int(res.Kind))}}
	}
	err := r.e.interp.ResolveDeadLetter(ctx, id, interpreter.Resolution{
		Kind: kind, Actor: res.Actor, Reason: res.Reason, Output: res.Output,
	})
	var invalid *interpreter.InvalidResolution
	switch {
	case err == nil:
		return nil
	case errors.Is(err, interpreter.ErrResolved):
		return fmt.Errorf("%w: dead letter %d", ErrAlreadyResolved, id)
	case errors.Is(err, store.ErrNotFound):
		return fmt.Errorf("%w: %w", ErrNotFound, err)
	case errors.As(err, &invalid):
		return ValidationErrors{{Path: resolutionPath(invalid.Path), Msg: invalid.Msg}}
	}
	return fmt.Errorf("flowkit: resolve dead letter %d: %w", id, err)
}

// resolutionPath maps the interpreter's path into a Resolution to the
// field names of Resolution.
func resolutionPath(p string) string {
	switch p {
	case "/kind":
		return "/Kind"
	case "/output":
		return "/Output"
	}
	return p
}
