package memstore

import (
	"cmp"
	"context"
	"encoding/json"
	"slices"
	"time"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/store"
)

func effectRow(e store.EffectRow) store.EffectRow {
	e.ClaimedAt = tsPtr(e.ClaimedAt)
	e.ClaimToken = cp(e.ClaimToken)
	e.DoneAt = tsPtr(e.DoneAt)
	e.Result = cpJSON(e.Result)
	e.LastError = cp(e.LastError)
	return e
}

func (s *Store) GetEffect(ctx context.Context, tx store.Tx, idemKey string) (store.EffectRow, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return store.EffectRow{}, err
	}
	e, ok := t.t.effects[idemKey]
	if !ok {
		return store.EffectRow{}, notFound("get effect")
	}
	return effectRow(e), nil
}

func (s *Store) InsertEffectClaim(ctx context.Context, tx store.Tx, c store.EffectClaim) (bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return false, err
	}
	if _, ok := t.t.effects[c.IdemKey]; ok {
		return false, nil
	}
	if _, ok := t.t.runs[c.RunID]; !ok {
		return false, t.fail("insert effect claim", foreignKeyErr("executed_effects_run_id_fkey"))
	}
	write(t, tabEffects, &t.t.effects)[c.IdemKey] = effectRow(store.EffectRow{
		IdemKey: c.IdemKey, RunID: c.RunID, StepID: c.StepID, Action: c.Action,
		ClaimedAt: &c.At, ClaimToken: &c.Token,
	})
	return true, nil
}

// updateEffect replaces the effect with the result of fn when it exists and
// match accepts it.
func (t *memTx) updateEffect(idemKey string, match func(store.EffectRow) bool, fn func(*store.EffectRow)) bool {
	e, ok := t.t.effects[idemKey]
	if !ok || !match(e) {
		return false
	}
	fn(&e)
	write(t, tabEffects, &t.t.effects)[idemKey] = effectRow(e)
	return true
}

// claimedBy reports whether the effect is unfinished, not in doubt and
// claimed with token.
func claimedBy(token ids.UUID) func(store.EffectRow) bool {
	return func(e store.EffectRow) bool {
		return e.ClaimToken != nil && *e.ClaimToken == token && e.DoneAt == nil && !e.InDoubt
	}
}

func (s *Store) ReclaimEffect(ctx context.Context, tx store.Tx, idemKey string, token ids.UUID, at time.Time, maxAttempts int) (bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return false, err
	}
	return t.updateEffect(idemKey, func(e store.EffectRow) bool {
		return e.ClaimedAt == nil && e.DoneAt == nil && !e.InDoubt && e.Attempt < maxAttempts
	}, func(e *store.EffectRow) {
		e.ClaimedAt, e.ClaimToken = &at, &token
	}), nil
}

func (s *Store) CompleteEffect(ctx context.Context, tx store.Tx, idemKey string, token ids.UUID, at time.Time, result json.RawMessage, lastError *string) (bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return false, err
	}
	if err := jsonCol("result", result, false); err != nil {
		return false, t.fail("complete effect", err)
	}
	return t.updateEffect(idemKey, claimedBy(token), func(e *store.EffectRow) {
		e.DoneAt, e.Result, e.LastError = &at, result, lastError
	}), nil
}

func (s *Store) FailEffect(ctx context.Context, tx store.Tx, idemKey string, token ids.UUID, lastError string) (int, bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return 0, false, err
	}
	var attempt int
	ok := t.updateEffect(idemKey, claimedBy(token), func(e *store.EffectRow) {
		e.Attempt++
		attempt = e.Attempt
		e.LastError, e.ClaimedAt, e.ClaimToken = &lastError, nil, nil
	})
	if !ok {
		return 0, false, nil
	}
	return attempt, true, nil
}

func (s *Store) MarkEffectInDoubt(ctx context.Context, tx store.Tx, idemKey string, token ids.UUID, lastError string) (int, bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return 0, false, err
	}
	var attempt int
	ok := t.updateEffect(idemKey, claimedBy(token), func(e *store.EffectRow) {
		attempt = e.Attempt
		e.InDoubt, e.LastError, e.ClaimToken = true, &lastError, nil
	})
	if !ok {
		return 0, false, nil
	}
	return attempt, true, nil
}

func (s *Store) ReleaseEffectClaim(ctx context.Context, tx store.Tx, idemKey string, token ids.UUID) (bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return false, err
	}
	return t.updateEffect(idemKey, claimedBy(token), func(e *store.EffectRow) {
		e.ClaimedAt, e.ClaimToken = nil, nil
	}), nil
}

func (s *Store) ListExpiredClaims(ctx context.Context, tx store.Tx, before time.Time, limit int) ([]store.EffectRow, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := t.limit("list expired claims", limit); err != nil {
		return nil, err
	}
	before = ts(before)
	out := []store.EffectRow{}
	for _, e := range t.t.effects {
		if e.ClaimedAt != nil && e.ClaimedAt.Before(before) && e.ClaimToken != nil && e.DoneAt == nil && !e.InDoubt {
			out = append(out, effectRow(e))
		}
	}
	slices.SortFunc(out, func(a, b store.EffectRow) int {
		return cmp.Or(a.ClaimedAt.Compare(*b.ClaimedAt), cmp.Compare(a.IdemKey, b.IdemKey))
	})
	return out[:min(len(out), limit)], nil
}

// unfinishedInDoubt reports whether the effect is in doubt and not done.
func unfinishedInDoubt(e store.EffectRow) bool { return e.InDoubt && e.DoneAt == nil }

func (s *Store) SetInDoubtEffectResult(ctx context.Context, tx store.Tx, idemKey string, result json.RawMessage) (bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return false, err
	}
	if err := jsonCol("result", result, false); err != nil {
		return false, t.fail("set in-doubt effect result", err)
	}
	return t.updateEffect(idemKey, func(e store.EffectRow) bool {
		return unfinishedInDoubt(e) && e.Result == nil
	}, func(e *store.EffectRow) {
		e.Result = result
	}), nil
}

func (s *Store) RedriveInDoubtEffect(ctx context.Context, tx store.Tx, idemKey string) (bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return false, err
	}
	return t.updateEffect(idemKey, func(e store.EffectRow) bool {
		return unfinishedInDoubt(e) && e.Result == nil
	}, func(e *store.EffectRow) {
		e.InDoubt, e.ClaimedAt, e.ClaimToken = false, nil, nil
	}), nil
}

func (s *Store) MarkInDoubtEffectDone(ctx context.Context, tx store.Tx, idemKey string, at time.Time, result json.RawMessage) (bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return false, err
	}
	if err := jsonCol("result", result, false); err != nil {
		return false, t.fail("mark in-doubt effect done", err)
	}
	return t.updateEffect(idemKey, unfinishedInDoubt, func(e *store.EffectRow) {
		e.DoneAt, e.Result = &at, result
	}), nil
}

// deadLetterKinds and resolutions are the values the dead_letters CHECK
// constraints allow.
var (
	deadLetterKinds = []string{"unroutable_event", "schema_mismatch", "effect_in_doubt", "exhausted"}
	resolutions     = []string{"redrive", "mark_done", "skip"}
)

func deadLetterRow(d store.DeadLetterRow) store.DeadLetterRow {
	d.RunID = cp(d.RunID)
	d.EventID = cp(d.EventID)
	d.Raw = cpJSON(d.Raw)
	d.Note = cp(d.Note)
	d.CreatedAt = ts(d.CreatedAt)
	d.ResolvedAt = tsPtr(d.ResolvedAt)
	d.Resolution = cp(d.Resolution)
	d.ResolvedBy = cp(d.ResolvedBy)
	d.ResolveReason = cp(d.ResolveReason)
	return d
}

// openDeadLetter returns the unresolved dead letter of (kind, ref), the key
// of the dl_open_ref unique index.
func (t *memTx) openDeadLetter(kind, ref string) (store.DeadLetterRow, bool) {
	for _, d := range t.t.deadLetters {
		if d.Kind == kind && d.Ref == ref && d.ResolvedAt == nil {
			return d, true
		}
	}
	return store.DeadLetterRow{}, false
}

func (s *Store) InsertDeadLetter(ctx context.Context, tx store.Tx, d store.DeadLetterRow) (int64, bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return 0, false, err
	}
	if err := jsonCol("raw", d.Raw, true); err != nil {
		return 0, false, t.fail("insert dead letter", err)
	}
	if !slices.Contains(deadLetterKinds, d.Kind) {
		return 0, false, t.fail("insert dead letter", checkErr("dead_letters_kind_check"))
	}
	if open, ok := t.openDeadLetter(d.Kind, d.Ref); ok {
		return open.ID, false, nil
	}
	s.lastDeadLetterID++
	row := store.DeadLetterRow{
		ID: s.lastDeadLetterID, Kind: d.Kind, Ref: d.Ref, RunID: d.RunID, EventID: d.EventID,
		Raw: d.Raw, Note: d.Note, CreatedAt: d.CreatedAt,
	}
	write(t, tabDeadLetters, &t.t.deadLetters)[row.ID] = deadLetterRow(row)
	return row.ID, true, nil
}

func (s *Store) GetDeadLetter(ctx context.Context, tx store.Tx, id int64) (store.DeadLetterRow, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return store.DeadLetterRow{}, err
	}
	d, ok := t.t.deadLetters[id]
	if !ok {
		return store.DeadLetterRow{}, notFound("get dead letter")
	}
	return deadLetterRow(d), nil
}

func (s *Store) ClaimDeadLetter(ctx context.Context, tx store.Tx, c store.DeadLetterClaim) (store.DeadLetterRow, bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return store.DeadLetterRow{}, false, err
	}
	d, ok := t.t.deadLetters[c.ID]
	if !ok || d.ResolvedAt != nil {
		return store.DeadLetterRow{}, false, nil
	}
	if !slices.Contains(resolutions, c.Resolution) {
		return store.DeadLetterRow{}, false, t.fail("claim dead letter", checkErr("dead_letters_resolution_check"))
	}
	resolution := c.Resolution
	d.ResolvedAt, d.Resolution, d.ResolvedBy, d.ResolveReason = &c.At, &resolution, c.ResolvedBy, c.Reason
	d = deadLetterRow(d)
	write(t, tabDeadLetters, &t.t.deadLetters)[d.ID] = d
	return deadLetterRow(d), true, nil
}

func (s *Store) AppendInDoubtEffectNote(ctx context.Context, tx store.Tx, idemKey, note string) (bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return false, err
	}
	d, ok := t.openDeadLetter("effect_in_doubt", idemKey)
	if !ok {
		return false, nil
	}
	// concat_ws(E'\n', nullif(note, ''), @note)
	joined := note
	if d.Note != nil && *d.Note != "" {
		joined = *d.Note + "\n" + note
	}
	d.Note = &joined
	write(t, tabDeadLetters, &t.t.deadLetters)[d.ID] = d
	return true, nil
}

func (s *Store) InsertRateLimit(ctx context.Context, tx store.Tx, r store.RateLimitRow) error {
	t, err := s.use(ctx, tx)
	if err != nil {
		return err
	}
	if _, ok := t.t.rateLimits[r.DefName]; !ok {
		r.RefilledAt = ts(r.RefilledAt)
		write(t, tabRateLimits, &t.t.rateLimits)[r.DefName] = r
	}
	return nil
}

func (s *Store) LockRateLimit(ctx context.Context, tx store.Tx, defName string) (store.RateLimitRow, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return store.RateLimitRow{}, err
	}
	r, ok := t.t.rateLimits[defName]
	if !ok {
		return store.RateLimitRow{}, notFound("lock rate limit")
	}
	return r, nil
}

func (s *Store) UpdateRateLimit(ctx context.Context, tx store.Tx, r store.RateLimitRow) error {
	t, err := s.use(ctx, tx)
	if err != nil {
		return err
	}
	if _, ok := t.t.rateLimits[r.DefName]; ok {
		r.RefilledAt = ts(r.RefilledAt)
		write(t, tabRateLimits, &t.t.rateLimits)[r.DefName] = r
	}
	return nil
}
