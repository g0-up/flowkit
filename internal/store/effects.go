package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/store/db"
)

// The writes that end a claim (CompleteEffect, FailEffect, MarkEffectInDoubt,
// ReleaseEffectClaim) match the effect only while token still holds it: the
// claim token is token and the effect is neither done nor in doubt. A lost
// claim yields false rather than an error, so the transaction stays usable.

// EffectClaim is the first claim on an effect: the row executed_effects gets
// with attempt 0, claimed at At by Token.
type EffectClaim struct {
	IdemKey string
	RunID   ids.UUID
	StepID  string
	Action  string
	Token   ids.UUID
	At      time.Time
}

func (s *PgStore) GetEffect(ctx context.Context, tx Tx, idemKey string) (EffectRow, error) {
	q, err := s.q(tx)
	if err != nil {
		return EffectRow{}, err
	}
	r, err := q.GetEffect(ctx, idemKey)
	if err != nil {
		return EffectRow{}, wrapErr("get effect", err)
	}
	return effectRow(r), nil
}

// InsertEffectClaim inserts c and reports whether it did; false means an
// effect with that key exists.
func (s *PgStore) InsertEffectClaim(ctx context.Context, tx Tx, c EffectClaim) (bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return false, err
	}
	n, err := q.InsertEffectClaim(ctx, db.InsertEffectClaimParams{
		IdemKey:    c.IdemKey,
		RunID:      c.RunID,
		StepID:     c.StepID,
		Action:     c.Action,
		ClaimedAt:  &c.At,
		ClaimToken: &c.Token,
	})
	if err != nil {
		return false, wrapErr("insert effect claim", err)
	}
	return n == 1, nil
}

// ReclaimEffect claims an effect whose earlier claim was released. It reports
// false when the effect is claimed, done or in doubt, or has used up
// maxAttempts attempts.
func (s *PgStore) ReclaimEffect(ctx context.Context, tx Tx, idemKey string, token ids.UUID, at time.Time, maxAttempts int) (bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return false, err
	}
	max32, err := toInt32("max attempts", maxAttempts)
	if err != nil {
		return false, err
	}
	n, err := q.ReclaimEffect(ctx, db.ReclaimEffectParams{IdemKey: idemKey, ClaimToken: &token, ClaimedAt: &at, MaxAttempts: max32})
	if err != nil {
		return false, wrapErr("reclaim effect", err)
	}
	return n == 1, nil
}

// CompleteEffect marks the effect claimed by token done at at, storing result
// and lastError (nil for SQL NULL).
func (s *PgStore) CompleteEffect(ctx context.Context, tx Tx, idemKey string, token ids.UUID, at time.Time, result json.RawMessage, lastError *string) (bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return false, err
	}
	n, err := q.CompleteEffect(ctx, db.CompleteEffectParams{
		IdemKey:    idemKey,
		ClaimToken: &token,
		DoneAt:     &at,
		Result:     result,
		LastError:  lastError,
	})
	if err != nil {
		return false, wrapErr("complete effect", err)
	}
	return n == 1, nil
}

// FailEffect records a failed attempt of the effect claimed by token and
// releases the claim. It returns the number of failed attempts so far.
func (s *PgStore) FailEffect(ctx context.Context, tx Tx, idemKey string, token ids.UUID, lastError string) (attempt int, ok bool, err error) {
	q, err := s.q(tx)
	if err != nil {
		return 0, false, err
	}
	n, err := q.FailEffect(ctx, db.FailEffectParams{IdemKey: idemKey, ClaimToken: &token, LastError: &lastError})
	return claimWrite("fail effect", int(n), err)
}

// MarkEffectInDoubt marks the effect claimed by token in doubt and revokes
// the token. It returns the effect's attempt count, which it leaves as is.
func (s *PgStore) MarkEffectInDoubt(ctx context.Context, tx Tx, idemKey string, token ids.UUID, lastError string) (attempt int, ok bool, err error) {
	q, err := s.q(tx)
	if err != nil {
		return 0, false, err
	}
	n, err := q.MarkEffectInDoubt(ctx, db.MarkEffectInDoubtParams{IdemKey: idemKey, ClaimToken: &token, LastError: &lastError})
	return claimWrite("mark effect in doubt", int(n), err)
}

// ReleaseEffectClaim clears the claim held by token, so the effect can be
// claimed again; the attempt count is unchanged.
func (s *PgStore) ReleaseEffectClaim(ctx context.Context, tx Tx, idemKey string, token ids.UUID) (bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return false, err
	}
	n, err := q.ReleaseEffectClaim(ctx, db.ReleaseEffectClaimParams{IdemKey: idemKey, ClaimToken: &token})
	if err != nil {
		return false, wrapErr("release effect claim", err)
	}
	return n == 1, nil
}

// ListExpiredClaims returns up to limit effects claimed by a token before
// before and neither done nor in doubt, oldest claim first.
func (s *PgStore) ListExpiredClaims(ctx context.Context, tx Tx, before time.Time, limit int) ([]EffectRow, error) {
	q, err := s.q(tx)
	if err != nil {
		return nil, err
	}
	limit32, err := toInt32("limit", limit)
	if err != nil {
		return nil, err
	}
	rows, err := q.ListExpiredClaims(ctx, db.ListExpiredClaimsParams{ClaimedBefore: &before, MaxRows: limit32})
	if err != nil {
		return nil, wrapErr("list expired claims", err)
	}
	out := make([]EffectRow, len(rows))
	for i, r := range rows {
		out[i] = effectRow(r)
	}
	return out, nil
}

// SetInDoubtEffectResult stores result on an in-doubt effect that is not done
// and has no result yet, leaving it in doubt. It reports false when there is
// no such effect.
func (s *PgStore) SetInDoubtEffectResult(ctx context.Context, tx Tx, idemKey string, result json.RawMessage) (bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return false, err
	}
	n, err := q.SetInDoubtEffectResult(ctx, db.SetInDoubtEffectResultParams{IdemKey: idemKey, Result: result})
	if err != nil {
		return false, wrapErr("set in-doubt effect result", err)
	}
	return n == 1, nil
}

// AppendInDoubtEffectNote adds note as a line to the note of the open
// effect_in_doubt dead letter of idemKey. It reports false when there is no
// open one.
func (s *PgStore) AppendInDoubtEffectNote(ctx context.Context, tx Tx, idemKey, note string) (bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return false, err
	}
	n, err := q.AppendInDoubtEffectNote(ctx, db.AppendInDoubtEffectNoteParams{IdemKey: idemKey, Note: note})
	if err != nil {
		return false, wrapErr("append in-doubt effect note", err)
	}
	return n == 1, nil
}

// claimWrite turns the result of an UPDATE ... RETURNING attempt into
// (attempt, ok, err), where no row means the claim was lost.
func claimWrite(op string, attempt int, err error) (int, bool, error) {
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, wrapErr(op, err)
	}
	return attempt, true, nil
}

func effectRow(r db.ExecutedEffect) EffectRow {
	return EffectRow{
		IdemKey:    r.IdemKey,
		RunID:      r.RunID,
		StepID:     r.StepID,
		Action:     r.Action,
		Attempt:    int(r.Attempt),
		ClaimedAt:  r.ClaimedAt,
		ClaimToken: r.ClaimToken,
		DoneAt:     r.DoneAt,
		Result:     r.Result,
		LastError:  r.LastError,
		InDoubt:    r.InDoubt,
	}
}

func (s *PgStore) RedriveInDoubtEffect(ctx context.Context, tx Tx, idemKey string) (bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return false, err
	}
	n, err := q.RedriveInDoubtEffect(ctx, idemKey)
	if err != nil {
		return false, wrapErr("redrive in-doubt effect", err)
	}
	return n == 1, nil
}

func (s *PgStore) MarkInDoubtEffectDone(ctx context.Context, tx Tx, idemKey string, at time.Time, result json.RawMessage) (bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return false, err
	}
	n, err := q.MarkInDoubtEffectDone(ctx, db.MarkInDoubtEffectDoneParams{IdemKey: idemKey, DoneAt: &at, Result: result})
	if err != nil {
		return false, wrapErr("mark in-doubt effect done", err)
	}
	return n == 1, nil
}
