package store

import (
	"context"
	"time"

	"github.com/g0-up/flowkit/internal/store/db"
)

// RateLimitRow is a row of rate_limits: the token bucket of one definition.
type RateLimitRow struct {
	DefName    string
	Tokens     float64
	RefilledAt time.Time
}

// InsertRateLimit creates the bucket of r.DefName with r's values unless it
// exists.
func (s *PgStore) InsertRateLimit(ctx context.Context, tx Tx, r RateLimitRow) error {
	q, err := s.q(tx)
	if err != nil {
		return err
	}
	err = q.InsertRateLimit(ctx, db.InsertRateLimitParams{DefName: r.DefName, Tokens: r.Tokens, RefilledAt: r.RefilledAt})
	if err != nil {
		return wrapErr("insert rate limit", err)
	}
	return nil
}

// LockRateLimit reads the bucket of defName and locks it until tx ends.
func (s *PgStore) LockRateLimit(ctx context.Context, tx Tx, defName string) (RateLimitRow, error) {
	q, err := s.q(tx)
	if err != nil {
		return RateLimitRow{}, err
	}
	r, err := q.LockRateLimit(ctx, defName)
	if err != nil {
		return RateLimitRow{}, wrapErr("lock rate limit", err)
	}
	return RateLimitRow{DefName: r.DefName, Tokens: r.Tokens, RefilledAt: r.RefilledAt}, nil
}

// UpdateRateLimit writes the tokens and refill time of r.DefName's bucket.
func (s *PgStore) UpdateRateLimit(ctx context.Context, tx Tx, r RateLimitRow) error {
	q, err := s.q(tx)
	if err != nil {
		return err
	}
	err = q.UpdateRateLimit(ctx, db.UpdateRateLimitParams{DefName: r.DefName, Tokens: r.Tokens, RefilledAt: r.RefilledAt})
	if err != nil {
		return wrapErr("update rate limit", err)
	}
	return nil
}
