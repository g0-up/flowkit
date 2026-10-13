package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRateLimitRows(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	inTx(t, s, func(tx Tx) {
		if err := s.InsertRateLimit(ctx, tx, RateLimitRow{DefName: "d", Tokens: 10, RefilledAt: t0}); err != nil {
			t.Fatal(err)
		}
		if err := s.InsertRateLimit(ctx, tx, RateLimitRow{DefName: "d", Tokens: 3, RefilledAt: t0.Add(time.Hour)}); err != nil {
			t.Fatalf("InsertRateLimit of an existing bucket: %v", err)
		}
		got, err := s.LockRateLimit(ctx, tx, "d")
		if err != nil || got.DefName != "d" || got.Tokens != 10 || !got.RefilledAt.Equal(t0) {
			t.Fatalf("LockRateLimit = %+v, %v", got, err)
		}
		if err := s.UpdateRateLimit(ctx, tx, RateLimitRow{DefName: "d", Tokens: 2.5, RefilledAt: t0.Add(time.Second)}); err != nil {
			t.Fatal(err)
		}
	})
	inTx(t, s, func(tx Tx) {
		got, err := s.LockRateLimit(ctx, tx, "d")
		if err != nil || got.Tokens != 2.5 || !got.RefilledAt.Equal(t0.Add(time.Second)) {
			t.Fatalf("LockRateLimit after update = %+v, %v", got, err)
		}
		if _, err := s.LockRateLimit(ctx, tx, "missing"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("LockRateLimit of a missing bucket: %v", err)
		}
	})
}
