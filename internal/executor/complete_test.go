package executor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/store"
)

func TestRecordingSuccess(t *testing.T) {
	for _, tc := range []struct {
		name string
		// fail runs before the nth try at recording the success; cancel
		// cancels the caller's context.
		fail      func(e *env, n int64, key string, token ids.UUID, cancel func()) error
		want      ExecKind
		wantTries int64
		wantDone  bool
	}{
		{
			name: "caller cancels while the success is recorded",
			fail: func(_ *env, _ int64, _ string, _ ids.UUID, cancel func()) error {
				cancel()
				return nil
			},
			want: Done, wantTries: 1, wantDone: true,
		},
		{
			name: "connection lost once",
			fail: func(_ *env, n int64, _ string, _ ids.UUID, _ func()) error {
				if n == 1 {
					return errors.New("connection reset by peer")
				}
				return nil
			},
			want: Done, wantTries: 2, wantDone: true,
		},
		{
			name: "commit landed but its reply was lost",
			fail: func(e *env, n int64, key string, token ids.UUID, _ func()) error {
				if n > 1 {
					return nil
				}
				e.inTx(func(tx store.Tx) {
					ok, err := e.store.CompleteEffect(context.Background(), tx, key, token, t0, json.RawMessage(`{}`), nil)
					if err != nil || !ok {
						e.t.Fatal(ok, err)
					}
				})
				return errors.New("unexpected EOF")
			},
			want: Done, wantTries: 2, wantDone: true,
		},
		{
			name: "connection lost on every try",
			fail: func(*env, int64, string, ids.UUID, func()) error { return errors.New("connection refused") },
			want: Errored, wantTries: completeTries,
		},
		{
			name: "statement rejected",
			fail: func(*env, int64, string, ids.UUID, func()) error {
				return &pgconn.PgError{Code: "22021", Message: "invalid byte sequence"}
			},
			want: Errored, wantTries: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			h := &hookStore{PgStore: e.store}
			h.beforeComplete = func(n int64, key string, token ids.UUID) error { return tc.fail(e, n, key, token, cancel) }
			ex := e.executor(func(o *Options) { o.Store = h })
			a, _ := countingAction("payments.charge", false, func(context.Context, int64) (map[string]any, error) {
				return nil, nil
			})

			out := ex.Run(ctx, e.request("k1", a))
			wantKind(t, out, tc.want)
			if got := h.completes.Load(); got != tc.wantTries {
				t.Fatalf("tries = %d; want %d", got, tc.wantTries)
			}
			row := e.effect("k1")
			if (row.DoneAt != nil) != tc.wantDone || row.InDoubt {
				t.Fatalf("effect = %+v; want done %v", row, tc.wantDone)
			}
			if !tc.wantDone && row.ClaimToken == nil {
				t.Fatalf("effect = %+v; want the claim kept until it expires", row)
			}
			if got := e.meter.count("flowkit.effect_orphaned"); got != 0 {
				t.Fatalf("effect_orphaned = %d; want 0", got)
			}
		})
	}
}

func TestTransient(t *testing.T) {
	for err, want := range map[error]bool{
		errors.New("unexpected EOF"):                    true,
		&pgconn.PgError{Code: "08006"}:                  true,
		&pgconn.PgError{Code: "40001"}:                  true,
		&pgconn.PgError{Code: "53300"}:                  true,
		&pgconn.PgError{Code: "57P01"}:                  true,
		&pgconn.PgError{Code: "22021"}:                  false,
		&pgconn.PgError{Code: "23505"}:                  false,
		context.Canceled:                                false,
		context.DeadlineExceeded:                        false,
		errors.Join(errors.New("x"), errActionDeadline): false,
	} {
		if got := transient(err); got != want {
			t.Errorf("transient(%v) = %v; want %v", err, got, want)
		}
	}
}
