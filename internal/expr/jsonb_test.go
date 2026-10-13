package expr

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/g0-up/flowkit/internal/testpg"
)

// TestJSONBRoundTrip stores payloads the way events are stored and checks
// that integer fields still compare equal to CEL ints after reading them
// back, and that a fractional value for an integer field is rejected.
func TestJSONBRoundTrip(t *testing.T) {
	pool, schemaName := testpg.New(t)
	ctx := context.Background()
	table := pgx.Identifier{schemaName, "payloads"}.Sanitize()
	if _, err := pool.Exec(ctx, "CREATE TABLE "+table+" (id int PRIMARY KEY, body jsonb NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	read := func(id int, body string) json.RawMessage {
		t.Helper()
		if _, err := pool.Exec(ctx, "INSERT INTO "+table+" (id, body) VALUES ($1, $2)", id, body); err != nil {
			t.Fatal(err)
		}
		var out []byte
		if err := pool.QueryRow(ctx, "SELECT body FROM "+table+" WHERE id = $1", id).Scan(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}

	shape := mustShape(t, orderSchema)
	env := testEnv(t)
	p := mustCompile(t, env, `event.qty == 3 && event.items.exists(i, i.qty == 2)`, WantBool)

	stored := read(1, `{"order_id": "o1", "qty": 3, "customer": {"id": "c1"}, "items": [{"sku": "A", "qty": 2.0}]}`)
	event, err := Activation(stored, shape)
	if err != nil {
		t.Fatalf("Activation(%s): %v", stored, err)
	}
	if !evalBool(t, p, Vars{Event: event}) {
		t.Fatalf("event.qty == 3 is false for %s", stored)
	}

	stored = read(2, `{"order_id": "o1", "qty": 3.5, "customer": {"id": "c1"}, "items": []}`)
	if _, err := Activation(stored, shape); err == nil || !strings.Contains(err.Error(), "/qty: must be an integer") {
		t.Fatalf("Activation(%s) = %v, want an integer error at /qty", stored, err)
	}
}
