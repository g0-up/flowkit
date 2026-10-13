package migrate

import (
	"context"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/riverqueue/river/rivermigrate"

	"github.com/g0-up/flowkit/internal/testpg"
)

var updateAllowlist = flag.Bool("update-allowlist", false,
	"rewrite the River allowlist from a schema holding only River's migrations; the result must be re-signed")

// TestRiverObjectsMatchAllowlist applies only River's migrations to a fresh
// schema and checks that the objects River leaves there are exactly the
// signed allowlist. With -update-allowlist it rewrites the allowlist instead,
// which changes the signed hash: the new file must be signed again before
// TestEmbeddedAllowlistIsTheSignedFile can pass.
func TestRiverObjectsMatchAllowlist(t *testing.T) {
	pool, schema := testpg.New(t)
	ctx := context.Background()
	m, err := newRiverMigrator(pool, schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	pending, _, err := riverPendingVersions(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		t.Fatal(err)
	}
	objects, err := dumpObjects(ctx, pool, schema)
	if err != nil {
		t.Fatal(err)
	}

	if *updateAllowlist {
		var pgMajor string
		if err := pool.QueryRow(ctx, "SELECT (current_setting('server_version_num')::int / 10000)::text").Scan(&pgMajor); err != nil {
			t.Fatal(err)
		}
		versions := make([]string, len(pending))
		for i, v := range pending {
			versions[i] = fmt.Sprintf("%03d", v.Version)
		}
		var b strings.Builder
		fmt.Fprintf(&b, "# Database objects River %s leaves in its schema after rivermigrate (line main).\n", RiverAllowlistVersion)
		fmt.Fprintf(&b, "# Applied migration versions: %s.\n", strings.Join(versions, ", "))
		fmt.Fprintf(&b, "# Captured from the PostgreSQL %s system catalogs with search_path set to the\n", pgMajor)
		b.WriteString("# River schema, so names are unqualified and independent of the schema name.\n")
		b.WriteString("# Regenerate: make pg-up, then\n")
		b.WriteString("#   go test ./internal/migrate -run TestRiverObjectsMatchAllowlist -update-allowlist\n")
		for _, o := range objects {
			b.WriteString(o)
			b.WriteByte('\n')
		}
		if err := os.WriteFile(signedAllowlistPath, []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s (%d objects); copy it into testdata, update RiverAllowlistVersion and have it signed",
			signedAllowlistPath, len(objects))
		return
	}

	allowlist := allowedRiverObjects()
	for _, o := range objects {
		if !slices.Contains(allowlist, o) {
			t.Errorf("River created an object missing from the allowlist: %s", o)
		}
	}
	for _, a := range allowlist {
		if !slices.Contains(objects, a) {
			t.Errorf("allowlisted object not created by River: %s", a)
		}
	}
}
