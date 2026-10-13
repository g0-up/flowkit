package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/g0-up/flowkit/internal/testpg"
)

var updateGolden = flag.Bool("update", false, "rewrite testdata/db-objects.golden from a freshly migrated schema")

const (
	goldenPath = "testdata/db-objects.golden"
	// signedAllowlistPath is the allowlist file the Owner signed; the
	// embedded copy in testdata must stay byte-identical to it.
	signedAllowlistPath   = "../../migrate/testdata/river-objects-v0.49.0.txt"
	signedAllowlistSHA256 = "5008d7b41ca7307958cefcf7e49b70c51af51ff5be09905afeea8c116d2a803e"
)

func TestEmbeddedAllowlistIsTheSignedFile(t *testing.T) {
	signed, err := os.ReadFile(signedAllowlistPath)
	if err != nil {
		t.Fatal(err)
	}
	if sum := sha256.Sum256(signed); hex.EncodeToString(sum[:]) != signedAllowlistSHA256 {
		t.Fatalf("%s has SHA-256 %x, signed hash is %s", signedAllowlistPath, sum, signedAllowlistSHA256)
	}
	if string(signed) != riverAllowlist {
		t.Fatalf("embedded allowlist differs from %s; copy the signed file into testdata", signedAllowlistPath)
	}
	if !strings.Contains(riverAllowlist, "River "+RiverAllowlistVersion+" ") {
		t.Fatalf("allowlist header does not name River %s", RiverAllowlistVersion)
	}
	if !strings.HasPrefix(RiverAllowlistVersion, RequiredRiverPrefix) {
		t.Fatalf("allowlist version %s is outside the required River line %s", RiverAllowlistVersion, RequiredRiverPrefix)
	}
}

// TestDBObjects migrates a fresh schema and lists every object in it. Every
// function, procedure, trigger, rule or custom type must be on the signed
// River allowlist, every allowlisted object must be present verbatim, and the
// full listing must match the golden file. Run with -update to regenerate the
// golden after an intended schema change.
func TestDBObjects(t *testing.T) {
	pool, schema := testpg.New(t)
	ctx := context.Background()
	if err := Run(ctx, pool, schema, Options{}); err != nil {
		t.Fatal(err)
	}
	objects, err := dumpObjects(ctx, pool, schema)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range objects {
		if strings.Contains(o, schema) {
			t.Errorf("listing still names the schema %s: %s", schema, o)
		}
	}

	allowlist := allowedRiverObjects()
	for _, a := range allowlist {
		if !slices.Contains(objects, a) {
			t.Errorf("allowlisted River object missing or changed: %s", a)
		}
	}
	var flowkitObjects []string
	for _, o := range objects {
		if slices.Contains(allowlist, o) {
			continue
		}
		flowkitObjects = append(flowkitObjects, o)
		if isForbiddenObject(o) {
			t.Errorf("object not created by River and not allowed for flowkit: %s", o)
		}
	}
	if len(flowkitObjects) == 0 {
		t.Fatal("no flowkit objects listed")
	}
	if problems := compareObjects(objects, objects, allowlist, false); len(problems) > 0 {
		t.Fatalf("listing fails its own runtime comparison: %v", problems)
	}

	var pgMajor string
	if err := pool.QueryRow(ctx, "SELECT (current_setting('server_version_num')::int / 10000)::text").Scan(&pgMajor); err != nil {
		t.Fatal(err)
	}
	lib, err := LibSchema()
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# Every database object in a schema migrated to flowkit library schema %d:\n", lib)
	fmt.Fprintf(&b, "# flowkit's own objects plus the signed River %s allowlist.\n", RiverAllowlistVersion)
	fmt.Fprintf(&b, "# Captured from the PostgreSQL %s system catalogs; names are unqualified.\n", pgMajor)
	b.WriteString("# Regenerate: make pg-up, then\n")
	b.WriteString("#   go test ./internal/migrate -run TestDBObjects -update\n")
	for _, o := range objects {
		b.WriteString(o)
		b.WriteByte('\n')
	}
	got := b.String()
	if *updateGolden {
		if err := os.WriteFile(goldenPath, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s (%d objects, %d from flowkit)", goldenPath, len(objects), len(flowkitObjects))
		return
	}
	if got != goldenObjects {
		t.Fatalf("schema objects differ from %s (run with -update after an intended change)\n--- got ---\n%s", goldenPath, got)
	}
}

func TestDumpObjectsOfMissingSchemaIsEmpty(t *testing.T) {
	pool := testpg.Pool(t)
	name, err := testpg.NewSchemaName()
	if err != nil {
		t.Fatal(err)
	}
	objects, err := dumpObjects(context.Background(), pool, name)
	if err != nil || len(objects) != 0 {
		t.Fatalf("dumpObjects of a missing schema = %v, %v", objects, err)
	}
}

func TestCompareObjects(t *testing.T) {
	allow := []string{
		"table river_job",
		"function river_f(integer) returns integer language=sql volatility=i body_md5=abc",
		"column_default river_job.created_at: now()",
	}
	golden := append(slices.Clone(allow), "table runs", "index runs_pkey: CREATE UNIQUE INDEX runs_pkey ON runs USING btree (id)")

	if p := compareObjects(golden, golden, allow, false); len(p) != 0 {
		t.Fatalf("identical listings: %v", p)
	}
	// Definitions may be formatted differently by another server version.
	reformatted := slices.Clone(golden)
	reformatted[len(reformatted)-1] = "index runs_pkey: CREATE UNIQUE INDEX runs_pkey ON runs (id)"
	if p := compareObjects(reformatted, golden, allow, false); len(p) != 0 {
		t.Fatalf("definition text differences reported: %v", p)
	}

	changedBody := slices.Clone(golden)
	changedBody[1] = strings.Replace(changedBody[1], "body_md5=abc", "body_md5=def", 1)
	cases := []struct {
		name   string
		got    []string
		newer  bool
		expect string
	}{
		{"missing table", golden[:len(golden)-2], false, "missing table runs"},
		{"extra table", append(slices.Clone(golden), "table extra"), false, "unexpected table extra"},
		{"extra function", append(slices.Clone(golden), "function f() returns void language=plpgsql volatility=v body_md5=x"), true, "object not on the River allowlist: function f()"},
		{"extra trigger", append(slices.Clone(golden), "trigger runs.t: CREATE TRIGGER t"), true, "object not on the River allowlist: trigger runs.t"},
		{"extra enum", append(slices.Clone(golden), "enum state: a,b"), true, "object not on the River allowlist: enum state"},
		{"function default", append(slices.Clone(golden), "column_default runs.created_at: now()"), true, "object not on the River allowlist: column_default runs.created_at"},
		{"current_timestamp default", append(slices.Clone(golden), "column_default runs.created_at: CURRENT_TIMESTAMP"), true, "object not on the River allowlist: column_default runs.created_at"},
		{"localtimestamp default", append(slices.Clone(golden), "column_default runs.created_at: LOCALTIMESTAMP"), true, "object not on the River allowlist: column_default runs.created_at"},
		{"current_date default", append(slices.Clone(golden), "column_default runs.day: CURRENT_DATE"), true, "object not on the River allowlist: column_default runs.day"},
		{"literal default", append(slices.Clone(golden), "column_default runs.flag: false"), true, "object not on the River allowlist: column_default runs.flag"},
		{"changed River function body", changedBody, false, "missing function river_f"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := compareObjects(c.got, golden, allow, c.newer)
			if !slices.ContainsFunc(p, func(s string) bool { return strings.HasPrefix(s, c.expect) }) {
				t.Fatalf("problems %v, want one starting %q", p, c.expect)
			}
		})
	}

	// A schema migrated by a newer library may add plain objects.
	newer := append(slices.Clone(golden), "table added_later", "index added_later_pkey: CREATE UNIQUE INDEX added_later_pkey ON added_later USING btree (id)")
	if p := compareObjects(newer, golden, allow, true); len(p) != 0 {
		t.Fatalf("newer schema with additive objects: %v", p)
	}
}
