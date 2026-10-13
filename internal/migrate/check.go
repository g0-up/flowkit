package migrate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DriftError reports that a schema does not match what this build of flowkit
// expects. NeedsMigrate is true when running the migrations would fix it.
type DriftError struct {
	Schema       string
	NeedsMigrate bool
	Problems     []string
}

func (e *DriftError) Error() string {
	return fmt.Sprintf("flowkit: schema %q drift: %s", e.Schema, strings.Join(e.Problems, "; "))
}

// Check verifies that schema can serve this build: flowkit_meta exists and
// min_lib_schema <= LibSchema <= schema_version, River's migrations are all
// applied, and the database objects in the schema match the embedded listing,
// with every function, trigger, rule or custom type on the signed River
// allowlist. A mismatch returns a *DriftError; failing to read the schema
// returns a plain error.
func Check(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	if err := ValidateSchemaName(schema); err != nil {
		return err
	}
	if pool == nil {
		return errors.New("flowkit migrate: nil pool")
	}
	lib, err := LibSchema()
	if err != nil {
		return err
	}
	meta, err := readMeta(ctx, pool, schema)
	if err != nil {
		return err
	}
	drift := func(needsMigrate bool, problems ...string) error {
		return &DriftError{Schema: schema, NeedsMigrate: needsMigrate, Problems: problems}
	}
	switch {
	case !meta.Exists:
		return drift(true, "schema is not migrated (no flowkit_meta table)")
	case meta.SchemaVersion < lib:
		return drift(true, fmt.Sprintf("schema_version %d is below library schema %d", meta.SchemaVersion, lib))
	case meta.MinLibSchema > lib:
		return drift(false, fmt.Sprintf("schema needs a library with schema >= %d; this build has %d", meta.MinLibSchema, lib))
	}

	riverMigrator, err := newRiverMigrator(pool, schema, slog.New(slog.DiscardHandler))
	if err != nil {
		return err
	}
	res, err := riverMigrator.Validate(ctx, nil)
	if err != nil {
		return fmt.Errorf("flowkit migrate: validate River migrations in %q: %w", schema, err)
	}
	if !res.OK {
		return drift(true, "River: "+strings.Join(res.Messages, "; "))
	}

	got, err := dumpObjects(ctx, pool, schema)
	if err != nil {
		return err
	}
	if problems := compareObjects(got, expectedObjects(), allowedRiverObjects(), meta.SchemaVersion > lib); len(problems) > 0 {
		return drift(false, problems...)
	}
	return nil
}

// compareObjects compares a schema's object listing with the golden listing
// by object identity (kind and name, not definition text, which can vary
// across PostgreSQL major versions). An exact match is required, except that
// a schema migrated by a newer library (newerSchema) may hold extra objects.
// Independently, every object of a kind flowkit never creates must be on the
// River allowlist.
func compareObjects(got, golden, allowlist []string, newerSchema bool) []string {
	gotKeys := keySet(got)
	goldenKeys := keySet(golden)
	allowedKeys := keySet(allowlist)
	var problems []string
	for _, line := range golden {
		if !gotKeys[objectKey(line)] {
			problems = append(problems, "missing "+objectKey(line))
		}
	}
	for _, line := range got {
		key := objectKey(line)
		switch {
		case isForbiddenObject(line) && !allowedKeys[key]:
			problems = append(problems, "object not on the River allowlist: "+key)
		case !newerSchema && !goldenKeys[key]:
			problems = append(problems, "unexpected "+key)
		}
	}
	return problems
}

func keySet(lines []string) map[string]bool {
	m := make(map[string]bool, len(lines))
	for _, l := range lines {
		m[objectKey(l)] = true
	}
	return m
}
