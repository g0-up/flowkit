package migrate

import (
	_ "embed"
)

// RiverAllowlistVersion is the River release the embedded allowlist was
// captured from and signed for.
const RiverAllowlistVersion = "v0.49.0"

// riverAllowlist lists every database object River's migrations create in a
// schema. It is a byte-for-byte copy of the signed file
// migrate/testdata/river-objects-v0.49.0.txt; a test keeps the two equal and
// checks the signed hash. Upgrading River means regenerating and re-signing
// it.
//
//go:embed testdata/river-objects-v0.49.0.txt
var riverAllowlist string

// goldenObjects lists every object a fully migrated schema holds at
// LibSchema: flowkit's own objects plus the River allowlist.
//
//go:embed testdata/db-objects.golden
var goldenObjects string

// allowedRiverObjects returns the allowlisted River objects.
func allowedRiverObjects() []string { return parseListing(riverAllowlist) }

// expectedObjects returns the golden listing of a migrated schema.
func expectedObjects() []string { return parseListing(goldenObjects) }
