package flowkit

import "github.com/jackc/pgx/v5/pgxpool"

// EngineDB returns the pool and schema an Engine runs on, for the external
// tests of this package that inspect the schema an Engine used, such as one
// a flowkittest.PGHarness created. It exists only in the test binary.
func EngineDB(e *Engine) (*pgxpool.Pool, string) { return e.cfg.Pool, e.cfg.Schema }
