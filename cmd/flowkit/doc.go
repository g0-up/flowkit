// Command flowkit is the operator CLI for flowkit.
//
//	flowkit migrate --schema <name> [--yes] [--dsn <connection string>]
//	flowkit validate <file>
//
// migrate applies River's and flowkit's migrations to the schema. The
// connection string comes from --dsn, then FLOWKIT_DATABASE_URL, then libpq's
// PG* environment variables and .pgpass. --dsn must not contain a password.
// On a schema that already holds data, migrate asks for --yes, to be given
// after a backup (pg_dump -n <schema>).
//
// validate checks the structure of a definition body without a registry or a
// database: the metamodel schema of its metamodel_version, step ids, that
// every transition names a step, that every step is reachable and reaches an
// end step, and the syntax of every expression, which is parsed but not
// type-checked. Each problem is printed to stderr as "<file>: <JSON Pointer>:
// <message>" and the exit status is 1; a valid structure exits 0. Either way
// the last line says that full validation (events, actions, facts, params and
// expression types) is Registry.ValidateDefinition with the application's
// registry. Usage errors exit 2.
package main
