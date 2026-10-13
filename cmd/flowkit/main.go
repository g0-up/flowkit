package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/g0-up/flowkit/internal/migrate"
)

// Exit codes.
const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2
)

const usage = `usage: flowkit <command> [flags]

commands:
  migrate   apply River and flowkit migrations to a schema
  validate  check the structure of a definition file

run "flowkit <command> -h" for the flags of a command
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr, os.Getenv)
	stop()
	os.Exit(code)
}

// run executes the command line args and returns the exit code.
func run(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return exitUsage
	}
	switch args[0] {
	case "migrate":
		return runMigrate(ctx, args[1:], stdout, stderr, getenv)
	case "validate":
		return runValidate(args[1:], stdout, stderr)
	case "help", "-h", "-help", "--help":
		fmt.Fprint(stdout, usage)
		return exitOK
	default:
		fmt.Fprintf(stderr, "flowkit: unknown command %q\n\n%s", args[0], usage)
		return exitUsage
	}
}

func runMigrate(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	schema := fs.String("schema", "", "schema to migrate (required)")
	yes := fs.Bool("yes", false, "migrate a schema that already holds data; take a backup first (pg_dump -n <schema>)")
	dsnFlag := fs.String("dsn", "", "connection string without a password (default: "+dsnEnvVar+", then libpq PG* variables)")
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), "usage: flowkit migrate --schema <name> [--yes] [--dsn <connection string>]\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "flowkit migrate: unexpected argument %q\n", fs.Arg(0))
		return exitUsage
	}
	if *schema == "" {
		fmt.Fprintln(stderr, "flowkit migrate: --schema is required")
		return exitUsage
	}
	if err := migrate.ValidateSchemaName(*schema); err != nil {
		fmt.Fprintf(stderr, "flowkit migrate: %v\n", err)
		return exitUsage
	}
	dsn, source, err := resolveDSN(*dsnFlag, getenv)
	if err != nil {
		fmt.Fprintf(stderr, "flowkit migrate: %v\n", err)
		return exitUsage
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		// The parse error quotes the connection string; report only its source.
		fmt.Fprintf(stderr, "flowkit migrate: cannot parse the connection string from %s\n", source)
		return exitUsage
	}
	// One connection holds the migration lock while another runs migrations.
	cfg.MaxConns = max(cfg.MaxConns, 2)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		fmt.Fprintf(stderr, "flowkit migrate: open pool (connection from %s): %v\n", source, connectErr(err))
		return exitError
	}
	defer pool.Close()

	before, err := migrate.Status(ctx, pool, *schema)
	if err != nil {
		fmt.Fprintf(stderr, "flowkit migrate: read status (connection from %s): %v\n", source, connectErr(err))
		return exitError
	}
	if len(before.Pending) == 0 {
		fmt.Fprintf(stdout, "schema %q is up to date: flowkit schema %d, River migration %d\n",
			*schema, before.SchemaVersion, before.RiverVersion)
		return exitOK
	}
	fmt.Fprintf(stdout, "schema %q: flowkit schema %d, River migration %d; pending: %s\n",
		*schema, before.SchemaVersion, before.RiverVersion, strings.Join(before.Pending, ", "))
	fmt.Fprintf(stderr, "flowkit migrate: back up the schema before migrating a database that holds data:\n"+
		"  pg_dump -n %s -Fc -f %s.dump\n", *schema, *schema)

	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	err = migrate.Run(ctx, pool, *schema, migrate.Options{ConfirmNonEmpty: *yes, Logger: logger})
	switch {
	case errors.Is(err, migrate.ErrConfirmationRequired):
		fmt.Fprintf(stderr, "flowkit migrate: schema %q already holds data. Back it up, then rerun with --yes.\n", *schema)
		return exitError
	case err != nil:
		fmt.Fprintf(stderr, "flowkit migrate: %v\n", err)
		return exitError
	}

	after, err := migrate.Status(ctx, pool, *schema)
	if err != nil {
		fmt.Fprintf(stderr, "flowkit migrate: read status after migrating: %v\n", err)
		return exitError
	}
	fmt.Fprintf(stdout, "schema %q migrated: flowkit schema %d, River migration %d\n",
		*schema, after.SchemaVersion, after.RiverVersion)
	return exitOK
}

// connectErr drops the connection string from parse errors that pgx reports
// lazily; other errors name at most the user, database and host.
func connectErr(err error) error {
	var pe *pgconn.ParseConfigError
	if errors.As(err, &pe) {
		return errors.New("cannot parse the connection string")
	}
	return err
}
