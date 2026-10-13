package main

import (
	"bytes"
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/g0-up/flowkit/internal/migrate"
	"github.com/g0-up/flowkit/internal/testpg"
)

type result struct {
	code           int
	stdout, stderr string
}

func runCLI(getenv func(string) string, args ...string) result {
	var out, errb bytes.Buffer
	code := run(context.Background(), args, &out, &errb, getenv)
	return result{code, out.String(), errb.String()}
}

func noEnv(string) string { return "" }

func TestUsageErrors(t *testing.T) {
	cases := []struct {
		args []string
		code int
		msg  string
	}{
		{nil, exitUsage, "usage: flowkit"},
		{[]string{"nope"}, exitUsage, `unknown command "nope"`},
		{[]string{"help"}, exitOK, ""},
		{[]string{"migrate"}, exitUsage, "--schema is required"},
		{[]string{"migrate", "--schema", "Flow-Kit"}, exitUsage, "invalid schema name"},
		{[]string{"migrate", "--schema", "public"}, exitUsage, "reserved"},
		{[]string{"migrate", "--schema", "fk", "extra"}, exitUsage, `unexpected argument "extra"`},
		{[]string{"migrate", "--bogus"}, exitUsage, "flag provided but not defined"},
		{[]string{"migrate", "-h"}, exitOK, "usage: flowkit migrate"},
		{[]string{"migrate", "--schema", "fk", "--dsn", "postgres://app:hunter2@db/x"}, exitUsage, "must not contain a password"},
		{[]string{"migrate", "--schema", "fk", "--dsn", "host=db password=hunter2"}, exitUsage, "must not contain a password"},
		{[]string{"migrate", "--schema", "fk", "--dsn", "host=db port=notaport"}, exitUsage, "cannot parse the connection string from --dsn"},
	}
	for _, c := range cases {
		r := runCLI(noEnv, c.args...)
		if r.code != c.code {
			t.Errorf("%v: exit %d, want %d (stderr %q)", c.args, r.code, c.code, r.stderr)
		}
		if !strings.Contains(r.stdout+r.stderr, c.msg) {
			t.Errorf("%v: output %q lacks %q", c.args, r.stdout+r.stderr, c.msg)
		}
		if strings.Contains(r.stdout+r.stderr, "hunter2") {
			t.Errorf("%v: output repeats the password", c.args)
		}
	}
}

func TestMigrateEndToEnd(t *testing.T) {
	dsn := testpg.DSN(t)
	pool := testpg.Pool(t)
	schema, err := testpg.NewSchemaName()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Errorf("drop schema %s: %v", schema, err)
		}
	})
	env := func(k string) string {
		if k == dsnEnvVar {
			return dsn
		}
		return ""
	}
	secrets := []string{dsn}
	if u, err := url.Parse(dsn); err == nil && u.User != nil {
		if pw, ok := u.User.Password(); ok && pw != "" {
			secrets = append(secrets, ":"+pw+"@")
		}
	}
	checkNoDSN := func(r result) {
		t.Helper()
		for _, s := range secrets {
			if strings.Contains(r.stdout+r.stderr, s) {
				t.Errorf("output contains the connection string or password:\n%s%s", r.stdout, r.stderr)
			}
		}
	}

	first := runCLI(env, "migrate", "--schema", schema)
	checkNoDSN(first)
	if first.code != exitOK {
		t.Fatalf("first migrate: exit %d\nstdout: %s\nstderr: %s", first.code, first.stdout, first.stderr)
	}
	if !strings.Contains(first.stdout, "flowkit:0001_init") || !strings.Contains(first.stdout, "migrated") {
		t.Errorf("first migrate stdout = %q", first.stdout)
	}
	if !strings.Contains(first.stderr, "pg_dump -n "+schema) {
		t.Errorf("first migrate gave no backup hint: %q", first.stderr)
	}
	if err := migrate.Check(context.Background(), pool, schema); err != nil {
		t.Fatalf("Check after CLI migrate: %v", err)
	}

	again := runCLI(env, "migrate", "--schema", schema, "--yes")
	checkNoDSN(again)
	if again.code != exitOK || !strings.Contains(again.stdout, "up to date") {
		t.Fatalf("second migrate: exit %d\nstdout: %s\nstderr: %s", again.code, again.stdout, again.stderr)
	}
}

func TestMigrateReportsConnectionFailureWithoutDSN(t *testing.T) {
	// Port 1 on localhost refuses connections; the password must not leak.
	env := func(k string) string {
		if k == dsnEnvVar {
			return "postgres://app:hunter2@127.0.0.1:1/flowkit?connect_timeout=2"
		}
		return ""
	}
	r := runCLI(env, "migrate", "--schema", "fk_unreachable")
	if r.code != exitError {
		t.Fatalf("exit %d, want %d\nstderr: %s", r.code, exitError, r.stderr)
	}
	if strings.Contains(r.stdout+r.stderr, "hunter2") {
		t.Fatalf("output repeats the password: %s", r.stderr)
	}
	if !strings.Contains(r.stderr, dsnEnvVar) {
		t.Errorf("stderr does not name the connection source: %s", r.stderr)
	}
}
