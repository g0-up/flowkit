package sqllint

import (
	"reflect"
	"testing"
)

func rules(fs []Finding) []string {
	var out []string
	for _, f := range fs {
		out = append(out, f.Rule)
	}
	return out
}

func TestCheckDDLRules(t *testing.T) {
	cases := []struct {
		sql  string
		want []string
	}{
		{"CREATE TABLE t (id int PRIMARY KEY)", nil},
		{"create or replace function f() returns int as $$ select 1 $$ language sql", []string{"function, procedure, trigger or rule"}},
		{"CREATE PROCEDURE p() LANGUAGE sql AS 'select 1'", []string{"function, procedure, trigger or rule"}},
		{"CREATE CONSTRAINT TRIGGER t AFTER INSERT ON x", []string{"function, procedure, trigger or rule"}},
		{"CREATE RULE r AS ON INSERT TO x DO NOTHING", []string{"function, procedure, trigger or rule"}},
		{"CREATE TYPE state AS ENUM ('a')", []string{"enum, domain or other custom type"}},
		{"CREATE DOMAIN d AS int", []string{"enum, domain or other custom type"}},
		{"DO $$ BEGIN END $$", []string{"DO block"}},
		{"do language plpgsql $body$ begin end $body$", []string{"DO block"}},
		{"INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING", nil},
		{"INSERT INTO t VALUES (1) ON CONFLICT (id) DO UPDATE SET v = 1", nil},
		{"created_at timestamptz NOT NULL DEFAULT now()", []string{"default computed by a function"}},
		{"created_at timestamptz DEFAULT CURRENT_TIMESTAMP", []string{"default computed by a function"}},
		{"id uuid DEFAULT gen_random_uuid()", []string{"default computed by a function", "gen_random_uuid"}},
		{"n int DEFAULT my_seq_fn ()", []string{"default computed by a function"}},
		{"n int NOT NULL DEFAULT 0, j jsonb DEFAULT '{}'::jsonb", nil},
		{"run_id uuid REFERENCES runs(id) ON DELETE CASCADE", []string{"ON DELETE CASCADE"}},
		{"run_id uuid REFERENCES runs(id)", nil},
		{"CREATE INDEX CONCURRENTLY i ON t(x)", []string{"CONCURRENTLY"}},
		{"SELECT pg_advisory_xact_lock(1)", []string{"advisory lock"}},
		{"SELECT pg_try_advisory_lock(1)", []string{"advisory lock"}},
		{"CREATE TABLE t (id int) PARTITION BY RANGE (id)", []string{"partitioning"}},
		{"CREATE TABLE t1 PARTITION OF t FOR VALUES FROM (1) TO (2)", []string{"partitioning"}},
		// Comments and string literals are not code.
		{"-- never CREATE FUNCTION or DEFAULT now()\nSELECT 1", nil},
		{"/* ON DELETE CASCADE */ SELECT 'CONCURRENTLY', 'it''s DO $$'", nil},
	}
	for _, c := range cases {
		if got := rules(CheckDDLRules(c.sql)); !reflect.DeepEqual(got, c.want) {
			t.Errorf("CheckDDLRules(%q) = %v, want %v", c.sql, got, c.want)
		}
	}
}

func TestCheckTimeFunctions(t *testing.T) {
	cases := map[string]int{
		"UPDATE t SET at = now()":                     1,
		"UPDATE t SET at = NOW ( )":                   1,
		"SELECT current_timestamp, CURRENT_DATE":      2,
		"SELECT clock_timestamp(), localtimestamp":    2,
		"SELECT statement_timestamp()":                1,
		"UPDATE t SET at = @at, known_at = $2":        0,
		"SELECT nowhere, now_ms FROM t":               0,
		"-- now()\nSELECT 'current_timestamp' FROM t": 0,
	}
	for sql, want := range cases {
		if got := CheckTimeFunctions(sql); len(got) != want {
			t.Errorf("CheckTimeFunctions(%q) = %v, want %d findings", sql, got, want)
		}
	}
}

func TestTableNames(t *testing.T) {
	ddl := "-- CREATE TABLE ignored (x int);\n" +
		"CREATE TABLE IF NOT EXISTS " + Token + "flowkit_meta (k text);\n" +
		"create table " + Token + "Runs (id uuid);\n" +
		"CREATE UNIQUE INDEX i ON " + Token + "runs(id);"
	want := []string{"flowkit_meta", "runs"}
	if got := TableNames(ddl); !reflect.DeepEqual(got, want) {
		t.Fatalf("TableNames = %v, want %v", got, want)
	}
}

func TestCheckSchemaToken(t *testing.T) {
	tables := []string{"runs", "run_steps", "domain_events"}
	cases := []struct {
		sql  string
		want []string // offending names, in order
	}{
		{"SELECT id FROM " + Token + "runs WHERE id = $1", nil},
		{"SELECT id FROM " + Token + " runs r JOIN " + Token + "run_steps s ON s.run_id = r.id", nil},
		{"SELECT id FROM runs", []string{"runs"}},
		{"DELETE FROM " + Token + "run_steps WHERE run_id IN (SELECT id FROM runs)", []string{"runs"}},
		{"UPDATE " + Token + "runs SET rev = rev + 1 WHERE runs.id = $1", []string{"runs"}},
		{"CREATE INDEX i ON domain_events(type)", []string{"domain_events"}},
		{"run_id uuid REFERENCES " + Token + "runs(id), event_id bigint REFERENCES domain_events(id)", []string{"domain_events"}},
		// Unknown tables are caught after table keywords.
		{"SELECT * FROM river_job", []string{"river_job"}},
		{"INSERT INTO other (a) VALUES (1)", []string{"other"}},
		{"SELECT * FROM " + Token + "river_job", nil},
		// Not table references.
		{"INSERT INTO " + Token + "runs (id) SELECT unnest(@ids::uuid[])", nil},
		{"SELECT x FROM unnest($1::text[]) AS u(x)", nil},
		{"SELECT a IS DISTINCT FROM b FROM " + Token + "runs", nil},
		{"SELECT extract(epoch FROM created_at) FROM " + Token + "runs", nil},
		{"SELECT 1 FROM " + Token + "runs FOR UPDATE SKIP LOCKED", nil},
		{"INSERT INTO " + Token + "runs (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET rev = 2", nil},
		{"SELECT run_id, steps_runs FROM " + Token + "run_steps", nil},
		{"SELECT 'runs' FROM " + Token + "runs -- FROM runs", nil},
		{"SELECT * FROM " + Token + "runs WHERE id IN (SELECT run_id FROM run_steps)", []string{"run_steps"}},
	}
	for _, c := range cases {
		var got []string
		for _, f := range CheckSchemaToken(c.sql, tables) {
			got = append(got, f.Text)
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("CheckSchemaToken(%q) = %v, want %v", c.sql, got, c.want)
		}
	}
}

func TestFindingLine(t *testing.T) {
	sql := "SELECT 1;\n/* a\nmultiline comment */\nSELECT now();"
	got := CheckTimeFunctions(sql)
	if len(got) != 1 || got[0].Line != 4 {
		t.Fatalf("findings = %v, want one on line 4", got)
	}
}
