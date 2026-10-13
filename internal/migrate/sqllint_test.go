package migrate

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/g0-up/flowkit/internal/sqllint"
)

// readSQLDir returns the .sql files of dir by name.
func readSQLDir(t *testing.T, dir string) map[string]string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatalf("no SQL files in %s", dir)
	}
	files := make(map[string]string, len(paths))
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		files[p] = string(data)
	}
	return files
}

func TestSQLLintTokenMatchesSchemaToken(t *testing.T) {
	if sqllint.Token != SchemaToken {
		t.Fatalf("sqllint.Token %q != SchemaToken %q", sqllint.Token, SchemaToken)
	}
}

// TestFlowkitSQLFollowsTheDatabaseRules scans every flowkit migration and
// sqlc query for server-side code, custom types, function defaults, database
// clock reads, cascading deletes, concurrent index builds, advisory locks,
// partitioning and table names without the schema token.
func TestFlowkitSQLFollowsTheDatabaseRules(t *testing.T) {
	migrations := readSQLDir(t, "sql")
	var tables []string
	for _, sql := range migrations {
		tables = append(tables, sqllint.TableNames(sql)...)
	}
	if len(tables) == 0 {
		t.Fatal("no CREATE TABLE found in the migrations")
	}
	files := readSQLDir(t, filepath.Join("..", "store", "queries"))
	for p, sql := range migrations {
		files[p] = sql
	}
	for path, sql := range files {
		for _, f := range sqllint.CheckDDLRules(sql) {
			t.Errorf("%s: %s", path, f)
		}
		for _, f := range sqllint.CheckTimeFunctions(sql) {
			t.Errorf("%s: %s", path, f)
		}
		for _, f := range sqllint.CheckSchemaToken(sql, tables) {
			t.Errorf("%s: %s", path, f)
		}
	}
}

// TestMigrationsLeaveTheLockTableAlone keeps migrations away from flowkit_meta
// apart from its creation: the runner holds a row lock there while migrations
// run on other connections, so altering the table or writing the lock row
// would wait on the runner itself. The runner records versions on its own.
func TestMigrationsLeaveTheLockTableAlone(t *testing.T) {
	lineComment := regexp.MustCompile(`--[^\n]*`)
	for path, sql := range readSQLDir(t, "sql") {
		code := strings.ReplaceAll(lineComment.ReplaceAllString(sql, ""), metaDDL, "")
		for _, name := range []string{"flowkit_meta", metaMigrateLock} {
			if strings.Contains(strings.ToLower(code), name) {
				t.Errorf("%s: references %s outside the flowkit_meta CREATE TABLE", path, name)
			}
		}
	}
}
