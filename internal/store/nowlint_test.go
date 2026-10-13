package store

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/g0-up/flowkit/internal/sqllint"
)

// sqlStatement recognises Go string literals that hold SQL. flowkit writes
// SQL keywords in upper case.
var sqlStatement = regexp.MustCompile(`\b(SELECT|INSERT INTO|UPDATE|DELETE FROM)\b`)

func flowkitTables(t *testing.T) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("..", "migrate", "sql", "*.sql"))
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		tables = append(tables, sqllint.TableNames(string(data))...)
	}
	if len(tables) == 0 {
		t.Fatal("no tables found in the migrations")
	}
	return tables
}

// storeSQL returns the queries and the SQL string literals of the store's Go
// code, including generated code, keyed by position.
func storeSQL(t *testing.T) map[string]string {
	t.Helper()
	out := map[string]string{}
	queries, err := filepath.Glob(filepath.Join("queries", "*.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if len(queries) == 0 {
		t.Fatal("no query files")
	}
	for _, p := range queries {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		out[p] = string(data)
	}
	literals := 0
	fset := token.NewFileSet()
	for _, dir := range []string{".", "db"} {
		goFiles, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range goFiles {
			if strings.HasSuffix(p, "_test.go") {
				continue
			}
			f, err := parser.ParseFile(fset, p, nil, parser.SkipObjectResolution)
			if err != nil {
				t.Fatal(err)
			}
			ast.Inspect(f, func(n ast.Node) bool {
				lit, ok := n.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return true
				}
				s, err := strconv.Unquote(lit.Value)
				if err != nil {
					t.Fatalf("%s: %v", fset.Position(lit.Pos()), err)
				}
				if sqlStatement.MatchString(s) {
					out[fset.Position(lit.Pos()).String()] = s
					literals++
				}
				return true
			})
		}
	}
	if literals == 0 {
		t.Fatal("found no SQL in the store's Go code; the scan is broken")
	}
	return out
}

// TestStoreSQLUsesTheAppClock fails when store SQL reads the database clock
// or names a table without the schema token. Timestamps are parameters taken
// from the configured clock.
func TestStoreSQLUsesTheAppClock(t *testing.T) {
	tables := flowkitTables(t)
	for where, sql := range storeSQL(t) {
		for _, f := range sqllint.CheckTimeFunctions(sql) {
			t.Errorf("%s: %s", where, f)
		}
		for _, f := range sqllint.CheckSchemaToken(sql, tables) {
			t.Errorf("%s: %s", where, f)
		}
	}
}
