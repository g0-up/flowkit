// Package sqllint checks flowkit SQL text for constructs the database layer
// rules forbid and for table names that lack the schema token. Tests run it
// over migrations, sqlc queries and SQL left in Go code.
package sqllint

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// Token is the schema placeholder every flowkit table name carries. It must
// equal migrate.SchemaToken; the migrate tests assert that.
const Token = "/* TEMPLATE: schema */"

// mark replaces Token in cleaned text so the token survives comment removal.
const mark = "\x01"

// Finding is one rule violation.
type Finding struct {
	Line int    // 1-based line in the checked text
	Rule string // short rule name
	Text string // the offending text
}

func (f Finding) String() string {
	return fmt.Sprintf("line %d: %s: %q", f.Line, f.Rule, f.Text)
}

type rule struct {
	name string
	re   *regexp.Regexp
}

// ddlRules are the constructs flowkit SQL never uses: server-side code, custom
// types, defaults computed by functions, cascading deletes, concurrent index
// builds, advisory locks and partitioning.
var ddlRules = []rule{
	{"function, procedure, trigger or rule", regexp.MustCompile(`(?i)\bCREATE\s+(OR\s+REPLACE\s+)?(CONSTRAINT\s+|EVENT\s+)?(FUNCTION|PROCEDURE|TRIGGER|RULE|AGGREGATE)\b`)},
	{"enum, domain or other custom type", regexp.MustCompile(`(?i)\bCREATE\s+(TYPE|DOMAIN)\b`)},
	{"DO block", regexp.MustCompile(`(?i)\bDO\s+(LANGUAGE\s+\w+\s+)?[$']`)},
	{"default computed by a function", regexp.MustCompile(`(?i)\bDEFAULT\s+((now|current_timestamp|current_date|current_time|localtimestamp|localtime|clock_timestamp|statement_timestamp|transaction_timestamp|gen_random_uuid)\b|[a-z_][a-z0-9_.]*\s*\()`)},
	{"gen_random_uuid", regexp.MustCompile(`(?i)\bgen_random_uuid\s*\(`)},
	{"ON DELETE CASCADE", regexp.MustCompile(`(?i)\bON\s+DELETE\s+CASCADE\b`)},
	{"CONCURRENTLY", regexp.MustCompile(`(?i)\bCONCURRENTLY\b`)},
	{"advisory lock", regexp.MustCompile(`(?i)\bpg_(try_)?advisory_\w*`)},
	{"partitioning", regexp.MustCompile(`(?i)\bPARTITION\s+(BY|OF)\b`)},
}

// timeRules match database clock reads; every timestamp comes from the
// application clock as a parameter.
var timeRules = []rule{
	{"database clock", regexp.MustCompile(`(?i)\b(now|clock_timestamp|statement_timestamp|transaction_timestamp|timeofday)\s*\(`)},
	{"database clock", regexp.MustCompile(`(?i)\b(current_timestamp|current_date|current_time|localtimestamp|localtime)\b`)},
}

// CheckDDLRules reports forbidden constructs in sql, ignoring comments and
// string literals.
func CheckDDLRules(sql string) []Finding {
	return check(clean(sql), ddlRules)
}

// CheckTimeFunctions reports reads of the database clock in sql, ignoring
// comments and string literals.
func CheckTimeFunctions(sql string) []Finding {
	return check(clean(sql), timeRules)
}

func check(code string, rules []rule) []Finding {
	var out []Finding
	for _, r := range rules {
		for _, loc := range r.re.FindAllStringIndex(code, -1) {
			out = append(out, Finding{Line: lineAt(code, loc[0]), Rule: r.name, Text: code[loc[0]:loc[1]]})
		}
	}
	slices.SortStableFunc(out, func(a, b Finding) int { return a.Line - b.Line })
	return out
}

var createTable = regexp.MustCompile(`(?i)\bCREATE\s+(?:UNLOGGED\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?` + mark + `?\s*([a-z_][a-z0-9_]*)`)

// TableNames returns the tables created by ddl, in order.
func TableNames(ddl string) []string {
	var names []string
	for _, m := range createTable.FindAllStringSubmatch(clean(ddl), -1) {
		names = append(names, strings.ToLower(m[1]))
	}
	return names
}

var (
	identifier = regexp.MustCompile(`(?i)[a-z_][a-z0-9_$]*`)
	// tableRef finds the identifier after a keyword that introduces a table.
	tableRef = regexp.MustCompile(`(?i)\b(FROM|JOIN|INTO|UPDATE|TABLE|REFERENCES|TRUNCATE)\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?(?:ONLY\s+)?(` + mark + `)?\s*([a-z_][a-z0-9_$]*)(\s*\()?`)
	// notTables follow a table keyword without naming a table.
	notTables = map[string]bool{
		"set": true, "skip": true, "nowait": true, "of": true, "lateral": true,
		"select": true, "values": true, "only": true,
	}
	// subqueryOpeners precede a parenthesis that holds a subquery rather than
	// function arguments.
	subqueryOpeners = map[string]bool{
		"in": true, "exists": true, "as": true, "any": true, "all": true, "some": true,
		"lateral": true, "not": true, "array": true, "from": true, "join": true,
		"select": true, "where": true, "and": true, "or": true, "on": true, "using": true,
	}
)

// CheckSchemaToken reports table names in sql written without Token. A name
// in tables must carry the token wherever it appears; any other identifier
// after FROM, JOIN, INTO, UPDATE, TABLE, REFERENCES or TRUNCATE must carry
// it too, except function calls and the FROM of IS DISTINCT FROM or of
// functions such as extract(... FROM ...). Qualify columns with an alias, not
// the table name.
func CheckSchemaToken(sql string, tables []string) []Finding {
	code := clean(sql)
	var out []Finding
	known := make(map[string]bool, len(tables))
	for _, t := range tables {
		known[strings.ToLower(t)] = true
	}
	for _, loc := range identifier.FindAllStringIndex(code, -1) {
		word := strings.ToLower(code[loc[0]:loc[1]])
		if !known[word] || (loc[0] > 0 && isIdentChar(code[loc[0]-1])) {
			continue
		}
		if !strings.HasSuffix(strings.TrimRight(code[:loc[0]], " \t\r\n"), mark) {
			out = append(out, Finding{Line: lineAt(code, loc[0]), Rule: "table without schema token", Text: word})
		}
	}
	for _, m := range tableRef.FindAllStringSubmatchIndex(code, -1) {
		keyword := strings.ToLower(code[m[2]:m[3]])
		hasMark := m[4] >= 0
		name := strings.ToLower(code[m[6]:m[7]])
		call := m[8] >= 0
		switch {
		case hasMark, known[name], notTables[name]:
			continue // carries the token, or already checked above
		case (keyword == "from" || keyword == "join") && call:
			continue // set-returning function such as unnest(...)
		case keyword == "from" && (prevWord(code, m[2]) == "distinct" || inFunctionCall(code, m[2])):
			continue
		}
		out = append(out, Finding{Line: lineAt(code, m[6]), Rule: "table without schema token", Text: name})
	}
	slices.SortStableFunc(out, func(a, b Finding) int { return a.Line - b.Line })
	return out
}

// clean blanks comments and the contents of string literals, keeping line
// breaks so line numbers stay valid, and turns Token into mark.
func clean(sql string) string {
	sql = strings.ReplaceAll(sql, Token, mark)
	var b strings.Builder
	b.Grow(len(sql))
	blank := func(s string) {
		for _, c := range []byte(s) {
			if c == '\n' {
				b.WriteByte('\n')
			} else {
				b.WriteByte(' ')
			}
		}
	}
	for i := 0; i < len(sql); {
		switch {
		case strings.HasPrefix(sql[i:], "--"):
			end := strings.IndexByte(sql[i:], '\n')
			if end < 0 {
				end = len(sql) - i
			}
			blank(sql[i : i+end])
			i += end
		case strings.HasPrefix(sql[i:], "/*"):
			end := strings.Index(sql[i+2:], "*/")
			if end < 0 {
				end = len(sql) - i
			} else {
				end += 4
			}
			blank(sql[i : i+end])
			i += end
		case sql[i] == '\'':
			// A quoted '' inside a literal ends one literal and starts the
			// next, which blanks the same text.
			end := strings.IndexByte(sql[i+1:], '\'')
			if end < 0 {
				end = len(sql) - i - 1
			}
			b.WriteByte('\'')
			blank(sql[i+1 : i+1+end])
			if i+1+end < len(sql) {
				b.WriteByte('\'')
			}
			i += end + 2
		default:
			b.WriteByte(sql[i])
			i++
		}
	}
	return b.String()
}

func lineAt(s string, off int) int {
	return strings.Count(s[:off], "\n") + 1
}

func isIdentChar(c byte) bool {
	return c == '_' || c == '$' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

// prevWord returns the lowercased word that ends right before off.
func prevWord(s string, off int) string {
	end := len(strings.TrimRight(s[:off], " \t\r\n"))
	start := end
	for start > 0 && isIdentChar(s[start-1]) {
		start--
	}
	return strings.ToLower(s[start:end])
}

// inFunctionCall reports whether off sits inside parentheses opened right
// after a function name, as in extract(epoch FROM x).
func inFunctionCall(s string, off int) bool {
	depth := 0
	for i := off - 1; i >= 0; i-- {
		switch s[i] {
		case ')':
			depth++
		case '(':
			if depth > 0 {
				depth--
				continue
			}
			w := prevWord(s, i)
			return w != "" && !subqueryOpeners[w]
		}
	}
	return false
}
