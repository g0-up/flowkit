package migrate

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
)

// catalogQueries lists, per object class, every object in the namespace whose
// OID is $1. They run with search_path set to that schema only, so regclass,
// regprocedure and most pg_get_*def output is unqualified; index definitions,
// which always name their table's schema, are unqualified afterwards. Each
// line starts with the object kind.
//
// The format matches the signed River allowlist line for line.
var catalogQueries = []string{
	// Relations: tables (with persistence), sequences, views, and every other
	// relkind except indexes, which are listed with their definition below.
	`SELECT CASE c.relkind
	            WHEN 'r' THEN 'table'   WHEN 'p' THEN 'partitioned_table'
	            WHEN 'S' THEN 'sequence' WHEN 'v' THEN 'view'
	            WHEN 'm' THEN 'materialized_view' WHEN 'f' THEN 'foreign_table'
	            WHEN 'c' THEN 'composite_type_relation' ELSE 'relkind_' || c.relkind::text END
	        || ' ' || c.relname
	        || CASE c.relpersistence WHEN 'u' THEN ' UNLOGGED' WHEN 't' THEN ' TEMPORARY' ELSE '' END
	 FROM pg_catalog.pg_class c
	 WHERE c.relnamespace = $1 AND c.relkind NOT IN ('i', 'I')`,
	`SELECT 'index ' || c.relname || ': ' || pg_catalog.pg_get_indexdef(c.oid)
	 FROM pg_catalog.pg_class c
	 WHERE c.relnamespace = $1 AND c.relkind IN ('i', 'I')`,
	`SELECT 'column_default ' || c.relname || '.' || a.attname || ': ' || pg_catalog.pg_get_expr(d.adbin, d.adrelid)
	 FROM pg_catalog.pg_attrdef d
	 JOIN pg_catalog.pg_class c ON c.oid = d.adrelid
	 JOIN pg_catalog.pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
	 WHERE c.relnamespace = $1`,
	`SELECT 'identity_column ' || c.relname || '.' || a.attname || ': ' || a.attidentity::text
	 FROM pg_catalog.pg_attribute a
	 JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
	 WHERE c.relnamespace = $1 AND a.attidentity <> '' AND NOT a.attisdropped`,
	`SELECT 'generated_column ' || c.relname || '.' || a.attname
	 FROM pg_catalog.pg_attribute a
	 JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
	 WHERE c.relnamespace = $1 AND a.attgenerated <> '' AND NOT a.attisdropped`,
	// NOT NULL constraints (contype 'n', catalogued from PostgreSQL 18) are
	// column properties already implied by the table and are left out so the
	// listing does not depend on the server major version.
	`SELECT 'constraint ' || COALESCE(c.relname, '-') || '.' || con.conname || ': ' || pg_catalog.pg_get_constraintdef(con.oid)
	 FROM pg_catalog.pg_constraint con
	 LEFT JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
	 WHERE con.connamespace = $1 AND con.contype <> 'n'`,
	// Functions, procedures and aggregates, with language, volatility and a
	// body fingerprint so a silent body change in a River upgrade shows up.
	`SELECT CASE p.prokind WHEN 'f' THEN 'function' WHEN 'p' THEN 'procedure'
	                       WHEN 'a' THEN 'aggregate' WHEN 'w' THEN 'window_function' END
	        || ' ' || p.oid::regprocedure::text
	        || ' returns ' || pg_catalog.format_type(p.prorettype, NULL)
	        || ' language=' || l.lanname
	        || ' volatility=' || p.provolatile::text
	        || ' body_md5=' || md5(COALESCE(p.prosrc, ''))
	 FROM pg_catalog.pg_proc p
	 JOIN pg_catalog.pg_language l ON l.oid = p.prolang
	 WHERE p.pronamespace = $1`,
	// Types that are objects in their own right: enums (with labels in sort
	// order), domains, ranges, multiranges, base types and standalone
	// composites. Array types and table row types are created implicitly and
	// are skipped.
	`SELECT CASE t.typtype WHEN 'e' THEN 'enum' WHEN 'd' THEN 'domain' WHEN 'r' THEN 'range'
	                       WHEN 'm' THEN 'multirange' WHEN 'b' THEN 'base_type'
	                       WHEN 'c' THEN 'composite_type' WHEN 'p' THEN 'pseudo_type' END
	        || ' ' || t.typname
	        || CASE t.typtype
	             WHEN 'e' THEN ': ' || (SELECT string_agg(e.enumlabel, ',' ORDER BY e.enumsortorder)
	                                    FROM pg_catalog.pg_enum e WHERE e.enumtypid = t.oid)
	             WHEN 'd' THEN ': ' || pg_catalog.format_type(t.typbasetype, t.typtypmod)
	             ELSE '' END
	 FROM pg_catalog.pg_type t
	 LEFT JOIN pg_catalog.pg_class rel ON rel.oid = t.typrelid
	 WHERE t.typnamespace = $1
	   AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_type el WHERE el.typarray = t.oid)
	   AND (t.typtype <> 'c' OR rel.relkind = 'c')`,
	// Foreign keys are enforced by internal triggers, which are not objects
	// anyone wrote; only user triggers count.
	`SELECT 'trigger ' || c.relname || '.' || tg.tgname || ': ' || pg_catalog.pg_get_triggerdef(tg.oid)
	 FROM pg_catalog.pg_trigger tg
	 JOIN pg_catalog.pg_class c ON c.oid = tg.tgrelid
	 WHERE c.relnamespace = $1 AND NOT tg.tgisinternal`,
	`SELECT 'rule ' || c.relname || '.' || r.rulename
	 FROM pg_catalog.pg_rewrite r
	 JOIN pg_catalog.pg_class c ON c.oid = r.ev_class
	 WHERE c.relnamespace = $1 AND r.rulename <> '_RETURN'`,
	`SELECT 'policy ' || c.relname || '.' || pol.polname
	 FROM pg_catalog.pg_policy pol
	 JOIN pg_catalog.pg_class c ON c.oid = pol.polrelid
	 WHERE c.relnamespace = $1`,
	`SELECT 'operator ' || o.oid::regoperator::text FROM pg_catalog.pg_operator o WHERE o.oprnamespace = $1`,
	`SELECT 'collation ' || collname FROM pg_catalog.pg_collation WHERE collnamespace = $1`,
	`SELECT 'conversion ' || conname FROM pg_catalog.pg_conversion WHERE connamespace = $1`,
	`SELECT 'statistics ' || stxname FROM pg_catalog.pg_statistic_ext WHERE stxnamespace = $1`,
	`SELECT 'text_search_config ' || cfgname FROM pg_catalog.pg_ts_config WHERE cfgnamespace = $1`,
	`SELECT 'text_search_dict ' || dictname FROM pg_catalog.pg_ts_dict WHERE dictnamespace = $1`,
	`SELECT 'operator_class ' || opcname FROM pg_catalog.pg_opclass WHERE opcnamespace = $1`,
	`SELECT 'operator_family ' || opfname FROM pg_catalog.pg_opfamily WHERE opfnamespace = $1`,
}

// forbiddenKinds are object kinds flowkit migrations never create. Any object
// of these kinds in a flowkit schema must come from the signed River
// allowlist.
var forbiddenKinds = map[string]bool{
	"function": true, "procedure": true, "aggregate": true, "window_function": true,
	"trigger": true, "rule": true, "policy": true,
	"enum": true, "domain": true, "range": true, "multirange": true,
	"base_type": true, "pseudo_type": true, "composite_type": true, "composite_type_relation": true,
	"operator": true, "operator_class": true, "operator_family": true,
	"collation": true, "conversion": true, "text_search_config": true, "text_search_dict": true,
	"partitioned_table": true, "generated_column": true,
}

// objectKind returns the kind word a listing line starts with.
func objectKind(line string) string {
	kind, _, _ := strings.Cut(line, " ")
	return kind
}

// objectKey is the identity of a listed object: the line up to the
// definition that follows ": ", e.g. "index runs_entity". Functions keep
// their signature and body fingerprint.
func objectKey(line string) string {
	key, _, _ := strings.Cut(line, ": ")
	return key
}

// isForbiddenObject reports whether a listing line describes an object
// flowkit migrations must not create: a forbidden kind or any column default.
// Flowkit tables take every value from the application, so a default can only
// come from River; keywords such as CURRENT_TIMESTAMP carry no parentheses,
// which is why the whole kind is forbidden rather than function calls.
func isForbiddenObject(line string) bool {
	kind := objectKind(line)
	return forbiddenKinds[kind] || kind == "column_default"
}

// txBeginner is implemented by pools and connections.
type txBeginner interface {
	BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error)
}

// dumpObjects returns the sorted listing of every object in schema, in the
// format of the River allowlist. A missing schema lists nothing.
func dumpObjects(ctx context.Context, db txBeginner, schema string) ([]string, error) {
	if err := ValidateSchemaName(schema); err != nil {
		return nil, err
	}
	tx, err := db.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("flowkit migrate: list objects in %q: %w", schema, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var nsOID uint32
	var quoted string
	err = tx.QueryRow(ctx, `SELECT n.oid, pg_catalog.quote_ident(n.nspname) FROM pg_catalog.pg_namespace n WHERE n.nspname = $1`,
		schema).Scan(&nsOID, &quoted)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("flowkit migrate: list objects in %q: %w", schema, err)
	}
	if _, err := tx.Exec(ctx, "SET LOCAL search_path TO "+pgx.Identifier{schema}.Sanitize()); err != nil {
		return nil, fmt.Errorf("flowkit migrate: list objects in %q: %w", schema, err)
	}
	// pg_get_indexdef qualifies the table even when its schema is on
	// search_path. Strip exactly that qualifier, not every occurrence of the
	// schema name, which may also be part of an object name.
	indexQualifier := " ON " + quoted + "."
	var lines []string
	for _, q := range catalogQueries {
		rows, err := tx.Query(ctx, q, nsOID)
		if err != nil {
			return nil, fmt.Errorf("flowkit migrate: list objects in %q: %w", schema, err)
		}
		got, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return nil, fmt.Errorf("flowkit migrate: list objects in %q: %w", schema, err)
		}
		for _, l := range got {
			if objectKind(l) == "index" {
				l = strings.Replace(l, indexQualifier, " ON ", 1)
			}
			lines = append(lines, strings.Join(strings.Fields(l), " "))
		}
	}
	slices.Sort(lines)
	return lines, nil
}

// parseListing reads an object listing file: one object per line, '#'
// comment lines and blank lines ignored.
func parseListing(data string) []string {
	var out []string
	for line := range strings.Lines(data) {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out = append(out, line)
	}
	return out
}
