package migrate

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
)

// SchemaToken prefixes every table name in flowkit DDL and queries. It is a
// SQL comment, so the text stays valid SQL for sqlc, and is replaced with the
// quoted schema name at run time.
const SchemaToken = "/* TEMPLATE: schema */"

// schemaNamePattern allows lowercase identifiers of at most 46 bytes, the same
// length limit River applies to the schema it shares with flowkit.
var schemaNamePattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,45}$`)

// ValidateSchemaName rejects schema names flowkit must not install into:
// anything outside schemaNamePattern, public, information_schema and the
// reserved pg_ prefix. It runs before any statement reaches the database.
func ValidateSchemaName(name string) error {
	switch {
	case !schemaNamePattern.MatchString(name):
		return fmt.Errorf("flowkit: invalid schema name %q: want a lowercase identifier matching %s", name, schemaNamePattern)
	case name == "public", name == "information_schema", strings.HasPrefix(name, "pg_"):
		return fmt.Errorf("flowkit: schema name %q is reserved", name)
	}
	return nil
}

// RenderSchema replaces every SchemaToken in sql with the quoted schema name
// and a dot. schema must have passed ValidateSchemaName; it is still quoted
// with pgx.Identifier so the output is safe for any input.
func RenderSchema(sql, schema string) string {
	return strings.ReplaceAll(sql, SchemaToken, pgx.Identifier{schema}.Sanitize()+".")
}
