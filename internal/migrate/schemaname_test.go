package migrate

import (
	"strings"
	"testing"
)

func TestValidateSchemaName(t *testing.T) {
	valid := []string{
		"flowkit",
		"_flowkit",
		"flow_kit_2",
		"a",
		"tenant_01_flowkit",
		strings.Repeat("a", 46),
	}
	for _, name := range valid {
		if err := ValidateSchemaName(name); err != nil {
			t.Errorf("ValidateSchemaName(%q) = %v, want nil", name, err)
		}
	}
	invalid := []string{
		"",
		"Flow-Kit",
		"flow-kit",
		"Flowkit",
		"FLOWKIT",
		"1flowkit",
		"flow kit",
		`flow"kit`,
		"flow.kit",
		"flowkit;drop",
		strings.Repeat("a", 47),
		"public",
		"information_schema",
		"pg_x",
		"pg_catalog",
		"pg_",
	}
	for _, name := range invalid {
		if err := ValidateSchemaName(name); err == nil {
			t.Errorf("ValidateSchemaName(%q) = nil, want error", name)
		}
	}
}

func TestRenderSchema(t *testing.T) {
	sql := "SELECT 1 FROM " + SchemaToken + "runs r JOIN " + SchemaToken + "run_steps s ON s.run_id = r.id"
	got := RenderSchema(sql, "tenant_1")
	want := `SELECT 1 FROM "tenant_1".runs r JOIN "tenant_1".run_steps s ON s.run_id = r.id`
	if got != want {
		t.Fatalf("RenderSchema = %s\nwant %s", got, want)
	}
	// Quoting holds even for a name that would never pass validation.
	if got := RenderSchema(SchemaToken+"runs", `x"; DROP`); got != `"x""; DROP".runs` {
		t.Fatalf("RenderSchema with hostile name = %s", got)
	}
	if got := RenderSchema("SELECT 1", "flowkit"); got != "SELECT 1" {
		t.Fatalf("RenderSchema without token changed the SQL: %s", got)
	}
}
