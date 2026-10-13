package schema

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

const orderSchema = `{
	"type": "object",
	"required": ["order_id", "amount"],
	"properties": {
		"order_id": {"type": "string", "minLength": 1},
		"amount": {"type": "integer", "minimum": 0},
		"lines": {
			"type": "array",
			"items": {
				"type": "object",
				"required": ["sku"],
				"properties": {"sku": {"type": "string"}}
			}
		}
	}
}`

func mustCompile(t *testing.T, doc string) *Compiled {
	t.Helper()
	c, err := Compile(json.RawMessage(doc))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return c
}

func mustDecode(t *testing.T, data string) any {
	t.Helper()
	v, err := Decode([]byte(data))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	return v
}

func TestValidate(t *testing.T) {
	c := mustCompile(t, orderSchema)
	tests := []struct {
		name  string
		doc   string
		paths []string
	}{
		{"valid", `{"order_id": "o-1", "amount": 5}`, nil},
		{"valid with lines", `{"order_id": "o-1", "amount": 5, "lines": [{"sku": "a"}]}`, nil},
		{"missing properties get their own paths", `{}`, []string{"/amount", "/order_id"}},
		{"wrong type", `{"order_id": 7, "amount": 5}`, []string{"/order_id"}},
		{"below minimum", `{"order_id": "o-1", "amount": -1}`, []string{"/amount"}},
		{"nested", `{"order_id": "o-1", "amount": 1, "lines": [{"sku": "a"}, {}]}`, []string{"/lines/1/sku"}},
		{"not an object", `[]`, []string{""}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := c.Validate(mustDecode(t, tt.doc))
			var paths []string
			for _, v := range got {
				if v.Msg == "" {
					t.Errorf("violation at %q has no message", v.Path)
				}
				paths = append(paths, v.Path)
			}
			if !reflect.DeepEqual(paths, tt.paths) {
				t.Fatalf("paths = %q, want %q (violations %+v)", paths, tt.paths, got)
			}
		})
	}
}

func TestValidateKeepsLargeIntegersExact(t *testing.T) {
	c := mustCompile(t, `{"type": "object", "properties": {"n": {"type": "integer", "maximum": 9007199254740993}}}`)
	if got := c.Validate(mustDecode(t, `{"n": 9007199254740993}`)); got != nil {
		t.Fatalf("2^53+1 against maximum 2^53+1: %+v", got)
	}
	if got := c.Validate(mustDecode(t, `{"n": 9007199254740994}`)); len(got) != 1 || got[0].Path != "/n" {
		t.Fatalf("2^53+2 against maximum 2^53+1: %+v", got)
	}
}

func TestCompileRejects(t *testing.T) {
	tests := []struct {
		name, doc, want string
	}{
		{"empty", "  ", "empty schema"},
		{"not JSON", `{"type":`, "not valid JSON"},
		{"bad keyword value", `{"type": 5}`, ""},
		{"external ref", `{"$ref": "https://example.com/s.json"}`, ""},
		{"external file ref", `{"$ref": "file:///etc/passwd"}`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Compile(json.RawMessage(tt.doc))
			if err == nil {
				t.Fatal("Compile succeeded")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error %q does not mention %q", err, tt.want)
			}
		})
	}
}

func TestCompileResolvesLocalRefs(t *testing.T) {
	c := mustCompile(t, `{
		"$defs": {"id": {"type": "string"}},
		"type": "object",
		"properties": {"a": {"$ref": "#/$defs/id"}}
	}`)
	if got := c.Validate(mustDecode(t, `{"a": 1}`)); len(got) != 1 || got[0].Path != "/a" {
		t.Fatalf("violations = %+v, want one at /a", got)
	}
}

func TestPointer(t *testing.T) {
	tests := []struct {
		tokens []string
		want   string
	}{
		{nil, ""},
		{[]string{"a"}, "/a"},
		{[]string{"a", "0", "b"}, "/a/0/b"},
		{[]string{"a/b", "c~d"}, "/a~1b/c~0d"},
		{[]string{""}, "/"},
	}
	for _, tt := range tests {
		if got := Pointer(tt.tokens...); got != tt.want {
			t.Errorf("Pointer(%q) = %q, want %q", tt.tokens, got, tt.want)
		}
	}
}

func TestDepth(t *testing.T) {
	tests := []struct {
		doc  string
		want int
	}{
		{`1`, 0},
		{`"x"`, 0},
		{`{}`, 1},
		{`{"a": [1, {"b": []}]}`, 4},
		{`[[], [[]]]`, 3},
		{`{"a": "{[{[{["}`, 1},
		{`{"a": "\"{["}`, 1},
		{`{"a": "\\", "b": [{}]}`, 3},
		{strings.Repeat("[", 40) + strings.Repeat("]", 40), 40},
	}
	for _, tt := range tests {
		if got := Depth([]byte(tt.doc)); got != tt.want {
			t.Errorf("Depth(%s) = %d, want %d", tt.doc, got, tt.want)
		}
	}
}

func TestTopLevelType(t *testing.T) {
	doc := json.RawMessage(`{
		"type": "object",
		"properties": {
			"s": {"type": "string"},
			"i": {"type": "integer"},
			"multi": {"type": ["string", "null"]},
			"untyped": {}
		}
	}`)
	tests := map[string]string{"s": "string", "i": "integer", "multi": "", "untyped": "", "absent": ""}
	for prop, want := range tests {
		if got := TopLevelType(doc, prop); got != want {
			t.Errorf("TopLevelType(%q) = %q, want %q", prop, got, want)
		}
	}
	if got := TopLevelType(json.RawMessage(`[`), "s"); got != "" {
		t.Errorf("TopLevelType on invalid JSON = %q", got)
	}
}
