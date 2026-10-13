package expr

import (
	"reflect"
	"strconv"
	"strings"
)

// branchKeywords choose between subschemas depending on the value.
var branchKeywords = []string{"oneOf", "anyOf", "not", "if"}

// Branch returns the shallowest place at or above pointer where the schema
// chooses between subschemas with oneOf, anyOf, not or if, and whether
// there is one. Whether the literal values there satisfy the schema can
// depend on what an expression at pointer produces, so a definition check
// leaves them to schema validation of the resolved value.
//
// The walk follows properties, additionalProperties, prefixItems, items,
// allOf and internal "$ref"s.
func (s *Shape) Branch(pointer string) (string, bool) {
	var tokens []string
	if pointer != "" {
		tokens = strings.Split(pointer[1:], "/")
	}
	level := []any{s.doc}
	for i := 0; ; i++ {
		schemas := expandSchemas(s.doc, level)
		for _, m := range schemas {
			for _, k := range branchKeywords {
				if _, ok := m[k]; ok {
					return pointerPrefix(pointer, i), true
				}
			}
		}
		if i == len(tokens) {
			return "", false
		}
		tok := strings.ReplaceAll(strings.ReplaceAll(tokens[i], "~1", "/"), "~0", "~")
		level = nil
		for _, m := range schemas {
			if c := childSchema(m, tok); c != nil {
				level = append(level, c)
			}
		}
		if len(level) == 0 {
			return "", false
		}
	}
}

// pointerPrefix returns the first n reference tokens of pointer, still
// escaped.
func pointerPrefix(pointer string, n int) string {
	end := 0
	for range n {
		next := strings.IndexByte(pointer[end+1:], '/')
		if next < 0 {
			return pointer
		}
		end += 1 + next
	}
	return pointer[:end]
}

// expandSchemas returns the schema objects that apply to one place: the
// given ones, their allOf members and the targets of their internal
// "$ref"s, each once. A reference that does not resolve contributes
// nothing.
func expandSchemas(root any, level []any) []map[string]any {
	var out []map[string]any
	seen := map[uintptr]bool{}
	var add func(v any)
	add = func(v any) {
		m, ok := v.(map[string]any)
		if !ok {
			return
		}
		id := reflect.ValueOf(m).Pointer()
		if seen[id] {
			return
		}
		seen[id] = true
		out = append(out, m)
		if ref, ok := m["$ref"].(string); ok {
			if target, err := resolvePointer(root, ref); err == nil {
				add(target)
			}
		}
		if all, ok := m["allOf"].([]any); ok {
			for _, sub := range all {
				add(sub)
			}
		}
	}
	for _, v := range level {
		add(v)
	}
	return out
}

// childSchema returns the schema m gives the member tok of an object or
// array value, or nil when m describes none.
func childSchema(m map[string]any, tok string) any {
	if props, ok := m["properties"].(map[string]any); ok {
		if c, ok := props[tok]; ok {
			return c
		}
	}
	if i, err := strconv.Atoi(tok); err == nil && i >= 0 {
		if prefix, ok := m["prefixItems"].([]any); ok && i < len(prefix) {
			return prefix[i]
		}
		if items, ok := m["items"]; ok {
			return items
		}
	}
	if ap, ok := m["additionalProperties"].(map[string]any); ok {
		return ap
	}
	return nil
}
