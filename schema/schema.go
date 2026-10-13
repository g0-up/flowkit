// Package schema embeds the definition metamodel JSON Schema and records the
// metamodel version and CEL environment options.
//
// The options of the expression environment are part of the metamodel
// contract: a definition saved under a metamodel version is evaluated with
// exactly that version's options. Changing an option means adding a new
// metamodel version.
package schema

import (
	_ "embed"
	"slices"
)

// CurrentVersion is the metamodel version new definitions are written in.
const CurrentVersion = 1

//go:embed m2-v1.json
var m2v1 []byte

// Metamodel describes one metamodel version.
type Metamodel struct {
	Version int
	// Schema is the JSON Schema (draft 2020-12) a definition body of this
	// version must satisfy.
	Schema []byte
	// Macros names the CEL macros expressions may use.
	Macros []string
	// Extensions names the CEL extension libraries enabled; none in
	// version 1.
	Extensions []string
}

var metamodels = map[int]Metamodel{
	1: {
		Version:    1,
		Schema:     m2v1,
		Macros:     []string{"has", "all", "exists", "exists_one", "map", "filter"},
		Extensions: nil,
	},
}

// Lookup returns the metamodel of version v. The returned slices are
// copies.
func Lookup(v int) (Metamodel, bool) {
	m, ok := metamodels[v]
	if !ok {
		return Metamodel{}, false
	}
	m.Schema = slices.Clone(m.Schema)
	m.Macros = slices.Clone(m.Macros)
	m.Extensions = slices.Clone(m.Extensions)
	return m, true
}

// Versions lists the supported metamodel versions in ascending order.
func Versions() []int {
	out := make([]int, 0, len(metamodels))
	for v := range metamodels {
		out = append(out, v)
	}
	slices.Sort(out)
	return out
}
