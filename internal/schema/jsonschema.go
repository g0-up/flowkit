package schema

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/santhosh-tekuri/jsonschema/v6/kind"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

// MaxDepth is the deepest nesting of objects and arrays a validated JSON
// document may have; a top-level object is depth 1.
const MaxDepth = 32

// Violation is one problem in a JSON document. Path is a JSON Pointer
// (RFC 6901); the empty string is the document itself.
type Violation struct {
	Path string
	Msg  string
}

// Compiled is a compiled JSON Schema. It is safe for concurrent use.
type Compiled struct {
	s *jsonschema.Schema
}

// resourceURL is where a compiled document is registered. Nothing is ever
// loaded from it; it only anchors relative references inside the document.
const resourceURL = "urn:flowkit:schema"

// errExternalRef is returned for any reference outside the document: schemas
// come from code, and resolving them must not read files or the network.
var errExternalRef = errors.New("external references are not allowed")

type noLoader struct{}

func (noLoader) Load(string) (any, error) { return nil, errExternalRef }

// Compile compiles a JSON Schema document. A document without $schema is
// read as draft 2020-12. References to anything outside the document fail.
func Compile(doc json.RawMessage) (*Compiled, error) {
	if len(bytes.TrimSpace(doc)) == 0 {
		return nil, errors.New("empty schema")
	}
	v, err := jsonschema.UnmarshalJSON(bytes.NewReader(doc))
	if err != nil {
		return nil, fmt.Errorf("schema is not valid JSON: %w", err)
	}
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.UseLoader(noLoader{})
	if err := c.AddResource(resourceURL, v); err != nil {
		return nil, err
	}
	s, err := c.Compile(resourceURL)
	if err != nil {
		return nil, err
	}
	return &Compiled{s: s}, nil
}

// Decode parses JSON for validation, keeping numbers exact as json.Number.
func Decode(data []byte) (any, error) {
	return jsonschema.UnmarshalJSON(bytes.NewReader(data))
}

var printer = message.NewPrinter(language.English)

// Validate checks v, a value produced by Decode, and returns every
// violation, sorted by path. It returns nil when v is valid.
func (c *Compiled) Validate(v any) []Violation {
	err := c.s.Validate(v)
	if err == nil {
		return nil
	}
	var verr *jsonschema.ValidationError
	if !errors.As(err, &verr) {
		return []Violation{{Msg: err.Error()}}
	}
	seen := map[Violation]bool{}
	var out []Violation
	add := func(p Violation) {
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	var walk func(e *jsonschema.ValidationError)
	walk = func(e *jsonschema.ValidationError) {
		if len(e.Causes) > 0 {
			for _, c := range e.Causes {
				walk(c)
			}
			return
		}
		base := Pointer(e.InstanceLocation...)
		// A missing property is reported at the object; point at the
		// property instead so each one gets its own path.
		if req, ok := e.ErrorKind.(*kind.Required); ok {
			for _, name := range req.Missing {
				add(Violation{Path: base + Pointer(name), Msg: "missing required property"})
			}
			return
		}
		add(Violation{Path: base, Msg: e.ErrorKind.LocalizedString(printer)})
	}
	walk(verr)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// Pointer builds a JSON Pointer from reference tokens, escaping "~" and "/".
func Pointer(tokens ...string) string {
	var b strings.Builder
	for _, t := range tokens {
		b.WriteByte('/')
		t = strings.ReplaceAll(t, "~", "~0")
		b.WriteString(strings.ReplaceAll(t, "/", "~1"))
	}
	return b.String()
}

// Depth returns the deepest nesting of objects and arrays in data, which
// must be valid JSON. It scans bytes without decoding, so it is safe to call
// before decoding untrusted input.
func Depth(data []byte) int {
	depth, deepest := 0, 0
	inString, escaped := false, false
	for _, c := range data {
		if inString {
			switch {
			case escaped:
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{', '[':
			depth++
			deepest = max(deepest, depth)
		case '}', ']':
			depth--
		}
	}
	return deepest
}

// TopLevelType returns the "type" a schema document declares for a top-level
// property, or "" when the document does not declare exactly one type name
// for it under "properties".
func TopLevelType(doc json.RawMessage, property string) string {
	var s struct {
		Properties map[string]struct {
			Type json.RawMessage `json:"type"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(doc, &s); err != nil {
		return ""
	}
	p, ok := s.Properties[property]
	if !ok {
		return ""
	}
	var t string
	if err := json.Unmarshal(p.Type, &t); err != nil {
		return ""
	}
	return t
}
