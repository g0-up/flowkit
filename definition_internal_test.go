package flowkit

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestDefinitionChecksum(t *testing.T) {
	sum := func(body string) string {
		t.Helper()
		s, err := definitionChecksum(json.RawMessage(body))
		if err != nil {
			t.Fatalf("checksum of %s: %v", body, err)
		}
		return s
	}
	same := [][]string{
		{`{"a": 1, "b": [2, "x"]}`, `{"b":[2,"x"],"a":1}`, "{\n\t\"b\": [2.0, \"x\"],\n\t\"a\": 1e0\n}", `{"a": 10E-1, "b": [200e-2, "x"]}`},
		{`{"a": 0.5}`, `{ "a" : 0.5 }`},
		{`{"s": "<&>"}`, `{"s": "<&>"}`},
		// An exponent too large to expand keeps its spelling.
		{`{"n": 1e1001}`, `{ "n": 1e1001 }`},
	}
	for _, group := range same {
		want := sum(group[0])
		for _, body := range group[1:] {
			if got := sum(body); got != want {
				t.Errorf("checksum of %s differs from that of %s", body, group[0])
			}
		}
	}
	distinct := []string{`{"a": 1}`, `{"a": 2}`, `{"a": "1"}`, `{"a": 0.5}`, `{"a": 0.50}`, `{"b": 1}`, `[1]`}
	seen := map[string]string{}
	for _, body := range distinct {
		s := sum(body)
		if prev, ok := seen[s]; ok {
			t.Errorf("%s and %s have the same checksum", prev, body)
		}
		seen[s] = body
	}
	if _, err := definitionChecksum(json.RawMessage(`{`)); err == nil {
		t.Fatal("checksum of invalid JSON succeeded")
	}
}

func TestValidateDefinitionReportsAnInvalidRegistry(t *testing.T) {
	r := NewRegistry()
	r.MustAddEvent(orderPaid())
	f := revenueFact()
	f.AvailableOn = []string{"invoice.*"}
	r.MustAddFact(f)
	err := r.ValidateDefinition(json.RawMessage(`{}`))
	var verrs ValidationErrors
	if err == nil || errors.As(err, &verrs) || !strings.Contains(err.Error(), "invalid registry") {
		t.Fatalf("ValidateDefinition = %v, want the registry error", err)
	}
}

func TestValidateDefinitionRejectsBadText(t *testing.T) {
	r := NewRegistry()
	r.MustAddEvent(orderPaid())
	for _, body := range []string{"{\"description\": \"\xff\"}", `{"description": "\ud800"}`} {
		err := r.ValidateDefinition(json.RawMessage(body))
		var verrs ValidationErrors
		if !errors.As(err, &verrs) || !strings.Contains(verrs[0].Msg, "UTF-8") {
			t.Errorf("ValidateDefinition(%q) = %v, want a UTF-8 error", body, err)
		}
	}
}
