package main

import (
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// lastLine returns the last line of out.
func lastLine(out string) string {
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	return lines[len(lines)-1]
}

func TestValidateAcceptsTheSamples(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "..", "testdata", "valid", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no sample definitions found")
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			r := runCLI(noEnv, "validate", f)
			if r.code != exitOK || r.stderr != "" {
				t.Fatalf("exit %d, stderr %q", r.code, r.stderr)
			}
			if !strings.Contains(r.stdout, f+": structure is valid\n") {
				t.Errorf("stdout %q does not report the file valid", r.stdout)
			}
			if got := lastLine(r.stdout); got != fullValidationNote {
				t.Errorf("last line %q, want the note on full validation", got)
			}
		})
	}
}

// TestValidateInvalidCorpus runs the CLI on every definition the registry
// rejects. The structural mistakes are reported at their path; the others
// need the registry and pass the CLI, which says so on its last line.
func TestValidateInvalidCorpus(t *testing.T) {
	type want struct{ path, msg string }
	corpus := map[string][]want{
		"dedup-window-missing.json":    {{"/concurrency/dedup_window", "required by policy"}},
		"expression-syntax-error.json": {{"/steps/0/when", "1:14: Syntax error"}},
		"loop-without-exit.json": {
			{"/steps/1", "no path from this step reaches an end step"},
			{"/steps/2", "no path from this step reaches an end step"},
		},
		"metamodel-version-unknown.json": {{"/metamodel_version", "unsupported metamodel version 2"}},
		"next-step-unknown.json": {
			{"/steps/0", "no path from this step reaches an end step"},
			{"/steps/0/next", `unknown step "finish"`},
			{"/steps/1", "unreachable"},
		},
		"serial-key-missing.json":    {{"/concurrency/serial_key", `required by policy "serial"`}},
		"step-field-unknown.json":    {{"/steps/0", "'retries' not allowed"}},
		"step-id-duplicate.json":     {{"/steps/2/id", `duplicate step id "done"`}},
		"step-unreachable.json":      {{"/steps/2", "unreachable"}},
		"trigger-missing.json":       {{"/trigger", "missing required property"}},
		"wait-timeout-too-long.json": {{"/steps/0/timeout", "at most 365 days"}},
		// Registry checks: registered components, expression types, has()
		// guards, fact availability, params schemas, cost, and values
		// Postgres cannot store.
		"action-unregistered.json":           nil,
		"estimated-cost-exceeded.json":       nil,
		"fact-not-available-on-trigger.json": nil,
		"fact-undeclared.json":               nil,
		"field-misspelt-nested.json":         nil,
		"has-guard-missing.json":             nil,
		"list-element-field-misspelt.json":   nil,
		"map-key-guard-missing.json":         nil,
		"nul-character.json":                 nil,
		"number-out-of-range.json":           nil,
		"output-fact-before-action.json":     nil,
		"output-fact-not-certain.json":       nil,
		"params-expression-wrong-type.json":  nil,
		"params-literal-invalid.json":        nil,
		"params-required-missing.json":       nil,
		"serial-key-not-string.json":         nil,
		"switch-case-not-bool.json":          nil,
		"trigger-event-unregistered.json":    nil,
		"wait-event-unregistered.json":       nil,
		"wait-match-field-of-trigger.json":   nil,
	}

	dir := filepath.Join("..", "..", "testdata", "invalid")
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, f := range files {
		names = append(names, filepath.Base(f))
	}
	if expected := slices.Sorted(maps.Keys(corpus)); !slices.Equal(expected, names) {
		t.Fatalf("corpus files %v do not match the expectations for %v", names, expected)
	}

	for _, name := range names {
		t.Run(strings.TrimSuffix(name, ".json"), func(t *testing.T) {
			file := filepath.Join(dir, name)
			r := runCLI(noEnv, "validate", file)
			wants := corpus[name]
			if len(wants) == 0 {
				if r.code != exitOK || r.stderr != "" {
					t.Fatalf("exit %d, stderr %q; want the structure accepted", r.code, r.stderr)
				}
				if got := lastLine(r.stdout); got != fullValidationNote {
					t.Errorf("last line %q, want the note on full validation", got)
				}
				return
			}
			if r.code != exitError || r.stdout != "" {
				t.Fatalf("exit %d, stdout %q; want %d with problems on stderr", r.code, r.stdout, exitError)
			}
			lines := strings.Split(strings.TrimRight(r.stderr, "\n"), "\n")
			if len(lines) != len(wants)+2 {
				t.Fatalf("stderr has %d lines, want %d problems, a count and the note:\n%s", len(lines), len(wants), r.stderr)
			}
			for i, w := range wants {
				prefix := file + ": " + w.path + ": "
				if !strings.HasPrefix(lines[i], prefix) || !strings.Contains(lines[i], w.msg) {
					t.Errorf("line %d is %q, want %q followed by a message containing %q", i, lines[i], prefix, w.msg)
				}
			}
			if got := lines[len(wants)]; !strings.HasPrefix(got, file+": ") || !strings.Contains(got, " problem") {
				t.Errorf("count line %q", got)
			}
			if got := lines[len(lines)-1]; got != fullValidationNote {
				t.Errorf("last line %q, want the note on full validation", got)
			}
		})
	}
}

func TestValidateParsesEveryExpression(t *testing.T) {
	body := `{
		"metamodel_version": 1,
		"trigger": {"event": "order.paid"},
		"concurrency": {"policy": "serial", "serial_key": "event.customer_id +"},
		"steps": [
			{"id": "notify", "type": "action", "action": "send_email", "when": "event.vip &&",
			 "params": {"to": {"$expr": "event.email"}, "vars": {"items": {"$expr": "event.items.map(i,"}}}, "next": "route"},
			{"id": "route", "type": "switch", "cases": [{"when": "event.total > 1", "next": "wait"}, {"when": "(", "next": "done"}], "default": "done"},
			{"id": "wait", "type": "wait_for_event", "event": "shipment.delivered", "match": "event.carrier ==",
			 "timeout": "14d", "on_timeout": "done", "next": "done"},
			{"id": "done", "type": "end", "name": "done"}
		]
	}`
	file := filepath.Join(t.TempDir(), "def.json")
	if err := os.WriteFile(file, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	r := runCLI(noEnv, "validate", file)
	if r.code != exitError {
		t.Fatalf("exit %d, want %d; stdout %q, stderr %q", r.code, exitError, r.stdout, r.stderr)
	}
	var paths []string
	for line := range strings.Lines(r.stderr) {
		rest, ok := strings.CutPrefix(line, file+": /")
		if !ok {
			continue
		}
		path, msg, _ := strings.Cut(rest, ": ")
		if !strings.Contains(msg, "Syntax error") {
			t.Errorf("problem at /%s is not a syntax error: %q", path, msg)
		}
		if !slices.Contains(paths, "/"+path) {
			paths = append(paths, "/"+path)
		}
	}
	want := []string{
		"/concurrency/serial_key",
		"/steps/0/params/vars/items/$expr",
		"/steps/0/when",
		"/steps/1/cases/1/when",
		"/steps/2/match",
	}
	if !slices.Equal(paths, want) {
		t.Errorf("problems at %v, want %v:\n%s", paths, want, r.stderr)
	}
}

func TestValidateUsageAndReadErrors(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.json")
	big := filepath.Join(t.TempDir(), "big.json")
	if err := os.WriteFile(big, []byte(`{"description": "`+strings.Repeat("x", 1<<20)+`"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		args []string
		code int
		msg  string
	}{
		{[]string{"validate"}, exitUsage, "a definition file is required"},
		{[]string{"validate", "a.json", "b.json"}, exitUsage, `unexpected argument "b.json"`},
		{[]string{"validate", "--bogus"}, exitUsage, "flag provided but not defined"},
		{[]string{"validate", "-h"}, exitOK, "usage: flowkit validate <file>"},
		{[]string{"validate", missing}, exitError, "no such file or directory"},
		{[]string{"validate", big}, exitError, "the limit is 1048576"},
		{[]string{"help"}, exitOK, "validate  check the structure of a definition file"},
	}
	for _, c := range cases {
		r := runCLI(noEnv, c.args...)
		if r.code != c.code {
			t.Errorf("%v: exit %d, want %d (stderr %q)", c.args, r.code, c.code, r.stderr)
		}
		if !strings.Contains(r.stdout+r.stderr, c.msg) {
			t.Errorf("%v: output %q lacks %q", c.args, r.stdout+r.stderr, c.msg)
		}
	}
}
