package flowkit_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/g0-up/flowkit"
	"github.com/g0-up/flowkit/flowkittest"
	"github.com/g0-up/flowkit/internal/dbgate"
	"github.com/g0-up/flowkit/schema"
)

// The compatibility corpus pins what published definitions do. Each case is
// a directory testdata/corpus/<metamodel_version>/<name> holding:
//
//   - definition.json, published as definition <name>;
//   - events.jsonl, the script: one JSON object per line, either
//     {"emit": {"type": ..., "payload": {...}, "dedup_key": ...}}, which emits
//     the event and runs what becomes due, or {"advance": "<duration>"}, which
//     moves the clock and runs what becomes due on the way;
//   - expected_trace.json, every run the script started, as Postgres ran it.
//
// The applications behind the definitions are corpusRegistry: actions
// record their calls and answer from their params, facts answer from the
// run's entity, and nothing leaves the test. A change that alters a trace
// changes what deployed definitions do; regenerate the traces only for an
// intended change, with -update-corpus.
var updateCorpus = flag.Bool("update-corpus", false, "rewrite testdata/corpus/*/*/expected_trace.json from Postgres runs")

const corpusRoot = "testdata/corpus"

// corpusStart is the clock at the start of every case, so that the
// recorded times are offsets from it.
var corpusStart = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

// TestCorpus runs every case on a PGHarness and compares its runs with the
// expected trace. The schema it ran on must still hold exactly the objects
// its migrations created.
func TestCorpus(t *testing.T) {
	t.Parallel()
	for _, c := range corpusCases(t) {
		t.Run(c.id(), func(t *testing.T) {
			t.Parallel()
			rec := newCorpusRecorder()
			h := flowkittest.NewPGHarness(t, corpusRegistry(rec), flowkittest.HarnessStart(corpusStart))
			got := runCorpusCase(t, pgCorpusEnv{h}, c, rec)
			pool, schemaName := flowkit.EngineDB(h.Engine())
			dbgate.Check(t, pool, schemaName)

			if *updateCorpus {
				// A run that failed, the gate above included, is no reference.
				if t.Failed() {
					t.Fatalf("not writing %s after a failure", c.expectedPath())
				}
				if err := os.WriteFile(c.expectedPath(), encodeCorpusTrace(t, got), 0o644); err != nil {
					t.Fatal(err)
				}
				t.Logf("wrote %s (%d runs)", c.expectedPath(), len(got.Runs))
				return
			}
			want := readCorpusTrace(t, c)
			if diffs := diffCorpusTraces(want, got); len(diffs) > 0 {
				t.Errorf("Postgres trace differs from %s (rerun with -update-corpus after an intended change):\n  %s",
					c.expectedPath(), strings.Join(diffs, "\n  "))
			}
		})
	}
}

// TestCorpusOnSim runs every case on a Sim and logs where its runs differ
// from the expected trace, which Postgres produced. The Sim is not the
// reference for flowkit's semantics, so a difference, or a Sim that stops
// the case, is reported and never fails the test.
func TestCorpusOnSim(t *testing.T) {
	if *updateCorpus {
		t.Skip("the expected traces are being rewritten")
	}
	t.Parallel()
	for _, c := range corpusCases(t) {
		t.Run(c.id(), func(t *testing.T) {
			t.Parallel()
			want := readCorpusTrace(t, c)
			var got corpusTrace
			soft := &softTB{TB: t}
			soft.run(func() {
				rec := newCorpusRecorder()
				sim := flowkittest.NewSim(soft, corpusRegistry(rec), flowkittest.SimStart(corpusStart))
				for _, a := range corpusActions() {
					sim.StubAction(a.def.Name, rec.execute(a))
				}
				got = runCorpusCase(soft, sim, c, rec)
			})
			if msgs := soft.failures(); len(msgs) > 0 {
				t.Logf("the Sim stopped before the end of the case (not blocking):\n  %s", strings.Join(msgs, "\n  "))
				return
			}
			if diffs := diffCorpusTraces(want, got); len(diffs) > 0 {
				t.Logf("the Sim differs from Postgres on %d points (not blocking):\n  %s", len(diffs), strings.Join(diffs, "\n  "))
				return
			}
			t.Logf("the Sim matches Postgres: %d runs", len(got.Runs))
		})
	}
}

// corpusCase is one directory of the corpus.
type corpusCase struct {
	version int
	name    string
	dir     string
}

func (c corpusCase) id() string           { return strconv.Itoa(c.version) + "/" + c.name }
func (c corpusCase) expectedPath() string { return filepath.Join(c.dir, "expected_trace.json") }

// corpusCases lists the cases, and fails unless every supported metamodel
// version has one and every case is written in its directory's version.
func corpusCases(t *testing.T) []corpusCase {
	t.Helper()
	dirs, err := filepath.Glob(filepath.Join(corpusRoot, "*", "*"))
	if err != nil {
		t.Fatal(err)
	}
	var cases []corpusCase
	covered := map[int]bool{}
	for _, dir := range dirs {
		version, err := strconv.Atoi(filepath.Base(filepath.Dir(dir)))
		if err != nil {
			t.Fatalf("%s: the parent directory must be a metamodel version", dir)
		}
		var head struct {
			MetamodelVersion int `json:"metamodel_version"`
		}
		if err := json.Unmarshal(readCorpusFile(t, dir, "definition.json"), &head); err != nil {
			t.Fatalf("%s/definition.json: %v", dir, err)
		}
		if head.MetamodelVersion != version {
			t.Fatalf("%s/definition.json has metamodel_version %d, want %d", dir, head.MetamodelVersion, version)
		}
		covered[version] = true
		cases = append(cases, corpusCase{version: version, name: filepath.Base(dir), dir: dir})
	}
	for _, v := range schema.Versions() {
		if !covered[v] {
			t.Errorf("metamodel version %d has no corpus case under %s/%d", v, corpusRoot, v)
		}
	}
	return cases
}

func readCorpusFile(t testing.TB, dir, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// corpusEnv is what a case needs from an environment, so that it runs
// unchanged on Postgres and on the Sim.
type corpusEnv interface {
	Publish(name string, body json.RawMessage) flowkit.Version
	Emit(evt flowkit.Event)
	Advance(d time.Duration)
	Runs() *flowkit.Runs
}

type pgCorpusEnv struct{ *flowkittest.PGHarness }

func (e pgCorpusEnv) Runs() *flowkit.Runs { return e.Engine().Runs() }

// corpusLine is one line of events.jsonl: exactly one of Emit and Advance.
type corpusLine struct {
	Emit    *corpusEmit `json:"emit"`
	Advance string      `json:"advance"`
}

type corpusEmit struct {
	Type     string         `json:"type"`
	Payload  map[string]any `json:"payload"`
	DedupKey string         `json:"dedup_key"`
}

// readCorpusScript parses events.jsonl. Payload numbers stay json.Number,
// so they reach Emit with the digits written.
func readCorpusScript(t testing.TB, c corpusCase) []corpusLine {
	t.Helper()
	var lines []corpusLine
	sc := bufio.NewScanner(bytes.NewReader(readCorpusFile(t, c.dir, "events.jsonl")))
	sc.Buffer(nil, 1<<20)
	for n := 1; sc.Scan(); n++ {
		text := bytes.TrimSpace(sc.Bytes())
		if len(text) == 0 {
			continue
		}
		dec := json.NewDecoder(bytes.NewReader(text))
		dec.UseNumber()
		dec.DisallowUnknownFields()
		var l corpusLine
		if err := dec.Decode(&l); err != nil {
			t.Fatalf("%s/events.jsonl:%d: %v", c.dir, n, err)
		}
		switch {
		case (l.Emit == nil) == (l.Advance == ""):
			t.Fatalf("%s/events.jsonl:%d: want exactly one of emit and advance", c.dir, n)
		case l.Emit != nil && l.Emit.Type == "":
			t.Fatalf("%s/events.jsonl:%d: emit without a type", c.dir, n)
		case l.Advance != "":
			if d, err := time.ParseDuration(l.Advance); err != nil || d < 0 {
				t.Fatalf("%s/events.jsonl:%d: advance %q is not a duration of zero or more", c.dir, n, l.Advance)
			}
		}
		lines = append(lines, l)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("%s/events.jsonl: %v", c.dir, err)
	}
	return lines
}

// runCorpusCase publishes the definition, plays the script and returns the
// trace of every run.
func runCorpusCase(t testing.TB, env corpusEnv, c corpusCase, rec *corpusRecorder) corpusTrace {
	t.Helper()
	script := readCorpusScript(t, c)
	env.Publish(c.name, readCorpusFile(t, c.dir, "definition.json"))
	for _, l := range script {
		if l.Emit != nil {
			env.Emit(flowkit.Event{Type: l.Emit.Type, Payload: l.Emit.Payload, DedupKey: l.Emit.DedupKey})
			env.Advance(0)
			continue
		}
		d, _ := time.ParseDuration(l.Advance)
		env.Advance(d)
	}
	if err := rec.failure(); err != nil {
		t.Fatalf("record action calls: %v", err)
	}
	return collectCorpusTrace(t, env.Runs(), rec)
}

// corpusTrace is what expected_trace.json holds. Its times are offsets
// from corpusStart, and it has no run ids: idempotency keys keep only their
// step id and visit. Event ids, in claim details and snapshots, stay: every
// case starts on an empty store, so the n-th emit of events.jsonl is event
// n.
type corpusTrace struct {
	Runs []corpusRun `json:"runs"`
}

type corpusRun struct {
	Definition       string       `json:"definition"`
	Version          int          `json:"version"`
	MetamodelVersion int          `json:"metamodel_version"`
	Trigger          corpusEvent  `json:"trigger"`
	State            string       `json:"state"`
	Terminal         string       `json:"terminal,omitempty"`
	Started          string       `json:"started"`
	Ended            string       `json:"ended,omitempty"`
	Steps            []corpusStep `json:"steps"`
	Calls            []corpusCall `json:"calls,omitempty"`
}

type corpusEvent struct {
	Type       string         `json:"type"`
	Payload    map[string]any `json:"payload"`
	OccurredAt string         `json:"occurred_at"`
}

type corpusStep struct {
	At       string         `json:"at"`
	Step     string         `json:"step"`
	Kind     string         `json:"kind"`
	Detail   string         `json:"detail,omitempty"`
	Attempt  int            `json:"attempt,omitempty"`
	Error    string         `json:"error,omitempty"`
	Snapshot map[string]any `json:"snapshot,omitempty"`
}

// corpusCall is one call of an action's Execute. Key is the idempotency
// key without its run id: step id and visit.
type corpusCall struct {
	Step   string         `json:"step"`
	Action string         `json:"action"`
	Key    string         `json:"key"`
	Params map[string]any `json:"params"`
}

// offset renders at as its distance from corpusStart; the zero time, of a
// run still going, renders as "".
func offset(at time.Time) string {
	if at.IsZero() {
		return ""
	}
	return "+" + at.Sub(corpusStart).String()
}

// collectCorpusTrace reads every run, in the order of their trigger
// events.
func collectCorpusTrace(t testing.TB, runs *flowkit.Runs, rec *corpusRecorder) corpusTrace {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var traces []flowkit.RunTrace
	for after := ""; ; {
		page, err := runs.List(ctx, flowkit.RunFilter{After: after, Limit: 1000})
		if err != nil {
			t.Fatalf("list runs: %v", err)
		}
		for _, s := range page {
			tr, err := runs.Trace(ctx, s.RunID)
			if err != nil {
				t.Fatalf("trace run %s: %v", s.RunID, err)
			}
			traces = append(traces, tr)
		}
		if len(page) < 1000 {
			break
		}
		after = page[len(page)-1].Cursor
	}
	slices.SortFunc(traces, func(a, b flowkit.RunTrace) int {
		if c := cmpInt64(a.TriggerEventID, b.TriggerEventID); c != 0 {
			return c
		}
		return strings.Compare(a.Run.DefName, b.Run.DefName)
	})

	out := corpusTrace{Runs: make([]corpusRun, 0, len(traces))}
	for _, tr := range traces {
		r := corpusRun{
			Definition:       tr.Run.DefName,
			Version:          tr.Run.DefVersion,
			MetamodelVersion: tr.MetamodelVersion,
			Trigger: corpusEvent{
				Type: tr.TriggerEvent.Type, Payload: tr.TriggerEvent.Payload, OccurredAt: offset(tr.TriggerEvent.OccurredAt),
			},
			State:    tr.Run.State,
			Terminal: tr.Run.TerminalName,
			Started:  offset(tr.Run.StartedAt),
			Ended:    offset(tr.Run.EndedAt),
			Steps:    make([]corpusStep, len(tr.Steps)),
			Calls:    rec.calls(tr.Run.RunID),
		}
		for i, s := range tr.Steps {
			snap := maps.Clone(s.Snapshot)
			if key, ok := snap["idem_key"].(string); ok {
				snap["idem_key"] = strings.TrimPrefix(key, tr.Run.RunID+":")
			}
			r.Steps[i] = corpusStep{
				At: offset(s.At), Step: s.StepID, Kind: s.Kind, Detail: s.Detail,
				Attempt: s.Attempt, Error: s.Error, Snapshot: snap,
			}
		}
		out.Runs = append(out.Runs, r)
	}
	// Normalise through JSON, as the expected trace is read back.
	var norm corpusTrace
	if err := decodeJSONNumbers(encodeCorpusTrace(t, out), &norm); err != nil {
		t.Fatalf("normalise the trace: %v", err)
	}
	return norm
}

func cmpInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func encodeCorpusTrace(t testing.TB, tr corpusTrace) []byte {
	t.Helper()
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(tr); err != nil {
		t.Fatalf("encode the trace: %v", err)
	}
	return b.Bytes()
}

func readCorpusTrace(t testing.TB, c corpusCase) corpusTrace {
	t.Helper()
	var tr corpusTrace
	if err := decodeJSONNumbers(readCorpusFile(t, c.dir, "expected_trace.json"), &tr); err != nil {
		t.Fatalf("%s: %v", c.expectedPath(), err)
	}
	return tr
}

func decodeJSONNumbers(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// diffCorpusTraces lists where got differs from want, run by run and
// step by step, each element as JSON.
func diffCorpusTraces(want, got corpusTrace) []string {
	var diffs []string
	for i := range max(len(want.Runs), len(got.Runs)) {
		switch {
		case i >= len(got.Runs):
			diffs = append(diffs, fmt.Sprintf("run %d missing: want %s", i, runLabel(want.Runs[i])))
			continue
		case i >= len(want.Runs):
			diffs = append(diffs, fmt.Sprintf("run %d unexpected: got %s", i, runLabel(got.Runs[i])))
			continue
		}
		w, g := want.Runs[i], got.Runs[i]
		label := fmt.Sprintf("run %d (%s)", i, runLabel(w))
		wHead, gHead := w, g
		wHead.Steps, wHead.Calls, gHead.Steps, gHead.Calls = nil, nil, nil, nil
		diffs = append(diffs, diffJSON(label, wHead, gHead)...)
		diffs = append(diffs, diffElems(label+" step", w.Steps, g.Steps)...)
		diffs = append(diffs, diffElems(label+" call", w.Calls, g.Calls)...)
	}
	return diffs
}

func runLabel(r corpusRun) string {
	payload, _ := json.Marshal(r.Trigger.Payload)
	return fmt.Sprintf("%s on %s %s", r.Definition, r.Trigger.Type, payload)
}

func diffElems[T any](label string, want, got []T) []string {
	var diffs []string
	for i := range max(len(want), len(got)) {
		switch {
		case i >= len(got):
			diffs = append(diffs, fmt.Sprintf("%s %d missing: want %s", label, i, mustJSON(want[i])))
		case i >= len(want):
			diffs = append(diffs, fmt.Sprintf("%s %d unexpected: got %s", label, i, mustJSON(got[i])))
		default:
			diffs = append(diffs, diffJSON(fmt.Sprintf("%s %d", label, i), want[i], got[i])...)
		}
	}
	return diffs
}

func diffJSON(label string, want, got any) []string {
	w, g := mustJSON(want), mustJSON(got)
	if w == g {
		return nil
	}
	return []string{fmt.Sprintf("%s:\n    want %s\n    got  %s", label, w, g)}
}

// mustJSON renders v, which holds only JSON values, with sorted keys.
func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("<%v>", err)
	}
	return string(b)
}

// corpusRecorder records every call of an action's Execute by run.
type corpusRecorder struct {
	mu    sync.Mutex
	byRun map[string][]corpusCall
	err   error
}

func newCorpusRecorder() *corpusRecorder {
	return &corpusRecorder{byRun: map[string][]corpusCall{}}
}

// execute is the Execute of a, on Postgres, and its stub, on the Sim.
func (r *corpusRecorder) execute(a corpusAction) func(context.Context, flowkit.ActionInput) (flowkit.ActionResult, error) {
	return func(_ context.Context, in flowkit.ActionInput) (flowkit.ActionResult, error) {
		var params map[string]any
		raw, err := json.Marshal(in.Params)
		if err == nil {
			err = decodeJSONNumbers(raw, &params)
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if err != nil {
			r.err = errors.Join(r.err, fmt.Errorf("params of %s at %s: %w", a.def.Name, in.StepID, err))
			return flowkit.ActionResult{}, flowkit.NotPerformed(err)
		}
		r.byRun[in.RunID] = append(r.byRun[in.RunID], corpusCall{
			Step: in.StepID, Action: a.def.Name, Key: strings.TrimPrefix(in.IdempotencyKey, in.RunID+":"), Params: params,
		})
		var out map[string]any
		if a.output != nil {
			out = a.output(in.Params)
		}
		return flowkit.ActionResult{Output: out}, nil
	}
}

func (r *corpusRecorder) calls(runID string) []corpusCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.byRun[runID])
}

func (r *corpusRecorder) failure() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

// corpusAction is an action of the corpus applications. output computes
// its output facts from its params.
type corpusAction struct {
	def    flowkit.ActionDef
	output func(params map[string]any) map[string]any
}

// corpusActions are the actions of Sellkit and the LMS. Inventory runs out
// for a SKU starting with "oos-".
func corpusActions() []corpusAction {
	return []corpusAction{
		{
			def: flowkit.ActionDef{
				Name: "reserve_inventory",
				ParamsSchema: json.RawMessage(`{
					"type": "object",
					"required": ["order_id", "skus", "units"],
					"properties": {
						"order_id": {"type": "string"},
						"skus": {"type": "array", "items": {"type": "string"}},
						"units": {"type": "array", "items": {"type": "integer"}}
					}
				}`),
				OutputFacts: []flowkit.OutputFact{{Name: "inventory.reserved", Type: flowkit.FactBool}},
			},
			output: func(p map[string]any) map[string]any {
				skus, _ := p["skus"].([]any)
				reserved := !slices.ContainsFunc(skus, func(s any) bool {
					sku, _ := s.(string)
					return strings.HasPrefix(sku, "oos-")
				})
				return map[string]any{"inventory.reserved": reserved}
			},
		},
		{
			def: flowkit.ActionDef{
				Name: "create_shipment",
				ParamsSchema: json.RawMessage(`{
					"type": "object",
					"required": ["order_id", "country"],
					"additionalProperties": false,
					"properties": {
						"order_id": {"type": "string"},
						"country": {"type": "string", "minLength": 2, "maxLength": 2},
						"express": {"type": "boolean"}
					}
				}`),
				OutputFacts: []flowkit.OutputFact{{Name: "shipment.tracking", Type: flowkit.FactString}},
			},
			output: func(p map[string]any) map[string]any {
				return map[string]any{"shipment.tracking": fmt.Sprintf("TRK-%v", p["order_id"])}
			},
		},
		{
			def: flowkit.ActionDef{
				Name: "send_email",
				ParamsSchema: json.RawMessage(`{
					"type": "object",
					"required": ["to", "template"],
					"additionalProperties": false,
					"properties": {
						"to": {"type": "string"},
						"template": {"type": "string", "pattern": "^[a-z_]+$"},
						"vars": {"type": "object", "additionalProperties": {"type": "string"}}
					}
				}`),
				Idempotent: true,
			},
		},
		{
			def: flowkit.ActionDef{
				Name: "open_ticket",
				ParamsSchema: json.RawMessage(`{
					"type": "object",
					"required": ["order_id", "reason"],
					"properties": {
						"order_id": {"type": "string"},
						"reason": {"enum": ["delivery_overdue", "damaged"]},
						"priority": {"type": "integer", "minimum": 1, "maximum": 3}
					}
				}`),
			},
		},
		{
			def: flowkit.ActionDef{
				Name: "refund_order",
				ParamsSchema: json.RawMessage(`{
					"type": "object",
					"required": ["order_id", "amount"],
					"properties": {"order_id": {"type": "string"}, "amount": {"type": "number"}}
				}`),
			},
		},
		{
			def: flowkit.ActionDef{
				Name: "assign_mentor",
				ParamsSchema: json.RawMessage(`{
					"type": "object",
					"required": ["student_id", "course_id"],
					"properties": {"student_id": {"type": "string"}, "course_id": {"type": "string"}}
				}`),
				OutputFacts: []flowkit.OutputFact{{Name: "mentor.id", Type: flowkit.FactString}},
			},
			output: func(p map[string]any) map[string]any {
				return map[string]any{"mentor.id": fmt.Sprintf("mentor-%v", p["course_id"])}
			},
		},
		{
			def: flowkit.ActionDef{
				Name: "grant_badge",
				ParamsSchema: json.RawMessage(`{
					"type": "object",
					"required": ["student_id", "badge"],
					"properties": {"student_id": {"type": "string"}, "badge": {"type": "string"}}
				}`),
			},
		},
	}
}

// corpusRegistry is what the corpus applications register. It is kept
// apart from the registries of other tests: changing it changes the
// expected traces. A customer whose order id starts with "vip-" is a VIP
// with a large lifetime value; a student whose id starts with "returning-"
// has taken courses before.
func corpusRegistry(rec *corpusRecorder) *flowkit.Registry {
	r := flowkit.NewRegistry()
	r.MustAddEvent(flowkit.EventDef{
		Type: "order.paid",
		PayloadSchema: json.RawMessage(`{
			"type": "object",
			"required": ["order_id", "customer_id", "email", "total", "items"],
			"properties": {
				"order_id": {"type": "string"},
				"customer_id": {"type": "string"},
				"email": {"type": "string", "format": "email"},
				"total": {"type": "number"},
				"items": {
					"type": "array",
					"maxItems": 50,
					"items": {
						"type": "object",
						"required": ["sku", "qty"],
						"properties": {
							"sku": {"type": "string", "maxLength": 64},
							"qty": {"type": "integer", "minimum": 1}
						}
					}
				},
				"shipping": {
					"type": "object",
					"required": ["country"],
					"properties": {
						"country": {"type": "string"},
						"express": {"type": "boolean"}
					}
				}
			}
		}`),
		DedupKeyField: "order_id",
		EntityKind:    "order",
		EntityIDField: "order_id",
	})
	r.MustAddEvent(flowkit.EventDef{
		Type: "shipment.delivered",
		PayloadSchema: json.RawMessage(`{
			"type": "object",
			"required": ["order_id"],
			"properties": {"order_id": {"type": "string"}, "carrier": {"type": "string"}}
		}`),
		EntityKind:    "order",
		EntityIDField: "order_id",
	})
	r.MustAddEvent(flowkit.EventDef{
		Type: "student.enrolled",
		PayloadSchema: json.RawMessage(`{
			"type": "object",
			"required": ["student_id", "course_id", "email"],
			"properties": {
				"student_id": {"type": "string"},
				"course_id": {"type": "string"},
				"email": {"type": "string"},
				"name": {"type": "string"},
				"school_id": {"type": "string"}
			}
		}`),
		EntityKind:    "student",
		EntityIDField: "student_id",
		ScopeFields:   map[string]string{"school": "school_id"},
	})
	r.MustAddEvent(flowkit.EventDef{
		Type: "lesson.completed",
		PayloadSchema: json.RawMessage(`{
			"type": "object",
			"required": ["student_id", "lesson_id"],
			"properties": {
				"student_id": {"type": "string"},
				"lesson_id": {"type": "string"},
				"score": {"type": "integer", "minimum": 0, "maximum": 100}
			}
		}`),
		EntityKind:    "student",
		EntityIDField: "student_id",
	})
	for _, a := range corpusActions() {
		def := a.def
		def.Execute = rec.execute(a)
		r.MustAddAction(def)
	}

	vip := func(s flowkit.Scope) bool { return strings.HasPrefix(s.EntityID, "vip-") }
	r.MustAddFact(flowkit.FactDef{
		Name: "customer.lifetime_value", Type: flowkit.FactDouble, AvailableOn: []string{"order.*"},
		Resolve: func(_ context.Context, _ flowkit.Querier, s flowkit.Scope) (any, error) {
			if vip(s) {
				return 2500.0, nil
			}
			return 120.0, nil
		},
	})
	r.MustAddFact(flowkit.FactDef{
		Name: "customer.is_vip", Type: flowkit.FactBool, AvailableOn: []string{"order.paid"},
		Resolve: func(_ context.Context, _ flowkit.Querier, s flowkit.Scope) (any, error) {
			return vip(s), nil
		},
	})
	r.MustAddFact(flowkit.FactDef{
		Name: "student.prior_courses", Type: flowkit.FactInt, AvailableOn: []string{"student.*"},
		Resolve: func(_ context.Context, _ flowkit.Querier, s flowkit.Scope) (any, error) {
			if strings.HasPrefix(s.EntityID, "returning-") {
				return int64(3), nil
			}
			return int64(0), nil
		},
	})
	return r
}

// softTB runs a function against a test without letting it fail the
// test: Error and Fatal are recorded and logged, and Fatal and FailNow end
// only the goroutine run started.
type softTB struct {
	testing.TB
	mu   sync.Mutex
	msgs []string
}

// run calls fn on a goroutine of its own and waits for it to return or
// stop.
func (s *softTB) run(fn func()) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	<-done
}

func (s *softTB) record(msg string) {
	s.mu.Lock()
	s.msgs = append(s.msgs, msg)
	s.mu.Unlock()
}

func (s *softTB) failures() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.msgs)
}

func (s *softTB) Error(args ...any)                 { s.record(fmt.Sprint(args...)) }
func (s *softTB) Errorf(format string, args ...any) { s.record(fmt.Sprintf(format, args...)) }
func (s *softTB) Fail()                             { s.record("Fail called") }
func (s *softTB) Failed() bool                      { return len(s.failures()) > 0 }
func (s *softTB) Fatal(args ...any)                 { s.record(fmt.Sprint(args...)); runtime.Goexit() }
func (s *softTB) Fatalf(format string, args ...any) {
	s.record(fmt.Sprintf(format, args...))
	runtime.Goexit()
}
func (s *softTB) FailNow() { s.record("FailNow called"); runtime.Goexit() }
