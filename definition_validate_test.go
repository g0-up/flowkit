package flowkit_test

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/g0-up/flowkit"
)

func noop(context.Context, flowkit.ActionInput) (flowkit.ActionResult, error) {
	return flowkit.ActionResult{}, nil
}

// sampleRegistry is what the Sellkit and LMS applications register for the
// definitions under testdata.
func sampleRegistry() *flowkit.Registry {
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
				"paid_at": {"type": "string", "format": "date-time"},
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
				"tags": {"type": "array", "items": {"type": "string"}},
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
			"properties": {
				"order_id": {"type": "string"},
				"carrier": {"type": "string"},
				"delivered_at": {"type": "string", "format": "date-time"}
			}
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

	r.MustAddAction(flowkit.ActionDef{
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
		Execute:     noop,
	})
	r.MustAddAction(flowkit.ActionDef{
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
		Execute:     noop,
	})
	r.MustAddAction(flowkit.ActionDef{
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
		Execute:    noop,
	})
	r.MustAddAction(flowkit.ActionDef{
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
		Execute: noop,
	})
	r.MustAddAction(flowkit.ActionDef{
		Name: "refund_order",
		ParamsSchema: json.RawMessage(`{
			"type": "object",
			"required": ["order_id", "amount"],
			"properties": {"order_id": {"type": "string"}, "amount": {"type": "number"}}
		}`),
		Execute: noop,
	})
	r.MustAddAction(flowkit.ActionDef{
		Name: "assign_mentor",
		ParamsSchema: json.RawMessage(`{
			"type": "object",
			"required": ["student_id", "course_id"],
			"properties": {"student_id": {"type": "string"}, "course_id": {"type": "string"}}
		}`),
		OutputFacts: []flowkit.OutputFact{{Name: "mentor.id", Type: flowkit.FactString}},
		Execute:     noop,
	})
	r.MustAddAction(flowkit.ActionDef{
		Name: "grant_badge",
		ParamsSchema: json.RawMessage(`{
			"type": "object",
			"required": ["student_id", "badge"],
			"properties": {"student_id": {"type": "string"}, "badge": {"type": "string"}}
		}`),
		Execute: noop,
	})

	resolve := func(context.Context, flowkit.Querier, flowkit.Scope) (any, error) { return nil, nil }
	r.MustAddFact(flowkit.FactDef{Name: "customer.lifetime_value", Type: flowkit.FactDouble, AvailableOn: []string{"order.*"}, Resolve: resolve})
	r.MustAddFact(flowkit.FactDef{Name: "customer.is_vip", Type: flowkit.FactBool, AvailableOn: []string{"order.paid"}, Resolve: resolve})
	r.MustAddFact(flowkit.FactDef{Name: "student.prior_courses", Type: flowkit.FactInt, AvailableOn: []string{"student.*"}, Resolve: resolve})
	return r
}

func readTestdata(t *testing.T, name string) json.RawMessage {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func validationErrors(t *testing.T, err error) flowkit.ValidationErrors {
	t.Helper()
	var verrs flowkit.ValidationErrors
	if !errors.As(err, &verrs) {
		t.Fatalf("error %v is not ValidationErrors", err)
	}
	return verrs
}

func TestValidateDefinitionAcceptsTheSamples(t *testing.T) {
	reg := sampleRegistry()
	for _, name := range []string{"sellkit-fulfillment.json", "lms-onboarding.json"} {
		t.Run(name, func(t *testing.T) {
			if err := reg.ValidateDefinition(readTestdata(t, filepath.Join("valid", name))); err != nil {
				t.Fatalf("ValidateDefinition: %v", err)
			}
		})
	}
}

func TestValidateDefinitionChecksNumbers(t *testing.T) {
	reg := sampleRegistry()
	body := string(readTestdata(t, "invalid/number-out-of-range.json"))
	for _, n := range []string{"1e131000", "1e-1000000000", "0e-999999"} {
		err := reg.ValidateDefinition(json.RawMessage(strings.Replace(body, "1e131072", n, 1)))
		if verrs := validationErrors(t, err); len(verrs) != 1 || verrs[0].Path != "/steps/0/params/amount" {
			t.Errorf("amount %s: %v", n, err)
		}
	}
	for _, n := range []string{"9007199254740993", "1e308", "5e-324", "12.50"} {
		if err := reg.ValidateDefinition(json.RawMessage(strings.Replace(body, "1e131072", n, 1))); err != nil {
			t.Errorf("amount %s: %v", n, err)
		}
	}
}

// An expression next to literals that a oneOf, anyOf, not or if chooses
// between decides which literals are valid, so those literals are checked
// only once the params are resolved; everything else is still checked.
func TestValidateDefinitionLeavesSchemaBranchesToRunTime(t *testing.T) {
	r := flowkit.NewRegistry()
	r.MustAddEvent(flowkit.EventDef{
		Type: "order.paid",
		PayloadSchema: json.RawMessage(`{
			"type": "object",
			"required": ["order_id", "channel"],
			"properties": {"order_id": {"type": "string"}, "channel": {"type": "string"}, "rush": {"type": "boolean"}}
		}`),
		EntityKind:    "order",
		EntityIDField: "order_id",
	})
	r.MustAddAction(flowkit.ActionDef{
		Name: "notify",
		ParamsSchema: json.RawMessage(`{
			"type": "object",
			"required": ["contact"],
			"properties": {
				"priority": {"type": "integer"},
				"contact": {"$ref": "#/$defs/contact"}
			},
			"$defs": {
				"contact": {
					"type": "object",
					"required": ["kind"],
					"properties": {"kind": {"type": "string"}, "phone": {"type": "string"}, "email": {"type": "string"}},
					"oneOf": [
						{"properties": {"kind": {"const": "sms"}}, "required": ["phone"]},
						{"properties": {"kind": {"const": "email"}}, "required": ["email"]}
					]
				}
			}
		}`),
		Execute: noop,
	})
	r.MustAddAction(flowkit.ActionDef{
		Name: "ship",
		ParamsSchema: json.RawMessage(`{
			"type": "object",
			"properties": {"rush": {"type": "boolean"}, "carrier": {"type": "string"}},
			"not": {"required": ["carrier"], "properties": {"carrier": {"const": "post"}, "rush": {"not": {"const": true}}}}
		}`),
		Execute: noop,
	})
	define := func(action, params string) json.RawMessage {
		return json.RawMessage(`{
			"metamodel_version": 1,
			"trigger": {"event": "order.paid"},
			"steps": [
				{"id": "act", "type": "action", "action": "` + action + `", "params": ` + params + `, "next": "done"},
				{"id": "done", "type": "end", "name": "done"}
			]
		}`)
	}

	for _, tc := range []struct{ name, action, params string }{
		{"oneOf discriminator", "notify", `{"contact": {"kind": {"$expr": "event.channel"}, "phone": "+1555"}}`},
		{"not at the root", "ship", `{"rush": {"$expr": "has(event.rush) && event.rush"}, "carrier": "post"}`},
	} {
		if err := r.ValidateDefinition(define(tc.action, tc.params)); err != nil {
			t.Errorf("%s: %v", tc.name, err)
		}
	}

	for _, tc := range []struct{ name, action, params, path, msg string }{
		{"literal outside the branch", "notify",
			`{"priority": "high", "contact": {"kind": {"$expr": "event.channel"}, "phone": "+1555"}}`,
			"/steps/0/params/priority", "want integer"},
		{"literal branch without an expression", "notify",
			`{"contact": {"kind": "sms"}}`,
			"/steps/0/params/contact/phone", "missing required property"},
		{"expression of the wrong type", "notify",
			`{"contact": {"kind": {"$expr": "event.order_id == 'A1'"}, "phone": "+1555"}}`,
			"/steps/0/params/contact/kind/$expr", "want string"},
	} {
		verrs := validationErrors(t, r.ValidateDefinition(define(tc.action, tc.params)))
		if !slices.ContainsFunc(verrs, func(e *flowkit.ValidationError) bool {
			return e.Path == tc.path && strings.Contains(e.Msg, tc.msg)
		}) {
			t.Errorf("%s: %v, want %s: %s", tc.name, verrs, tc.path, tc.msg)
		}
	}
}

func TestValidateDefinitionCorpus(t *testing.T) {
	// Each invalid definition has one mistake, caught at these paths with
	// these message fragments.
	type want struct{ path, msg string }
	corpus := map[string][]want{
		"action-unregistered.json":           {{"/steps/0/action", `action "send_sms" is not registered`}},
		"dedup-window-missing.json":          {{"/concurrency/dedup_window", "required by policy"}},
		"estimated-cost-exceeded.json":       {{"/steps/0/cases/0/when", "estimated cost"}},
		"expression-syntax-error.json":       {{"/steps/0/when", "1:14: Syntax error"}},
		"fact-not-available-on-trigger.json": {{"/steps/0/cases/0/when", "undefined field 'customer'"}},
		"fact-undeclared.json":               {{"/steps/0/cases/0/when", "undefined field 'churn_risk'"}},
		"field-misspelt-nested.json":         {{"/steps/0/cases/0/when", "undefined field 'contry'"}},
		"has-guard-missing.json":             {{"/steps/0/params/country/$expr", "guard the read with has(event.shipping)"}},
		"list-element-field-misspelt.json":   {{"/steps/0/cases/0/when", "undefined field 'quantity'"}},
		"loop-without-exit.json": {
			{"/steps/1", "no path from this step reaches an end step"},
			{"/steps/2", "no path from this step reaches an end step"},
		},
		"map-key-guard-missing.json":     {{"/steps/0/params/vars/school/$expr", "guard the read with has(scope.extra.school)"}},
		"metamodel-version-unknown.json": {{"/metamodel_version", "unsupported metamodel version 2"}},
		"next-step-unknown.json": {
			{"/steps/0", "no path from this step reaches an end step"},
			{"/steps/0/next", `unknown step "finish"`},
			{"/steps/1", "unreachable"},
		},
		"nul-character.json":                {{"/description", "U+0000"}},
		"number-out-of-range.json":          {{"/steps/0/params/amount", "number is out of the float64 range"}},
		"output-fact-before-action.json":    {{"/steps/0/params/vars/tracking/$expr", "undefined field 'shipment'"}},
		"output-fact-not-certain.json":      {{"/steps/1/cases/0/when", "guard the read with has(facts.inventory.reserved)"}},
		"params-expression-wrong-type.json": {{"/steps/0/params/express/$expr", "type double, want bool"}},
		"params-literal-invalid.json":       {{"/steps/0/params/template", "does not match pattern"}},
		"params-required-missing.json":      {{"/steps/0/params/template", "missing required property"}},
		"serial-key-missing.json":           {{"/concurrency/serial_key", `required by policy "serial"`}},
		"serial-key-not-string.json":        {{"/concurrency/serial_key", "type double, want string"}},
		"step-field-unknown.json":           {{"/steps/0", "'retries' not allowed"}},
		"step-id-duplicate.json":            {{"/steps/2/id", `duplicate step id "done"`}},
		"step-unreachable.json":             {{"/steps/2", "unreachable"}},
		"switch-case-not-bool.json":         {{"/steps/0/cases/0/when", "type double, want bool"}},
		"trigger-event-unregistered.json":   {{"/trigger/event", `event "order.placed" is not registered`}},
		"trigger-missing.json":              {{"/trigger", "missing required property"}},
		"wait-event-unregistered.json":      {{"/steps/0/event", `event "shipment.shipped" is not registered`}},
		"wait-match-field-of-trigger.json":  {{"/steps/0/match", "undefined field 'customer_id'"}},
		"wait-timeout-too-long.json":        {{"/steps/0/timeout", "at most 365 days"}},
	}

	files, err := filepath.Glob(filepath.Join("testdata", "invalid", "*.json"))
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
	if len(names) < 18 {
		t.Fatalf("corpus has %d files, want at least 18", len(names))
	}

	reg := sampleRegistry()
	for _, name := range names {
		t.Run(strings.TrimSuffix(name, ".json"), func(t *testing.T) {
			err := reg.ValidateDefinition(readTestdata(t, filepath.Join("invalid", name)))
			if err == nil {
				t.Fatal("ValidateDefinition accepted the definition")
			}
			got := validationErrors(t, err)
			wants := corpus[name]
			if len(got) != len(wants) {
				t.Fatalf("got %d errors, want %d:\n%v", len(got), len(wants), err)
			}
			for i, w := range wants {
				if got[i].Path != w.path || !strings.Contains(got[i].Msg, w.msg) {
					t.Errorf("error %d is %q: %q, want %q containing %q", i, got[i].Path, got[i].Msg, w.path, w.msg)
				}
			}
		})
	}
}
