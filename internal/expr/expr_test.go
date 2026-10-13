package expr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"cel.dev/cel-go/common/types"
)

const orderSchema = `{
  "type": "object",
  "required": ["order_id", "qty", "items", "customer"],
  "properties": {
    "order_id": {"type": "string"},
    "qty": {"type": "integer"},
    "total": {"type": "number"},
    "placed_at": {"type": "string", "format": "date-time"},
    "note": {"type": "string"},
    "customer": {
      "type": "object",
      "required": ["id"],
      "properties": {
        "id": {"type": "string"},
        "tier": {"type": "string"},
        "address": {"$ref": "#/$defs/address"}
      }
    },
    "items": {
      "type": "array",
      "maxItems": 10,
      "items": {
        "type": "object",
        "required": ["sku", "qty"],
        "properties": {
          "sku": {"type": "string", "maxLength": 64},
          "qty": {"type": "integer"}
        }
      }
    },
    "tags": {"type": "array", "items": {"type": "string"}},
    "attrs": {"type": "object", "additionalProperties": {"type": "string"}},
    "meta": {"oneOf": [{"type": "string"}, {"type": "integer"}]}
  },
  "$defs": {
    "address": {
      "type": "object",
      "required": ["country"],
      "properties": {"country": {"type": "string"}, "zip": {"type": "string"}}
    }
  }
}`

var testFacts = []Fact{
	{Name: "customer.revenue_90d", Type: Double},
	{Name: "customer.segment", Type: String},
	{Name: "risk_score", Type: Int},
	{Name: "vip", Type: Bool, Optional: true},
	{Name: "last_seen", Type: Timestamp},
}

func mustShape(t *testing.T, doc string) *Shape {
	t.Helper()
	s, err := NewShape(json.RawMessage(doc))
	if err != nil {
		t.Fatalf("NewShape: %v", err)
	}
	return s
}

func mustMetamodel(t *testing.T) MetamodelOptions {
	t.Helper()
	mm, err := Metamodel(1)
	if err != nil {
		t.Fatalf("Metamodel: %v", err)
	}
	return mm
}

func testEnv(t *testing.T) *Env {
	t.Helper()
	env, err := NewEnv(mustShape(t, orderSchema), testFacts, mustMetamodel(t))
	if err != nil {
		t.Fatalf("NewEnv: %v", err)
	}
	return env
}

func mustCompile(t *testing.T, env *Env, src string, want Want) *Program {
	t.Helper()
	p, errs := env.Compile(src, want)
	if len(errs) > 0 {
		t.Fatalf("Compile(%q): %v", src, errs)
	}
	return p
}

func TestCompileAccepts(t *testing.T) {
	env := testEnv(t)
	for _, src := range []string{
		`event.qty == 3`,
		`event.customer.id == "c1" && event.order_id != ""`,
		`has(event.note) && event.note != ""`,
		`event.note != "" && has(event.note)`,
		`!has(event.note) || event.note == "x"`,
		`has(event.customer.tier) ? event.customer.tier == "gold" : false`,
		`has(event.customer.address) && has(event.customer.address.zip) && event.customer.address.zip.startsWith("7")`,
		`has(event.customer.address) && event.customer.address.country == "VN"`,
		`has(event.attrs) && has(event.attrs.color) && event.attrs.color == "red"`,
		`event.items.exists(i, i.qty > 1 && i.sku.startsWith("A"))`,
		`event.items.map(i, i.qty).exists(q, q > 2)`,
		`event.items.filter(i, i.qty > 1).size() > 0`,
		`has(event.placed_at) && event.placed_at < now`,
		`has(event.total) && event.total > 10.5`,
		`has(event.meta) && event.meta == 1`,
		`scope.entity_kind == "order" && scope.entity_id == event.order_id`,
		`has(scope.extra.region) && scope.extra.region == "eu"`,
		`facts.customer.revenue_90d > 100.0 && facts.risk_score < 5`,
		`has(facts.vip) && facts.vip`,
		`facts.last_seen < now - duration("24h")`,
		`"region" in scope.extra && scope.extra["region"] == "eu"`,
		`has(scope.extra.region) && scope.extra["region"] == "eu"`,
		`"region" in scope.extra && scope.extra.region == "eu"`,
		`!("region" in scope.extra) || scope.extra["region"] == "eu"`,
		`has(event.attrs) && "a-b" in event.attrs && event.attrs["a-b"] == "x"`,
		`has(event.attrs) && event.attrs.exists(k, event.attrs[k] == "red")`,
		`event.order_id in scope.extra ? scope.extra[event.order_id] == "x" : false`,
		`event.items[0].sku == "A"`,
	} {
		if _, errs := env.Compile(src, WantBool); len(errs) > 0 {
			t.Errorf("Compile(%q): %v", src, errs)
		}
	}
}

func TestCompileRejects(t *testing.T) {
	env := testEnv(t)
	for _, tc := range []struct {
		src, want string
	}{
		{`event.qtyy == 3`, "undefined field 'qtyy'"},
		{`event.customer.idd == "x"`, "undefined field 'idd'"},
		{`has(event.customer.address) && event.customer.address.countryy == "VN"`, "undefined field 'countryy'"},
		{`event.items.exists(i, i.skuu == "x")`, "undefined field 'skuu'"},
		{`facts.customer.revenue == 1.0`, "undefined field 'revenue'"},
		{`event.qty == "3"`, "no matching overload"},
		{`event.note == "x"`, "event.note may be absent; guard the read with has(event.note)"},
		{`event.customer.address.country == "VN"`, "event.customer.address may be absent"},
		{`event.attrs.color == "red"`, "event.attrs may be absent"},
		{`has(event.customer.address.zip)`, "event.customer.address may be absent"},
		{`has(event.attrs) && event.attrs.color == "red"`, "event.attrs.color may be absent"},
		{`has(event.note) || event.note == "x"`, "event.note may be absent"},
		{`facts.vip`, "facts.vip may be absent"},
		{`event.items.exists(i, has(event.note)) && event.note == ""`, "event.note may be absent"},
		{`event.qty`, "expression has type int, want bool"},
		{`scope.extra["region"] == "eu"`, "scope.extra.region may be absent; guard the read with has(scope.extra.region)"},
		{`"region" in scope.extra || scope.extra["region"] == "eu"`, "scope.extra.region may be absent"},
		{`"country" in scope.extra && scope.extra["region"] == "eu"`, "scope.extra.region may be absent"},
		{`has(event.attrs) && event.attrs["a-b"] == "x"`, `event.attrs["a-b"] may be absent; guard the read with "a-b" in event.attrs`},
		{`scope.extra[event.order_id] == "x"`, "scope.extra[event.order_id] may be absent; guard the read with event.order_id in scope.extra"},
		{`event.tags.exists(t, scope.extra[t] == "x")`, "scope.extra[t] may be absent"},
		{`scope.extra.exists(k, event.tags.exists(k, scope.extra[k] == "x"))`, "scope.extra[k] may be absent"},
		{`{"a": "b"}[event.order_id] == "b"`, "a map key may be absent and its presence cannot be checked here"},
		{`event.qty ==`, "1:13:"},
		{`event.tags.exists(a, event.tags.exists(b, event.tags.exists(c, a + b + c == "x")))`, "estimated cost"},
		{`size(event) > 0 && undeclared_var`, "undeclared reference"},
	} {
		_, errs := env.Compile(tc.src, WantBool)
		if len(errs) == 0 {
			t.Errorf("Compile(%q) succeeded, want error containing %q", tc.src, tc.want)
			continue
		}
		if msg := fmt.Sprint(errs); !strings.Contains(msg, tc.want) {
			t.Errorf("Compile(%q) = %s, want error containing %q", tc.src, msg, tc.want)
		}
	}
}

func TestCompileErrorLocation(t *testing.T) {
	env := testEnv(t)
	_, errs := env.Compile("event.qty > 1 &&\n  event.note == \"x\"", WantBool)
	if len(errs) != 1 {
		t.Fatalf("errs = %v, want one", errs)
	}
	var e *Error
	if !errors.As(errs[0], &e) || e.Line != 2 || e.Column != 8 {
		t.Fatalf("error = %#v, want line 2 column 8", errs[0])
	}
}

func TestEventEnvHasNoFacts(t *testing.T) {
	env, err := NewEventEnv(mustShape(t, orderSchema), mustMetamodel(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, errs := env.Compile(`facts.risk_score > 1`, WantBool); len(errs) == 0 {
		t.Fatal("facts compiled in an event environment")
	}
	mustCompile(t, env, `event.order_id`, WantString)
}

func TestMacrosOutsideMetamodelAreUndefined(t *testing.T) {
	env, err := NewEnv(mustShape(t, orderSchema), nil, MetamodelOptions{Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, errs := env.Compile(`event.items.exists(i, i.qty > 1)`, WantBool); len(errs) == 0 {
		t.Fatal("exists compiled without the macro")
	}
}

func TestShapeWant(t *testing.T) {
	s := mustShape(t, `{
	  "type": "object",
	  "properties": {
	    "n": {"type": "integer"},
	    "x": {"type": "number"},
	    "b": {"type": "boolean"},
	    "s": {"type": "string"},
	    "at": {"type": "string", "format": "date-time"},
	    "skus": {"type": "array", "items": {"type": "string"}},
	    "obj": {"type": "object", "properties": {"k": {"type": "integer"}}},
	    "m": {"type": "object"}
	  }
	}`)
	for ptr, want := range map[string]string{
		"/n": "int", "/x": "number", "/b": "bool", "/s": "string", "/at": "timestamp or string",
		"/skus": "list(string)", "/obj": "any value", "/obj/k": "int", "/m": "any value",
		"/m/anything": "any value", "/missing": "any value",
	} {
		if got := s.Want(ptr).String(); got != want {
			t.Errorf("Want(%q) = %q, want %q", ptr, got, want)
		}
	}
	env := testEnv(t)
	mustCompile(t, env, `event.qty * 2`, s.Want("/x"))
	mustCompile(t, env, `event.items.map(i, i.sku)`, s.Want("/skus"))
	if _, errs := env.Compile(`event.order_id`, s.Want("/n")); len(errs) == 0 {
		t.Fatal("string compiled where an int is wanted")
	}
}

func orderPayload() string {
	return `{"order_id": "o1", "qty": 3, "total": 12.5, "placed_at": "2026-10-01T10:00:00Z",
	  "customer": {"id": "c1", "tier": "gold", "address": {"country": "VN"}},
	  "items": [{"sku": "A1", "qty": 2}, {"sku": "B2", "qty": 1}],
	  "attrs": {"color": "red"}, "meta": 7}`
}

func evalBool(t *testing.T, p *Program, vars Vars) bool {
	t.Helper()
	v, err := p.Eval(context.Background(), vars)
	if err != nil {
		t.Fatalf("Eval(%q): %v", p.Source(), err)
	}
	b, ok := v.(types.Bool)
	if !ok {
		t.Fatalf("Eval(%q) = %v, want bool", p.Source(), v)
	}
	return bool(b)
}

func TestEval(t *testing.T) {
	env := testEnv(t)
	shape := mustShape(t, orderSchema)
	event, err := Activation(json.RawMessage(orderPayload()), shape)
	if err != nil {
		t.Fatal(err)
	}
	facts, err := env.FactValues(map[string]any{
		"customer.revenue_90d": json.Number("150"),
		"risk_score":           int64(2),
		"last_seen":            "2026-10-01T00:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	vars := Vars{
		Event: event,
		Facts: facts,
		Now:   time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC),
		Scope: Scope{EntityKind: "order", EntityID: "o1", Extra: map[string]string{"region": "eu"}},
	}
	for src, want := range map[string]bool{
		`event.qty == 3`:                         true,
		`has(event.total) && event.total > 12.0`: true,
		`has(event.customer.address) && event.customer.address.country == "VN"`: true,
		`event.items.exists(i, i.qty == 2)`:                                     true,
		`event.items.all(i, i.qty > 1)`:                                         false,
		`has(event.placed_at) && event.placed_at < now`:                         true,
		`has(event.note)`:                    false,
		`has(event.meta) && event.meta == 7`: true,
		`scope.entity_id == event.order_id && has(scope.extra.region)`: true,
		`has(scope.extra.region) && scope.extra.region == "eu"`:        true,
		`facts.customer.revenue_90d > 100.0`:                           true,
		`has(facts.customer.segment)`:                                  false,
		`has(facts.vip)`:                                               false,
		`facts.last_seen < now - duration("24h")`:                      true,
	} {
		p := mustCompile(t, env, src, WantBool)
		if got := evalBool(t, p, vars); got != want {
			t.Errorf("Eval(%q) = %v, want %v", src, got, want)
		}
	}
}

func TestActivationCoercesToSchema(t *testing.T) {
	shape := mustShape(t, orderSchema)
	got, err := Activation(json.RawMessage(`{"order_id": "o1", "qty": 3.0, "total": 4,
	  "customer": {"id": "c1"}, "items": [{"sku": "A", "qty": 1e1}],
	  "placed_at": "2026-10-01T10:00:00.5+07:00", "meta": 2.5, "tags": ["x"]}`), shape)
	if err != nil {
		t.Fatal(err)
	}
	if got["qty"] != int64(3) || got["total"] != float64(4) || got["meta"] != 2.5 {
		t.Errorf("scalars = %#v %#v %#v", got["qty"], got["total"], got["meta"])
	}
	if q := got["items"].([]any)[0].(map[string]any)["qty"]; q != int64(10) {
		t.Errorf("items/0/qty = %#v, want int64(10)", q)
	}
	at, ok := got["placed_at"].(time.Time)
	if !ok || !at.Equal(time.Date(2026, 10, 1, 3, 0, 0, 5e8, time.UTC)) {
		t.Errorf("placed_at = %#v", got["placed_at"])
	}
	zero, err := Activation(json.RawMessage(`{"qty": 0e-1000000000, "items": [], "customer": {"id": "c"}, "order_id": "o"}`), mustShape(t, orderSchema))
	if err != nil || zero["qty"] != int64(0) {
		t.Errorf("qty 0e-1000000000 = %#v, %v", zero["qty"], err)
	}
	dyn, err := Activation(json.RawMessage(`{"a": {"n": 2, "f": 2.5, "l": [1, 1.5]}}`), mustShape(t, `{"type": "object"}`))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"a": map[string]any{"n": int64(2), "f": 2.5, "l": []any{int64(1), 1.5}}}
	if !reflect.DeepEqual(dyn, want) {
		t.Errorf("dyn = %#v, want %#v", dyn, want)
	}
}

func TestActivationRejects(t *testing.T) {
	shape := mustShape(t, orderSchema)
	for payload, want := range map[string]string{
		`{"qty": 3.5}`:               "/qty: must be an integer, got 3.5",
		`{"qty": 1e400}`:             "/qty: integer 1e400 does not fit in 64 bits",
		`{"qty": 1e1000000000}`:      "/qty: integer 1e1000000000 does not fit in 64 bits",
		`{"qty": 1e-1000000000}`:     "/qty: must be an integer, got 1e-1000000000",
		`{"qty": 5.0e-400}`:          "/qty: must be an integer, got 5.0e-400",
		`{"qty": "3"}`:               "/qty: must be an integer, got string",
		`{"items": [{"qty": 0.1}]}`:  "/items/0/qty: must be an integer",
		`{"customer": []}`:           "/customer: must be an object, got an array",
		`{"placed_at": "yesterday"}`: "/placed_at: must be an RFC 3339 date-time",
		`{"attrs": {"a/b": 1}}`:      "/attrs/a~1b: must be a string",
		`[1]`:                        "payload must be a JSON object",
		`{"qty": 1} {}`:              "payload has data after the JSON value",
		`{"qty": `:                   "payload is not valid JSON",
	} {
		_, err := Activation(json.RawMessage(payload), shape)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Activation(%s) = %v, want error containing %q", payload, err, want)
		}
	}
}

func TestRefs(t *testing.T) {
	env := testEnv(t)
	for src, want := range map[string][]string{
		`event.qty == 1`:                                       nil,
		`facts.customer.revenue_90d > 100.0`:                   {"customer.revenue_90d"},
		`has(facts.vip) && facts.risk_score > 1`:               {"risk_score", "vip"},
		`[facts.customer].size() == 1`:                         {"customer.revenue_90d", "customer.segment"},
		`[facts].size() == 1`:                                  {"customer.revenue_90d", "customer.segment", "last_seen", "risk_score", "vip"},
		`[1, 2].exists(facts, facts > 1)`:                      nil,
		`[1].exists(facts, facts > 1) || facts.risk_score > 0`: {"risk_score"},
	} {
		p := mustCompile(t, env, src, WantBool)
		if got := p.Refs(); !reflect.DeepEqual(got, want) {
			t.Errorf("Refs(%q) = %q, want %q", src, got, want)
		}
	}
}

func TestFactValues(t *testing.T) {
	env := testEnv(t)
	got, err := env.FactValues(map[string]any{"risk_score": 3.0, "vip": true, "unknown": 1})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"customer": map[string]any{}, "risk_score": int64(3), "vip": true}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("FactValues = %#v, want %#v", got, want)
	}
	if _, err := env.FactValues(map[string]any{"risk_score": 3.5}); err == nil || !strings.Contains(err.Error(), `fact "risk_score"`) {
		t.Errorf("FactValues(3.5) = %v, want an integer error", err)
	}
}

func TestFactTreeRejectsClashes(t *testing.T) {
	mm := mustMetamodel(t)
	shape := mustShape(t, orderSchema)
	for _, facts := range [][]Fact{
		{{Name: "a", Type: Int}, {Name: "a.b", Type: Int}},
		{{Name: "a.b", Type: Int}, {Name: "a.b", Type: String}},
		{{Name: "a..b", Type: Int}},
		{{Name: "a", Type: Type(99)}},
	} {
		if _, err := NewEnv(shape, facts, mm); err == nil {
			t.Errorf("NewEnv(%v) succeeded", facts)
		}
	}
}

// bigList is an event whose tags exceed their schema bounds in number and
// length, so evaluation costs more than the estimate allowed for.
func bigList(n int) map[string]any {
	tags := make([]any, n)
	for i := range tags {
		tags[i] = fmt.Sprint(i) + strings.Repeat("x", 2000)
	}
	return map[string]any{"tags": tags}
}

const boundedTags = `{"type": "object", "required": ["tags"], "properties": {
  "tags": {"type": "array", "maxItems": 10, "items": {"type": "string", "maxLength": 8}}}}`

func TestEvalCostLimit(t *testing.T) {
	env, err := NewEventEnv(mustShape(t, boundedTags), mustMetamodel(t))
	if err != nil {
		t.Fatal(err)
	}
	p := mustCompile(t, env, `event.tags.exists(a, event.tags.exists(b, event.tags.exists(c, a + b + c == "x")))`, WantBool)
	// The cost limit, not the timeout, must stop this evaluation, even
	// under the race detector.
	p.timeout = time.Minute
	_, err = p.Eval(context.Background(), Vars{Event: bigList(500)})
	var le *LimitError
	if !errors.As(err, &le) || le.Timeout || !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("Eval = %v, want a cost LimitError", err)
	}
}

func TestEvalTimeout(t *testing.T) {
	env, err := NewEventEnv(mustShape(t, boundedTags), mustMetamodel(t))
	if err != nil {
		t.Fatal(err)
	}
	p := mustCompile(t, env, `event.tags.exists(a, event.tags.exists(b, a + b == "x"))`, WantBool)
	p.timeout = time.Nanosecond
	_, err = p.Eval(context.Background(), Vars{Event: bigList(500)})
	var le *LimitError
	if !errors.As(err, &le) || !le.Timeout {
		t.Fatalf("Eval = %v, want a timeout LimitError", err)
	}
}

func TestEvalCancelled(t *testing.T) {
	env, err := NewEventEnv(mustShape(t, boundedTags), mustMetamodel(t))
	if err != nil {
		t.Fatal(err)
	}
	p := mustCompile(t, env, `event.tags.exists(a, event.tags.exists(b, a + b == "x"))`, WantBool)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = p.Eval(ctx, Vars{Event: bigList(500)})
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("Eval = %v, want context.Canceled", err)
	}
}

func TestEvalChecksDynResult(t *testing.T) {
	env := testEnv(t)
	p := mustCompile(t, env, `has(event.meta) ? event.meta : ""`, WantString)
	event, err := Activation(json.RawMessage(`{"meta": 7}`), mustShape(t, orderSchema))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Eval(context.Background(), Vars{Event: event}); err == nil || !strings.Contains(err.Error(), "want string") {
		t.Fatalf("Eval = %v, want a result type error", err)
	}
}

func TestToNative(t *testing.T) {
	env := testEnv(t)
	at := time.Date(2026, 10, 12, 1, 2, 3, 0, time.UTC)
	p := mustCompile(t, env, `{"n": event.qty, "skus": event.items.map(i, i.sku), "at": now, "x": 1.5, "ok": true, "nil": null}`, WantAny)
	v, err := p.Eval(context.Background(), Vars{
		Event: map[string]any{"qty": int64(2), "items": []any{map[string]any{"sku": "A", "qty": int64(1)}}},
		Now:   at,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := ToNative(v)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"n": int64(2), "skus": []any{"A"}, "at": "2026-10-12T01:02:03Z", "x": 1.5, "ok": true, "nil": nil}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ToNative = %#v, want %#v", got, want)
	}
}

func TestShapeConversion(t *testing.T) {
	// A recursive $ref types as dyn instead of looping.
	s := mustShape(t, `{"$ref": "#/$defs/node", "$defs": {"node": {"type": "object",
	  "properties": {"name": {"type": "string"}, "child": {"$ref": "#/$defs/node"}}}}}`)
	env, err := NewEventEnv(s, mustMetamodel(t))
	if err != nil {
		t.Fatal(err)
	}
	mustCompile(t, env, `has(event.child) && has(event.child.child) && event.child.child.anything == 1`, WantBool)
	if _, errs := env.Compile(`has(event.nam)`, WantBool); len(errs) == 0 {
		t.Fatal("unknown field of a recursive object compiled")
	}
	for _, doc := range []string{`{"$ref": "#/missing"}`, `{"$ref": "other.json"}`, `{"$ref": "#anchor"}`, `not json`} {
		if _, err := NewShape(json.RawMessage(doc)); err == nil {
			t.Errorf("NewShape(%s) succeeded", doc)
		}
	}
}

func TestShapeBranch(t *testing.T) {
	s := mustShape(t, `{
		"type": "object",
		"properties": {
			"plain": {"type": "object", "properties": {"a": {"type": "string"}}},
			"contact": {"$ref": "#/$defs/contact"},
			"list": {"type": "array", "prefixItems": [{"type": "string"}], "items": {"anyOf": [{"type": "string"}, {"type": "integer"}]}},
			"merged": {"allOf": [{"properties": {"x": {"if": {"type": "string"}, "then": {"minLength": 1}}}}]},
			"a~b/c": {"type": "object", "additionalProperties": {"not": {"type": "null"}}},
			"loop": {"$ref": "#/properties/loop"}
		},
		"$defs": {
			"contact": {"type": "object", "properties": {"kind": {"type": "string"}}, "oneOf": [{"required": ["phone"]}, {"required": ["email"]}]}
		}
	}`)
	for pointer, want := range map[string]string{
		"/contact/kind":    "/contact",
		"/contact":         "/contact",
		"/list/1/deep":     "/list/1",
		"/merged/x":        "/merged/x",
		"/a~0b~1c/k/z":     "/a~0b~1c/k",
		"/plain/a":         "",
		"/list/0":          "",
		"/undescribed/a/b": "",
		"/loop/x":          "",
		"":                 "",
	} {
		got, ok := s.Branch(pointer)
		if got != want || ok != (want != "") {
			t.Errorf("Branch(%q) = %q, %v; want %q", pointer, got, ok, want)
		}
	}

	root := mustShape(t, `{"type": "object", "anyOf": [{"required": ["a"]}, {"required": ["b"]}]}`)
	if got, ok := root.Branch("/a"); got != "" || !ok {
		t.Errorf("Branch under a root anyOf = %q, %v; want \"\", true", got, ok)
	}
}
