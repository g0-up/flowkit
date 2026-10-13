package model

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// body builds a definition body around steps, a JSON array.
func body(concurrency, steps string) []byte {
	b := `{"metamodel_version":1,"trigger":{"event":"order.paid"},`
	if concurrency != "" {
		b += `"concurrency":` + concurrency + `,`
	}
	return []byte(b + `"steps":` + steps + `}`)
}

const endStep = `{"id":"done","type":"end","name":"completed"}`

func TestParseValid(t *testing.T) {
	b := body(`{"policy":"dedup_window","serial_key":"event.customer_id","dedup_window":"1h30m"}`, `[
		{"id":"charge","type":"action","action":"payment.capture","params":{"amount":{"$expr":"event.total"},"note":"x"},"when":"event.total > 0","next":"wait"},
		{"id":"wait","type":"wait_for_event","event":"order.shipped","match":"event.order_id == 1","timeout":"7d","on_timeout":"route","next":"done"},
		{"id":"route","type":"switch","cases":[{"when":"true","next":"done"}],"default":"done"},
		`+endStep+`]`)
	d, vs := Parse(b)
	if len(vs) > 0 {
		t.Fatalf("violations: %v", vs)
	}
	if d.Trigger.Event != "order.paid" || d.MetamodelVersion != 1 {
		t.Fatalf("trigger %+v version %d", d.Trigger, d.MetamodelVersion)
	}
	want := Concurrency{Policy: PolicyDedupWindow, SerialKey: "event.customer_id", DedupWindow: 90 * time.Minute}
	if d.Concurrency != want {
		t.Fatalf("concurrency %+v, want %+v", d.Concurrency, want)
	}
	if d.Entry().ID != "charge" || d.Steps[0].Action.When != "event.total > 0" {
		t.Fatalf("entry %+v", d.Steps[0])
	}
	w, ok := d.Step("wait")
	if !ok || w.Wait.Timeout != 7*24*time.Hour || w.Wait.OnTimeout != "route" || w.Path() != "/steps/1" {
		t.Fatalf("wait step %+v", w)
	}
	sw, _ := d.Step("route")
	if len(sw.Switch.Cases) != 1 || sw.Switch.Cases[0].Next != "done" || sw.Switch.Default != "done" {
		t.Fatalf("switch %+v", sw.Switch)
	}
	if e, _ := d.Step("done"); e.End.Name != "completed" {
		t.Fatalf("end %+v", e)
	}
}

func TestParseAcceptsAnIntegralVersionSpelledAsANumber(t *testing.T) {
	for _, v := range []string{"1.0", "1e0", "10e-1", "0.1e1"} {
		b := []byte(strings.Replace(string(body("", `[`+endStep+`]`)), `"metamodel_version":1`, `"metamodel_version":`+v, 1))
		d, vs := Parse(b)
		if len(vs) > 0 || d.MetamodelVersion != 1 {
			t.Errorf("metamodel_version %s: %+v %v", v, d, vs)
		}
	}
}

func TestParseDefaultsToConcurrent(t *testing.T) {
	d, vs := Parse(body("", `[`+endStep+`]`))
	if len(vs) > 0 || d.Concurrency.Policy != PolicyConcurrent {
		t.Fatalf("got %+v %v", d.Concurrency, vs)
	}
}

func TestParseViolations(t *testing.T) {
	big := `{"metamodel_version":1,"description":"` + strings.Repeat("x", MaxBodyBytes) + `"}`
	deep := `{"metamodel_version":1,"x":` + strings.Repeat("[", 40) + strings.Repeat("]", 40) + `}`
	var many []string
	for i := range MaxSteps + 1 {
		many = append(many, `{"id":"s`+strconv.Itoa(i)+`","type":"end","name":"n"}`)
	}
	tests := []struct {
		name string
		body string
		want []string // paths
	}{
		{"too large", big, []string{""}},
		{"not json", `{`, []string{""}},
		{"not an object", `[]`, []string{""}},
		{"too deep", deep, []string{""}},
		{"missing version", `{"trigger":{"event":"a"},"steps":[]}`, []string{"/metamodel_version"}},
		{"unsupported version", `{"metamodel_version":7,"trigger":{"event":"a"},"steps":[]}`, []string{"/metamodel_version"}},
		{"version not integer", `{"metamodel_version":"1"}`, []string{"/metamodel_version"}},
		{"version with a fraction", `{"metamodel_version":1.5,"trigger":{"event":"a"},"steps":[]}`, []string{"/metamodel_version"}},
		{"version with a huge exponent", `{"metamodel_version":1e-999999999,"trigger":{"event":"a"},"steps":[]}`, []string{"/metamodel_version"}},
		{"too many steps", string(body("", "["+strings.Join(many, ",")+"]")), []string{"/steps"}},
		{"no steps", string(body("", `[]`)), []string{"/steps"}},
		{"unknown property", string(body("", `[{"id":"s","type":"end","name":"n","next":"s"}]`)), []string{"/steps/0"}},
		{"missing next", string(body("", `[{"id":"s","type":"action","action":"a"},`+endStep+`]`)), []string{"/steps/0/next"}},
		{"bad step type", string(body("", `[{"id":"s","type":"loop"}]`)), []string{"/steps/0/type"}},
		{"bad step id", string(body("", `[{"id":"a:b","type":"end","name":"n"}]`)), []string{"/steps/0/id"}},
		{"bad duration", string(body("", `[{"id":"w","type":"wait_for_event","event":"e","timeout":"1w","on_timeout":"done","next":"done"},`+endStep+`]`)), []string{"/steps/0/timeout"}},
		{"bad policy", string(body(`{"policy":"queue"}`, `[`+endStep+`]`)), []string{"/concurrency/policy"}},
		{"empty expression", string(body("", `[{"id":"s","type":"action","action":"a","when":"","next":"done"},`+endStep+`]`)), []string{"/steps/0/when"}},
		{"duplicate id", string(body("", `[{"id":"a","type":"action","action":"x","next":"done"},`+endStep+`,{"id":"done","type":"end","name":"again"}]`)), []string{"/steps/2/id"}},
		{"unknown next", string(body("", `[{"id":"s","type":"action","action":"a","next":"nowhere"},`+endStep+`]`)),
			[]string{"/steps/0", "/steps/0/next", "/steps/1"}},
		{"unknown switch target", string(body("", `[{"id":"s","type":"switch","cases":[{"when":"true","next":"done"},{"when":"false","next":"x"}],"default":"done"},`+endStep+`]`)),
			[]string{"/steps/0/cases/1/next"}},
		{"zero timeout", string(body("", `[{"id":"w","type":"wait_for_event","event":"e","timeout":"0s","on_timeout":"done","next":"done"},`+endStep+`]`)), []string{"/steps/0/timeout"}},
		{"timeout over a year", string(body("", `[{"id":"w","type":"wait_for_event","event":"e","timeout":"366d","on_timeout":"done","next":"done"},`+endStep+`]`)), []string{"/steps/0/timeout"}},
		{"timeout overflows", string(body("", `[{"id":"w","type":"wait_for_event","event":"e","timeout":"999999999d999999999d999999999d999999999d","on_timeout":"done","next":"done"},`+endStep+`]`)), []string{"/steps/0/timeout"}},
		{"serial without key", string(body(`{"policy":"serial"}`, `[`+endStep+`]`)), []string{"/concurrency/serial_key"}},
		{"concurrent with key", string(body(`{"policy":"concurrent","serial_key":"scope.entity_id"}`, `[`+endStep+`]`)), []string{"/concurrency/serial_key"}},
		{"dedup without window", string(body(`{"policy":"dedup_window","serial_key":"scope.entity_id"}`, `[`+endStep+`]`)), []string{"/concurrency/dedup_window"}},
		{"window without dedup", string(body(`{"policy":"serial","serial_key":"scope.entity_id","dedup_window":"1h"}`, `[`+endStep+`]`)), []string{"/concurrency/dedup_window"}},
		{"window over a year", string(body(`{"policy":"dedup_window","serial_key":"scope.entity_id","dedup_window":"400d"}`, `[`+endStep+`]`)), []string{"/concurrency/dedup_window"}},
		{"expr with sibling key", string(body("", `[{"id":"s","type":"action","action":"a","params":{"to":{"$expr":"event.x","y":1}},"next":"done"},`+endStep+`]`)), []string{"/steps/0/params/to"}},
		{"expr not a string", string(body("", `[{"id":"s","type":"action","action":"a","params":{"list":[{"$expr":3}]},"next":"done"},`+endStep+`]`)), []string{"/steps/0/params/list/0"}},
		{"unreachable step", string(body("", `[`+endStep+`,{"id":"orphan","type":"end","name":"x"}]`)), []string{"/steps/1"}},
		{"loop without exit", string(body("", `[{"id":"a","type":"action","action":"x","next":"b"},{"id":"b","type":"action","action":"x","next":"a"}]`)),
			[]string{"/steps/0", "/steps/1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, vs := Parse([]byte(tt.body))
			var got []string
			for _, v := range vs {
				got = append(got, v.Path)
				if v.Msg == "" {
					t.Errorf("violation at %q has no message", v.Path)
				}
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("paths %q, want %q (violations %v)", got, tt.want, vs)
			}
		})
	}
}

func TestLoopWithExitIsValid(t *testing.T) {
	_, vs := Parse(body("", `[
		{"id":"remind","type":"action","action":"email.send","next":"wait"},
		{"id":"wait","type":"wait_for_event","event":"lesson.done","timeout":"1d","on_timeout":"remind","next":"done"},
		`+endStep+`]`))
	if len(vs) > 0 {
		t.Fatalf("violations: %v", vs)
	}
}

func TestParseDuration(t *testing.T) {
	for in, want := range map[string]time.Duration{
		"90s":   90 * time.Second,
		"1h30m": 90 * time.Minute,
		"7d":    7 * 24 * time.Hour,
		"1d1s":  24*time.Hour + time.Second,
	} {
		if got, err := ParseDuration(in); err != nil || got != want {
			t.Errorf("ParseDuration(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, in := range []string{"", "h", "1", "1w", "-1s", "99999999999999999999s"} {
		if _, err := ParseDuration(in); err == nil {
			t.Errorf("ParseDuration(%q) succeeded", in)
		}
	}
}

func TestFlows(t *testing.T) {
	// a (produces x) -> s: switch to b (produces y, has when) or c; both
	// join at j.
	d, vs := Parse(body("", `[
		{"id":"a","type":"action","action":"pa","next":"s"},
		{"id":"s","type":"switch","cases":[{"when":"true","next":"b"}],"default":"c"},
		{"id":"b","type":"action","action":"pb","when":"true","next":"j"},
		{"id":"c","type":"action","action":"pc","next":"j"},
		{"id":"j","type":"action","action":"pj","next":"done"},
		`+endStep+`]`))
	if len(vs) > 0 {
		t.Fatal(vs)
	}
	outputs := map[string][]string{"pa": {"x"}, "pb": {"y"}, "pc": {"y", "z"}, "pj": {"w"}}
	flows := d.Flows(func(s *Step) ([]string, []string) {
		if s.Type != StepAction {
			return nil, nil
		}
		out := outputs[s.Action.Action]
		if s.Action.When != "" {
			return nil, out
		}
		return out, nil
	})
	check := func(id string, certain, possible []string) {
		t.Helper()
		s, _ := d.Step(id)
		f := flows[s.Index]
		if !reflect.DeepEqual(f.Certain, certain) || !reflect.DeepEqual(f.Possible, possible) {
			t.Errorf("%s: certain %q possible %q, want %q %q", id, f.Certain, f.Possible, certain, possible)
		}
	}
	check("a", nil, nil)
	check("s", []string{"x"}, nil)
	check("j", []string{"x"}, []string{"y", "z"})
	check("done", []string{"w", "x"}, []string{"y", "z"})
}

func TestFlowsLoopBackToEntry(t *testing.T) {
	d, vs := Parse(body("", `[
		{"id":"a","type":"action","action":"pa","next":"w"},
		{"id":"w","type":"wait_for_event","event":"e","timeout":"1h","on_timeout":"a","next":"done"},
		`+endStep+`]`))
	if len(vs) > 0 {
		t.Fatal(vs)
	}
	flows := d.Flows(func(s *Step) ([]string, []string) {
		if s.Type == StepAction {
			return []string{"x"}, nil
		}
		return nil, nil
	})
	if f := flows[0]; len(f.Certain) != 0 || len(f.Possible) != 1 {
		t.Fatalf("entry flow %+v: x is only there after the loop", f)
	}
	if f := flows[2]; !reflect.DeepEqual(f.Certain, []string{"x"}) {
		t.Fatalf("end flow %+v", f)
	}
}

func TestParamExpressions(t *testing.T) {
	params := json.RawMessage(`{"to":{"$expr":"event.email"},"lines":[{"sku":{"$expr":"event.sku"}},"literal"],"a/b":{"$expr":"1"}}`)
	got, err := ParamExpressions(params)
	if err != nil {
		t.Fatal(err)
	}
	want := []ParamExpr{{Pointer: "/a~1b", Source: "1"}, {Pointer: "/lines/0/sku", Source: "event.sku"}, {Pointer: "/to", Source: "event.email"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	resolved, err := ResolveParams(params, func(p ParamExpr) (any, error) { return "(" + p.Source + ")", nil })
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(resolved)
	if string(b) != `{"a/b":"(1)","lines":[{"sku":"(event.sku)"},"literal"],"to":"(event.email)"}` {
		t.Fatalf("resolved %s", b)
	}
	if _, err := ResolveParams(json.RawMessage(`{"x":{"$expr":""}}`), nil); err == nil {
		t.Fatal("malformed placeholder resolved")
	}
}
