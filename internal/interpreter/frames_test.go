package interpreter

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestFramesRoundTrip(t *testing.T) {
	f, err := decodeFrames(initialFrames)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := f.top(); ok || f.Visits == nil {
		t.Fatalf("initial frames = %+v, want an empty stack and visits", f)
	}

	saved := f.clone()
	fr := f.enter("charge", t0)
	if fr != (frame{Step: "charge", Entered: true, Visit: 1, Now: t0}) {
		t.Fatalf("entered frame = %+v", fr)
	}
	if len(saved.Stack) != 0 || len(saved.Visits) != 0 {
		t.Fatalf("clone shares state with the frames: %+v", saved)
	}
	f.moveTo("wait")
	f.enter("wait", t0)
	f.moveTo("charge")
	if fr := f.enter("charge", t0.Add(1)); fr.Visit != 2 {
		t.Fatalf("second visit = %d", fr.Visit)
	}

	raw, err := f.encode()
	if err != nil {
		t.Fatal(err)
	}
	back, err := decodeFrames(raw)
	if err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	if !reflect.DeepEqual(back, f) {
		t.Fatalf("round trip = %+v, want %+v", back, f)
	}

	// A frame that is not entered stores only its step.
	f.moveTo("done")
	raw, err = f.encode()
	if err != nil {
		t.Fatal(err)
	}
	var stored map[string]any
	if err := json.Unmarshal(raw, &stored); err != nil {
		t.Fatal(err)
	}
	if got := stored["stack"]; !reflect.DeepEqual(got, []any{map[string]any{"step": "done"}}) {
		t.Fatalf("stored stack = %v", got)
	}
}

func TestDecodeFramesIsStrict(t *testing.T) {
	for _, c := range []struct{ raw, want string }{
		{`{"stack":[],"visits":{},"extra":1}`, "unknown field"},
		{`{"stack":[{"step":"a","depth":2}],"visits":{}}`, "unknown field"},
		{`{"stack":[{"step":"a"},{"step":"b"}],"visits":{}}`, "2 frames"},
		{`{"stack":[{"entered":true}],"visits":{}}`, "without a step"},
		{`{"stack":[],"visits":{}} {}`, "after the frames"},
		{`[]`, "decode frames"},
	} {
		if _, err := decodeFrames(json.RawMessage(c.raw)); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("decodeFrames(%s) = %v, want an error with %q", c.raw, err, c.want)
		}
	}
	f, err := decodeFrames(json.RawMessage(`{"stack":[{"step":"a"}]}`))
	if err != nil || f.Visits == nil {
		t.Fatalf("frames without visits = %+v, %v; want an empty map", f, err)
	}
}
