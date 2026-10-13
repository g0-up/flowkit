package interpreter

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// frames is the position of a run, stored in runs.frames. The step graph of
// metamodel v1 is flat (steps jump to steps by id), so the stack holds at
// most one frame: moving to a step replaces it. An empty stack means the
// run has not left its entry step.
type frames struct {
	Stack []frame `json:"stack"`
	// Visits counts how often the run entered each action and wait step.
	// The visit number of an action is part of its idempotency key, so a
	// loop that runs an action again gets a new effect.
	Visits map[string]int `json:"visits"`
}

// frame is the step a run is at.
type frame struct {
	Step string `json:"step"`
	// Entered marks an action whose entry is committed: the branch that led
	// to it, its visit number, the facts and the time of its params are
	// fixed, and a pump after a crash runs the same effect again instead of
	// walking the definition anew.
	Entered bool      `json:"entered,omitempty"`
	Visit   int       `json:"visit,omitempty"`
	Now     time.Time `json:"now,omitzero"`
}

// initialFrames is the position of a new run.
var initialFrames = json.RawMessage(`{"stack":[],"visits":{}}`)

// decodeFrames reads runs.frames strictly: an unknown field or a stack of
// more than one frame means the run was written by an incompatible
// version.
func decodeFrames(raw json.RawMessage) (frames, error) {
	var f frames
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return frames{}, fmt.Errorf("decode frames: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return frames{}, errors.New("decode frames: data after the frames object")
	}
	if len(f.Stack) > 1 {
		return frames{}, fmt.Errorf("decode frames: %d frames on the stack, at most 1 expected", len(f.Stack))
	}
	if len(f.Stack) == 1 && f.Stack[0].Step == "" {
		return frames{}, errors.New("decode frames: frame without a step")
	}
	if f.Visits == nil {
		f.Visits = map[string]int{}
	}
	return f, nil
}

func (f frames) encode() (json.RawMessage, error) {
	if f.Stack == nil {
		f.Stack = []frame{}
	}
	if f.Visits == nil {
		f.Visits = map[string]int{}
	}
	b, err := json.Marshal(f)
	if err != nil {
		return nil, fmt.Errorf("encode frames: %w", err)
	}
	return b, nil
}

// top returns the current frame, or false at the entry step.
func (f *frames) top() (frame, bool) {
	if len(f.Stack) == 0 {
		return frame{}, false
	}
	return f.Stack[len(f.Stack)-1], true
}

// set makes fr the current frame.
func (f *frames) set(fr frame) {
	if len(f.Stack) == 0 {
		f.Stack = append(f.Stack, fr)
		return
	}
	f.Stack[len(f.Stack)-1] = fr
}

// moveTo makes step the current step, not yet entered.
func (f *frames) moveTo(step string) { f.set(frame{Step: step}) }

// enter counts a new visit of step and makes it the current, entered
// frame.
func (f *frames) enter(step string, now time.Time) frame {
	if f.Visits == nil {
		f.Visits = map[string]int{}
	}
	f.Visits[step]++
	fr := frame{Step: step, Entered: true, Visit: f.Visits[step], Now: now}
	f.set(fr)
	return fr
}

// clone returns a deep copy, so a failed write leaves f as it was.
func (f frames) clone() frames {
	c := frames{Stack: append([]frame(nil), f.Stack...), Visits: make(map[string]int, len(f.Visits))}
	for k, v := range f.Visits {
		c.Visits[k] = v
	}
	return c
}
