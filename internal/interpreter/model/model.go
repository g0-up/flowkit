// Package model is the in-memory form of a workflow definition: the parsed
// body, independent of its JSON shape, plus the structural checks that need
// no registry.
package model

import (
	"encoding/json"
	"strconv"
	"time"
)

// StepType names a kind of step.
type StepType string

// Step types.
const (
	StepAction StepType = "action"
	StepWait   StepType = "wait_for_event"
	StepSwitch StepType = "switch"
	StepEnd    StepType = "end"
)

// Concurrency policies.
const (
	PolicyConcurrent     = "concurrent"
	PolicySerial         = "serial"
	PolicyCancelPrevious = "cancel_previous"
	PolicyDedupWindow    = "dedup_window"
)

// Definition is a parsed definition body. The first step is the entry.
type Definition struct {
	MetamodelVersion int
	Description      string
	Trigger          Trigger
	Concurrency      Concurrency
	Steps            []*Step
	// index maps a step id to its first position in Steps.
	index map[string]int
}

// Trigger starts a run.
type Trigger struct {
	Event string
}

// Concurrency limits how runs of the definition overlap. Policy is never
// empty; an absent concurrency block means PolicyConcurrent.
type Concurrency struct {
	Policy string
	// SerialKey is the CEL expression grouping runs; set for every policy
	// but PolicyConcurrent.
	SerialKey string
	// DedupWindow is set for PolicyDedupWindow only.
	DedupWindow time.Duration
}

// Step is one step. Exactly one of Action, Wait, Switch and End is set,
// matching Type.
type Step struct {
	ID     string
	Type   StepType
	Action *ActionStep
	Wait   *WaitStep
	Switch *SwitchStep
	End    *EndStep
	// Index is the position in Definition.Steps.
	Index int
}

// ActionStep calls a registered action.
type ActionStep struct {
	Action string
	// Params is the params object as written; values of the form
	// {"$expr": "<cel>"} are expressions resolved when the step runs. Nil
	// when the step has no params.
	Params json.RawMessage
	// When is an optional bool expression; when it is false the action is
	// skipped and the run moves to Next.
	When string
	Next string
}

// WaitStep waits for an event.
type WaitStep struct {
	Event string
	// Match is an optional bool expression over the waited event; empty
	// matches every event of the type for the run's scope.
	Match     string
	Timeout   time.Duration
	OnTimeout string
	Next      string
}

// SwitchStep branches on the first case whose When is true.
type SwitchStep struct {
	Cases   []Case
	Default string
}

// Case is one branch of a switch.
type Case struct {
	When string `json:"when"`
	Next string `json:"next"`
}

// EndStep finishes the run; Name becomes the run's terminal name.
type EndStep struct {
	Name string
}

// Step returns the step with the given id.
func (d *Definition) Step(id string) (*Step, bool) {
	i, ok := d.index[id]
	if !ok {
		return nil, false
	}
	return d.Steps[i], true
}

// Entry returns the first step.
func (d *Definition) Entry() *Step { return d.Steps[0] }

// Path is the JSON Pointer of the step in the body.
func (s *Step) Path() string { return "/steps/" + strconv.Itoa(s.Index) }

// Edge is a transition out of a step.
type Edge struct {
	// Path is the JSON Pointer of the field naming the target.
	Path string
	To   string
}

// Edges lists the transitions out of s in body order.
func (s *Step) Edges() []Edge {
	p := s.Path()
	switch s.Type {
	case StepAction:
		return []Edge{{Path: p + "/next", To: s.Action.Next}}
	case StepWait:
		return []Edge{{Path: p + "/next", To: s.Wait.Next}, {Path: p + "/on_timeout", To: s.Wait.OnTimeout}}
	case StepSwitch:
		out := make([]Edge, 0, len(s.Switch.Cases)+1)
		for i, c := range s.Switch.Cases {
			out = append(out, Edge{Path: p + "/cases/" + strconv.Itoa(i) + "/next", To: c.Next})
		}
		return append(out, Edge{Path: p + "/default", To: s.Switch.Default})
	}
	return nil
}
