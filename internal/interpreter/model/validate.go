package model

import (
	"fmt"
	"sort"

	"github.com/g0-up/flowkit/internal/schema"
)

// check runs the structural checks on a built definition. reported holds
// the paths build already reported, which are not checked again.
func check(d *Definition, reported map[string]bool) []schema.Violation {
	var vs []schema.Violation
	add := func(path, format string, args ...any) {
		vs = append(vs, schema.Violation{Path: path, Msg: fmt.Sprintf(format, args...)})
	}

	c := d.Concurrency
	switch {
	case c.Policy == PolicyConcurrent && c.SerialKey != "":
		add("/concurrency/serial_key", "not allowed with policy %q", c.Policy)
	case c.Policy != PolicyConcurrent && c.SerialKey == "":
		add("/concurrency/serial_key", "required by policy %q", c.Policy)
	}
	switch {
	case reported["/concurrency/dedup_window"]:
	case c.Policy == PolicyDedupWindow && c.DedupWindow == 0:
		add("/concurrency/dedup_window", "required by policy %q and must be greater than zero", c.Policy)
	case c.Policy != PolicyDedupWindow && c.DedupWindow != 0:
		add("/concurrency/dedup_window", "only allowed with policy %q", PolicyDedupWindow)
	case c.DedupWindow > MaxDuration:
		add("/concurrency/dedup_window", "must be at most 365 days")
	}

	for _, s := range d.Steps {
		if first := d.index[s.ID]; first != s.Index {
			add(s.Path()+"/id", "duplicate step id %q, first used at %s", s.ID, d.Steps[first].Path())
		}
		for _, e := range s.Edges() {
			if _, ok := d.index[e.To]; !ok {
				add(e.Path, "unknown step %q", e.To)
			}
		}
		switch s.Type {
		case StepWait:
			switch t := s.Wait.Timeout; {
			case reported[s.Path()+"/timeout"]:
			case t == 0:
				add(s.Path()+"/timeout", "must be greater than zero")
			case t > MaxDuration:
				add(s.Path()+"/timeout", "must be at most 365 days")
			}
		case StepAction:
			if s.Action.Params != nil {
				vs = append(vs, checkParams(s.Path()+"/params", s.Action.Params)...)
			}
		}
	}
	return append(vs, checkGraph(d)...)
}

func sortViolations(vs []schema.Violation) {
	sort.SliceStable(vs, func(i, j int) bool { return vs[i].Path < vs[j].Path })
}
