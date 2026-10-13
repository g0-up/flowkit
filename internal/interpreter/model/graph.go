package model

import (
	"maps"
	"slices"

	"github.com/g0-up/flowkit/internal/schema"
)

// successors returns the existing steps s can move to.
func (d *Definition) successors(s *Step) []*Step {
	var out []*Step
	for _, e := range s.Edges() {
		if t, ok := d.Step(e.To); ok {
			out = append(out, t)
		}
	}
	return out
}

// Reachable reports, by step index, which steps a run can enter from the
// first step.
func (d *Definition) Reachable() []bool {
	seen := make([]bool, len(d.Steps))
	stack := []*Step{d.Entry()}
	seen[0] = true
	for len(stack) > 0 {
		s := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, t := range d.successors(s) {
			if !seen[t.Index] {
				seen[t.Index] = true
				stack = append(stack, t)
			}
		}
	}
	return seen
}

// checkGraph reports steps a run can never enter and steps from which no
// path leads to an end step. Loops are fine as long as they have an exit.
func checkGraph(d *Definition) []schema.Violation {
	reachable := d.Reachable()
	// Walk backwards from every end step.
	preds := make([][]*Step, len(d.Steps))
	for _, s := range d.Steps {
		for _, t := range d.successors(s) {
			preds[t.Index] = append(preds[t.Index], s)
		}
	}
	ends := make([]bool, len(d.Steps))
	var stack []*Step
	for _, s := range d.Steps {
		if s.Type == StepEnd {
			ends[s.Index] = true
			stack = append(stack, s)
		}
	}
	for len(stack) > 0 {
		s := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, p := range preds[s.Index] {
			if !ends[p.Index] {
				ends[p.Index] = true
				stack = append(stack, p)
			}
		}
	}
	var vs []schema.Violation
	for _, s := range d.Steps {
		switch {
		case d.index[s.ID] != s.Index:
			// A duplicate id is reported once; no transition can enter it.
		case !reachable[s.Index]:
			vs = append(vs, schema.Violation{Path: s.Path(), Msg: "step is unreachable from the first step"})
		case !ends[s.Index]:
			vs = append(vs, schema.Violation{Path: s.Path(), Msg: "no path from this step reaches an end step"})
		}
	}
	return vs
}

// Flow is what a step can rely on when a run enters it.
type Flow struct {
	// Certain lists the facts produced on every path from the first step.
	Certain []string
	// Possible lists the facts produced on some but not all paths.
	Possible []string
}

// Flows computes, for every reachable step, the facts produced by the steps
// before it. gen returns the facts a step produces when it completes:
// certain ones always, possible ones only on some executions (an action
// with a when condition may be skipped). The result is indexed like Steps;
// an unreachable step gets nil.
func (d *Definition) Flows(gen func(*Step) (certain, possible []string)) []*Flow {
	reachable := d.Reachable()
	n := len(d.Steps)
	certainGen := make([]map[string]bool, n)
	possibleGen := make([]map[string]bool, n)
	universe := map[string]bool{}
	for _, s := range d.Steps {
		c, p := gen(s)
		certainGen[s.Index], possibleGen[s.Index] = set(c), set(p)
		for _, f := range c {
			universe[f] = true
		}
	}
	// must[i] / may[i]: facts on entry to step i. must starts at the
	// universe and shrinks; may starts empty and grows.
	must := make([]map[string]bool, n)
	may := make([]map[string]bool, n)
	for i := range n {
		may[i] = map[string]bool{}
		if i == 0 {
			must[i] = map[string]bool{}
		} else {
			must[i] = maps.Clone(universe)
		}
	}
	for changed := true; changed; {
		changed = false
		for _, s := range d.Steps {
			if !reachable[s.Index] {
				continue
			}
			outMust := union(must[s.Index], certainGen[s.Index])
			outMay := union(may[s.Index], certainGen[s.Index], possibleGen[s.Index])
			for _, t := range d.successors(s) {
				if t.Index != 0 {
					for f := range must[t.Index] {
						if !outMust[f] {
							delete(must[t.Index], f)
							changed = true
						}
					}
				}
				for f := range outMay {
					if !may[t.Index][f] {
						may[t.Index][f] = true
						changed = true
					}
				}
			}
		}
	}
	out := make([]*Flow, n)
	for _, s := range d.Steps {
		if !reachable[s.Index] {
			continue
		}
		var certain, possible []string
		for f := range may[s.Index] {
			if !must[s.Index][f] {
				possible = append(possible, f)
			}
		}
		for f := range must[s.Index] {
			certain = append(certain, f)
		}
		slices.Sort(certain)
		slices.Sort(possible)
		out[s.Index] = &Flow{Certain: certain, Possible: possible}
	}
	return out
}

func set(xs []string) map[string]bool {
	m := make(map[string]bool, len(xs))
	for _, x := range xs {
		m[x] = true
	}
	return m
}

func union(sets ...map[string]bool) map[string]bool {
	out := map[string]bool{}
	for _, s := range sets {
		for k := range s {
			out[k] = true
		}
	}
	return out
}
