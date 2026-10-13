package expr

import (
	"errors"
	"fmt"
	"time"

	"cel.dev/cel-go/checker"
	"cel.dev/cel-go/common/types"
)

// Cost limits. The estimate made when a definition is saved and the cost
// counted while evaluating share one limit.
const (
	// MaxCost bounds the estimated and the actual cost of one evaluation,
	// in CEL cost units.
	MaxCost = 1_000_000
	// EvalTimeout bounds the wall time of one evaluation.
	EvalTimeout = 100 * time.Millisecond
	// interruptEvery is how many comprehension iterations run between
	// checks of the evaluation deadline.
	interruptEvery = 100

	// defaultMaxItems sizes a list or map whose schema sets no bound.
	defaultMaxItems = 1000
	// defaultMaxLength sizes a string whose schema sets no maxLength.
	defaultMaxLength = 4096
)

// ErrLimitExceeded matches a LimitError with errors.Is.
var ErrLimitExceeded = errors.New("expression exceeded its evaluation limit")

// LimitError reports an evaluation stopped by MaxCost or EvalTimeout. It is
// a definition problem, not a transient failure: the same expression on the
// same input stops the same way.
type LimitError struct {
	// Timeout is true when EvalTimeout stopped the evaluation, false for
	// MaxCost.
	Timeout bool
}

func (e *LimitError) Error() string {
	if e.Timeout {
		return fmt.Sprintf("expression evaluation took longer than %s", EvalTimeout)
	}
	return fmt.Sprintf("expression evaluation exceeded the cost limit %d", MaxCost)
}

// Is matches ErrLimitExceeded.
func (e *LimitError) Is(target error) bool { return target == ErrLimitExceeded }

// estimator sizes variables for checker.Cost from their schema bounds:
// maxItems, maxProperties and maxLength, with defaults for unbounded ones.
type estimator struct {
	env *Env
}

// EstimateSize implements checker.CostEstimator.
func (s estimator) EstimateSize(n checker.AstNode) *checker.SizeEstimate {
	if nd := s.env.nodeAt(n.Path()); nd != nil && nd.max > 0 {
		return &checker.SizeEstimate{Min: 0, Max: nd.max}
	}
	if t := n.Type(); t != nil {
		switch t.Kind() {
		case types.StringKind, types.BytesKind:
			return &checker.SizeEstimate{Min: 0, Max: defaultMaxLength}
		case types.ListKind, types.MapKind:
			return &checker.SizeEstimate{Min: 0, Max: defaultMaxItems}
		}
	}
	return nil
}

// EstimateCallCost implements checker.CostEstimator; the standard costs
// apply to every function.
func (estimator) EstimateCallCost(string, string, *checker.AstNode, []checker.AstNode) *checker.CallEstimate {
	return nil
}

// nodeAt follows a checker field path: a variable name, then field names,
// "@items", "@values", "@keys" or "@indices".
func (e *Env) nodeAt(path []string) *node {
	if len(path) == 0 {
		return nil
	}
	n, ok := e.roots[path[0]]
	if !ok {
		return nil
	}
	for _, seg := range path[1:] {
		switch {
		case seg == "@keys" || seg == "@indices":
			return nil
		case seg == "@items" && n.kind == kindList, seg == "@values" && n.kind == kindMap:
			n = n.elem
		case n.kind == kindMap:
			n = n.elem
		case n.kind == kindObject:
			f, ok := n.fields[seg]
			if !ok {
				return nil
			}
			n = f.node
		default:
			return nil
		}
	}
	return n
}
