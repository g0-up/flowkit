package interpreter

import (
	"strconv"

	"github.com/g0-up/flowkit/internal/ids"
)

// runNamespace is the UUIDv5 namespace of run ids. It must never change:
// a route that runs again derives the id of the run it already created
// from it, and so does the promotion of a queued trigger.
var runNamespace = ids.MustParse("8e1e9dd6-3472-415a-945b-87f12b358fab")

// RunID returns the id of the run a definition starts for a trigger event.
// It leaves out the definition version, so publishing between two routes of
// one event cannot start a second run. A run id is derivable, not secret.
func RunID(defName string, eventID int64) ids.UUID {
	return ids.NewV5(runNamespace, defName+":"+strconv.FormatInt(eventID, 10))
}
