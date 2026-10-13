package testhooks

// Crash points production code reports through Reach. Each sits right after
// a commit, or right around an action's effect, where a process dying
// leaves state only recovery can finish.
const (
	// PointClaimed is after an action's claim committed, before Execute.
	PointClaimed = "executor.claimed"
	// PointExecuted is after Execute returned, before its result committed.
	PointExecuted = "executor.executed"
	// PointRecorded is after the effect's result committed, before the run
	// moved past the action.
	PointRecorded = "executor.recorded"
	// PointActionEntered is after the run committed entering an action,
	// before the executor ran it.
	PointActionEntered = "interpreter.action_entered"
	// PointWaitEntered is after the run committed entering a wait, before
	// its look-back for already stored events.
	PointWaitEntered = "interpreter.wait_entered"
	// PointRouted is after an event's routing committed a new run, before
	// any pump of that run.
	PointRouted = "interpreter.routed"
)

// Point, when set, is called with the name of every crash point production
// code passes. A crash test sets it to stop the process at one of them.
//
// It is nil in production, where Reach does nothing.
var Point func(name string)

// Reach reports that the caller is at the named crash point.
func Reach(name string) {
	if Point != nil {
		Point(name)
	}
}
