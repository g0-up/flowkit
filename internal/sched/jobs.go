package sched

import (
	"time"

	"github.com/riverqueue/river"

	"github.com/g0-up/flowkit/internal/ids"
)

// Queue and job kind names. They are stored with every job: renaming one
// orphans the jobs already waiting under the old name.
const (
	// Queue is the River queue all flowkit jobs go to.
	Queue = "flowkit"
	// IdleQueue is the queue a client without RoleWorker subscribes to,
	// because River does not start a client without queues. flowkit never
	// inserts into it.
	IdleQueue = "flowkit_janitor_idle"

	KindRouteEvent  = "flowkit.route_event"
	KindPumpRun     = "flowkit.pump_run"
	KindWaitTimeout = "flowkit.wait_timeout"
	KindJanitor     = "flowkit.janitor"
)

// RouteEventArgs routes one stored event to the runs it starts or resumes.
type RouteEventArgs struct {
	EventID int64 `json:"event_id"`
}

// Kind returns KindRouteEvent.
func (RouteEventArgs) Kind() string { return KindRouteEvent }

// InsertOpts puts the job on the flowkit queue.
func (RouteEventArgs) InsertOpts() river.InsertOpts { return river.InsertOpts{Queue: Queue} }

// PumpRunArgs advances one run. Duplicate pump jobs for a run are allowed;
// the run's revision decides which one makes progress, so no River unique
// option is used (River's unique states would have to include running and
// would swallow a pump enqueued while the previous one is still running).
type PumpRunArgs struct {
	RunID ids.UUID `json:"run_id"`
}

// Kind returns KindPumpRun.
func (PumpRunArgs) Kind() string { return KindPumpRun }

// InsertOpts puts the job on the flowkit queue.
func (PumpRunArgs) InsertOpts() river.InsertOpts { return river.InsertOpts{Queue: Queue} }

// WaitTimeoutArgs fires the deadline of one wait of a run step. WaitSeq
// tells a stale timeout from the current wait of the same step.
type WaitTimeoutArgs struct {
	RunID   ids.UUID  `json:"run_id"`
	StepID  string    `json:"step_id"`
	WaitSeq int       `json:"wait_seq"`
	DueAt   time.Time `json:"due_at"`
}

// Kind returns KindWaitTimeout.
func (WaitTimeoutArgs) Kind() string { return KindWaitTimeout }

// InsertOpts puts the job on the flowkit queue.
func (WaitTimeoutArgs) InsertOpts() river.InsertOpts { return river.InsertOpts{Queue: Queue} }

// JanitorArgs runs one janitor pass. Only the leader's periodic job inserts
// it.
type JanitorArgs struct{}

// Kind returns KindJanitor.
func (JanitorArgs) Kind() string { return KindJanitor }

// InsertOpts puts the job on the flowkit queue.
func (JanitorArgs) InsertOpts() river.InsertOpts { return river.InsertOpts{Queue: Queue} }
