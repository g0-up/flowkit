package flowkit

import "fmt"

// Role selects the work an Engine instance takes on. Config.Roles holds the
// roles of one instance; the default is both.
//
// At least one running instance across the deployment must have RoleJanitor:
// only the elected janitor leader promotes scheduled jobs, so without one no
// wait timeout or delayed pump ever becomes due.
type Role int

const (
	// RoleWorker processes the flowkit queue: event routing, run pumps and
	// wait timeouts.
	RoleWorker Role = iota
	// RoleJanitor takes part in leader election. The leader runs the periodic
	// janitor and River's scheduler and rescuer. An instance with only
	// RoleJanitor never processes flowkit jobs.
	RoleJanitor
)

// roleName names r in logs and errors. Role has no String method so the
// public method set stays the frozen one.
func roleName(r Role) string {
	switch r {
	case RoleWorker:
		return "worker"
	case RoleJanitor:
		return "janitor"
	default:
		return fmt.Sprintf("Role(%d)", int(r))
	}
}
