package flowkittest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/g0-up/flowkit"
)

// onboardingBody welcomes a student, waits three days for a first login
// and, failing that, pings Slack about a VIP.
const onboardingBody = `{
	"metamodel_version": 1,
	"trigger": {"event": "student.registered"},
	"steps": [
		{"id": "welcome", "type": "action", "action": "send_email",
		 "params": {"to": {"$expr": "event.email"}}, "next": "first_login"},
		{"id": "first_login", "type": "wait_for_event", "event": "student.logged_in",
		 "timeout": "72h", "on_timeout": "vip-slack", "next": "done"},
		{"id": "vip-slack", "type": "action", "action": "notify_slack", "when": "event.vip",
		 "params": {"student": {"$expr": "event.student_id"}}, "next": "done"},
		{"id": "done", "type": "end", "name": "onboarded"}
	]
}`

// goldenEnv is what the onboarding sample needs from an environment, so
// it runs unchanged on Postgres and on the simulator.
type goldenEnv interface {
	Publish(name string, body json.RawMessage) flowkit.Version
	Emit(evt flowkit.Event)
	Advance(d time.Duration)
	Runs() *flowkit.Runs
}

// pgGolden runs the sample on a PGHarness.
type pgGolden struct{ *PGHarness }

func (g pgGolden) Runs() *flowkit.Runs { return g.Engine().Runs() }

// goldenOK is the outcome of every action of the sample.
func goldenOK(context.Context, flowkit.ActionInput) (flowkit.ActionResult, error) {
	return flowkit.ActionResult{}, nil
}

func goldenRegistry() *flowkit.Registry {
	return pgStudentRegistry(
		flowkit.ActionDef{Name: "send_email", ParamsSchema: json.RawMessage(`{"type":"object"}`), Idempotent: true, Execute: goldenOK},
		flowkit.ActionDef{Name: "notify_slack", ParamsSchema: json.RawMessage(`{"type":"object"}`), Idempotent: true, Execute: goldenOK},
	)
}

func newPGGolden(t *testing.T) goldenEnv {
	return pgGolden{NewPGHarness(t, goldenRegistry())}
}

// The sample runs on each environment: newSimGolden builds the simulator's.
func TestOnboardingPingsSlackOnlyAboutAVIPWhoNeverLoggedIn(t *testing.T) {
	t.Parallel()
	for name, newEnv := range map[string]func(*testing.T) goldenEnv{
		"postgres": newPGGolden,
		"sim":      newSimGolden,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			env := newEnv(t)
			env.Publish("onboarding", json.RawMessage(onboardingBody))
			env.Emit(pgRegistered("s-regular", false))
			env.Emit(pgRegistered("s-vip", true))
			// Settle first, so the waits start their 72h now.
			env.Advance(0)
			env.Advance(72 * time.Hour)

			AssertTrace(t, env.Runs(), goldenRun(t, env, "s-regular"),
				StepExecutedTimes("welcome", 1),
				StepSkipped("vip-slack", "when"),
				StepExecutedTimes("vip-slack", 0),
				RunTerminal("onboarded"),
			)
			AssertTrace(t, env.Runs(), goldenRun(t, env, "s-vip"),
				StepExecutedTimes("vip-slack", 1),
				RunTerminal("onboarded"),
			)
		})
	}
}

// goldenRun returns the id of the onboarding run of student studentID.
func goldenRun(t *testing.T, env goldenEnv, studentID string) string {
	t.Helper()
	runs, err := env.Runs().List(context.Background(), flowkit.RunFilter{
		DefName: "onboarding", EntityKind: "student", EntityID: studentID,
	})
	if err != nil {
		t.Fatalf("List onboarding runs of %s: %v", studentID, err)
	}
	if len(runs) != 1 {
		t.Fatalf("%s has %d onboarding runs, want 1: %+v", studentID, len(runs), runs)
	}
	return runs[0].RunID
}
