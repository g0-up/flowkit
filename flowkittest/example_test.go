package flowkittest_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/g0-up/flowkit"
	"github.com/g0-up/flowkit/flowkittest"
)

// onboardingRegistry is the registry an application builds for its Engine.
func onboardingRegistry() *flowkit.Registry {
	reg := flowkit.NewRegistry()
	reg.MustAddEvent(flowkit.EventDef{
		Type: "student.registered",
		PayloadSchema: json.RawMessage(`{"type":"object","required":["student_id","email","vip"],"properties":{
			"student_id":{"type":"string"},"email":{"type":"string"},"vip":{"type":"boolean"}}}`),
		EntityKind:    "student",
		EntityIDField: "student_id",
	})
	reg.MustAddEvent(flowkit.EventDef{
		Type:          "student.logged_in",
		PayloadSchema: json.RawMessage(`{"type":"object","required":["student_id"],"properties":{"student_id":{"type":"string"}}}`),
		EntityKind:    "student",
		EntityIDField: "student_id",
	})
	for _, name := range []string{"send_email", "notify_slack"} {
		reg.MustAddAction(flowkit.ActionDef{
			Name:         name,
			ParamsSchema: json.RawMessage(`{"type":"object"}`),
			Idempotent:   true,
			Execute: func(context.Context, flowkit.ActionInput) (flowkit.ActionResult, error) {
				return flowkit.ActionResult{}, nil
			},
		})
	}
	return reg
}

// A test drives a real Engine on Postgres through virtual time and checks
// what the run did. It needs FLOWKIT_TEST_DATABASE_URL, and is skipped
// without it.
func ExamplePGHarness() {
	var t *testing.T // the test's *testing.T

	h := flowkittest.NewPGHarness(t, onboardingRegistry())
	h.Publish("onboarding", json.RawMessage(`{
		"metamodel_version": 1,
		"trigger": {"event": "student.registered"},
		"steps": [
			{"id": "welcome", "type": "action", "action": "send_email",
			 "params": {"to": {"$expr": "event.email"}}, "next": "first_login"},
			{"id": "first_login", "type": "wait_for_event", "event": "student.logged_in",
			 "timeout": "72h", "on_timeout": "vip-slack", "next": "done"},
			{"id": "vip-slack", "type": "action", "action": "notify_slack", "when": "event.vip", "next": "done"},
			{"id": "done", "type": "end", "name": "onboarded"}
		]
	}`))
	h.Emit(flowkit.Event{Type: "student.registered", Payload: map[string]any{
		"student_id": "s-1", "email": "s-1@example.com", "vip": false,
	}})
	// Run what is due now: the welcome email, then the wait starts.
	h.Advance(0)

	runs, err := h.Engine().Runs().List(context.Background(), flowkit.RunFilter{DefName: "onboarding"})
	if err != nil || len(runs) != 1 {
		t.Fatalf("List = %v, %v", runs, err)
	}
	runID := runs[0].RunID
	if v := h.Run(runID); v.WaitStep != "first_login" {
		t.Fatalf("run %+v is not waiting for the first login", v)
	}

	// Three days pass without a login: the wait times out.
	h.Advance(72 * time.Hour)
	flowkittest.AssertTrace(t, h.Engine().Runs(), runID,
		flowkittest.StepExecuted("welcome"),
		flowkittest.StepSkipped("vip-slack", "when"),
		flowkittest.RunTerminal("onboarded"),
	)
}

// Matchers check the trace of a run, the same on PGHarness and Sim. On a
// mismatch AssertTrace lists what each failing matcher wanted and found,
// followed by the trace.
func ExampleAssertTrace() {
	var (
		t     *testing.T    // the test's *testing.T
		runs  *flowkit.Runs // PGHarness.Engine().Runs() or Sim.Runs()
		runID string        // the run to check
	)
	flowkittest.AssertTrace(t, runs, runID,
		flowkittest.StepExecutedTimes("charge", 1),
		flowkittest.RunFailed(""),
		flowkittest.DeadLetter("effect_in_doubt"),
	)
}
