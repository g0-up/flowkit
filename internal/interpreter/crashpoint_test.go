package interpreter

import (
	"slices"
	"testing"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/testhooks"
)

func TestCrashPointsDoNotChangeARun(t *testing.T) {
	e := newEnv(t)
	e.planner.action("act", testAction{execute: (&recorder{output: map[string]any{}}).execute})
	e.publish(defSpec{name: "flow", steps: steps(
		`{"id":"act","type":"action","action":"act","next":"w"}`,
		waitStep("w", "", "got", "late"),
		endStep("got", "got"), endStep("late", "late"),
	)})
	// drive starts a run of key and pumps it until it waits, returning the
	// run, its state and its step records.
	drive := func(key string) (ids.UUID, string, []string) {
		id := e.start("flow", key)
		for range 3 {
			e.mustPump(id)
			if r := e.run(id); r.State != stateRunning {
				break
			}
		}
		return id, e.run(id).State, e.kinds(id)
	}

	_, plainState, plainSteps := drive("plain")

	var points []string
	testhooks.Point = func(name string) { points = append(points, name) }
	t.Cleanup(func() { testhooks.Point = nil })
	_, hookedState, hookedSteps := drive("hooked")

	want := []string{
		testhooks.PointRouted, testhooks.PointActionEntered,
		testhooks.PointClaimed, testhooks.PointExecuted, testhooks.PointRecorded,
		testhooks.PointWaitEntered,
	}
	if !slices.Equal(points, want) {
		t.Fatalf("crash points reached %v, want %v", points, want)
	}
	if plainState != stateWaiting || hookedState != plainState || !slices.Equal(hookedSteps, plainSteps) {
		t.Fatalf("without hook: %s %v; with hook: %s %v", plainState, plainSteps, hookedState, hookedSteps)
	}
}
