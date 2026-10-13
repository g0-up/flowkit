package executor

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/g0-up/flowkit/internal/testhooks"
)

// recordPoints sets testhooks.Point to record the crash points reached,
// until the test ends.
func recordPoints(t *testing.T) *[]string {
	t.Helper()
	var got []string
	testhooks.Point = func(name string) { got = append(got, name) }
	t.Cleanup(func() { testhooks.Point = nil })
	return &got
}

func TestCrashPointsDoNotChangeARun(t *testing.T) {
	e := newEnv(t)
	ex := e.executor()
	ok, _ := countingAction("mail.send", false, func(context.Context, int64) (map[string]any, error) {
		return map[string]any{}, nil
	})
	broken, _ := countingAction("mail.retry", true, func(context.Context, int64) (map[string]any, error) {
		return nil, errors.New("smtp down")
	})
	run := func(key string, a Action) (ExecKind, bool) {
		out := ex.Run(context.Background(), e.request(key, a))
		row := e.effect(key)
		return out.Kind, row.DoneAt != nil
	}

	plainOK, plainOKDone := run("plain-ok", ok)
	plainBroken, plainBrokenDone := run("plain-broken", broken)

	points := recordPoints(t)
	hookedOK, hookedOKDone := run("hooked-ok", ok)
	if want := []string{testhooks.PointClaimed, testhooks.PointExecuted, testhooks.PointRecorded}; !slices.Equal(*points, want) {
		t.Fatalf("a successful action reached %v, want %v", *points, want)
	}
	*points = nil
	hookedBroken, hookedBrokenDone := run("hooked-broken", broken)
	if want := []string{testhooks.PointClaimed, testhooks.PointExecuted}; !slices.Equal(*points, want) {
		t.Fatalf("a failing action reached %v, want %v", *points, want)
	}

	if plainOK != Done || hookedOK != plainOK || hookedOKDone != plainOKDone {
		t.Fatalf("success: without hook %s (done %v), with hook %s (done %v)", plainOK, plainOKDone, hookedOK, hookedOKDone)
	}
	if plainBroken != RetryLater || hookedBroken != plainBroken || hookedBrokenDone != plainBrokenDone {
		t.Fatalf("failure: without hook %s (done %v), with hook %s (done %v)", plainBroken, plainBrokenDone, hookedBroken, hookedBrokenDone)
	}
}
