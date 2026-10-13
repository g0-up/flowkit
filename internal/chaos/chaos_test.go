package chaos

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/g0-up/flowkit"
	"github.com/g0-up/flowkit/internal/sched"
	"github.com/g0-up/flowkit/internal/testhooks"
)

// chaosEnv enables the chaos tests when set to 1; they skip otherwise, and
// with -short.
const chaosEnv = "FLOWKIT_CHAOS"

// roundsEnv sets how many processes TestChaosKillAtCrashPoints kills at
// each crash point. The default is 50.
const roundsEnv = "FLOWKIT_CHAOS_ROUNDS"

func requireChaos(t *testing.T) {
	t.Helper()
	switch v := os.Getenv(chaosEnv); {
	case testing.Short():
		t.Skip("chaos tests skip with -short")
	case v == "":
		t.Skipf("chaos tests run only with %s=1", chaosEnv)
	case v != "1":
		t.Fatalf("%s=%q: want 1 or unset", chaosEnv, v)
	}
}

func chaosRounds(t *testing.T) int {
	t.Helper()
	s := os.Getenv(roundsEnv)
	if s == "" {
		return 50
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 {
		t.Fatalf("%s=%q: want a positive integer", roundsEnv, s)
	}
	return n
}

// crashPoint is a point worker processes are killed at, and how a run of
// the non-idempotent flow (once) and of the idempotent flow (retry) killed
// there ends after recovery: its state, end, open effect_in_doubt dead
// letters and sink writes. A worker process runs one job at a time and, as
// it takes no part in leader election, neither rescues jobs nor resolves
// claims, so it stops at its round's own run, and only a kill at claimed or
// executed leaves a claim held.
type crashPoint struct {
	name        string
	once, retry string
}

// completedOnce is a run that completed on its awaited event after its
// action wrote the sink exactly once.
const completedOnce = "completed got 0 1"

var crashPoints = []crashPoint{
	{testhooks.PointRouted, completedOnce, completedOnce},
	{testhooks.PointActionEntered, completedOnce, completedOnce},
	// The expired claim puts the non-idempotent effect in doubt although
	// the action never ran; the idempotent one counts as a failed attempt
	// and is retried.
	{testhooks.PointClaimed, "running - 1 0", completedOnce},
	// The action ran but its result was lost: in doubt after one write, or
	// retried and written again.
	{testhooks.PointExecuted, "running - 1 1", "completed got 0 2"},
	{testhooks.PointRecorded, completedOnce, completedOnce},
	{testhooks.PointWaitEntered, completedOnce, completedOnce},
}

// TestChaosKillAtCrashPoints kills a worker process at one crash point per
// round, each round a new run, alternating a non-idempotent and an
// idempotent action, and an awaited event stored before the run (found by
// look-back) or after the kill (delivered to the wait's subscription).
// Then a new instance with a janitor recovers the schema: every event is
// routed and every awaited event consumed, a non-idempotent effect happened
// at most once, and each run ends as its crash point implies: only a
// non-idempotent run killed holding its claim waits on an effect_in_doubt
// dead letter.
func TestChaosKillAtCrashPoints(t *testing.T) {
	requireChaos(t)
	t.Parallel()
	rounds := chaosRounds(t)
	for _, point := range crashPoints {
		t.Run(point.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			runs := map[flow]int{}
			began := time.Now()
			for k := range rounds {
				fl := flowOnce
				if k%2 == 1 {
					fl = flowRetry
				}
				runs[fl]++
				key := "r" + strconv.Itoa(k)
				stored := k/2%2 == 0
				if stored {
					f.emit(key, fl.trigger, evWake)
				} else {
					f.emit(key, fl.trigger)
				}
				f.killAt(point.name, 1)
				if !stored {
					f.emit(key, evWake)
				}
			}
			killed := time.Since(began)
			f.start(recoveryWorkers, flowkit.RoleWorker, flowkit.RoleJanitor)
			eventually(t, settleTimeout, func() error { return f.recovered(rounds) })
			t.Logf("killed %d workers at %s in %s; recovered in %s", rounds, point.name, killed.Round(time.Millisecond),
				(time.Since(began) - killed).Round(time.Millisecond))
			f.checkEffects()

			var want []string
			for fl, end := range map[flow]string{flowOnce: point.once, flowRetry: point.retry} {
				if runs[fl] > 0 {
					want = append(want, fmt.Sprintf("%s %s %d", fl.name, end, runs[fl]))
				}
			}
			slices.Sort(want)
			if got := f.mustLines(`SELECT def_name, state, terminal, doubts, writes, count(*) FROM (
					SELECT r.def_name, r.state, coalesce(r.terminal_name, '-') AS terminal,
						(SELECT count(*) FROM {s}dead_letters d WHERE d.run_id = r.id
							AND d.kind = 'effect_in_doubt' AND d.resolved_at IS NULL) AS doubts,
						(SELECT coalesce(sum(k.n), 0) FROM {s}executed_effects e
							JOIN {sink} k USING (idem_key) WHERE e.run_id = r.id) AS writes
					FROM {s}runs r) x
				GROUP BY 1, 2, 3, 4, 5 ORDER BY 1, 2, 3, 4, 5`); !slices.Equal(got, want) {
				t.Errorf("runs by flow, state, end, open in-doubt dead letters, sink writes, count:\n got %q\nwant %q", got, want)
			}
		})
	}
}

// recovered returns nil once there are runs runs, no claim is left
// unresolved, and every run either completed on its awaited event or waits
// on an open effect_in_doubt dead letter.
func (f *fixture) recovered(runs int) error {
	var errs []error
	if n, err := f.count(`SELECT count(*) FROM {s}runs`); err != nil || n != runs {
		errs = append(errs, fmt.Errorf("%d runs (err %v), want %d", n, err, runs))
	}
	unresolved, err := f.lines(`SELECT idem_key, action, attempt FROM {s}executed_effects
		WHERE done_at IS NULL AND NOT in_doubt ORDER BY idem_key`)
	if err != nil || len(unresolved) > 0 {
		errs = append(errs, fmt.Errorf("unresolved effects (err %v): %s", err, strings.Join(unresolved, "; ")))
	}
	pending, err := f.lines(`SELECT r.id, r.def_name, r.state, coalesce(r.terminal_name, '-'), coalesce(r.wait_step, '-'),
			r.lookback_pending
		FROM {s}runs r
		WHERE NOT (r.state = 'completed' AND r.terminal_name = 'got')
		AND NOT EXISTS (SELECT 1 FROM {s}dead_letters d
			WHERE d.run_id = r.id AND d.kind = 'effect_in_doubt' AND d.resolved_at IS NULL)
		ORDER BY r.created_at`)
	if err != nil || len(pending) > 0 {
		errs = append(errs, fmt.Errorf("runs neither completed nor blocked (err %v): %s", err, strings.Join(pending, "; ")))
	}
	return errors.Join(errs...)
}

// checkEffects fails the test unless the sink and the effects agree: no
// write without a claim, a non-idempotent effect written at most once and
// exactly once when done, every effect not done in doubt with exactly one
// open dead letter, no other dead letter, no blocked run ended, and every
// completed run consumed its awaited event.
func (f *fixture) checkEffects() {
	f.t.Helper()
	for _, c := range []struct {
		what, query string
		args        []any
	}{
		{"sink writes without a claim", `SELECT k.idem_key, k.n FROM {sink} k
			LEFT JOIN {s}executed_effects e USING (idem_key) WHERE e.idem_key IS NULL`, nil},
		{"non-idempotent effects written more than once", `SELECT k.idem_key, k.n FROM {sink} k
			JOIN {s}executed_effects e USING (idem_key) WHERE e.action = $1 AND k.n > 1`, []any{flowOnce.action}},
		{"done non-idempotent effects not written exactly once", `SELECT e.idem_key, coalesce(k.n, 0)
			FROM {s}executed_effects e LEFT JOIN {sink} k USING (idem_key)
			WHERE e.action = $1 AND e.done_at IS NOT NULL AND coalesce(k.n, 0) <> 1`, []any{flowOnce.action}},
		{"effects not done without exactly one open effect_in_doubt dead letter", `SELECT e.idem_key, e.action, e.in_doubt, d.n
			FROM {s}executed_effects e,
			LATERAL (SELECT count(*) AS n FROM {s}dead_letters
				WHERE kind = 'effect_in_doubt' AND ref = e.idem_key AND resolved_at IS NULL) d
			WHERE e.done_at IS NULL AND NOT (e.action = $1 AND e.in_doubt AND d.n = 1)`, []any{flowOnce.action}},
		{"dead letters other than an in-doubt effect", `SELECT d.kind, d.ref FROM {s}dead_letters d
			LEFT JOIN {s}executed_effects e ON e.idem_key = d.ref
			WHERE NOT (d.kind = 'effect_in_doubt' AND e.in_doubt AND e.done_at IS NULL)`, nil},
		{"ended runs with an open dead letter", `SELECT r.id, r.state FROM {s}runs r
			JOIN {s}dead_letters d ON d.run_id = r.id AND d.resolved_at IS NULL WHERE r.terminal`, nil},
		{"completed runs that consumed no awaited event", `SELECT r.id FROM {s}runs r
			WHERE r.state = 'completed' AND NOT EXISTS (SELECT 1 FROM {s}consumed_events c
				JOIN {s}domain_events ev ON ev.id = c.event_id
				WHERE c.run_id = r.id AND ev.type = $1 AND ev.payload->>'key' = r.entity_id)`, []any{evWake}},
	} {
		if bad := f.mustLines(c.query, c.args...); len(bad) > 0 {
			f.t.Errorf("%s: %s", c.what, strings.Join(bad, "; "))
		}
	}
}

// TestChaosDeadlineAfterWrite runs actions that write the sink, then
// overrun their deadline. The non-idempotent one goes in doubt with exactly
// one dead letter and is never executed again; the idempotent one is
// retried and completes.
func TestChaosDeadlineAfterWrite(t *testing.T) {
	requireChaos(t)
	t.Parallel()
	f := newFixture(t)
	f.start(recoveryWorkers, flowkit.RoleWorker, flowkit.RoleJanitor)
	f.emit("once", flowStallOnce.trigger, evWake)
	f.emit("retry", flowStallRetry.trigger, evWake)
	eventually(t, settleTimeout, func() error {
		doubt, err := f.count(`SELECT count(*) FROM {s}dead_letters WHERE kind = 'effect_in_doubt'`)
		if err != nil {
			return err
		}
		done, err := f.count(`SELECT count(*) FROM {s}runs WHERE def_name = $1 AND state = 'completed'`, flowStallRetry.name)
		if err != nil {
			return err
		}
		if doubt == 0 || done == 0 {
			return fmt.Errorf("%d effect_in_doubt dead letters, %d idempotent runs completed; want both", doubt, done)
		}
		return nil
	})
	// A janitor pass that starts now finds nothing to retry.
	since := time.Now()
	eventually(t, settleTimeout, func() error {
		n, err := f.count(`SELECT count(*) FROM {s}river_job WHERE kind = $1 AND state = 'completed' AND attempted_at > $2`,
			sched.KindJanitor, since)
		if err == nil && n == 0 {
			err = errors.New("no janitor pass completed yet")
		}
		return err
	})

	want := map[string]string{
		flowStallOnce.name: "running - true <nil> 0 1 1",
		// The first execution overran; the retry wrote the sink again and
		// completed.
		flowStallRetry.name: "completed got false done 1 2 0",
	}
	for def, w := range want {
		got := f.mustLines(`SELECT r.state, coalesce(r.terminal_name, '-'), e.in_doubt,
				CASE WHEN e.done_at IS NULL THEN NULL ELSE 'done' END, e.attempt, k.n,
				(SELECT count(*) FROM {s}dead_letters d WHERE d.ref = e.idem_key)
			FROM {s}runs r JOIN {s}executed_effects e ON e.run_id = r.id JOIN {sink} k USING (idem_key)
			WHERE r.def_name = $1`, def)
		if len(got) != 1 || got[0] != w {
			t.Errorf("%s: run state, end, in doubt, done, attempt, sink writes, dead letters = %q, want %q", def, got, w)
		}
	}
	if n := f.mustCount(`SELECT count(*) FROM {s}dead_letters`); n != 1 {
		t.Errorf("%d dead letters, want only the in-doubt effect's", n)
	}
}

// TestChaosRepeatedKillsUseUpAttempts kills the worker executing an
// idempotent action until its attempts are used up: each expired claim the
// janitor reclaims counts as an attempt, and the run then fails with an
// exhausted dead letter.
func TestChaosRepeatedKillsUseUpAttempts(t *testing.T) {
	requireChaos(t)
	t.Parallel()
	f := newFixture(t)
	// The janitor runs in this process for the whole test, so its leader
	// keeps scheduling passes while worker processes die.
	f.start(1, flowkit.RoleJanitor)
	f.emit("x", flowExhaust.trigger)
	for range exhaustAttempts {
		f.killAt(testhooks.PointExecuted, 2)
	}
	f.start(recoveryWorkers, flowkit.RoleWorker)
	eventually(t, settleTimeout, func() error {
		got, err := f.lines(`SELECT state, coalesce(terminal_name, '-') FROM {s}runs`)
		if err == nil && (len(got) != 1 || got[0] != "failed $action_failed") {
			err = fmt.Errorf("runs %q, want one failed $action_failed", got)
		}
		return err
	})

	if got := f.mustLines(`SELECT e.attempt, k.n, e.done_at IS NULL, e.in_doubt FROM {s}executed_effects e
		JOIN {sink} k USING (idem_key)`); len(got) != 1 || got[0] != fmt.Sprintf("%d %d true false", exhaustAttempts, exhaustAttempts) {
		t.Errorf("effect attempt, sink writes, not done, in doubt = %q, want %d %d true false", got, exhaustAttempts, exhaustAttempts)
	}
	if n := f.mustCount(`SELECT count(*) FROM {s}run_steps WHERE kind = 'attempt_failed' AND error LIKE '%claim expired%'`); n != exhaustAttempts {
		t.Errorf("%d attempts failed on an expired claim, want %d", n, exhaustAttempts)
	}
	if got := f.mustLines(`SELECT kind FROM {s}dead_letters`); len(got) != 1 || got[0] != "exhausted" {
		t.Errorf("dead letters %q, want one exhausted", got)
	}
}
