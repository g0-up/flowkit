package interpreter

import (
	"time"

	"github.com/g0-up/flowkit/internal/store"
)

// eligibility is the one rule deciding which events a wait of a run may
// claim, shared by the look-back and the route of new events so both
// paths agree on every event whichever comes first:
//
//   - the event has the waited type;
//   - it was stored no earlier than the run's creation minus
//     LookbackMargin;
//   - it is not the run's trigger;
//   - the run has not consumed it.
//
// The last condition is checked in SQL by both queries (LookbackPage and
// ListEventSubscriptions); admits checks the others.
type eligibility struct {
	eventType string
	since     time.Time
	trigger   int64
}

func (in *Interpreter) eligibility(run store.RunRow, eventType string) eligibility {
	return eligibility{eventType: eventType, since: run.CreatedAt.Add(-in.lookbackMargin), trigger: run.TriggerEventID}
}

func (e eligibility) admits(ev store.EventRow) bool {
	return ev.Type == e.eventType && !ev.InsertedAt.Before(e.since) && ev.ID != e.trigger
}
