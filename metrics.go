package flowkit

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/g0-up/flowkit/internal/sched"
	"github.com/g0-up/flowkit/internal/store"
)

// deadLetterKinds are the kinds a dead letter can have. The open gauge
// reports every kind, zero included, so an alert on it resolves.
var deadLetterKinds = []string{"effect_in_doubt", "exhausted", "schema_mismatch", "unroutable_event"}

// gaugeReadTimeout bounds the database read behind one collection of the
// gauges.
const gaugeReadTimeout = 5 * time.Second

// engineMetrics are the instruments the Engine owns itself.
type engineMetrics struct {
	emitted metric.Int64Counter
	gauges  metric.Registration
}

// newEngineMetrics creates the Engine's instruments and registers the
// gauges, read from the database whenever the MeterProvider collects.
// Every instance reports the same database-wide values, except
// scheduler_leader, which is 1 only on the instance that leads.
func newEngineMetrics(meter metric.Meter, e *Engine) (*engineMetrics, error) {
	emitted, err := meter.Int64Counter("flowkit.events_emitted",
		metric.WithDescription("Events Emit stored, by type; an event whose transaction rolls back still counts."))
	if err != nil {
		return nil, err
	}
	open, err := meter.Int64ObservableGauge("flowkit.dead_letters_open",
		metric.WithDescription("Unresolved dead letters, by kind."))
	if err != nil {
		return nil, err
	}
	inDoubt, err := meter.Int64ObservableGauge("flowkit.in_doubt_open",
		metric.WithDescription("Effects in doubt of runs that are not terminal."))
	if err != nil {
		return nil, err
	}
	depth, err := meter.Int64ObservableGauge("flowkit.queue_depth",
		metric.WithDescription("Jobs available to work in the flowkit queue."))
	if err != nil {
		return nil, err
	}
	leader, err := meter.Int64ObservableGauge("flowkit.scheduler_leader",
		metric.WithDescription("1 on the instance that is the scheduler leader, 0 on the others."))
	if err != nil {
		return nil, err
	}
	reg, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		ctx, cancel := context.WithTimeout(ctx, gaugeReadTimeout)
		defer cancel()
		var g store.Gauges
		err := e.inTx(ctx, func(tx store.Tx) error {
			var err error
			g, err = e.store.ReadGauges(ctx, tx, sched.Queue)
			return err
		})
		if err != nil {
			return fmt.Errorf("flowkit: read gauges: %w", err)
		}
		for _, kind := range deadLetterKinds {
			o.ObserveInt64(open, int64(g.OpenDeadLetters[kind]), metric.WithAttributes(attribute.String("kind", kind)))
		}
		o.ObserveInt64(inDoubt, int64(g.InDoubtOpen))
		o.ObserveInt64(depth, int64(g.QueueDepth))
		var leading int64
		if e.client != nil && g.LeaderID != "" && g.LeaderID == e.client.ID() {
			leading = 1
		}
		o.ObserveInt64(leader, leading, metric.WithAttributes(attribute.String("instance", e.cfg.InstanceID)))
		return nil
	}, open, inDoubt, depth, leader)
	if err != nil {
		return nil, err
	}
	return &engineMetrics{emitted: emitted, gauges: reg}, nil
}
