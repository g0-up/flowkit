// Package obs holds the OpenTelemetry plumbing flowkit's packages share:
// the instrumentation scope, no-op defaults for absent providers, and unit
// conversions. It uses the OpenTelemetry API only; the consumer supplies
// the SDK through Config.Tracer and Config.Meter.
package obs

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// Scope is flowkit's instrumentation scope: its module path.
const Scope = "github.com/g0-up/flowkit"

// Tracer returns flowkit's tracer of tp, or a no-op tracer when tp is nil.
func Tracer(tp trace.TracerProvider) trace.Tracer {
	if tp == nil {
		tp = tracenoop.NewTracerProvider()
	}
	return tp.Tracer(Scope)
}

// Meter returns flowkit's meter of mp, or a no-op meter when mp is nil.
func Meter(mp metric.MeterProvider) metric.Meter {
	if mp == nil {
		mp = metricnoop.NewMeterProvider()
	}
	return mp.Meter(Scope)
}

// RecordDelay records d in seconds, the unit of flowkit's latency
// histograms. A negative d, which clock skew between instances can give,
// counts as zero.
func RecordDelay(ctx context.Context, h metric.Float64Histogram, d time.Duration, opts ...metric.RecordOption) {
	h.Record(ctx, max(d, 0).Seconds(), opts...)
}
