package obs

import (
	"context"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestNilProvidersAreNoOps(t *testing.T) {
	_, span := Tracer(nil).Start(context.Background(), "s")
	span.End()
	c, err := Meter(nil).Int64Counter("c")
	if err != nil {
		t.Fatal(err)
	}
	c.Add(context.Background(), 1)
}

func TestRecordDelayInSecondsClampingSkew(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	h, err := Meter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))).Float64Histogram("delay")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	RecordDelay(ctx, h, 1500*time.Millisecond)
	RecordDelay(ctx, h, -time.Second)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	if len(rm.ScopeMetrics) != 1 || rm.ScopeMetrics[0].Scope.Name != Scope {
		t.Fatalf("scopes = %+v, want flowkit's", rm.ScopeMetrics)
	}
	hist := rm.ScopeMetrics[0].Metrics[0].Data.(metricdata.Histogram[float64])
	p := hist.DataPoints[0]
	if p.Count != 2 || p.Sum != 1.5 {
		t.Fatalf("count %d, sum %v; want 2 records summing to 1.5s", p.Count, p.Sum)
	}
	if lo, _ := p.Min.Value(); lo != 0 {
		t.Fatalf("min = %v, want a negative delay recorded as 0", lo)
	}
}
