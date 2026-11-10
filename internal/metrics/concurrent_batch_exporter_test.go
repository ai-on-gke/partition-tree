/*
Copyright 2026 Google LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package metrics

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

type recordingExporter struct {
	mu          sync.Mutex
	exported    []*metricdata.ResourceMetrics
	delay       time.Duration
	err         error
	activeCalls int32
	maxObserved int32
}

func (r *recordingExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	cur := atomic.AddInt32(&r.activeCalls, 1)
	defer atomic.AddInt32(&r.activeCalls, -1)

	for {
		max := atomic.LoadInt32(&r.maxObserved)
		if cur <= max || atomic.CompareAndSwapInt32(&r.maxObserved, max, cur) {
			break
		}
	}

	if r.delay > 0 {
		time.Sleep(r.delay)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.exported = append(r.exported, rm)
	return r.err
}

func (r *recordingExporter) ForceFlush(ctx context.Context) error { return nil }
func (r *recordingExporter) Shutdown(ctx context.Context) error   { return nil }
func (r *recordingExporter) Temporality(kind sdkmetric.InstrumentKind) metricdata.Temporality {
	return sdkmetric.DefaultTemporalitySelector(kind)
}
func (r *recordingExporter) Aggregation(kind sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.DefaultAggregationSelector(kind)
}

func TestConcurrentBatchExporter_Chunking(t *testing.T) {
	ctx := context.Background()
	rec := &recordingExporter{}
	exp := NewConcurrentBatchExporterWithLimits(rec, 200, 10)

	// Create 550 data points across 2 metrics in 1 scope
	dps1 := make([]metricdata.DataPoint[int64], 350)
	for i := range dps1 {
		dps1[i] = metricdata.DataPoint[int64]{
			Value:      int64(i),
			Attributes: attribute.NewSet(attribute.Int("idx", i)),
		}
	}
	dps2 := make([]metricdata.DataPoint[int64], 200)
	for i := range dps2 {
		dps2[i] = metricdata.DataPoint[int64]{
			Value:      int64(i),
			Attributes: attribute.NewSet(attribute.Int("idx", i)),
		}
	}

	testRes := resource.NewSchemaless(attribute.String("res.key", "res.val"))
	rm := &metricdata.ResourceMetrics{
		Resource: testRes,
		ScopeMetrics: []metricdata.ScopeMetrics{{
			Scope: instrumentation.Scope{Name: "test-scope"},
			Metrics: []metricdata.Metrics{
				{
					Name: "metric1",
					Data: metricdata.Gauge[int64]{DataPoints: dps1},
				},
				{
					Name: "metric2",
					Data: metricdata.Gauge[int64]{DataPoints: dps2},
				},
			},
		}},
	}

	if err := exp.Export(ctx, rm); err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()

	// 550 total points / 200 batchSize = 3 batches (200, 200, 150)
	if len(rec.exported) != 3 {
		t.Fatalf("expected 3 exported batches, got %d", len(rec.exported))
	}

	totalPoints := 0
	for idx, b := range rec.exported {
		pointsInBatch := 0
		for _, sm := range b.ScopeMetrics {
			for _, m := range sm.Metrics {
				pointsInBatch += metricPointCount(m)
			}
		}
		if pointsInBatch > 200 {
			t.Errorf("batch %d has %d points, want <= 200", idx, pointsInBatch)
		}
		totalPoints += pointsInBatch
	}

	if totalPoints != 550 {
		t.Errorf("total points across batches = %d, want 550", totalPoints)
	}
}

func TestConcurrentBatchExporter_Parallelism(t *testing.T) {
	ctx := context.Background()
	rec := &recordingExporter{
		delay: 15 * time.Millisecond,
	}
	exp := NewConcurrentBatchExporterWithLimits(rec, 10, 5)

	// Create 100 points -> 10 batches of 10 points
	dps := make([]metricdata.DataPoint[int64], 100)
	for i := range dps {
		dps[i] = metricdata.DataPoint[int64]{Value: int64(i)}
	}
	rm := &metricdata.ResourceMetrics{
		Resource: resource.Empty(),
		ScopeMetrics: []metricdata.ScopeMetrics{{
			Metrics: []metricdata.Metrics{{
				Name: "concurrent_test",
				Data: metricdata.Gauge[int64]{DataPoints: dps},
			}},
		}},
	}

	start := time.Now()
	if err := exp.Export(ctx, rm); err != nil {
		t.Fatalf("Export failed: %v", err)
	}
	elapsed := time.Since(start)

	maxConcurrent := atomic.LoadInt32(&rec.maxObserved)
	if maxConcurrent < 2 {
		t.Errorf("max observed concurrent calls = %d, want >= 2 (parallelism expected)", maxConcurrent)
	}
	if maxConcurrent > 5 {
		t.Errorf("max observed concurrent calls = %d, exceeded limit 5", maxConcurrent)
	}

	// 10 batches / 5 concurrency = 2 rounds * 15ms = ~30ms (+ overhead).
	// If it were sequential, it would take 10 * 15ms = 150ms.
	if elapsed > 100*time.Millisecond {
		t.Errorf("export took %v, expected < 100ms with 5-way parallelism", elapsed)
	}
}

func TestConcurrentBatchExporter_ErrorHandling(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("simulated gRPC failure")
	rec := &recordingExporter{
		err: expectedErr,
	}
	exp := NewConcurrentBatchExporterWithLimits(rec, 10, 4)

	dps := make([]metricdata.DataPoint[int64], 40)
	rm := &metricdata.ResourceMetrics{
		Resource: resource.Empty(),
		ScopeMetrics: []metricdata.ScopeMetrics{{
			Metrics: []metricdata.Metrics{{
				Name: "err_test",
				Data: metricdata.Gauge[int64]{DataPoints: dps},
			}},
		}},
	}

	err := exp.Export(ctx, rm)
	if err == nil {
		t.Fatalf("expected error from Export, got nil")
	}
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error to wrap %v, got %v", expectedErr, err)
	}
}

func TestConcurrentBatchExporter_Nil(t *testing.T) {
	rec := &recordingExporter{}
	exp := NewConcurrentBatchExporter(rec)

	if err := exp.Export(context.Background(), nil); err == nil {
		t.Error("Export(nil) expected error, got nil")
	}
}
