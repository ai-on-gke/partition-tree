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
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Stream names, phase values, and status values for partition_tree_metrics_production_duration_seconds.
const (
	StreamCustomer = "customer"
	StreamGoogle   = "google"

	PhaseObserve = "observe"
	PhaseExport  = "export"
	PhaseTotal   = "total"

	StatusSuccess = "success"
	StatusError   = "error"
)

// DefaultProductionDurationBuckets defines explicit histogram bucket boundaries (in seconds)
// from 0.1s to 120s for cluster metric observation and export passes.
var DefaultProductionDurationBuckets = []float64{
	0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 60.0, 90.0, 120.0,
}

var (
	globalProductionRecorder = newProductionDurationRecorder()
	globalGoogleTracker      = &streamCycleTracker{}
	globalCustomerTracker    = &streamCycleTracker{}
)

func trackerForStream(streamName string) *streamCycleTracker {
	if streamName == StreamGoogle {
		return globalGoogleTracker
	}
	return globalCustomerTracker
}

// streamCycleTracker tracks the start timestamp of a collection cycle for a specific stream
// so that when Exporter.Export() completes, it can compute true end-to-end (observe + export) duration.
type streamCycleTracker struct {
	mu        sync.Mutex
	lastStart time.Time
}

func (t *streamCycleTracker) setStart(now time.Time) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastStart = now
}

func (t *streamCycleTracker) takeStart() (time.Time, bool) {
	if t == nil {
		return time.Time{}, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lastStart.IsZero() {
		return time.Time{}, false
	}
	start := t.lastStart
	t.lastStart = time.Time{}
	return start, true
}

func (t *streamCycleTracker) reset() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastStart = time.Time{}
}

// productionDurationRecorder safely records observe, export, and total durations
// into the customer-facing Float64Histogram instrument when customer metrics are enabled.
type productionDurationRecorder struct {
	mu   sync.RWMutex
	hist metric.Float64Histogram
}

func newProductionDurationRecorder() *productionDurationRecorder {
	return &productionDurationRecorder{}
}

func (r *productionDurationRecorder) setHistogram(h metric.Float64Histogram) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hist = h
}

func (r *productionDurationRecorder) record(ctx context.Context, stream, phase, status string, d time.Duration) {
	if r == nil {
		return
	}
	r.mu.RLock()
	h := r.hist
	r.mu.RUnlock()
	if h == nil {
		return
	}
	h.Record(ctx, d.Seconds(), metric.WithAttributes(
		attribute.String(Attr_stream, stream),
		attribute.String(Attr_phase, phase),
		attribute.String(Attr_status, status),
	))
}

// instrumentedExporter wraps an sdkmetric.Exporter to record the duration of Exporter.Export()
// ("export" phase) and the end-to-end duration since the collection callback started ("total" phase).
type instrumentedExporter struct {
	inner      sdkmetric.Exporter
	streamName string
	tracker    *streamCycleTracker
	recorder   *productionDurationRecorder
}

// WrapExporter wraps inner with latency tracking for export and total cycle duration
// for the specified stream (StreamCustomer or StreamGoogle).
func WrapExporter(inner sdkmetric.Exporter, streamName string) sdkmetric.Exporter {
	if inner == nil {
		return nil
	}
	return &instrumentedExporter{
		inner:      inner,
		streamName: streamName,
		tracker:    trackerForStream(streamName),
		recorder:   globalProductionRecorder,
	}
}

func (e *instrumentedExporter) Temporality(kind sdkmetric.InstrumentKind) metricdata.Temporality {
	return e.inner.Temporality(kind)
}

func (e *instrumentedExporter) Aggregation(kind sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return e.inner.Aggregation(kind)
}

func (e *instrumentedExporter) ForceFlush(ctx context.Context) error {
	return e.inner.ForceFlush(ctx)
}

func (e *instrumentedExporter) Shutdown(ctx context.Context) error {
	return e.inner.Shutdown(ctx)
}

func (e *instrumentedExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	exportStart := time.Now()
	err := e.inner.Export(ctx, rm)
	now := time.Now()

	status := StatusSuccess
	if err != nil {
		status = StatusError
	}

	if e.recorder != nil {
		e.recorder.record(ctx, e.streamName, PhaseExport, status, now.Sub(exportStart))
		if e.tracker != nil {
			if cycleStart, ok := e.tracker.takeStart(); ok {
				e.recorder.record(ctx, e.streamName, PhaseTotal, status, now.Sub(cycleStart))
			}
		}
	}
	return err
}
