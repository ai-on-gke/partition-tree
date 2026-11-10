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

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const (
	defaultCustomerBatchSize     = 200
	defaultCustomerMaxConcurrent = 10
)

// ConcurrentBatchExporter wraps any sdkmetric.Exporter (such as gcmetric for customer direct-push),
// slices large ResourceMetrics payloads into sub-batches of at most maxTimeSeriesPerBatch points,
// and exports them concurrently across a bounded pool of worker goroutines.
//
// This is critical for Cloud Monitoring direct-push because upstream gcmetric processes batches
// sequentially in a single-threaded loop (200 series per CreateTimeSeries gRPC RPC). By splitting
// into sub-batches of <= 200 series before calling inner.Export(), each worker triggers exactly one
// gRPC call, executing up to maxConcurrentExports RPCs in parallel.
type ConcurrentBatchExporter struct {
	inner                 sdkmetric.Exporter
	maxTimeSeriesPerBatch int
	maxConcurrentExports  int
}

// NewConcurrentBatchExporter creates a new ConcurrentBatchExporter wrapping inner with default
// batch size (200) and concurrency (10).
func NewConcurrentBatchExporter(inner sdkmetric.Exporter) *ConcurrentBatchExporter {
	return NewConcurrentBatchExporterWithLimits(inner, defaultCustomerBatchSize, defaultCustomerMaxConcurrent)
}

// NewConcurrentBatchExporterWithLimits creates a ConcurrentBatchExporter with custom limits.
func NewConcurrentBatchExporterWithLimits(inner sdkmetric.Exporter, batchSize, maxConcurrency int) *ConcurrentBatchExporter {
	if batchSize <= 0 {
		batchSize = defaultCustomerBatchSize
	}
	if maxConcurrency <= 0 {
		maxConcurrency = defaultCustomerMaxConcurrent
	}
	return &ConcurrentBatchExporter{
		inner:                 inner,
		maxTimeSeriesPerBatch: batchSize,
		maxConcurrentExports:  maxConcurrency,
	}
}

func (e *ConcurrentBatchExporter) Temporality(kind sdkmetric.InstrumentKind) metricdata.Temporality {
	return e.inner.Temporality(kind)
}

func (e *ConcurrentBatchExporter) Aggregation(kind sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return e.inner.Aggregation(kind)
}

func (e *ConcurrentBatchExporter) ForceFlush(ctx context.Context) error {
	return e.inner.ForceFlush(ctx)
}

func (e *ConcurrentBatchExporter) Shutdown(ctx context.Context) error {
	return e.inner.Shutdown(ctx)
}

func (e *ConcurrentBatchExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	if rm == nil {
		return errors.New("nil resource metrics")
	}

	batches := splitResourceMetrics(rm, e.maxTimeSeriesPerBatch)
	return exportConcurrentBatches(ctx, e.inner, batches, e.maxConcurrentExports)
}

// exportConcurrentBatches exports batches using a bounded worker pool. If len(batches) <= 2,
// it exports sequentially to avoid goroutine overhead.
func exportConcurrentBatches(ctx context.Context, exporter sdkmetric.Exporter, batches []metricdata.ResourceMetrics, maxConcurrency int) error {
	if len(batches) == 0 {
		return nil
	}

	var (
		exportErrors []error
		errMu        sync.Mutex
	)
	recordErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		exportErrors = append(exportErrors, err)
		errMu.Unlock()
	}

	if len(batches) <= 2 {
		for i := range batches {
			if err := ctx.Err(); err != nil {
				recordErr(err)
				break
			}
			if err := exporter.Export(ctx, &batches[i]); err != nil {
				recordErr(err)
			}
		}
	} else {
		var wg sync.WaitGroup
		sem := make(chan struct{}, maxConcurrency)
		for i := range batches {
			if err := ctx.Err(); err != nil {
				recordErr(err)
				break
			}
			sem <- struct{}{}
			wg.Add(1)
			go func(b *metricdata.ResourceMetrics) {
				defer wg.Done()
				defer func() { <-sem }()
				if err := ctx.Err(); err != nil {
					recordErr(err)
					return
				}
				if err := exporter.Export(ctx, b); err != nil {
					recordErr(err)
				}
			}(&batches[i])
		}
		wg.Wait()
	}

	if len(exportErrors) > 0 {
		return errors.Join(exportErrors...)
	}
	return ctx.Err()
}

func metricPointCount(m metricdata.Metrics) int {
	switch d := m.Data.(type) {
	case metricdata.Gauge[int64]:
		return len(d.DataPoints)
	case metricdata.Gauge[float64]:
		return len(d.DataPoints)
	case metricdata.Histogram[float64]:
		return len(d.DataPoints)
	case metricdata.Histogram[int64]:
		return len(d.DataPoints)
	case metricdata.Sum[int64]:
		return len(d.DataPoints)
	case metricdata.Sum[float64]:
		return len(d.DataPoints)
	default:
		return 1
	}
}

func sliceMetricData(m metricdata.Metrics, start, end int) metricdata.Metrics {
	clone := m
	switch d := m.Data.(type) {
	case metricdata.Gauge[int64]:
		clone.Data = metricdata.Gauge[int64]{DataPoints: d.DataPoints[start:end]}
	case metricdata.Gauge[float64]:
		clone.Data = metricdata.Gauge[float64]{DataPoints: d.DataPoints[start:end]}
	case metricdata.Histogram[float64]:
		clone.Data = metricdata.Histogram[float64]{
			DataPoints:  d.DataPoints[start:end],
			Temporality: d.Temporality,
		}
	case metricdata.Histogram[int64]:
		clone.Data = metricdata.Histogram[int64]{
			DataPoints:  d.DataPoints[start:end],
			Temporality: d.Temporality,
		}
	case metricdata.Sum[int64]:
		clone.Data = metricdata.Sum[int64]{
			DataPoints:  d.DataPoints[start:end],
			Temporality: d.Temporality,
			IsMonotonic: d.IsMonotonic,
		}
	case metricdata.Sum[float64]:
		clone.Data = metricdata.Sum[float64]{
			DataPoints:  d.DataPoints[start:end],
			Temporality: d.Temporality,
			IsMonotonic: d.IsMonotonic,
		}
	}
	return clone
}

// splitResourceMetrics chunks rm into smaller ResourceMetrics slices where each slice
// contains at most maxPoints total data points across all its metrics.
func splitResourceMetrics(rm *metricdata.ResourceMetrics, maxPoints int) []metricdata.ResourceMetrics {
	if rm == nil {
		return nil
	}
	var batches []metricdata.ResourceMetrics

	var currentScopeMetrics []metricdata.ScopeMetrics
	var currentMetrics []metricdata.Metrics
	currentPointCount := 0

	flushBatch := func() {
		if len(currentMetrics) > 0 {
			currentScopeMetrics = append(currentScopeMetrics, metricdata.ScopeMetrics{
				Metrics: currentMetrics,
			})
			currentMetrics = nil
		}
		if len(currentScopeMetrics) > 0 {
			batches = append(batches, metricdata.ResourceMetrics{
				Resource:     rm.Resource,
				ScopeMetrics: currentScopeMetrics,
			})
			currentScopeMetrics = nil
			currentPointCount = 0
		}
	}

	for _, sm := range rm.ScopeMetrics {
		scope := sm.Scope
		for _, m := range sm.Metrics {
			total := metricPointCount(m)
			if total == 0 {
				continue
			}
			offset := 0
			for offset < total {
				space := maxPoints - currentPointCount
				take := min(space, total-offset)
				currentMetrics = append(currentMetrics, sliceMetricData(m, offset, offset+take))
				currentPointCount += take
				offset += take
				if currentPointCount >= maxPoints {
					if len(currentMetrics) > 0 {
						currentScopeMetrics = append(currentScopeMetrics, metricdata.ScopeMetrics{
							Scope:   scope,
							Metrics: currentMetrics,
						})
						currentMetrics = nil
					}
					flushBatch()
				}
			}
		}
		if len(currentMetrics) > 0 {
			currentScopeMetrics = append(currentScopeMetrics, metricdata.ScopeMetrics{
				Scope:   scope,
				Metrics: currentMetrics,
			})
			currentMetrics = nil
		}
	}
	flushBatch()
	return batches
}
