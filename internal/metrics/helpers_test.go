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
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

// setupTestMeters initializes OTEL providers with manual readers, registers cleanup, and clears gauge sets.
func setupTestMeters(t *testing.T) (*sdkmetric.ManualReader, *sdkmetric.ManualReader) {
	t.Helper()
	rc := sdkmetric.NewManualReader()
	rg := sdkmetric.NewManualReader()
	shutdown, err := InitWithReaders(context.Background(), rc, rg, testMeta, 30*time.Minute, testCardinalityLimit, true, true)
	if err != nil {
		t.Fatalf("setupTestMeters: %v", err)
	}
	t.Cleanup(func() { shutdown() })
	ClearGaugeSets()
	return rc, rg
}

// getMetricPoints extracts int64 gauge data points for targetName from ResourceMetrics.
func getMetricPoints(rm metricdata.ResourceMetrics, targetName string) []metricdata.DataPoint[int64] {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == targetName {
				if g, ok := m.Data.(metricdata.Gauge[int64]); ok {
					return g.DataPoints
				}
			}
		}
	}
	return nil
}

// attrsMap converts data point attributes to a string map for concise assertions.
func attrsMap(dp metricdata.DataPoint[int64]) map[string]string {
	m := make(map[string]string, dp.Attributes.Len())
	for _, a := range dp.Attributes.ToSlice() {
		m[string(a.Key)] = a.Value.AsString()
	}
	return m
}

// resAttrsMap converts resource attributes to a string map for concise assertions.
func resAttrsMap(res *resource.Resource) map[string]string {
	m := make(map[string]string)
	if res == nil {
		return m
	}
	for _, a := range res.Attributes() {
		m[string(a.Key)] = a.Value.AsString()
	}
	return m
}

// gaugeRM constructs a single-metric ResourceMetrics wrapping the given points.
func gaugeRM(name string, dps ...metricdata.DataPoint[int64]) metricdata.ResourceMetrics {
	return metricdata.ResourceMetrics{
		Resource: resource.Empty(),
		ScopeMetrics: []metricdata.ScopeMetrics{{
			Metrics: []metricdata.Metrics{{
				Name: name,
				Data: metricdata.Gauge[int64]{DataPoints: dps},
			}},
		}},
	}
}

// getHistogramPoints extracts float64 histogram data points for targetName from ResourceMetrics.
func getHistogramPoints(rm metricdata.ResourceMetrics, targetName string) []metricdata.HistogramDataPoint[float64] {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == targetName {
				if h, ok := m.Data.(metricdata.Histogram[float64]); ok {
					return h.DataPoints
				}
			}
		}
	}
	return nil
}

// histAttrsMap converts histogram data point attributes to a string map for concise assertions.
func histAttrsMap(dp metricdata.HistogramDataPoint[float64]) map[string]string {
	m := make(map[string]string, dp.Attributes.Len())
	for _, a := range dp.Attributes.ToSlice() {
		m[string(a.Key)] = a.Value.AsString()
	}
	return m
}
