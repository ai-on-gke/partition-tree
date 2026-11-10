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

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"

	"github.com/ai-on-gke/partition-tree/internal/metadata"
)

// GoogleExporter is an OpenTelemetry exporter wrapper that intercepts
// fleet-wide metricdata.ResourceMetrics, routes metrics to the appropriate
// monitored resource exporter (k8s_entity for domain metrics, k8s_container
// for build_info), groups domain data points by PartitionTree entity identity,
// strips promoted attributes from metric payloads to avoid Cloud Monitoring
// descriptor collisions, and emits per-resource ResourceMetrics.
type GoogleExporter struct {
	entityExporter    sdkmetric.Exporter
	containerExporter sdkmetric.Exporter
	meta              metadata.GoogleMetadata
}

// NewGoogleExporter creates a new GoogleExporter wrapping the provided entity and container exporters.
// If containerExporter is nil, entityExporter is used for both resource types (useful in tests).
func NewGoogleExporter(entityExporter, containerExporter sdkmetric.Exporter, meta metadata.GoogleMetadata) *GoogleExporter {
	if containerExporter == nil {
		containerExporter = entityExporter
	}
	return &GoogleExporter{
		entityExporter:    entityExporter,
		containerExporter: containerExporter,
		meta:              meta,
	}
}

// Temporality returns the metric temporality selector for the underlying exporter.
func (e *GoogleExporter) Temporality(kind sdkmetric.InstrumentKind) metricdata.Temporality {
	return e.entityExporter.Temporality(kind)
}

// Aggregation returns the metric aggregation selector for the underlying exporter.
func (e *GoogleExporter) Aggregation(kind sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return e.entityExporter.Aggregation(kind)
}

// ForceFlush flushes all pending metric data in the underlying exporter.
func (e *GoogleExporter) ForceFlush(ctx context.Context) error {
	if e.entityExporter == e.containerExporter {
		return e.entityExporter.ForceFlush(ctx)
	}
	return errors.Join(e.entityExporter.ForceFlush(ctx), e.containerExporter.ForceFlush(ctx))
}

// Shutdown flushes and terminates the underlying exporter.
func (e *GoogleExporter) Shutdown(ctx context.Context) error {
	if e.entityExporter == e.containerExporter {
		return e.entityExporter.Shutdown(ctx)
	}
	return errors.Join(e.entityExporter.Shutdown(ctx), e.containerExporter.Shutdown(ctx))
}

// entityKey uniquely identifies a PartitionTree custom resource entity.
type entityKey struct {
	name string
	uid  string
}

const (
	maxTimeSeriesPerBatch = 200
	maxConcurrentExports  = 10
)

// entityMetricSlice holds stripped data points for a single metric belonging to a specific entity.
type entityMetricSlice struct {
	name        string
	description string
	unit        string
	points      []metricdata.DataPoint[int64]
}

// Export processes the ResourceMetrics by splitting them per entity and promoting
// partition_tree_name/uid to k8s_entity resource labels.
func (e *GoogleExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	if rm == nil {
		return errors.New("nil resource metrics")
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

	var batches []metricdata.ResourceMetrics

	for _, sm := range rm.ScopeMetrics {
		// Map entityKey -> list of metrics for that entity so all domain metrics
		// (occupancy, taint, info) for the same PartitionTree CR are packed together
		// into batches of at most maxTimeSeriesPerBatch (200) points.
		entityMap := make(map[entityKey][]entityMetricSlice)
		var entityOrder []entityKey

		for _, m := range sm.Metrics {
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			if !ok {
				// Forward non-int64 gauge metrics unchanged.
				subRM := metricdata.ResourceMetrics{
					Resource: rm.Resource,
					ScopeMetrics: []metricdata.ScopeMetrics{{
						Scope:   sm.Scope,
						Metrics: []metricdata.Metrics{m},
					}},
				}
				if err := e.entityExporter.Export(ctx, &subRM); err != nil {
					recordErr(err)
				}
				continue
			}

			// Handle build_info directly as a daemon container metric.
			// While domain metrics (occupancy, taint, info) represent individual PartitionTree custom resources
			// under the k8s_entity resource, build_info represents the running controller daemon container.
			// It targets k8s_container with pod_name, namespace_name, and container_name as resource labels,
			// stripping pod_name from the metric data point to avoid Cloud Monitoring descriptor collisions.
			if m.Name == MetricGooglePartitionTreeBuildInfo {
				namespace, _ := readNamespace()
				groups := make(map[string][]metricdata.DataPoint[int64])
				for _, dp := range gauge.DataPoints {
					var podName string
					filteredSet, _ := dp.Attributes.Filter(func(kv attribute.KeyValue) bool {
						if kv.Key == Attr_pod_name {
							podName = kv.Value.AsString()
							return false
						}
						return true
					})
					if podName == "" {
						podName = getPodName()
					}
					dp.Attributes = filteredSet
					groups[podName] = append(groups[podName], dp)
				}

				for podName, points := range groups {
					res := resource.NewSchemaless(
						attribute.String(Attr_gcp_resource_type, ResourceTypeGoogleContainer),
						attribute.String(Attr_project_id, e.meta.ProjectID),
						attribute.String(Attr_location, e.meta.ClusterLocation),
						attribute.String(Attr_cluster_name, e.meta.ClusterName),
						attribute.String(Attr_namespace_name, namespace),
						attribute.String(Attr_pod_name, podName),
						attribute.String(Attr_container_name, ServiceName),
					)
					subRM := metricdata.ResourceMetrics{
						Resource: res,
						ScopeMetrics: []metricdata.ScopeMetrics{{
							Scope: sm.Scope,
							Metrics: []metricdata.Metrics{{
								Name:        m.Name,
								Description: m.Description,
								Unit:        m.Unit,
								Data:        metricdata.Gauge[int64]{DataPoints: points},
							}},
						}},
					}
					if err := e.containerExporter.Export(ctx, &subRM); err != nil {
						recordErr(err)
					}
				}
				continue
			}

			// Group points for this metric by (partition_tree_name, partition_tree_uid)
			perEntityPts := make(map[entityKey][]metricdata.DataPoint[int64])
			for _, dp := range gauge.DataPoints {
				var name, uid string

				// CRITICAL: Strip partition_tree_name and partition_tree_uid from the metric attributes
				// so they do not leak into the Cloud Monitoring metric labels and violate metric descriptors.
				filteredSet, _ := dp.Attributes.Filter(func(kv attribute.KeyValue) bool {
					switch kv.Key {
					case Attr_partition_tree_name:
						name = kv.Value.AsString()
						return false
					case Attr_partition_tree_uid:
						uid = kv.Value.AsString()
						return false
					default:
						return true
					}
				})

				dp.Attributes = filteredSet
				key := entityKey{name: name, uid: uid}
				if _, seen := entityMap[key]; !seen {
					entityMap[key] = nil
					entityOrder = append(entityOrder, key)
				}
				perEntityPts[key] = append(perEntityPts[key], dp)
			}

			for _, key := range entityOrder {
				if pts, ok := perEntityPts[key]; ok {
					entityMap[key] = append(entityMap[key], entityMetricSlice{
						name:        m.Name,
						description: m.Description,
						unit:        m.Unit,
						points:      pts,
					})
				}
			}
		}

		// Pack each entity's intermingled metrics into ResourceMetrics batches of at most maxTimeSeriesPerBatch (200) points.
		for _, key := range entityOrder {
			res := resource.NewSchemaless(
				attribute.String(Attr_gcp_resource_type, ResourceTypeGoogle),
				attribute.String(Attr_project_id, e.meta.ProjectID),
				attribute.String(Attr_location, e.meta.ClusterLocation),
				attribute.String(Attr_cluster_name, e.meta.ClusterName),
				attribute.String(Attr_entity_type, EntityTypePartitionTree),
				attribute.String(Attr_entity_namespace, ""),
				attribute.String(Attr_entity_name, key.name),
				attribute.String(Attr_entity_uid, key.uid),
			)

			var batchMetrics []metricdata.Metrics
			batchPointCount := 0

			flushBatch := func() {
				if len(batchMetrics) == 0 {
					return
				}
				batches = append(batches, metricdata.ResourceMetrics{
					Resource: res,
					ScopeMetrics: []metricdata.ScopeMetrics{{
						Scope:   sm.Scope,
						Metrics: batchMetrics,
					}},
				})
				batchMetrics = nil
				batchPointCount = 0
			}

			for _, slice := range entityMap[key] {
				rem := slice.points
				for len(rem) > 0 {
					space := maxTimeSeriesPerBatch - batchPointCount
					take := min(space, len(rem))
					batchMetrics = append(batchMetrics, metricdata.Metrics{
						Name:        slice.name,
						Description: slice.description,
						Unit:        slice.unit,
						Data:        metricdata.Gauge[int64]{DataPoints: rem[:take]},
					})
					batchPointCount += take
					rem = rem[take:]
					if batchPointCount == maxTimeSeriesPerBatch {
						flushBatch()
					}
				}
			}
			flushBatch()
		}
	}

	if len(batches) > 0 {
		if err := exportConcurrentBatches(ctx, e.entityExporter, batches, maxConcurrentExports); err != nil {
			recordErr(err)
		}
	}

	if len(exportErrors) > 0 {
		return errors.Join(exportErrors...)
	}
	return ctx.Err()
}
