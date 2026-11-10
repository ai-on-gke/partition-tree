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
	"fmt"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"

	"github.com/ai-on-gke/partition-tree/internal/metadata"
)

type fakeExporter struct {
	mu       sync.Mutex
	exported []*metricdata.ResourceMetrics
}

func (*fakeExporter) Temporality(kind sdkmetric.InstrumentKind) metricdata.Temporality {
	return sdkmetric.DefaultTemporalitySelector(kind)
}

func (*fakeExporter) Aggregation(kind sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.DefaultAggregationSelector(kind)
}

func (*fakeExporter) ForceFlush(ctx context.Context) error { return nil }

func (*fakeExporter) Shutdown(ctx context.Context) error { return nil }

func (f *fakeExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.exported = append(f.exported, rm)
	return nil
}

func TestGoogleExporter(t *testing.T) {
	ctx := context.Background()
	fake := &fakeExporter{}
	meta := metadata.GoogleMetadata{
		ProjectID:       "test-project",
		ClusterLocation: "us-central1-a",
		ClusterName:     "test-cluster",
	}
	googleExporter := NewGoogleExporter(fake, fake, meta)

	dp1 := metricdata.DataPoint[int64]{
		Attributes: attribute.NewSet(
			attribute.String(Attr_partition_tree_name, "tree-1"),
			attribute.String(Attr_partition_tree_uid, "uid-1"),
			attribute.String(Attr_partition_id, "p-1"),
			attribute.String(Attr_state, "Occupied"),
		),
		Value: 1,
	}
	dp2 := metricdata.DataPoint[int64]{
		Attributes: attribute.NewSet(
			attribute.String(Attr_partition_tree_name, "tree-2"),
			attribute.String(Attr_partition_tree_uid, "uid-2"),
			attribute.String(Attr_partition_id, "p-2"),
			attribute.String(Attr_state, "Empty"),
		),
		Value: 1,
	}

	rm := gaugeRM(MetricGooglePartitionTreeOccupancy, dp1, dp2)
	if err := googleExporter.Export(ctx, &rm); err != nil {
		t.Fatalf("Export() error = %v", err)
	}

	if len(fake.exported) != 2 {
		t.Fatalf("expected 2 exported ResourceMetrics, got %d", len(fake.exported))
	}

	for _, erm := range fake.exported {
		resAttrs := resAttrsMap(erm.Resource)

		if resAttrs[Attr_gcp_resource_type] != ResourceTypeGoogle {
			t.Errorf("resource %s = %q, want %q", Attr_gcp_resource_type, resAttrs[Attr_gcp_resource_type], ResourceTypeGoogle)
		}
		if resAttrs[Attr_project_id] != "test-project" {
			t.Errorf("resource %s = %q, want %q", Attr_project_id, resAttrs[Attr_project_id], "test-project")
		}
		if resAttrs[Attr_location] != "us-central1-a" {
			t.Errorf("resource %s = %q, want %q", Attr_location, resAttrs[Attr_location], "us-central1-a")
		}
		if resAttrs[Attr_cluster_name] != "test-cluster" {
			t.Errorf("resource %s = %q, want %q", Attr_cluster_name, resAttrs[Attr_cluster_name], "test-cluster")
		}
		if resAttrs[Attr_entity_type] != EntityTypePartitionTree {
			t.Errorf("resource %s = %q, want %q", Attr_entity_type, resAttrs[Attr_entity_type], EntityTypePartitionTree)
		}

		treeName := resAttrs[Attr_entity_name]
		treeUID := resAttrs[Attr_entity_uid]
		if (treeName == "tree-1" && treeUID != "uid-1") || (treeName == "tree-2" && treeUID != "uid-2") {
			t.Errorf("mismatched entity %q / %q", treeName, treeUID)
		}

		// Verify that metric attributes do NOT contain partition_tree_name or partition_tree_uid
		points := erm.ScopeMetrics[0].Metrics[0].Data.(metricdata.Gauge[int64]).DataPoints
		if len(points) != 1 {
			t.Fatalf("expected 1 point per entity, got %d", len(points))
		}
		dpAttrs := attrsMap(points[0])
		if _, exists := dpAttrs[Attr_partition_tree_name]; exists {
			t.Errorf("metric attribute %q was not stripped", Attr_partition_tree_name)
		}
		if _, exists := dpAttrs[Attr_partition_tree_uid]; exists {
			t.Errorf("metric attribute %q was not stripped", Attr_partition_tree_uid)
		}
	}
}

func TestGoogleExporter_BuildInfo(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "partition-system")
	ctx := context.Background()
	fake := &fakeExporter{}
	meta := metadata.GoogleMetadata{
		ProjectID:       "test-project",
		ClusterLocation: "us-central1-a",
		ClusterName:     "test-cluster",
	}
	googleExporter := NewGoogleExporter(fake, fake, meta)

	buildDP := metricdata.DataPoint[int64]{
		Attributes: attribute.NewSet(
			attribute.String(Attr_build_date, "2026-09-10"),
			attribute.String(Attr_compiler, "gc"),
			attribute.String(Attr_git_commit, "abc1234"),
			attribute.String(Attr_git_version, "v0.2.0"),
			attribute.String(Attr_go_version, "go1.24"),
			attribute.String(Attr_platform, "linux/amd64"),
			attribute.String(Attr_pod_name, "partition-controller-test-pod"),
		),
		Value: 1,
	}

	rm := gaugeRM(MetricGooglePartitionTreeBuildInfo, buildDP)
	if err := googleExporter.Export(ctx, &rm); err != nil {
		t.Fatalf("Export() error = %v", err)
	}

	if len(fake.exported) != 1 {
		t.Fatalf("expected 1 exported ResourceMetrics for build_info, got %d", len(fake.exported))
	}

	resAttrs := resAttrsMap(fake.exported[0].Resource)
	wantRes := map[string]string{
		Attr_gcp_resource_type: ResourceTypeGoogleContainer,
		Attr_project_id:        "test-project",
		Attr_location:          "us-central1-a",
		Attr_cluster_name:      "test-cluster",
		Attr_namespace_name:    "partition-system",
		Attr_pod_name:          "partition-controller-test-pod",
		Attr_container_name:    ServiceName,
	}
	for k, want := range wantRes {
		if got := resAttrs[k]; got != want {
			t.Errorf("resource %s = %q, want %q", k, got, want)
		}
	}

	// Verify pod_name was stripped from metric data point attributes to prevent Cloud Monitoring descriptor collision
	exportedPoints := fake.exported[0].ScopeMetrics[0].Metrics[0].Data.(metricdata.Gauge[int64]).DataPoints
	if len(exportedPoints) != 1 {
		t.Fatalf("expected 1 point, got %d", len(exportedPoints))
	}
	exportedAttrs := attrsMap(exportedPoints[0])
	if _, exists := exportedAttrs[Attr_pod_name]; exists {
		t.Errorf("metric attribute %s should have been stripped, but was present", Attr_pod_name)
	}

	wantAttrs := map[string]string{
		Attr_git_version: "v0.2.0",
		Attr_git_commit:  "abc1234",
		Attr_build_date:  "2026-09-10",
		Attr_go_version:  "go1.24",
		Attr_compiler:    "gc",
		Attr_platform:    "linux/amd64",
	}
	for k, want := range wantAttrs {
		if got := exportedAttrs[k]; got != want {
			t.Errorf("metric attribute %s = %q, want %q", k, got, want)
		}
	}
}

func TestGoogleExporter_DualExporterRouting(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "partition-system")
	ctx := context.Background()
	fakeEntity := &fakeExporter{}
	fakeContainer := &fakeExporter{}
	meta := metadata.GoogleMetadata{
		ProjectID:       "test-project",
		ClusterLocation: "us-central1-a",
		ClusterName:     "test-cluster",
	}
	googleExporter := NewGoogleExporter(fakeEntity, fakeContainer, meta)

	occDP := metricdata.DataPoint[int64]{
		Attributes: attribute.NewSet(
			attribute.String(Attr_partition_tree_name, "tree-1"),
			attribute.String(Attr_partition_tree_uid, "uid-1"),
			attribute.String(Attr_partition_id, "p-1"),
			attribute.String(Attr_state, "Occupied"),
		),
		Value: 1,
	}
	buildDP := metricdata.DataPoint[int64]{
		Attributes: attribute.NewSet(
			attribute.String(Attr_git_version, "v0.2.0"),
			attribute.String(Attr_pod_name, "pod-abc"),
		),
		Value: 1,
	}

	rm := metricdata.ResourceMetrics{
		Resource: resource.Empty(),
		ScopeMetrics: []metricdata.ScopeMetrics{{
			Metrics: []metricdata.Metrics{
				{
					Name: MetricGooglePartitionTreeOccupancy,
					Data: metricdata.Gauge[int64]{DataPoints: []metricdata.DataPoint[int64]{occDP}},
				},
				{
					Name: MetricGooglePartitionTreeBuildInfo,
					Data: metricdata.Gauge[int64]{DataPoints: []metricdata.DataPoint[int64]{buildDP}},
				},
			},
		}},
	}

	if err := googleExporter.Export(ctx, &rm); err != nil {
		t.Fatalf("Export() error = %v", err)
	}

	if len(fakeEntity.exported) != 1 {
		t.Fatalf("expected 1 exported ResourceMetrics for fakeEntity, got %d", len(fakeEntity.exported))
	}
	if len(fakeContainer.exported) != 1 {
		t.Fatalf("expected 1 exported ResourceMetrics for fakeContainer, got %d", len(fakeContainer.exported))
	}

	entityMetricName := fakeEntity.exported[0].ScopeMetrics[0].Metrics[0].Name
	if entityMetricName != MetricGooglePartitionTreeOccupancy {
		t.Errorf("fakeEntity received metric %q, want %q", entityMetricName, MetricGooglePartitionTreeOccupancy)
	}
	containerMetricName := fakeContainer.exported[0].ScopeMetrics[0].Metrics[0].Name
	if containerMetricName != MetricGooglePartitionTreeBuildInfo {
		t.Errorf("fakeContainer received metric %q, want %q", containerMetricName, MetricGooglePartitionTreeBuildInfo)
	}

	entityResType, _ := fakeEntity.exported[0].Resource.Set().Value(Attr_gcp_resource_type)
	if entityResType.AsString() != ResourceTypeGoogle {
		t.Errorf("fakeEntity resource type = %q, want %q", entityResType.AsString(), ResourceTypeGoogle)
	}
	containerResType, _ := fakeContainer.exported[0].Resource.Set().Value(Attr_gcp_resource_type)
	if containerResType.AsString() != ResourceTypeGoogleContainer {
		t.Errorf("fakeContainer resource type = %q, want %q", containerResType.AsString(), ResourceTypeGoogleContainer)
	}
}

func TestGoogleExporter_BatchChunking(t *testing.T) {
	ctx := context.Background()
	fake := &fakeExporter{}
	meta := metadata.GoogleMetadata{
		ProjectID:       "test-project",
		ClusterLocation: "us-central1-a",
		ClusterName:     "test-cluster",
	}
	googleExporter := NewGoogleExporter(fake, nil, meta)

	// Create 250 data points for a single PartitionTree entity
	var points []metricdata.DataPoint[int64]
	for i := 0; i < 250; i++ {
		points = append(points, metricdata.DataPoint[int64]{
			Attributes: attribute.NewSet(
				attribute.String(Attr_partition_tree_name, "tree-large"),
				attribute.String(Attr_partition_tree_uid, "uid-large"),
				attribute.String(Attr_partition_id, fmt.Sprintf("p-%d", i)),
				attribute.String(Attr_state, "Occupied"),
			),
			Value: 1,
		})
	}

	rm := gaugeRM(MetricGooglePartitionTreeOccupancy, points...)

	if err := googleExporter.Export(ctx, &rm); err != nil {
		t.Fatalf("Export() error = %v", err)
	}

	if len(fake.exported) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(fake.exported))
	}

	// First chunk should have 200 points
	pts1 := fake.exported[0].ScopeMetrics[0].Metrics[0].Data.(metricdata.Gauge[int64]).DataPoints
	if len(pts1) != 200 {
		t.Errorf("chunk 0 points = %d, want 200", len(pts1))
	}
	// Second chunk should have 50 points
	pts2 := fake.exported[1].ScopeMetrics[0].Metrics[0].Data.(metricdata.Gauge[int64]).DataPoints
	if len(pts2) != 50 {
		t.Errorf("chunk 1 points = %d, want 50", len(pts2))
	}
}

func TestGoogleExporter_IntermingledMetrics(t *testing.T) {
	ctx := context.Background()
	fake := &fakeExporter{}
	meta := metadata.GoogleMetadata{
		ProjectID:       "test-project",
		ClusterLocation: "us-central1-a",
		ClusterName:     "test-cluster",
	}
	googleExporter := NewGoogleExporter(fake, nil, meta)

	// Single entity with 31 occupancy points, 1 info point, and 210 taint points = 242 total points
	var occPoints []metricdata.DataPoint[int64]
	for i := 0; i < 31; i++ {
		occPoints = append(occPoints, metricdata.DataPoint[int64]{
			Attributes: attribute.NewSet(
				attribute.String(Attr_partition_tree_name, "tree-mix"),
				attribute.String(Attr_partition_tree_uid, "uid-mix"),
				attribute.String(Attr_partition_id, fmt.Sprintf("p-%d", i)),
			),
			Value: 1,
		})
	}
	infoPoint := metricdata.DataPoint[int64]{
		Attributes: attribute.NewSet(
			attribute.String(Attr_partition_tree_name, "tree-mix"),
			attribute.String(Attr_partition_tree_uid, "uid-mix"),
			attribute.String(Attr_accelerator_type, "v4"),
		),
		Value: 1,
	}
	var taintPoints []metricdata.DataPoint[int64]
	for i := 0; i < 210; i++ {
		taintPoints = append(taintPoints, metricdata.DataPoint[int64]{
			Attributes: attribute.NewSet(
				attribute.String(Attr_partition_tree_name, "tree-mix"),
				attribute.String(Attr_partition_tree_uid, "uid-mix"),
				attribute.String(Attr_taint_key, fmt.Sprintf("k-%d", i)),
			),
			Value: 1,
		})
	}

	rm := metricdata.ResourceMetrics{
		ScopeMetrics: []metricdata.ScopeMetrics{{
			Metrics: []metricdata.Metrics{
				{
					Name: MetricGooglePartitionTreeOccupancy,
					Data: metricdata.Gauge[int64]{DataPoints: occPoints},
				},
				{
					Name: MetricGooglePartitionTreeInfo,
					Data: metricdata.Gauge[int64]{DataPoints: []metricdata.DataPoint[int64]{infoPoint}},
				},
				{
					Name: MetricGooglePartitionTreeTaint,
					Data: metricdata.Gauge[int64]{DataPoints: taintPoints},
				},
			},
		}},
	}

	if err := googleExporter.Export(ctx, &rm); err != nil {
		t.Fatalf("Export() error = %v", err)
	}

	// 242 points total -> Batch 1 has 200 points, Batch 2 has 42 points
	if len(fake.exported) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(fake.exported))
	}

	countPoints := func(rm *metricdata.ResourceMetrics) int {
		total := 0
		for _, m := range rm.ScopeMetrics[0].Metrics {
			total += len(m.Data.(metricdata.Gauge[int64]).DataPoints)
		}
		return total
	}

	b1Count := countPoints(fake.exported[0])
	b2Count := countPoints(fake.exported[1])
	if b1Count != 200 {
		t.Errorf("batch 1 point count = %d, want 200", b1Count)
	}
	if b2Count != 42 {
		t.Errorf("batch 2 point count = %d, want 42", b2Count)
	}

	// Verify Batch 1 contains intermingled metrics: occupancy (31), info (1), taint (168)
	b1Metrics := fake.exported[0].ScopeMetrics[0].Metrics
	if len(b1Metrics) != 3 {
		t.Fatalf("batch 1 expected 3 distinct metric types, got %d", len(b1Metrics))
	}
	if b1Metrics[0].Name != MetricGooglePartitionTreeOccupancy || len(b1Metrics[0].Data.(metricdata.Gauge[int64]).DataPoints) != 31 {
		t.Errorf("expected 31 occupancy points in batch 1")
	}
	if b1Metrics[1].Name != MetricGooglePartitionTreeInfo || len(b1Metrics[1].Data.(metricdata.Gauge[int64]).DataPoints) != 1 {
		t.Errorf("expected 1 info point in batch 1")
	}
	if b1Metrics[2].Name != MetricGooglePartitionTreeTaint || len(b1Metrics[2].Data.(metricdata.Gauge[int64]).DataPoints) != 168 {
		t.Errorf("expected 168 taint points in batch 1, got %d", len(b1Metrics[2].Data.(metricdata.Gauge[int64]).DataPoints))
	}

	// Verify Batch 2 contains remaining taints (42)
	b2Metrics := fake.exported[1].ScopeMetrics[0].Metrics
	if len(b2Metrics) != 1 || b2Metrics[0].Name != MetricGooglePartitionTreeTaint || len(b2Metrics[0].Data.(metricdata.Gauge[int64]).DataPoints) != 42 {
		t.Errorf("expected 42 taint points in batch 2")
	}

	// Verify both batches target the exact same k8s_entity resource
	for i, b := range fake.exported {
		resAttrs := resAttrsMap(b.Resource)
		if resAttrs[Attr_entity_name] != "tree-mix" || resAttrs[Attr_entity_uid] != "uid-mix" {
			t.Errorf("batch %d resource mismatch: %v", i, resAttrs)
		}
	}
}

func TestGoogleExporter_Lifecycle(t *testing.T) {
	ctx := context.Background()
	fake1 := &fakeExporter{}
	fake2 := &fakeExporter{}
	meta := metadata.GoogleMetadata{ProjectID: "test"}
	exporter := NewGoogleExporter(fake1, fake2, meta)

	_ = exporter.Temporality(sdkmetric.InstrumentKindGauge)
	_ = exporter.Aggregation(sdkmetric.InstrumentKindGauge)
	if err := exporter.ForceFlush(ctx); err != nil {
		t.Errorf("ForceFlush error = %v", err)
	}
	if err := exporter.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown error = %v", err)
	}

	// Test with identical underlying exporters (single exporter constructor branch)
	singleExporter := NewGoogleExporter(fake1, nil, meta)
	if err := singleExporter.ForceFlush(ctx); err != nil {
		t.Errorf("ForceFlush single error = %v", err)
	}
	if err := singleExporter.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown single error = %v", err)
	}

	// Nil export
	if err := exporter.Export(ctx, nil); err == nil {
		t.Error("Export(nil) want error, got nil")
	}

	// Canceled context
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	rm := metricdata.ResourceMetrics{
		ScopeMetrics: []metricdata.ScopeMetrics{{
			Metrics: []metricdata.Metrics{{
				Name: MetricGooglePartitionTreeOccupancy,
				Data: metricdata.Gauge[int64]{DataPoints: []metricdata.DataPoint[int64]{{
					Attributes: attribute.NewSet(
						attribute.String(Attr_partition_tree_name, "t"),
						attribute.String(Attr_partition_tree_uid, "u"),
					),
				}}},
			}},
		}},
	}
	if err := exporter.Export(canceledCtx, &rm); err == nil {
		t.Error("Export with canceled context want error, got nil")
	}
}
