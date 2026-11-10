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
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
)

const (
	benchNumTPUPods        = 4
	benchTreesPerPod       = 144
	benchPartitionsPerTree = 31
	benchTotalTrees        = benchNumTPUPods * benchTreesPerPod       // 576
	benchTotalPartitions   = benchTotalTrees * benchPartitionsPerTree // 17,856
)

// noopMetricExporter implements sdkmetric.Exporter with zero cost to isolate
// GoogleExporter transformation and chunking overhead in benchmarks.
type noopMetricExporter struct {
	delay time.Duration
}

func (e *noopMetricExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	if e.delay > 0 {
		select {
		case <-time.After(e.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (e *noopMetricExporter) Shutdown(ctx context.Context) error {
	return nil
}

func (e *noopMetricExporter) ForceFlush(ctx context.Context) error {
	return nil
}

func (e *noopMetricExporter) Temporality(kind sdkmetric.InstrumentKind) metricdata.Temporality {
	return metricdata.CumulativeTemporality
}

func (e *noopMetricExporter) Aggregation(kind sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.DefaultAggregationSelector(kind)
}

// buildScaleResourceMetrics pre-allocates a ResourceMetrics struct matching the scale:
// 4 TPU pods, 144 partition trees per pod (576 trees total), 31 partitions per tree,
// across both occupancy and taint metrics (17,856 data points each = 35,712 total points).
func buildScaleResourceMetrics() metricdata.ResourceMetrics {
	occPoints := make([]metricdata.DataPoint[int64], 0, benchTotalPartitions)
	taintPoints := make([]metricdata.DataPoint[int64], 0, benchTotalPartitions)

	for pod := 0; pod < benchNumTPUPods; pod++ {
		for tree := 0; tree < benchTreesPerPod; tree++ {
			treeName := fmt.Sprintf("pt-pod%d-tree%03d", pod, tree)
			treeUID := fmt.Sprintf("uid-pod%d-tree%03d", pod, tree)

			for part := 0; part < benchPartitionsPerTree; part++ {
				partID := fmt.Sprintf("p-%02d", part)

				occPoints = append(occPoints, metricdata.DataPoint[int64]{
					Value: 1,
					Attributes: attribute.NewSet(
						attribute.String(Attr_partition_tree_name, treeName),
						attribute.String(Attr_partition_tree_uid, treeUID),
						attribute.String(Attr_topology, "2x2x1"),
						attribute.String(Attr_partition_id, partID),
						attribute.String(Attr_state, string(partitionv1beta1.OccupancyOccupied)),
					),
				})

				taintPoints = append(taintPoints, metricdata.DataPoint[int64]{
					Value: 1,
					Attributes: attribute.NewSet(
						attribute.String(Attr_partition_tree_name, treeName),
						attribute.String(Attr_partition_tree_uid, treeUID),
						attribute.String(Attr_partition_id, partID),
						attribute.String(Attr_topology, "2x2x1"),
						attribute.String(Attr_taint_effect, string(partitionv1beta1.TaintEffectNoSchedule)),
						attribute.String(Attr_taint_key, "tpu.google.com/reserved"),
						attribute.String(Attr_taint_value, "true"),
					),
				})
			}
		}
	}

	return metricdata.ResourceMetrics{
		Resource: resource.NewSchemaless(
			attribute.String(Attr_gcp_resource_type, ResourceTypeGoogle),
			attribute.String(Attr_project_id, testMeta.ProjectID),
			attribute.String(Attr_location, testMeta.ClusterLocation),
			attribute.String(Attr_cluster_name, testMeta.ClusterName),
		),
		ScopeMetrics: []metricdata.ScopeMetrics{{
			Metrics: []metricdata.Metrics{
				{
					Name: MetricGooglePartitionTreeOccupancy,
					Data: metricdata.Gauge[int64]{DataPoints: occPoints},
				},
				{
					Name: MetricGooglePartitionTreeTaint,
					Data: metricdata.Gauge[int64]{DataPoints: taintPoints},
				},
			},
		}},
	}
}

// buildScaleTreeHierarchies constructs 576 PartitionInfo trees with 31 partitions each.
func buildScaleTreeHierarchies() []*partitionv1beta1.PartitionInfo {
	trees := make([]*partitionv1beta1.PartitionInfo, 0, benchTotalTrees)
	for i := 0; i < benchTotalTrees; i++ {
		root := &partitionv1beta1.PartitionInfo{
			PartitionID: fmt.Sprintf("root-tree-%04d", i),
			Occupancy:   partitionv1beta1.OccupancyOccupied,
			Topology:    "4x4x4",
			Taints: []partitionv1beta1.PartitionTaint{
				{Key: "tpu.google.com/reserved", Value: "true", Effect: partitionv1beta1.TaintEffectNoSchedule},
			},
			ChildPartitions: make([]partitionv1beta1.PartitionInfo, 30),
		}
		for c := 0; c < 30; c++ {
			root.ChildPartitions[c] = partitionv1beta1.PartitionInfo{
				PartitionID: fmt.Sprintf("child-%02d", c),
				Occupancy:   partitionv1beta1.OccupancyOccupied,
				Topology:    "2x2x1",
				Taints: []partitionv1beta1.PartitionTaint{
					{Key: "tpu.google.com/reserved", Value: "true", Effect: partitionv1beta1.TaintEffectNoSchedule},
				},
			}
		}
		trees = append(trees, root)
	}
	return trees
}

// BenchmarkGoogleExporter_Export_Scale benchmarks the transformation, grouping,
// attribute stripping, and chunking of 35,712 data points across 576 PartitionTrees
// with a realistic 20ms gRPC network RTT per Cloud Monitoring CreateTimeSeries call.
func BenchmarkGoogleExporter_Export_Scale(b *testing.B) {
	exporter := &GoogleExporter{
		entityExporter:    &noopMetricExporter{delay: 20 * time.Millisecond},
		containerExporter: &noopMetricExporter{delay: 20 * time.Millisecond},
		meta:              testMeta,
	}

	rm := buildScaleResourceMetrics()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := exporter.Export(ctx, &rm); err != nil {
			b.Fatalf("export failed: %v", err)
		}
	}
}

// BenchmarkRecordPartitionTreeMetrics_SyncAll_Scale benchmarks recording all 576 PartitionTrees
// (35,712 total partition metrics) from scratch into the gauge sets.
func BenchmarkRecordPartitionTreeMetrics_SyncAll_Scale(b *testing.B) {
	trees := buildScaleTreeHierarchies()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		b.StopTimer()
		ClearGaugeSets()
		now := time.Now()
		b.StartTimer()

		for i, root := range trees {
			podIdx := i / benchTreesPerPod
			treeIdx := i % benchTreesPerPod
			recordPartitionTreeMetricsAt(
				ctx,
				root,
				fmt.Sprintf("pt-pod%d-tree%03d", podIdx, treeIdx),
				fmt.Sprintf("uid-pod%d-tree%03d", podIdx, treeIdx),
				"test-pool",
				"test-mig",
				"tpu-v5p",
				fmt.Sprintf("tpu-pod-%d", podIdx),
				now,
			)
		}
	}
}

// BenchmarkRecordPartitionTreeMetrics_SingleTree_SteadyState benchmarks updating a single
// PartitionTree (31 partitions) when the gauge set already holds all 576 trees (steady state).
func BenchmarkRecordPartitionTreeMetrics_SingleTree_SteadyState(b *testing.B) {
	trees := buildScaleTreeHierarchies()
	ctx := context.Background()

	ClearGaugeSets()
	now := time.Now()
	for i, root := range trees {
		podIdx := i / benchTreesPerPod
		treeIdx := i % benchTreesPerPod
		recordPartitionTreeMetricsAt(
			ctx,
			root,
			fmt.Sprintf("pt-pod%d-tree%03d", podIdx, treeIdx),
			fmt.Sprintf("uid-pod%d-tree%03d", podIdx, treeIdx),
			"test-pool",
			"test-mig",
			"tpu-v5p",
			fmt.Sprintf("tpu-pod-%d", podIdx),
			now,
		)
	}

	targetTree := trees[0]
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		recordPartitionTreeMetricsAt(
			ctx,
			targetTree,
			"pt-pod0-tree000",
			"uid-pod0-tree000",
			"test-pool",
			"test-mig",
			"tpu-v5p",
			"tpu-pod-0",
			now,
		)
	}
}

// BenchmarkMetricsCollection_Scale benchmarks the OpenTelemetry Reader.Collect cycle
// reading 35,712 data points across the gauge sets under read locks.
func BenchmarkMetricsCollection_Scale(b *testing.B) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	shutdown, err := InitWithReaders(ctx, sdkmetric.NewManualReader(), reader, testMeta, 30*time.Minute, 100000, true, true)
	if err != nil {
		b.Fatalf("InitWithReaders failed: %v", err)
	}
	defer shutdown()

	trees := buildScaleTreeHierarchies()
	now := time.Now()
	for i, root := range trees {
		podIdx := i / benchTreesPerPod
		treeIdx := i % benchTreesPerPod
		recordPartitionTreeMetricsAt(
			ctx,
			root,
			fmt.Sprintf("pt-pod%d-tree%03d", podIdx, treeIdx),
			fmt.Sprintf("uid-pod%d-tree%03d", podIdx, treeIdx),
			"test-pool",
			"test-mig",
			"tpu-v5p",
			fmt.Sprintf("tpu-pod-%d", podIdx),
			now,
		)
	}

	var rm metricdata.ResourceMetrics
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := reader.Collect(ctx, &rm); err != nil {
			b.Fatalf("Collect failed: %v", err)
		}
	}
}

// BenchmarkEndToEndScrapeCycle_Scale benchmarks the complete periodic export cycle:
// SDK Collect (35,712 points) -> GoogleExporter.Export.
func BenchmarkEndToEndScrapeCycle_Scale(b *testing.B) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	shutdown, err := InitWithReaders(ctx, sdkmetric.NewManualReader(), reader, testMeta, 30*time.Minute, 100000, true, true)
	if err != nil {
		b.Fatalf("InitWithReaders failed: %v", err)
	}
	defer shutdown()

	trees := buildScaleTreeHierarchies()
	now := time.Now()
	for i, root := range trees {
		podIdx := i / benchTreesPerPod
		treeIdx := i % benchTreesPerPod
		recordPartitionTreeMetricsAt(
			ctx,
			root,
			fmt.Sprintf("pt-pod%d-tree%03d", podIdx, treeIdx),
			fmt.Sprintf("uid-pod%d-tree%03d", podIdx, treeIdx),
			"test-pool",
			"test-mig",
			"tpu-v5p",
			fmt.Sprintf("tpu-pod-%d", podIdx),
			now,
		)
	}

	exporter := &GoogleExporter{
		entityExporter:    &noopMetricExporter{delay: 20 * time.Millisecond},
		containerExporter: &noopMetricExporter{delay: 20 * time.Millisecond},
		meta:              testMeta,
	}

	var rm metricdata.ResourceMetrics
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := reader.Collect(ctx, &rm); err != nil {
			b.Fatalf("Collect failed: %v", err)
		}
		if err := exporter.Export(ctx, &rm); err != nil {
			b.Fatalf("Export failed: %v", err)
		}
	}
}

// BenchmarkConcurrentRecordAndCollect_Scale benchmarks concurrent controller reconciler updates
// and metric reader collection cycles under parallel goroutines to measure lock contention.
func BenchmarkConcurrentRecordAndCollect_Scale(b *testing.B) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	shutdown, err := InitWithReaders(ctx, sdkmetric.NewManualReader(), reader, testMeta, 30*time.Minute, 100000, true, true)
	if err != nil {
		b.Fatalf("InitWithReaders failed: %v", err)
	}
	defer shutdown()

	trees := buildScaleTreeHierarchies()
	now := time.Now()
	for i, root := range trees {
		podIdx := i / benchTreesPerPod
		treeIdx := i % benchTreesPerPod
		recordPartitionTreeMetricsAt(
			ctx,
			root,
			fmt.Sprintf("pt-pod%d-tree%03d", podIdx, treeIdx),
			fmt.Sprintf("uid-pod%d-tree%03d", podIdx, treeIdx),
			"test-pool",
			"test-mig",
			"tpu-v5p",
			fmt.Sprintf("tpu-pod-%d", podIdx),
			now,
		)
	}

	targetTree := trees[0]
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var rm metricdata.ResourceMetrics
		iter := 0
		for pb.Next() {
			iter++
			if iter%10 == 0 {
				_ = reader.Collect(ctx, &rm)
			} else {
				recordPartitionTreeMetricsAt(
					ctx,
					targetTree,
					"pt-pod0-tree000",
					"uid-pod0-tree000",
					"test-pool",
					"test-mig",
					"tpu-v5p",
					"tpu-pod-0",
					time.Now(),
				)
			}
		}
	})
}
