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

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
	"github.com/ai-on-gke/partition-tree/internal/metadata"
)

var testMeta = metadata.GoogleMetadata{
	ProjectID:       "test-project",
	ClusterLocation: "us-central1-a",
	ClusterName:     "test-cluster",
}

const testCardinalityLimit = 100000

func TestRecordPartitionTreeMetrics_Lifecycle(t *testing.T) {
	ctx := context.Background()
	readerCustomer, readerGoogle := setupTestMeters(t)

	// 1. Test Active State: Emits 1 for both Occupancy and Taint gauges
	root := &partitionv1beta1.PartitionInfo{
		PartitionID: "test-id",
		Occupancy:   partitionv1beta1.OccupancyOccupied,
		Topology:    "2x2x1",
		Taints: []partitionv1beta1.PartitionTaint{
			{Key: "test-key", Effect: partitionv1beta1.TaintEffectNoSchedule},
		},
	}
	recordPartitionTreeMetricsAt(ctx, root, "test-tree", "test-uid", "test-pool", "test-mig", "tpu-v4", "pod-1", time.Now())

	var rmGoogle, rmCustomer metricdata.ResourceMetrics
	if err := readerGoogle.Collect(ctx, &rmGoogle); err != nil {
		t.Fatalf("failed to collect google metrics: %v", err)
	}
	if err := readerCustomer.Collect(ctx, &rmCustomer); err != nil {
		t.Fatalf("failed to collect customer metrics: %v", err)
	}

	assertDomainMetricAttrs := func(t *testing.T, metricName string, dps []metricdata.DataPoint[int64]) {
		t.Helper()
		if len(dps) == 0 {
			t.Fatalf("no data points for %s", metricName)
		}
		attrs := attrsMap(dps[0])
		if got := attrs[Attr_partition_tree_name]; got != "test-tree" {
			t.Errorf("%s partition_tree_name = %q, want %q", metricName, got, "test-tree")
		}
		if got := attrs[Attr_partition_tree_uid]; got != "test-uid" {
			t.Errorf("%s partition_tree_uid = %q, want %q", metricName, got, "test-uid")
		}
		if _, exists := attrs[Attr_entity_name]; exists {
			t.Errorf("%s has entity_name attribute, want it absent on raw metric", metricName)
		}
		if _, exists := attrs[Attr_entity_uid]; exists {
			t.Errorf("%s has entity_uid attribute, want it absent on raw metric", metricName)
		}
		if _, exists := attrs[Attr_entity_type]; exists {
			t.Errorf("%s has entity_type attribute, want it absent on raw metric", metricName)
		}
	}

	occDPs := getMetricPoints(rmGoogle, MetricGooglePartitionTreeOccupancy)
	if len(occDPs) != 1 || occDPs[0].Value != 1 {
		t.Fatalf("Collect(%q) data points = %v, want [{Value: 1}] (active occupancy)", MetricGooglePartitionTreeOccupancy, occDPs)
	}
	assertDomainMetricAttrs(t, "occupancy", occDPs)
	occAttrs := attrsMap(occDPs[0])
	if occAttrs[Attr_partition_id] != "test-id" || occAttrs[Attr_topology] != "2x2x1" || occAttrs[Attr_state] != "Occupied" {
		t.Errorf("unexpected occupancy attrs: %v", occAttrs)
	}

	custOccDPs := getMetricPoints(rmCustomer, MetricCustomerPartitionTreeOccupancy)
	if len(custOccDPs) != 1 || custOccDPs[0].Value != 1 {
		t.Fatalf("Collect(%q) data points = %v, want [{Value: 1}] (active customer occupancy)", MetricCustomerPartitionTreeOccupancy, custOccDPs)
	}
	assertDomainMetricAttrs(t, "customer occupancy", custOccDPs)

	taintDPs := getMetricPoints(rmGoogle, MetricGooglePartitionTreeTaint)
	if len(taintDPs) != 1 || taintDPs[0].Value != 1 {
		t.Fatalf("Collect(%q) data points = %v, want [{Value: 1}] (active taint)", MetricGooglePartitionTreeTaint, taintDPs)
	}
	assertDomainMetricAttrs(t, "taint", taintDPs)
	taintAttrs := attrsMap(taintDPs[0])
	if taintAttrs[Attr_partition_id] != "test-id" || taintAttrs[Attr_topology] != "2x2x1" || taintAttrs[Attr_taint_key] != "test-key" || taintAttrs[Attr_taint_effect] != "NoSchedule" {
		t.Errorf("unexpected taint attrs: %v", taintAttrs)
	}

	infoDPs := getMetricPoints(rmGoogle, MetricGooglePartitionTreeInfo)
	if len(infoDPs) != 1 || infoDPs[0].Value != 1 {
		t.Fatalf("Collect(%q) data points = %v, want [{Value: 1}] (active info)", MetricGooglePartitionTreeInfo, infoDPs)
	}
	assertDomainMetricAttrs(t, "info", infoDPs)
	infoAttrs := attrsMap(infoDPs[0])
	if infoAttrs[Attr_root_partition_id] != "test-id" || infoAttrs[Attr_node_pool_name] != "test-pool" || infoAttrs[Attr_mig_name] != "test-mig" || infoAttrs[Attr_accelerator_type] != "tpu-v4" || infoAttrs[Attr_tpu_pod_id] != "pod-1" || infoAttrs[Attr_topology] != "2x2x1" {
		t.Errorf("unexpected info attrs: %v", infoAttrs)
	}

	// 2. Test Tombstone State: Emits 0 for both when cleared (within 30 mins TTL)
	clearPartitionTreeMetricsAt(ctx, "test-tree", time.Now().Add(-5*time.Minute))
	if err := readerGoogle.Collect(ctx, &rmGoogle); err != nil {
		t.Fatalf("Collect() error = %v, want nil", err)
	}

	occDPs = getMetricPoints(rmGoogle, MetricGooglePartitionTreeOccupancy)
	if len(occDPs) != 1 || occDPs[0].Value != 0 {
		t.Errorf("Collect(%q) data points = %v, want [{Value: 0}] (tombstone occupancy)", MetricGooglePartitionTreeOccupancy, occDPs)
	}

	taintDPs = getMetricPoints(rmGoogle, MetricGooglePartitionTreeTaint)
	if len(taintDPs) != 1 || taintDPs[0].Value != 0 {
		t.Errorf("Collect(%q) data points = %v, want [{Value: 0}] (tombstone taint)", MetricGooglePartitionTreeTaint, taintDPs)
	}

	infoDPs = getMetricPoints(rmGoogle, MetricGooglePartitionTreeInfo)
	if len(infoDPs) != 1 || infoDPs[0].Value != 0 {
		t.Errorf("Collect(%q) data points = %v, want [{Value: 0}] (tombstone info)", MetricGooglePartitionTreeInfo, infoDPs)
	}

	// 3. Test Expiration State: Exceeds 30 mins TTL, both pruned from output AND from memory maps
	// Reset to active first so transition to absent captures the -31m timestamp
	recordPartitionTreeMetricsAt(ctx, root, "test-tree", "test-uid", "test-pool", "test-mig", "tpu-v4", "pod-1", time.Now().Add(-32*time.Minute))
	clearPartitionTreeMetricsAt(ctx, "test-tree", time.Now().Add(-31*time.Minute))
	if err := readerGoogle.Collect(ctx, &rmGoogle); err != nil {
		t.Fatalf("Collect() error = %v, want nil", err)
	}

	occDPs = getMetricPoints(rmGoogle, MetricGooglePartitionTreeOccupancy)
	if len(occDPs) != 0 {
		t.Errorf("Collect(%q) data points = %v, want [] (expired occupancy pruned)", MetricGooglePartitionTreeOccupancy, occDPs)
	}

	taintDPs = getMetricPoints(rmGoogle, MetricGooglePartitionTreeTaint)
	if len(taintDPs) != 0 {
		t.Errorf("Collect(%q) data points = %v, want [] (expired taint pruned)", MetricGooglePartitionTreeTaint, taintDPs)
	}

	infoDPs = getMetricPoints(rmGoogle, MetricGooglePartitionTreeInfo)
	if len(infoDPs) != 0 {
		t.Errorf("Collect(%q) data points = %v, want [] (expired info pruned)", MetricGooglePartitionTreeInfo, infoDPs)
	}

	// Verify in-memory gauge sets were actually pruned when only readerGoogle collected
	globalOccupancyGaugeSet.mu.Lock()
	if len(globalOccupancyGaugeSet.sets) != 0 {
		t.Errorf("globalOccupancyGaugeSet.sets = %v, want empty map after pruning", globalOccupancyGaugeSet.sets)
	}
	globalOccupancyGaugeSet.mu.Unlock()
}

func TestRecordPartitionTreeMetrics_ScopeIsolation(t *testing.T) {
	ctx := context.Background()
	_, readerGoogle := setupTestMeters(t)

	// 1. Mark both scope A and scope B active
	rootA := &partitionv1beta1.PartitionInfo{
		PartitionID: "A",
		Occupancy:   partitionv1beta1.OccupancyOccupied,
		Topology:    "2x2x1",
		Taints:      []partitionv1beta1.PartitionTaint{{Key: "key-A"}},
	}
	rootB := &partitionv1beta1.PartitionInfo{
		PartitionID: "B",
		Occupancy:   partitionv1beta1.OccupancyEmpty,
		Topology:    "2x2x1",
		Taints:      []partitionv1beta1.PartitionTaint{{Key: "key-B"}},
	}

	recordPartitionTreeMetricsAt(ctx, rootA, "test-tree-a", "test-uid-a", "test-pool", "test-mig", "tpu-v4", "pod-A", time.Now())
	recordPartitionTreeMetricsAt(ctx, rootB, "test-tree-b", "test-uid-b", "test-pool", "test-mig", "tpu-v4", "pod-B", time.Now())

	// 2. Clear scope A, while scope B remains untouched
	clearPartitionTreeMetricsAt(ctx, "test-tree-a", time.Now())

	var rm metricdata.ResourceMetrics
	if err := readerGoogle.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	// Verify occupancy isolation
	for _, dp := range getMetricPoints(rm, MetricGooglePartitionTreeOccupancy) {
		switch attrsMap(dp)[Attr_partition_id] {
		case "A":
			if dp.Value != 0 {
				t.Errorf("partition A occupancy = %d, want 0 (tombstoned)", dp.Value)
			}
		case "B":
			if dp.Value != 1 {
				t.Errorf("partition B occupancy = %d, want 1 (active)", dp.Value)
			}
		}
	}

	// Verify taint isolation
	for _, dp := range getMetricPoints(rm, MetricGooglePartitionTreeTaint) {
		switch attrsMap(dp)[Attr_partition_id] {
		case "A":
			if dp.Value != 0 {
				t.Errorf("partition A taint = %d, want 0 (tombstoned)", dp.Value)
			}
		case "B":
			if dp.Value != 1 {
				t.Errorf("partition B taint = %d, want 1 (active)", dp.Value)
			}
		}
	}

	// Verify info isolation
	for _, dp := range getMetricPoints(rm, MetricGooglePartitionTreeInfo) {
		switch attrsMap(dp)[Attr_partition_tree_name] {
		case "test-tree-a":
			if dp.Value != 0 {
				t.Errorf("tree A info = %d, want 0 (tombstoned)", dp.Value)
			}
		case "test-tree-b":
			if dp.Value != 1 {
				t.Errorf("tree B info = %d, want 1 (active)", dp.Value)
			}
		}
	}
}

func TestBuildInfoMetric(t *testing.T) {
	t.Setenv("POD_NAME", "partition-controller-pod-xyz")
	ctx := context.Background()
	readerCustomer, readerGoogle := setupTestMeters(t)

	var rmGoogle, rmCustomer metricdata.ResourceMetrics
	if err := readerGoogle.Collect(ctx, &rmGoogle); err != nil {
		t.Fatalf("failed to collect google metrics: %v", err)
	}
	if err := readerCustomer.Collect(ctx, &rmCustomer); err != nil {
		t.Fatalf("failed to collect customer metrics: %v", err)
	}

	assertBuildInfoPoints := func(t *testing.T, metricName string, dps []metricdata.DataPoint[int64]) {
		t.Helper()
		if len(dps) != 1 {
			t.Fatalf("expected 1 data point for %s, got %d", metricName, len(dps))
		}
		if dps[0].Value != 1 {
			t.Errorf("%s value = %d, want 1", metricName, dps[0].Value)
		}

		attrs := attrsMap(dps[0])
		requiredKeys := []string{
			Attr_build_date,
			Attr_compiler,
			Attr_git_commit,
			Attr_git_version,
			Attr_go_version,
			Attr_platform,
			Attr_pod_name,
		}
		for _, k := range requiredKeys {
			if _, ok := attrs[k]; !ok {
				t.Errorf("%s missing required attribute %q", metricName, k)
			}
		}

		if got := attrs[Attr_pod_name]; got != "partition-controller-pod-xyz" {
			t.Errorf("%s pod_name = %q, want %q", metricName, got, "partition-controller-pod-xyz")
		}

		// Ensure no CR-level dimensions leaked onto build_info
		unwantedKeys := []string{
			Attr_partition_tree_name,
			Attr_partition_tree_uid,
			Attr_partition_id,
			Attr_state,
			Attr_instance,
		}
		for _, k := range unwantedKeys {
			if _, exists := attrs[k]; exists {
				t.Errorf("%s contains unwanted attribute %q", metricName, k)
			}
		}
	}

	assertBuildInfoPoints(t, "google build_info", getMetricPoints(rmGoogle, MetricGooglePartitionTreeBuildInfo))
	assertBuildInfoPoints(t, "customer build_info", getMetricPoints(rmCustomer, MetricCustomerPartitionTreeBuildInfo))

	// Verify customer resource labels has StaticInstance
	custResAttrs := resAttrsMap(rmCustomer.Resource)
	if got := custResAttrs[Attr_instance]; got != StaticInstance {
		t.Errorf("customer resource %s = %q, want %q", Attr_instance, got, StaticInstance)
	}
	if got := custResAttrs[Attr_job]; got != ServiceName {
		t.Errorf("customer resource %s = %q, want %q", Attr_job, got, ServiceName)
	}
}

func TestCardinalityLimitOption(t *testing.T) {
	ctx := context.Background()

	shutdown, err := InitWithReaders(ctx, sdkmetric.NewManualReader(), sdkmetric.NewManualReader(), testMeta, 30*time.Minute, 0, true, true)
	if err != nil {
		t.Fatalf("InitWithReaders with 0 cardinality limit failed: %v", err)
	}
	shutdown()

	shutdown, err = InitWithReaders(ctx, sdkmetric.NewManualReader(), sdkmetric.NewManualReader(), testMeta, 30*time.Minute, 500, true, true)
	if err != nil {
		t.Fatalf("InitWithReaders with custom cardinality limit failed: %v", err)
	}
	shutdown()
}

func TestRecordPartitionTreeMetrics_HierarchicalTree(t *testing.T) {
	ctx := context.Background()
	_, readerGoogle := setupTestMeters(t)

	root := &partitionv1beta1.PartitionInfo{
		PartitionID: "root-tree",
		Occupancy:   partitionv1beta1.OccupancyOccupied,
		Topology:    "2x2x2",
		Taints: []partitionv1beta1.PartitionTaint{
			{Key: "taint-1", Value: "val-1", Effect: partitionv1beta1.TaintEffectNoSchedule},
			{Key: "taint-2", Value: "val-2", Effect: partitionv1beta1.TaintEffectPreferNoSchedule},
		},
		ChildPartitions: []partitionv1beta1.PartitionInfo{
			{
				PartitionID: "child-node-1",
				Occupancy:   partitionv1beta1.OccupancyOccupied,
				Topology:    "2x2x1",
			},
			{
				PartitionID: "child-node-2",
				Occupancy:   partitionv1beta1.OccupancyEmpty,
				Topology:    "2x2x1",
				Taints: []partitionv1beta1.PartitionTaint{
					{Key: "drain-in-progress", Value: "true", Effect: partitionv1beta1.TaintEffectNoExecute},
				},
			},
		},
	}

	recordPartitionTreeMetricsAt(ctx, root, "complex-tree", "complex-uid", "pool-1", "mig-1", "tpu-v5p", "pod-slice-0", time.Now())

	var rmGoogle metricdata.ResourceMetrics
	if err := readerGoogle.Collect(ctx, &rmGoogle); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}

	// Should have 3 occupancy points: root + 2 children
	occPoints := getMetricPoints(rmGoogle, MetricGooglePartitionTreeOccupancy)
	if len(occPoints) != 3 {
		t.Fatalf("expected 3 occupancy points, got %d", len(occPoints))
	}
	partitionsFound := make(map[string]string)
	for _, dp := range occPoints {
		attrs := attrsMap(dp)
		partitionsFound[attrs[Attr_partition_id]] = attrs[Attr_state]
	}
	if partitionsFound["root-tree"] != string(partitionv1beta1.OccupancyOccupied) {
		t.Errorf("root state = %q, want Occupied", partitionsFound["root-tree"])
	}
	if partitionsFound["child-node-1"] != string(partitionv1beta1.OccupancyOccupied) {
		t.Errorf("child 1 state = %q, want Occupied", partitionsFound["child-node-1"])
	}
	if partitionsFound["child-node-2"] != string(partitionv1beta1.OccupancyEmpty) {
		t.Errorf("child 2 state = %q, want Empty", partitionsFound["child-node-2"])
	}

	// Should have 3 taint points: 2 on root + 1 on child 2
	taintPoints := getMetricPoints(rmGoogle, MetricGooglePartitionTreeTaint)
	if len(taintPoints) != 3 {
		t.Fatalf("expected 3 taint points, got %d", len(taintPoints))
	}
	taintsFound := make(map[string]string)
	for _, dp := range taintPoints {
		attrs := attrsMap(dp)
		taintsFound[attrs[Attr_taint_key]] = attrs[Attr_taint_value]
	}
	if taintsFound["taint-1"] != "val-1" || taintsFound["taint-2"] != "val-2" || taintsFound["drain-in-progress"] != "true" {
		t.Errorf("unexpected taints found: %v", taintsFound)
	}

	// Should have 1 info point with root partition metadata
	infoPoints := getMetricPoints(rmGoogle, MetricGooglePartitionTreeInfo)
	if len(infoPoints) != 1 {
		t.Fatalf("expected 1 info point, got %d", len(infoPoints))
	}
}

func TestInit_Mock(t *testing.T) {
	ctx := context.Background()
	mockMeta := metadata.GoogleMetadata{
		ProjectID:       "mock",
		ClusterLocation: "us-central1-a",
		ClusterName:     "test-cluster",
	}

	t.Run("without direct push", func(t *testing.T) {
		shutdown, err := Init(ctx, "", false, mockMeta, 30*time.Minute, 100000, true, true)
		if err != nil {
			t.Fatalf("Init without direct push failed: %v", err)
		}
		shutdown()
	})

	t.Run("with direct push", func(t *testing.T) {
		shutdown, err := Init(ctx, "", true, mockMeta, 30*time.Minute, 100000, true, true)
		if err != nil {
			t.Fatalf("Init with direct push failed: %v", err)
		}
		shutdown()
	})
}

func TestNewCustomerReader(t *testing.T) {
	ctx := context.Background()
	mockMeta := metadata.GoogleMetadata{
		ProjectID:       "mock",
		ClusterLocation: "us-central1-a",
		ClusterName:     "test-cluster",
	}

	t.Run("disabled customer metrics", func(t *testing.T) {
		r, err := newCustomerReader(ctx, "localhost:4317", false, mockMeta, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := r.(*sdkmetric.ManualReader); !ok {
			t.Errorf("expected ManualReader, got %T", r)
		}
	})

	t.Run("direct push with both directPush and endpoint errors", func(t *testing.T) {
		_, err := newCustomerReader(ctx, "localhost:4317", true, mockMeta, true)
		if err == nil {
			t.Fatal("expected error when both directPush and endpoint are specified, got nil")
		}
	})

	t.Run("direct push with mock project returns ManualReader", func(t *testing.T) {
		r, err := newCustomerReader(ctx, "", true, mockMeta, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := r.(*sdkmetric.ManualReader); !ok {
			t.Errorf("expected ManualReader for mock project, got %T", r)
		}
	})

	t.Run("direct push with empty project returns ManualReader", func(t *testing.T) {
		r, err := newCustomerReader(ctx, "", true, metadata.GoogleMetadata{}, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := r.(*sdkmetric.ManualReader); !ok {
			t.Errorf("expected ManualReader for empty project, got %T", r)
		}
	})

	t.Run("empty endpoint without direct push returns ManualReader", func(t *testing.T) {
		r, err := newCustomerReader(ctx, "", false, mockMeta, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := r.(*sdkmetric.ManualReader); !ok {
			t.Errorf("expected ManualReader for empty endpoint, got %T", r)
		}
	})

	t.Run("valid endpoint creates periodic reader", func(t *testing.T) {
		r, err := newCustomerReader(ctx, "localhost:4317", false, mockMeta, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if r == nil {
			t.Fatal("expected reader, got nil")
		}
		_ = r.Shutdown(ctx)
	})
}

func TestTopLevelRecordAndClear(t *testing.T) {
	ctx := context.Background()
	ClearGaugeSets()
	root := &partitionv1beta1.PartitionInfo{
		PartitionID: "root-top",
		Occupancy:   partitionv1beta1.OccupancyOccupied,
		Topology:    "2x2x1",
	}
	RecordPartitionTreeMetrics(ctx, root, "top-tree", "top-uid", "pool", "mig", "v4", "pod")
	RecordPartitionTreeMetrics(ctx, nil, "top-tree", "top-uid", "pool", "mig", "v4", "pod")
	ClearPartitionTreeMetrics(ctx, "top-tree")
}

func TestRecordPartitionTreeMetrics_TaintValueChangeTombstoning(t *testing.T) {
	ctx := context.Background()
	_, readerGoogle := setupTestMeters(t)

	now := time.Now()
	// Step 1: Record taint with value="1"
	root1 := &partitionv1beta1.PartitionInfo{
		PartitionID: "p-1",
		Occupancy:   partitionv1beta1.OccupancyOccupied,
		Topology:    "2x2x1",
		Taints: []partitionv1beta1.PartitionTaint{
			{Key: "unhealthy-chips", Value: "1", Effect: partitionv1beta1.TaintEffectNoSchedule},
		},
	}
	recordPartitionTreeMetricsAt(ctx, root1, "taint-tree", "uid-1", "pool", "mig", "v4", "pod", now)

	// Step 2: Update taint to value="2" (same key, new value)
	root2 := &partitionv1beta1.PartitionInfo{
		PartitionID: "p-1",
		Occupancy:   partitionv1beta1.OccupancyOccupied,
		Topology:    "2x2x1",
		Taints: []partitionv1beta1.PartitionTaint{
			{Key: "unhealthy-chips", Value: "2", Effect: partitionv1beta1.TaintEffectNoSchedule},
		},
	}
	recordPartitionTreeMetricsAt(ctx, root2, "taint-tree", "uid-1", "pool", "mig", "v4", "pod", now.Add(time.Minute))

	var rm metricdata.ResourceMetrics
	if err := readerGoogle.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	taintDPs := getMetricPoints(rm, MetricGooglePartitionTreeTaint)
	if len(taintDPs) != 2 {
		t.Fatalf("expected 2 taint points (1 active for value=2, 1 tombstone for value=1), got %d", len(taintDPs))
	}

	valToMetric := make(map[string]int64)
	for _, dp := range taintDPs {
		attrs := attrsMap(dp)
		valToMetric[attrs[Attr_taint_value]] = dp.Value
	}
	if valToMetric["1"] != 0 {
		t.Errorf("taint value=1 metric = %d, want 0 (tombstoned)", valToMetric["1"])
	}
	if valToMetric["2"] != 1 {
		t.Errorf("taint value=2 metric = %d, want 1 (active)", valToMetric["2"])
	}
}

func TestRecordPartitionTreeMetrics_LabelChangesTombstoneOldSeries(t *testing.T) {
	ctx := context.Background()
	readerCustomer, readerGoogle := setupTestMeters(t)

	root := &partitionv1beta1.PartitionInfo{
		PartitionID: "root-p1",
		Occupancy:   partitionv1beta1.OccupancyEmpty,
		Topology:    "4x4x4",
		Taints: []partitionv1beta1.PartitionTaint{
			{Key: "cloud.google.com/partition-state", Value: "Unhealthy", Effect: partitionv1beta1.TaintEffectNoExecute},
		},
	}

	// Step 1: Record initial state (mig-v1), then simulate GKE MIG recreation (mig-v1 -> mig-v2) within the 30m TTL.
	now := time.Now()
	recordPartitionTreeMetricsAt(ctx, root, "tree-1", "uid-v1", "pool-v1", "mig-v1", "tpu7x", "pod-0", now.Add(-6*time.Minute))
	recordPartitionTreeMetricsAt(ctx, root, "tree-1", "uid-v1", "pool-v1", "mig-v2", "tpu7x", "pod-0", now.Add(-5*time.Minute))

	var rmCustomer, rmGoogle metricdata.ResourceMetrics
	if err := readerCustomer.Collect(ctx, &rmCustomer); err != nil {
		t.Fatalf("readerCustomer.Collect failed: %v", err)
	}
	if err := readerGoogle.Collect(ctx, &rmGoogle); err != nil {
		t.Fatalf("readerGoogle.Collect failed: %v", err)
	}

	assertInfoByMIG := func(stream string, dps []metricdata.DataPoint[int64], want map[string]int64) {
		t.Helper()
		if len(dps) != len(want) {
			t.Fatalf("%s info points = %d, want %d (%v)", stream, len(dps), len(want), dps)
		}
		got := make(map[string]int64, len(dps))
		for _, dp := range dps {
			got[attrsMap(dp)[Attr_mig_name]] = dp.Value
		}
		for mig, wantVal := range want {
			if gotVal, ok := got[mig]; !ok || gotVal != wantVal {
				t.Errorf("%s info[mig_name=%q] = (%d, %v), want (%d, true); full=%v", stream, mig, gotVal, ok, wantVal, got)
			}
		}
	}

	assertInfoByMIG("customer", getMetricPoints(rmCustomer, MetricCustomerPartitionTreeInfo), map[string]int64{
		"mig-v1": 0,
		"mig-v2": 1,
	})
	assertInfoByMIG("google", getMetricPoints(rmGoogle, MetricGooglePartitionTreeInfo), map[string]int64{
		"mig-v1": 0,
		"mig-v2": 1,
	})

	// Step 3: Simulate CR deletion + recreation with a new UID (uid-v1 -> uid-v2) and new pool (pool-v2).
	// Verify info, occupancy, and taint all tombstone uid-v1 while activating uid-v2.
	recordPartitionTreeMetricsAt(ctx, root, "tree-1", "uid-v2", "pool-v2", "mig-v3", "tpu7x", "pod-0", now.Add(-2*time.Minute))

	if err := readerCustomer.Collect(ctx, &rmCustomer); err != nil {
		t.Fatalf("readerCustomer.Collect failed: %v", err)
	}
	if err := readerGoogle.Collect(ctx, &rmGoogle); err != nil {
		t.Fatalf("readerGoogle.Collect failed: %v", err)
	}

	assertInfoByMIG("customer", getMetricPoints(rmCustomer, MetricCustomerPartitionTreeInfo), map[string]int64{
		"mig-v1": 0,
		"mig-v2": 0,
		"mig-v3": 1,
	})

	occByUID := make(map[string]int64)
	for _, dp := range getMetricPoints(rmCustomer, MetricCustomerPartitionTreeOccupancy) {
		occByUID[attrsMap(dp)[Attr_partition_tree_uid]] = dp.Value
	}
	if occByUID["uid-v1"] != 0 || occByUID["uid-v2"] != 1 {
		t.Errorf("customer occupancy by uid = %v, want uid-v1=0, uid-v2=1", occByUID)
	}

	taintByUID := make(map[string]int64)
	for _, dp := range getMetricPoints(rmCustomer, MetricCustomerPartitionTreeTaint) {
		taintByUID[attrsMap(dp)[Attr_partition_tree_uid]] = dp.Value
	}
	if taintByUID["uid-v1"] != 0 || taintByUID["uid-v2"] != 1 {
		t.Errorf("customer taint by uid = %v, want uid-v1=0, uid-v2=1", taintByUID)
	}

	// Step 4: Age tombstoned series past DefaultTombstoneTTL (30m) and verify pruning.
	ClearGaugeSets()
	recordPartitionTreeMetricsAt(ctx, root, "tree-1", "uid-v1", "pool-v1", "mig-v1", "tpu7x", "pod-0", now.Add(-32*time.Minute))
	recordPartitionTreeMetricsAt(ctx, root, "tree-1", "uid-v1", "pool-v1", "mig-v2", "tpu7x", "pod-0", now.Add(-31*time.Minute))

	if err := readerCustomer.Collect(ctx, &rmCustomer); err != nil {
		t.Fatalf("readerCustomer.Collect after TTL failed: %v", err)
	}
	if err := readerGoogle.Collect(ctx, &rmGoogle); err != nil {
		t.Fatalf("readerGoogle.Collect after TTL failed: %v", err)
	}

	assertInfoByMIG("customer post-TTL", getMetricPoints(rmCustomer, MetricCustomerPartitionTreeInfo), map[string]int64{
		"mig-v2": 1,
	})
	assertInfoByMIG("google post-TTL", getMetricPoints(rmGoogle, MetricGooglePartitionTreeInfo), map[string]int64{
		"mig-v2": 1,
	})
}

func TestGetPodNameAndNamespace(t *testing.T) {
	t.Setenv("POD_NAME", "custom-pod")
	if got := getPodName(); got != "custom-pod" {
		t.Errorf("getPodName() = %q, want custom-pod", got)
	}

	t.Setenv("POD_NAMESPACE", "custom-ns")
	ns, err := readNamespace()
	if err != nil || ns != "custom-ns" {
		t.Errorf("readNamespace() = %q, %v, want custom-ns, nil", ns, err)
	}
}

func TestFormatCustomerMetricName(t *testing.T) {
	tests := []struct {
		input string
		data  metricdata.Aggregation
		want  string
	}{
		{
			input: "prometheus.googleapis.com/partition_tree/occupancy",
			want:  "prometheus.googleapis.com/partition_tree/occupancy/gauge",
		},
		{
			input: "prometheus.googleapis.com/partition_tree/taint",
			want:  "prometheus.googleapis.com/partition_tree/taint/gauge",
		},
		{
			input: "prometheus.googleapis.com/partition_tree/info",
			want:  "prometheus.googleapis.com/partition_tree/info/gauge",
		},
		{
			input: "prometheus.googleapis.com/partition_tree/build_info",
			want:  "prometheus.googleapis.com/partition_tree/build_info/gauge",
		},
		{
			input: "prometheus.googleapis.com/partition_tree/occupancy/gauge",
			want:  "prometheus.googleapis.com/partition_tree/occupancy/gauge",
		},
		{
			input: "prometheus.googleapis.com/partition_tree/metrics_production_duration_seconds",
			data:  metricdata.Histogram[float64]{},
			want:  "prometheus.googleapis.com/partition_tree/metrics_production_duration_seconds/histogram",
		},
		{
			input: "prometheus.googleapis.com/partition_tree/metrics_production_duration_seconds/histogram",
			data:  metricdata.Histogram[float64]{},
			want:  "prometheus.googleapis.com/partition_tree/metrics_production_duration_seconds/histogram",
		},
	}

	for _, tc := range tests {
		m := metricdata.Metrics{Name: tc.input, Data: tc.data}
		if got := formatCustomerMetricName(m); got != tc.want {
			t.Errorf("formatCustomerMetricName(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestMetricsProductionDuration_CustomerOnly(t *testing.T) {
	ctx := context.Background()
	readerCustomer, readerGoogle := setupTestMeters(t)

	root := &partitionv1beta1.PartitionInfo{
		PartitionID: "prod-test-p1",
		Occupancy:   partitionv1beta1.OccupancyOccupied,
		Topology:    "2x2x1",
		Taints: []partitionv1beta1.PartitionTaint{
			{Key: "test-key", Value: "val", Effect: partitionv1beta1.TaintEffectNoSchedule},
		},
	}
	recordPartitionTreeMetricsAt(ctx, root, "prod-tree", "uid-123", "pool-1", "mig-1", "tpu-v4", "pod-1", time.Now())

	// 1. Collect Google metrics first (triggers Google callback observation pass)
	var rmGoogle metricdata.ResourceMetrics
	if err := readerGoogle.Collect(ctx, &rmGoogle); err != nil {
		t.Fatalf("readerGoogle.Collect failed: %v", err)
	}

	// Verify Google stream DOES NOT emit the production duration metric
	for _, sm := range rmGoogle.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == MetricCustomerPartitionTreeMetricsProductionDuration || m.Name == InstrumentMetricsProductionDuration {
				t.Errorf("Google ResourceMetrics unexpectedly contains customer-only metric %q", m.Name)
			}
		}
	}

	// 2. Collect Customer metrics (triggers Customer callback observation pass and exports accumulated histogram points)
	var rmCustomer metricdata.ResourceMetrics
	if err := readerCustomer.Collect(ctx, &rmCustomer); err != nil {
		t.Fatalf("readerCustomer.Collect failed: %v", err)
	}

	histDPs := getHistogramPoints(rmCustomer, MetricCustomerPartitionTreeMetricsProductionDuration)
	if len(histDPs) == 0 {
		t.Fatalf("expected %q histogram points on customer stream, got none", MetricCustomerPartitionTreeMetricsProductionDuration)
	}
	if len(histDPs[0].Bounds) != len(DefaultProductionDurationBuckets) ||
		histDPs[0].Bounds[0] != 0.1 ||
		histDPs[0].Bounds[len(histDPs[0].Bounds)-1] != 120.0 {
		t.Errorf("histogram bounds = %v, want %v (0.1s to 120s)", histDPs[0].Bounds, DefaultProductionDurationBuckets)
	}

	findPoint := func(stream, phase, status string) *metricdata.HistogramDataPoint[float64] {
		for i := range histDPs {
			attrs := histAttrsMap(histDPs[i])
			if attrs[Attr_stream] == stream && attrs[Attr_phase] == phase && attrs[Attr_status] == status {
				return &histDPs[i]
			}
		}
		return nil
	}

	// Verify observe phase was recorded for both streams
	if dp := findPoint(StreamGoogle, PhaseObserve, StatusSuccess); dp == nil || dp.Count != 1 {
		t.Errorf("expected count=1 for stream=%s phase=%s status=%s, got %+v", StreamGoogle, PhaseObserve, StatusSuccess, dp)
	}
	if dp := findPoint(StreamCustomer, PhaseObserve, StatusSuccess); dp == nil || dp.Count != 1 {
		t.Errorf("expected count=1 for stream=%s phase=%s status=%s, got %+v", StreamCustomer, PhaseObserve, StatusSuccess, dp)
	}

	// 3. Test Export & Total phases using WrapExporter
	mockExp := &fakeExporter{}
	wrappedGoogleExp := WrapExporter(mockExp, StreamGoogle)

	// Simulate Google cycle: Collect (sets start time) -> Export (records export and total)
	if err := readerGoogle.Collect(ctx, &rmGoogle); err != nil {
		t.Fatalf("readerGoogle.Collect failed: %v", err)
	}
	if err := wrappedGoogleExp.Export(ctx, &rmGoogle); err != nil {
		t.Fatalf("wrappedGoogleExp.Export failed: %v", err)
	}

	// Collect customer metrics to inspect newly recorded export and total durations
	if err := readerCustomer.Collect(ctx, &rmCustomer); err != nil {
		t.Fatalf("readerCustomer.Collect failed: %v", err)
	}
	histDPs = getHistogramPoints(rmCustomer, MetricCustomerPartitionTreeMetricsProductionDuration)

	if dp := findPoint(StreamGoogle, PhaseExport, StatusSuccess); dp == nil || dp.Count != 1 {
		t.Errorf("expected count=1 for stream=%s phase=%s status=%s, got %+v", StreamGoogle, PhaseExport, StatusSuccess, dp)
	}
	if dp := findPoint(StreamGoogle, PhaseTotal, StatusSuccess); dp == nil || dp.Count != 1 {
		t.Errorf("expected count=1 for stream=%s phase=%s status=%s, got %+v", StreamGoogle, PhaseTotal, StatusSuccess, dp)
	}
}

func TestStartTombstoneSweeper_Integration(t *testing.T) {
	ctx := context.Background()
	ClearGaugeSets()

	now := time.Now()
	root := &partitionv1beta1.PartitionInfo{
		PartitionID: "sweeper-p1",
		Occupancy:   partitionv1beta1.OccupancyOccupied,
		Topology:    "2x2x1",
		Taints: []partitionv1beta1.PartitionTaint{
			{Key: "test-taint", Value: "val", Effect: partitionv1beta1.TaintEffectNoSchedule},
		},
	}

	// 1. Record metrics and tombstone them with timestamps older than 30m TTL
	recordPartitionTreeMetricsAt(ctx, root, "sweeper-tree", "uid-1", "pool", "mig", "v4", "pod", now.Add(-35*time.Minute))
	clearPartitionTreeMetricsAt(ctx, "sweeper-tree", now.Add(-31*time.Minute))

	// Verify entries exist in memory as absent tombstones
	globalOccupancyGaugeSet.mu.Lock()
	if len(globalOccupancyGaugeSet.sets) == 0 {
		t.Fatalf("expected entries in globalOccupancyGaugeSet.sets before sweep")
	}
	// Simulate last callback being 20 minutes ago (> 15m threshold)
	globalOccupancyGaugeSet.lastCallback = now.Add(-20 * time.Minute)
	globalTaintGaugeSet.lastCallback = now.Add(-20 * time.Minute)
	globalInfoGaugeSet.lastCallback = now.Add(-20 * time.Minute)
	globalOccupancyGaugeSet.mu.Unlock()

	// 2. Start sweeper with 10ms check interval and 15ms stale threshold
	sweepCtx, sweepCancel := context.WithCancel(context.Background())
	defer sweepCancel()
	go startTombstoneSweeper(sweepCtx, 10*time.Millisecond, 15*time.Millisecond)

	// 3. Wait for background sweeper to run and verify zero-memory state without any reader Collect()
	assertEmpty := func() bool {
		globalOccupancyGaugeSet.mu.Lock()
		defer globalOccupancyGaugeSet.mu.Unlock()
		globalTaintGaugeSet.mu.Lock()
		defer globalTaintGaugeSet.mu.Unlock()
		globalInfoGaugeSet.mu.Lock()
		defer globalInfoGaugeSet.mu.Unlock()

		return len(globalOccupancyGaugeSet.sets) == 0 &&
			len(globalTaintGaugeSet.sets) == 0 &&
			len(globalInfoGaugeSet.sets) == 0
	}

	deadline := time.Now().Add(2 * time.Second)
	cleaned := false
	for time.Now().Before(deadline) {
		if assertEmpty() {
			cleaned = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !cleaned {
		t.Errorf("background sweeper failed to purge expired tombstones when no reader callbacks occurred")
	}
}

func TestGoroutineLifecycle_ShutdownStopsSweeper(t *testing.T) {
	ctx := context.Background()
	mockMeta := metadata.GoogleMetadata{ProjectID: "mock"}
	shutdown, err := Init(ctx, "", false, mockMeta, 30*time.Minute, 100000, true, true)
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Verify shutdown completes without hang or error
	shutdown()
}
