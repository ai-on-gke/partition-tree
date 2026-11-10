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

package viz

import (
	"fmt"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
)

func sampleTree() *partitionv1beta1.PartitionTree {
	return &partitionv1beta1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-0"},
		Status: partitionv1beta1.PartitionTreeStatus{
			Conditions: []metav1.Condition{
				{Type: partitionv1beta1.ConditionTypeSynced, Status: metav1.ConditionTrue, Reason: "InSync"},
			},
			AcceleratorType: "v7x",
			Root: &partitionv1beta1.PartitionInfo{
				PartitionID: "root",
				Topology:    "4x4x4",
				State:       partitionv1beta1.PartitionStateHealthy,
				Occupancy:   partitionv1beta1.OccupancyOccupied,
				ChildPartitions: []partitionv1beta1.PartitionInfo{
					{
						PartitionID: "p-0",
						Topology:    "2x2x4",
						State:       partitionv1beta1.PartitionStateHealthy,
						Occupancy:   partitionv1beta1.OccupancyEmpty,
						ChildPartitions: []partitionv1beta1.PartitionInfo{
							{
								PartitionID: "leaf-0",
								Topology:    "2x2x1",
								State:       partitionv1beta1.PartitionStateHealthy,
								Occupancy:   partitionv1beta1.OccupancyEmpty,
								NodeNames:   []string{"n1", "n2"},
								Conditions: []metav1.Condition{
									{Type: partitionv1beta1.ConditionTypeNodeReady, Status: metav1.ConditionTrue, Reason: "NodesReady"},
								},
							},
							{
								PartitionID: "leaf-1",
								Topology:    "2x2x1",
								State:       partitionv1beta1.PartitionStateDegraded,
								Occupancy:   partitionv1beta1.OccupancyEmpty,
								NodeNames:   []string{"n3", "n4"},
								Conditions: []metav1.Condition{
									{Type: partitionv1beta1.ConditionTypeNodeReady, Status: metav1.ConditionFalse, Reason: "NodesNotReady"},
								},
								Taints: []partitionv1beta1.PartitionTaint{
									{Key: "maintenance", Effect: partitionv1beta1.TaintEffectNoSchedule},
								},
							},
						},
					},
					{
						PartitionID: "p-1",
						Topology:    "2x2x4",
						State:       partitionv1beta1.PartitionStateHealthy,
						Occupancy:   partitionv1beta1.OccupancyOccupied,
						ChildPartitions: []partitionv1beta1.PartitionInfo{
							{
								PartitionID: "leaf-2",
								Topology:    "2x2x1",
								State:       partitionv1beta1.PartitionStateHealthy,
								Occupancy:   partitionv1beta1.OccupancyOccupied,
								NodeNames:   []string{"n5", "n6"},
								Conditions: []metav1.Condition{
									{Type: partitionv1beta1.ConditionTypeNodeReady, Status: metav1.ConditionTrue, Reason: "NodesReady"},
								},
							},
							{
								PartitionID: "leaf-3",
								Topology:    "2x2x1",
								State:       partitionv1beta1.PartitionStateHealthy,
								Occupancy:   partitionv1beta1.OccupancyOccupied,
								NodeNames:   []string{"n7", "n8"},
								Conditions: []metav1.Condition{
									{Type: partitionv1beta1.ConditionTypeNodeReady, Status: metav1.ConditionTrue, Reason: "NodesReady"},
								},
							},
						},
					},
				},
			},
		},
	}
}

func TestRenderTree_Structure(t *testing.T) {
	cr := sampleTree()
	out := RenderTree(cr, false)

	// Verify header.
	if !strings.HasPrefix(out, "pool-0  (v7x)  Synced=True\n") {
		t.Errorf("header mismatch:\n%s", out)
	}

	// Verify all partition IDs appear.
	for _, id := range []string{"root", "p-0", "p-1", "leaf-0", "leaf-1", "leaf-2", "leaf-3"} {
		if !strings.Contains(out, id) {
			t.Errorf("missing partition %q in output:\n%s", id, out)
		}
	}

	// Verify tree connectors.
	if !strings.Contains(out, "├── ") {
		t.Error("missing ├── connector")
	}
	if !strings.Contains(out, "└── ") {
		t.Error("missing └── connector")
	}

	// Verify node names on leaves.
	if !strings.Contains(out, "nodes=[n1,n2]") {
		t.Errorf("missing node names:\n%s", out)
	}

	// Verify state/occupancy indicators.
	if !strings.Contains(out, "[Occupied]") {
		t.Error("missing Occupied indicator")
	}
	if !strings.Contains(out, "[Empty]") {
		t.Error("missing Empty indicator")
	}
	if !strings.Contains(out, "NodesReady") {
		t.Error("missing NodesReady reason")
	}
	if !strings.Contains(out, "NodesNotReady") {
		t.Error("missing NodesNotReady reason")
	}
	if !strings.Contains(out, "[Taints: maintenance:NoSchedule]") {
		t.Error("missing Taints string")
	}
}

func TestRenderTree_NoColor(t *testing.T) {
	cr := sampleTree()
	out := RenderTree(cr, false)
	if strings.Contains(out, "\033[") {
		t.Errorf("no-color output contains ANSI codes:\n%s", out)
	}
}

func TestRenderTree_WithColor(t *testing.T) {
	cr := sampleTree()
	out := RenderTree(cr, true)
	if !strings.Contains(out, "\033[") {
		t.Errorf("color output missing ANSI codes:\n%s", out)
	}
}

func TestRenderFleetTable(t *testing.T) {
	// Build a small fleet.
	items := make([]partitionv1beta1.PartitionTree, 0, 5)
	for i := range 5 {
		cr := *sampleTree()
		cr.Name = strings.Replace(cr.Name, "pool-0", "pool-"+string(rune('a'+i)), 1)
		items = append(items, cr)
	}

	out := RenderFleetTable(items, false)

	// Verify fleet header.
	if !strings.Contains(out, "Fleet: 5 PartitionTrees") {
		t.Errorf("fleet header missing:\n%s", out)
	}

	// Verify column headers.
	if !strings.Contains(out, "NAME") || !strings.Contains(out, "NODES") {
		t.Errorf("column headers missing:\n%s", out)
	}

	// Verify all pools appear.
	for i := range 5 {
		name := "pool-" + string(rune('a'+i))
		if !strings.Contains(out, name) {
			t.Errorf("missing pool %q in fleet table:\n%s", name, out)
		}
	}
}

func TestRenderFleetTable_LargeFleet(t *testing.T) {
	// Simulate 500 CRs to verify no panics and reasonable output size.
	items := make([]partitionv1beta1.PartitionTree, 500)
	for i := range items {
		items[i] = partitionv1beta1.PartitionTree{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pool-%04d", i)},
			Status: partitionv1beta1.PartitionTreeStatus{
				Conditions: []metav1.Condition{
					{Type: partitionv1beta1.ConditionTypeSynced, Status: metav1.ConditionTrue, Reason: "InSync"},
				},
				AcceleratorType: "v7x",
				Root: &partitionv1beta1.PartitionInfo{
					PartitionID: "root",
					Topology:    "4x4x4",
					State:       partitionv1beta1.PartitionStateHealthy,
					Occupancy:   partitionv1beta1.OccupancyOccupied,
					ChildPartitions: []partitionv1beta1.PartitionInfo{
						{
							PartitionID: "leaf-0",
							Topology:    "2x2x1",
							State:       partitionv1beta1.PartitionStateHealthy,
							Occupancy:   partitionv1beta1.OccupancyOccupied,
							NodeNames:   []string{"n1", "n2"},
						},
					},
				},
			},
		}
	}

	out := RenderFleetTable(items, false)
	if !strings.Contains(out, "Fleet: 500 PartitionTrees") {
		t.Errorf("500-CR header missing:\n%.200s", out)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	// 3 header lines + 500 data rows = 503 lines
	if len(lines) < 503 {
		t.Errorf("expected at least 503 lines, got %d", len(lines))
	}
}

func TestSummarizeFleet(t *testing.T) {
	cr := sampleTree()
	summaries := SummarizeFleet([]partitionv1beta1.PartitionTree{*cr})
	if len(summaries) != 1 {
		t.Fatalf("expected 1 summary, got %d", len(summaries))
	}
	s := summaries[0]
	if s.Name != "pool-0" {
		t.Errorf("name: %q", s.Name)
	}
	if s.AcceleratorType != "v7x" {
		t.Errorf("accelerator: %q", s.AcceleratorType)
	}
	// root + p-0 + p-1 + leaf-0 + leaf-1 + leaf-2 + leaf-3 = 7
	if s.TotalPartitions != 7 {
		t.Errorf("total partitions: %d (want 7)", s.TotalPartitions)
	}
	if s.LeafPartitions != 4 {
		t.Errorf("leaf partitions: %d (want 4)", s.LeafPartitions)
	}
	if s.TotalNodes != 8 {
		t.Errorf("total nodes: %d (want 8)", s.TotalNodes)
	}
	if s.HealthyLeaves != 3 {
		t.Errorf("healthy leaves: %d (want 3)", s.HealthyLeaves)
	}
	if s.DegradedLeaves != 1 {
		t.Errorf("degraded leaves: %d (want 1)", s.DegradedLeaves)
	}
}

func TestTruncate(t *testing.T) {
	if got := truncate("short", 10); got != "short" {
		t.Errorf("short: %q", got)
	}
	if got := truncate("a-very-long-string", 10); got != "a-very-..." {
		t.Errorf("long: %q", got)
	}
}

func TestColorHelpers_EdgeCases(t *testing.T) {
	// Test stateColor defaults
	if got := stateColor(partitionv1beta1.PartitionStateType("UnknownState")); got != colorGray {
		t.Errorf("stateColor(UnknownState) = %q, want %q", got, colorGray)
	}
	if got := stateColor(partitionv1beta1.PartitionStateUnhealthy); got != colorRed {
		t.Errorf("stateColor(Unhealthy) = %q, want %q", got, colorRed)
	}
	if got := stateColor(partitionv1beta1.PartitionStateIncomplete); got != colorYellow {
		t.Errorf("stateColor(Incomplete) = %q, want %q", got, colorYellow)
	}

	// Test occupancyColor defaults
	if got := occupancyColor(partitionv1beta1.OccupancyState("UnknownOccupancy")); got != colorGray {
		t.Errorf("occupancyColor(UnknownOccupancy) = %q, want %q", got, colorGray)
	}

	// Test nodeReadyColor defaults
	if got := nodeReadyColor("UnknownReason"); got != colorGray {
		t.Errorf("nodeReadyColor(UnknownReason) = %q, want %q", got, colorGray)
	}
	if got := nodeReadyColor("NodesNotFound"); got != colorRed {
		t.Errorf("nodeReadyColor(NodesNotFound) = %q, want %q", got, colorRed)
	}
	if got := nodeReadyColor("TPUsNotAllocatable"); got != colorYellow {
		t.Errorf("nodeReadyColor(TPUsNotAllocatable) = %q, want %q", got, colorYellow)
	}
	if got := nodeReadyColor("NodesNotReady"); got != colorYellow {
		t.Errorf("nodeReadyColor(NodesNotReady) = %q, want %q", got, colorYellow)
	}
}
