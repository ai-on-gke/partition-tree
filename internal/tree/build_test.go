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

package tree

import (
	"testing"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
	"github.com/ai-on-gke/partition-tree/internal/gce"
)

// v7xResponse is a 5-level hierarchy root→child→grand→great→leaf that
// mirrors the shape of a real v7x 4x4x4 → 2x4x4 → 2x2x4 → 2x2x2 →
// 2x2x1 partition tree. Each level has one child to keep assertions
// simple; structure correctness is the point, not fan-out.
func v7xResponse() *gce.TopologyResponse {
	return &gce.TopologyResponse{
		Entries: []gce.TopologyEntry{
			{PartitionID: "p0", ParentID: "", Topology: "4x4x4", State: "Healthy"},
			{PartitionID: "p1", ParentID: "p0", Topology: "2x4x4", State: "Healthy"},
			{PartitionID: "p2", ParentID: "p1", Topology: "2x2x4", State: "Healthy"},
			{PartitionID: "p3", ParentID: "p2", Topology: "2x2x2", State: "Healthy"},
			{PartitionID: "p4", ParentID: "p3", Topology: "2x2x1", State: "Healthy"},
		},
	}
}

func TestBuild_BasicHierarchy(t *testing.T) {
	root, err := Build(v7xResponse(), nil)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if root.PartitionID != "p0" || root.Topology != "4x4x4" {
		t.Errorf("unexpected root: %+v", root)
	}
	if root.State != partitionv1beta1.PartitionStateHealthy {
		t.Errorf("root state: got %q want Healthy", root.State)
	}

	// Walk down 4 levels; every level should have exactly 1 child.
	cur := &root
	ids := []string{"p0"}
	for depth := 1; depth <= 4; depth++ {
		if len(cur.ChildPartitions) != 1 {
			t.Fatalf("depth %d: got %d children, want 1", depth, len(cur.ChildPartitions))
		}
		cur = &cur.ChildPartitions[0]
		ids = append(ids, cur.PartitionID)
	}
	want := []string{"p0", "p1", "p2", "p3", "p4"}
	for i := range want {
		if ids[i] != want[i] {
			t.Errorf("depth %d: got %q want %q", i, ids[i], want[i])
		}
	}
	if len(cur.ChildPartitions) != 0 {
		t.Errorf("leaf should have no children, got %d", len(cur.ChildPartitions))
	}
}

func TestBuild_SeedsOccupancy(t *testing.T) {
	resp := v7xResponse()
	resp.Entries[4].SliceID = "slice-abc" // leaf
	root, err := Build(resp, nil)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	// Navigate to the leaf.
	cur := &root
	for len(cur.ChildPartitions) > 0 {
		cur = &cur.ChildPartitions[0]
	}
	if cur.Occupancy != partitionv1beta1.OccupancyOccupied {
		t.Errorf("leaf occupancy: got %q want Occupied", cur.Occupancy)
	}
	if cur.SliceId != "slice-abc" {
		t.Errorf("leaf sliceId: got %q want slice-abc", cur.SliceId)
	}
	// Ancestors should still be Unspecified (no Bubble yet).
	if root.Occupancy != partitionv1beta1.OccupancyUnspecified {
		t.Errorf("root occupancy before Bubble: got %q want Unspecified", root.Occupancy)
	}
}

func TestBuild_RootMultiMigSeed(t *testing.T) {
	resp := v7xResponse()
	resp.Entries[0].MultiMig = true
	root, err := Build(resp, nil)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if root.Occupancy != partitionv1beta1.OccupancyOccupied {
		t.Errorf("root occupancy: got %q want Occupied", root.Occupancy)
	}
}

func TestBuild_RootMultiMigSliceId(t *testing.T) {
	resp := v7xResponse()
	resp.Entries[0].MultiMig = true
	resp.MultiMig = "projects/123456789/regions/us-central1/multiMigs/test-multimig-abcdef"
	root, err := Build(resp, nil)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if root.SliceId != "test-multimig-abcdef" {
		t.Errorf("root sliceId: got %q want test-multimig-abcdef", root.SliceId)
	}
}

func TestBuildAndBubble_RootMultiMig(t *testing.T) {
	resp := v7xResponse()
	resp.Entries[0].MultiMig = true
	resp.MultiMig = "projects/123456789/regions/us-central1/multiMigs/test-multimig-abcdef"
	root, err := Build(resp, nil)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	// Before Bubble, only root is occupied (seeded via MultiMig)
	if root.Occupancy != partitionv1beta1.OccupancyOccupied {
		t.Errorf("root occupancy before bubble: got %q want Occupied", root.Occupancy)
	}
	if root.ChildPartitions[0].Occupancy != partitionv1beta1.OccupancyUnspecified {
		t.Errorf("child occupancy before bubble: got %q want Unspecified", root.ChildPartitions[0].Occupancy)
	}
	// Run Bubble to propagate occupancy
	Bubble(&root)
	// After Bubble, all nodes should be occupied because of the root's SliceId
	cur := &root
	for depth := 0; depth <= 4; depth++ {
		if cur.Occupancy != partitionv1beta1.OccupancyOccupied {
			t.Errorf("depth %d after bubble: got %q want Occupied", depth, cur.Occupancy)
		}
		if len(cur.ChildPartitions) > 0 {
			cur = &cur.ChildPartitions[0]
		}
	}
}

func TestBuild_ErrorCases(t *testing.T) {
	cases := []struct {
		name string
		resp *gce.TopologyResponse
	}{
		{
			name: "nil",
			resp: nil,
		},
		{
			name: "empty",
			resp: &gce.TopologyResponse{},
		},
		{
			name: "multi-root",
			resp: &gce.TopologyResponse{Entries: []gce.TopologyEntry{
				{PartitionID: "r1", ParentID: ""},
				{PartitionID: "r2", ParentID: ""},
			}},
		},
		{
			name: "orphan",
			resp: &gce.TopologyResponse{Entries: []gce.TopologyEntry{
				{PartitionID: "root", ParentID: ""},
				{PartitionID: "child", ParentID: "missing"},
			}},
		},
		{
			name: "duplicate-id",
			resp: &gce.TopologyResponse{Entries: []gce.TopologyEntry{
				{PartitionID: "root", ParentID: ""},
				{PartitionID: "root", ParentID: ""},
			}},
		},
		{
			name: "no-root",
			resp: &gce.TopologyResponse{Entries: []gce.TopologyEntry{
				{PartitionID: "a", ParentID: "b"},
				{PartitionID: "b", ParentID: "a"},
			}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Build(tc.resp, nil); err == nil {
				t.Errorf("expected error, got nil")
			}
		})
	}
}

// TestBuild_ChildrenSortedDeterministically verifies that sibling
// partitions are sorted by partition ID regardless of input order.
func TestBuild_ChildrenSortedDeterministically(t *testing.T) {
	// Feed children of "root" in reverse alphabetical order.
	resp := &gce.TopologyResponse{
		Entries: []gce.TopologyEntry{
			{PartitionID: "root", ParentID: "", Topology: "4x4x4", State: "Healthy"},
			{PartitionID: "c", ParentID: "root", Topology: "2x2x4", State: "Healthy"},
			{PartitionID: "b", ParentID: "root", Topology: "2x2x4", State: "Healthy"},
			{PartitionID: "a", ParentID: "root", Topology: "2x2x4", State: "Healthy"},
		},
	}
	root, err := Build(resp, nil)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(root.ChildPartitions) != 3 {
		t.Fatalf("expected 3 children, got %d", len(root.ChildPartitions))
	}
	got := []string{
		root.ChildPartitions[0].PartitionID,
		root.ChildPartitions[1].PartitionID,
		root.ChildPartitions[2].PartitionID,
	}
	want := []string{"a", "b", "c"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("child[%d]: got %q want %q", i, got[i], want[i])
		}
	}
}

func TestBuild_TaintsNoPropagation(t *testing.T) {
	resp := &gce.TopologyResponse{
		Entries: []gce.TopologyEntry{
			{PartitionID: "root", ParentID: "", Topology: "4x4x4", State: "Healthy"},
			{PartitionID: "c1", ParentID: "root", Topology: "2x2x4", State: "Healthy"},
			{PartitionID: "c2", ParentID: "root", Topology: "2x2x4", State: "Healthy"},
		},
	}
	taintA := partitionv1beta1.PartitionTaint{Key: "k1", Value: "v1", Effect: partitionv1beta1.TaintEffectNoSchedule}
	taints := map[string][]partitionv1beta1.PartitionTaint{
		"c1": {taintA},
		"c2": {taintA},
	}

	root, err := Build(resp, taints)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	// Verify taints are on children
	if len(root.ChildPartitions[0].Taints) != 1 || root.ChildPartitions[0].Taints[0] != taintA {
		t.Errorf("expected taint on c1, got: %v", root.ChildPartitions[0].Taints)
	}
	if len(root.ChildPartitions[1].Taints) != 1 || root.ChildPartitions[1].Taints[0] != taintA {
		t.Errorf("expected taint on c2, got: %v", root.ChildPartitions[1].Taints)
	}

	// Verify taint did NOT propagate to root (even though all children have it)
	if len(root.Taints) != 0 {
		t.Errorf("expected no taints on root, but got: %v", root.Taints)
	}
}
