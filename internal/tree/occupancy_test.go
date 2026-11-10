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
)

// flatten returns every node in the tree in pre-order for bulk assertions.
func flatten(p *partitionv1beta1.PartitionInfo) []*partitionv1beta1.PartitionInfo {
	out := make([]*partitionv1beta1.PartitionInfo, 0, 1+len(p.ChildPartitions))
	out = append(out, p)
	for i := range p.ChildPartitions {
		out = append(out, flatten(&p.ChildPartitions[i])...)
	}
	return out
}

// branchyTree: root has two subtrees of depth 2 each. Useful for
// checking that occupancy on one branch propagates correctly.
//
//	   root
//	   /  \
//	  a    b
//	 / \  / \
//	a1 a2 b1 b2
func branchyTree() partitionv1beta1.PartitionInfo {
	leaf := func(id string) partitionv1beta1.PartitionInfo {
		return partitionv1beta1.PartitionInfo{
			PartitionID: id,
			Occupancy:   partitionv1beta1.OccupancyUnspecified,
		}
	}
	return partitionv1beta1.PartitionInfo{
		PartitionID: "root",
		Occupancy:   partitionv1beta1.OccupancyUnspecified,
		ChildPartitions: []partitionv1beta1.PartitionInfo{
			{
				PartitionID: "a",
				Occupancy:   partitionv1beta1.OccupancyUnspecified,
				ChildPartitions: []partitionv1beta1.PartitionInfo{
					leaf("a1"), leaf("a2"),
				},
			},
			{
				PartitionID: "b",
				Occupancy:   partitionv1beta1.OccupancyUnspecified,
				ChildPartitions: []partitionv1beta1.PartitionInfo{
					leaf("b1"), leaf("b2"),
				},
			},
		},
	}
}

func TestBubble_AllEmpty(t *testing.T) {
	root := branchyTree()
	Bubble(&root)
	for _, p := range flatten(&root) {
		if p.Occupancy != partitionv1beta1.OccupancyEmpty {
			t.Errorf("node %q: got %q want Empty", p.PartitionID, p.Occupancy)
		}
	}
}

func TestBubble_LeafOccupiedContaminatesOnlyAncestors(t *testing.T) {
	// Per the corrected logic, a single occupied leaf turns only its
	// ancestors Occupied (bubbling up), while sibling branches remain Empty.
	root := branchyTree()
	// a1 is occupied.
	root.ChildPartitions[0].ChildPartitions[0].Occupancy = partitionv1beta1.OccupancyOccupied
	root.ChildPartitions[0].ChildPartitions[0].SliceId = "slice-abc"
	Bubble(&root)

	expected := map[string]partitionv1beta1.OccupancyState{
		"root": partitionv1beta1.OccupancyOccupied,
		"a":    partitionv1beta1.OccupancyOccupied,
		"a1":   partitionv1beta1.OccupancyOccupied,
		"a2":   partitionv1beta1.OccupancyEmpty,
		"b":    partitionv1beta1.OccupancyEmpty,
		"b1":   partitionv1beta1.OccupancyEmpty,
		"b2":   partitionv1beta1.OccupancyEmpty,
	}

	for _, p := range flatten(&root) {
		want := expected[p.PartitionID]
		if p.Occupancy != want {
			t.Errorf("node %q: got %q want %q", p.PartitionID, p.Occupancy, want)
		}
	}
}

func TestBubble_RootMultiMigContaminatesDescendants(t *testing.T) {
	root := branchyTree()
	root.Occupancy = partitionv1beta1.OccupancyOccupied
	root.SliceId = "slice-abc"
	Bubble(&root)
	for _, p := range flatten(&root) {
		if p.Occupancy != partitionv1beta1.OccupancyOccupied {
			t.Errorf("node %q: got %q want Occupied", p.PartitionID, p.Occupancy)
		}
	}
}

func TestBubble_Idempotent(t *testing.T) {
	root := branchyTree()
	root.ChildPartitions[1].ChildPartitions[1].Occupancy = partitionv1beta1.OccupancyOccupied
	root.ChildPartitions[1].ChildPartitions[1].SliceId = "slice-abc"
	Bubble(&root)
	allPartitions := flatten(&root)
	first := make([]partitionv1beta1.OccupancyState, 0, len(allPartitions))
	for _, p := range allPartitions {
		first = append(first, p.Occupancy)
	}
	Bubble(&root)
	for i, p := range flatten(&root) {
		if p.Occupancy != first[i] {
			t.Errorf("node %q not idempotent: %q vs %q", p.PartitionID, first[i], p.Occupancy)
		}
	}
}

func TestBubble_NilSafe(t *testing.T) {
	// Must not panic.
	Bubble(nil)
}
