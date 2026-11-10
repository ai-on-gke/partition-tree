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
	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
)

// Bubble propagates Occupancy across a seeded tree in three passes:
//
//  1. Bottom-up: any descendant Occupied ⇒ ancestors Occupied.
//  2. Top-down: any ancestor Occupied ⇒ descendants Occupied (an occupied
//     super-partition reserves the whole sub-tree).
//  3. Fill: remaining non-Occupied nodes ⇒ Empty if all descendant leaves
//     are Empty, else Unspecified.
//
// After Bubble, every partition's Occupancy is one of Occupied or Empty
// (Unspecified only survives for input shapes with no information).
func Bubble(root *partitionv1beta1.PartitionInfo) {
	if root == nil {
		return
	}
	bubbleUp(root)
	bubbleDown(root, false)
	fillEmpty(root)
}

// bubbleUp returns true if this node or any descendant is Occupied. It
// sets the current node to Occupied if any descendant is.
func bubbleUp(p *partitionv1beta1.PartitionInfo) bool {
	occupiedBelow := false
	for i := range p.ChildPartitions {
		if bubbleUp(&p.ChildPartitions[i]) {
			occupiedBelow = true
		}
	}
	if occupiedBelow {
		p.Occupancy = partitionv1beta1.OccupancyOccupied
	}
	return p.Occupancy == partitionv1beta1.OccupancyOccupied
}

// bubbleDown forces descendants of an Occupied node to also be Occupied.
func bubbleDown(p *partitionv1beta1.PartitionInfo, parentOccupied bool) {
	if parentOccupied {
		p.Occupancy = partitionv1beta1.OccupancyOccupied
	}
	thisOccupied := p.SliceId != "" || parentOccupied
	for i := range p.ChildPartitions {
		bubbleDown(&p.ChildPartitions[i], thisOccupied)
	}
}

// fillEmpty converts remaining non-Occupied nodes to Empty. Returns true
// if all leaves under this node are Empty. (After bubbleUp+bubbleDown any
// node with an Occupied descendant is itself Occupied, so the "else
// Unspecified" branch only fires on tree shapes with missing
// information.)
func fillEmpty(p *partitionv1beta1.PartitionInfo) bool {
	if len(p.ChildPartitions) == 0 {
		if p.Occupancy != partitionv1beta1.OccupancyOccupied {
			p.Occupancy = partitionv1beta1.OccupancyEmpty
			return true
		}
		return false
	}
	allLeavesEmpty := true
	for i := range p.ChildPartitions {
		if !fillEmpty(&p.ChildPartitions[i]) {
			allLeavesEmpty = false
		}
	}
	if p.Occupancy != partitionv1beta1.OccupancyOccupied {
		if allLeavesEmpty {
			p.Occupancy = partitionv1beta1.OccupancyEmpty
		} else {
			p.Occupancy = partitionv1beta1.OccupancyUnspecified
		}
	}
	return allLeavesEmpty && p.Occupancy == partitionv1beta1.OccupancyEmpty
}
