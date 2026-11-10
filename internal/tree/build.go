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

// Package tree holds the pure functions that turn a flat GCE topology
// response into a recursive PartitionInfo tree, bubble Occupancy across
// the tree, and derive per-partition conditions. Nothing in this package
// talks to the apiserver or to GCE — it is unit-testable without either.
package tree

import (
	"fmt"
	"path"
	"sort"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
	"github.com/ai-on-gke/partition-tree/internal/gce"
)

// Build turns a flat parent-ref topology response into a recursive
// PartitionInfo tree. It seeds the per-partition Occupancy field from the
// GCE hints (SliceID on leaves, MultiMig on root); call Bubble afterward
// to propagate Occupancy across the tree.
//
// Build errors out on: empty responses, duplicate partition IDs, multiple
// roots, orphaned entries (parent not present), and cycles.
func Build(resp *gce.TopologyResponse, taints map[string][]partitionv1beta1.PartitionTaint) (partitionv1beta1.PartitionInfo, error) {
	if resp == nil || len(resp.Entries) == 0 {
		return partitionv1beta1.PartitionInfo{}, fmt.Errorf("empty topology response")
	}

	byID := make(map[string]*gce.TopologyEntry, len(resp.Entries))
	for i := range resp.Entries {
		e := &resp.Entries[i]
		if _, dup := byID[e.PartitionID]; dup {
			return partitionv1beta1.PartitionInfo{}, fmt.Errorf("duplicate partition id %q", e.PartitionID)
		}
		byID[e.PartitionID] = e
	}

	children := make(map[string][]string)
	var rootID string
	for _, e := range resp.Entries {
		if e.ParentID == "" {
			if rootID != "" {
				return partitionv1beta1.PartitionInfo{}, fmt.Errorf("multiple roots: %q and %q", rootID, e.PartitionID)
			}
			rootID = e.PartitionID
			continue
		}
		if _, ok := byID[e.ParentID]; !ok {
			return partitionv1beta1.PartitionInfo{}, fmt.Errorf("orphan partition %q: parent %q not in response", e.PartitionID, e.ParentID)
		}
		children[e.ParentID] = append(children[e.ParentID], e.PartitionID)
	}
	if rootID == "" {
		return partitionv1beta1.PartitionInfo{}, fmt.Errorf("no root partition in response")
	}

	// Sort each parent's children by partition ID so the tree is
	// deterministic regardless of the GCE response entry order.
	for _, kids := range children {
		sort.Strings(kids)
	}

	root, err := buildNode(rootID, byID, children, map[string]bool{}, true, resp.MultiMig, taints)
	return root, err
}

func buildNode(
	id string,
	byID map[string]*gce.TopologyEntry,
	children map[string][]string,
	visiting map[string]bool,
	isRoot bool,
	multiMig string,
	taints map[string][]partitionv1beta1.PartitionTaint,
) (partitionv1beta1.PartitionInfo, error) {
	if visiting[id] {
		return partitionv1beta1.PartitionInfo{}, fmt.Errorf("cycle detected at %q", id)
	}
	visiting[id] = true
	defer delete(visiting, id)

	e := byID[id]
	sliceId := e.SliceID
	if isRoot && multiMig != "" {
		sliceId = path.Base(multiMig)
	}
	info := partitionv1beta1.PartitionInfo{
		PartitionID: e.PartitionID,
		Topology:    e.Topology,
		// AcceleratorType is stamped by the reconciler from the
		// cached node-pool label, not by tree.Build — GCE's response
		// does not carry it.
		State:     partitionv1beta1.PartitionStateType(e.State),
		Occupancy: seedOccupancy(e),
		SliceId:   sliceId,
		Taints:    taints[e.PartitionID],
	}
	for _, cid := range children[id] {
		child, err := buildNode(cid, byID, children, visiting, false, "", taints)
		if err != nil {
			return partitionv1beta1.PartitionInfo{}, err
		}
		info.ChildPartitions = append(info.ChildPartitions, child)
	}
	return info, nil
}

// seedOccupancy implements the "seed from GCE response" half of the
// occupancy rules: SliceID set (on any partition — leaf, middle, or
// root) ⇒ Occupied; MultiMig set (on the root) ⇒ Occupied; otherwise
// Unspecified. Bubble fills in the rest.
func seedOccupancy(e *gce.TopologyEntry) partitionv1beta1.OccupancyState {
	if e.SliceID != "" || e.MultiMig {
		return partitionv1beta1.OccupancyOccupied
	}
	return partitionv1beta1.OccupancyUnspecified
}
