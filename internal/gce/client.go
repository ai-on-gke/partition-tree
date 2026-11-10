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

// Package gce defines the controller-side abstraction over the GCE compute
// API. The reconciler depends only on the Client interface; production wires
// a real client, tests wire MockClient.
package gce

import "context"

// Partition health states returned by combineHealth / the INCOMPLETE override.
// These mirror api/v1beta1.PartitionStateType values but are plain strings
// to avoid a dependency from gce → api.
const (
	StateHealthy    = "Healthy"
	StateUnhealthy  = "Unhealthy"
	StateDegraded   = "Degraded"
	StateIncomplete = "Incomplete"
	StateUnknown    = "Unknown"
)

// TopologyEntry is one partition returned by
// InstanceGroupManagers.GetAvailableAcceleratorTopologies. This is the
// controller's domain shape — it is translated from the GCE Go client type
// at the edge, so nothing else in the controller imports compute/v0.beta.
//
// Note: AcceleratorType (v7x/v6e/v5p/…) is intentionally absent here —
// the beta API does not surface it, so the reconciler derives it from
// the cloud.google.com/gke-tpu-accelerator node label via the cache
// and stamps it onto each partition after tree.Build.
type TopologyEntry struct {
	// PartitionID uniquely identifies this partition within the MIG.
	PartitionID string
	// ParentID is the partition ID of this entry's parent, or "" for the root.
	ParentID string
	// Topology is the shape string, e.g. "4x4x4", "2x2", "16x16".
	Topology string
	// State is the combined health derived from AcceleratorTopologyHealth
	// and InstancesHealth. Either being unhealthy forces Unhealthy;
	// AcceleratorTopologyHealth being DEGRADED forces Degraded (InstancesHealth
	// has no degraded state). For non-root partitions, INCOMPLETE from
	// AcceleratorTopologyState has the highest priority and overrides both.
	// The root's Incomplete is derived separately from total vs existing nodes.
	State string
	// SliceID is non-empty when this partition is currently reserved by a
	// slice, which seeds Occupancy = Occupied. This applies to any level
	// of the tree — leaves, middle partitions (e.g. 2x2x4), and root.
	SliceID string
	// MultiMig, when true on the root, seeds root Occupancy = Occupied
	// (the entire MIG is participating in a multi-MIG reservation).
	MultiMig bool
}

// TopologyResponse is the flat parent-ref view of one MIG's partition tree.
type TopologyResponse struct {
	Entries  []TopologyEntry
	MultiMig string
}

// Client abstracts the subset of the compute API this controller needs.
type Client interface {
	GetAvailableAcceleratorTopologies(
		ctx context.Context,
		projectNumber, zone, mig string,
	) (*TopologyResponse, error)
}
