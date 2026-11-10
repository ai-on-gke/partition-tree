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

package gce

import (
	"context"
	"fmt"

	computebeta "google.golang.org/api/compute/v0.beta"
	"google.golang.org/api/option"
)

// RealClient is the production implementation of Client, wrapping
// google.golang.org/api/compute/v0.beta. It holds one *compute.Service
// and translates the GCE response shape into our domain TopologyEntry
// so the rest of the controller never imports compute/v0.beta.
type RealClient struct {
	svc *computebeta.Service
}

// NewRealClient builds a production client. Callers pass the usual
// option.ClientOption slice — WithCredentials / WithTokenSource in
// prod, WithEndpoint + WithoutAuthentication in tests that stand up an
// httptest server — and we forward them to compute.NewService.
func NewRealClient(ctx context.Context, opts ...option.ClientOption) (*RealClient, error) {
	svc, err := computebeta.NewService(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("building compute beta service: %w", err)
	}
	return &RealClient{svc: svc}, nil
}

// GetAvailableAcceleratorTopologies calls
// InstanceGroupManagers.GetAvailableAcceleratorTopologies and converts
// the response map into a flat []TopologyEntry. The caller
// (internal/tree.Build) then walks ParentID refs to recover the tree.
//
// Mapping notes (from the compute/v0.beta schema):
//
//   - The map key is our PartitionID.
//   - info.Parent is "" on the single root entry and set otherwise.
//   - State combines AcceleratorTopologyHealth and InstancesHealth:
//     either being unhealthy forces Unhealthy; AcceleratorTopologyHealth
//     DEGRADED forces Degraded (InstancesHealth has no degraded state).
//     For non-root partitions, AcceleratorTopologyState.CurrentState of
//     INCOMPLETE has the highest priority and overrides both. The root's
//     Incomplete is derived separately from total vs existing nodes.
//   - SliceID is lifted from AcceleratorTopologyState.ExternalId,
//     which GCE sets during ConfigureAcceleratorTopologies. This can be
//     set on any partition level (leaf, middle, or root), not just leaves.
//   - MultiMig on the root entry reflects whether the response's
//     top-level MultiMig URL is populated.
//   - AcceleratorType is intentionally left empty — the beta API does
//     not surface it on this call. The reconciler stamps it from the
//     cloud.google.com/gke-tpu-accelerator node label onto the
//     top-level PartitionTreeStatus.
func (r *RealClient) GetAvailableAcceleratorTopologies(
	ctx context.Context,
	projectNumber, zone, mig string,
) (*TopologyResponse, error) {
	raw, err := r.svc.InstanceGroupManagers.
		GetAvailableAcceleratorTopologies(projectNumber, zone, mig).
		Context(ctx).
		Do()
	if err != nil {
		return nil, fmt.Errorf("gce: get available accelerator topologies %s/%s/%s: %w",
			projectNumber, zone, mig, err)
	}

	out := &TopologyResponse{
		Entries:  make([]TopologyEntry, 0, len(raw.AcceleratorTopologiesInfo)),
		MultiMig: raw.MultiMig,
	}
	for id, info := range raw.AcceleratorTopologiesInfo {
		entry := TopologyEntry{
			PartitionID: id,
			ParentID:    info.Parent,
			Topology:    info.AcceleratorTopology,
			State:       combineHealth(info.AcceleratorTopologyHealth, info.InstancesHealth),
		}
		if info.AcceleratorTopologyState != nil {
			entry.SliceID = info.AcceleratorTopologyState.ExternalId
			// INCOMPLETE has the highest priority, overriding Unhealthy/Degraded
			// from combineHealth.
			if info.AcceleratorTopologyState.CurrentState == "INCOMPLETE" {
				entry.State = StateIncomplete
			}
		}
		if info.Parent == "" && raw.MultiMig != "" {
			entry.MultiMig = true
		}
		out.Entries = append(out.Entries, entry)
	}
	return out, nil
}

// combineHealth merges AcceleratorTopologyHealth and InstancesHealth
// into a single partition state. Either signal being unhealthy forces
// the partition to Unhealthy — a partition with healthy inter-chip
// connectivity but downed VMs is not usable, and vice versa.
// AcceleratorTopologyHealth can be DEGRADED; InstancesHealth cannot.
//
// Precedence (non-root): Incomplete > Unhealthy > Degraded > Healthy > Unknown.
// Incomplete is applied separately after combineHealth; this function handles
// the health-only precedence: Unhealthy > Degraded (topology only) > Healthy > Unknown.
func combineHealth(topoHealth, instancesHealth string) string {
	topo := mapTopologyHealth(topoHealth)
	inst := mapInstancesHealth(instancesHealth)

	// Either unhealthy → Unhealthy.
	if topo == StateUnhealthy || inst == StateUnhealthy {
		return StateUnhealthy
	}
	// Topology degraded → Degraded (InstancesHealth has no degraded state).
	if topo == StateDegraded {
		return StateDegraded
	}
	// Both healthy → Healthy; otherwise keep topo (may be Unknown).
	return topo
}

// mapTopologyHealth converts AcceleratorTopologyHealth ("HEALTHY",
// "DEGRADED", "UNHEALTHY") into the title-cased form the
// v1beta1.PartitionStateType constants use.
func mapTopologyHealth(h string) string {
	switch h {
	case "HEALTHY":
		return StateHealthy
	case "DEGRADED":
		return StateDegraded
	case "UNHEALTHY":
		return StateUnhealthy
	default:
		return StateUnknown
	}
}

// mapInstancesHealth converts InstancesHealth ("ALL_HEALTHY",
// "UNHEALTHY_OR_MISSING") into the same title-cased form.
func mapInstancesHealth(h string) string {
	switch h {
	case "ALL_HEALTHY":
		return StateHealthy
	case "UNHEALTHY_OR_MISSING":
		return StateUnhealthy
	default:
		return StateUnknown
	}
}

// Compile-time interface conformance.
var _ Client = (*RealClient)(nil)
