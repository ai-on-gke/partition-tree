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

package controller

import corev1 "k8s.io/api/core/v1"

// Label keys read off GKE Node objects.
const (
	// LabelTopologyMode marks a node as participating in the TPU
	// accelerator topology feature. The filter requires the exact value
	// LabelTopologyModeProvisionOnly.
	LabelTopologyMode              = "cloud.google.com/gke-accelerator-topology-mode"
	LabelTopologyModeProvisionOnly = "PROVISION_ONLY"

	// LabelNodePool is the GKE node pool name, used as the PartitionTree
	// CR name.
	LabelNodePool = "cloud.google.com/gke-nodepool"

	// LabelPartitionIDPrefix prefixes all "cloud.google.com/gke-tpu-
	// partition-<topology>-id" labels (v7x and later generations). Each
	// matching label contributes one entry to NodeEntry.PartitionIDs.
	LabelPartitionIDPrefix = "cloud.google.com/gke-tpu-partition-"
	LabelPartitionIDSuffix = "-id"

	// LabelSliceIDPrefix prefixes the v6e-generation
	// "cloud.google.com/gke-tpu-slice-<topology>-id" labels. v6e nodes
	// surface their partition hierarchy via "slice" labels rather than
	// the "partition" labels used by v7x and later, so both prefixes are
	// treated as partition-ID labels. Shares LabelPartitionIDSuffix.
	LabelSliceIDPrefix = "cloud.google.com/gke-tpu-slice-"

	// LabelTPUAccelerator is the GKE-managed label that identifies the
	// TPU generation and machine class. Only v6e and later generations
	// are supported. Example values:
	//
	//   tpu-v6e-slice           → v6e
	//   tpu-v7x-slice           → v7x
	//
	// The reconciler extracts the family and stamps it onto every
	// partition of the tree. It is a best-effort enrichment — a node
	// without this label still passes the filter and simply surfaces
	// AcceleratorType="".
	LabelTPUAccelerator = "cloud.google.com/gke-tpu-accelerator"

	// LabelTopologyBlock is the GCE-managed label that identifies the
	// physical superpod ("block") a node belongs to. All nodes in the
	// same superpod share the same value. The partition-tree controller
	// propagates this onto the PartitionTree CR as
	// cloud.google.com/tpu-pod-id so the slice-scheduler can group trees
	// belonging to the same physical pod for multi-cube scheduling.
	LabelTopologyBlock = "cloud.google.com/gce-topology-block"
)

// Labels written by the controller onto PartitionTree CRs.
const (
	// LabelTpuPodID is stamped on the PartitionTree CR at creation
	// time with the value of the node's cloud.google.com/gce-topology-block
	// label. The slice-scheduler reads this to group trees belonging
	// to the same physical TPU pod for multi-cube scheduling.
	LabelTpuPodID = "cloud.google.com/tpu-pod-id"
)

// ResourceTPU is the extended resource the TPU device plugin advertises
// in Node status.allocatable. With --enable-tpu-allocatable-readiness the
// node cache keeps this one allocatable entry and a leaf's NodeReady
// condition requires every node to have its full chip count allocatable.
const ResourceTPU corev1.ResourceName = "google.com/tpu"

// Taint keys written by the controller onto PartitionTree status.
const (
	TaintKeyPartitionState = "cloud.google.com/partition-state"
	TaintKeyNodesNotFound  = "cloud.google.com/nodes-not-found"
)

// Options for the --enable-taints flag.
const (
	TaintOptionAll            = "All"
	TaintOptionNodeTaints     = "NodeTaints"
	TaintOptionNodesNotFound  = "NodesNotFound"
	TaintOptionPartitionState = "PartitionState"
)

// Annotation keys.
const (
	// AnnotationCreatedByMIG is the GKE-owned annotation on Nodes that
	// carries the full MIG self-link.
	AnnotationCreatedByMIG = "node.gke.io/created-by-mig"

	// AnnotationMIGName is the annotation the controller stamps on the
	// PartitionTree CR at creation time so that reconciliation can
	// survive a restart without waiting for the node informer's initial
	// list.
	AnnotationMIGName = "tpu.gke.io/mig-name"
)

// FinalizerName is added to every PartitionTree CR at first reconcile
// and removed only after cleanup. It protects against the CR being
// deleted out from under the reconciler mid-flight.
const FinalizerName = "partition.tpu.google.com/finalizer"

// Event reason strings emitted by the controller.
const (
	EventReasonCreated         = "PartitionTreeCreated"
	EventReasonPoolDrained     = "NodePoolDrained"
	EventReasonGCEPollError    = "GCEPollFailed"
	EventReasonTaintParseError = "TaintParseError"
	EventReasonUnusedTaint     = "UnusedTaintTarget"
	EventReasonSyncLost        = "SyncLost"
	EventReasonSyncRestored    = "SyncRestored"
	EventReasonReprovision     = "NodePoolReprovisioned"
)

// ReasonPoolDrained is the Synced condition reason set while a decoupled-mode
// CR's node pool is drained (MIG annotation cleared, GCE polling suspended).
const ReasonPoolDrained = "PoolDrained"

// topologyRootLabel maps accelerator types to their root partition label
// key. The node informer uses this mapping to extract the root partition
// ID from a node's labels. The root partition ID becomes the CR name in
// decoupled lifecycle mode. Unexported to prevent accidental mutation.
var topologyRootLabel = map[string]string{
	"tpu-v6e-slice": "cloud.google.com/gke-tpu-slice-16x16-id",
	"tpu7x":         "cloud.google.com/gke-tpu-partition-4x4x4-id",
}

// tpuChipsPerHost maps accelerator types to the number of TPU chips each
// host exposes, i.e. the google.com/tpu allocatable a fully healthy node
// reports. It must cover every accelerator admitted by parseNode:
//
//	tpu7x          → tpu7x-standard-4t
//	tpu-v6e-slice  → ct6e-standard-4t
//
// The value is deliberately not derived from status.capacity (the kubelet
// publishes capacity 0 together with allocatable 0 after a reboot) nor
// from the leaf topology divided by the cached node count (a missing host
// would raise the expectation for the healthy ones). Unexported to
// prevent accidental mutation.
var tpuChipsPerHost = map[string]int64{
	"tpu-v6e-slice": 4,
	"tpu7x":         4,
}

// expectedTPUChipsPerHost returns the google.com/tpu allocatable a fully
// healthy node of the given accelerator type reports. An unknown
// accelerator returns 0, which disables the chip check for that node.
func expectedTPUChipsPerHost(acceleratorType string) int64 {
	return tpuChipsPerHost[acceleratorType]
}
