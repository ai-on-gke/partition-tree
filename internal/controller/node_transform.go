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

import (
	"slices"

	corev1 "k8s.io/api/core/v1"
	clientgocache "k8s.io/client-go/tools/cache"
)

// StripNodeForCache is a client-go TransformFunc that trims a cached
// *corev1.Node down to only the fields read by parseNode, isNodeReady,
// rootPartitionIDForNode, and matchingNodesPredicate.
//
// Status.Allocatable is dropped, so parseNode's TPUAllocatable reads 0 and
// the predicate's google.com/tpu allocatable diff never fires.
// StripNodeForCacheKeepTPUAllocatable keeps that one entry for
// --enable-tpu-allocatable-readiness.
//
// Important:
//   - In controller-runtime, a per-object ByObject[T].Transform replaces
//     Options.DefaultTransform rather than composing with it, so n.ManagedFields
//     must be cleared explicitly here even when DefaultTransform is set to
//     cache.TransformStripManagedFields().
//   - StripNodeForCache is idempotent: client-go (>=1.27) invokes TransformFunc
//     before inserting an object and may invoke it again on objects already in
//     the store or wrapped in DeletedFinalStateUnknown.
func StripNodeForCache(obj any) (any, error) {
	switch t := obj.(type) {
	case *corev1.Node:
		stripNode(t)
		return t, nil
	case clientgocache.DeletedFinalStateUnknown:
		// Defensive: client-go already transforms objects before tombstoning,
		// so in-place mutation of t.Obj is safe only because stripNode is idempotent.
		if n, ok := t.Obj.(*corev1.Node); ok {
			stripNode(n)
		}
		return t, nil
	default:
		return obj, nil
	}
}

// StripNodeForCacheKeepTPUAllocatable is the TransformFunc used with
// --enable-tpu-allocatable-readiness. It strips exactly like
// StripNodeForCache, then keeps Status.Allocatable as a fresh 1-key
// ResourceList holding only google.com/tpu, and only if the incoming Node
// reports that key. Like StripNodeForCache it is idempotent and handles
// DeletedFinalStateUnknown tombstones.
func StripNodeForCacheKeepTPUAllocatable(obj any) (any, error) {
	switch t := obj.(type) {
	case *corev1.Node:
		stripNodeKeepTPUAllocatable(t)
		return t, nil
	case clientgocache.DeletedFinalStateUnknown:
		// Same reasoning as StripNodeForCache: safe because the strip is
		// idempotent.
		if n, ok := t.Obj.(*corev1.Node); ok {
			stripNodeKeepTPUAllocatable(n)
		}
		return t, nil
	default:
		return obj, nil
	}
}

func isRetainedNodeLabel(k string) bool {
	switch k {
	case LabelTopologyMode, LabelNodePool, LabelTPUAccelerator, LabelTopologyBlock:
		return true
	}
	return isPartitionLabel(k)
}

func stripNode(n *corev1.Node) {
	if n == nil {
		return
	}
	n.ManagedFields = nil
	n.OwnerReferences = nil
	n.Finalizers = nil

	// Prune n.Labels to only the keys read by parseNode and rootPartitionIDForNode,
	// reallocating into a compact map when extra labels are present so the large
	// decoded hmap bucket array (~40-60 labels on GKE nodes) is freed for GC.
	keptLabels := 0
	hasUnneededLabel := false
	for k := range n.Labels {
		if isRetainedNodeLabel(k) {
			keptLabels++
		} else {
			hasUnneededLabel = true
		}
	}
	if hasUnneededLabel {
		if keptLabels == 0 {
			n.Labels = nil
		} else {
			pruned := make(map[string]string, keptLabels)
			for k, v := range n.Labels {
				if isRetainedNodeLabel(k) {
					pruned[k] = v
				}
			}
			n.Labels = pruned
		}
	}

	// Retain only the MIG self-link annotation read by parseNode.
	// Avoid reallocating if the map already contains only this key.
	if mig, ok := n.Annotations[AnnotationCreatedByMIG]; ok {
		if len(n.Annotations) > 1 {
			n.Annotations = map[string]string{AnnotationCreatedByMIG: mig}
		}
	} else {
		n.Annotations = nil
	}

	var taints []corev1.Taint
	if len(n.Spec.Taints) > 0 {
		taints = n.Spec.Taints
		if cap(taints) > len(taints) {
			taints = slices.Clone(taints)
		}
	}
	n.Spec = corev1.NodeSpec{
		Taints: taints,
	}

	// Allocate a per-node 1-element slice holding only the NodeReady condition
	// status so the decoded []NodeCondition backing array (~1.8 KB for 12 GKE
	// conditions) is freed for GC without aliasing a shared backing array across
	// cached Nodes.
	var strippedConds []corev1.NodeCondition
	for i := range n.Status.Conditions {
		if n.Status.Conditions[i].Type == corev1.NodeReady {
			strippedConds = []corev1.NodeCondition{{
				Type:   corev1.NodeReady,
				Status: n.Status.Conditions[i].Status,
			}}
			break
		}
	}
	// Status.Allocatable is intentionally dropped here; see
	// stripNodeKeepTPUAllocatable for the flag-on variant.
	n.Status = corev1.NodeStatus{
		Conditions: strippedConds,
	}
}

// stripNodeKeepTPUAllocatable runs stripNode and then restores the
// google.com/tpu allocatable entry read by parseNode and
// matchingNodesPredicate. The entry is copied into a new 1-key
// ResourceList so the decoded allocatable map (cpu, memory, pods,
// ephemeral-storage, hugepages, ...) is freed for GC rather than aliased.
// A map that already holds only google.com/tpu (a re-applied transform)
// is kept as is.
func stripNodeKeepTPUAllocatable(n *corev1.Node) {
	if n == nil {
		return
	}
	alloc := n.Status.Allocatable
	tpu, hasTPU := alloc[ResourceTPU]
	stripNode(n)
	switch {
	case !hasTPU:
		// stripNode already dropped Allocatable.
	case len(alloc) == 1:
		n.Status.Allocatable = alloc
	default:
		n.Status.Allocatable = corev1.ResourceList{ResourceTPU: tpu.DeepCopy()}
	}
}
