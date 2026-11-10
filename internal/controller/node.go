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
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"

	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
	"github.com/ai-on-gke/partition-tree/internal/cache"
)

// ---------------------------------------------------------------------------
// Node parsing
// ---------------------------------------------------------------------------

// parseNode extracts the cache NodeEntry from a Node object, applying
// a strict AND filter:
//
//   - label cloud.google.com/gke-accelerator-topology-mode=PROVISION_ONLY
//   - label cloud.google.com/gke-nodepool=<pool>
//   - annotation node.gke.io/created-by-mig=<self-link>
//   - at least one cloud.google.com/gke-tpu-partition-<topology>-id
//     (v7x and later) or cloud.google.com/gke-tpu-slice-<topology>-id
//     (v6e) label
//
// Any node missing any of the four elements is dropped (ok=false).
func parseNode(n *corev1.Node) (entry cache.NodeEntry, ok bool) {
	if n == nil {
		return cache.NodeEntry{}, false
	}
	labels := n.Labels
	annotations := n.Annotations

	if labels[LabelTopologyMode] != LabelTopologyModeProvisionOnly {
		return cache.NodeEntry{}, false
	}
	pool := labels[LabelNodePool]
	if pool == "" {
		return cache.NodeEntry{}, false
	}
	mig := annotations[AnnotationCreatedByMIG]
	if mig == "" {
		return cache.NodeEntry{}, false
	}

	var pids []string
	for k, v := range labels {
		if isPartitionLabel(k) && v != "" {
			pids = append(pids, v)
		}
	}
	if len(pids) == 0 {
		return cache.NodeEntry{}, false
	}

	accel := labels[LabelTPUAccelerator]
	// Every accelerator admitted here needs an entry in tpuChipsPerHost
	// (constants.go) for the --enable-tpu-allocatable-readiness check.
	//nolint:goconst // These specific TPU family strings are repeated in tests, bypassing constant extraction.
	if accel != "tpu7x" && accel != "tpu-v6e-slice" {
		return cache.NodeEntry{}, false
	}

	var taints []corev1.Taint
	if len(n.Spec.Taints) > 0 {
		taints = make([]corev1.Taint, len(n.Spec.Taints))
		for i := range n.Spec.Taints {
			taints[i] = *n.Spec.Taints[i].DeepCopy()
		}
	}

	return cache.NodeEntry{
		NodePool:        pool,
		MIG:             mig,
		AcceleratorType: labels[LabelTPUAccelerator],
		TopologyBlock:   labels[LabelTopologyBlock],
		PartitionIDs:    pids,
		Ready:           isNodeReady(n),
		TPUAllocatable:  tpuAllocatable(n),
		Taints:          taints,
	}, true
}

// tpuAllocatable returns the Node's google.com/tpu allocatable, or 0 when
// the resource is absent. The node cache only keeps this entry with
// --enable-tpu-allocatable-readiness, so it reads 0 otherwise. A
// fractional quantity is truncated (Value would round it up), so it can
// never satisfy the chip check by rounding.
func tpuAllocatable(n *corev1.Node) int64 {
	q, ok := n.Status.Allocatable[ResourceTPU]
	if !ok {
		return 0
	}
	return q.MilliValue() / 1000
}

// isNodeReady reports whether the Node's Ready condition is True.
func isNodeReady(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Predicate
// ---------------------------------------------------------------------------

// nodeMatchesFilter is the coarse gate: a node event is eligible for
// the controller only if the node carries the topology-mode label.
// The finer per-field filter lives in parseNode.
func nodeMatchesFilter(n *corev1.Node) bool {
	return n != nil && n.Labels[LabelTopologyMode] == LabelTopologyModeProvisionOnly
}

// matchingNodesPredicate admits Create/Delete events for matching
// nodes unconditionally, and admits Update events only when something
// material changed: the Ready condition, the google.com/tpu allocatable
// quantity, the MIG annotation, taints, or the partition-ID, node-pool,
// accelerator or topology-block labels. This keeps heartbeat-only updates
// off the work queue. The allocatable diff is not gated on
// --enable-tpu-allocatable-readiness: with the flag off the cached Nodes
// carry no allocatable, so the diff never fires.
func matchingNodesPredicate() predicate.TypedPredicate[*corev1.Node] {
	return predicate.TypedFuncs[*corev1.Node]{
		CreateFunc: func(e event.TypedCreateEvent[*corev1.Node]) bool {
			return nodeMatchesFilter(e.Object)
		},
		DeleteFunc: func(e event.TypedDeleteEvent[*corev1.Node]) bool {
			// Always admit deletes: a tombstone (watch reconnect) carries
			// only the name, so label checks would silently drop the
			// event and leak a ghost entry in the cache. The handler's
			// cache.Delete is indexed by name and returns "" for unknown
			// nodes, so admitting extra deletes is harmless.
			return true
		},
		GenericFunc: func(e event.TypedGenericEvent[*corev1.Node]) bool {
			return nodeMatchesFilter(e.Object)
		},
		UpdateFunc: func(e event.TypedUpdateEvent[*corev1.Node]) bool {
			oldN := e.ObjectOld
			newN := e.ObjectNew
			if oldN == nil || newN == nil {
				return false
			}
			// If neither the old nor the new version matches the
			// coarse filter, skip. A node that flips into or out of
			// the filter is material.
			oldMatch := nodeMatchesFilter(oldN)
			newMatch := nodeMatchesFilter(newN)
			if !oldMatch && !newMatch {
				return false
			}
			if oldMatch != newMatch {
				return true
			}
			// Both match: only material diffs count.
			if isNodeReady(oldN) != isNodeReady(newN) {
				return true
			}
			if !sameTPUAllocatable(oldN, newN) {
				return true
			}
			if oldN.Annotations[AnnotationCreatedByMIG] != newN.Annotations[AnnotationCreatedByMIG] {
				return true
			}
			if !samePartitionLabels(oldN.Labels, newN.Labels) {
				return true
			}
			if oldN.Labels[LabelNodePool] != newN.Labels[LabelNodePool] {
				return true
			}
			if !sameTaints(oldN.Spec.Taints, newN.Spec.Taints) {
				return true
			}
			if oldN.Labels[LabelTPUAccelerator] != newN.Labels[LabelTPUAccelerator] {
				return true
			}
			if oldN.Labels[LabelTopologyBlock] != newN.Labels[LabelTopologyBlock] {
				return true
			}
			return false
		},
	}
}

// sameTPUAllocatable reports whether two Nodes carry the same
// google.com/tpu allocatable quantity. Absent on one side and present on
// the other counts as a change, even when the present value is 0.
func sameTPUAllocatable(a, b *corev1.Node) bool {
	qa, okA := a.Status.Allocatable[ResourceTPU]
	qb, okB := b.Status.Allocatable[ResourceTPU]
	if okA != okB {
		return false
	}
	return !okA || qa.Cmp(qb) == 0
}

// samePartitionLabels reports whether two label maps carry the same
// partition-ID entries (same keys, same values). Both the v7x+
// gke-tpu-partition-*-id and the v6e gke-tpu-slice-*-id forms count.
// Order-insensitive.
func samePartitionLabels(a, b map[string]string) bool {
	count := func(m map[string]string) int {
		n := 0
		for k := range m {
			if isPartitionLabel(k) {
				n++
			}
		}
		return n
	}
	if count(a) != count(b) {
		return false
	}
	for k, va := range a {
		if !isPartitionLabel(k) {
			continue
		}
		if b[k] != va {
			return false
		}
	}
	return true
}

// isPartitionLabel reports whether k is a partition-ID label. Both the
// v7x+ "gke-tpu-partition-<topology>-id" form and the v6e
// "gke-tpu-slice-<topology>-id" form are recognised; the <topology>
// segment between prefix and suffix must be non-empty.
func isPartitionLabel(k string) bool {
	if !strings.HasSuffix(k, LabelPartitionIDSuffix) {
		return false
	}
	switch {
	case strings.HasPrefix(k, LabelPartitionIDPrefix):
		return len(k) > len(LabelPartitionIDPrefix)+len(LabelPartitionIDSuffix)
	case strings.HasPrefix(k, LabelSliceIDPrefix):
		return len(k) > len(LabelSliceIDPrefix)+len(LabelPartitionIDSuffix)
	default:
		return false
	}
}

// sameTaints reports whether two sets of taints are identical (same keys,
// values, effects, and TimeAdded timestamps at second precision). Order-insensitive.
func sameTaints(a, b []corev1.Taint) bool {
	if len(a) != len(b) {
		return false
	}
	type coreTaintKey struct {
		key           string
		value         string
		effect        corev1.TaintEffect
		hasTimeAdded  bool
		timeAddedUnix int64
	}
	toKey := func(t corev1.Taint) coreTaintKey {
		k := coreTaintKey{key: t.Key, value: t.Value, effect: t.Effect}
		if t.TimeAdded != nil {
			k.hasTimeAdded = true
			k.timeAddedUnix = t.TimeAdded.Unix()
		}
		return k
	}
	count := func(list []corev1.Taint) map[coreTaintKey]int {
		m := make(map[coreTaintKey]int)
		for _, t := range list {
			m[toKey(t)]++
		}
		return m
	}
	ma := count(a)
	mb := count(b)
	if len(ma) != len(mb) {
		return false
	}
	for k, va := range ma {
		if mb[k] != va {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Event handler
// ---------------------------------------------------------------------------

// nodeEventHandler returns a typed handler.TypedFuncs that updates
// the in-memory topology cache on every node add/update/delete and
// enqueues a reconcile request for the corresponding PartitionTree CR
// (named after the node pool).
//
// Cache updates happen *before* the reconcile request is enqueued so
// the reconciler always sees a fresh cache snapshot.
func nodeEventHandler(c *cache.Cache, decoupled bool, ptUpdateCh chan<- event.TypedGenericEvent[*partitionv1beta1.PartitionTree]) handler.TypedEventHandler[*corev1.Node, reconcile.Request] {
	enqueuePool := func(name string, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
		if name == "" {
			return
		}
		q.Add(reconcile.Request{NamespacedName: types.NamespacedName{Name: name}})
	}

	// cleanupDecoupledPool reconciles the given pool, pushing a drain update and
	// cleaning up the pool->rootPartition mapping if the pool was emptied.
	//
	// Invariant: In decoupled mode, each node pool maps 1:1 to a root partition ID.
	// When a pool becomes empty (all nodes drained/migrated/deleted), draining its
	// root partition ID cleans up the MIG configuration on the PartitionTree CR.
	cleanupDecoupledPool := func(pool string, emptied bool, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
		if pool == "" {
			return
		}
		if !decoupled {
			enqueuePool(pool, q)
			return
		}

		target := pool
		partID, ok := c.PoolRootPartitionID(pool)
		if ok {
			target = partID
			if emptied {
				// Last node removed from pool -> push drain update (clear MIG).
				c.PushUpdate(partID, cache.PTUpdateRequest{
					CRName:  partID,
					Drained: true,
				})
				sendPTUpdate(ptUpdateCh, partID)
				// Clean up pool->rootPartition 1:1 mapping now that the pool is empty.
				c.DeletePoolRootPartitionID(pool)
			}
		}
		enqueuePool(target, q)
	}

	onDelete := func(nodeName string, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
		if nodeName == "" {
			return
		}
		pool, emptied := c.Delete(nodeName)
		cleanupDecoupledPool(pool, emptied, q)
	}

	onAddOrUpdate := func(n *corev1.Node, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
		if n == nil {
			return
		}
		entry, ok := parseNode(n)
		if !ok {
			// Node dropped by the filter; remove it from cache if previously present.
			onDelete(n.Name, q)
			return
		}
		oldPool, pool, _ := c.Upsert(n.Name, entry)

		// If node migrated across pools, reconcile the old pool (and drain if emptied).
		if oldPool != "" {
			_, oldPoolStillExists := c.NodePool(oldPool)
			cleanupDecoupledPool(oldPool, !oldPoolStillExists, q)
		}

		if !decoupled {
			enqueuePool(pool, q)
			return
		}

		// Decoupled mode: extract root partition ID from node labels.
		rootPID, err := rootPartitionIDForNode(entry.AcceleratorType, n.Labels)
		if err != nil {
			logf.Log.Error(err, "decoupled: cannot extract root partition ID, skipping",
				"node", n.Name, "accelerator", entry.AcceleratorType)
			return
		}
		sanitized, err := sanitizePartitionID(rootPID)
		if err != nil {
			logf.Log.Error(err, "decoupled: invalid partition ID",
				"node", n.Name, "rawID", rootPID)
			return
		}

		// Record pool→rootPartition mapping for drain lookup.
		c.SetPoolRootPartitionID(pool, sanitized)

		// Check if a CR already exists for this partition.
		crEntry, crExists := c.PartitionCR(sanitized)
		if !crExists {
			// No CR registered for this partition yet — push a create
			// update so the reconciler can create a decoupled CR on
			// its next pass. This handles controller startup when
			// nodes already exist but no CR has been created yet.
			c.PushUpdate(sanitized, cache.PTUpdateRequest{
				CRName: sanitized,
				MIG:    entry.MIG,
			})
			sendPTUpdate(ptUpdateCh, sanitized)
		} else if crEntry.MIG != entry.MIG {
			// MIG changed (re-provision with different MIG) → push update.
			c.PushUpdate(sanitized, cache.PTUpdateRequest{
				CRName: sanitized,
				MIG:    entry.MIG,
			})
			sendPTUpdate(ptUpdateCh, sanitized)
		}

		// Enqueue the partition ID (= CR name in decoupled mode).
		enqueuePool(sanitized, q)
	}

	return handler.TypedFuncs[*corev1.Node, reconcile.Request]{
		CreateFunc: func(_ context.Context, e event.TypedCreateEvent[*corev1.Node], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			onAddOrUpdate(e.Object, q)
		},
		UpdateFunc: func(_ context.Context, e event.TypedUpdateEvent[*corev1.Node], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			onAddOrUpdate(e.ObjectNew, q)
		},
		GenericFunc: func(_ context.Context, e event.TypedGenericEvent[*corev1.Node], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			onAddOrUpdate(e.Object, q)
		},
		DeleteFunc: func(_ context.Context, e event.TypedDeleteEvent[*corev1.Node], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			n := e.Object
			if n == nil {
				return
			}
			onDelete(n.Name, q)
		},
	}
}

// sendPTUpdate sends a GenericEvent on the PT update channel to trigger
// reconciliation for the named CR. Non-blocking: drops the event if the
// channel buffer is full (reconcile will pick up the pending update on
// its next natural tick).
func sendPTUpdate(ch chan<- event.TypedGenericEvent[*partitionv1beta1.PartitionTree], crName string) {
	if ch == nil {
		return
	}
	evt := event.TypedGenericEvent[*partitionv1beta1.PartitionTree]{
		Object: &partitionv1beta1.PartitionTree{
			ObjectMeta: metav1.ObjectMeta{Name: crName},
		},
	}
	select {
	case ch <- evt:
	default:
		// Channel full — the reconciler will pick up pending updates
		// on its next scheduled tick.
	}
}
