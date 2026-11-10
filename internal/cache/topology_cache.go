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

// Package cache holds the in-memory three-map topology cache that the
// node informer feeds. Its invariant is that the nodes, nodePools and
// partitions maps are always consistent with each other under the same
// mutex.
//
// Scale note: the partitions index is scoped by (pool, partitionID)
// because partition-ID labels are *not* globally unique across pools —
// two different MIGs can legitimately use the same "root" / "leaf"
// strings. A global key would cross-contaminate leaves between pools
// and would also cause every refreshLeaves call to scan nodes from
// every pool cluster-wide (O(N_total) per reconcile).
package cache

import (
	"sort"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

// NodeEntry is the snapshot cached for one Node. MIG and
// PartitionIDs are snapshotted at Upsert time because delete events may
// not carry the full object and the reconciler still needs them to
// clean indexes. TPUAllocatable is the node's google.com/tpu allocatable
// (0 when absent, and always 0 unless the node cache keeps allocatable,
// i.e. with --enable-tpu-allocatable-readiness).
//
// For node.kubernetes.io/{not-ready,unreachable}:NoExecute, Taints holds the
// start of the node's current outage episode rather than the raw TimeAdded:
// see Upsert.
type NodeEntry struct {
	NodePool        string
	MIG             string
	AcceleratorType string
	TopologyBlock   string
	PartitionIDs    []string
	Ready           bool
	TPUAllocatable  int64
	Taints          []corev1.Taint

	// nodeHealthRawTimeAdded records the TimeAdded each node-health NoExecute
	// taint had on the Node object at the last Upsert, so the next Upsert can
	// tell an unchanged taint from one that was removed and re-added.
	nodeHealthRawTimeAdded map[string]metav1.Time
}

// NodePoolEntry is the per-pool view: which MIG it targets, which TPU
// family it hosts (derived from the gke-tpu-accelerator label at Upsert
// time), and which nodes currently belong to it.
type NodePoolEntry struct {
	MIG             string
	AcceleratorType string
	Nodes           sets.Set[string]
}

// PartitionCREntry records the mapping between a root partition ID and
// its PartitionTree CR. Written by the reconciler after CR creation;
// read by the node informer to avoid k8s API calls in the hot path.
type PartitionCREntry struct {
	CRName string
	MIG    string
}

// PTUpdateRequest is queued by the node informer and drained by the
// reconciler. It carries the details of a pending annotation change
// (MIG swap or drain) that should be applied to the named CR.
type PTUpdateRequest struct {
	CRName  string
	MIG     string
	Drained bool
}

// Cache is the three-map structure. All mutations take the write lock
// and update every relevant map before releasing it.
type Cache struct {
	mu        sync.RWMutex
	nodes     map[string]NodeEntry
	nodePools map[string]NodePoolEntry
	// partitions is pool-scoped: partitions[pool][partitionID] -> nodes.
	// Keeping the outer key lets the reconciler grab all leaves of one
	// pool in a single locked lookup (see PoolLeaves) and avoids
	// collisions between pools that legitimately share partition-ID
	// strings like "root"/"leaf".
	partitions map[string]map[string]sets.Set[string]

	// partitionCRs maps root-partition-ID → CR entry. This is the ONLY
	// cache field written by the reconciler (all others are written by
	// the node informer). It enables the node informer to check CR
	// existence without calling the k8s API.
	partitionCRs map[string]PartitionCREntry

	// poolToRootPartition maps poolName → root partition ID. Populated
	// during Upsert from node labels. Survives node deletion so the
	// delete handler can look up the partition ID after the node entry
	// is gone.
	poolToRootPartition map[string]string

	// pendingUpdates maps CR name → queued updates. The node informer
	// pushes updates; the reconciler drains them.
	pendingUpdates map[string][]PTUpdateRequest
}

// New returns an empty Cache ready for use.
func New() *Cache {
	return &Cache{
		nodes:               map[string]NodeEntry{},
		nodePools:           map[string]NodePoolEntry{},
		partitions:          map[string]map[string]sets.Set[string]{},
		partitionCRs:        map[string]PartitionCREntry{},
		poolToRootPartition: map[string]string{},
		pendingUpdates:      map[string][]PTUpdateRequest{},
	}
}

// Upsert inserts or replaces a node entry. It returns the old node
// pool name (if the node was previously cached under a different
// pool), the new node pool name the entry belongs to, and created=true
// iff this call caused the new pool to appear for the first time
// (the reconciler uses this to create the PartitionTree CR).
//
// Upsert also keeps a node's outage clock running when node-lifecycle-controller
// swaps node.kubernetes.io/not-ready:NoExecute for node.kubernetes.io/unreachable:NoExecute
// (or back): the swap stamps the new taint with a fresh TimeAdded, so the new
// taint carries the earlier start the other key had on this same node. A taint
// whose key was already present keeps its carried start while its raw TimeAdded
// is unchanged; a newer raw TimeAdded on the same key means the taint was removed
// and re-added (node-lifecycle-controller never advances an existing taint's
// TimeAdded), so it starts a fresh episode. Continuity is tracked per node only,
// and only in memory: a controller restart mid-episode restarts the clock at the
// current raw TimeAdded.
func (c *Cache) Upsert(nodeName string, e NodeEntry) (oldPool, nodePool string, created bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	old, hadOld := c.nodes[nodeName]
	if hadOld {
		if old.NodePool != e.NodePool {
			oldPool = old.NodePool
		}
		c.removeFromIndexesLocked(nodeName, old)
	}
	e.Taints, e.nodeHealthRawTimeAdded = carryNodeHealthEpisode(old, e.Taints)
	c.nodes[nodeName] = e

	pool, exists := c.nodePools[e.NodePool]
	if !exists {
		pool = NodePoolEntry{Nodes: sets.New[string]()}
		created = true
	}
	pool.MIG = e.MIG
	// Last writer wins on AcceleratorType. In practice every node in a
	// MIG reports the same value, so flapping is not expected.
	if e.AcceleratorType != "" {
		pool.AcceleratorType = e.AcceleratorType
	}
	pool.Nodes.Insert(nodeName)
	c.nodePools[e.NodePool] = pool

	poolParts := c.partitions[e.NodePool]
	if poolParts == nil {
		poolParts = map[string]sets.Set[string]{}
		c.partitions[e.NodePool] = poolParts
	}
	for _, pid := range e.PartitionIDs {
		set := poolParts[pid]
		if set == nil {
			set = sets.New[string]()
			poolParts[pid] = set
		}
		set.Insert(nodeName)
	}

	return oldPool, e.NodePool, created
}

// Delete removes a node entry. It returns the node pool name the node
// belonged to, and emptied=true iff the pool is now gone (the
// reconciler uses this to delete the PartitionTree CR). If the node was
// not cached at all, both return values are zero.
func (c *Cache) Delete(nodeName string) (nodePool string, emptied bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.nodes[nodeName]
	if !ok {
		return "", false
	}
	delete(c.nodes, nodeName)
	c.removeFromIndexesLocked(nodeName, e)
	_, stillExists := c.nodePools[e.NodePool]
	return e.NodePool, !stillExists
}

// removeFromIndexesLocked removes a node from the nodePools and
// partitions indexes based on a previously cached entry. Caller must
// hold the write lock. Empty pools / partitions are dropped.
func (c *Cache) removeFromIndexesLocked(nodeName string, e NodeEntry) {
	if pool, ok := c.nodePools[e.NodePool]; ok {
		pool.Nodes.Delete(nodeName)
		if pool.Nodes.Len() == 0 {
			delete(c.nodePools, e.NodePool)
		} else {
			c.nodePools[e.NodePool] = pool
		}
	}
	if poolParts, ok := c.partitions[e.NodePool]; ok {
		for _, pid := range e.PartitionIDs {
			if set, ok := poolParts[pid]; ok {
				set.Delete(nodeName)
				if set.Len() == 0 {
					delete(poolParts, pid)
				}
			}
		}
		if len(poolParts) == 0 {
			delete(c.partitions, e.NodePool)
		}
	}
}

// Get returns a snapshot of the node entry for nodeName.
func (c *Cache) Get(nodeName string) (NodeEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.nodes[nodeName]
	return e, ok
}

// NodePool returns a snapshot of the pool entry. The returned set is a
// clone — callers may mutate it freely.
func (c *Cache) NodePool(name string) (NodePoolEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	p, ok := c.nodePools[name]
	if !ok {
		return NodePoolEntry{}, false
	}
	return NodePoolEntry{
		MIG:             p.MIG,
		AcceleratorType: p.AcceleratorType,
		Nodes:           p.Nodes.Clone(),
	}, true
}

// LeafSnapshot is the per-pool snapshot the reconciler uses on the hot
// path: one lock acquisition produces the full partition→nodes map plus
// a node-name→Ready lookup. This replaces N per-leaf calls, each of
// which would otherwise take the RLock and clone a set.
type LeafSnapshot struct {
	// Partitions maps partition-ID → sorted node names belonging to this
	// pool's partition. Keys cover every partition the pool's nodes have
	// ever participated in; a leaf walker only reads the ones it cares
	// about.
	Partitions map[string][]string
	// Ready maps node name → readiness for every node in this pool.
	// Any node missing from the map can be treated as Unknown.
	Ready map[string]bool
	// Taints maps node name → taints for every node in this pool.
	Taints map[string][]corev1.Taint
	// TPU maps node name → TPU chip data for every node in this pool.
	// It has the same keys as Ready.
	TPU map[string]NodeTPU
}

// NodeTPU is the per-node TPU chip data carried by LeafSnapshot. The
// reconciler turns AcceleratorType into the expected chips per host.
type NodeTPU struct {
	Allocatable     int64
	AcceleratorType string
}

// PoolLeaves returns a single-shot snapshot of all partition→nodes
// mappings plus per-node readiness, taints and TPU chip data for pool.
// The returned slices are sorted; the returned maps are owned by the
// caller (safe to mutate).
// Returns a zero-valued snapshot if the pool is unknown.
func (c *Cache) PoolLeaves(pool string) LeafSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	snap := LeafSnapshot{
		Partitions: map[string][]string{},
		Ready:      map[string]bool{},
		Taints:     map[string][]corev1.Taint{},
		TPU:        map[string]NodeTPU{},
	}
	poolParts, ok := c.partitions[pool]
	if !ok {
		return snap
	}
	for pid, set := range poolParts {
		names := set.UnsortedList()
		sort.Strings(names)
		snap.Partitions[pid] = names
	}
	if pe, ok := c.nodePools[pool]; ok {
		for name := range pe.Nodes {
			if n, ok := c.nodes[name]; ok {
				snap.Ready[name] = n.Ready
				snap.TPU[name] = NodeTPU{
					Allocatable:     n.TPUAllocatable,
					AcceleratorType: n.AcceleratorType,
				}
				if len(n.Taints) > 0 {
					// Deep-copy taints (including TimeAdded pointers) to avoid sharing mutated state.
					taints := make([]corev1.Taint, len(n.Taints))
					for i := range n.Taints {
						taints[i] = *n.Taints[i].DeepCopy()
					}
					snap.Taints[name] = taints
				}
			}
		}
	}
	return snap
}

// NodesForPartition returns the sorted list of node names that belong
// to (pool, partitionID). Empty slice if either the pool or the
// partition is unknown. This helper is retained for callers that need
// a single partition; reconcilers that walk every leaf of a pool
// should use PoolLeaves to avoid N lock acquisitions.
func (c *Cache) NodesForPartition(pool, partitionID string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	poolParts, ok := c.partitions[pool]
	if !ok {
		return nil
	}
	set, ok := poolParts[partitionID]
	if !ok {
		return nil
	}
	names := set.UnsortedList()
	sort.Strings(names)
	return names
}

// Len returns the number of cached nodes and pools. Used by metrics
// and scale tests; takes the read lock briefly.
func (c *Cache) Len() (nodes, pools int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.nodes), len(c.nodePools)
}

// PoolTopologyBlock returns the cloud.google.com/gce-topology-block value
// for any node in the pool. All nodes in a pool share the same block
// (they belong to the same GCE superpod). Returns "" if the pool is
// unknown or no node carries the label.
func (c *Cache) PoolTopologyBlock(pool string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	pe, ok := c.nodePools[pool]
	if !ok {
		return ""
	}
	for name := range pe.Nodes {
		if n, ok := c.nodes[name]; ok && n.TopologyBlock != "" {
			return n.TopologyBlock
		}
	}
	return ""
}

// SetPartitionCR records the mapping from a root partition ID to its
// CR name and MIG. Called by the reconciler after CR creation and
// during startup CR list.
func (c *Cache) SetPartitionCR(partitionID, crName, mig string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.partitionCRs[partitionID] = PartitionCREntry{CRName: crName, MIG: mig}
}

// PartitionCR returns the CR entry for a root partition ID.
func (c *Cache) PartitionCR(partitionID string) (PartitionCREntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.partitionCRs[partitionID]
	return e, ok
}

// SetPoolRootPartitionID records the mapping from a pool name to its
// root partition ID. Called by the node informer during Upsert.
func (c *Cache) SetPoolRootPartitionID(poolName, partitionID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.poolToRootPartition[poolName] = partitionID
}

// PoolRootPartitionID returns the root partition ID for a pool.
func (c *Cache) PoolRootPartitionID(poolName string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	id, ok := c.poolToRootPartition[poolName]
	return id, ok
}

// PushUpdate queues a pending update for the named CR. The update
// will be drained by the reconciler on its next pass.
func (c *Cache) PushUpdate(crName string, update PTUpdateRequest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pendingUpdates[crName] = append(c.pendingUpdates[crName], update)
}

// DrainUpdates atomically returns and clears all pending updates for
// the named CR.
func (c *Cache) DrainUpdates(crName string) []PTUpdateRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	updates := c.pendingUpdates[crName]
	delete(c.pendingUpdates, crName)
	return updates
}

// RootPartitionPool performs the reverse lookup of PoolRootPartitionID:
// given a root partition ID, it returns the pool name that maps to it.
// This is O(n) in the number of pools but the map is small (one entry
// per pool in the cluster).
func (c *Cache) RootPartitionPool(partitionID string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for pool, pid := range c.poolToRootPartition {
		if pid == partitionID {
			// Validate the pool still exists to avoid returning stale
			// entries from pools that have been drained.
			if _, exists := c.nodePools[pool]; exists {
				return pool, true
			}
		}
	}
	return "", false
}

// DeletePartitionCR removes the partition CR entry for the given
// partition ID. Called when a decoupled CR is deleted to prevent stale
// cache entries.
func (c *Cache) DeletePartitionCR(partitionID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.partitionCRs, partitionID)
}

// DeletePoolRootPartitionID removes the pool→partition mapping for a
// drained pool. Called by the node informer delete handler after the
// last node in a pool is removed.
func (c *Cache) DeletePoolRootPartitionID(poolName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.poolToRootPartition, poolName)
}

// isNodeHealthNoExecute reports whether t is one of the NoExecute taints that
// node-lifecycle-controller swaps between as a node goes NotReady/Unreachable.
func isNodeHealthNoExecute(t *corev1.Taint) bool {
	return t.Effect == corev1.TaintEffectNoExecute &&
		(t.Key == corev1.TaintNodeNotReady || t.Key == corev1.TaintNodeUnreachable)
}

// findNodeHealthNoExecute returns the node-health NoExecute taint with key, or nil.
func findNodeHealthNoExecute(taints []corev1.Taint, key string) *corev1.Taint {
	for i := range taints {
		if taints[i].Key == key && isNodeHealthNoExecute(&taints[i]) {
			return &taints[i]
		}
	}
	return nil
}

// carryNodeHealthEpisode returns taints with each node-health NoExecute taint's
// TimeAdded moved back to the start of the node's current outage episode (see
// Upsert), plus the raw TimeAdded of those taints for the next Upsert. The input
// slice is never modified.
func carryNodeHealthEpisode(old NodeEntry, taints []corev1.Taint) ([]corev1.Taint, map[string]metav1.Time) {
	var raw map[string]metav1.Time
	out := taints
	copied := false
	for i := range taints {
		t := &taints[i]
		if !isNodeHealthNoExecute(t) || t.TimeAdded == nil {
			continue
		}
		if raw == nil {
			raw = make(map[string]metav1.Time, 2)
		}
		raw[t.Key] = *t.TimeAdded
		start := episodeStart(old, t)
		if start == nil || !start.Before(t.TimeAdded) {
			continue
		}
		if !copied {
			out = make([]corev1.Taint, len(taints))
			for j := range taints {
				out[j] = *taints[j].DeepCopy()
			}
			copied = true
		}
		out[i].TimeAdded = start
	}
	return out, raw
}

// episodeStart returns the earlier episode start that node-health taint t
// inherits from the node's previous entry, or nil if t starts a new episode.
func episodeStart(old NodeEntry, t *corev1.Taint) *metav1.Time {
	if prev := findNodeHealthNoExecute(old.Taints, t.Key); prev != nil {
		// Same key: keep its (possibly carried) start while the raw TimeAdded
		// is unchanged. A different raw TimeAdded means the taint was removed
		// and re-added in between, which is a new episode.
		if r, ok := old.nodeHealthRawTimeAdded[t.Key]; ok && r.Equal(t.TimeAdded) {
			return prev.TimeAdded
		}
		return nil
	}
	other := corev1.TaintNodeUnreachable
	if t.Key == corev1.TaintNodeUnreachable {
		other = corev1.TaintNodeNotReady
	}
	if prev := findNodeHealthNoExecute(old.Taints, other); prev != nil {
		// not-ready <-> unreachable swap on this node: same episode.
		return prev.TimeAdded
	}
	return nil
}
