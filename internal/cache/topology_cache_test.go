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

package cache

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
)

func mkEntry(pool, mig string, pids ...string) NodeEntry {
	return NodeEntry{
		NodePool:     pool,
		MIG:          mig,
		PartitionIDs: pids,
		Ready:        true,
	}
}

func TestUpsert_FirstNodeCreatesPool(t *testing.T) {
	c := New()
	oldPool, pool, created := c.Upsert("n1", mkEntry("poolA", "mig-link-A", "p0", "p1"))
	if oldPool != "" || pool != "poolA" || !created {
		t.Errorf("got oldPool=%q pool=%q created=%v want '',poolA,true", oldPool, pool, created)
	}
	entry, ok := c.Get("n1")
	if !ok || entry.NodePool != "poolA" {
		t.Errorf("Get: %+v ok=%v", entry, ok)
	}
	np, ok := c.NodePool("poolA")
	if !ok || np.MIG != "mig-link-A" || np.Nodes.Len() != 1 {
		t.Errorf("NodePool: %+v ok=%v", np, ok)
	}
	if got := c.NodesForPartition("poolA", "p0"); len(got) != 1 {
		t.Errorf("partition reverse index missing p0: %v", got)
	}
}

func TestUpsert_SecondNodeDoesNotMarkCreated(t *testing.T) {
	c := New()
	c.Upsert("n1", mkEntry("poolA", "mig", "p0"))
	oldPool, pool, created := c.Upsert("n2", mkEntry("poolA", "mig", "p0"))
	if oldPool != "" || pool != "poolA" || created {
		t.Errorf("got oldPool=%q pool=%q created=%v want '',poolA,false", oldPool, pool, created)
	}
	np, _ := c.NodePool("poolA")
	if np.Nodes.Len() != 2 {
		t.Errorf("pool node count: got %d want 2", np.Nodes.Len())
	}
	if got := c.NodesForPartition("poolA", "p0"); len(got) != 2 {
		t.Errorf("partition count: got %d want 2", len(got))
	}
}

func TestUpsert_NodeMigratesPool(t *testing.T) {
	c := New()
	oldPool, pool, created := c.Upsert("n1", mkEntry("poolA", "mig-A", "p0"))
	if oldPool != "" || pool != "poolA" || !created {
		t.Errorf("first upsert: got oldPool=%q pool=%q created=%v want '',poolA,true", oldPool, pool, created)
	}

	// Move n1 from poolA to poolB.
	oldPool, pool, created = c.Upsert("n1", mkEntry("poolB", "mig-B", "p0"))
	if oldPool != "poolA" || pool != "poolB" || !created {
		t.Errorf("second upsert: got oldPool=%q pool=%q created=%v want poolA,poolB,true", oldPool, pool, created)
	}

	// poolA should now be empty and dropped.
	if _, ok := c.NodePool("poolA"); ok {
		t.Errorf("poolA should be dropped after its only node moved")
	}
	if got := c.NodesForPartition("poolA", "p0"); len(got) != 0 {
		t.Errorf("poolA partition p0 should be empty, got %v", got)
	}

	// poolB should have n1.
	np, ok := c.NodePool("poolB")
	if !ok || np.Nodes.Len() != 1 || !np.Nodes.Has("n1") {
		t.Errorf("poolB invalid: %+v ok=%v", np, ok)
	}
	if got := c.NodesForPartition("poolB", "p0"); len(got) != 1 || got[0] != "n1" {
		t.Errorf("poolB partition p0 should have [n1], got %v", got)
	}
}

func TestUpsert_ReplacesOldIndexes(t *testing.T) {
	// A node changes which partition IDs it participates in. The old
	// ones must be scrubbed from the reverse index.
	c := New()
	c.Upsert("n1", mkEntry("poolA", "mig", "old-pid"))
	c.Upsert("n1", mkEntry("poolA", "mig", "new-pid"))
	if got := c.NodesForPartition("poolA", "old-pid"); len(got) != 0 {
		t.Errorf("old partition should be empty, got %v", got)
	}
	if got := c.NodesForPartition("poolA", "new-pid"); len(got) != 1 {
		t.Errorf("new partition should have 1 node, got %v", got)
	}
}

func TestDelete_NonLastNodeLeavesPoolIntact(t *testing.T) {
	c := New()
	c.Upsert("n1", mkEntry("poolA", "mig", "p0"))
	c.Upsert("n2", mkEntry("poolA", "mig", "p0"))
	pool, emptied := c.Delete("n1")
	if pool != "poolA" || emptied {
		t.Errorf("got pool=%q emptied=%v want poolA,false", pool, emptied)
	}
	np, ok := c.NodePool("poolA")
	if !ok || np.Nodes.Len() != 1 {
		t.Errorf("pool after delete: %+v ok=%v", np, ok)
	}
	if got := c.NodesForPartition("poolA", "p0"); len(got) != 1 {
		t.Errorf("partition index after delete: want 1, got %v", got)
	}
}

func TestDelete_LastNodeEmptiesPool(t *testing.T) {
	c := New()
	c.Upsert("n1", mkEntry("poolA", "mig", "p0", "p1"))
	pool, emptied := c.Delete("n1")
	if pool != "poolA" || !emptied {
		t.Errorf("got pool=%q emptied=%v want poolA,true", pool, emptied)
	}
	if _, ok := c.NodePool("poolA"); ok {
		t.Errorf("pool should be gone")
	}
	if got := c.NodesForPartition("poolA", "p0"); len(got) != 0 {
		t.Errorf("partition p0 should be gone, got %v", got)
	}
	if _, ok := c.Get("n1"); ok {
		t.Errorf("node should be gone")
	}
}

func TestDelete_UnknownNodeReturnsZero(t *testing.T) {
	c := New()
	pool, emptied := c.Delete("ghost")
	if pool != "" || emptied {
		t.Errorf("got pool=%q emptied=%v want '',false", pool, emptied)
	}
}

func TestReverseIndex_MultiPartitionPerNode(t *testing.T) {
	// v7x nodes sit in 5 partition IDs (one per hierarchy level).
	c := New()
	c.Upsert("n1", mkEntry("poolA", "mig", "p4x4x4", "p2x4x4", "p2x2x4", "p2x2x2", "p2x2x1"))
	for _, pid := range []string{"p4x4x4", "p2x4x4", "p2x2x4", "p2x2x2", "p2x2x1"} {
		if got := c.NodesForPartition("poolA", pid); len(got) != 1 {
			t.Errorf("partition %q missing node, got %v", pid, got)
		}
	}
	c.Delete("n1")
	for _, pid := range []string{"p4x4x4", "p2x4x4", "p2x2x4", "p2x2x2", "p2x2x1"} {
		if got := c.NodesForPartition("poolA", pid); len(got) != 0 {
			t.Errorf("partition %q should be empty after delete, got %v", pid, got)
		}
	}
}

func TestNodePoolClone_IsolatedFromCache(t *testing.T) {
	// Mutating the returned set must not affect the cache.
	c := New()
	c.Upsert("n1", mkEntry("poolA", "mig", "p0"))
	c.Upsert("n2", mkEntry("poolA", "mig", "p0"))
	np, _ := c.NodePool("poolA")
	np.Nodes.Delete("n1")
	np.Nodes.Delete("n2")

	again, _ := c.NodePool("poolA")
	got := again.Nodes.UnsortedList()
	sort.Strings(got)
	want := []string{"n1", "n2"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("cache mutated through clone: got %v want %v", got, want)
	}
}

// TestPartitionScopedByPool is the regression for the cross-pool
// contamination bug: two pools use the same partition ID label "root"
// and must not see each other's nodes in NodesForPartition.
func TestPartitionScopedByPool(t *testing.T) {
	c := New()
	c.Upsert("h0", mkEntry("healthy-pool", "healthy-mig", "root", "leaf"))
	c.Upsert("i0", mkEntry("incomplete-pool", "incomplete-mig", "root"))

	if got := c.NodesForPartition("healthy-pool", "root"); !reflect.DeepEqual(got, []string{"h0"}) {
		t.Errorf("healthy-pool root: %v, want [h0]", got)
	}
	if got := c.NodesForPartition("incomplete-pool", "root"); !reflect.DeepEqual(got, []string{"i0"}) {
		t.Errorf("incomplete-pool root: %v, want [i0]", got)
	}
	if got := c.NodesForPartition("healthy-pool", "leaf"); !reflect.DeepEqual(got, []string{"h0"}) {
		t.Errorf("healthy-pool leaf: %v, want [h0]", got)
	}
	if got := c.NodesForPartition("incomplete-pool", "leaf"); len(got) != 0 {
		t.Errorf("incomplete-pool leaf: %v, want empty", got)
	}
}

// TestPoolLeaves_Snapshot exercises the hot-path helper: one call
// returns every partition + readiness flag for a pool, avoiding N
// lock acquisitions per reconcile.
func TestPoolLeaves_Snapshot(t *testing.T) {
	c := New()
	c.Upsert("n1", NodeEntry{
		NodePool: "poolA", MIG: "mig",
		PartitionIDs: []string{"root", "leaf"}, Ready: true,
	})
	c.Upsert("n2", NodeEntry{
		NodePool: "poolA", MIG: "mig",
		PartitionIDs: []string{"root", "leaf"}, Ready: false,
	})
	// Contaminant in a different pool that must NOT leak into poolA's snapshot.
	c.Upsert("other", NodeEntry{
		NodePool: "poolB", MIG: "mig2",
		PartitionIDs: []string{"root"}, Ready: true,
	})

	snap := c.PoolLeaves("poolA")
	if !reflect.DeepEqual(snap.Partitions["root"], []string{"n1", "n2"}) {
		t.Errorf("root: %v", snap.Partitions["root"])
	}
	if !reflect.DeepEqual(snap.Partitions["leaf"], []string{"n1", "n2"}) {
		t.Errorf("leaf: %v", snap.Partitions["leaf"])
	}
	if snap.Ready["n1"] != true || snap.Ready["n2"] != false {
		t.Errorf("ready map: %v", snap.Ready)
	}
	if _, ok := snap.Ready["other"]; ok {
		t.Errorf("cross-pool leak: 'other' in poolA snapshot")
	}
}

// TestPoolLeaves_TPUData verifies PoolLeaves carries each node's
// google.com/tpu allocatable and accelerator type for the pool's nodes
// only, keyed like Ready.
func TestPoolLeaves_TPUData(t *testing.T) {
	c := New()
	c.Upsert("n1", NodeEntry{
		NodePool: "poolA", MIG: "mig", AcceleratorType: "tpu7x",
		PartitionIDs: []string{"leaf"}, Ready: true, TPUAllocatable: 4,
	})
	c.Upsert("n2", NodeEntry{
		NodePool: "poolA", MIG: "mig", AcceleratorType: "tpu7x",
		PartitionIDs: []string{"leaf"}, Ready: true,
	})
	c.Upsert("other", NodeEntry{
		NodePool: "poolB", MIG: "mig2", AcceleratorType: "tpu-v6e-slice",
		PartitionIDs: []string{"leaf"}, Ready: true, TPUAllocatable: 4,
	})

	snap := c.PoolLeaves("poolA")
	want := map[string]NodeTPU{
		"n1": {Allocatable: 4, AcceleratorType: "tpu7x"},
		"n2": {Allocatable: 0, AcceleratorType: "tpu7x"},
	}
	if !reflect.DeepEqual(snap.TPU, want) {
		t.Errorf("TPU map = %+v, want %+v", snap.TPU, want)
	}
	if len(snap.TPU) != len(snap.Ready) {
		t.Errorf("TPU map has %d keys, Ready has %d", len(snap.TPU), len(snap.Ready))
	}

	// A later Upsert replaces the allocatable value.
	c.Upsert("n2", NodeEntry{
		NodePool: "poolA", MIG: "mig", AcceleratorType: "tpu7x",
		PartitionIDs: []string{"leaf"}, Ready: true, TPUAllocatable: 4,
	})
	if got := c.PoolLeaves("poolA").TPU["n2"].Allocatable; got != 4 {
		t.Errorf("n2 allocatable after update = %d, want 4", got)
	}
}

// TestPoolLeaves_UnknownPool returns a zero-valued snapshot, not nil.
func TestPoolLeaves_UnknownPool(t *testing.T) {
	c := New()
	snap := c.PoolLeaves("ghost")
	if snap.Partitions == nil || snap.Ready == nil || snap.TPU == nil {
		t.Errorf("zero snapshot should have non-nil maps: %+v", snap)
	}
	if len(snap.Partitions) != 0 || len(snap.Ready) != 0 {
		t.Errorf("ghost pool non-empty: %+v", snap)
	}
}

// TestConcurrentUpsertDelete exercises concurrent Upsert and Delete on
// overlapping nodes/pools. Run with -race to detect data races.
func TestConcurrentUpsertDelete(t *testing.T) {
	c := New()
	const (
		numWorkers   = 8
		opsPerWorker = 200
		numPools     = 3
		nodesPerPool = 5
	)

	var wg sync.WaitGroup

	// Half the workers upsert, half delete, all on overlapping pools/nodes.
	for w := range numWorkers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range opsPerWorker {
				pool := fmt.Sprintf("pool-%d", i%numPools)
				node := fmt.Sprintf("node-%d-%d", i%nodesPerPool, id)
				pid := fmt.Sprintf("p-%d", i%4)

				if id%2 == 0 {
					c.Upsert(node, NodeEntry{
						NodePool:     pool,
						MIG:          "mig-" + pool,
						PartitionIDs: []string{pid, "root"},
						Ready:        i%3 == 0,
					})
				} else {
					c.Delete(node)
				}
			}
		}(w)
	}
	wg.Wait()

	// Final state must be self-consistent: Len() returns sane values.
	nodes, pools := c.Len()
	if nodes < 0 || pools < 0 {
		t.Errorf("negative Len() after concurrent ops: nodes=%d pools=%d", nodes, pools)
	}
	// Every node still in the cache must resolve to a valid pool.
	for i := range nodesPerPool * numWorkers {
		for w := range numWorkers {
			node := fmt.Sprintf("node-%d-%d", i%nodesPerPool, w)
			if e, ok := c.Get(node); ok {
				if _, poolOK := c.NodePool(e.NodePool); !poolOK {
					t.Errorf("node %q cached but pool %q missing", node, e.NodePool)
				}
			}
		}
	}
}

// TestConcurrentPoolLeavesWhileUpsert has one goroutine continuously
// calling PoolLeaves while another does Upserts. The goal is to verify
// no panics and that every returned snapshot is self-consistent (sorted
// slices, non-nil maps).
func TestConcurrentPoolLeavesWhileUpsert(t *testing.T) {
	c := New()
	const pool = "concurrent-pool"
	const iterations = 500

	var wg sync.WaitGroup

	// Writer goroutine: upsert nodes into the pool.
	wg.Go(func() {
		for i := range iterations {
			node := fmt.Sprintf("node-%d", i%20)
			c.Upsert(node, NodeEntry{
				NodePool:     pool,
				MIG:          "mig-link",
				PartitionIDs: []string{"root", fmt.Sprintf("leaf-%d", i%5)},
				Ready:        i%2 == 0,
			})
		}
	})

	// Reader goroutine: continuously snapshot PoolLeaves.
	wg.Go(func() {
		for i := range iterations {
			snap := c.PoolLeaves(pool)
			// Snapshot must be self-consistent: non-nil maps.
			if snap.Partitions == nil {
				t.Errorf("iteration %d: Partitions map is nil", i)
				return
			}
			if snap.Ready == nil {
				t.Errorf("iteration %d: Ready map is nil", i)
				return
			}
			// Every partition's node list must be sorted.
			for pid, names := range snap.Partitions {
				if !sort.StringsAreSorted(names) {
					t.Errorf("iteration %d: partition %q not sorted: %v", i, pid, names)
					return
				}
			}
			// Every node in Ready should appear in at least one partition.
			allPartNodes := map[string]bool{}
			for _, names := range snap.Partitions {
				for _, n := range names {
					allPartNodes[n] = true
				}
			}
			for name := range snap.Ready {
				if !allPartNodes[name] {
					t.Errorf("iteration %d: node %q in Ready but not in any partition", i, name)
					return
				}
			}
		}
	})

	wg.Wait()
}

// ---------------------------------------------------------------------------
// PoolTopologyBlock tests — slice-scheduler integration readiness
// ---------------------------------------------------------------------------

// TestPoolTopologyBlock_Found verifies the happy path: a pool with one
// node that carries the topology block label returns that value.
func TestPoolTopologyBlock_Found(t *testing.T) {
	c := New()
	c.Upsert("n1", NodeEntry{
		NodePool:      "poolA",
		MIG:           "mig",
		TopologyBlock: "block-42",
		PartitionIDs:  []string{"p0"},
		Ready:         true,
	})
	got := c.PoolTopologyBlock("poolA")
	if got != "block-42" {
		t.Errorf("PoolTopologyBlock = %q, want %q", got, "block-42")
	}
}

// TestPoolTopologyBlock_UnknownPool returns "" for a pool that was
// never inserted.
func TestPoolTopologyBlock_UnknownPool(t *testing.T) {
	c := New()
	got := c.PoolTopologyBlock("ghost-pool")
	if got != "" {
		t.Errorf("PoolTopologyBlock(ghost) = %q, want empty", got)
	}
}

// TestPoolTopologyBlock_NoBlockLabel returns "" when all nodes in the
// pool lack the topology block label.
func TestPoolTopologyBlock_NoBlockLabel(t *testing.T) {
	c := New()
	c.Upsert("n1", NodeEntry{
		NodePool:      "poolA",
		MIG:           "mig",
		TopologyBlock: "", // no block label
		PartitionIDs:  []string{"p0"},
		Ready:         true,
	})
	c.Upsert("n2", NodeEntry{
		NodePool:      "poolA",
		MIG:           "mig",
		TopologyBlock: "",
		PartitionIDs:  []string{"p0"},
		Ready:         true,
	})
	got := c.PoolTopologyBlock("poolA")
	if got != "" {
		t.Errorf("PoolTopologyBlock = %q, want empty (no nodes carry block)", got)
	}
}

// TestPoolTopologyBlock_MixedNodes returns the block from whichever
// node carries it, even when some nodes lack the label (e.g. during
// node startup race).
func TestPoolTopologyBlock_MixedNodes(t *testing.T) {
	c := New()
	c.Upsert("n-without", NodeEntry{
		NodePool:      "poolA",
		MIG:           "mig",
		TopologyBlock: "",
		PartitionIDs:  []string{"p0"},
		Ready:         true,
	})
	c.Upsert("n-with", NodeEntry{
		NodePool:      "poolA",
		MIG:           "mig",
		TopologyBlock: "block-7",
		PartitionIDs:  []string{"p0"},
		Ready:         true,
	})
	got := c.PoolTopologyBlock("poolA")
	if got != "block-7" {
		t.Errorf("PoolTopologyBlock = %q, want %q", got, "block-7")
	}
}

// TestPoolTopologyBlock_CrossPoolIsolation verifies that topology block
// values from one pool do not leak into lookups for another pool.
func TestPoolTopologyBlock_CrossPoolIsolation(t *testing.T) {
	c := New()
	c.Upsert("n1", NodeEntry{
		NodePool:      "poolA",
		MIG:           "mig-a",
		TopologyBlock: "block-A",
		PartitionIDs:  []string{"p0"},
		Ready:         true,
	})
	c.Upsert("n2", NodeEntry{
		NodePool:      "poolB",
		MIG:           "mig-b",
		TopologyBlock: "block-B",
		PartitionIDs:  []string{"p0"},
		Ready:         true,
	})
	if got := c.PoolTopologyBlock("poolA"); got != "block-A" {
		t.Errorf("poolA: got %q want block-A", got)
	}
	if got := c.PoolTopologyBlock("poolB"); got != "block-B" {
		t.Errorf("poolB: got %q want block-B", got)
	}
}

// TestPoolTopologyBlock_AfterDelete returns "" once all nodes with the
// block label are removed from the pool.
func TestPoolTopologyBlock_AfterDelete(t *testing.T) {
	c := New()
	c.Upsert("n1", NodeEntry{
		NodePool:      "poolA",
		MIG:           "mig",
		TopologyBlock: "block-42",
		PartitionIDs:  []string{"p0"},
		Ready:         true,
	})
	if got := c.PoolTopologyBlock("poolA"); got != "block-42" {
		t.Fatalf("before delete: got %q", got)
	}
	c.Delete("n1")
	if got := c.PoolTopologyBlock("poolA"); got != "" {
		t.Errorf("after delete: got %q, want empty (pool gone)", got)
	}
}

// TestPoolTopologyBlock_ConcurrentSafety exercises PoolTopologyBlock
// under concurrent Upsert/Delete operations. Run with -race.
func TestPoolTopologyBlock_ConcurrentSafety(t *testing.T) {
	c := New()
	const iterations = 500

	var wg sync.WaitGroup

	// Writer: continuously upsert nodes with block labels.
	wg.Go(func() {
		for i := range iterations {
			node := fmt.Sprintf("node-%d", i%10)
			c.Upsert(node, NodeEntry{
				NodePool:      "poolA",
				MIG:           "mig",
				TopologyBlock: fmt.Sprintf("block-%d", i%3),
				PartitionIDs:  []string{"p0"},
				Ready:         true,
			})
		}
	})

	// Reader: continuously query PoolTopologyBlock.
	wg.Go(func() {
		for range iterations {
			// Should not panic; result is either "" or a valid block string.
			got := c.PoolTopologyBlock("poolA")
			_ = got
		}
	})

	wg.Wait()
}

// --- Decoupled lifecycle cache tests ---

func TestSetPartitionCR(t *testing.T) {
	c := New()
	c.SetPartitionCR("abc123", "abc123", "mig-link")
	e, ok := c.PartitionCR("abc123")
	if !ok || e.CRName != "abc123" || e.MIG != "mig-link" {
		t.Errorf("got %+v ok=%v", e, ok)
	}
}

func TestPartitionCR_NotFound(t *testing.T) {
	c := New()
	_, ok := c.PartitionCR("nonexistent")
	if ok {
		t.Error("expected not found")
	}
}

func TestSetPartitionCR_Overwrite(t *testing.T) {
	c := New()
	c.SetPartitionCR("abc", "abc", "mig-old")
	c.SetPartitionCR("abc", "abc", "mig-new")
	e, ok := c.PartitionCR("abc")
	if !ok || e.MIG != "mig-new" {
		t.Errorf("overwrite failed: %+v", e)
	}
}

func TestPoolRootPartitionID(t *testing.T) {
	c := New()
	c.SetPoolRootPartitionID("my-pool", "abc123")
	id, ok := c.PoolRootPartitionID("my-pool")
	if !ok || id != "abc123" {
		t.Errorf("got id=%q ok=%v", id, ok)
	}
}

func TestPoolRootPartitionID_NotFound(t *testing.T) {
	c := New()
	_, ok := c.PoolRootPartitionID("unknown")
	if ok {
		t.Error("expected not found")
	}
}

func TestPushUpdate_Single(t *testing.T) {
	c := New()
	c.PushUpdate("cr1", PTUpdateRequest{CRName: "cr1", Drained: true})
	updates := c.DrainUpdates("cr1")
	if len(updates) != 1 || !updates[0].Drained {
		t.Errorf("got %+v", updates)
	}
}

func TestPushUpdate_Multiple(t *testing.T) {
	c := New()
	c.PushUpdate("cr1", PTUpdateRequest{CRName: "cr1", MIG: "mig-1"})
	c.PushUpdate("cr1", PTUpdateRequest{CRName: "cr1", Drained: true})
	updates := c.DrainUpdates("cr1")
	if len(updates) != 2 {
		t.Fatalf("want 2 updates, got %d", len(updates))
	}
	if updates[0].MIG != "mig-1" {
		t.Errorf("first update: %+v", updates[0])
	}
	if !updates[1].Drained {
		t.Errorf("second update: %+v", updates[1])
	}
}

func TestDrainUpdates_Empty(t *testing.T) {
	c := New()
	updates := c.DrainUpdates("cr1")
	if len(updates) != 0 {
		t.Errorf("expected empty, got %+v", updates)
	}
}

func TestDrainUpdates_ClearsQueue(t *testing.T) {
	c := New()
	c.PushUpdate("cr1", PTUpdateRequest{CRName: "cr1", Drained: true})
	c.DrainUpdates("cr1")
	updates := c.DrainUpdates("cr1")
	if len(updates) != 0 {
		t.Errorf("second drain should be empty, got %+v", updates)
	}
}

func TestPartitionCR_Concurrent(t *testing.T) {
	c := New()
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("part-%d", i)
			c.SetPartitionCR(id, id, "mig")
			c.PartitionCR(id)
		}(i)
	}
	wg.Wait()
}

func TestPushDrainUpdate_Concurrent(t *testing.T) {
	c := New()
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cr := fmt.Sprintf("cr-%d", i)
			c.PushUpdate(cr, PTUpdateRequest{CRName: cr})
			c.DrainUpdates(cr)
		}(i)
	}
	wg.Wait()
}

func TestRootPartitionPool(t *testing.T) {
	c := New()
	// Must populate nodePools via Upsert so the pool exists for validation.
	c.Upsert("n1", mkEntry("my-pool", "mig", "p0"))
	c.SetPoolRootPartitionID("my-pool", "abc-123")
	pool, ok := c.RootPartitionPool("abc-123")
	if !ok || pool != "my-pool" {
		t.Errorf("got pool=%q ok=%v", pool, ok)
	}
}

func TestRootPartitionPool_NotFound(t *testing.T) {
	c := New()
	_, ok := c.RootPartitionPool("unknown")
	if ok {
		t.Error("expected not found")
	}
}

func TestDeletePartitionCR(t *testing.T) {
	c := New()
	c.SetPartitionCR("abc", "abc", "mig")
	c.DeletePartitionCR("abc")
	_, ok := c.PartitionCR("abc")
	if ok {
		t.Error("entry should be deleted")
	}
}

func TestDeletePartitionCR_Nonexistent(t *testing.T) {
	c := New()
	// Should not panic.
	c.DeletePartitionCR("nonexistent")
}

// TestRootPartitionPool_StaleAfterPoolDrain verifies that
// RootPartitionPool does NOT return a pool that has been drained
// (all nodes removed), even if poolToRootPartition still has the entry.
func TestRootPartitionPool_StaleAfterPoolDrain(t *testing.T) {
	c := New()
	c.Upsert("n1", mkEntry("my-pool", "mig", "p0"))
	c.SetPoolRootPartitionID("my-pool", "abc-123")

	// Before drain: should find it.
	pool, ok := c.RootPartitionPool("abc-123")
	if !ok || pool != "my-pool" {
		t.Fatalf("before drain: got pool=%q ok=%v, want my-pool/true", pool, ok)
	}

	// Drain the pool by deleting its only node.
	c.Delete("n1")

	// After drain: should NOT return stale pool.
	_, ok = c.RootPartitionPool("abc-123")
	if ok {
		t.Error("after drain: expected not found, but got stale pool")
	}
}

// TestDeletePoolRootPartitionID verifies cleanup of the pool→partition
// mapping.
func TestDeletePoolRootPartitionID(t *testing.T) {
	c := New()
	c.SetPoolRootPartitionID("pool-a", "part-1")
	c.SetPoolRootPartitionID("pool-b", "part-2")

	c.DeletePoolRootPartitionID("pool-a")

	// pool-a should be gone.
	_, ok := c.PoolRootPartitionID("pool-a")
	if ok {
		t.Error("pool-a should be deleted")
	}
	// pool-b should remain.
	id, ok := c.PoolRootPartitionID("pool-b")
	if !ok || id != "part-2" {
		t.Errorf("pool-b: got id=%q ok=%v, want part-2/true", id, ok)
	}
}

// TestDeletePoolRootPartitionID_Nonexistent verifies no panic.
func TestDeletePoolRootPartitionID_Nonexistent(t *testing.T) {
	c := New()
	c.DeletePoolRootPartitionID("nonexistent") // should not panic
}

// TestRootPartitionPool_RecoversAfterReProvision verifies that after a
// pool drains and a new pool is created in the same partition, the
// reverse lookup returns the new pool.
func TestRootPartitionPool_RecoversAfterReProvision(t *testing.T) {
	c := New()
	// Initial pool.
	c.Upsert("n1", mkEntry("pool-v1", "mig-1", "p0"))
	c.SetPoolRootPartitionID("pool-v1", "abc-123")

	// Drain.
	c.Delete("n1")
	c.DeletePoolRootPartitionID("pool-v1")

	// Should not find stale entry.
	_, ok := c.RootPartitionPool("abc-123")
	if ok {
		t.Fatal("expected not found after drain+cleanup")
	}

	// Re-provision with new pool name.
	c.Upsert("n2", mkEntry("pool-v2", "mig-2", "p0"))
	c.SetPoolRootPartitionID("pool-v2", "abc-123")

	pool, ok := c.RootPartitionPool("abc-123")
	if !ok || pool != "pool-v2" {
		t.Errorf("after re-provision: got pool=%q ok=%v, want pool-v2/true", pool, ok)
	}
}
