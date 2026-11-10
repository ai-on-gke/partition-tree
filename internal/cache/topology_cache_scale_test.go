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
	"runtime"
	"testing"
	"time"
)

func BenchmarkUpsert500MIGs(b *testing.B) {
	const pools = 500
	const nodesPerPool = 16
	entries := make([]struct {
		name  string
		entry NodeEntry
	}, 0, pools*nodesPerPool)
	for p := range pools {
		pool := fmt.Sprintf("pool-%d", p)
		mig := fmt.Sprintf("projects/123/zones/us-central1-a/instanceGroupManagers/mig-%d", p)
		for n := range nodesPerPool {
			name := fmt.Sprintf("%s-node-%d", pool, n)
			entries = append(entries, struct {
				name  string
				entry NodeEntry
			}{
				name: name,
				entry: NodeEntry{
					NodePool:        pool,
					MIG:             mig,
					AcceleratorType: "v6e",
					PartitionIDs:    []string{fmt.Sprintf("root-%d", p), fmt.Sprintf("leaf-%d-%d", p, n%4)},
					Ready:           n%3 != 0,
				},
			})
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		c := New()
		for _, e := range entries {
			c.Upsert(e.name, e.entry)
		}
	}
}

func BenchmarkPoolLeaves500MIGs(b *testing.B) {
	const pools = 500
	const nodesPerPool = 16
	c := New()
	for p := range pools {
		pool := fmt.Sprintf("pool-%d", p)
		mig := fmt.Sprintf("projects/123/zones/us-central1-a/instanceGroupManagers/mig-%d", p)
		for n := range nodesPerPool {
			name := fmt.Sprintf("%s-node-%d", pool, n)
			c.Upsert(name, NodeEntry{
				NodePool:        pool,
				MIG:             mig,
				AcceleratorType: "v6e",
				PartitionIDs:    []string{fmt.Sprintf("root-%d", p), fmt.Sprintf("leaf-%d-%d", p, n%4)},
				Ready:           n%3 != 0,
			})
		}
	}
	nodes, poolCount := c.Len()
	b.Logf("cache: %d nodes, %d pools", nodes, poolCount)

	b.ReportAllocs()
	b.ResetTimer()
	iter := 0
	for b.Loop() {
		c.PoolLeaves(fmt.Sprintf("pool-%d", iter%pools))
		iter++
	}
}

func TestScale500MIGs(t *testing.T) {
	const pools = 500
	const nodesPerPool = 16
	const totalNodes = pools * nodesPerPool

	c := New()

	start := time.Now()
	for p := range pools {
		pool := fmt.Sprintf("pool-%d", p)
		mig := fmt.Sprintf("projects/123/zones/us-central1-a/instanceGroupManagers/mig-%d", p)
		for n := range nodesPerPool {
			name := fmt.Sprintf("%s-node-%d", pool, n)
			c.Upsert(name, NodeEntry{
				NodePool:        pool,
				MIG:             mig,
				AcceleratorType: "v6e",
				PartitionIDs:    []string{fmt.Sprintf("root-%d", p), fmt.Sprintf("leaf-%d-%d", p, n%4)},
				Ready:           n%3 != 0,
			})
		}
	}
	upsertDur := time.Since(start)

	gotNodes, gotPools := c.Len()
	if gotNodes != totalNodes {
		t.Fatalf("node count: got %d want %d", gotNodes, totalNodes)
	}
	if gotPools != pools {
		t.Fatalf("pool count: got %d want %d", gotPools, pools)
	}

	// Verify pool isolation: each pool's PoolLeaves must contain
	// exactly its own 16 nodes across the expected partitions.
	start = time.Now()
	for p := range pools {
		pool := fmt.Sprintf("pool-%d", p)
		snap := c.PoolLeaves(pool)

		// 16 nodes spread across 4 leaf partitions + 1 root = 5 partitions
		if len(snap.Partitions) != 5 {
			t.Errorf("pool %s: %d partitions, want 5", pool, len(snap.Partitions))
		}

		rootNodes := snap.Partitions[fmt.Sprintf("root-%d", p)]
		if len(rootNodes) != nodesPerPool {
			t.Errorf("pool %s root: %d nodes, want %d", pool, len(rootNodes), nodesPerPool)
		}

		if len(snap.Ready) != nodesPerPool {
			t.Errorf("pool %s: %d ready entries, want %d", pool, len(snap.Ready), nodesPerPool)
		}

		// No cross-pool contamination: check a neighbor pool's partition
		neighbor := fmt.Sprintf("root-%d", (p+1)%pools)
		if nodes, ok := snap.Partitions[neighbor]; ok && len(nodes) > 0 {
			t.Errorf("pool %s sees neighbor partition %s nodes: %v", pool, neighbor, nodes)
		}
	}
	leafDur := time.Since(start)

	// Verify delete path at scale: drain one pool.
	start = time.Now()
	for n := range nodesPerPool {
		c.Delete(fmt.Sprintf("pool-0-node-%d", n))
	}
	deleteDur := time.Since(start)

	gotNodes, gotPools = c.Len()
	if gotNodes != totalNodes-nodesPerPool {
		t.Errorf("after delete: %d nodes, want %d", gotNodes, totalNodes-nodesPerPool)
	}
	if gotPools != pools-1 {
		t.Errorf("after delete: %d pools, want %d", gotPools, pools-1)
	}

	t.Logf("Scale results (500 MIGs × 16 nodes = %d nodes):", totalNodes)
	t.Logf("  Upsert all:        %v (%.0f nodes/ms)", upsertDur, float64(totalNodes)/float64(upsertDur.Milliseconds()))
	t.Logf("  PoolLeaves × 500:  %v (%.0f pools/ms)", leafDur, float64(pools)/float64(max(leafDur.Milliseconds(), 1)))
	t.Logf("  Delete one pool:   %v", deleteDur)

	// Guardrails: the cache must handle this at interactive speed.
	if upsertDur > 5*time.Second {
		t.Errorf("upsert too slow: %v > 5s", upsertDur)
	}
	if leafDur > 2*time.Second {
		t.Errorf("PoolLeaves scan too slow: %v > 2s", leafDur)
	}
}

// TestScale1440MIGs validates the cache at large scale: 1440 MIGs
// x 16 nodes = 23,040 nodes. Structure mirrors TestScale500MIGs.
func TestScale1440MIGs(t *testing.T) {
	const pools = 1440
	const nodesPerPool = 16
	const totalNodes = pools * nodesPerPool

	c := New()

	start := time.Now()
	for p := range pools {
		pool := fmt.Sprintf("pool-%d", p)
		mig := fmt.Sprintf("projects/123/zones/us-central1-a/instanceGroupManagers/mig-%d", p)
		for n := range nodesPerPool {
			name := fmt.Sprintf("%s-node-%d", pool, n)
			c.Upsert(name, NodeEntry{
				NodePool:        pool,
				MIG:             mig,
				AcceleratorType: "v6e",
				PartitionIDs:    []string{fmt.Sprintf("root-%d", p), fmt.Sprintf("leaf-%d-%d", p, n%4)},
				Ready:           n%3 != 0,
			})
		}
	}
	upsertDur := time.Since(start)

	gotNodes, gotPools := c.Len()
	if gotNodes != totalNodes {
		t.Fatalf("node count: got %d want %d", gotNodes, totalNodes)
	}
	if gotPools != pools {
		t.Fatalf("pool count: got %d want %d", gotPools, pools)
	}

	// Verify pool isolation for a sample of pools.
	start = time.Now()
	for p := range pools {
		pool := fmt.Sprintf("pool-%d", p)
		snap := c.PoolLeaves(pool)

		if len(snap.Partitions) != 5 {
			t.Errorf("pool %s: %d partitions, want 5", pool, len(snap.Partitions))
		}
		if len(snap.Ready) != nodesPerPool {
			t.Errorf("pool %s: %d ready entries, want %d", pool, len(snap.Ready), nodesPerPool)
		}
	}
	leafDur := time.Since(start)

	// Delete one pool.
	start = time.Now()
	for n := range nodesPerPool {
		c.Delete(fmt.Sprintf("pool-0-node-%d", n))
	}
	deleteDur := time.Since(start)

	gotNodes, gotPools = c.Len()
	if gotNodes != totalNodes-nodesPerPool {
		t.Errorf("after delete: %d nodes, want %d", gotNodes, totalNodes-nodesPerPool)
	}
	if gotPools != pools-1 {
		t.Errorf("after delete: %d pools, want %d", gotPools, pools-1)
	}

	t.Logf("Scale results (1440 MIGs × 16 nodes = %d nodes):", totalNodes)
	t.Logf("  Upsert all:        %v (%.0f nodes/ms)", upsertDur, float64(totalNodes)/float64(max(upsertDur.Milliseconds(), 1)))
	t.Logf("  PoolLeaves × 1440: %v (%.0f pools/ms)", leafDur, float64(pools)/float64(max(leafDur.Milliseconds(), 1)))
	t.Logf("  Delete one pool:   %v", deleteDur)

	if upsertDur > 15*time.Second {
		t.Errorf("upsert too slow: %v > 15s", upsertDur)
	}
	if leafDur > 5*time.Second {
		t.Errorf("PoolLeaves scan too slow: %v > 5s", leafDur)
	}
}

func BenchmarkUpsert1440MIGs(b *testing.B) {
	const pools = 1440
	const nodesPerPool = 16
	entries := make([]struct {
		name  string
		entry NodeEntry
	}, 0, pools*nodesPerPool)
	for p := range pools {
		pool := fmt.Sprintf("pool-%d", p)
		mig := fmt.Sprintf("projects/123/zones/us-central1-a/instanceGroupManagers/mig-%d", p)
		for n := range nodesPerPool {
			name := fmt.Sprintf("%s-node-%d", pool, n)
			entries = append(entries, struct {
				name  string
				entry NodeEntry
			}{
				name: name,
				entry: NodeEntry{
					NodePool:        pool,
					MIG:             mig,
					AcceleratorType: "v6e",
					PartitionIDs:    []string{fmt.Sprintf("root-%d", p), fmt.Sprintf("leaf-%d-%d", p, n%4)},
					Ready:           n%3 != 0,
				},
			})
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		c := New()
		for _, e := range entries {
			c.Upsert(e.name, e.entry)
		}
	}
}

func BenchmarkPoolLeaves1440MIGs(b *testing.B) {
	const pools = 1440
	const nodesPerPool = 16
	c := New()
	for p := range pools {
		pool := fmt.Sprintf("pool-%d", p)
		mig := fmt.Sprintf("projects/123/zones/us-central1-a/instanceGroupManagers/mig-%d", p)
		for n := range nodesPerPool {
			name := fmt.Sprintf("%s-node-%d", pool, n)
			c.Upsert(name, NodeEntry{
				NodePool:        pool,
				MIG:             mig,
				AcceleratorType: "v6e",
				PartitionIDs:    []string{fmt.Sprintf("root-%d", p), fmt.Sprintf("leaf-%d-%d", p, n%4)},
				Ready:           n%3 != 0,
			})
		}
	}
	nodes, poolCount := c.Len()
	b.Logf("cache: %d nodes, %d pools", nodes, poolCount)

	b.ReportAllocs()
	b.ResetTimer()
	iter := 0
	for b.Loop() {
		c.PoolLeaves(fmt.Sprintf("pool-%d", iter%pools))
		iter++
	}
}

// TestMemoryProfile1440MIGs measures heap growth when populating the
// cache at large scale (1440 MIGs × 16 nodes = 23,040 nodes).
// Written as a test (not benchmark) because it needs a single controlled
// measurement with GC pauses, not iterated timing.
func TestMemoryProfile1440MIGs(t *testing.T) {
	const pools = 1440
	const nodesPerPool = 16
	const totalNodes = pools * nodesPerPool

	entries := make([]struct {
		name  string
		entry NodeEntry
	}, 0, totalNodes)
	for p := range pools {
		pool := fmt.Sprintf("pool-%d", p)
		mig := fmt.Sprintf("projects/123/zones/us-central1-a/instanceGroupManagers/mig-%d", p)
		for n := range nodesPerPool {
			name := fmt.Sprintf("%s-node-%d", pool, n)
			entries = append(entries, struct {
				name  string
				entry NodeEntry
			}{
				name: name,
				entry: NodeEntry{
					NodePool:        pool,
					MIG:             mig,
					AcceleratorType: "v6e",
					PartitionIDs:    []string{fmt.Sprintf("root-%d", p), fmt.Sprintf("leaf-%d-%d", p, n%4)},
					Ready:           n%3 != 0,
				},
			})
		}
	}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	c := New()
	for _, e := range entries {
		c.Upsert(e.name, e.entry)
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	totalAllocMB := float64(after.TotalAlloc-before.TotalAlloc) / (1024 * 1024)
	var heapGrowthMB float64
	if after.HeapAlloc > before.HeapAlloc {
		heapGrowthMB = float64(after.HeapAlloc-before.HeapAlloc) / (1024 * 1024)
	}
	bytesPerNode := totalAllocMB * 1024 * 1024 / float64(totalNodes)

	t.Logf("Memory profile (%d nodes):", totalNodes)
	t.Logf("  Heap growth:   %.1f MB", heapGrowthMB)
	t.Logf("  Total alloc:   %.1f MB", totalAllocMB)
	t.Logf("  Bytes/node:    %.0f", bytesPerNode)

	// Guardrail: total allocations should stay well under 200 MB for 23K nodes.
	if totalAllocMB > 200 {
		t.Errorf("total allocations %.1f MB exceeds 200 MB guardrail", totalAllocMB)
	}

	_ = c // keep cache alive past the ReadMemStats call
}

// BenchmarkUpsertAllocs measures per-operation allocation cost for a
// single Upsert into a pre-populated cache.
func BenchmarkUpsertAllocs(b *testing.B) {
	c := New()
	for p := range 100 {
		pool := fmt.Sprintf("pool-%d", p)
		mig := fmt.Sprintf("projects/123/zones/us-central1-a/instanceGroupManagers/mig-%d", p)
		for n := range 16 {
			name := fmt.Sprintf("%s-node-%d", pool, n)
			c.Upsert(name, NodeEntry{
				NodePool:        pool,
				MIG:             mig,
				AcceleratorType: "v6e",
				PartitionIDs:    []string{fmt.Sprintf("root-%d", p), fmt.Sprintf("leaf-%d-%d", p, n%4)},
				Ready:           true,
			})
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	iter := 0
	for b.Loop() {
		name := fmt.Sprintf("pool-0-node-%d", iter%16)
		c.Upsert(name, NodeEntry{
			NodePool:        "pool-0",
			MIG:             "projects/123/zones/us-central1-a/instanceGroupManagers/mig-0",
			AcceleratorType: "v6e",
			PartitionIDs:    []string{"root-0", fmt.Sprintf("leaf-0-%d", iter%4)},
			Ready:           iter%3 != 0,
		})
		iter++
	}
}

// TestNodeChurn500MIGs simulates rolling updates: populate 500 pools,
// then run 5 rounds of 25% node replacement per pool. Verifies cache
// invariants after each round and measures total churn throughput.
func TestNodeChurn500MIGs(t *testing.T) {
	const pools = 500
	const nodesPerPool = 16
	const churnPerPool = 4 // 25% replaced per round
	const rounds = 5

	c := New()

	// Track live node names per pool so we always delete real nodes.
	liveNodes := make([][]string, pools)
	for p := range pools {
		pool := fmt.Sprintf("pool-%d", p)
		mig := fmt.Sprintf("projects/123/zones/us-central1-a/instanceGroupManagers/mig-%d", p)
		liveNodes[p] = make([]string, nodesPerPool)
		for n := range nodesPerPool {
			name := fmt.Sprintf("%s-node-%d", pool, n)
			liveNodes[p][n] = name
			c.Upsert(name, NodeEntry{
				NodePool:        pool,
				MIG:             mig,
				AcceleratorType: "v6e",
				PartitionIDs:    []string{fmt.Sprintf("root-%d", p), fmt.Sprintf("leaf-%d-%d", p, n%4)},
				Ready:           true,
			})
		}
	}

	gotNodes, gotPools := c.Len()
	if gotNodes != pools*nodesPerPool {
		t.Fatalf("initial nodes: got %d want %d", gotNodes, pools*nodesPerPool)
	}
	if gotPools != pools {
		t.Fatalf("initial pools: got %d want %d", gotPools, pools)
	}

	start := time.Now()
	seq := 0 // global sequence for unique replacement names
	for round := range rounds {
		for p := range pools {
			mig := fmt.Sprintf("projects/123/zones/us-central1-a/instanceGroupManagers/mig-%d", p)
			for ci := range churnPerPool {
				// Delete the first churnPerPool nodes in the live list.
				oldName := liveNodes[p][ci]
				c.Delete(oldName)

				// Add a replacement with a globally unique name.
				newName := fmt.Sprintf("churn-%d", seq)
				seq++
				c.Upsert(newName, NodeEntry{
					NodePool:        fmt.Sprintf("pool-%d", p),
					MIG:             mig,
					AcceleratorType: "v6e",
					PartitionIDs:    []string{fmt.Sprintf("root-%d", p), fmt.Sprintf("leaf-%d-%d", p, ci%4)},
					Ready:           true,
				})
				liveNodes[p][ci] = newName
			}
		}

		gotNodes, gotPools = c.Len()
		if gotPools != pools {
			t.Errorf("round %d: pool count = %d want %d", round, gotPools, pools)
		}
		if gotNodes != pools*nodesPerPool {
			t.Errorf("round %d: node count = %d want %d", round, gotNodes, pools*nodesPerPool)
		}

		// Spot-check pool isolation.
		for _, checkPool := range []int{0, 249, 499} {
			pool := fmt.Sprintf("pool-%d", checkPool)
			snap := c.PoolLeaves(pool)
			totalNodes := 0
			for _, nodes := range snap.Partitions {
				totalNodes += len(nodes)
			}
			// Each node appears in root + 1 leaf = 2 partitions.
			if totalNodes != nodesPerPool*2 {
				t.Errorf("round %d pool %s: total partition entries = %d want %d",
					round, pool, totalNodes, nodesPerPool*2)
			}
		}
	}
	churnDur := time.Since(start)

	totalOps := rounds * pools * churnPerPool * 2
	t.Logf("Churn results (%d rounds × %d pools × %d ops = %d total ops):",
		rounds, pools, churnPerPool*2, totalOps)
	t.Logf("  Total duration: %v (%.0f ops/ms)", churnDur, float64(totalOps)/float64(max(churnDur.Milliseconds(), 1)))

	if churnDur > 5*time.Second {
		t.Errorf("churn too slow: %v > 5s", churnDur)
	}
}
