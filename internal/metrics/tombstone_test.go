// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// mockObserver is a simple metric.Observer for testing callbacks.
type mockObserver struct {
	metric.Observer
	observations map[int64]int // value -> count
}

func (m *mockObserver) ObserveInt64(obs metric.Int64Observable, value int64, opts ...metric.ObserveOption) {
	if m.observations == nil {
		m.observations = make(map[int64]int)
	}
	m.observations[value]++
}

func distinctKey(kvs ...attribute.KeyValue) attribute.Distinct {
	s := attribute.NewSet(kvs...)
	return s.Equivalent()
}

func TestTTLTombstoneGaugeSet_Update(t *testing.T) {
	tc := newTTLTombstoneGaugeSet(30 * time.Minute)
	now := time.Now()

	// 0. Nil/empty update for unknown tree should not allocate map entry.
	tc.update("nonexistent-tree", nil, now)
	tc.mu.Lock()
	if len(tc.sets) != 0 {
		t.Errorf("len(tc.sets) = %d after nil update on unknown tree, want 0", len(tc.sets))
	}
	tc.mu.Unlock()

	// 1. Add an entry
	attrs := []attribute.KeyValue{
		attribute.String("partition_id", "partition-1"),
		attribute.String("k", "v"),
	}
	key := distinctKey(attrs...)
	items := []tombstoneItem{{attrs: attrs}}
	tc.update("tree-1", items, now)

	tc.mu.Lock()
	entry := tc.sets["tree-1"][key]
	if entry == nil || !entry.present {
		t.Fatalf("sets[%q][key].present = false/nil, want true (after adding items)", "tree-1")
	}
	tc.mu.Unlock()

	// 2. Mark absent
	tc.update("tree-1", nil, now.Add(time.Minute))

	tc.mu.Lock()
	entry = tc.sets["tree-1"][key]
	if entry.present {
		t.Errorf("sets[%q][key].present = true, want false (after nil update)", "tree-1")
	}
	if entry.absentAt != now.Add(time.Minute) {
		t.Errorf("sets[%q][key].absentAt = %v, want %v (after nil update)", "tree-1", entry.absentAt, now.Add(time.Minute))
	}
	tc.mu.Unlock()

	// 3. Re-add
	tc.update("tree-1", items, now.Add(2*time.Minute))

	tc.mu.Lock()
	entry = tc.sets["tree-1"][key]
	if !entry.present {
		t.Errorf("sets[%q][key].present = false, want true (after re-adding items)", "tree-1")
	}
	if !entry.absentAt.IsZero() {
		t.Errorf("sets[%q][key].absentAt = %v, want zero time (after re-adding items)", "tree-1", entry.absentAt)
	}
	tc.mu.Unlock()
}

func TestTTLTombstoneGaugeSet_AttributeMutationEmitsTombstone(t *testing.T) {
	tc := newTTLTombstoneGaugeSet(30 * time.Minute)
	meter := noop.NewMeterProvider().Meter("test")
	gauge, _ := meter.Int64ObservableGauge("test")

	now := time.Now()
	attrsV1 := []attribute.KeyValue{
		attribute.String("partition_tree_name", "tree-1"),
		attribute.String("root_partition_id", "partition-1"),
		attribute.String("mig_name", "mig-1"),
	}
	attrsV2 := []attribute.KeyValue{
		attribute.String("partition_tree_name", "tree-1"),
		attribute.String("root_partition_id", "partition-1"),
		attribute.String("mig_name", "mig-2"),
	}
	keyV1 := distinctKey(attrsV1...)
	keyV2 := distinctKey(attrsV2...)

	// 1. Initial record with mig-1
	tc.update("tree-1", []tombstoneItem{{attrs: attrsV1}}, now)

	// 2. Mutate mig_name to mig-2 on the same tree and partition
	mutatedAt := now.Add(time.Minute)
	tc.update("tree-1", []tombstoneItem{{attrs: attrsV2}}, mutatedAt)

	// Observe within the 30m TTL window: expect 1 active (mig-2) and 1 tombstone (mig-1).
	obs := &mockObserver{}
	tc.observe(context.Background(), obs, gauge, mutatedAt.Add(5*time.Minute))
	if obs.observations[1] != 1 {
		t.Errorf("active observations (val=1) = %d, want 1", obs.observations[1])
	}
	if obs.observations[0] != 1 {
		t.Errorf("tombstone observations (val=0) = %d, want 1", obs.observations[0])
	}

	tc.mu.Lock()
	if e1 := tc.sets["tree-1"][keyV1]; e1 == nil || e1.present {
		t.Errorf("mig-1 entry = %+v, want present=false tombstone", e1)
	}
	if e2 := tc.sets["tree-1"][keyV2]; e2 == nil || !e2.present {
		t.Errorf("mig-2 entry = %+v, want present=true active", e2)
	}
	tc.mu.Unlock()

	// 3. Observe past the 30m TTL: mig-1 should be pruned, mig-2 stays active.
	obs = &mockObserver{}
	tc.observe(context.Background(), obs, gauge, mutatedAt.Add(31*time.Minute))
	if obs.observations[1] != 1 {
		t.Errorf("active observations (val=1) after TTL = %d, want 1", obs.observations[1])
	}
	if obs.observations[0] != 0 {
		t.Errorf("tombstone observations (val=0) after TTL = %d, want 0", obs.observations[0])
	}

	tc.mu.Lock()
	if _, exists := tc.sets["tree-1"][keyV1]; exists {
		t.Errorf("mig-1 entry still exists after TTL expiration, want pruned")
	}
	if _, exists := tc.sets["tree-1"][keyV2]; !exists {
		t.Errorf("mig-2 entry missing after TTL expiration, want retained")
	}
	tc.mu.Unlock()
}

func TestTTLTombstoneGaugeSet_BuildCallback(t *testing.T) {
	tc := newTTLTombstoneGaugeSet(30 * time.Minute)
	meter := noop.NewMeterProvider().Meter("test")
	gauge, _ := meter.Int64ObservableGauge("test")

	now := time.Now()
	attrs1 := []attribute.KeyValue{
		attribute.String("partition_id", "partition-1"),
		attribute.String("k", "v1"),
	}
	attrs2 := []attribute.KeyValue{
		attribute.String("partition_id", "partition-1"),
		attribute.String("k", "v2"),
	}
	key1 := distinctKey(attrs1...)

	items := []tombstoneItem{
		{attrs: attrs1},
		{attrs: attrs2},
	}
	tc.update("tree-1", items, now)

	cb := tc.buildCallback(gauge)
	obs := &mockObserver{}

	// 1. Observe active entries
	_ = cb(context.Background(), obs)
	if obs.observations[1] != 2 {
		t.Errorf("callback active observations (val=1) = %d, want 2", obs.observations[1])
	}
	if obs.observations[0] != 0 {
		t.Errorf("callback tombstone observations (val=0) = %d, want 0", obs.observations[0])
	}

	// 2. Mark attrs1 absent (within TTL)
	tc.update("tree-1", []tombstoneItem{{attrs: attrs2}}, now.Add(15*time.Minute))

	obs = &mockObserver{}
	_ = cb(context.Background(), obs)
	if obs.observations[1] != 1 {
		t.Errorf("callback active observations (val=1) = %d, want 1 (after attrs1 tombstoned)", obs.observations[1])
	}
	if obs.observations[0] != 1 {
		t.Errorf("callback tombstone observations (val=0) = %d, want 1 (after attrs1 tombstoned)", obs.observations[0])
	}

	// 3. Wait past TTL (attrs1 should be pruned, attrs2 still active)
	tc.mu.Lock()
	tc.sets["tree-1"][key1].absentAt = time.Now().Add(-31 * time.Minute)
	tc.mu.Unlock()

	obs = &mockObserver{}
	_ = cb(context.Background(), obs)
	if obs.observations[1] != 1 {
		t.Errorf("callback active observations (val=1) = %d, want 1 (after TTL expiration)", obs.observations[1])
	}
	if obs.observations[0] != 0 {
		t.Errorf("callback tombstone observations (val=0) = %d, want 0 (expired tombstone should be pruned)", obs.observations[0])
	}

	tc.mu.Lock()
	if _, exists := tc.sets["tree-1"][key1]; exists {
		t.Errorf("sets[%q][key1] exists = true, want false (expired tombstone pruned after collection)", "tree-1")
	}
	tc.mu.Unlock()
}

func TestTTLTombstoneGaugeSet_PruneIfStale(t *testing.T) {
	tc := newTTLTombstoneGaugeSet(30 * time.Minute)
	now := time.Now()

	attrs := []attribute.KeyValue{
		attribute.String("partition_id", "p-1"),
		attribute.String("k", "v"),
	}
	key := distinctKey(attrs...)

	// 1. Add and then tombstone an item
	tc.update("tree-1", []tombstoneItem{{attrs: attrs}}, now)
	tc.update("tree-1", nil, now.Add(time.Minute))

	// Manually age the absentAt timestamp past TTL (31 minutes ago)
	tc.mu.Lock()
	tc.sets["tree-1"][key].absentAt = now.Add(-31 * time.Minute)
	tc.mu.Unlock()

	// 2. Case A: Recent callback within 15 minutes -> pruneIfStale should NO-OP
	tc.mu.Lock()
	tc.lastCallback = now.Add(-5 * time.Minute)
	tc.mu.Unlock()

	tc.pruneIfStale(15*time.Minute, now)

	tc.mu.Lock()
	if _, exists := tc.sets["tree-1"][key]; !exists {
		t.Errorf("entry was pruned prematurely when callback was observed 5 minutes ago")
	}
	tc.mu.Unlock()

	// 3. Case B: No callback in over 15 minutes (e.g. 16 minutes ago) -> pruneIfStale should PRUNE
	tc.mu.Lock()
	tc.lastCallback = now.Add(-16 * time.Minute)
	tc.mu.Unlock()

	tc.pruneIfStale(15*time.Minute, now)

	tc.mu.Lock()
	if _, exists := tc.sets["tree-1"]; exists {
		t.Errorf("tree-1 still exists after pruneIfStale, want it pruned")
	}
	tc.mu.Unlock()
}

func TestMemoryLeak_ContinualChurnWithoutReaders(t *testing.T) {
	tc := newTTLTombstoneGaugeSet(30 * time.Minute)
	baseTime := time.Now()

	// Simulate 10 generations of 50 trees being created, tainted, and deleted over time
	for gen := 0; gen < 10; gen++ {
		genTime := baseTime.Add(time.Duration(gen) * 45 * time.Minute)

		// 1. Create 50 trees
		for i := 0; i < 50; i++ {
			treeName := fmt.Sprintf("gen%d-tree%02d", gen, i)
			var items []tombstoneItem
			for p := 0; p < 10; p++ {
				items = append(items, tombstoneItem{
					attrs: []attribute.KeyValue{
						attribute.String("partition_id", fmt.Sprintf("p-%d", p)),
						attribute.String("state", "occ-state"),
					},
				})
			}
			tc.update(treeName, items, genTime)
		}

		// 2. Delete the 50 trees 5 minutes later
		deleteTime := genTime.Add(5 * time.Minute)
		for i := 0; i < 50; i++ {
			treeName := fmt.Sprintf("gen%d-tree%02d", gen, i)
			tc.update(treeName, nil, deleteTime)
		}

		// 3. Stale sweeper runs 40 minutes after deletion (> 30m TTL, and > 15m without callbacks)
		sweepTime := deleteTime.Add(40 * time.Minute)
		tc.mu.Lock()
		tc.lastCallback = deleteTime // No callbacks occurred since deletion
		tc.mu.Unlock()

		tc.pruneIfStale(15*time.Minute, sweepTime)

		// Verify that previous generation's trees were completely pruned from memory
		tc.mu.Lock()
		activeCount := len(tc.sets)
		tc.mu.Unlock()
		if activeCount != 0 {
			t.Errorf("generation %d: expected 0 trees remaining in memory after sweep, got %d", gen, activeCount)
		}
	}
}
