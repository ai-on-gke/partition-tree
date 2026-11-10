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
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// ttlTombstoneGaugeSet tracks the lifecycle of metric dimensions (labels) and automatically
// emits a tombstone value (0) for a TTL period after a dimension is removed or mutated.
// This prevents monitoring backends from interpolating stale values for gauges.
type ttlTombstoneGaugeSet struct {
	mu           sync.Mutex
	ttl          time.Duration
	lastCallback time.Time

	// sets maps treeName -> canonical attribute.Distinct -> entry
	sets map[string]map[attribute.Distinct]*boolWithTTLTombstoneEntry
}

type boolWithTTLTombstoneEntry struct {
	opts     []metric.ObserveOption
	present  bool
	absentAt time.Time
}

func newTTLTombstoneGaugeSet(ttl time.Duration) *ttlTombstoneGaugeSet {
	return &ttlTombstoneGaugeSet{
		ttl:          ttl,
		lastCallback: time.Now(),
		sets:         make(map[string]map[attribute.Distinct]*boolWithTTLTombstoneEntry),
	}
}

// SetTTL updates the TTL for the tombstone gauge set.
func (gs *ttlTombstoneGaugeSet) SetTTL(ttl time.Duration) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	gs.ttl = ttl
}

type tombstoneItem struct {
	attrs []attribute.KeyValue
}

type preparedTombstoneItem struct {
	attrSet attribute.Set
	key     attribute.Distinct
}

// update atomically replaces the active entries for a given PartitionTree.
// Any previously active entries whose canonical attribute.Set is not in the new
// slice are marked as tombstones. If items is nil or empty, all current entries
// for the tree are tombstoned.
func (gs *ttlTombstoneGaugeSet) update(treeName string, items []tombstoneItem, now time.Time) {
	var prepared []preparedTombstoneItem
	if len(items) > 0 {
		prepared = make([]preparedTombstoneItem, len(items))
		for i, item := range items {
			attrSet := attribute.NewSet(item.attrs...)
			prepared[i] = preparedTombstoneItem{
				attrSet: attrSet,
				key:     attrSet.Equivalent(),
			}
		}
	}

	gs.mu.Lock()
	defer gs.mu.Unlock()

	treeSets, exists := gs.sets[treeName]
	if !exists {
		if len(prepared) == 0 {
			return
		}
		treeSets = make(map[attribute.Distinct]*boolWithTTLTombstoneEntry, len(prepared))
		gs.sets[treeName] = treeSets
	}

	// 1. Mark all existing entries for this tree as absent (tombstoned) initially.
	for _, entry := range treeSets {
		if entry.present {
			entry.present = false
			entry.absentAt = now
		}
	}

	// 2. Add or update active entries from the new items slice.
	for _, p := range prepared {
		if entry, ok := treeSets[p.key]; ok {
			entry.present = true
			// Clear the absent timestamp since it's active again
			entry.absentAt = time.Time{}
		} else {
			treeSets[p.key] = &boolWithTTLTombstoneEntry{
				opts:    []metric.ObserveOption{metric.WithAttributeSet(p.attrSet)},
				present: true,
			}
		}
	}
}

// clear removes all entries from the gauge set (for testing only).
func (gs *ttlTombstoneGaugeSet) clear() {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	gs.sets = make(map[string]map[attribute.Distinct]*boolWithTTLTombstoneEntry)
	gs.lastCallback = time.Now()
}

// pruneLocked deletes entries that have been absent for longer than TTL.
// Must be called with gs.mu held.
func (gs *ttlTombstoneGaugeSet) pruneLocked(now time.Time) {
	for treeName, treeSets := range gs.sets {
		for key, entry := range treeSets {
			if !entry.present && now.Sub(entry.absentAt) > gs.ttl {
				delete(treeSets, key)
			}
		}
		if len(treeSets) == 0 {
			delete(gs.sets, treeName)
		}
	}
}

// pruneIfStale prunes expired tombstones if no metric read callback has been observed
// in over staleThreshold (e.g. 15 minutes). If callbacks are active, it is a no-op.
func (gs *ttlTombstoneGaugeSet) pruneIfStale(staleThreshold time.Duration, now time.Time) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	if !gs.lastCallback.IsZero() && now.Sub(gs.lastCallback) < staleThreshold {
		return
	}
	gs.pruneLocked(now)
}

// observe emits all active and non-expired tombstone entries into obs using the provided timestamp.
// Expired tombstones (absent longer than TTL) are deleted from the gauge set during collection.
func (gs *ttlTombstoneGaugeSet) observe(ctx context.Context, obs metric.Observer, gauge metric.Int64ObservableGauge, now time.Time) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	gs.lastCallback = now
	for treeName, treeSets := range gs.sets {
		for key, entry := range treeSets {
			if !entry.present {
				if now.Sub(entry.absentAt) > gs.ttl {
					delete(treeSets, key)
					continue
				}
				// Emit tombstone (0)
				obs.ObserveInt64(gauge, 0, entry.opts...)
			} else {
				// Emit active state (1)
				obs.ObserveInt64(gauge, 1, entry.opts...)
			}
		}
		if len(treeSets) == 0 {
			delete(gs.sets, treeName)
		}
	}
}

// buildCallback creates an OTEL metric.Callback for the given ObservableGauge.
func (gs *ttlTombstoneGaugeSet) buildCallback(gauge metric.Int64ObservableGauge) metric.Callback {
	return func(ctx context.Context, obs metric.Observer) error {
		gs.observe(ctx, obs, gauge, time.Now())
		return nil
	}
}
