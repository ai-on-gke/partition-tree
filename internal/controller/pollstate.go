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
	"maps"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/ai-on-gke/partition-tree/internal/config"
	"github.com/ai-on-gke/partition-tree/internal/tree"
)

// pollState is the in-memory per-CR record the reconciler uses to
// decide whether a GCE poll is due and whether the Synced condition
// should flip. Nothing in here is persisted — on restart the
// reconciler starts with a fresh map and polls eagerly.
type pollState struct {
	lastPolledAt     time.Time
	lastSuccessfulAt time.Time
	lastErr          error
	// failures is the consecutive failed-poll count since the last
	// success; used to compute exponential backoff. Zero on success.
	failures int
	// nextEligibleAt is the earliest time the next poll may run after
	// a failure. Zero when not in a backoff window.
	nextEligibleAt time.Time
	// lastAnnotations stores the taint annotations from the last reconcile
	// to detect changes and force GCE polls.
	lastAnnotations map[string]string
}

// pollStateStore is a trivial mutex-guarded map keyed by CR name.
// Reads (get) take the read lock; writes (recordSuccess, recordFailure,
// forget) take the write lock.
type pollStateStore struct {
	mu sync.RWMutex
	m  map[string]*pollState
}

func newPollStateStore() *pollStateStore {
	return &pollStateStore{m: map[string]*pollState{}}
}

// get returns a copy of the state for crName. The zero value is a
// valid "never polled" state.
func (s *pollStateStore) get(crName string) pollState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if p, ok := s.m[crName]; ok {
		return *p
	}
	return pollState{}
}

// recordSuccess marks a successful poll at now and clears any backoff
// state.
func (s *pollStateStore) recordSuccess(crName string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.entryLocked(crName)
	p.lastPolledAt = now
	p.lastSuccessfulAt = now
	p.lastErr = nil
	p.failures = 0
	p.nextEligibleAt = time.Time{}
}

// recordFailure marks a failed poll at now and arms the next backoff
// window using the supplied retry config. lastSuccessfulAt is left
// alone so the Synced grace check still works.
func (s *pollStateStore) recordFailure(crName string, now time.Time, err error, retry config.Retry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.entryLocked(crName)
	p.lastPolledAt = now
	p.lastErr = err
	p.failures++
	p.nextEligibleAt = now.Add(computeBackoff(retry, p.failures))
}

// forget drops the state for crName. Used on CR deletion so that a
// re-created CR with the same name starts fresh.
func (s *pollStateStore) forget(crName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, crName)
}

// UpdateAnnotations updates the cached taint annotations for crName and returns
// true if any annotations with the taints.partitiontree.gke.io/ prefix have changed.
func (s *pollStateStore) UpdateAnnotations(crName string, annotations map[string]string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.entryLocked(crName)
	taintAnnotations := extractTaintAnnotations(annotations)
	changed := !maps.Equal(p.lastAnnotations, taintAnnotations)
	if changed {
		p.lastAnnotations = taintAnnotations
	}
	return changed
}

func extractTaintAnnotations(annotations map[string]string) map[string]string {
	var taintAnnotations map[string]string
	for k, v := range annotations {
		if strings.HasPrefix(k, tree.TaintAnnotationPrefix) {
			if taintAnnotations == nil {
				taintAnnotations = make(map[string]string)
			}
			taintAnnotations[k] = v
		}
	}
	return taintAnnotations
}

func (s *pollStateStore) entryLocked(crName string) *pollState {
	p, ok := s.m[crName]
	if !ok {
		p = &pollState{}
		s.m[crName] = p
	}
	return p
}

// computeBackoff returns the wait time for the n-th consecutive
// failure (n >= 1): initial * multiplier^(n-1), capped at max. n <= 0
// returns 0 (no wait).
func computeBackoff(retry config.Retry, n int) time.Duration {
	if n <= 0 {
		return 0
	}
	initial := retry.InitialBackoff.D()
	if initial <= 0 {
		return 0
	}
	mult := retry.Multiplier
	if mult < 1 {
		mult = 1
	}
	maxBackoff := retry.MaxBackoff.D()
	val := float64(initial) * math.Pow(mult, float64(n-1))
	if maxBackoff > 0 && val >= float64(maxBackoff) {
		return maxBackoff
	}
	if val >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(val)
}
