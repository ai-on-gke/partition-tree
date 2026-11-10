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
	"errors"
	"math"
	"testing"
	"time"

	"github.com/ai-on-gke/partition-tree/internal/config"
	"github.com/ai-on-gke/partition-tree/internal/tree"
)

func retryConfig(initial, max time.Duration, mult float64) config.Retry {
	return config.Retry{
		InitialBackoff: config.Duration(initial),
		MaxBackoff:     config.Duration(max),
		Multiplier:     mult,
	}
}

func TestComputeBackoff_ExponentialGrowth(t *testing.T) {
	retry := retryConfig(1*time.Second, 5*time.Minute, 2.0)
	// 1s * 2^(n-1) for n=1..5 → 1s, 2s, 4s, 8s, 16s
	want := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
	}
	for i, w := range want {
		n := i + 1
		got := computeBackoff(retry, n)
		if got != w {
			t.Errorf("n=%d: got %v, want %v", n, got, w)
		}
	}
}

func TestComputeBackoff_CapsAtMax(t *testing.T) {
	retry := retryConfig(1*time.Second, 30*time.Second, 2.0)
	// At n=6: 1s * 2^5 = 32s, but max is 30s.
	// Test standard values and large values of n (including n=35 which previously overflowed int64).
	for _, n := range []int{6, 7, 10, 20, 34, 35, 36, 64, 100, 1000, 10000} {
		got := computeBackoff(retry, n)
		if got > 30*time.Second {
			t.Errorf("n=%d: got %v, exceeds max 30s", n, got)
		}
		if got != 30*time.Second {
			t.Errorf("n=%d: got %v, want capped at 30s", n, got)
		}
	}
}

func TestComputeBackoff_LargeNWithoutMaxBackoff(t *testing.T) {
	retry := retryConfig(1*time.Second, 0, 2.0)
	for _, n := range []int{35, 36, 64, 100, 1000, 10000} {
		got := computeBackoff(retry, n)
		if got <= 0 {
			t.Errorf("n=%d without max backoff: got %v (overflowed to <= 0), want positive duration capped at MaxInt64", n, got)
		}
		if got != time.Duration(math.MaxInt64) {
			t.Errorf("n=%d without max backoff: got %v, want %v", n, got, time.Duration(math.MaxInt64))
		}
	}
}

func TestComputeBackoff_ZeroFailures(t *testing.T) {
	retry := retryConfig(1*time.Second, 30*time.Second, 2.0)
	got := computeBackoff(retry, 0)
	if got != 0 {
		t.Errorf("n=0: got %v, want 0", got)
	}
}

func TestComputeBackoff_ZeroInitial(t *testing.T) {
	retry := retryConfig(0, 30*time.Second, 2.0)
	for _, n := range []int{0, 1, 5} {
		got := computeBackoff(retry, n)
		if got != 0 {
			t.Errorf("n=%d with zero initial: got %v, want 0", n, got)
		}
	}
}

func TestPollState_RecordSuccessResetsFailures(t *testing.T) {
	s := newPollStateStore()
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	retry := retryConfig(1*time.Second, 30*time.Second, 2.0)

	// Record several failures.
	for i := range 5 {
		s.recordFailure("cr1", now.Add(time.Duration(i)*time.Second), errors.New("fail"), retry)
	}
	state := s.get("cr1")
	if state.failures != 5 {
		t.Fatalf("before success: failures=%d, want 5", state.failures)
	}

	// A success should reset failures to 0.
	s.recordSuccess("cr1", now.Add(10*time.Second))
	state = s.get("cr1")
	if state.failures != 0 {
		t.Errorf("after success: failures=%d, want 0", state.failures)
	}
	if state.lastErr != nil {
		t.Errorf("after success: lastErr=%v, want nil", state.lastErr)
	}
	if state.nextEligibleAt != (time.Time{}) {
		t.Errorf("after success: nextEligibleAt=%v, want zero", state.nextEligibleAt)
	}
}

func TestPollState_RecordFailureIncrementsConsecutive(t *testing.T) {
	s := newPollStateStore()
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	retry := retryConfig(1*time.Second, 30*time.Second, 2.0)

	for i := 1; i <= 4; i++ {
		s.recordFailure("cr1", now.Add(time.Duration(i)*time.Second), errors.New("fail"), retry)
		state := s.get("cr1")
		if state.failures != i {
			t.Errorf("after failure %d: failures=%d, want %d", i, state.failures, i)
		}
	}
}

func TestPollState_ForgetCleansUp(t *testing.T) {
	s := newPollStateStore()
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)

	s.recordSuccess("cr1", now)
	state := s.get("cr1")
	if state.lastSuccessfulAt.IsZero() {
		t.Fatal("expected non-zero state before forget")
	}

	s.forget("cr1")
	state = s.get("cr1")
	if !state.lastPolledAt.IsZero() || !state.lastSuccessfulAt.IsZero() || state.failures != 0 || state.lastErr != nil {
		t.Errorf("after forget: got %+v, want zero state", state)
	}
}

func TestPollState_UpdateAnnotations(t *testing.T) {
	s := newPollStateStore()

	if changed := s.UpdateAnnotations("cr1", nil); changed {
		t.Errorf("Initial nil -> nil: got changed=%v, want false", changed)
	}

	nonTaintAnn := map[string]string{"k1": "v1"}
	if changed := s.UpdateAnnotations("cr1", nonTaintAnn); changed {
		t.Errorf("nil -> non-taint annotation: got changed=%v, want false", changed)
	}

	taintPrefix := tree.TaintAnnotationPrefix
	taintAnn1 := map[string]string{"k1": "v1", taintPrefix + "node1": "val1"}
	if changed := s.UpdateAnnotations("cr1", taintAnn1); !changed {
		t.Errorf("adding taint annotation: got changed=%v, want true", changed)
	}

	if changed := s.UpdateAnnotations("cr1", taintAnn1); changed {
		t.Errorf("same annotations: got changed=%v, want false", changed)
	}

	taintAnn1UpdatedNonTaint := map[string]string{"k1": "v2", taintPrefix + "node1": "val1"}
	if changed := s.UpdateAnnotations("cr1", taintAnn1UpdatedNonTaint); changed {
		t.Errorf("modifying non-taint annotation: got changed=%v, want false", changed)
	}

	taintAnn2 := map[string]string{"k1": "v2", taintPrefix + "node1": "val1", taintPrefix + "node2": "val2"}
	if changed := s.UpdateAnnotations("cr1", taintAnn2); !changed {
		t.Errorf("adding second taint annotation: got changed=%v, want true", changed)
	}

	taintAnn3 := map[string]string{"k1": "v2", taintPrefix + "node1": "new-val", taintPrefix + "node2": "val2"}
	if changed := s.UpdateAnnotations("cr1", taintAnn3); !changed {
		t.Errorf("modifying taint annotation value: got changed=%v, want true", changed)
	}

	if changed := s.UpdateAnnotations("cr1", nonTaintAnn); !changed {
		t.Errorf("removing taint annotations: got changed=%v, want true", changed)
	}
}
