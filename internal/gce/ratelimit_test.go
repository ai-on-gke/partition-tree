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

package gce

import (
	"context"
	"errors"
	"testing"
)

func TestTryGet_Allowed(t *testing.T) {
	mock := NewMockClient()
	mock.SetResponse("mig-a", &TopologyResponse{
		Entries: []TopologyEntry{{PartitionID: "root", State: "Healthy"}},
	})
	rl := NewRateLimitedClient(mock, 100, 10)

	resp, err := rl.TryGetAvailableAcceleratorTopologies(context.Background(), "proj", "zone", "mig-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil || len(resp.Entries) != 1 {
		t.Fatalf("unexpected response: %v", resp)
	}
	if mock.Calls() != 1 {
		t.Errorf("calls = %d want 1", mock.Calls())
	}
}

func TestTryGet_RateLimited(t *testing.T) {
	mock := NewMockClient()
	mock.SetResponse("mig-a", &TopologyResponse{
		Entries: []TopologyEntry{{PartitionID: "root", State: "Healthy"}},
	})
	// Burst=1: first call consumes the only token.
	rl := NewRateLimitedClient(mock, 1, 1)

	// First call: token available.
	_, err := rl.TryGetAvailableAcceleratorTopologies(context.Background(), "proj", "zone", "mig-a")
	if err != nil {
		t.Fatalf("first call: %v", err)
	}

	// Second call: no token left.
	_, err = rl.TryGetAvailableAcceleratorTopologies(context.Background(), "proj", "zone", "mig-a")
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("second call: got %v, want ErrRateLimited", err)
	}
	// Inner client should not have been called for the second attempt.
	if mock.Calls() != 1 {
		t.Errorf("calls = %d want 1 (second call should not reach inner)", mock.Calls())
	}
}

func TestTryClient_InterfaceConformance(t *testing.T) {
	mock := NewMockClient()
	rl := NewRateLimitedClient(mock, 50, 50)
	var _ TryClient = rl // compile-time check
}

func TestGetBlocking_ContextCancellation(t *testing.T) {
	mock := NewMockClient()
	mock.SetResponse("mig-a", &TopologyResponse{
		Entries: []TopologyEntry{{PartitionID: "root", State: "Healthy"}},
	})
	// Burst=1 with very low QPS: after consuming the token, the blocking
	// Get must wait — a cancelled context should unblock it immediately.
	rl := NewRateLimitedClient(mock, 0.001, 1)

	// Consume the single token.
	_, err := rl.GetAvailableAcceleratorTopologies(context.Background(), "proj", "zone", "mig-a")
	if err != nil {
		t.Fatalf("first call: %v", err)
	}

	// Cancel context immediately.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = rl.GetAvailableAcceleratorTopologies(ctx, "proj", "zone", "mig-a")
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
}
