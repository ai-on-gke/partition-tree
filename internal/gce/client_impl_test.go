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
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"google.golang.org/api/option"
)

// TestRealClient_Maps_Success stands up a canned httptest server that
// serves the exact JSON shape of a compute/v0.beta
// InstanceGroupManagers.GetAvailableAcceleratorTopologies response and
// verifies the RealClient maps every domain field correctly. This is
// the only place the JSON wire contract is pinned; the rest of the
// controller works off the translated TopologyEntry type.
func TestRealClient_Maps_Success(t *testing.T) {
	const body = `{
	  "multiMig": "https://example.com/multiMigs/mm-1",
	  "acceleratorTopologiesInfo": {
	    "root": {
	      "acceleratorTopology": "4x4x4",
	      "acceleratorTopologyHealth": "HEALTHY",
	      "acceleratorTopologyState": {"currentState": "ACTIVE", "externalId": ""}
	    },
	    "leaf-a": {
	      "parent": "root",
	      "acceleratorTopology": "2x2x1",
	      "acceleratorTopologyHealth": "DEGRADED",
	      "acceleratorTopologyState": {"currentState": "ACTIVE", "externalId": "slice-42"}
	    },
	    "leaf-b": {
	      "parent": "root",
	      "acceleratorTopology": "2x2x1",
	      "acceleratorTopologyHealth": "HEALTHY",
	      "acceleratorTopologyState": {"currentState": "INCOMPLETE"}
	    }
	  }
	}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The generated client hits this path pattern — just sanity
		// check so a future SDK rename doesn't silently pass.
		if !strings.Contains(r.URL.Path, "getAvailableAcceleratorTopologies") &&
			!strings.Contains(r.URL.Path, "instanceGroupManagers") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	ctx := context.Background()
	client, err := NewRealClient(ctx,
		option.WithEndpoint(srv.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	resp, err := client.GetAvailableAcceleratorTopologies(ctx, "proj-1", "us-central2-b", "mig-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(resp.Entries) != 3 {
		t.Fatalf("entries = %d want 3", len(resp.Entries))
	}

	// Response map iteration order is nondeterministic, so sort by ID
	// before asserting field values.
	sort.Slice(resp.Entries, func(i, j int) bool {
		return resp.Entries[i].PartitionID < resp.Entries[j].PartitionID
	})
	leafA, leafB, root := resp.Entries[0], resp.Entries[1], resp.Entries[2]

	if root.PartitionID != "root" || root.ParentID != "" {
		t.Errorf("root: %+v", root)
	}
	if !root.MultiMig {
		t.Errorf("root.MultiMig = false, want true (multiMig URL present)")
	}
	if root.Topology != "4x4x4" || root.State != "Healthy" {
		t.Errorf("root shape/state: %+v", root)
	}

	if leafA.ParentID != "root" || leafA.State != "Degraded" {
		t.Errorf("leaf-a: %+v", leafA)
	}
	if leafA.SliceID != "slice-42" {
		t.Errorf("leaf-a slice id = %q", leafA.SliceID)
	}
	if leafA.MultiMig {
		t.Errorf("leaf-a.MultiMig = true, want false (only root carries it)")
	}

	// leaf-b exercises the CurrentState=INCOMPLETE override: health is
	// HEALTHY but the state must flip to "Incomplete".
	if leafB.State != "Incomplete" {
		t.Errorf("leaf-b state = %q, want Incomplete", leafB.State)
	}
}

// TestRealClient_RootIncompleteOverridden verifies that a root partition
// with CurrentState=INCOMPLETE overrides its health-derived state to Incomplete,
// just like non-root partitions.
func TestRealClient_RootIncompleteOverridden(t *testing.T) {
	const body = `{
	  "acceleratorTopologiesInfo": {
	    "root": {
	      "acceleratorTopology": "4x4x4",
	      "acceleratorTopologyHealth": "HEALTHY",
	      "instancesHealth": "ALL_HEALTHY",
	      "acceleratorTopologyState": {"currentState": "INCOMPLETE"}
	    },
	    "leaf-a": {
	      "parent": "root",
	      "acceleratorTopology": "2x2x1",
	      "acceleratorTopologyHealth": "HEALTHY",
	      "acceleratorTopologyState": {"currentState": "INCOMPLETE"}
	    }
	  }
	}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	ctx := context.Background()
	client, err := NewRealClient(ctx,
		option.WithEndpoint(srv.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	resp, err := client.GetAvailableAcceleratorTopologies(ctx, "p", "z", "m")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	sort.Slice(resp.Entries, func(i, j int) bool {
		return resp.Entries[i].PartitionID < resp.Entries[j].PartitionID
	})
	leafA, root := resp.Entries[0], resp.Entries[1]

	// Root INCOMPLETE must override.
	if root.State != "Incomplete" {
		t.Errorf("root state = %q, want Incomplete", root.State)
	}
	// Non-root INCOMPLETE must override.
	if leafA.State != "Incomplete" {
		t.Errorf("leaf-a state = %q, want Incomplete", leafA.State)
	}
}

// TestRealClient_InstancesHealth_Override verifies that InstancesHealth
// of UNHEALTHY_OR_MISSING forces the partition to Unhealthy even when
// AcceleratorTopologyHealth is HEALTHY.
func TestRealClient_InstancesHealth_Override(t *testing.T) {
	const body = `{
	  "acceleratorTopologiesInfo": {
	    "root": {
	      "acceleratorTopology": "4x4x4",
	      "acceleratorTopologyHealth": "HEALTHY",
	      "instancesHealth": "ALL_HEALTHY"
	    },
	    "leaf-a": {
	      "parent": "root",
	      "acceleratorTopology": "2x2x1",
	      "acceleratorTopologyHealth": "HEALTHY",
	      "instancesHealth": "UNHEALTHY_OR_MISSING"
	    }
	  }
	}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	ctx := context.Background()
	client, err := NewRealClient(ctx,
		option.WithEndpoint(srv.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	resp, err := client.GetAvailableAcceleratorTopologies(ctx, "p", "z", "m")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	sort.Slice(resp.Entries, func(i, j int) bool {
		return resp.Entries[i].PartitionID < resp.Entries[j].PartitionID
	})
	leafA, root := resp.Entries[0], resp.Entries[1]
	if root.State != "Healthy" {
		t.Errorf("root state = %q, want Healthy", root.State)
	}
	if leafA.State != "Unhealthy" {
		t.Errorf("leaf-a state = %q, want Unhealthy (instances UNHEALTHY_OR_MISSING)", leafA.State)
	}
}

// TestRealClient_Maps_UnknownHealth confirms an unrecognized health
// enum falls back to "Unknown" rather than propagating garbage into
// the tree.
func TestRealClient_Maps_UnknownHealth(t *testing.T) {
	const body = `{
	  "acceleratorTopologiesInfo": {
	    "root": {"acceleratorTopology": "16x16", "acceleratorTopologyHealth": "WEIRD_VALUE"}
	  }
	}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	ctx := context.Background()
	client, err := NewRealClient(ctx,
		option.WithEndpoint(srv.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	resp, err := client.GetAvailableAcceleratorTopologies(ctx, "p", "z", "m")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(resp.Entries) != 1 || resp.Entries[0].State != "Unknown" {
		t.Errorf("state = %+v, want Unknown", resp.Entries)
	}
	// An empty MultiMig URL must not flag the root.
	if resp.Entries[0].MultiMig {
		t.Errorf("MultiMig = true, want false (no top-level multiMig URL)")
	}
}

// TestRealClient_Maps_HTTPError verifies that a 5xx from the server
// produces a descriptive wrapped error — the reconciler uses this as a
// signal to hold its tree and eventually flip Synced=False.
func TestRealClient_Maps_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":{"code":503,"message":"backend unavailable"}}`, http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	ctx := context.Background()
	client, err := NewRealClient(ctx,
		option.WithEndpoint(srv.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	_, err = client.GetAvailableAcceleratorTopologies(ctx, "p", "z", "m")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "p/z/m") {
		t.Errorf("error does not include target: %v", err)
	}
}

// TestMapTopologyHealth_AllCases tests the mapTopologyHealth function
// for every known input and the unknown/empty fallback.
func TestMapTopologyHealth_AllCases(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"HEALTHY", "Healthy"},
		{"DEGRADED", "Degraded"},
		{"UNHEALTHY", "Unhealthy"},
		{"", "Unknown"},
		{"GARBAGE", "Unknown"},
	}
	for _, tc := range cases {
		got := mapTopologyHealth(tc.in)
		if got != tc.want {
			t.Errorf("mapTopologyHealth(%q) = %q want %q", tc.in, got, tc.want)
		}
	}
}

// TestCombineHealth verifies that AcceleratorTopologyHealth and
// InstancesHealth are merged correctly: either unhealthy → Unhealthy,
// either degraded → Degraded, both healthy → Healthy.
func TestCombineHealth(t *testing.T) {
	cases := []struct {
		name           string
		topoHealth     string
		instanceHealth string
		want           string
	}{
		{"both healthy", "HEALTHY", "ALL_HEALTHY", "Healthy"},
		{"topo degraded, instances healthy", "DEGRADED", "ALL_HEALTHY", "Degraded"},
		{"topo healthy, instances unhealthy", "HEALTHY", "UNHEALTHY_OR_MISSING", "Unhealthy"},
		{"topo degraded, instances unhealthy", "DEGRADED", "UNHEALTHY_OR_MISSING", "Unhealthy"},
		{"topo unhealthy, instances healthy", "UNHEALTHY", "ALL_HEALTHY", "Unhealthy"},
		{"topo unhealthy, instances unhealthy", "UNHEALTHY", "UNHEALTHY_OR_MISSING", "Unhealthy"},
		{"topo healthy, instances empty", "HEALTHY", "", "Healthy"},
		{"topo empty, instances healthy", "", "ALL_HEALTHY", "Unknown"},
		{"both empty", "", "", "Unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := combineHealth(tc.topoHealth, tc.instanceHealth)
			if got != tc.want {
				t.Errorf("combineHealth(%q, %q) = %q want %q",
					tc.topoHealth, tc.instanceHealth, got, tc.want)
			}
		})
	}
}
