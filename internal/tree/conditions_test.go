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

package tree

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
)

func TestDeriveNodeReady(t *testing.T) {
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name       string
		nodes      []NodeStatus
		wantStatus metav1.ConditionStatus
		wantReason string
		// wantMessage is checked only when non-empty.
		wantMessage string
	}{
		{
			name: "all-ready",
			nodes: []NodeStatus{
				{Name: "n1", Ready: true, Found: true},
				{Name: "n2", Ready: true, Found: true},
			},
			wantStatus: metav1.ConditionTrue,
			wantReason: ReasonNodesReady,
		},
		{
			name: "one-not-ready",
			nodes: []NodeStatus{
				{Name: "n1", Ready: true, Found: true},
				{Name: "n2", Ready: false, Found: true},
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonNodesNotReady,
		},
		{
			name: "missing-takes-precedence-over-not-ready",
			nodes: []NodeStatus{
				{Name: "n1", Ready: false, Found: true},
				{Name: "n2", Found: false},
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonNodesNotFound,
		},
		{
			name:       "empty-list-reports-no-nodes",
			nodes:      nil,
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonNodesNotFound,
		},
		{
			name:       "explicitly-empty-slice-reports-no-nodes",
			nodes:      []NodeStatus{},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonNodesNotFound,
		},
		{
			name: "tpu-expected-zero-disables-chip-check",
			nodes: []NodeStatus{
				{Name: "n1", Ready: true, Found: true, TPUAllocatable: 0, TPUExpected: 0},
			},
			wantStatus:  metav1.ConditionTrue,
			wantReason:  ReasonNodesReady,
			wantMessage: "all expected nodes are Ready",
		},
		{
			name: "tpu-allocatable-absent",
			nodes: []NodeStatus{
				{Name: "n1", Ready: true, Found: true, TPUExpected: 4},
			},
			wantStatus:  metav1.ConditionFalse,
			wantReason:  ReasonTPUsNotAllocatable,
			wantMessage: "nodes with TPU chips not allocatable: [n1 (0/4)]",
		},
		{
			name: "tpu-allocatable-3-of-4",
			nodes: []NodeStatus{
				{Name: "n1", Ready: true, Found: true, TPUAllocatable: 3, TPUExpected: 4},
			},
			wantStatus:  metav1.ConditionFalse,
			wantReason:  ReasonTPUsNotAllocatable,
			wantMessage: "nodes with TPU chips not allocatable: [n1 (3/4)]",
		},
		{
			name: "tpu-allocatable-4-of-4",
			nodes: []NodeStatus{
				{Name: "n1", Ready: true, Found: true, TPUAllocatable: 4, TPUExpected: 4},
			},
			wantStatus:  metav1.ConditionTrue,
			wantReason:  ReasonNodesReady,
			wantMessage: "all expected nodes are Ready",
		},
		{
			name: "tpu-allocatable-above-expected",
			nodes: []NodeStatus{
				{Name: "n1", Ready: true, Found: true, TPUAllocatable: 8, TPUExpected: 4},
			},
			wantStatus: metav1.ConditionTrue,
			wantReason: ReasonNodesReady,
		},
		{
			name: "not-ready-takes-precedence-over-tpus-not-allocatable",
			nodes: []NodeStatus{
				{Name: "n1", Ready: false, Found: true, TPUAllocatable: 0, TPUExpected: 4},
			},
			wantStatus:  metav1.ConditionFalse,
			wantReason:  ReasonNodesNotReady,
			wantMessage: "nodes not ready: [n1]",
		},
		{
			name: "not-ready-with-stale-allocatable-is-not-ready",
			nodes: []NodeStatus{
				{Name: "n1", Ready: false, Found: true, TPUAllocatable: 4, TPUExpected: 4},
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonNodesNotReady,
		},
		{
			name: "missing-takes-precedence-over-tpus-not-allocatable",
			nodes: []NodeStatus{
				{Name: "n1", Found: false},
				{Name: "n2", Ready: true, Found: true, TPUAllocatable: 0, TPUExpected: 4},
			},
			wantStatus:  metav1.ConditionFalse,
			wantReason:  ReasonNodesNotFound,
			wantMessage: "nodes not found in cache: [n1]",
		},
		{
			name: "two-node-leaf-names-only-the-short-node",
			nodes: []NodeStatus{
				{Name: "n1", Ready: true, Found: true, TPUAllocatable: 4, TPUExpected: 4},
				{Name: "n2", Ready: true, Found: true, TPUAllocatable: 0, TPUExpected: 4},
			},
			wantStatus:  metav1.ConditionFalse,
			wantReason:  ReasonTPUsNotAllocatable,
			wantMessage: "nodes with TPU chips not allocatable: [n2 (0/4)]",
		},
		{
			name: "tpus-not-allocatable-message-keeps-input-order",
			nodes: []NodeStatus{
				{Name: "n1", Ready: true, Found: true, TPUAllocatable: 0, TPUExpected: 4},
				{Name: "n2", Ready: true, Found: true, TPUAllocatable: 3, TPUExpected: 4},
				{Name: "n3", Ready: true, Found: true, TPUAllocatable: 4, TPUExpected: 4},
			},
			wantStatus:  metav1.ConditionFalse,
			wantReason:  ReasonTPUsNotAllocatable,
			wantMessage: "nodes with TPU chips not allocatable: [n1 (0/4) n2 (3/4)]",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := DeriveNodeReady(tc.nodes, now)
			if c.Type != partitionv1beta1.ConditionTypeNodeReady {
				t.Errorf("type: got %q want %q", c.Type, partitionv1beta1.ConditionTypeNodeReady)
			}
			if c.Status != tc.wantStatus {
				t.Errorf("status: got %q want %q", c.Status, tc.wantStatus)
			}
			if c.Reason != tc.wantReason {
				t.Errorf("reason: got %q want %q", c.Reason, tc.wantReason)
			}
			if tc.wantMessage != "" && c.Message != tc.wantMessage {
				t.Errorf("message: got %q want %q", c.Message, tc.wantMessage)
			}
		})
	}
}

func TestDeriveSynced(t *testing.T) {
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	in := DeriveSynced(true, now, "ok")
	if in.Status != metav1.ConditionTrue || in.Reason != ReasonInSync {
		t.Errorf("in-sync: %+v", in)
	}
	out := DeriveSynced(false, now, "stale")
	if out.Status != metav1.ConditionFalse || out.Reason != ReasonOutOfSync {
		t.Errorf("out-of-sync: %+v", out)
	}
	if out.Type != partitionv1beta1.ConditionTypeSynced {
		t.Errorf("type: got %q want %q", out.Type, partitionv1beta1.ConditionTypeSynced)
	}
}
