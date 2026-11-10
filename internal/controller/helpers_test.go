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
	"reflect"
	"sort"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
	"github.com/ai-on-gke/partition-tree/internal/cache"
	"github.com/ai-on-gke/partition-tree/internal/config"
	"github.com/ai-on-gke/partition-tree/internal/tree"
)

// syncedReconciler returns a zero-valued reconciler suitable for calling
// applySynced, which uses none of the reconciler's fields.
func syncedReconciler() *PartitionTreeReconciler {
	return &PartitionTreeReconciler{}
}

func TestApplySynced_FirstSuccessSetsSynced(t *testing.T) {
	r := syncedReconciler()
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	cr := &partitionv1beta1.PartitionTree{}
	state := pollState{lastSuccessfulAt: now}
	cfg := config.Default()

	r.applySynced(cr, state, cfg, now, nil)

	c := meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeSynced)
	if c == nil {
		t.Fatal("Synced condition not set")
	}
	if c.Status != metav1.ConditionTrue {
		t.Errorf("Synced status = %q, want True", c.Status)
	}
	if c.Reason != "InSync" {
		t.Errorf("Synced reason = %q, want InSync", c.Reason)
	}
}

func TestApplySynced_ErrorWithinGraceWindow_LeavesConditionUntouched(t *testing.T) {
	r := syncedReconciler()
	cfg := config.Default() // syncTimeout = 15s
	baseTime := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)

	// Pre-set a Synced=True condition (as if a previous success set it).
	cr := &partitionv1beta1.PartitionTree{
		Status: partitionv1beta1.PartitionTreeStatus{
			Conditions: []metav1.Condition{
				{
					Type:               partitionv1beta1.ConditionTypeSynced,
					Status:             metav1.ConditionTrue,
					Reason:             "InSync",
					Message:            "baseline",
					LastTransitionTime: metav1.NewTime(baseTime),
				},
			},
		},
	}

	state := pollState{
		lastSuccessfulAt: baseTime,
	}

	// Error arrives 5s after last success — well within the 15s grace window.
	now := baseTime.Add(5 * time.Second)
	pollErr := errors.New("transient GCE error")

	r.applySynced(cr, state, cfg, now, pollErr)

	// The condition should be untouched — still True with the original message.
	c := meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeSynced)
	if c == nil {
		t.Fatal("Synced condition disappeared")
	}
	if c.Status != metav1.ConditionTrue {
		t.Errorf("within grace: Synced = %q, want True (untouched)", c.Status)
	}
	if c.Message != "baseline" {
		t.Errorf("within grace: message changed to %q, want 'baseline'", c.Message)
	}
}

func TestApplySynced_ErrorAfterGraceWindow_SetsOutOfSync(t *testing.T) {
	r := syncedReconciler()
	cfg := config.Default() // syncTimeout = 15s
	baseTime := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)

	cr := &partitionv1beta1.PartitionTree{
		Status: partitionv1beta1.PartitionTreeStatus{
			Conditions: []metav1.Condition{
				{
					Type:               partitionv1beta1.ConditionTypeSynced,
					Status:             metav1.ConditionTrue,
					Reason:             "InSync",
					LastTransitionTime: metav1.NewTime(baseTime),
				},
			},
		},
	}

	state := pollState{
		lastSuccessfulAt: baseTime,
	}

	// Error arrives 20s after last success — past the 15s grace window.
	now := baseTime.Add(20 * time.Second)
	pollErr := errors.New("persistent GCE error")

	r.applySynced(cr, state, cfg, now, pollErr)

	c := meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeSynced)
	if c == nil {
		t.Fatal("Synced condition not set")
	}
	if c.Status != metav1.ConditionFalse {
		t.Errorf("post-grace: Synced = %q, want False", c.Status)
	}
	if c.Reason != "OutOfSync" {
		t.Errorf("post-grace: reason = %q, want OutOfSync", c.Reason)
	}
}

func TestApplySynced_NeverSucceeded_SetsOutOfSync(t *testing.T) {
	r := syncedReconciler()
	cfg := config.Default()
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)

	cr := &partitionv1beta1.PartitionTree{}
	// lastSuccessfulAt is zero — never succeeded.
	state := pollState{}
	pollErr := errors.New("first poll failed")

	r.applySynced(cr, state, cfg, now, pollErr)

	c := meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeSynced)
	if c == nil {
		t.Fatal("Synced condition not set")
	}
	if c.Status != metav1.ConditionFalse {
		t.Errorf("never-succeeded: Synced = %q, want False", c.Status)
	}
	if c.Reason != "OutOfSync" {
		t.Errorf("never-succeeded: reason = %q, want OutOfSync", c.Reason)
	}
}

func TestApplySynced_NoPollNoError_NoConditionChange(t *testing.T) {
	r := syncedReconciler()
	cfg := config.Default()
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)

	cr := &partitionv1beta1.PartitionTree{}
	state := pollState{} // zero: never polled, no error

	r.applySynced(cr, state, cfg, now, nil)

	c := meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeSynced)
	if c != nil {
		t.Errorf("expected no Synced condition, got %+v", c)
	}
}

func TestApplySynced_NoPollAfterGraceWindow_SetsOutOfSync(t *testing.T) {
	r := syncedReconciler()
	cfg := config.Default() // syncTimeout = 15s
	baseTime := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)

	cr := &partitionv1beta1.PartitionTree{
		Status: partitionv1beta1.PartitionTreeStatus{
			Conditions: []metav1.Condition{
				{
					Type:               partitionv1beta1.ConditionTypeSynced,
					Status:             metav1.ConditionTrue,
					Reason:             "InSync",
					LastTransitionTime: metav1.NewTime(baseTime),
				},
			},
		},
	}

	state := pollState{
		lastSuccessfulAt: baseTime,
	}

	// No poll in this cycle (pollErr == nil), but arrives 20s after last success — past the 15s grace window.
	now := baseTime.Add(20 * time.Second)

	r.applySynced(cr, state, cfg, now, nil)

	c := meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeSynced)
	if c == nil {
		t.Fatal("Synced condition not set")
	}
	if c.Status != metav1.ConditionFalse {
		t.Errorf("no poll post-grace: Synced = %q, want False", c.Status)
	}
	if c.Reason != "OutOfSync" {
		t.Errorf("no poll post-grace: reason = %q, want OutOfSync", c.Reason)
	}
}

func TestApplySynced_NoPollWithinGraceWindow_LeavesConditionUntouched(t *testing.T) {
	r := syncedReconciler()
	cfg := config.Default() // syncTimeout = 15s
	baseTime := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)

	// Pre-set a Synced=True condition.
	cr := &partitionv1beta1.PartitionTree{
		Status: partitionv1beta1.PartitionTreeStatus{
			Conditions: []metav1.Condition{
				{
					Type:               partitionv1beta1.ConditionTypeSynced,
					Status:             metav1.ConditionTrue,
					Reason:             "InSync",
					Message:            "baseline",
					LastTransitionTime: metav1.NewTime(baseTime),
				},
			},
		},
	}

	state := pollState{
		lastSuccessfulAt: baseTime,
	}

	// No poll in this cycle (pollErr == nil), arrives 5s after last success — within the 15s grace window.
	now := baseTime.Add(5 * time.Second)

	r.applySynced(cr, state, cfg, now, nil)

	// The condition should be untouched — still True with the original message.
	c := meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeSynced)
	if c == nil {
		t.Fatal("Synced condition disappeared")
	}
	if c.Status != metav1.ConditionTrue {
		t.Errorf("no poll within grace: Synced = %q, want True (untouched)", c.Status)
	}
	if c.Message != "baseline" {
		t.Errorf("no poll within grace: message changed to %q, want 'baseline'", c.Message)
	}
}

func TestUnionNodeTaints(t *testing.T) {
	t1 := corev1.Taint{Key: "example.com/a", Value: "v1", Effect: corev1.TaintEffectNoSchedule}
	t2 := corev1.Taint{Key: "example.com/b", Value: "v2", Effect: corev1.TaintEffectNoExecute}
	t3 := corev1.Taint{Key: "other.com/c", Value: "v3", Effect: corev1.TaintEffectNoSchedule}

	allTaints := map[string][]corev1.Taint{
		"node1": {t1, t2, t3},
		"node2": {t1, t3},
		"node3": {t1, t2},
	}

	tests := []struct {
		name             string
		nodes            []string
		excludedPrefixes []string
		want             []partitionv1beta1.PartitionTaint
	}{
		{
			name:             "empty nodes",
			nodes:            []string{},
			excludedPrefixes: []string{"example.com/"},
			want:             nil,
		},
		{
			name:             "single node, filtered (exclude other.com/)",
			nodes:            []string{"node1"},
			excludedPrefixes: []string{"other.com/"},
			want: []partitionv1beta1.PartitionTaint{
				{Key: "example.com/a", Value: "v1", Effect: partitionv1beta1.TaintEffectNoSchedule},
				{Key: "example.com/b", Value: "v2", Effect: partitionv1beta1.TaintEffectNoExecute},
			},
		},
		{
			name:             "multiple nodes, union (exclude nothing)",
			nodes:            []string{"node1", "node2"},
			excludedPrefixes: []string{},
			want: []partitionv1beta1.PartitionTaint{
				{Key: "example.com/a", Value: "v1", Effect: partitionv1beta1.TaintEffectNoSchedule},
				{Key: "example.com/b", Value: "v2", Effect: partitionv1beta1.TaintEffectNoExecute},
				{Key: "other.com/c", Value: "v3", Effect: partitionv1beta1.TaintEffectNoSchedule},
			},
		},
		{
			name:             "multiple nodes, union with filter (exclude other.com/)",
			nodes:            []string{"node1", "node2"},
			excludedPrefixes: []string{"other.com/"},
			want: []partitionv1beta1.PartitionTaint{
				{Key: "example.com/a", Value: "v1", Effect: partitionv1beta1.TaintEffectNoSchedule},
				{Key: "example.com/b", Value: "v2", Effect: partitionv1beta1.TaintEffectNoExecute},
			},
		},
		{
			name:             "union with filter (exclude example.com/)",
			nodes:            []string{"node2", "node3"},
			excludedPrefixes: []string{"example.com/"},
			want: []partitionv1beta1.PartitionTaint{
				{Key: "other.com/c", Value: "v3", Effect: partitionv1beta1.TaintEffectNoSchedule},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := unionNodeTaints(tt.nodes, allTaints, tt.excludedPrefixes, nil)
			if len(got) != len(tt.want) {
				t.Fatalf("unionNodeTaints() returned %d taints, want %d. Got: %+v", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("unionNodeTaints()[%d] = %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestRefreshAndPropagate_ResetTaints(t *testing.T) {
	pt := partitionv1beta1.PartitionTaint{Key: "k1", Value: "v1", Effect: partitionv1beta1.TaintEffectNoSchedule}
	pt2 := partitionv1beta1.PartitionTaint{Key: "k2", Value: "v2", Effect: partitionv1beta1.TaintEffectNoExecute}

	taintsMap := map[string][]partitionv1beta1.PartitionTaint{
		"root":  {pt},
		"child": {pt2},
	}

	t.Run("EnableTaints true", func(t *testing.T) {
		tree := &partitionv1beta1.PartitionInfo{
			PartitionID: "root",
			Taints:      []partitionv1beta1.PartitionTaint{{Key: "old", Value: "old", Effect: partitionv1beta1.TaintEffectNoSchedule}},
			ChildPartitions: []partitionv1beta1.PartitionInfo{
				{
					PartitionID: "child",
					Taints:      []partitionv1beta1.PartitionTaint{{Key: "old", Value: "old", Effect: partitionv1beta1.TaintEffectNoSchedule}},
				},
				{
					PartitionID: "child-no-taint",
					Taints:      []partitionv1beta1.PartitionTaint{{Key: "old", Value: "old", Effect: partitionv1beta1.TaintEffectNoSchedule}},
				},
			},
		}

		r := &PartitionTreeReconciler{
			EnableTaints: NewTaintConfig(true, TaintOptionNodeTaints),
			ConfigStore: config.NewStore(&config.Config{
				Taints: config.Taints{
					NodePropagation: config.NodePropagation{
						ExcludedPrefixes: nil,
					},
				},
			}),
		}
		now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
		wantNow := metav1.NewTime(now)
		r.refreshAndPropagate(tree, cache.LeafSnapshot{}, taintsMap, nil, now, false, nil)

		wantPT := partitionv1beta1.PartitionTaint{Key: "k1", Value: "v1", Effect: partitionv1beta1.TaintEffectNoSchedule, TimeAdded: &wantNow}
		wantPT2 := partitionv1beta1.PartitionTaint{Key: "k2", Value: "v2", Effect: partitionv1beta1.TaintEffectNoExecute, TimeAdded: &wantNow}
		if len(tree.Taints) != 1 || !reflect.DeepEqual(tree.Taints[0], wantPT) {
			t.Errorf("root taints = %+v, want [%+v]", tree.Taints, wantPT)
		}
		if len(tree.ChildPartitions[0].Taints) != 1 || !reflect.DeepEqual(tree.ChildPartitions[0].Taints[0], wantPT2) {
			t.Errorf("child taints = %+v, want [%+v]", tree.ChildPartitions[0].Taints, wantPT2)
		}
		if len(tree.ChildPartitions[1].Taints) != 0 {
			t.Errorf("child-no-taint taints = %+v, want empty", tree.ChildPartitions[1].Taints)
		}
	})

	t.Run("EnableTaints false", func(t *testing.T) {
		tree := &partitionv1beta1.PartitionInfo{
			PartitionID: "root",
			Taints:      []partitionv1beta1.PartitionTaint{{Key: "old", Value: "old", Effect: partitionv1beta1.TaintEffectNoSchedule}},
			ChildPartitions: []partitionv1beta1.PartitionInfo{
				{
					PartitionID: "child",
					Taints:      []partitionv1beta1.PartitionTaint{{Key: "old", Value: "old", Effect: partitionv1beta1.TaintEffectNoSchedule}},
				},
			},
		}

		r := &PartitionTreeReconciler{
			EnableTaints: NewTaintConfig(false),
		}
		r.refreshAndPropagate(tree, cache.LeafSnapshot{}, taintsMap, nil, time.Now(), false, nil)

		if len(tree.Taints) != 0 {
			t.Errorf("root taints = %+v, want empty", tree.Taints)
		}
		if len(tree.ChildPartitions[0].Taints) != 0 {
			t.Errorf("child taints = %+v, want empty", tree.ChildPartitions[0].Taints)
		}
	})
}

func TestMergeTaints(t *testing.T) {
	t1 := partitionv1beta1.PartitionTaint{Key: "k1", Value: "v1", Effect: partitionv1beta1.TaintEffectNoSchedule}
	t2 := partitionv1beta1.PartitionTaint{Key: "k2", Value: "v2", Effect: partitionv1beta1.TaintEffectNoExecute}
	t3 := partitionv1beta1.PartitionTaint{Key: "k1", Value: "v1", Effect: partitionv1beta1.TaintEffectNoSchedule} // duplicate of t1

	tests := []struct {
		name string
		a    []partitionv1beta1.PartitionTaint
		b    []partitionv1beta1.PartitionTaint
		want []partitionv1beta1.PartitionTaint
	}{
		{
			name: "both nil",
			a:    nil,
			b:    nil,
			want: nil,
		},
		{
			name: "one nil",
			a:    []partitionv1beta1.PartitionTaint{t1},
			b:    nil,
			want: []partitionv1beta1.PartitionTaint{t1},
		},
		{
			name: "no duplicates",
			a:    []partitionv1beta1.PartitionTaint{t1},
			b:    []partitionv1beta1.PartitionTaint{t2},
			want: []partitionv1beta1.PartitionTaint{t1, t2},
		},
		{
			name: "with duplicates in b",
			a:    []partitionv1beta1.PartitionTaint{t1},
			b:    []partitionv1beta1.PartitionTaint{t2, t3},
			want: []partitionv1beta1.PartitionTaint{t1, t2},
		},
		{
			name: "with duplicates in a",
			a:    []partitionv1beta1.PartitionTaint{t1, t3},
			b:    []partitionv1beta1.PartitionTaint{t2},
			want: []partitionv1beta1.PartitionTaint{t1, t2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mergeTaints(tt.a, tt.b)
			if len(got) != len(tt.want) {
				t.Fatalf("mergeTaints() returned %d taints, want %d. Got: %+v", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("mergeTaints()[%d] = %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestMergeTaints_KeepsEarliestTimeAddedWithoutAliasing(t *testing.T) {
	earlier := metav1.NewTime(time.Date(2026, 11, 3, 12, 1, 0, 0, time.UTC))
	wantEarliest := earlier
	later := metav1.NewTime(time.Date(2026, 11, 3, 12, 5, 0, 0, time.UTC))

	a := []partitionv1beta1.PartitionTaint{{
		Key:       corev1.TaintNodeNotReady,
		Effect:    partitionv1beta1.TaintEffectNoExecute,
		TimeAdded: &later,
	}}
	b := []partitionv1beta1.PartitionTaint{{
		Key:       corev1.TaintNodeNotReady,
		Effect:    partitionv1beta1.TaintEffectNoExecute,
		TimeAdded: &earlier,
	}}

	got := mergeTaints(a, b)
	if len(got) != 1 || got[0].TimeAdded == nil || !got[0].TimeAdded.Equal(&wantEarliest) {
		t.Fatalf("mergeTaints() = %+v, want single taint with earliest TimeAdded %v", got, wantEarliest)
	}

	// Mutating input TimeAdded pointer must not affect merged output.
	*b[0].TimeAdded = metav1.NewTime(time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC))
	if !got[0].TimeAdded.Equal(&wantEarliest) {
		t.Errorf("mergeTaints() output aliased input TimeAdded pointer: got %v, want %v", got[0].TimeAdded, wantEarliest)
	}
}

func TestStateToTaint(t *testing.T) {
	tests := []struct {
		state partitionv1beta1.PartitionStateType
		want  *partitionv1beta1.PartitionTaint
	}{
		{
			state: partitionv1beta1.PartitionStateUnhealthy,
			want: &partitionv1beta1.PartitionTaint{
				Key:    TaintKeyPartitionState,
				Value:  "Unhealthy",
				Effect: partitionv1beta1.TaintEffectNoExecute,
			},
		},
		{
			state: partitionv1beta1.PartitionStateDegraded,
			want: &partitionv1beta1.PartitionTaint{
				Key:    TaintKeyPartitionState,
				Value:  "Degraded",
				Effect: partitionv1beta1.TaintEffectPreferNoSchedule,
			},
		},
		{
			state: partitionv1beta1.PartitionStateIncomplete,
			want: &partitionv1beta1.PartitionTaint{
				Key:    TaintKeyPartitionState,
				Value:  "Incomplete",
				Effect: partitionv1beta1.TaintEffectNoSchedule,
			},
		},
		{
			state: partitionv1beta1.PartitionStateUnknown,
			want: &partitionv1beta1.PartitionTaint{
				Key:    TaintKeyPartitionState,
				Value:  "Unknown",
				Effect: partitionv1beta1.TaintEffectNoSchedule,
			},
		},
		{
			state: partitionv1beta1.PartitionStateHealthy,
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			got := stateToTaint(tt.state)
			if (got == nil) != (tt.want == nil) {
				t.Fatalf("stateToTaint(%q) = %v, want %v", tt.state, got, tt.want)
			}
			if got != nil && *got != *tt.want {
				t.Errorf("stateToTaint(%q) = %+v, want %+v", tt.state, *got, *tt.want)
			}
		})
	}
}

func TestRefreshAndPropagate_RefreshLeafStatus(t *testing.T) {
	snap := cache.LeafSnapshot{
		Partitions: map[string][]string{
			"leaf1": {"node1", "node2"},
			"leaf2": {"node3"},
		},
		Ready: map[string]bool{
			"node1": true,
			"node2": true,
			"node3": false,
		},
	}

	oldTaint := partitionv1beta1.PartitionTaint{Key: "old", Value: "old", Effect: partitionv1beta1.TaintEffectNoSchedule}
	root := &partitionv1beta1.PartitionInfo{
		PartitionID: "root",
		Taints:      []partitionv1beta1.PartitionTaint{oldTaint},
		ChildPartitions: []partitionv1beta1.PartitionInfo{
			{
				PartitionID: "leaf1",
				Taints:      []partitionv1beta1.PartitionTaint{oldTaint},
			},
			{
				PartitionID: "leaf2",
			},
		},
	}

	now := time.Now()
	r := &PartitionTreeReconciler{
		EnableTaints: NewTaintConfig(false),
	}
	r.refreshAndPropagate(root, snap, nil, nil, now, false, nil)

	// Verify leaf1 status
	leaf1 := &root.ChildPartitions[0]
	if len(leaf1.NodeNames) != 2 || leaf1.NodeNames[0] != "node1" || leaf1.NodeNames[1] != "node2" {
		t.Errorf("leaf1 nodes = %v, want [node1, node2]", leaf1.NodeNames)
	}
	cond1 := meta.FindStatusCondition(leaf1.Conditions, partitionv1beta1.ConditionTypeNodeReady)
	if cond1 == nil || cond1.Status != metav1.ConditionTrue {
		t.Errorf("leaf1 NodeReady condition = %v, want True", cond1)
	}

	// Verify leaf2 status
	leaf2 := &root.ChildPartitions[1]
	if len(leaf2.NodeNames) != 1 || leaf2.NodeNames[0] != "node3" {
		t.Errorf("leaf2 nodes = %v, want [node3]", leaf2.NodeNames)
	}
	cond2 := meta.FindStatusCondition(leaf2.Conditions, partitionv1beta1.ConditionTypeNodeReady)
	if cond2 == nil || cond2.Status != metav1.ConditionFalse {
		t.Errorf("leaf2 NodeReady condition = %v, want False", cond2)
	}

	// Verify taints were cleared because EnableTaints = false
	if len(root.Taints) != 0 {
		t.Errorf("root taints not cleared, got %v", root.Taints)
	}
	if len(leaf1.Taints) != 0 {
		t.Errorf("leaf1 taints not cleared, got %v", leaf1.Taints)
	}
}

func TestRefreshAndPropagate_UpdateTreeTaints(t *testing.T) {
	externalTaints := map[string][]partitionv1beta1.PartitionTaint{
		"root": {
			{Key: "ext-root", Value: "v", Effect: partitionv1beta1.TaintEffectNoSchedule},
		},
		"leaf1": {
			{Key: "ext-leaf1", Value: "v", Effect: partitionv1beta1.TaintEffectNoSchedule},
		},
	}

	snap := cache.LeafSnapshot{
		Partitions: map[string][]string{
			"leaf1": {"node1"},
		},
		Taints: map[string][]corev1.Taint{
			"node1": {
				{Key: "node-taint", Value: "v", Effect: corev1.TaintEffectNoSchedule},
			},
		},
	}

	root := &partitionv1beta1.PartitionInfo{
		PartitionID: "root",
		ChildPartitions: []partitionv1beta1.PartitionInfo{
			{
				PartitionID: "leaf1",
			},
		},
	}

	// 1. Update with propagation enabled
	r := &PartitionTreeReconciler{
		EnableTaints: NewTaintConfig(true, TaintOptionNodeTaints),
		ConfigStore: config.NewStore(&config.Config{
			Taints: config.Taints{
				NodePropagation: config.NodePropagation{
					ExcludedPrefixes: nil,
				},
			},
		}),
	}
	r.refreshAndPropagate(root, snap, externalTaints, nil, time.Now(), false, nil)

	// Verify root has only external taint
	if len(root.Taints) != 1 || root.Taints[0].Key != "ext-root" {
		t.Errorf("root taints = %v, want [ext-root]", root.Taints)
	}

	// Verify leaf1 has both external and propagated taint
	leaf1 := &root.ChildPartitions[0]
	if len(leaf1.Taints) != 2 {
		t.Errorf("leaf1 taints = %v, want 2 taints", leaf1.Taints)
	}
	if leaf1.Taints[0].Key != "ext-leaf1" || leaf1.Taints[1].Key != "node-taint" {
		t.Errorf("leaf1 taints = %v, want [ext-leaf1, node-taint]", leaf1.Taints)
	}

	// 2. Update again with propagation disabled globally - should clear ALL taints
	r.EnableTaints = NewTaintConfig(false)
	r.refreshAndPropagate(root, snap, externalTaints, nil, time.Now(), false, nil)

	// Verify leaf1 has no taints now
	if len(leaf1.Taints) != 0 {
		t.Errorf("after global disable, leaf1 taints = %v, want empty", leaf1.Taints)
	}
	// Verify root has no taints now
	if len(root.Taints) != 0 {
		t.Errorf("after global disable, root taints = %v, want empty", root.Taints)
	}

	// 3. Re-enable globally to propagate taints again
	r.EnableTaints = NewTaintConfig(true, TaintOptionNodeTaints)
	r.refreshAndPropagate(root, snap, externalTaints, nil, time.Now(), false, nil)
	if len(leaf1.Taints) != 2 {
		t.Errorf("after re-enable, leaf1 taints = %v, want 2 taints", leaf1.Taints)
	}
}

func TestRefreshAndPropagate_FreezeTaints(t *testing.T) {
	snap := cache.LeafSnapshot{
		Partitions: map[string][]string{
			"leaf1": {"node1"},
		},
		Ready: map[string]bool{
			"node1": true,
		},
		Taints: map[string][]corev1.Taint{
			"node1": {
				{Key: "node-taint-new", Value: "v", Effect: corev1.TaintEffectNoSchedule},
			},
		},
	}

	oldTaint := partitionv1beta1.PartitionTaint{Key: "old-taint", Value: "v", Effect: partitionv1beta1.TaintEffectNoSchedule}
	root := &partitionv1beta1.PartitionInfo{
		PartitionID: "root",
		Taints:      []partitionv1beta1.PartitionTaint{oldTaint},
		ChildPartitions: []partitionv1beta1.PartitionInfo{
			{
				PartitionID: "leaf1",
				Taints:      []partitionv1beta1.PartitionTaint{oldTaint},
			},
		},
	}

	r := &PartitionTreeReconciler{
		EnableTaints: NewTaintConfig(true),
	}

	// We pass skipTaintUpdates = true
	r.refreshAndPropagate(root, snap, nil, nil, time.Now(), true /* skipTaintUpdates */, nil)

	// 1. Verify taints are UNCHANGED (frozen)
	if len(root.Taints) != 1 || root.Taints[0] != oldTaint {
		t.Errorf("root taints changed, got %v, want %v", root.Taints, oldTaint)
	}
	leaf1 := &root.ChildPartitions[0]
	if len(leaf1.Taints) != 1 || leaf1.Taints[0] != oldTaint {
		t.Errorf("leaf1 taints changed, got %v, want %v", leaf1.Taints, oldTaint)
	}

	// 2. Verify leaf status WAS updated
	if len(leaf1.NodeNames) != 1 || leaf1.NodeNames[0] != "node1" {
		t.Errorf("leaf1 node names not updated, got %v, want [node1]", leaf1.NodeNames)
	}
	cond := meta.FindStatusCondition(leaf1.Conditions, partitionv1beta1.ConditionTypeNodeReady)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Errorf("leaf1 NodeReady condition not updated, got %v", cond)
	}
}

func TestToPartitionTaintEffect(t *testing.T) {
	tests := []struct {
		input  corev1.TaintEffect
		expect partitionv1beta1.TaintEffect
	}{
		{corev1.TaintEffectNoSchedule, partitionv1beta1.TaintEffectNoSchedule},
		{corev1.TaintEffectPreferNoSchedule, partitionv1beta1.TaintEffectPreferNoSchedule},
		{corev1.TaintEffectNoExecute, partitionv1beta1.TaintEffectNoExecute},
		{corev1.TaintEffect("UnrecognizedEffect"), partitionv1beta1.TaintEffectNoSchedule},
	}

	for _, tc := range tests {
		got := toPartitionTaintEffect(tc.input, "test-node", "test-taint-key")
		if got != tc.expect {
			t.Errorf("toPartitionTaintEffect(%q) = %q, want %q", tc.input, got, tc.expect)
		}
	}
}

func TestRootPartitionIDForNode_V6e(t *testing.T) {
	labels := map[string]string{
		"cloud.google.com/gke-tpu-slice-16x16-id": "abc-123",
	}
	id, err := rootPartitionIDForNode("tpu-v6e-slice", labels)
	if err != nil {
		t.Fatal(err)
	}
	if id != "abc-123" {
		t.Errorf("got %q", id)
	}
}

func TestRootPartitionIDForNode_V7x(t *testing.T) {
	labels := map[string]string{
		"cloud.google.com/gke-tpu-partition-4x4x4-id": "def-456",
	}
	id, err := rootPartitionIDForNode("tpu7x", labels)
	if err != nil {
		t.Fatal(err)
	}
	if id != "def-456" {
		t.Errorf("got %q", id)
	}
}

func TestRootPartitionIDForNode_Unknown(t *testing.T) {
	_, err := rootPartitionIDForNode("tpu-v99z", nil)
	if err == nil {
		t.Error("expected error for unknown accelerator")
	}
}

func TestRootPartitionIDForNode_MissingLabel(t *testing.T) {
	labels := map[string]string{} // label present but value empty
	_, err := rootPartitionIDForNode("tpu-v6e-slice", labels)
	if err == nil {
		t.Error("expected error for missing label")
	}
}

func TestSanitizePartitionID_Valid(t *testing.T) {
	id, err := sanitizePartitionID("abc-123-def")
	if err != nil || id != "abc-123-def" {
		t.Errorf("got %q err=%v", id, err)
	}
}

func TestSanitizePartitionID_Uppercase(t *testing.T) {
	id, err := sanitizePartitionID("ABC-123")
	if err != nil || id != "abc-123" {
		t.Errorf("got %q err=%v", id, err)
	}
}

func TestSanitizePartitionID_SpecialChars(t *testing.T) {
	id, err := sanitizePartitionID("abc_123.def")
	if err != nil || id != "abc-123-def" {
		t.Errorf("got %q err=%v", id, err)
	}
}

func TestSanitizePartitionID_LeadingDash(t *testing.T) {
	id, err := sanitizePartitionID("-abc-123")
	if err != nil || id != "abc-123" {
		t.Errorf("got %q err=%v", id, err)
	}
}

func TestSanitizePartitionID_TooLong(t *testing.T) {
	long := ""
	for i := 0; i < 70; i++ {
		long += "a"
	}
	_, err := sanitizePartitionID(long)
	if err == nil {
		t.Error("expected error for too-long ID")
	}
}

func TestSanitizePartitionID_Empty(t *testing.T) {
	_, err := sanitizePartitionID("")
	if err == nil {
		t.Error("expected error for empty ID")
	}
}

// ---------------------------------------------------------------------------
// carryForwardConditions tests
// ---------------------------------------------------------------------------

func TestCarryForwardConditions_PreservesLastTransitionTime(t *testing.T) {
	oldTime := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	oldTree := &partitionv1beta1.PartitionInfo{
		PartitionID: "root",
		ChildPartitions: []partitionv1beta1.PartitionInfo{
			{
				PartitionID: "leaf-a",
				Conditions: []metav1.Condition{
					{
						Type:               partitionv1beta1.ConditionTypeNodeReady,
						Status:             metav1.ConditionTrue,
						Reason:             "NodesReady",
						LastTransitionTime: oldTime,
					},
				},
			},
			{
				PartitionID: "leaf-b",
				Conditions: []metav1.Condition{
					{
						Type:               partitionv1beta1.ConditionTypeNodeReady,
						Status:             metav1.ConditionFalse,
						Reason:             "NodesNotReady",
						LastTransitionTime: oldTime,
					},
				},
			},
		},
	}

	// Fresh tree from GCE — same structure, no conditions.
	newTree := &partitionv1beta1.PartitionInfo{
		PartitionID: "root",
		ChildPartitions: []partitionv1beta1.PartitionInfo{
			{PartitionID: "leaf-a"},
			{PartitionID: "leaf-b"},
		},
	}

	carryForwardConditions(oldTree, newTree)

	// Verify conditions were carried forward.
	for _, child := range newTree.ChildPartitions {
		if len(child.Conditions) != 1 {
			t.Fatalf("partition %s: got %d conditions, want 1", child.PartitionID, len(child.Conditions))
		}
		if !child.Conditions[0].LastTransitionTime.Equal(&oldTime) {
			t.Errorf("partition %s: LastTransitionTime = %v, want %v",
				child.PartitionID, child.Conditions[0].LastTransitionTime, oldTime)
		}
	}
}

func TestCarryForwardConditions_NewPartitionGetsNoConditions(t *testing.T) {
	oldTree := &partitionv1beta1.PartitionInfo{
		PartitionID: "root",
		ChildPartitions: []partitionv1beta1.PartitionInfo{
			{
				PartitionID: "leaf-a",
				Conditions: []metav1.Condition{
					{Type: partitionv1beta1.ConditionTypeNodeReady, Status: metav1.ConditionTrue},
				},
			},
		},
	}

	// New tree has an additional partition not in the old tree.
	newTree := &partitionv1beta1.PartitionInfo{
		PartitionID: "root",
		ChildPartitions: []partitionv1beta1.PartitionInfo{
			{PartitionID: "leaf-a"},
			{PartitionID: "leaf-new"},
		},
	}

	carryForwardConditions(oldTree, newTree)

	if len(newTree.ChildPartitions[0].Conditions) != 1 {
		t.Errorf("leaf-a: expected carried-forward condition, got %d", len(newTree.ChildPartitions[0].Conditions))
	}
	if len(newTree.ChildPartitions[1].Conditions) != 0 {
		t.Errorf("leaf-new: expected no conditions, got %d", len(newTree.ChildPartitions[1].Conditions))
	}
}

func TestCarryForwardConditions_NilOldTree(t *testing.T) {
	newTree := &partitionv1beta1.PartitionInfo{
		PartitionID: "root",
		ChildPartitions: []partitionv1beta1.PartitionInfo{
			{PartitionID: "leaf-a"},
		},
	}
	// Should not panic.
	carryForwardConditions(nil, newTree)

	if len(newTree.ChildPartitions[0].Conditions) != 0 {
		t.Errorf("expected no conditions when old tree is nil, got %d", len(newTree.ChildPartitions[0].Conditions))
	}
}

func TestApplySynced_MessageIsStable(t *testing.T) {
	now := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	r := syncedReconciler()
	cr := &partitionv1beta1.PartitionTree{}
	cfg := config.Default()
	state := pollState{lastSuccessfulAt: now}

	r.applySynced(cr, state, cfg, now, nil)

	c := meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeSynced)
	if c == nil {
		t.Fatal("Synced condition not set")
	}
	if c.Message != "in sync with GCE" {
		t.Errorf("Synced message = %q, want stable 'in sync with GCE'", c.Message)
	}

	// Call again with same state — message should not change.
	later := now.Add(8 * time.Second)
	state2 := pollState{lastSuccessfulAt: later}
	r.applySynced(cr, state2, cfg, later, nil)

	c2 := meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeSynced)
	if c2.Message != c.Message {
		t.Errorf("Synced message changed from %q to %q across polls", c.Message, c2.Message)
	}
	// LastTransitionTime should NOT have changed (status stayed True).
	if !c2.LastTransitionTime.Equal(&c.LastTransitionTime) {
		t.Errorf("LastTransitionTime changed from %v to %v without status change",
			c.LastTransitionTime, c2.LastTransitionTime)
	}
}

// tpuLeafNode returns a tpu7x Node in pool-a whose 2x2x1 partition label
// puts it in leaf "leaf1". allocatable is the google.com/tpu allocatable;
// "" leaves the resource absent (plugin never registered).
func tpuLeafNode(name string, ready bool, allocatable string) *corev1.Node {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				LabelTopologyMode:   LabelTopologyModeProvisionOnly,
				LabelNodePool:       "pool-a",
				LabelTPUAccelerator: "tpu7x",
				"cloud.google.com/gke-tpu-partition-2x2x1-id": "leaf1",
			},
			Annotations: map[string]string{
				AnnotationCreatedByMIG: "projects/1/zones/us/instanceGroupManagers/mig-a",
			},
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: status}},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("178"),
			},
		},
	}
	if allocatable != "" {
		n.Status.Allocatable[ResourceTPU] = resource.MustParse(allocatable)
	}
	return n
}

// tpuLeafSnapshot runs nodes through the given cache transform, parseNode
// and the topology cache, the same path the Node informer takes, and
// returns pool-a's leaf snapshot.
func tpuLeafSnapshot(t *testing.T, transform func(any) (any, error), nodes ...*corev1.Node) cache.LeafSnapshot {
	t.Helper()
	c := cache.New()
	for _, n := range nodes {
		obj, err := transform(n)
		if err != nil {
			t.Fatalf("transform(%s): %v", n.Name, err)
		}
		entry, ok := parseNode(obj.(*corev1.Node))
		if !ok {
			t.Fatalf("parseNode(%s) rejected the node", n.Name)
		}
		c.Upsert(n.Name, entry)
	}
	return c.PoolLeaves("pool-a")
}

func tpuLeafTree() *partitionv1beta1.PartitionInfo {
	return &partitionv1beta1.PartitionInfo{
		PartitionID:     "root",
		ChildPartitions: []partitionv1beta1.PartitionInfo{{PartitionID: "leaf1"}},
	}
}

func TestRefreshAndPropagate_TPUAllocatableReadiness(t *testing.T) {
	tests := []struct {
		name        string
		flag        bool
		nodes       []*corev1.Node
		wantStatus  metav1.ConditionStatus
		wantReason  string
		wantMessage string
	}{
		{
			name:        "flag-on-zero-allocatable",
			flag:        true,
			nodes:       []*corev1.Node{tpuLeafNode("n1", true, "4"), tpuLeafNode("n2", true, "0")},
			wantStatus:  metav1.ConditionFalse,
			wantReason:  tree.ReasonTPUsNotAllocatable,
			wantMessage: "nodes with TPU chips not allocatable: [n2 (0/4)]",
		},
		{
			name:        "flag-on-absent-allocatable",
			flag:        true,
			nodes:       []*corev1.Node{tpuLeafNode("n1", true, "")},
			wantStatus:  metav1.ConditionFalse,
			wantReason:  tree.ReasonTPUsNotAllocatable,
			wantMessage: "nodes with TPU chips not allocatable: [n1 (0/4)]",
		},
		{
			name:       "flag-on-full-allocatable",
			flag:       true,
			nodes:      []*corev1.Node{tpuLeafNode("n1", true, "4"), tpuLeafNode("n2", true, "4")},
			wantStatus: metav1.ConditionTrue,
			wantReason: tree.ReasonNodesReady,
		},
		{
			name:       "flag-on-not-ready-wins",
			flag:       true,
			nodes:      []*corev1.Node{tpuLeafNode("n1", false, "0"), tpuLeafNode("n2", true, "0")},
			wantStatus: metav1.ConditionFalse,
			wantReason: tree.ReasonNodesNotReady,
		},
		{
			name:        "flag-off-zero-allocatable-is-ready",
			flag:        false,
			nodes:       []*corev1.Node{tpuLeafNode("n1", true, "0"), tpuLeafNode("n2", true, "")},
			wantStatus:  metav1.ConditionTrue,
			wantReason:  tree.ReasonNodesReady,
			wantMessage: "all expected nodes are Ready",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			transform := StripNodeForCache
			if tc.flag {
				transform = StripNodeForCacheKeepTPUAllocatable
			}
			snap := tpuLeafSnapshot(t, transform, tc.nodes...)
			root := tpuLeafTree()
			r := &PartitionTreeReconciler{
				EnableTaints:                  NewTaintConfig(true), // All, incl. NodesNotFound
				EnableTPUAllocatableReadiness: tc.flag,
			}
			r.refreshAndPropagate(root, snap, nil, nil, time.Now(), false, nil)

			leaf := &root.ChildPartitions[0]
			cond := meta.FindStatusCondition(leaf.Conditions, partitionv1beta1.ConditionTypeNodeReady)
			if cond == nil {
				t.Fatalf("leaf has no NodeReady condition")
			}
			if cond.Status != tc.wantStatus || cond.Reason != tc.wantReason {
				t.Errorf("NodeReady = %s/%s (%q), want %s/%s", cond.Status, cond.Reason, cond.Message, tc.wantStatus, tc.wantReason)
			}
			if tc.wantMessage != "" && cond.Message != tc.wantMessage {
				t.Errorf("NodeReady message = %q, want %q", cond.Message, tc.wantMessage)
			}
			// The chip check must never produce the NoExecute
			// nodes-not-found taint, even with every taint option on.
			for _, taint := range leaf.Taints {
				if taint.Key == TaintKeyNodesNotFound {
					t.Errorf("leaf carries %s taint for Reason %s: %v", TaintKeyNodesNotFound, cond.Reason, leaf.Taints)
				}
			}
		})
	}
}

// TestRefreshAndPropagate_TPUAllocatableKeepsTransitionTime walks a reboot:
// NotReady → Ready with 0 chips → Ready with 4 chips. The first step keeps
// Status=False, so lastTransitionTime must not move; the second flips to
// True and moves it.
func TestRefreshAndPropagate_TPUAllocatableKeepsTransitionTime(t *testing.T) {
	r := &PartitionTreeReconciler{
		EnableTaints:                  NewTaintConfig(true),
		EnableTPUAllocatableReadiness: true,
	}
	root := tpuLeafTree()
	leaf := &root.ChildPartitions[0]
	t0 := time.Date(2026, 10, 15, 10, 52, 0, 0, time.UTC)
	t1 := t0.Add(30 * time.Second)
	t2 := t1.Add(30 * time.Second)

	snap := tpuLeafSnapshot(t, StripNodeForCacheKeepTPUAllocatable, tpuLeafNode("n1", false, "0"))
	r.refreshAndPropagate(root, snap, nil, tree.ExtractTaints(root), t0, false, nil)
	cond := meta.FindStatusCondition(leaf.Conditions, partitionv1beta1.ConditionTypeNodeReady)
	if cond == nil || cond.Reason != tree.ReasonNodesNotReady || !cond.LastTransitionTime.Time.Equal(t0) {
		t.Fatalf("after NotReady: %+v, want NodesNotReady at %v", cond, t0)
	}

	snap = tpuLeafSnapshot(t, StripNodeForCacheKeepTPUAllocatable, tpuLeafNode("n1", true, "0"))
	r.refreshAndPropagate(root, snap, nil, tree.ExtractTaints(root), t1, false, nil)
	cond = meta.FindStatusCondition(leaf.Conditions, partitionv1beta1.ConditionTypeNodeReady)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != tree.ReasonTPUsNotAllocatable {
		t.Fatalf("after Ready with 0 chips: %+v, want False/TPUsNotAllocatable", cond)
	}
	if !cond.LastTransitionTime.Time.Equal(t0) {
		t.Errorf("lastTransitionTime moved on NodesNotReady -> TPUsNotAllocatable: got %v, want %v", cond.LastTransitionTime.Time, t0)
	}

	snap = tpuLeafSnapshot(t, StripNodeForCacheKeepTPUAllocatable, tpuLeafNode("n1", true, "4"))
	r.refreshAndPropagate(root, snap, nil, tree.ExtractTaints(root), t2, false, nil)
	cond = meta.FindStatusCondition(leaf.Conditions, partitionv1beta1.ConditionTypeNodeReady)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != tree.ReasonNodesReady {
		t.Fatalf("after 4 chips: %+v, want True/NodesReady", cond)
	}
	if !cond.LastTransitionTime.Time.Equal(t2) {
		t.Errorf("lastTransitionTime on False -> True: got %v, want %v", cond.LastTransitionTime.Time, t2)
	}
}

// TestFinalizePartitionTaints_NoPartitionLevelNodeHealthCarry checks that a
// partition never inherits an earlier node-health NoExecute TimeAdded from its
// previous status: the not-ready <-> unreachable swap is tracked per node by the
// node cache, so at partition level a different key or a later TimeAdded means
// another node or a new outage episode.
func TestFinalizePartitionTaints_NoPartitionLevelNodeHealthCarry(t *testing.T) {
	t0 := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	t0Meta := metav1.NewTime(t0)
	at := func(d time.Duration) *metav1.Time { m := metav1.NewTime(t0.Add(d)); return &m }
	prevNotReady := []partitionv1beta1.PartitionTaint{{
		Key:       corev1.TaintNodeNotReady,
		Effect:    partitionv1beta1.TaintEffectNoExecute,
		TimeAdded: &t0Meta,
	}}
	unstampedUnreachable := []partitionv1beta1.PartitionTaint{{
		Key:    corev1.TaintNodeUnreachable,
		Effect: partitionv1beta1.TaintEffectNoExecute,
	}}
	notReadyOn := func(nodes map[string]*metav1.Time) []partitionv1beta1.PartitionTaint {
		all := map[string][]corev1.Taint{}
		names := make([]string, 0, len(nodes))
		for n, ts := range nodes {
			names = append(names, n)
			if ts != nil {
				all[n] = []corev1.Taint{{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoExecute, TimeAdded: ts}}
			}
		}
		sort.Strings(names)
		return unionNodeTaints(names, all, nil, nil)
	}

	cases := []struct {
		name string
		node *partitionv1beta1.PartitionInfo
		key  string
		now  time.Duration
		want *metav1.Time
	}{
		{
			name: "interior partition: unstamped unreachable after not-ready is first seen now",
			node: &partitionv1beta1.PartitionInfo{PartitionID: "root", ChildPartitions: []partitionv1beta1.PartitionInfo{{PartitionID: "leaf"}},
				Taints: append([]partitionv1beta1.PartitionTaint(nil), unstampedUnreachable...)},
			key: corev1.TaintNodeUnreachable, now: 5 * time.Minute, want: at(5 * time.Minute),
		},
		{
			name: "leaf: unstamped unreachable after not-ready is first seen now",
			node: &partitionv1beta1.PartitionInfo{PartitionID: "leaf", Taints: append([]partitionv1beta1.PartitionTaint(nil), unstampedUnreachable...)},
			key:  corev1.TaintNodeUnreachable, now: 5 * time.Minute, want: at(5 * time.Minute),
		},
		{
			name: "multi-node leaf: n1 (NotReady since T0) recovered, n2 keeps its own T0+250s start",
			node: &partitionv1beta1.PartitionInfo{PartitionID: "leaf", NodeNames: []string{"n1", "n2"},
				Taints: notReadyOn(map[string]*metav1.Time{"n1": nil, "n2": at(250 * time.Second)})},
			key: corev1.TaintNodeNotReady, now: 260 * time.Second, want: at(250 * time.Second),
		},
		{
			name: "leaf: same key re-added after a recovery the controller missed starts a new episode",
			node: &partitionv1beta1.PartitionInfo{PartitionID: "leaf", NodeNames: []string{"n1"},
				Taints: notReadyOn(map[string]*metav1.Time{"n1": at(101 * time.Second)})},
			key: corev1.TaintNodeNotReady, now: 102 * time.Second, want: at(101 * time.Second),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			finalizePartitionTaints(tc.node, newTaintTimeResolver(prevNotReady, t0.Add(tc.now)))
			var got *metav1.Time
			for i := range tc.node.Taints {
				if tc.node.Taints[i].Key == tc.key {
					got = tc.node.Taints[i].TimeAdded
				}
			}
			if got == nil || !got.Equal(tc.want) {
				t.Errorf("%s TimeAdded = %v, want %v (never the previous status' T0=%v)", tc.key, got, tc.want, t0)
			}
		})
	}
}

// TestTaintTimeResolver_UnstampedSourceKeepsStartWhenExplicitSourceJoins checks
// that an unstamped taint present since T0 keeps T0 when another source (an
// annotation, or another node) adds the same taint with a later explicit T1.
func TestTaintTimeResolver_UnstampedSourceKeepsStartWhenExplicitSourceJoins(t *testing.T) {
	t0 := metav1.NewTime(time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC))
	t1 := metav1.NewTime(t0.Add(time.Minute))
	key := "example.com/maint"
	prev := []partitionv1beta1.PartitionTaint{{Key: key, Value: "true", Effect: partitionv1beta1.TaintEffectNoSchedule, TimeAdded: &t0}}
	unstamped := corev1.Taint{Key: key, Value: "true", Effect: corev1.TaintEffectNoSchedule}
	stamped := corev1.Taint{Key: key, Value: "true", Effect: corev1.TaintEffectNoSchedule, TimeAdded: &t1}

	for name, build := range map[string]func(*taintTimeResolver) []partitionv1beta1.PartitionTaint{
		"annotation joins node source": func(times *taintTimeResolver) []partitionv1beta1.PartitionTaint {
			annotation := []partitionv1beta1.PartitionTaint{{Key: key, Value: "true", Effect: partitionv1beta1.TaintEffectNoSchedule, TimeAdded: &t1}}
			nodes := unionNodeTaints([]string{"n0"}, map[string][]corev1.Taint{"n0": {unstamped}}, nil, times.episodeTimeFor)
			return mergeTaints(times.stamp(annotation), nodes)
		},
		"second node joins unstamped node": func(times *taintTimeResolver) []partitionv1beta1.PartitionTaint {
			return unionNodeTaints([]string{"n0", "n1"}, map[string][]corev1.Taint{"n0": {unstamped}, "n1": {stamped}}, nil, times.episodeTimeFor)
		},
	} {
		t.Run(name, func(t *testing.T) {
			times := newTaintTimeResolver(prev, t0.Add(2*time.Minute))
			leaf := &partitionv1beta1.PartitionInfo{PartitionID: "leaf", Taints: build(times)}
			finalizePartitionTaints(leaf, times)
			if len(leaf.Taints) != 1 || leaf.Taints[0].TimeAdded == nil || !leaf.Taints[0].TimeAdded.Equal(&t0) {
				t.Errorf("taints = %+v, want one %s with TimeAdded=%v", leaf.Taints, key, t0)
			}
		})
	}
}

// TestTaintTimeResolver_ExplicitSingleSourceEditTakesEffect checks that an
// explicit TimeAdded from the only source wins over the previous status, so an
// operator can still edit an annotation's timeAdded.
func TestTaintTimeResolver_ExplicitSingleSourceEditTakesEffect(t *testing.T) {
	t0 := metav1.NewTime(time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC))
	t1 := metav1.NewTime(t0.Add(time.Minute))
	key := "example.com/maint"
	prev := []partitionv1beta1.PartitionTaint{{Key: key, Value: "true", Effect: partitionv1beta1.TaintEffectNoSchedule, TimeAdded: &t0}}
	times := newTaintTimeResolver(prev, t0.Add(2*time.Minute))
	annotation := []partitionv1beta1.PartitionTaint{{Key: key, Value: "true", Effect: partitionv1beta1.TaintEffectNoSchedule, TimeAdded: &t1}}
	leaf := &partitionv1beta1.PartitionInfo{PartitionID: "leaf", Taints: times.stamp(annotation)}
	finalizePartitionTaints(leaf, times)
	if got := leaf.Taints[0].TimeAdded; got == nil || !got.Equal(&t1) {
		t.Errorf("edited annotation TimeAdded = %v, want %v", got, t1)
	}
}

func TestMergeTaints_MixedNilAndExplicitTimeAdded(t *testing.T) {
	ts := metav1.NewTime(time.Date(2026, 11, 3, 12, 10, 0, 0, time.UTC))
	unstamped := []partitionv1beta1.PartitionTaint{{
		Key:    corev1.TaintNodeNotReady,
		Effect: partitionv1beta1.TaintEffectNoExecute,
	}}
	stamped := []partitionv1beta1.PartitionTaint{{
		Key:       corev1.TaintNodeNotReady,
		Effect:    partitionv1beta1.TaintEffectNoExecute,
		TimeAdded: &ts,
	}}

	for name, got := range map[string][]partitionv1beta1.PartitionTaint{
		"nil-then-stamped": mergeTaints(unstamped, stamped),
		"stamped-then-nil": mergeTaints(stamped, unstamped),
	} {
		if len(got) != 1 || got[0].TimeAdded == nil || !got[0].TimeAdded.Equal(&ts) {
			t.Errorf("%s: mergeTaints() = %+v, want single taint with TimeAdded=%v", name, got, ts)
		}
	}
}

func TestUnionNodeTaints_KeepsEarliestTimeAddedAcrossNodes(t *testing.T) {
	early := metav1.NewTime(time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC))
	late := metav1.NewTime(early.Add(time.Minute))
	all := map[string][]corev1.Taint{
		"n1": {{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoExecute, TimeAdded: &late}},
		"n2": {{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoExecute, TimeAdded: &early}},
		"n3": {{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoExecute}},
	}
	for _, order := range [][]string{{"n1", "n2", "n3"}, {"n3", "n2", "n1"}} {
		got := unionNodeTaints(order, all, nil, nil)
		if len(got) != 1 || got[0].TimeAdded == nil || !got[0].TimeAdded.Equal(&early) {
			t.Errorf("unionNodeTaints(%v) = %+v, want one taint with TimeAdded=%v", order, got, early)
		}
	}
}
