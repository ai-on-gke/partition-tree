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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
	topocache "github.com/ai-on-gke/partition-tree/internal/cache"
)

// TestParseNode_PopulatesAcceleratorType verifies the full node parser
// path lifts the TPU label into NodeEntry.AcceleratorType alongside the
// existing four filter fields.
func TestParseNode_PopulatesAcceleratorType(t *testing.T) {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n1",
			Labels: map[string]string{
				LabelTopologyMode:   LabelTopologyModeProvisionOnly,
				LabelNodePool:       "poolA",
				LabelTPUAccelerator: "tpu7x",
				"cloud.google.com/gke-tpu-partition-x-id": "p0",
			},
			Annotations: map[string]string{
				AnnotationCreatedByMIG: "projects/1/zones/us/instanceGroupManagers/mig",
			},
		},
	}
	entry, ok := parseNode(n)
	if !ok {
		t.Fatalf("parseNode rejected a well-formed node")
	}
	if entry.AcceleratorType != "tpu7x" {
		t.Errorf("AcceleratorType = %q, want tpu7x", entry.AcceleratorType)
	}
}

// TestParseNode_PopulatesAcceleratorType_V6e ensures we extract the family
// string from the tpu-accelerator label for v6e.
func TestParseNode_PopulatesAcceleratorType_V6e(t *testing.T) {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n1",
			Labels: map[string]string{
				LabelTopologyMode:   LabelTopologyModeProvisionOnly,
				LabelNodePool:       "poolA",
				LabelTPUAccelerator: "tpu-v6e-slice",
				"cloud.google.com/gke-tpu-partition-x-id": "p0",
			},
			Annotations: map[string]string{
				AnnotationCreatedByMIG: "projects/1/zones/us/instanceGroupManagers/mig",
			},
		},
	}
	entry, ok := parseNode(n)
	if !ok {
		t.Fatalf("parseNode rejected a well-formed node")
	}
	if entry.AcceleratorType != "tpu-v6e-slice" {
		t.Errorf("AcceleratorType = %q, want tpu-v6e-slice", entry.AcceleratorType)
	}
}

// TestParseNode_V6eSliceLabelAdmitted ensures a v6e node that surfaces its
// partition hierarchy via cloud.google.com/gke-tpu-slice-<topology>-id
// labels (rather than the v7x "partition" labels) is admitted and its
// slice IDs are lifted into NodeEntry.PartitionIDs. This is the shared
// path used by both decoupled and non-decoupled lifecycle modes.
func TestParseNode_V6eSliceLabelAdmitted(t *testing.T) {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n1",
			Labels: map[string]string{
				LabelTopologyMode:   LabelTopologyModeProvisionOnly,
				LabelNodePool:       "poolA",
				LabelTPUAccelerator: "tpu-v6e-slice",
				"cloud.google.com/gke-tpu-slice-16x16-id": "root-abc",
				"cloud.google.com/gke-tpu-slice-2x2-id":   "leaf-0",
			},
			Annotations: map[string]string{
				AnnotationCreatedByMIG: "projects/1/zones/us/instanceGroupManagers/mig",
			},
		},
	}
	entry, ok := parseNode(n)
	if !ok {
		t.Fatalf("parseNode rejected a well-formed v6e node using slice labels")
	}
	if len(entry.PartitionIDs) != 2 {
		t.Fatalf("PartitionIDs = %v, want 2 entries", entry.PartitionIDs)
	}
}

// TestParseNode_MissingAcceleratorLabelRejected ensures a node
// that lacks the tpu-accelerator label is rejected.
func TestParseNode_MissingAcceleratorLabelRejected(t *testing.T) {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n1",
			Labels: map[string]string{
				LabelTopologyMode: LabelTopologyModeProvisionOnly,
				LabelNodePool:     "poolA",
				"cloud.google.com/gke-tpu-partition-x-id": "p0",
			},
			Annotations: map[string]string{
				AnnotationCreatedByMIG: "projects/1/zones/us/instanceGroupManagers/mig",
			},
		},
	}
	_, ok := parseNode(n)
	if ok {
		t.Fatalf("parseNode accepted a node missing the accelerator label")
	}
}

// TestParseNode_PopulatesTPUAllocatable verifies parseNode lifts the
// google.com/tpu allocatable quantity into NodeEntry.TPUAllocatable and
// reads an absent resource as 0. Other allocatable resources are ignored.
func TestParseNode_PopulatesTPUAllocatable(t *testing.T) {
	newNode := func(alloc corev1.ResourceList) *corev1.Node {
		return &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "n1",
				Labels: map[string]string{
					LabelTopologyMode:   LabelTopologyModeProvisionOnly,
					LabelNodePool:       "poolA",
					LabelTPUAccelerator: "tpu7x",
					"cloud.google.com/gke-tpu-partition-x-id": "p0",
				},
				Annotations: map[string]string{
					AnnotationCreatedByMIG: "projects/1/zones/us/instanceGroupManagers/mig",
				},
			},
			Status: corev1.NodeStatus{Allocatable: alloc},
		}
	}
	tests := []struct {
		name  string
		alloc corev1.ResourceList
		want  int64
	}{
		{name: "absent", alloc: nil, want: 0},
		{name: "only-cpu", alloc: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("178")}, want: 0},
		{name: "zero", alloc: corev1.ResourceList{ResourceTPU: resource.MustParse("0")}, want: 0},
		{name: "three", alloc: corev1.ResourceList{ResourceTPU: resource.MustParse("3")}, want: 3},
		{name: "fractional-truncates", alloc: corev1.ResourceList{ResourceTPU: resource.MustParse("3500m")}, want: 3},
		{name: "four", alloc: corev1.ResourceList{
			ResourceTPU:        resource.MustParse("4"),
			corev1.ResourceCPU: resource.MustParse("178"),
		}, want: 4},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entry, ok := parseNode(newNode(tc.alloc))
			if !ok {
				t.Fatalf("parseNode rejected a well-formed node")
			}
			if entry.TPUAllocatable != tc.want {
				t.Errorf("TPUAllocatable = %d, want %d", entry.TPUAllocatable, tc.want)
			}
		})
	}
}

// TestExpectedTPUChipsPerHost pins the per-accelerator chip count used by
// the --enable-tpu-allocatable-readiness check, and that every
// accelerator parseNode admits has an entry.
func TestExpectedTPUChipsPerHost(t *testing.T) {
	for accel, want := range map[string]int64{
		"tpu7x":         4,
		"tpu-v6e-slice": 4,
		"tpu-v5p-slice": 0,
		"":              0,
	} {
		if got := expectedTPUChipsPerHost(accel); got != want {
			t.Errorf("expectedTPUChipsPerHost(%q) = %d, want %d", accel, got, want)
		}
	}
	// topologyRootLabel's keys mirror the accelerators parseNode admits.
	for accel := range topologyRootLabel {
		if expectedTPUChipsPerHost(accel) == 0 {
			t.Errorf("accelerator %q admitted but has no chips-per-host entry", accel)
		}
	}
}

func TestIsPartitionLabel(t *testing.T) {
	tests := []struct {
		key  string
		want bool
	}{
		{"cloud.google.com/gke-tpu-partition-4x4x4-id", true},
		{"cloud.google.com/gke-tpu-partition-2x2x1-id", true},
		{"cloud.google.com/gke-tpu-slice-16x16-id", true},   // v6e slice label
		{"cloud.google.com/gke-tpu-slice-4x4-id", true},     // v6e slice label
		{"cloud.google.com/gke-tpu-partition--id", false},   // empty topology
		{"cloud.google.com/gke-tpu-slice--id", false},       // empty topology
		{"cloud.google.com/gke-tpu-partition-4x4x4", false}, // missing suffix
		{"cloud.google.com/gke-tpu-slice-16x16", false},     // missing suffix
		{"gke-tpu-partition-4x4x4-id", false},               // missing prefix
		{"other-label", false},
	}

	for _, tt := range tests {
		if got := isPartitionLabel(tt.key); got != tt.want {
			t.Errorf("isPartitionLabel(%q) = %v, want %v", tt.key, got, tt.want)
		}
	}
}

func TestSamePartitionLabels(t *testing.T) {
	tests := []struct {
		name string
		a    map[string]string
		b    map[string]string
		want bool
	}{
		{
			name: "identical",
			a:    map[string]string{"cloud.google.com/gke-tpu-partition-4x4x4-id": "p0"},
			b:    map[string]string{"cloud.google.com/gke-tpu-partition-4x4x4-id": "p0"},
			want: true,
		},
		{
			name: "different values",
			a:    map[string]string{"cloud.google.com/gke-tpu-partition-4x4x4-id": "p0"},
			b:    map[string]string{"cloud.google.com/gke-tpu-partition-4x4x4-id": "p1"},
			want: false,
		},
		{
			name: "different keys",
			a:    map[string]string{"cloud.google.com/gke-tpu-partition-4x4x4-id": "p0"},
			b:    map[string]string{"cloud.google.com/gke-tpu-partition-2x2x1-id": "p0"},
			want: false,
		},
		{
			name: "one empty",
			a:    map[string]string{"cloud.google.com/gke-tpu-partition-4x4x4-id": "p0"},
			b:    map[string]string{},
			want: false,
		},
		{
			name: "non-partition labels ignored",
			a:    map[string]string{"cloud.google.com/gke-tpu-partition-4x4x4-id": "p0", "other": "v1"},
			b:    map[string]string{"cloud.google.com/gke-tpu-partition-4x4x4-id": "p0", "other": "v2"},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := samePartitionLabels(tt.a, tt.b); got != tt.want {
				t.Errorf("samePartitionLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMatchingNodesPredicate(t *testing.T) {
	p := matchingNodesPredicate()

	// Helper to create a node with topology mode
	nodeWithMode := func(name, mode string) *corev1.Node {
		return &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   name,
				Labels: map[string]string{LabelTopologyMode: mode},
			},
		}
	}

	// Test Create
	if !p.Create(event.TypedCreateEvent[*corev1.Node]{Object: nodeWithMode("n1", LabelTopologyModeProvisionOnly)}) {
		t.Error("Create should admit node with PROVISION_ONLY")
	}
	if p.Create(event.TypedCreateEvent[*corev1.Node]{Object: nodeWithMode("n1", "other")}) {
		t.Error("Create should reject node without PROVISION_ONLY")
	}

	// Test Delete
	if !p.Delete(event.TypedDeleteEvent[*corev1.Node]{Object: &corev1.Node{}}) {
		t.Error("Delete should always admit")
	}

	// Test Update
	nOld := nodeWithMode("n1", LabelTopologyModeProvisionOnly)
	nNew := nodeWithMode("n1", LabelTopologyModeProvisionOnly)

	// No change
	if p.Update(event.TypedUpdateEvent[*corev1.Node]{ObjectOld: nOld, ObjectNew: nNew}) {
		t.Error("Update should reject no-op change")
	}

	// Ready change
	nNewReady := nNew.DeepCopy()
	nNewReady.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	if !p.Update(event.TypedUpdateEvent[*corev1.Node]{ObjectOld: nOld, ObjectNew: nNewReady}) {
		t.Error("Update should admit Ready change")
	}

	// Label change
	nNewLabel := nNew.DeepCopy()
	nNewLabel.Labels["cloud.google.com/gke-tpu-partition-4x4x4-id"] = "p0"
	if !p.Update(event.TypedUpdateEvent[*corev1.Node]{ObjectOld: nOld, ObjectNew: nNewLabel}) {
		t.Error("Update should admit Label change")
	}

	// Taint change
	nNewTaint := nNew.DeepCopy()
	nNewTaint.Spec.Taints = []corev1.Taint{{Key: "test-key", Value: "value", Effect: corev1.TaintEffectNoSchedule}}
	if !p.Update(event.TypedUpdateEvent[*corev1.Node]{ObjectOld: nOld, ObjectNew: nNewTaint}) {
		t.Error("Update should admit Taint change")
	}

	// Taint TimeAdded change (same Key/Value/Effect, different TimeAdded)
	t1 := metav1.NewTime(time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC))
	t2 := metav1.NewTime(time.Date(2026, 11, 3, 12, 1, 0, 0, time.UTC))
	nTaintT1 := nNew.DeepCopy()
	nTaintT1.Spec.Taints = []corev1.Taint{{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoExecute, TimeAdded: &t1}}
	nTaintT1Same := nTaintT1.DeepCopy()
	if p.Update(event.TypedUpdateEvent[*corev1.Node]{ObjectOld: nTaintT1, ObjectNew: nTaintT1Same}) {
		t.Error("Update should reject identical Taint TimeAdded")
	}
	nTaintT2 := nNew.DeepCopy()
	nTaintT2.Spec.Taints = []corev1.Taint{{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoExecute, TimeAdded: &t2}}
	if !p.Update(event.TypedUpdateEvent[*corev1.Node]{ObjectOld: nTaintT1, ObjectNew: nTaintT2}) {
		t.Error("Update should admit Taint TimeAdded change")
	}
	nTaintNil := nNew.DeepCopy()
	nTaintNil.Spec.Taints = []corev1.Taint{{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoExecute}}
	if !p.Update(event.TypedUpdateEvent[*corev1.Node]{ObjectOld: nTaintNil, ObjectNew: nTaintT1}) {
		t.Error("Update should admit nil -> non-nil Taint TimeAdded change")
	}

	// Accelerator label change
	nNewAccel := nNew.DeepCopy()
	nNewAccel.Labels[LabelTPUAccelerator] = "tpu-v6e-slice"
	if !p.Update(event.TypedUpdateEvent[*corev1.Node]{ObjectOld: nOld, ObjectNew: nNewAccel}) {
		t.Error("Update should admit TPU accelerator label change")
	}

	// Topology block label change
	nNewBlock := nNew.DeepCopy()
	nNewBlock.Labels[LabelTopologyBlock] = "block-1"
	if !p.Update(event.TypedUpdateEvent[*corev1.Node]{ObjectOld: nOld, ObjectNew: nNewBlock}) {
		t.Error("Update should admit Topology block label change")
	}
}

// TestMatchingNodesPredicate_TPUAllocatable verifies the Update predicate
// admits a change in the google.com/tpu allocatable quantity (absent and
// present count as different, even at 0) and still ignores changes to
// other allocatable resources.
func TestMatchingNodesPredicate_TPUAllocatable(t *testing.T) {
	p := matchingNodesPredicate()
	withAlloc := func(alloc corev1.ResourceList) *corev1.Node {
		return &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "n1",
				Labels: map[string]string{LabelTopologyMode: LabelTopologyModeProvisionOnly},
			},
			Status: corev1.NodeStatus{
				Conditions:  []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
				Allocatable: alloc,
			},
		}
	}
	tpu := func(v string) corev1.ResourceList {
		return corev1.ResourceList{ResourceTPU: resource.MustParse(v)}
	}
	cpu := func(v string) corev1.ResourceList {
		return corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(v)}
	}

	tests := []struct {
		name     string
		old, new corev1.ResourceList
		want     bool
	}{
		{name: "absent-to-4", old: nil, new: tpu("4"), want: true},
		{name: "4-to-absent", old: tpu("4"), new: nil, want: true},
		{name: "absent-to-0", old: nil, new: tpu("0"), want: true},
		{name: "0-to-4", old: tpu("0"), new: tpu("4"), want: true},
		{name: "4-to-3", old: tpu("4"), new: tpu("3"), want: true},
		{name: "4-to-4", old: tpu("4"), new: tpu("4"), want: false},
		{name: "4-to-4-different-format", old: tpu("4"), new: tpu("4000m"), want: false},
		{name: "absent-to-absent", old: nil, new: nil, want: false},
		{name: "cpu-only-change", old: cpu("178"), new: cpu("170"), want: false},
		{
			name: "cpu-change-tpu-unchanged",
			old:  corev1.ResourceList{ResourceTPU: resource.MustParse("4"), corev1.ResourceCPU: resource.MustParse("178")},
			new:  corev1.ResourceList{ResourceTPU: resource.MustParse("4"), corev1.ResourceCPU: resource.MustParse("170")},
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := p.Update(event.TypedUpdateEvent[*corev1.Node]{
				ObjectOld: withAlloc(tc.old),
				ObjectNew: withAlloc(tc.new),
			})
			if got != tc.want {
				t.Errorf("Update(%v -> %v) = %v, want %v", tc.old, tc.new, got, tc.want)
			}
		})
	}
}

func TestSameTaints(t *testing.T) {
	t1 := corev1.Taint{Key: "k1", Value: "v1", Effect: corev1.TaintEffectNoSchedule}
	t2 := corev1.Taint{Key: "k2", Value: "v2", Effect: corev1.TaintEffectNoExecute}
	t3 := corev1.Taint{Key: "k1", Value: "v1", Effect: corev1.TaintEffectNoSchedule} // duplicate of t1

	tests := []struct {
		name string
		a    []corev1.Taint
		b    []corev1.Taint
		want bool
	}{
		{
			name: "both nil",
			a:    nil,
			b:    nil,
			want: true,
		},
		{
			name: "both empty",
			a:    []corev1.Taint{},
			b:    []corev1.Taint{},
			want: true,
		},
		{
			name: "one nil, one empty",
			a:    nil,
			b:    []corev1.Taint{},
			want: true,
		},
		{
			name: "identical single",
			a:    []corev1.Taint{t1},
			b:    []corev1.Taint{t1},
			want: true,
		},
		{
			name: "identical multiple same order",
			a:    []corev1.Taint{t1, t2},
			b:    []corev1.Taint{t1, t2},
			want: true,
		},
		{
			name: "identical multiple different order",
			a:    []corev1.Taint{t1, t2},
			b:    []corev1.Taint{t2, t1},
			want: true,
		},
		{
			name: "different lengths",
			a:    []corev1.Taint{t1},
			b:    []corev1.Taint{t1, t2},
			want: false,
		},
		{
			name: "different keys",
			a:    []corev1.Taint{t1},
			b:    []corev1.Taint{{Key: "other", Value: "v1", Effect: corev1.TaintEffectNoSchedule}},
			want: false,
		},
		{
			name: "different values",
			a:    []corev1.Taint{t1},
			b:    []corev1.Taint{{Key: "k1", Value: "other", Effect: corev1.TaintEffectNoSchedule}},
			want: false,
		},
		{
			name: "different effects",
			a:    []corev1.Taint{t1},
			b:    []corev1.Taint{{Key: "k1", Value: "v1", Effect: corev1.TaintEffectNoExecute}},
			want: false,
		},
		{
			name: "duplicate taints handled correctly",
			a:    []corev1.Taint{t1, t3},
			b:    []corev1.Taint{t1, t1},
			want: true,
		},
		{
			name: "duplicate taints count mismatch",
			a:    []corev1.Taint{t1, t1},
			b:    []corev1.Taint{t1},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sameTaints(tt.a, tt.b); got != tt.want {
				t.Errorf("sameTaints() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TopologyBlock / slice-scheduler integration tests
// ---------------------------------------------------------------------------

// TestParseNode_PopulatesTopologyBlock verifies that the GCE topology
// block label (cloud.google.com/gce-topology-block) is extracted by
// parseNode and propagated into the NodeEntry. The slice-scheduler reads
// this value (via the PartitionTree CR's cloud.google.com/tpu-pod-id label)
// to group cubes for multi-cube scheduling.
func TestParseNode_PopulatesTopologyBlock(t *testing.T) {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n1",
			Labels: map[string]string{
				LabelTopologyMode:   LabelTopologyModeProvisionOnly,
				LabelNodePool:       "poolA",
				LabelTPUAccelerator: "tpu7x",
				LabelTopologyBlock:  "block-us-central2-b-42",
				"cloud.google.com/gke-tpu-partition-4x4x4-id": "p0",
			},
			Annotations: map[string]string{
				AnnotationCreatedByMIG: "projects/1/zones/us-central2-b/instanceGroupManagers/mig",
			},
		},
	}
	entry, ok := parseNode(n)
	if !ok {
		t.Fatalf("parseNode rejected a well-formed node with topology block")
	}
	if entry.TopologyBlock != "block-us-central2-b-42" {
		t.Errorf("TopologyBlock = %q, want %q", entry.TopologyBlock, "block-us-central2-b-42")
	}
}

// TestParseNode_MissingTopologyBlock verifies that parseNode still
// succeeds for nodes that lack the topology block label (it's optional).
// TopologyBlock should be empty, not cause rejection.
func TestParseNode_MissingTopologyBlock(t *testing.T) {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n1",
			Labels: map[string]string{
				LabelTopologyMode:   LabelTopologyModeProvisionOnly,
				LabelNodePool:       "poolA",
				LabelTPUAccelerator: "tpu7x",
				// No LabelTopologyBlock
				"cloud.google.com/gke-tpu-partition-4x4x4-id": "p0",
			},
			Annotations: map[string]string{
				AnnotationCreatedByMIG: "projects/1/zones/us-central2-b/instanceGroupManagers/mig",
			},
		},
	}
	entry, ok := parseNode(n)
	if !ok {
		t.Fatalf("parseNode should accept nodes without topology block (it's optional)")
	}
	if entry.TopologyBlock != "" {
		t.Errorf("TopologyBlock = %q, want empty", entry.TopologyBlock)
	}
}

// TestParseNode_CopiesTaints verifies that parseNode defensively copies
// the taints slice from the Node object, so that subsequent mutations to
// the Node's taints do not affect the cached NodeEntry.
func TestParseNode_CopiesTaints(t *testing.T) {
	taint1 := corev1.Taint{Key: "k1", Value: "v1", Effect: corev1.TaintEffectNoSchedule}
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n1",
			Labels: map[string]string{
				LabelTopologyMode:   LabelTopologyModeProvisionOnly,
				LabelNodePool:       "poolA",
				LabelTPUAccelerator: "tpu7x",
				"cloud.google.com/gke-tpu-partition-x-id": "p0",
			},
			Annotations: map[string]string{
				AnnotationCreatedByMIG: "projects/1/zones/us/instanceGroupManagers/mig",
			},
		},
		Spec: corev1.NodeSpec{
			Taints: []corev1.Taint{taint1},
		},
	}
	entry, ok := parseNode(n)
	if !ok {
		t.Fatalf("parseNode rejected a well-formed node")
	}

	if !sameTaints(entry.Taints, n.Spec.Taints) {
		t.Errorf("Taints = %v, want %v", entry.Taints, n.Spec.Taints)
	}

	// Mutate the original taints to verify defensive copy
	n.Spec.Taints[0].Value = "mutated"

	if entry.Taints[0].Value == "mutated" {
		t.Errorf("entry.Taints was mutated when original taints were changed, copy was not defensive")
	}
}

// ---------------------------------------------------------------------------
// nodeEventHandler — decoupled mode tests
// ---------------------------------------------------------------------------

// fakeQueue is a minimal workqueue that just records Add calls.
type fakeQueue struct {
	items []reconcile.Request
}

func newFakeQueue() *fakeQueue                                 { return &fakeQueue{} }
func (q *fakeQueue) Add(item reconcile.Request)                { q.items = append(q.items, item) }
func (q *fakeQueue) Len() int                                  { return len(q.items) }
func (q *fakeQueue) Get() (reconcile.Request, bool)            { return reconcile.Request{}, false }
func (q *fakeQueue) Done(reconcile.Request)                    {}
func (q *fakeQueue) ShutDown()                                 {}
func (q *fakeQueue) ShutDownWithDrain()                        {}
func (q *fakeQueue) ShuttingDown() bool                        { return false }
func (q *fakeQueue) AddAfter(reconcile.Request, time.Duration) {}
func (q *fakeQueue) AddRateLimited(reconcile.Request)          {}
func (q *fakeQueue) Forget(reconcile.Request)                  {}
func (q *fakeQueue) NumRequeues(reconcile.Request) int         { return 0 }

// makeV7xNode builds a well-formed v7x node with the given name, pool,
// MIG, and root partition label.
func makeV7xNode(name, pool, mig, rootPartID string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				LabelTopologyMode:   LabelTopologyModeProvisionOnly,
				LabelNodePool:       pool,
				LabelTPUAccelerator: "tpu7x",
				// v7x root partition label
				"cloud.google.com/gke-tpu-partition-4x4x4-id": rootPartID,
				// leaf partition label
				"cloud.google.com/gke-tpu-partition-2x2x1-id": "leaf-0",
			},
			Annotations: map[string]string{
				AnnotationCreatedByMIG: mig,
			},
		},
	}
}

// TestNodeHandler_Decoupled_CreateEnqueuesPartitionID verifies that in
// decoupled mode, creating a node enqueues the sanitized root partition
// ID (not the pool name).
func TestNodeHandler_Decoupled_CreateEnqueuesPartitionID(t *testing.T) {
	c := topocache.New()
	ptCh := make(chan event.TypedGenericEvent[*partitionv1beta1.PartitionTree], 10)
	h := nodeEventHandler(c, true, ptCh)

	n := makeV7xNode("node-0", "my-pool", "projects/1/zones/us/instanceGroupManagers/mig", "ROOT-ABC")
	q := newFakeQueue()

	h.Create(ctxBG(), event.TypedCreateEvent[*corev1.Node]{Object: n}, q)

	// Should enqueue the sanitized partition ID, not pool name.
	if len(q.items) != 1 {
		t.Fatalf("enqueued %d items, want 1", len(q.items))
	}
	got := q.items[0].Name
	// "ROOT-ABC" → sanitized to "root-abc"
	if got != "root-abc" {
		t.Errorf("enqueued name = %q, want %q", got, "root-abc")
	}

	// Pool→partition mapping should be set.
	pid, ok := c.PoolRootPartitionID("my-pool")
	if !ok || pid != "root-abc" {
		t.Errorf("pool→partition = %q ok=%v, want root-abc", pid, ok)
	}

	// When no CR exists in cache, handler should push a create update.
	updates := c.DrainUpdates("root-abc")
	if len(updates) != 1 {
		t.Fatalf("pending updates = %d, want 1 (create update for new partition)", len(updates))
	}
	if updates[0].CRName != "root-abc" {
		t.Errorf("update CRName = %q, want %q", updates[0].CRName, "root-abc")
	}
	if updates[0].MIG != "projects/1/zones/us/instanceGroupManagers/mig" {
		t.Errorf("update MIG = %q", updates[0].MIG)
	}

	// Should send event on ptUpdateCh for the create.
	select {
	case evt := <-ptCh:
		if evt.Object.Name != "root-abc" {
			t.Errorf("ptUpdate event name = %q, want %q", evt.Object.Name, "root-abc")
		}
	default:
		t.Error("expected ptUpdate event for create, got none")
	}
}

// TestNodeHandler_Decoupled_MIGChangeDetection verifies that when a CR
// exists in the cache with a different MIG, the handler pushes an update
// and sends an event on ptUpdateCh.
func TestNodeHandler_Decoupled_MIGChangeDetection(t *testing.T) {
	c := topocache.New()
	ptCh := make(chan event.TypedGenericEvent[*partitionv1beta1.PartitionTree], 10)
	h := nodeEventHandler(c, true, ptCh)

	// Pre-populate: CR exists with old MIG.
	c.SetPartitionCR("root-abc", "root-abc", "old-mig")

	n := makeV7xNode("node-0", "my-pool", "projects/1/zones/us/instanceGroupManagers/new-mig", "ROOT-ABC")
	q := newFakeQueue()

	h.Create(ctxBG(), event.TypedCreateEvent[*corev1.Node]{Object: n}, q)

	// Should push a MIG-change update.
	updates := c.DrainUpdates("root-abc")
	if len(updates) != 1 {
		t.Fatalf("pending updates = %d, want 1", len(updates))
	}
	if updates[0].MIG != "projects/1/zones/us/instanceGroupManagers/new-mig" {
		t.Errorf("update MIG = %q", updates[0].MIG)
	}

	// Should send event on ptUpdateCh.
	select {
	case evt := <-ptCh:
		if evt.Object.Name != "root-abc" {
			t.Errorf("ptUpdate event name = %q, want root-abc", evt.Object.Name)
		}
	default:
		t.Error("no event sent on ptUpdateCh")
	}
}

// TestNodeHandler_Decoupled_NoUpdateWhenMIGSame verifies that when the
// MIG hasn't changed, no update is pushed.
func TestNodeHandler_Decoupled_NoUpdateWhenMIGSame(t *testing.T) {
	c := topocache.New()
	ptCh := make(chan event.TypedGenericEvent[*partitionv1beta1.PartitionTree], 10)
	h := nodeEventHandler(c, true, ptCh)

	sameMIG := "projects/1/zones/us/instanceGroupManagers/mig"
	c.SetPartitionCR("root-abc", "root-abc", sameMIG)

	n := makeV7xNode("node-0", "my-pool", sameMIG, "ROOT-ABC")
	q := newFakeQueue()

	h.Create(ctxBG(), event.TypedCreateEvent[*corev1.Node]{Object: n}, q)

	// No update should be pushed.
	updates := c.DrainUpdates("root-abc")
	if len(updates) != 0 {
		t.Errorf("unexpected updates: %+v", updates)
	}
	// No event on channel.
	select {
	case evt := <-ptCh:
		t.Errorf("unexpected ptUpdate event: %+v", evt)
	default:
	}
}

// TestNodeHandler_Decoupled_DeleteDrainPushesUpdate verifies that when
// the last node of a pool is deleted (emptied=true), a drain update
// is pushed and an event sent.
func TestNodeHandler_Decoupled_DeleteDrainPushesUpdate(t *testing.T) {
	c := topocache.New()
	ptCh := make(chan event.TypedGenericEvent[*partitionv1beta1.PartitionTree], 10)
	h := nodeEventHandler(c, true, ptCh)

	// First add a node so there's something to delete.
	n := makeV7xNode("node-0", "my-pool", "projects/1/zones/us/instanceGroupManagers/mig", "ROOT-ABC")
	q := newFakeQueue()
	h.Create(ctxBG(), event.TypedCreateEvent[*corev1.Node]{Object: n}, q)
	// Drain the channel from create.
	select {
	case <-ptCh:
	default:
	}
	// Drain queue from create.
	q.items = nil

	// Set pool→partition mapping (normally set by create handler).
	c.SetPoolRootPartitionID("my-pool", "root-abc")

	// Delete the node — should empty the pool.
	h.Delete(ctxBG(), event.TypedDeleteEvent[*corev1.Node]{Object: n}, q)

	// Should push a drain update.
	updates := c.DrainUpdates("root-abc")
	drainFound := false
	for _, u := range updates {
		if u.Drained {
			drainFound = true
		}
	}
	if !drainFound {
		t.Errorf("no drain update found, updates = %+v", updates)
	}

	// Should send event on ptUpdateCh.
	select {
	case evt := <-ptCh:
		if evt.Object.Name != "root-abc" {
			t.Errorf("ptUpdate event name = %q, want root-abc", evt.Object.Name)
		}
	default:
		t.Error("no event sent on ptUpdateCh after drain")
	}

	// Should enqueue partition ID.
	if len(q.items) != 1 || q.items[0].Name != "root-abc" {
		t.Errorf("enqueued items = %+v, want [root-abc]", q.items)
	}
}

// TestNodeHandler_Decoupled_DeleteNotEmptied verifies that when a node
// is deleted but the pool still has other nodes (emptied=false), no
// drain update is pushed.
func TestNodeHandler_Decoupled_DeleteNotEmptied(t *testing.T) {
	c := topocache.New()
	ptCh := make(chan event.TypedGenericEvent[*partitionv1beta1.PartitionTree], 10)
	h := nodeEventHandler(c, true, ptCh)

	// Add two nodes.
	n0 := makeV7xNode("node-0", "my-pool", "projects/1/zones/us/instanceGroupManagers/mig", "ROOT-ABC")
	n1 := makeV7xNode("node-1", "my-pool", "projects/1/zones/us/instanceGroupManagers/mig", "ROOT-ABC")
	q := newFakeQueue()
	h.Create(ctxBG(), event.TypedCreateEvent[*corev1.Node]{Object: n0}, q)
	h.Create(ctxBG(), event.TypedCreateEvent[*corev1.Node]{Object: n1}, q)
	c.SetPoolRootPartitionID("my-pool", "root-abc")

	// Drain everything from setup.
	for len(ptCh) > 0 {
		<-ptCh
	}
	c.DrainUpdates("root-abc")
	q.items = nil

	// Delete one node — pool still has node-1.
	h.Delete(ctxBG(), event.TypedDeleteEvent[*corev1.Node]{Object: n0}, q)

	// Should NOT push a drain update.
	updates := c.DrainUpdates("root-abc")
	for _, u := range updates {
		if u.Drained {
			t.Errorf("unexpected drain update when pool not empty: %+v", u)
		}
	}
}

// TestNodeHandler_Legacy_EnqueuesPoolName verifies that in non-decoupled
// mode, the handler still enqueues the pool name (unchanged behavior).
func TestNodeHandler_Legacy_EnqueuesPoolName(t *testing.T) {
	c := topocache.New()
	h := nodeEventHandler(c, false, nil)

	n := makeV7xNode("node-0", "my-pool", "projects/1/zones/us/instanceGroupManagers/mig", "ROOT-ABC")
	q := newFakeQueue()

	h.Create(ctxBG(), event.TypedCreateEvent[*corev1.Node]{Object: n}, q)

	if len(q.items) != 1 {
		t.Fatalf("enqueued %d items, want 1", len(q.items))
	}
	if q.items[0].Name != "my-pool" {
		t.Errorf("enqueued name = %q, want my-pool", q.items[0].Name)
	}
}

// ---------------------------------------------------------------------------
// sendPTUpdate tests
// ---------------------------------------------------------------------------

// TestSendPTUpdate_NilChannel verifies no panic on nil channel.
func TestSendPTUpdate_NilChannel(t *testing.T) {
	// Should not panic.
	sendPTUpdate(nil, "cr-name")
}

// TestSendPTUpdate_Sends verifies an event is sent on a buffered channel.
func TestSendPTUpdate_Sends(t *testing.T) {
	ch := make(chan event.TypedGenericEvent[*partitionv1beta1.PartitionTree], 1)
	sendPTUpdate(ch, "cr-name")

	select {
	case evt := <-ch:
		if evt.Object.Name != "cr-name" {
			t.Errorf("event name = %q, want cr-name", evt.Object.Name)
		}
	default:
		t.Error("no event sent")
	}
}

// TestSendPTUpdate_FullChannelDrops verifies the event is silently
// dropped when the channel buffer is full.
func TestSendPTUpdate_FullChannelDrops(t *testing.T) {
	ch := make(chan event.TypedGenericEvent[*partitionv1beta1.PartitionTree], 1)
	// Fill the buffer.
	sendPTUpdate(ch, "first")
	// This should be silently dropped.
	sendPTUpdate(ch, "second")

	evt := <-ch
	if evt.Object.Name != "first" {
		t.Errorf("expected first event, got %q", evt.Object.Name)
	}
	// Channel should be empty now.
	select {
	case extra := <-ch:
		t.Errorf("unexpected extra event: %q", extra.Object.Name)
	default:
	}
}

// TestNodeHandler_UpdateFailsFilter_EvictsFromCache_Legacy verifies that
// when an existing cached node is updated such that it no longer matches
// the filter (e.g. topology mode label removed), it is evicted from the
// cache and its former pool is enqueued for reconciliation.
func TestNodeHandler_UpdateFailsFilter_EvictsFromCache_Legacy(t *testing.T) {
	c := topocache.New()
	h := nodeEventHandler(c, false, nil)

	n := makeV7xNode("node-0", "my-pool", "projects/1/zones/us/instanceGroupManagers/mig", "ROOT-ABC")
	q := newFakeQueue()

	h.Create(ctxBG(), event.TypedCreateEvent[*corev1.Node]{Object: n}, q)
	if len(q.items) != 1 || q.items[0].Name != "my-pool" {
		t.Fatalf("enqueued items on create = %+v, want [my-pool]", q.items)
	}
	if _, ok := c.Get("node-0"); !ok {
		t.Fatalf("node-0 should be cached after create")
	}
	q.items = nil

	// Update node to remove the topology mode label (causing parseNode to fail)
	nUpdated := n.DeepCopy()
	delete(nUpdated.Labels, LabelTopologyMode)

	h.Update(ctxBG(), event.TypedUpdateEvent[*corev1.Node]{ObjectOld: n, ObjectNew: nUpdated}, q)

	// Node should be deleted from cache
	if _, ok := c.Get("node-0"); ok {
		t.Errorf("node-0 should be evicted from cache after dropping filter")
	}
	if _, ok := c.NodePool("my-pool"); ok {
		t.Errorf("my-pool should be deleted from cache when last node is evicted")
	}
	// Pool should be enqueued for reconciliation
	if len(q.items) != 1 || q.items[0].Name != "my-pool" {
		t.Errorf("enqueued items on filter drop = %+v, want [my-pool]", q.items)
	}
}

// TestNodeHandler_UpdateFailsFilter_EvictsFromCache_Decoupled verifies that
// in decoupled mode, dropping filter on the last node of a pool pushes a
// drain update and enqueues the root partition ID.
func TestNodeHandler_UpdateFailsFilter_EvictsFromCache_Decoupled(t *testing.T) {
	c := topocache.New()
	ptCh := make(chan event.TypedGenericEvent[*partitionv1beta1.PartitionTree], 10)
	h := nodeEventHandler(c, true, ptCh)

	n := makeV7xNode("node-0", "my-pool", "projects/1/zones/us/instanceGroupManagers/mig", "ROOT-ABC")
	q := newFakeQueue()

	h.Create(ctxBG(), event.TypedCreateEvent[*corev1.Node]{Object: n}, q)
	// Drain setup
	for len(ptCh) > 0 {
		<-ptCh
	}
	c.DrainUpdates("root-abc")
	q.items = nil

	// Update node to remove required MIG annotation (causing parseNode to fail)
	nUpdated := n.DeepCopy()
	delete(nUpdated.Annotations, AnnotationCreatedByMIG)

	h.Update(ctxBG(), event.TypedUpdateEvent[*corev1.Node]{ObjectOld: n, ObjectNew: nUpdated}, q)

	// Node should be evicted from cache
	if _, ok := c.Get("node-0"); ok {
		t.Errorf("node-0 should be evicted from cache after dropping filter")
	}

	// Drain update should be pushed
	updates := c.DrainUpdates("root-abc")
	drainFound := false
	for _, u := range updates {
		if u.Drained {
			drainFound = true
		}
	}
	if !drainFound {
		t.Errorf("expected drain update after dropping filter on only node, got %+v", updates)
	}

	// Partition ID should be enqueued
	if len(q.items) != 1 || q.items[0].Name != "root-abc" {
		t.Errorf("enqueued items = %+v, want [root-abc]", q.items)
	}
}

// TestNodeHandler_UpdateFailsFilter_NodeNotCached_Noop verifies that
// updating an un-cached node that fails parseNode is a no-op.
func TestNodeHandler_UpdateFailsFilter_NodeNotCached_Noop(t *testing.T) {
	c := topocache.New()
	h := nodeEventHandler(c, false, nil)

	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "uncached-node",
		},
	}
	q := newFakeQueue()

	h.Update(ctxBG(), event.TypedUpdateEvent[*corev1.Node]{ObjectOld: n, ObjectNew: n}, q)

	if len(q.items) != 0 {
		t.Errorf("expected no enqueued items for uncached node, got %+v", q.items)
	}
}

// TestNodeHandler_Legacy_NodePoolMigrationEnqueuesBothPools verifies that
// when a node's nodepool label changes from pool-A to pool-B in legacy mode,
// both pool-A and pool-B are enqueued so that pool-A's PartitionTree CR can
// be cleaned up (deleted) when it becomes empty.
func TestNodeHandler_Legacy_NodePoolMigrationEnqueuesBothPools(t *testing.T) {
	c := topocache.New()
	h := nodeEventHandler(c, false, nil)

	nOld := makeV7xNode("node-0", "pool-A", "projects/1/zones/us/instanceGroupManagers/mig", "ROOT-ABC")
	q := newFakeQueue()

	h.Create(ctxBG(), event.TypedCreateEvent[*corev1.Node]{Object: nOld}, q)
	if len(q.items) != 1 || q.items[0].Name != "pool-A" {
		t.Fatalf("enqueued items on create = %+v, want [pool-A]", q.items)
	}
	q.items = nil

	// Update node to point to pool-B
	nNew := makeV7xNode("node-0", "pool-B", "projects/1/zones/us/instanceGroupManagers/mig", "ROOT-ABC")
	h.Update(ctxBG(), event.TypedUpdateEvent[*corev1.Node]{ObjectOld: nOld, ObjectNew: nNew}, q)

	// Both pool-A and pool-B should be enqueued
	if len(q.items) != 2 {
		t.Fatalf("enqueued items on pool migration = %+v, want [pool-A pool-B]", q.items)
	}
	if q.items[0].Name != "pool-A" || q.items[1].Name != "pool-B" {
		t.Errorf("enqueued items = %+v, want [pool-A pool-B]", q.items)
	}

	// Cache verification: pool-A should be deleted from cache, pool-B present
	if _, ok := c.NodePool("pool-A"); ok {
		t.Errorf("pool-A should be removed from cache after its only node moved")
	}
	if np, ok := c.NodePool("pool-B"); !ok || !np.Nodes.Has("node-0") {
		t.Errorf("pool-B should contain node-0, got %+v, ok=%v", np, ok)
	}
}

// TestNodeHandler_Decoupled_NodePoolMigrationDrain verifies that when a
// node migrates across pools in decoupled mode and drains the old pool,
// a drain update is pushed for the old pool's partition and both partitions
// are enqueued.
func TestNodeHandler_Decoupled_NodePoolMigrationDrain(t *testing.T) {
	c := topocache.New()
	ptCh := make(chan event.TypedGenericEvent[*partitionv1beta1.PartitionTree], 10)
	h := nodeEventHandler(c, true, ptCh)

	nOld := makeV7xNode("node-0", "pool-A", "projects/1/zones/us/instanceGroupManagers/mig", "ROOT-A")
	q := newFakeQueue()

	h.Create(ctxBG(), event.TypedCreateEvent[*corev1.Node]{Object: nOld}, q)
	for len(ptCh) > 0 {
		<-ptCh
	}
	c.DrainUpdates("root-a")
	q.items = nil

	// Move node-0 to pool-B with root partition ROOT-B
	nNew := makeV7xNode("node-0", "pool-B", "projects/1/zones/us/instanceGroupManagers/mig", "ROOT-B")
	h.Update(ctxBG(), event.TypedUpdateEvent[*corev1.Node]{ObjectOld: nOld, ObjectNew: nNew}, q)

	// Old partition root-a should receive a drain update
	updates := c.DrainUpdates("root-a")
	drainFound := false
	for _, u := range updates {
		if u.Drained {
			drainFound = true
		}
	}
	if !drainFound {
		t.Errorf("expected drain update for root-a, got %+v", updates)
	}

	// Queue should have root-a and root-b
	if len(q.items) != 2 || q.items[0].Name != "root-a" || q.items[1].Name != "root-b" {
		t.Errorf("enqueued items = %+v, want [root-a root-b]", q.items)
	}
}

// TestNodeHandler_Legacy_NodePoolMigrationPartial_NoDrain verifies that
// when a node migrates from pool-A to pool-B but pool-A still contains other
// nodes, both pools are enqueued and pool-A remains in the cache without being
// deleted.
func TestNodeHandler_Legacy_NodePoolMigrationPartial_NoDrain(t *testing.T) {
	c := topocache.New()
	h := nodeEventHandler(c, false, nil)

	n0 := makeV7xNode("node-0", "pool-A", "projects/1/zones/us/instanceGroupManagers/mig", "ROOT-ABC")
	n1 := makeV7xNode("node-1", "pool-A", "projects/1/zones/us/instanceGroupManagers/mig", "ROOT-ABC")
	q := newFakeQueue()

	h.Create(ctxBG(), event.TypedCreateEvent[*corev1.Node]{Object: n0}, q)
	h.Create(ctxBG(), event.TypedCreateEvent[*corev1.Node]{Object: n1}, q)
	q.items = nil

	// Move only node-0 to pool-B
	n0New := makeV7xNode("node-0", "pool-B", "projects/1/zones/us/instanceGroupManagers/mig", "ROOT-ABC")
	h.Update(ctxBG(), event.TypedUpdateEvent[*corev1.Node]{ObjectOld: n0, ObjectNew: n0New}, q)

	// Both pool-A and pool-B should be enqueued
	if len(q.items) != 2 {
		t.Fatalf("enqueued items on pool migration = %+v, want [pool-A pool-B]", q.items)
	}
	if q.items[0].Name != "pool-A" || q.items[1].Name != "pool-B" {
		t.Errorf("enqueued items = %+v, want [pool-A pool-B]", q.items)
	}

	// Cache verification: pool-A must still exist and contain node-1
	npA, okA := c.NodePool("pool-A")
	if !okA || !npA.Nodes.Has("node-1") || npA.Nodes.Has("node-0") {
		t.Errorf("pool-A should contain only node-1, got %+v, ok=%v", npA, okA)
	}
	npB, okB := c.NodePool("pool-B")
	if !okB || !npB.Nodes.Has("node-0") {
		t.Errorf("pool-B should contain node-0, got %+v, ok=%v", npB, okB)
	}
}

// TestNodeHandler_Decoupled_NodePoolMigrationPartial_NoDrain verifies that
// when a node migrates across pools in decoupled mode but the old pool is
// not emptied, both root partitions are enqueued, no drain update is pushed
// for the old partition, and the old pool->rootPartition mapping remains intact.
func TestNodeHandler_Decoupled_NodePoolMigrationPartial_NoDrain(t *testing.T) {
	c := topocache.New()
	ptCh := make(chan event.TypedGenericEvent[*partitionv1beta1.PartitionTree], 10)
	h := nodeEventHandler(c, true, ptCh)

	n0 := makeV7xNode("node-0", "pool-A", "projects/1/zones/us/instanceGroupManagers/mig", "ROOT-A")
	n1 := makeV7xNode("node-1", "pool-A", "projects/1/zones/us/instanceGroupManagers/mig", "ROOT-A")
	q := newFakeQueue()

	h.Create(ctxBG(), event.TypedCreateEvent[*corev1.Node]{Object: n0}, q)
	h.Create(ctxBG(), event.TypedCreateEvent[*corev1.Node]{Object: n1}, q)
	for len(ptCh) > 0 {
		<-ptCh
	}
	c.DrainUpdates("root-a")
	q.items = nil

	// Move node-0 to pool-B with root partition ROOT-B (pool-A still has node-1)
	n0New := makeV7xNode("node-0", "pool-B", "projects/1/zones/us/instanceGroupManagers/mig", "ROOT-B")
	h.Update(ctxBG(), event.TypedUpdateEvent[*corev1.Node]{ObjectOld: n0, ObjectNew: n0New}, q)

	// Old partition root-a should NOT receive a drain update
	updates := c.DrainUpdates("root-a")
	for _, u := range updates {
		if u.Drained {
			t.Errorf("unexpected drain update for root-a when pool-A still has active nodes, got %+v", u)
		}
	}

	// Mapping for pool-A should still be preserved
	if partID, ok := c.PoolRootPartitionID("pool-A"); !ok || partID != "root-a" {
		t.Errorf("pool-A mapping should be preserved, got (%q, %v)", partID, ok)
	}

	// Queue should have both root-a and root-b
	if len(q.items) != 2 || q.items[0].Name != "root-a" || q.items[1].Name != "root-b" {
		t.Errorf("enqueued items = %+v, want [root-a root-b]", q.items)
	}
}
