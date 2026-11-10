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
	"context"
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientgocache "k8s.io/client-go/tools/cache"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/event"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
	"github.com/ai-on-gke/partition-tree/internal/cache"
	"github.com/ai-on-gke/partition-tree/internal/gce"
	"github.com/ai-on-gke/partition-tree/internal/metrics"
	"github.com/ai-on-gke/partition-tree/internal/tree"
)

func buildFatNode(accelType string) *corev1.Node {
	labels := map[string]string{
		LabelTopologyMode:                          LabelTopologyModeProvisionOnly,
		LabelNodePool:                              "pool-a",
		LabelTPUAccelerator:                        accelType,
		LabelTopologyBlock:                         "block-1",
		"kubernetes.io/hostname":                   "gke-tpu-node-1",
		"kubernetes.io/arch":                       "amd64",
		"kubernetes.io/os":                         "linux",
		"beta.kubernetes.io/arch":                  "amd64",
		"beta.kubernetes.io/os":                    "linux",
		"beta.kubernetes.io/instance-type":         "tpu7x-standard-4t",
		"node.kubernetes.io/instance-type":         "tpu7x-standard-4t",
		"topology.kubernetes.io/region":            "us-central1",
		"topology.kubernetes.io/zone":              "us-central1-a",
		"failure-domain.beta.kubernetes.io/region": "us-central1",
		"failure-domain.beta.kubernetes.io/zone":   "us-central1-a",
		"cloud.google.com/gke-boot-disk":           "pd-balanced",
		"cloud.google.com/gke-container-runtime":   "containerd",
		"cloud.google.com/gke-cpu-scaling-level":   "4",
		"cloud.google.com/gke-logging-variant":     "DEFAULT",
		"cloud.google.com/gke-max-pods-per-node":   "110",
		"cloud.google.com/gke-netd-ready":          "true",
		"cloud.google.com/gke-os-distribution":     "cos",
		"cloud.google.com/gke-provisioning":        "standard",
		"cloud.google.com/gke-stack-type":          "IPV4",
		"cloud.google.com/machine-family":          "tpu7x",
		"cloud.google.com/private-node":            "false",
		"iam.gke.io/gke-metadata-server-enabled":   "true",
		"topology.gke.io/zone":                     "us-central1-a",
		"cloud.google.com/gke-tpu-ChipsPerHost":    "4",
		"cloud.google.com/gke-tpu-topology":        "4x4x4",
		"cloud.google.com/reservation-name":        "example-reservation",
		"cloud.google.com/reservation-project":     "example-project",
		"cloud.google.com/reservation-type":        "specific",
		"cloud.google.com/gce-topology-compact":    "compact-placement-policy-01",
		"cloud.google.com/gce-topology-subblock":   "subblock-001",
		"cloud.google.com/gce-topology-host":       "host-0001",
		"node.gke.io/maintenance-restriction":      "restricted",
		"gpu.nvidia.com/Present":                   "false",
		"networking.gke.io/default-interface":      "eth0",
	}
	if accelType == "tpu-v6e-slice" {
		labels["cloud.google.com/gke-tpu-slice-16x16-id"] = "slice-root-1"
		labels["cloud.google.com/gke-tpu-partition-4x4-id"] = "part-4x4-1"
	} else {
		labels["cloud.google.com/gke-tpu-partition-4x4x4-id"] = "part-4x4x4-1"
		labels["cloud.google.com/gke-tpu-partition-2x4x4-id"] = "part-2x4x4-1"
		labels["cloud.google.com/gke-tpu-partition-2x2x4-id"] = "part-2x2x4-1"
		labels["cloud.google.com/gke-tpu-partition-2x2x2-id"] = "part-2x2x2-1"
		labels["cloud.google.com/gke-tpu-partition-2x2x1-id"] = "part-2x2x1-1"
	}

	now := metav1.NewTime(time.Now())
	images := make([]corev1.ContainerImage, 25)
	for i := range images {
		images[i] = corev1.ContainerImage{
			Names: []string{
				fmt.Sprintf("us-central1-docker.pkg.dev/gke-release/images/system-daemon-%02d@sha256:%s", i, strings.Repeat("a1b2c3d4", 8)),
				fmt.Sprintf("us-central1-docker.pkg.dev/gke-release/images/system-daemon-%02d:v1.32.2-gke.1100", i),
			},
			SizeBytes: int64(35000000 + i*1000000),
		}
	}

	conditionTypes := []corev1.NodeConditionType{
		corev1.NodeMemoryPressure,
		corev1.NodeDiskPressure,
		corev1.NodePIDPressure,
		corev1.NodeNetworkUnavailable,
		"FrequentKubeletRestart",
		"FrequentDockerRestart",
		"FrequentContainerdRestart",
		"FrequentUnregisterNetDevice",
		"KernelDeadlock",
		"ReadonlyFilesystem",
		"CorruptDockerOverlay2",
		corev1.NodeReady,
	}
	conditions := make([]corev1.NodeCondition, len(conditionTypes))
	for i, ct := range conditionTypes {
		status := corev1.ConditionFalse
		if ct == corev1.NodeReady {
			status = corev1.ConditionTrue
		}
		conditions[i] = corev1.NodeCondition{
			Type:               ct,
			Status:             status,
			LastHeartbeatTime:  now,
			LastTransitionTime: now,
			Reason:             fmt.Sprintf("NodeProblemDetectorCheck%02d", i),
			Message:            strings.Repeat(fmt.Sprintf("node-problem-detector health check message for %s ", ct), 3),
		}
	}

	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "gke-tpu-node-1",
			UID:             "uid-1234",
			ResourceVersion: "98765",
			Labels:          labels,
			Annotations: map[string]string{
				AnnotationCreatedByMIG:                 "https://www.googleapis.com/compute/v1/projects/p/zones/z/instanceGroupManagers/mig-1",
				"node.gke.io/last-applied-node-labels": strings.Repeat(`{"cloud.google.com/gke-tpu-partition-4x4x4-id":"part-4x4x4-1"},`, 50),
				"csi.volume.kubernetes.io/nodeid":      "{\"pd.csi.storage.gke.io\":\"projects/p/zones/z/instances/gke-tpu-node-1\"}",
				"Alpha.kubernetes.io/provided-node-ip": "10.0.0.1",
				"networking.gke.io/last-applied-cidrs": "10.244.0.0/24,10.244.1.0/24",
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "Node",
					Name:       "parent-owner",
					UID:        "owner-uid-1",
				},
			},
			Finalizers: []string{"cloud.google.com/node-finalizer"},
			ManagedFields: []metav1.ManagedFieldsEntry{
				{
					Manager:    "kubelet",
					Operation:  metav1.ManagedFieldsOperationUpdate,
					APIVersion: "v1",
					Time:       &now,
					FieldsType: "FieldsV1",
					FieldsV1:   &metav1.FieldsV1{Raw: []byte(strings.Repeat(`{"f:status":{"f:conditions":{},"f:images":{}}},`, 35))},
				},
				{
					Manager:    "node-problem-detector",
					Operation:  metav1.ManagedFieldsOperationUpdate,
					APIVersion: "v1",
					Time:       &now,
					FieldsType: "FieldsV1",
					FieldsV1:   &metav1.FieldsV1{Raw: []byte(strings.Repeat(`{"f:status":{"f:conditions":{"k:{\"type\":\"KernelDeadlock\"}":{}}}},`, 20))},
				},
				{
					Manager:    "kube-controller-manager",
					Operation:  metav1.ManagedFieldsOperationUpdate,
					APIVersion: "v1",
					Time:       &now,
					FieldsType: "FieldsV1",
					FieldsV1:   &metav1.FieldsV1{Raw: []byte(strings.Repeat(`{"f:metadata":{"f:annotations":{},"f:labels":{}}},`, 20))},
				},
			},
		},
		Spec: corev1.NodeSpec{
			PodCIDR:       "10.244.0.0/24",
			PodCIDRs:      []string{"10.244.0.0/24"},
			ProviderID:    "gce://project/us-central1-a/gke-tpu-node-1",
			Unschedulable: true,
			Taints: append(make([]corev1.Taint, 0, 8), corev1.Taint{
				Key:    "google.com/tpu",
				Value:  "present",
				Effect: corev1.TaintEffectNoSchedule,
			}),
		},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceCPU:              resource.MustParse("180"),
				corev1.ResourceMemory:           resource.MustParse("1400Gi"),
				corev1.ResourceEphemeralStorage: resource.MustParse("200Gi"),
				corev1.ResourcePods:             resource.MustParse("110"),
			},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:              resource.MustParse("178"),
				corev1.ResourceMemory:           resource.MustParse("1350Gi"),
				corev1.ResourceEphemeralStorage: resource.MustParse("180Gi"),
				corev1.ResourcePods:             resource.MustParse("110"),
			},
			Conditions: conditions,
			Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeInternalIP, Address: "10.0.0.1"},
				{Type: corev1.NodeExternalIP, Address: "203.0.113.1"},
				{Type: corev1.NodeHostName, Address: "gke-tpu-node-1"},
			},
			NodeInfo: corev1.NodeSystemInfo{
				MachineID:               "7890abcdef1234567890abcdef123456",
				SystemUUID:              "12345678-90ab-cdef-1234-567890abcdef",
				BootID:                  "abcdef12-3456-7890-abcd-ef1234567890",
				KernelVersion:           "6.6.30+",
				OSImage:                 "Container-Optimized OS from Google",
				ContainerRuntimeVersion: "containerd://1.7.15",
				KubeletVersion:          "v1.32.2-gke.1100",
				Architecture:            "amd64",
				OperatingSystem:         "linux",
			},
			Images: images,
		},
	}
}

func TestStripNodeForCache_PreservesParseNodeAndRootPartitionID(t *testing.T) {
	for _, accel := range []string{"tpu7x", "tpu-v6e-slice"} {
		t.Run(accel, func(t *testing.T) {
			orig := buildFatNode(accel)
			wantEntry, wantOK := parseNode(orig)
			if !wantOK {
				t.Fatalf("parseNode(orig) = false, want true")
			}
			wantRootID, err := rootPartitionIDForNode(accel, orig.Labels)
			if err != nil || wantRootID == "" {
				t.Fatalf("rootPartitionIDForNode(orig) = (%q, %v), want non-empty", wantRootID, err)
			}

			gotObj, err := StripNodeForCache(orig.DeepCopy())
			if err != nil {
				t.Fatalf("StripNodeForCache returned unexpected error: %v", err)
			}
			stripped, ok := gotObj.(*corev1.Node)
			if !ok {
				t.Fatalf("expected *corev1.Node, got %T", gotObj)
			}

			gotEntry, gotOK := parseNode(stripped)
			if gotOK != wantOK {
				t.Fatalf("parseNode(stripped) ok = %v, want %v", gotOK, wantOK)
			}
			slices.Sort(wantEntry.PartitionIDs)
			slices.Sort(gotEntry.PartitionIDs)
			if !reflect.DeepEqual(gotEntry, wantEntry) {
				t.Errorf("parseNode(stripped) = %+v, want %+v", gotEntry, wantEntry)
			}
			if gotRootID, err := rootPartitionIDForNode(accel, stripped.Labels); err != nil || gotRootID != wantRootID {
				t.Errorf("rootPartitionIDForNode(stripped) = (%q, %v), want (%q, nil)", gotRootID, err, wantRootID)
			}
			if !nodeMatchesFilter(stripped) {
				t.Errorf("nodeMatchesFilter(stripped) = false, want true")
			}

			// Verify unused fields are stripped.
			assertStrippedNodeFields(t, stripped)
		})
	}
}

func assertStrippedNodeFields(t *testing.T, stripped *corev1.Node) {
	t.Helper()
	if stripped.ManagedFields != nil || stripped.OwnerReferences != nil || stripped.Finalizers != nil {
		t.Errorf("ObjectMeta retained unused slices: ManagedFields=%v, OwnerReferences=%v, Finalizers=%v",
			stripped.ManagedFields, stripped.OwnerReferences, stripped.Finalizers)
	}
	if _, ok := stripped.Labels["kubernetes.io/hostname"]; ok {
		t.Errorf("Labels retained non-TPU label kubernetes.io/hostname: %v", stripped.Labels)
	}
	if len(stripped.Annotations) != 1 || stripped.Annotations[AnnotationCreatedByMIG] == "" {
		t.Errorf("Annotations = %v, want only %q", stripped.Annotations, AnnotationCreatedByMIG)
	}
	if stripped.Spec.ProviderID != "" || stripped.Spec.PodCIDR != "" || stripped.Spec.PodCIDRs != nil || stripped.Spec.Unschedulable {
		t.Errorf("Spec retained unused fields: %+v", stripped.Spec)
	}
	if cap(stripped.Spec.Taints) != len(stripped.Spec.Taints) {
		t.Errorf("cap(Spec.Taints) = %d, want clipped to len %d", cap(stripped.Spec.Taints), len(stripped.Spec.Taints))
	}
	if stripped.Status.Images != nil || stripped.Status.Addresses != nil || stripped.Status.Capacity != nil || stripped.Status.Allocatable != nil {
		t.Errorf("Status retained unused fields: %+v", stripped.Status)
	}
	if len(stripped.Status.Conditions) != 1 || cap(stripped.Status.Conditions) != 1 {
		t.Fatalf("len/cap(Status.Conditions) = %d/%d, want 1/1", len(stripped.Status.Conditions), cap(stripped.Status.Conditions))
	}
	cond := stripped.Status.Conditions[0]
	if cond.Type != corev1.NodeReady || cond.Status != corev1.ConditionTrue {
		t.Errorf("Status.Conditions[0] = %+v, want NodeReady=True", cond)
	}
	if cond.Message != "" || cond.Reason != "" {
		t.Errorf("Status.Conditions[0] retained Message/Reason: %+v", cond)
	}
}

// withTPUResources sets google.com/tpu capacity and allocatable on n, as the
// TPU device plugin does once it has registered. buildFatNode leaves them
// out so the flag-off fixtures stay as they were.
func withTPUResources(n *corev1.Node, allocatable string) *corev1.Node {
	n.Status.Capacity[ResourceTPU] = resource.MustParse("4")
	n.Status.Allocatable[ResourceTPU] = resource.MustParse(allocatable)
	return n
}

// TestStripNodeForCache_DropsTPUAllocatable pins the flag-off transform:
// even when the Node reports google.com/tpu, StripNodeForCache keeps no
// allocatable, so parseNode reads TPUAllocatable=0 and the predicate's
// allocatable diff never fires.
func TestStripNodeForCache_DropsTPUAllocatable(t *testing.T) {
	gotObj, err := StripNodeForCache(withTPUResources(buildFatNode("tpu7x"), "4"))
	if err != nil {
		t.Fatalf("StripNodeForCache returned unexpected error: %v", err)
	}
	stripped := gotObj.(*corev1.Node)
	if stripped.Status.Allocatable != nil || stripped.Status.Capacity != nil {
		t.Errorf("Status retained Allocatable=%v Capacity=%v, want both nil", stripped.Status.Allocatable, stripped.Status.Capacity)
	}
	entry, ok := parseNode(stripped)
	if !ok {
		t.Fatalf("parseNode(stripped) = false, want true")
	}
	if entry.TPUAllocatable != 0 {
		t.Errorf("parseNode(stripped).TPUAllocatable = %d, want 0", entry.TPUAllocatable)
	}
}

func TestStripNodeForCacheKeepTPUAllocatable_KeepsOnlyTPU(t *testing.T) {
	for _, accel := range []string{"tpu7x", "tpu-v6e-slice"} {
		t.Run(accel, func(t *testing.T) {
			orig := withTPUResources(buildFatNode(accel), "4")
			wantEntry, wantOK := parseNode(orig)
			if !wantOK || wantEntry.TPUAllocatable != 4 {
				t.Fatalf("parseNode(orig) = (%+v, %v), want TPUAllocatable=4, true", wantEntry, wantOK)
			}

			in := orig.DeepCopy()
			decodedAlloc := in.Status.Allocatable
			gotObj, err := StripNodeForCacheKeepTPUAllocatable(in)
			if err != nil {
				t.Fatalf("StripNodeForCacheKeepTPUAllocatable returned unexpected error: %v", err)
			}
			stripped, ok := gotObj.(*corev1.Node)
			if !ok {
				t.Fatalf("expected *corev1.Node, got %T", gotObj)
			}

			gotTPU := stripped.Status.Allocatable[ResourceTPU]
			if len(stripped.Status.Allocatable) != 1 || gotTPU.Cmp(resource.MustParse("4")) != 0 {
				t.Errorf("Status.Allocatable = %v, want only google.com/tpu: 4", stripped.Status.Allocatable)
			}
			// The kept entry must live in a fresh map, not the decoded one.
			decodedAlloc[ResourceTPU] = resource.MustParse("1")
			if q := stripped.Status.Allocatable[ResourceTPU]; q.Value() != 4 {
				t.Errorf("Status.Allocatable aliases the decoded map: got %s after mutating it", q.String())
			}

			gotEntry, gotOK := parseNode(stripped)
			if !gotOK {
				t.Fatalf("parseNode(stripped) = false, want true")
			}
			slices.Sort(wantEntry.PartitionIDs)
			slices.Sort(gotEntry.PartitionIDs)
			if !reflect.DeepEqual(gotEntry, wantEntry) {
				t.Errorf("parseNode(stripped) = %+v, want %+v", gotEntry, wantEntry)
			}

			// Everything else is stripped exactly like StripNodeForCache.
			rest := stripped.DeepCopy()
			rest.Status.Allocatable = nil
			assertStrippedNodeFields(t, rest)
		})
	}
}

func TestStripNodeForCacheKeepTPUAllocatable_AbsentTPU(t *testing.T) {
	// buildFatNode carries cpu/memory/pods/ephemeral-storage but no
	// google.com/tpu (a node whose plugin has not registered yet).
	gotObj, err := StripNodeForCacheKeepTPUAllocatable(buildFatNode("tpu7x"))
	if err != nil {
		t.Fatalf("StripNodeForCacheKeepTPUAllocatable returned unexpected error: %v", err)
	}
	stripped := gotObj.(*corev1.Node)
	if stripped.Status.Allocatable != nil {
		t.Errorf("Status.Allocatable = %v, want nil when google.com/tpu is absent", stripped.Status.Allocatable)
	}
	assertStrippedNodeFields(t, stripped)
	if entry, _ := parseNode(stripped); entry.TPUAllocatable != 0 {
		t.Errorf("parseNode(stripped).TPUAllocatable = %d, want 0", entry.TPUAllocatable)
	}

	// A present 0 is kept: it differs from absent for the predicate.
	gotObj, err = StripNodeForCacheKeepTPUAllocatable(withTPUResources(buildFatNode("tpu7x"), "0"))
	if err != nil {
		t.Fatalf("StripNodeForCacheKeepTPUAllocatable returned unexpected error: %v", err)
	}
	if alloc := gotObj.(*corev1.Node).Status.Allocatable; len(alloc) != 1 {
		t.Errorf("Status.Allocatable = %v, want {google.com/tpu: 0}", alloc)
	}
}

func TestStripNodeForCacheKeepTPUAllocatable_IdempotentTombstoneAndNonNode(t *testing.T) {
	first, err := StripNodeForCacheKeepTPUAllocatable(withTPUResources(buildFatNode("tpu7x"), "4"))
	if err != nil {
		t.Fatalf("first StripNodeForCacheKeepTPUAllocatable error: %v", err)
	}
	snapshot := first.(*corev1.Node).DeepCopy()
	second, err := StripNodeForCacheKeepTPUAllocatable(first)
	if err != nil {
		t.Fatalf("second StripNodeForCacheKeepTPUAllocatable error: %v", err)
	}
	if !reflect.DeepEqual(second, snapshot) {
		t.Errorf("StripNodeForCacheKeepTPUAllocatable is not idempotent: got %+v, want %+v", second, snapshot)
	}

	tombstone := clientgocache.DeletedFinalStateUnknown{
		Key: "gke-tpu-node-1",
		Obj: withTPUResources(buildFatNode("tpu7x"), "4"),
	}
	got, err := StripNodeForCacheKeepTPUAllocatable(tombstone)
	if err != nil {
		t.Fatalf("StripNodeForCacheKeepTPUAllocatable(tombstone) returned error: %v", err)
	}
	gotTombstone, ok := got.(clientgocache.DeletedFinalStateUnknown)
	if !ok {
		t.Fatalf("expected DeletedFinalStateUnknown, got %T", got)
	}
	inner, ok := gotTombstone.Obj.(*corev1.Node)
	if !ok {
		t.Fatalf("expected inner *corev1.Node, got %T", gotTombstone.Obj)
	}
	if inner.ManagedFields != nil || inner.Status.Images != nil || len(inner.Status.Conditions) != 1 || len(inner.Status.Allocatable) != 1 {
		t.Errorf("inner Node in tombstone was not stripped to the TPU-allocatable shape: %+v", inner)
	}

	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "cfg"}}
	gotCM, err := StripNodeForCacheKeepTPUAllocatable(cm)
	if err != nil || gotCM != cm {
		t.Errorf("StripNodeForCacheKeepTPUAllocatable(ConfigMap) = %v, %v; want %v, nil", gotCM, err, cm)
	}
}

// TestManagerCacheOptions_NodeTransform verifies the keepTPUAllocatable
// argument selects the Node transform.
func TestManagerCacheOptions_NodeTransform(t *testing.T) {
	for _, keep := range []bool{false, true} {
		t.Run(fmt.Sprintf("keepTPUAllocatable=%v", keep), func(t *testing.T) {
			opts := ManagerCacheOptions("", "", keep)
			var transform clientgocache.TransformFunc
			for obj, bo := range opts.ByObject {
				if _, ok := obj.(*corev1.Node); ok {
					transform = bo.Transform
				}
			}
			if transform == nil {
				t.Fatalf("no Node transform in ManagerCacheOptions")
			}
			got, err := transform(withTPUResources(buildFatNode("tpu7x"), "4"))
			if err != nil {
				t.Fatalf("transform returned error: %v", err)
			}
			alloc := got.(*corev1.Node).Status.Allocatable
			if keep && len(alloc) != 1 {
				t.Errorf("keepTPUAllocatable=true: Status.Allocatable = %v, want only google.com/tpu", alloc)
			}
			if !keep && alloc != nil {
				t.Errorf("keepTPUAllocatable=false: Status.Allocatable = %v, want nil", alloc)
			}
		})
	}
}

func TestStripNodeForCache_Tombstone(t *testing.T) {
	n := buildFatNode("tpu7x")
	tombstone := clientgocache.DeletedFinalStateUnknown{
		Key: "gke-tpu-node-1",
		Obj: n,
	}
	got, err := StripNodeForCache(tombstone)
	if err != nil {
		t.Fatalf("StripNodeForCache(tombstone) returned error: %v", err)
	}
	gotTombstone, ok := got.(clientgocache.DeletedFinalStateUnknown)
	if !ok {
		t.Fatalf("expected DeletedFinalStateUnknown, got %T", got)
	}
	inner, ok := gotTombstone.Obj.(*corev1.Node)
	if !ok {
		t.Fatalf("expected inner *corev1.Node, got %T", gotTombstone.Obj)
	}
	if inner.ManagedFields != nil || inner.Status.Images != nil || len(inner.Status.Conditions) != 1 {
		t.Errorf("inner Node in tombstone was not stripped: %+v", inner)
	}
}

func TestStripNodeForCache_IdempotentAndNonNode(t *testing.T) {
	n := buildFatNode("tpu7x")
	first, err := StripNodeForCache(n)
	if err != nil {
		t.Fatalf("first StripNodeForCache error: %v", err)
	}
	snapshot := first.(*corev1.Node).DeepCopy()
	second, err := StripNodeForCache(first)
	if err != nil {
		t.Fatalf("second StripNodeForCache error: %v", err)
	}
	if !reflect.DeepEqual(second, snapshot) {
		t.Errorf("StripNodeForCache is not idempotent: got %+v, want %+v", second, snapshot)
	}

	// Non-Node objects pass through untouched.
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "cfg"}}
	gotCM, err := StripNodeForCache(cm)
	if err != nil || gotCM != cm {
		t.Errorf("StripNodeForCache(ConfigMap) = %v, %v; want %v, nil", gotCM, err, cm)
	}
}

// buildTPU7x31PartitionResponse constructs a realistic 31-partition 5-level
// TPU7x (4x4x4 -> 2x4x4 -> 2x2x4 -> 2x2x2 -> 2x2x1) GCE response for a MIG.
func buildTPU7x31PartitionResponse(poolIdx int) *gce.TopologyResponse {
	entries := make([]gce.TopologyEntry, 0, 31)
	rootID := fmt.Sprintf("p-%04d-4x4x4", poolIdx)
	entries = append(entries, gce.TopologyEntry{
		PartitionID: rootID,
		ParentID:    "",
		Topology:    "4x4x4",
		State:       "HEALTHY",
	})
	for i := range 2 {
		id244 := fmt.Sprintf("p-%04d-2x4x4-%d", poolIdx, i)
		entries = append(entries, gce.TopologyEntry{
			PartitionID: id244,
			ParentID:    rootID,
			Topology:    "2x4x4",
			State:       "HEALTHY",
		})
		for j := range 2 {
			id224 := fmt.Sprintf("p-%04d-2x2x4-%d-%d", poolIdx, i, j)
			entries = append(entries, gce.TopologyEntry{
				PartitionID: id224,
				ParentID:    id244,
				Topology:    "2x2x4",
				State:       "HEALTHY",
			})
			for k := range 2 {
				id222 := fmt.Sprintf("p-%04d-2x2x2-%d-%d-%d", poolIdx, i, j, k)
				entries = append(entries, gce.TopologyEntry{
					PartitionID: id222,
					ParentID:    id224,
					Topology:    "2x2x2",
					State:       "HEALTHY",
				})
				for l := range 2 {
					leafIdx := i*8 + j*4 + k*2 + l
					id221 := fmt.Sprintf("p-%04d-2x2x1-%02d", poolIdx, leafIdx)
					entries = append(entries, gce.TopologyEntry{
						PartitionID: id221,
						ParentID:    id222,
						Topology:    "2x2x1",
						State:       "HEALTHY",
					})
				}
			}
		}
	}
	return &gce.TopologyResponse{Entries: entries}
}

// TestStripNodeForCache_AndControllerChurnMemoryBound verifies memory guardrails
// across 3 churn waves (Ready -> NotReady -> Ready + NPD condition + taint churn)
// at both 8-pod TPU7x scale (1,152 MIGs / 18,432 Nodes) and 24-pod TPU7x scale
// (3,456 MIGs / 55,296 Nodes / 107,136 partitions):
//   - Phase 1: Per-node Node informer + topoCache ceiling (<= 4.00 KB/node),
//     which fails immediately if StripNodeForCache regresses.
//   - Phase 2: Combined controller live heap (Node informer + CR informer +
//     topoCache + pollStateStore + metric gauge/tombstone maps) under the
//     512 MiB container memory limit (GOGC=100 peak RSS < 512 MiB at 8 pods;
//     GOMEMLIMIT=400MiB live heap + runtime overhead < 512 MiB at 24 pods).
func TestStripNodeForCache_AndControllerChurnMemoryBound(t *testing.T) {
	for _, tc := range []struct {
		name    string
		numPods int
	}{
		{
			name:    "8_pods_1152_migs_18432_nodes",
			numPods: 8,
		},
		{
			name:    "24_pods_3456_migs_55296_nodes",
			numPods: 24,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runControllerChurnMemoryBound(t, tc.numPods, false)
		})
	}
}

// TestStripNodeForCacheKeepTPUAllocatable_AndControllerChurnMemoryBound runs
// the same churn and budgets with --enable-tpu-allocatable-readiness on:
// the Node informer uses StripNodeForCacheKeepTPUAllocatable and every Node
// reports google.com/tpu: 4 in wave 0, 0 in the NotReady wave, and still 0
// once Ready again in the final wave (a reboot before the device plugin
// re-registers). Phase 2 therefore measures every leaf at
// TPUsNotAllocatable, with its per-node message retained in the CR.
func TestStripNodeForCacheKeepTPUAllocatable_AndControllerChurnMemoryBound(t *testing.T) {
	for _, tc := range []struct {
		name    string
		numPods int
	}{
		{
			name:    "8_pods_1152_migs_18432_nodes",
			numPods: 8,
		},
		{
			name:    "24_pods_3456_migs_55296_nodes",
			numPods: 24,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runControllerChurnMemoryBound(t, tc.numPods, true)
		})
	}
}

func runControllerChurnMemoryBound(t *testing.T, numPods int, keepTPUAllocatable bool) {
	t.Helper()
	metrics.ClearGaugeSets()
	t.Cleanup(metrics.ClearGaugeSets)

	const (
		poolsPerPod  = 144
		nodesPerPool = 16
		churnWaves   = 3

		// Tight per-node budget for StripNodeForCache + topoCache: an unstripped
		// GKE TPU7x node from buildFatNode takes ~18-20 KB on heap, whereas a
		// stripped node + topoCache entry must stay below 4.0 KB/node.
		maxNodeStoreKBPerNode = 4.0

		// Container memory limit from config/manager/manager.yaml:88 (1Gi = 1024 MiB).
		containerMemoryLimitMiB = 1024.0
		gogc100HeapMultiplier   = 2.0
		runtimeOverheadMiB      = 30.0
	)

	numPools := numPods * poolsPerPod
	totalNodes := numPools * nodesPerPool

	ctx := context.Background()
	nodeStore := make(map[string]*corev1.Node, totalNodes)
	crStore := make(map[string]*partitionv1beta1.PartitionTree, numPools)
	topoCache := cache.New()
	pollStates := newPollStateStore()
	pred := matchingNodesPredicate()
	stripCR := ctrlcache.TransformStripManagedFields()
	reconciler := &PartitionTreeReconciler{
		EnableTaints: TaintConfig{
			Enabled:        true,
			PartitionState: true,
			NodeTaints:     true,
			NodesNotFound:  true,
		},
		EnableTPUAllocatableReadiness: keepTPUAllocatable,
	}
	transform := StripNodeForCache
	if keepTPUAllocatable {
		transform = StripNodeForCacheKeepTPUAllocatable
	}
	now := time.Now()

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	// Phase 1: Simulate 3 churn waves (Wave 0: Ready=True, Wave 1: Ready=False/NotReady
	// + NPD condition + repair taint, Wave 2: Ready=True recovery) across all TPU7x
	// Nodes in the Node informer store + topoCache and assert <= 4.00 KB/node.
	// With keepTPUAllocatable, google.com/tpu is 4 in wave 0 and 0 afterwards.
	for wave := range churnWaves {
		for p := range numPools {
			poolName := fmt.Sprintf("pool-%04d", p)
			migName := fmt.Sprintf("mig-%04d", p)
			migLink := fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/1234/zones/us-central1-a/instanceGroupManagers/%s", migName)
			podBlock := fmt.Sprintf("pod-block-%d", p/poolsPerPod)

			for idx := range nodesPerPool {
				nodeName := fmt.Sprintf("gke-%s-node-%02d", poolName, idx)
				incoming := buildFatNode("tpu7x")
				incoming.Name = nodeName
				incoming.UID = types.UID(fmt.Sprintf("uid-%s-%d", nodeName, wave))
				incoming.ResourceVersion = fmt.Sprintf("%d", (wave+1)*1000+idx)
				incoming.Labels[LabelNodePool] = poolName
				incoming.Labels[LabelTopologyBlock] = podBlock
				incoming.Labels["cloud.google.com/gke-tpu-partition-4x4x4-id"] = fmt.Sprintf("p-%04d-4x4x4", p)
				incoming.Labels["cloud.google.com/gke-tpu-partition-2x2x1-id"] = fmt.Sprintf("p-%04d-2x2x1-%02d", p, idx)
				incoming.Annotations[AnnotationCreatedByMIG] = migLink
				if keepTPUAllocatable {
					tpu := "4"
					if wave > 0 {
						tpu = "0"
					}
					withTPUResources(incoming, tpu)
				}

				// Wave 1 simulates a fleet-wide power/repair event: flip Ready -> False (NotReady),
				// churn NPD conditions, and attach a repair taint; Wave 2 recovers to Ready -> True.
				if wave == 1 {
					incoming.Status.Conditions[0].Status = corev1.ConditionTrue
					incoming.Status.Conditions[len(incoming.Status.Conditions)-1].Status = corev1.ConditionFalse
					incoming.Spec.Taints = append(incoming.Spec.Taints, corev1.Taint{
						Key:    "cloud.google.com/tpu-repair",
						Value:  fmt.Sprintf("wave-%d", wave),
						Effect: corev1.TaintEffectNoSchedule,
					})
				}

				transformed, err := transform(incoming)
				if err != nil {
					t.Fatalf("node transform (%s) failed: %v", nodeName, err)
				}
				newNode := transformed.(*corev1.Node)
				if oldNode, exists := nodeStore[nodeName]; exists {
					_ = pred.Update(event.TypedUpdateEvent[*corev1.Node]{
						ObjectOld: oldNode,
						ObjectNew: newNode,
					})
				}
				nodeStore[nodeName] = newNode

				if entry, ok := parseNode(newNode); ok {
					topoCache.Upsert(newNode.Name, entry)
				}
			}
		}
	}

	runtime.GC()
	var afterNodes runtime.MemStats
	runtime.ReadMemStats(&afterNodes)

	var nodeLiveHeapBytes int64
	if afterNodes.HeapAlloc > before.HeapAlloc {
		nodeLiveHeapBytes = int64(afterNodes.HeapAlloc - before.HeapAlloc)
	}
	nodeLiveMiB := float64(nodeLiveHeapBytes) / (1024 * 1024)
	kbPerNode := float64(nodeLiveHeapBytes) / float64(totalNodes) / 1024.0

	if kbPerNode > maxNodeStoreKBPerNode {
		t.Errorf("%d-Pod Node informer + topoCache retained %.2f KB/node (%.2f MiB total), want <= %.2f KB/node",
			numPods, kbPerNode, nodeLiveMiB, maxNodeStoreKBPerNode)
	}

	// Phase 2: Reconcile the full 31-partition TPU7x PartitionTree CR, PollState,
	// and Metric GaugeSet/Tombstone store across all numPools pools x 3 waves.
	var noExternalTaints map[string][]partitionv1beta1.PartitionTaint
	const skipTaintUpdates = false
	var noExcludedPrefixes []string

	for wave := range churnWaves {
		for p := range numPools {
			poolName := fmt.Sprintf("pool-%04d", p)
			migName := fmt.Sprintf("mig-%04d", p)
			podBlock := fmt.Sprintf("pod-block-%d", p/poolsPerPod)

			gceResp := buildTPU7x31PartitionResponse(p)
			root, err := tree.Build(gceResp, nil)
			if err != nil {
				t.Fatalf("tree.Build(%s) failed: %v", poolName, err)
			}
			tree.Bubble(&root)
			snap := topoCache.PoolLeaves(poolName)
			reconciler.refreshAndPropagate(&root, snap, noExternalTaints, nil, now, skipTaintUpdates, noExcludedPrefixes)
			pollStates.recordSuccess(poolName, now)

			cr := &partitionv1beta1.PartitionTree{
				ObjectMeta: metav1.ObjectMeta{
					Name:            poolName,
					UID:             types.UID(fmt.Sprintf("cr-uid-%04d", p)),
					ResourceVersion: fmt.Sprintf("%d", wave+1),
					ManagedFields:   incomingManagedFields(),
				},
				Status: partitionv1beta1.PartitionTreeStatus{
					AcceleratorType: "tpu7x",
					Root:            &root,
				},
			}
			strippedCR, err := stripCR(cr)
			if err != nil {
				t.Fatalf("TransformStripManagedFields(CR) failed: %v", err)
			}
			crObj := strippedCR.(*partitionv1beta1.PartitionTree)
			crStore[poolName] = crObj

			metrics.RecordPartitionTreeMetrics(ctx, crObj.Status.Root, crObj.Name, string(crObj.UID), poolName, migName, "tpu7x", podBlock)
		}
	}

	if keepTPUAllocatable {
		// Pin that the measured CRs hold the TPUsNotAllocatable worst case.
		var leaves, notAlloc int
		var walk func(p *partitionv1beta1.PartitionInfo)
		walk = func(p *partitionv1beta1.PartitionInfo) {
			if len(p.ChildPartitions) == 0 {
				leaves++
				if c := meta.FindStatusCondition(p.Conditions, partitionv1beta1.ConditionTypeNodeReady); c != nil && c.Reason == tree.ReasonTPUsNotAllocatable {
					notAlloc++
				}
				return
			}
			for i := range p.ChildPartitions {
				walk(&p.ChildPartitions[i])
			}
		}
		walk(crStore["pool-0000"].Status.Root)
		if leaves == 0 || notAlloc != leaves {
			t.Fatalf("pool-0000: %d of %d leaves at %s, want all", notAlloc, leaves, tree.ReasonTPUsNotAllocatable)
		}
	}

	runtime.GC()
	var afterNodeAndController runtime.MemStats
	runtime.ReadMemStats(&afterNodeAndController)

	var totalLiveHeapBytes int64
	if afterNodeAndController.HeapAlloc > before.HeapAlloc {
		totalLiveHeapBytes = int64(afterNodeAndController.HeapAlloc - before.HeapAlloc)
	}
	totalLiveMB := float64(totalLiveHeapBytes) / (1024 * 1024)
	estimatedGOGC100PeakMiB := (totalLiveMB * gogc100HeapMultiplier) + runtimeOverheadMiB
	livePlusRuntimeMiB := totalLiveMB + runtimeOverheadMiB

	t.Logf("%d-Pod Full-Controller Churn (%d MIGs / %d TPU7x nodes / %d partitions x %d waves, keepTPUAllocatable=%v):",
		numPods, numPools, totalNodes, numPools*31, churnWaves, keepTPUAllocatable)
	t.Logf("  -> Node Informer + topoCache Live Heap = %.2f MiB (%.2f KB/node, ceiling <= %.2f KB/node)",
		nodeLiveMiB, kbPerNode, maxNodeStoreKBPerNode)
	t.Logf("  -> Combined Live Heap (Node Informer + CR Informer + topoCache + PollState + Metric Gauge/Tombstone Store) = %.2f MiB (Live + %.0f MiB runtime = %.2f MiB)",
		totalLiveMB, runtimeOverheadMiB, livePlusRuntimeMiB)
	t.Logf("  -> Worst-Case GOGC=100 Peak RSS (%.1fx Live Heap + %.0f MiB runtime) = %.2f MiB (Container Limit = %.0f MiB in config/manager/manager.yaml:89)",
		gogc100HeapMultiplier, runtimeOverheadMiB, estimatedGOGC100PeakMiB, containerMemoryLimitMiB)

	if livePlusRuntimeMiB >= containerMemoryLimitMiB {
		t.Errorf("%d-Pod live heap + runtime overhead %.2f MiB exceeds container limit %.0f MiB",
			numPods, livePlusRuntimeMiB, containerMemoryLimitMiB)
	}
	if estimatedGOGC100PeakMiB >= containerMemoryLimitMiB {
		t.Errorf("%d-Pod estimated GOGC=100 peak RSS %.2f MiB exceeds container limit %.0f MiB (OOM risk!)",
			numPods, estimatedGOGC100PeakMiB, containerMemoryLimitMiB)
	}
	if len(nodeStore) != totalNodes || len(crStore) != numPools {
		t.Fatalf("len(nodeStore)=%d (want %d), len(crStore)=%d (want %d)",
			len(nodeStore), totalNodes, len(crStore), numPools)
	}
}

func incomingManagedFields() []metav1.ManagedFieldsEntry {
	now := metav1.Now()
	return []metav1.ManagedFieldsEntry{
		{
			Manager:    "partition-tree-controller",
			Operation:  metav1.ManagedFieldsOperationUpdate,
			APIVersion: "partition.tpu.google.com/v1beta1",
			Time:       &now,
			FieldsType: "FieldsV1",
			FieldsV1:   &metav1.FieldsV1{Raw: []byte(`{"f:status":{"f:root":{"f:childPartitions":{}}}}`)},
		},
	}
}
