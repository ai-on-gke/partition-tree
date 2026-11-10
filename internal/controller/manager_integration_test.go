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
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
	topocache "github.com/ai-on-gke/partition-tree/internal/cache"
	"github.com/ai-on-gke/partition-tree/internal/config"
	"github.com/ai-on-gke/partition-tree/internal/gce"
	"github.com/ai-on-gke/partition-tree/internal/tree"
)

// TestManagerIntegration_NodeWatchEndToEnd is the full wiring test: a
// real ctrl.Manager is started against the envtest apiserver, the
// reconciler is registered via SetupWithManager (so the Node watch,
// predicate, and source.Kind plumbing are exercised end-to-end), and
// the test drives it entirely through the apiserver — apply a Node
// YAML, assert a PartitionTree CR materializes, delete the Node, assert
// the CR disappears via the scale-to-zero path.
//
// This is the test that catches regressions in SetupWithManager, the
// typed predicates, the Node→pool enqueue mapping, and the finalizer
// cleanup path — none of which are reachable by calling Reconcile()
// directly.
func TestManagerIntegration_NodeWatchEndToEnd(t *testing.T) {
	env := newTestEnv(t)

	mgr, err := ctrl.NewManager(env.cfg, ctrl.Options{
		Scheme: scheme.Scheme,
		// Disable metrics + health probe — envtest doesn't need them
		// and leaving the defaults in place fights over ports in
		// parallel test runs.
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Cache:                  ManagerCacheOptions("default", "partition-tree-config", false),
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	gceMock := gce.NewMockClient()
	gceMock.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))

	cch := topocache.New()
	cfg := config.Default()
	cfg.Taints.NodePropagation.ExcludedPrefixes = []string{"node.kubernetes.io/"}
	rec := &PartitionTreeReconciler{
		Client:       mgr.GetClient(),
		Scheme:       mgr.GetScheme(),
		GCE:          gceMock,
		Cache:        cch,
		ConfigStore:  config.NewStore(cfg),
		Clock:        clock.RealClock{},
		EnableTaints: NewTaintConfig(true),
	}
	if err := rec.SetupWithManager(mgr); err != nil {
		t.Fatalf("setup with manager: %v", err)
	}

	ctx, cancel := context.WithCancel(ctxBG())
	t.Cleanup(cancel)

	mgrDone := make(chan error, 1)
	go func() {
		mgrDone <- mgr.Start(ctx)
	}()

	// Wait for the manager cache to sync before driving any apiserver
	// writes — otherwise the Node watch can miss the first Create.
	syncCtx, syncCancel := context.WithTimeout(ctx, 10*time.Second)
	defer syncCancel()
	if !mgr.GetCache().WaitForCacheSync(syncCtx) {
		t.Fatalf("cache failed to sync")
	}

	nodeName := "node-e2e-0"
	unfilteredNodeName := "node-cpu-unfiltered"
	var node *corev1.Node
	crName := apitypes.NamespacedName{Name: testPoolName}

	t.Run("Create Node and Materialize PartitionTree", func(t *testing.T) {
		// Create a non-PROVISION_ONLY node first to verify the ByObject[Node].Label
		// selector keeps it out of the manager's informer cache entirely.
		cpuNode := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: unfilteredNodeName,
				Labels: map[string]string{
					LabelNodePool: "cpu-pool",
				},
			},
		}
		if err := env.client.Create(ctx, cpuNode); err != nil {
			t.Fatalf("create cpu node: %v", err)
		}
		t.Cleanup(func() {
			_ = env.client.Delete(context.Background(), cpuNode)
		})

		// Apply a Node that matches the four-element filter: topology mode,
		// nodepool label, MIG annotation, and at least one partition-id
		// label. Include fat fields (extra annotations, PodCIDR, extra conditions,
		// images) so we can assert StripNodeForCache trimmed the cached object.
		node = &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: nodeName,
				Labels: map[string]string{
					LabelTopologyMode:   LabelTopologyModeProvisionOnly,
					LabelNodePool:       testPoolName,
					LabelTPUAccelerator: "tpu7x",
					"cloud.google.com/gke-tpu-partition-leaf-id": "leaf",
					"cloud.google.com/gke-tpu-partition-root-id": "root",
				},
				Annotations: map[string]string{
					AnnotationCreatedByMIG:                 testMIG,
					"node.gke.io/last-applied-node-labels": "should-be-stripped",
				},
			},
			Spec: corev1.NodeSpec{
				PodCIDR:       "10.244.0.0/24",
				Unschedulable: true,
			},
		}
		if err := env.client.Create(ctx, node); err != nil {
			t.Fatalf("create node: %v", err)
		}
		// The Ready condition has to be set via the /status subresource —
		// envtest enforces this the same way real apiservers do.
		node.Status.Conditions = []corev1.NodeCondition{
			{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse, Message: "should be stripped"},
			{Type: corev1.NodeReady, Status: corev1.ConditionTrue, Message: "should be stripped"},
		}
		node.Status.Images = []corev1.ContainerImage{
			{Names: []string{"gke.gcr.io/pause:3.9"}, SizeBytes: 1000000},
		}
		if err := env.client.Status().Update(ctx, node); err != nil {
			t.Fatalf("update node status: %v", err)
		}

		// Wait for the CR to materialize with the populated tree. Polling
		// via the direct client avoids any cache-consistency flakiness.
		waitFor(t, 15*time.Second, func() bool {
			var cr partitionv1beta1.PartitionTree
			if err := env.client.Get(ctx, crName, &cr); err != nil {
				return false
			}
			if cr.Status.Root == nil {
				return false
			}
			if cr.Status.Root.PartitionID != "root" {
				return false
			}
			if len(cr.Status.Root.ChildPartitions) != 1 {
				return false
			}
			leaf := cr.Status.Root.ChildPartitions[0]
			if leaf.PartitionID != "leaf" {
				return false
			}
			if len(leaf.NodeNames) != 1 || leaf.NodeNames[0] != nodeName {
				return false
			}
			// AcceleratorType must be stamped on the status from the node label.
			if cr.Status.AcceleratorType != "tpu7x" {
				return false
			}
			return true
		})

		// Verify the informer cache itself:
		// 1. Only the PROVISION_ONLY node is present (ByObject[Node].Label filtered out cpuNode).
		// 2. The cached PROVISION_ONLY node was stripped by StripNodeForCache.
		var cachedNodes corev1.NodeList
		if err := mgr.GetCache().List(ctx, &cachedNodes); err != nil {
			t.Fatalf("mgr.GetCache().List(NodeList): %v", err)
		}
		if len(cachedNodes.Items) != 1 || cachedNodes.Items[0].Name != nodeName {
			t.Fatalf("cached Nodes = %+v, want only [%s] (non-PROVISION_ONLY node %s should be excluded by label selector)",
				cachedNodes.Items, nodeName, unfilteredNodeName)
		}
		cached := cachedNodes.Items[0]
		if cached.ManagedFields != nil || cached.Status.Images != nil || cached.Spec.PodCIDR != "" || cached.Spec.Unschedulable {
			t.Errorf("cached Node retained stripped fields: ManagedFields=%v, Images=%v, Spec=%+v",
				cached.ManagedFields, cached.Status.Images, cached.Spec)
		}
		if len(cached.Annotations) != 1 || cached.Annotations[AnnotationCreatedByMIG] != testMIG {
			t.Errorf("cached Node Annotations = %v, want only %s", cached.Annotations, AnnotationCreatedByMIG)
		}
		if len(cached.Status.Conditions) != 1 || cached.Status.Conditions[0].Type != corev1.NodeReady || cached.Status.Conditions[0].Message != "" {
			t.Errorf("cached Node Status.Conditions = %+v, want single stripped NodeReady condition", cached.Status.Conditions)
		}
	})

	t.Run("Taints Integration", func(t *testing.T) {
		t.Run("Apply valid taint on leaf", func(t *testing.T) {
			var cr partitionv1beta1.PartitionTree
			if err := env.client.Get(ctx, crName, &cr); err != nil {
				t.Fatalf("get cr: %v", err)
			}
			if cr.Annotations == nil {
				cr.Annotations = make(map[string]string)
			}
			cr.Annotations[tree.TaintAnnotationPrefix+"initial"] = `{"partitionId":"leaf", "key":"initial", "effect":"NoSchedule"}`
			if err := env.client.Update(ctx, &cr); err != nil {
				t.Fatalf("update cr: %v", err)
			}

			waitFor(t, 15*time.Second, func() bool {
				var tree partitionv1beta1.PartitionTree
				if err := env.client.Get(ctx, crName, &tree); err != nil {
					return false
				}
				if tree.Status.Root == nil || len(tree.Status.Root.ChildPartitions) != 1 {
					return false
				}
				leaf1 := &tree.Status.Root.ChildPartitions[0]
				taintApplied := len(leaf1.Taints) == 1 && leaf1.Taints[0].Key == "initial"

				cond := meta.FindStatusCondition(tree.Status.Conditions, partitionv1beta1.ConditionTypeTaintsValid)
				condValid := cond != nil && cond.Status == metav1.ConditionTrue

				return taintApplied && condValid
			})
		})

		t.Run("Malformed JSON preserves taint and sets condition", func(t *testing.T) {
			var cr partitionv1beta1.PartitionTree
			if err := env.client.Get(ctx, crName, &cr); err != nil {
				t.Fatalf("get cr: %v", err)
			}
			cr.Annotations[tree.TaintAnnotationPrefix+"initial"] = `{"partitionId":"leaf", "key": "k"`
			if err := env.client.Update(ctx, &cr); err != nil {
				t.Fatalf("update cr: %v", err)
			}

			waitFor(t, 15*time.Second, func() bool {
				var tree partitionv1beta1.PartitionTree
				if err := env.client.Get(ctx, crName, &tree); err != nil {
					return false
				}
				if tree.Status.Root == nil || len(tree.Status.Root.ChildPartitions) != 1 {
					return false
				}
				leaf := &tree.Status.Root.ChildPartitions[0]
				// Taint should be preserved (still "initial")
				taintPreserved := len(leaf.Taints) == 1 && leaf.Taints[0].Key == "initial"

				// Condition should be False
				cond := meta.FindStatusCondition(tree.Status.Conditions, partitionv1beta1.ConditionTypeTaintsValid)
				condFailed := cond != nil && cond.Status == metav1.ConditionFalse && cond.Reason == "TaintParseFailed"

				return taintPreserved && condFailed
			})
		})

		t.Run("Ignore taint on missing partition", func(t *testing.T) {
			var cr partitionv1beta1.PartitionTree
			if err := env.client.Get(ctx, crName, &cr); err != nil {
				t.Fatalf("get cr: %v", err)
			}
			delete(cr.Annotations, tree.TaintAnnotationPrefix+"initial")
			cr.Annotations[tree.TaintAnnotationPrefix+"ghost"] = `{"partitionId":"missing", "key":"ghost", "effect":"NoSchedule"}`
			cr.Annotations[tree.TaintAnnotationPrefix+"maint"] = `{"partitionId":"leaf", "key":"maintenance", "effect":"NoSchedule"}`
			if err := env.client.Update(ctx, &cr); err != nil {
				t.Fatalf("update cr: %v", err)
			}

			waitFor(t, 15*time.Second, func() bool {
				var tree partitionv1beta1.PartitionTree
				if err := env.client.Get(ctx, crName, &tree); err != nil {
					return false
				}
				if tree.Status.Root == nil || len(tree.Status.Root.ChildPartitions) != 1 {
					return false
				}
				leaf1 := &tree.Status.Root.ChildPartitions[0]
				if len(leaf1.Taints) == 0 {
					return false
				}
				return leaf1.Taints[0].Key == "maintenance" && leaf1.Taints[0].Effect == partitionv1beta1.TaintEffectNoSchedule
			})

			// Ensure the "missing" taint didn't end up anywhere
			var finalTree partitionv1beta1.PartitionTree
			if err := env.client.Get(ctx, crName, &finalTree); err != nil {
				t.Fatalf("get cr: %v", err)
			}
			if len(finalTree.Status.Root.ChildPartitions[0].Taints) != 1 {
				t.Errorf("expected exactly 1 taint on leaf, got %v", finalTree.Status.Root.ChildPartitions[0].Taints)
			}
			for _, tnt := range finalTree.Status.Root.Taints {
				if tnt.Key == "ghost" {
					t.Errorf("taint targeting non-existent partition was applied to root")
				}
			}

			// Verify that a Warning event was emitted for the unused taint
			var eventList corev1.EventList
			if err := env.client.List(ctx, &eventList, client.InNamespace("default")); err != nil {
				t.Fatalf("list events: %v", err)
			}
			foundUnusedTaintEvent := false
			for _, e := range eventList.Items {
				if e.Reason == EventReasonUnusedTaint && e.Type == corev1.EventTypeWarning {
					if strings.Contains(e.Message, `Taint targeted at non-existent partition "missing"`) {
						foundUnusedTaintEvent = true
						break
					}
				}
			}
			if !foundUnusedTaintEvent {
				t.Errorf("expected warning event for unused taint target, but none found")
			}
		})

		t.Run("Verify no bottom-up aggregation", func(t *testing.T) {
			waitFor(t, 15*time.Second, func() bool {
				var tree partitionv1beta1.PartitionTree
				if err := env.client.Get(ctx, crName, &tree); err != nil {
					return false
				}
				if tree.Status.Root == nil {
					return false
				}
				// Root should NOT have taints, even though its only child (leaf) has a taint.
				return len(tree.Status.Root.Taints) == 0
			})
		})
	})

	t.Run("Drain Pool and Scale to Zero", func(t *testing.T) {
		if err := env.client.Delete(ctx, node); err != nil {
			t.Fatalf("delete node: %v", err)
		}

		waitFor(t, 15*time.Second, func() bool {
			var cr partitionv1beta1.PartitionTree
			err := env.client.Get(ctx, crName, &cr)
			return apierrors.IsNotFound(err)
		})
	})

	// Manager should still be running — shut it down cleanly.
	cancel()
	select {
	case err := <-mgrDone:
		if err != nil && err != context.Canceled {
			t.Errorf("manager exited: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Errorf("manager did not exit within 5s")
	}
}

// TestManagerIntegration_TaintsDisabled verifies that when EnableTaints is false,
// the controller ignores the taints annotation and does not apply taints to the status.
func TestManagerIntegration_TaintsDisabled(t *testing.T) {
	env := newTestEnv(t)

	mgr, err := ctrl.NewManager(env.cfg, ctrl.Options{
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Cache:                  ManagerCacheOptions("", "", false),
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	gceMock := gce.NewMockClient()
	gceMock.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))

	cch := topocache.New()
	rec := &PartitionTreeReconciler{
		Client:         mgr.GetClient(),
		Scheme:         mgr.GetScheme(),
		GCE:            gceMock,
		Cache:          cch,
		ConfigStore:    config.NewStore(config.Default()),
		Clock:          clock.RealClock{},
		EnableTaints:   NewTaintConfig(false),
		ControllerName: "partitiontree-disabled",
	}
	if err := rec.SetupWithManager(mgr); err != nil {
		t.Fatalf("setup with manager: %v", err)
	}

	ctx, cancel := context.WithCancel(ctxBG())
	t.Cleanup(cancel)

	mgrDone := make(chan error, 1)
	go func() {
		mgrDone <- mgr.Start(ctx)
	}()

	if !mgr.GetCache().WaitForCacheSync(ctx) {
		t.Fatalf("cache failed to sync")
	}

	nodeName := "node-e2e-0"
	crName := apitypes.NamespacedName{Name: testPoolName}

	// Create Node to materialize CR
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
			Labels: map[string]string{
				LabelTopologyMode:   LabelTopologyModeProvisionOnly,
				LabelNodePool:       testPoolName,
				LabelTPUAccelerator: "tpu7x",
				"cloud.google.com/gke-tpu-partition-leaf-id": "leaf",
				"cloud.google.com/gke-tpu-partition-root-id": "root",
			},
			Annotations: map[string]string{
				AnnotationCreatedByMIG: testMIG,
			},
		},
	}
	if err := env.client.Create(ctx, node); err != nil {
		t.Fatalf("create node: %v", err)
	}
	node.Status.Conditions = []corev1.NodeCondition{
		{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
	}
	if err := env.client.Status().Update(ctx, node); err != nil {
		t.Fatalf("update node status: %v", err)
	}

	// Wait for CR
	waitFor(t, 15*time.Second, func() bool {
		var cr partitionv1beta1.PartitionTree
		if err := env.client.Get(ctx, crName, &cr); err != nil {
			return false
		}
		return cr.Status.Root != nil
	})

	// Apply taint annotation AND change GCE mock response state to "Degraded"
	// so we can wait for this change to be reflected, proving the reconciler ran.
	gceMock.SetResponse("testpool-mig", &gce.TopologyResponse{
		Entries: []gce.TopologyEntry{
			{PartitionID: "root", Topology: "4x4x4", State: "Healthy"},
			{PartitionID: "leaf", ParentID: "root", Topology: "2x2x1", State: "Degraded"},
		},
	})

	var cr partitionv1beta1.PartitionTree
	if err := env.client.Get(ctx, crName, &cr); err != nil {
		t.Fatalf("get cr: %v", err)
	}
	if cr.Annotations == nil {
		cr.Annotations = make(map[string]string)
	}
	cr.Annotations[tree.TaintAnnotationPrefix+"test-taint"] = `{"partitionId":"leaf", "key":"test-taint", "effect":"NoSchedule"}`
	if err := env.client.Update(ctx, &cr); err != nil {
		t.Fatalf("update cr: %v", err)
	}

	// Wait for the status to reflect the GCE topology change (State=Degraded).
	// This ensures the reconciler has run after the annotation update.
	waitFor(t, 15*time.Second, func() bool {
		var tree partitionv1beta1.PartitionTree
		if err := env.client.Get(ctx, crName, &tree); err != nil {
			return false
		}
		if tree.Status.Root == nil || len(tree.Status.Root.ChildPartitions) != 1 {
			return false
		}
		return tree.Status.Root.ChildPartitions[0].State == "Degraded"
	})

	var finalCR partitionv1beta1.PartitionTree
	if err := env.client.Get(ctx, crName, &finalCR); err != nil {
		t.Fatalf("get cr: %v", err)
	}

	if finalCR.Status.Root == nil {
		t.Fatalf("root is nil")
	}

	// Verify no taints on root or leaf
	if len(finalCR.Status.Root.Taints) != 0 {
		t.Errorf("expected no taints on root, got: %v", finalCR.Status.Root.Taints)
	}
	if len(finalCR.Status.Root.ChildPartitions) != 1 {
		t.Fatalf("expected 1 child partition")
	}
	leaf := finalCR.Status.Root.ChildPartitions[0]
	if len(leaf.Taints) != 0 {
		t.Errorf("expected no taints on leaf, got: %v", leaf.Taints)
	}

	// Verify TaintsValid condition is NOT present
	cond := meta.FindStatusCondition(finalCR.Status.Conditions, partitionv1beta1.ConditionTypeTaintsValid)
	if cond != nil {
		t.Errorf("expected no TaintsValid condition, got: %+v", cond)
	}

	// Clean up manager
	cancel()
	<-mgrDone
}

// TestManagerIntegration_TPUAllocatableReadiness drives the
// --enable-tpu-allocatable-readiness path through a real manager: the
// keep-TPU transform, the predicate's allocatable diff, and the Node
// handler's cache update. A Ready node with google.com/tpu 0 must turn the
// leaf False/TPUsNotAllocatable; an allocatable-only status update to 4
// (Ready unchanged) must flip it to NodesReady. The topology cache is only
// fed by predicate-admitted events, so the periodic requeue alone cannot
// make the second assertion pass.
func TestManagerIntegration_TPUAllocatableReadiness(t *testing.T) {
	env := newTestEnv(t)

	mgr, err := ctrl.NewManager(env.cfg, ctrl.Options{
		Scheme:                 scheme.Scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Cache:                  ManagerCacheOptions("", "", true),
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	gceMock := gce.NewMockClient()
	gceMock.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))

	rec := &PartitionTreeReconciler{
		Client:                        mgr.GetClient(),
		Scheme:                        mgr.GetScheme(),
		GCE:                           gceMock,
		Cache:                         topocache.New(),
		ConfigStore:                   config.NewStore(config.Default()),
		Clock:                         clock.RealClock{},
		EnableTaints:                  NewTaintConfig(true),
		EnableTPUAllocatableReadiness: true,
		ControllerName:                "partitiontree-tpu-allocatable",
	}
	if err := rec.SetupWithManager(mgr); err != nil {
		t.Fatalf("setup with manager: %v", err)
	}

	ctx, cancel := context.WithCancel(ctxBG())
	t.Cleanup(cancel)

	mgrDone := make(chan error, 1)
	go func() {
		mgrDone <- mgr.Start(ctx)
	}()

	syncCtx, syncCancel := context.WithTimeout(ctx, 10*time.Second)
	defer syncCancel()
	if !mgr.GetCache().WaitForCacheSync(syncCtx) {
		t.Fatalf("cache failed to sync")
	}

	nodeName := "node-e2e-0"
	crName := apitypes.NamespacedName{Name: testPoolName}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
			Labels: map[string]string{
				LabelTopologyMode:   LabelTopologyModeProvisionOnly,
				LabelNodePool:       testPoolName,
				LabelTPUAccelerator: "tpu7x",
				"cloud.google.com/gke-tpu-partition-leaf-id": "leaf",
				"cloud.google.com/gke-tpu-partition-root-id": "root",
			},
			Annotations: map[string]string{
				AnnotationCreatedByMIG: testMIG,
			},
		},
	}
	if err := env.client.Create(ctx, node); err != nil {
		t.Fatalf("create node: %v", err)
	}
	node.Status.Conditions = []corev1.NodeCondition{
		{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
	}
	node.Status.Allocatable = corev1.ResourceList{
		ResourceTPU:        resource.MustParse("0"),
		corev1.ResourceCPU: resource.MustParse("178"),
	}
	if err := env.client.Status().Update(ctx, node); err != nil {
		t.Fatalf("update node status: %v", err)
	}

	leafNodeReady := func() *metav1.Condition {
		var cr partitionv1beta1.PartitionTree
		if err := env.client.Get(ctx, crName, &cr); err != nil {
			return nil
		}
		if cr.Status.Root == nil || len(cr.Status.Root.ChildPartitions) != 1 {
			return nil
		}
		return meta.FindStatusCondition(cr.Status.Root.ChildPartitions[0].Conditions, partitionv1beta1.ConditionTypeNodeReady)
	}

	waitFor(t, 15*time.Second, func() bool {
		c := leafNodeReady()
		return c != nil && c.Status == metav1.ConditionFalse && c.Reason == tree.ReasonTPUsNotAllocatable
	})

	// The cached Node keeps only the google.com/tpu allocatable entry.
	var cached corev1.Node
	if err := mgr.GetCache().Get(ctx, apitypes.NamespacedName{Name: nodeName}, &cached); err != nil {
		t.Fatalf("mgr.GetCache().Get(Node): %v", err)
	}
	if len(cached.Status.Allocatable) != 1 {
		t.Errorf("cached Node Status.Allocatable = %v, want only %s", cached.Status.Allocatable, ResourceTPU)
	}

	// Allocatable-only update: Ready and every label stay the same.
	if err := env.client.Get(ctx, apitypes.NamespacedName{Name: nodeName}, node); err != nil {
		t.Fatalf("get node: %v", err)
	}
	node.Status.Allocatable[ResourceTPU] = resource.MustParse("4")
	if err := env.client.Status().Update(ctx, node); err != nil {
		t.Fatalf("update node allocatable: %v", err)
	}

	waitFor(t, 15*time.Second, func() bool {
		c := leafNodeReady()
		return c != nil && c.Status == metav1.ConditionTrue && c.Reason == tree.ReasonNodesReady
	})

	cancel()
	<-mgrDone
}
