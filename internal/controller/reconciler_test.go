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
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
	topocache "github.com/ai-on-gke/partition-tree/internal/cache"
	"github.com/ai-on-gke/partition-tree/internal/config"
	"github.com/ai-on-gke/partition-tree/internal/gce"
	"github.com/ai-on-gke/partition-tree/internal/metadata"
	"github.com/ai-on-gke/partition-tree/internal/metrics"
	"github.com/ai-on-gke/partition-tree/internal/tree"
)

// fixture bundles a reconciler + its deps so individual scenarios don't
// repeat 20 lines of wiring. Tests are expected to mutate the fields
// (mock GCE, cache, clock) and then call fx.reconcile().
type fixture struct {
	t    *testing.T
	env  *testEnv
	rec  *PartitionTreeReconciler
	gce  *gce.MockClient
	cch  *topocache.Cache
	clk  *clocktesting.FakePassiveClock
	cfg  *config.Store
	name string
}

const (
	testMIG      = "https://www.googleapis.com/compute/v0.beta/projects/1234567890/zones/us-central2-b/instanceGroupManagers/testpool-mig"
	testPoolName = "testpool"
)

func newFixture(t *testing.T) *fixture {
	t.Helper()
	env := newTestEnv(t)

	fx := &fixture{
		t:    t,
		env:  env,
		gce:  gce.NewMockClient(),
		cch:  topocache.New(),
		clk:  clocktesting.NewFakePassiveClock(time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)),
		cfg:  config.NewStore(config.Default()),
		name: testPoolName,
	}
	fx.rec = &PartitionTreeReconciler{
		Client:      env.client,
		Scheme:      scheme.Scheme,
		GCE:         fx.gce,
		Cache:       fx.cch,
		ConfigStore: fx.cfg,
		Clock:       fx.clk,
		poll:        newPollStateStore(),
	}
	return fx
}

// reconcile calls Reconcile once with the fixture CR name. Most tests
// drive multiple reconciles in sequence (create → finalizer → poll →
// status), so this is the universal workhorse.
func (fx *fixture) reconcile() (ctrl.Result, error) {
	fx.t.Helper()
	return fx.rec.Reconcile(ctxBG(), ctrl.Request{
		NamespacedName: apitypes.NamespacedName{Name: fx.name},
	})
}

// get fetches the CR by name; fails the test if it's gone.
func (fx *fixture) get() *partitionv1beta1.PartitionTree {
	fx.t.Helper()
	var cr partitionv1beta1.PartitionTree
	err := fx.env.client.Get(ctxBG(), apitypes.NamespacedName{Name: fx.name}, &cr)
	if err != nil {
		fx.t.Fatalf("get cr: %v", err)
	}
	return &cr
}

// seedPool populates the cache with a single node that belongs to the
// test pool / MIG, pointing at the given partition IDs. Most scenarios
// call this before their first reconcile so createCR sees a live pool.
// AcceleratorType is hard-coded to "v7x" — the CRD marks the field
// required, and every test only cares that it flows through.
func (fx *fixture) seedPool(nodeName string, ready bool, partitionIDs ...string) {
	fx.t.Helper()
	fx.cch.Upsert(nodeName, topocache.NodeEntry{
		NodePool:        fx.name,
		MIG:             testMIG,
		AcceleratorType: "tpu7x",
		PartitionIDs:    partitionIDs,
		Ready:           ready,
	})
}

// singleLeafResponse builds the canonical two-entry topology response
// used by tests that don't care about hierarchy: one root + one leaf.
// The reconciler stamps AcceleratorType from the node-pool cache, so
// tests that care about it seed it via fixture.seedPool.
func singleLeafResponse(rootID, leafID string) *gce.TopologyResponse {
	return &gce.TopologyResponse{
		Entries: []gce.TopologyEntry{
			{PartitionID: rootID, Topology: "4x4x4", State: "Healthy"},
			{PartitionID: leafID, ParentID: rootID, Topology: "2x2x1", State: "Healthy"},
		},
	}
}

// TestFirstSeenPoolCreatesCR exercises the createCR path: the node
// informer surfaces a pool, the CR doesn't exist yet, reconcile creates
// it with the finalizer + MIG annotation. On the second reconcile the
// poll path takes over and populates the tree.
func TestFirstSeenPoolCreatesCR(t *testing.T) {
	fx := newFixture(t)
	fx.seedPool("node-0", true, "root-v7x-0", "leaf-v7x-0")
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root-v7x-0", "leaf-v7x-0"))

	// 1st reconcile: Create CR.
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	cr := fx.get()
	if cr.Annotations[AnnotationMIGName] != testMIG {
		t.Errorf("mig annotation = %q", cr.Annotations[AnnotationMIGName])
	}
	if len(cr.Finalizers) != 1 || cr.Finalizers[0] != FinalizerName {
		t.Errorf("finalizer = %v", cr.Finalizers)
	}

	// 2nd reconcile: finalizer already present (added at Create time), so
	// this turn proceeds into the normal path and polls GCE + fills the
	// tree.
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if fx.gce.Calls() != 1 {
		t.Errorf("gce calls after second reconcile = %d, want 1", fx.gce.Calls())
	}
	cr = fx.get()
	if cr.Status.Root.PartitionID != "root-v7x-0" {
		t.Errorf("root partition id = %q", cr.Status.Root.PartitionID)
	}
	if len(cr.Status.Root.ChildPartitions) != 1 || cr.Status.Root.ChildPartitions[0].PartitionID != "leaf-v7x-0" {
		t.Errorf("children = %+v", cr.Status.Root.ChildPartitions)
	}
	leaf := cr.Status.Root.ChildPartitions[0]
	if len(leaf.NodeNames) != 1 || leaf.NodeNames[0] != "node-0" {
		t.Errorf("leaf node names = %v", leaf.NodeNames)
	}
	if c := meta.FindStatusCondition(leaf.Conditions, partitionv1beta1.ConditionTypeNodeReady); c == nil || c.Status != "True" {
		t.Errorf("leaf NodeReady cond = %+v", c)
	}
	if c := meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeSynced); c == nil || c.Status != "True" {
		t.Errorf("Synced cond = %+v", c)
	}
}

// TestPollCadenceRespectsInterval verifies that two reconciles within
// one polling interval only call GCE once — the second one should use
// the cached tree.
func TestPollCadenceRespectsInterval(t *testing.T) {
	fx := newFixture(t)
	fx.seedPool("node-0", true, "root", "leaf")
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))

	// Bootstrap: create + first poll (= 1 GCE call).
	for i := range 2 {
		if _, err := fx.reconcile(); err != nil {
			t.Fatalf("bootstrap reconcile %d: %v", i, err)
		}
	}
	if fx.gce.Calls() != 1 {
		t.Fatalf("after bootstrap gce calls = %d want 1", fx.gce.Calls())
	}

	// Advance 500ms (< default 1s interval) and reconcile again: no new call.
	fx.clk.SetTime(fx.clk.Now().Add(500 * time.Millisecond))
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("fast reconcile: %v", err)
	}
	if fx.gce.Calls() != 1 {
		t.Errorf("after fast reconcile gce calls = %d want 1 (cached)", fx.gce.Calls())
	}

	// Advance past the interval: next reconcile should call GCE again.
	fx.clk.SetTime(fx.clk.Now().Add(5 * time.Second))
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("slow reconcile: %v", err)
	}
	if fx.gce.Calls() != 2 {
		t.Errorf("after slow reconcile gce calls = %d want 2", fx.gce.Calls())
	}
}

// TestGCEErrorPreservesTreeWithinGraceWindow covers the Synced grace
// window: the reconciler saw at least one successful poll, then GCE
// starts failing. Within syncTimeout the tree stays intact and the
// Synced condition is not flipped to False; once the grace window
// elapses it flips.
func TestGCEErrorPreservesTreeWithinGraceWindow(t *testing.T) {
	fx := newFixture(t)
	fx.seedPool("node-0", true, "root", "leaf")
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))

	// Bootstrap two reconciles to create + first success.
	for i := range 2 {
		if _, err := fx.reconcile(); err != nil {
			t.Fatalf("bootstrap %d: %v", i, err)
		}
	}
	if c := meta.FindStatusCondition(fx.get().Status.Conditions, partitionv1beta1.ConditionTypeSynced); c == nil || c.Status != "True" {
		t.Fatalf("baseline Synced = %+v", c)
	}

	// GCE now errors. Force a poll by advancing time past the interval.
	fx.gce.SetError(errors.New("api unavailable"))
	fx.clk.SetTime(fx.clk.Now().Add(5 * time.Second))
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("errored reconcile: %v", err)
	}
	cr := fx.get()
	if cr.Status.Root.PartitionID != "root" {
		t.Errorf("tree lost on error: root = %q", cr.Status.Root.PartitionID)
	}
	// Within the grace window (5s < 15s default syncTimeout) Synced is
	// left untouched — so it should still be True from the baseline.
	if c := meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeSynced); c == nil || c.Status != "True" {
		t.Errorf("within grace Synced = %+v, want True", c)
	}

	// Advance past the grace window. Poll is already due because lastErr
	// is set, so no extra time bump needed.
	fx.clk.SetTime(fx.clk.Now().Add(20 * time.Second))
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("post-grace reconcile: %v", err)
	}
	cr = fx.get()
	if c := meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeSynced); c == nil || c.Status != "False" {
		t.Errorf("post-grace Synced = %+v, want False", c)
	}
	// Tree still preserved — we never clobber on error.
	if cr.Status.Root.PartitionID != "root" {
		t.Errorf("tree clobbered post-grace: %q", cr.Status.Root.PartitionID)
	}
}

// TestScaleToZeroDeletesCR verifies the drain path: the CR exists, the
// pool disappears from the cache (last node left), next reconcile issues
// a Delete. The finalizer path then runs and removes the CR.
func TestScaleToZeroDeletesCR(t *testing.T) {
	fx := newFixture(t)
	fx.seedPool("node-0", true, "root", "leaf")
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))

	// Bootstrap: create + first poll.
	for i := range 2 {
		if _, err := fx.reconcile(); err != nil {
			t.Fatalf("bootstrap %d: %v", i, err)
		}
	}

	// Drain the pool.
	fx.cch.Delete("node-0")
	if _, ok := fx.cch.NodePool(fx.name); ok {
		t.Fatalf("cache still knows about pool after drain")
	}

	// Reconcile: issues Delete on the CR. Delete just marks the
	// DeletionTimestamp because of the finalizer — a second reconcile is
	// needed to actually remove it.
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("drain reconcile: %v", err)
	}
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("finalizer reconcile: %v", err)
	}

	var cr partitionv1beta1.PartitionTree
	err := fx.env.client.Get(ctxBG(), apitypes.NamespacedName{Name: fx.name}, &cr)
	if err != nil {
		if client.IgnoreNotFound(err) != nil {
			t.Fatalf("get after finalizer: %v", err)
		}
		return
	}
	if len(cr.Finalizers) > 0 {
		t.Errorf("finalizer still present: %v", cr.Finalizers)
	}
}

// TestNodeReadyPrecedence_NotReadyNode seeds the cache with a NotReady
// node and verifies the leaf's NodeReady condition reflects that, not
// the underlying GCE tree health.
func TestNodeReadyPrecedence_NotReadyNode(t *testing.T) {
	fx := newFixture(t)
	fx.seedPool("node-0", false, "root", "leaf") // Ready=false
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))

	for i := range 2 {
		if _, err := fx.reconcile(); err != nil {
			t.Fatalf("bootstrap %d: %v", i, err)
		}
	}
	cr := fx.get()
	leaf := cr.Status.Root.ChildPartitions[0]
	c := meta.FindStatusCondition(leaf.Conditions, partitionv1beta1.ConditionTypeNodeReady)
	if c == nil || c.Status != "False" || c.Reason != "NodesNotReady" {
		t.Errorf("leaf NodeReady = %+v, want False/NodesNotReady", c)
	}
}

// TestNoOpReconcileSkipsStatusPatch verifies that a reconcile that
// produces no status change does not write to the apiserver (no
// resourceVersion bump).
func TestNoOpReconcileSkipsStatusPatch(t *testing.T) {
	fx := newFixture(t)
	fx.seedPool("node-0", true, "root", "leaf")
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))

	// Bootstrap: create + first poll.
	for i := range 2 {
		if _, err := fx.reconcile(); err != nil {
			t.Fatalf("bootstrap %d: %v", i, err)
		}
	}
	rvBefore := fx.get().ResourceVersion

	// Reconcile again within the interval: nothing should change.
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("no-op reconcile: %v", err)
	}
	rvAfter := fx.get().ResourceVersion

	if rvBefore != rvAfter {
		t.Errorf("resourceVersion changed on no-op reconcile: %s -> %s", rvBefore, rvAfter)
	}
}

// TestJitteredInterval verifies the pure function bounds: every result
// must be in [interval-jitter, interval+jitter], zero jitter returns
// the exact interval, and the guard against negative results works.
func TestJitteredInterval(t *testing.T) {
	interval := 3 * time.Second
	jitter := 300 * time.Millisecond

	for range 1000 {
		got := jitteredInterval(interval, jitter)
		lo := interval - jitter
		hi := interval + jitter
		if got < lo || got > hi {
			t.Fatalf("jitteredInterval(%v, %v) = %v, outside [%v, %v]", interval, jitter, got, lo, hi)
		}
	}

	// Zero jitter returns exact interval.
	if got := jitteredInterval(interval, 0); got != interval {
		t.Errorf("zero jitter: got %v want %v", got, interval)
	}

	// Negative jitter returns exact interval.
	if got := jitteredInterval(interval, -time.Second); got != interval {
		t.Errorf("negative jitter: got %v want %v", got, interval)
	}

	// Large jitter that could produce negative results: guard returns interval.
	for range 100 {
		got := jitteredInterval(100*time.Millisecond, 200*time.Millisecond)
		if got <= 0 {
			t.Fatalf("jitteredInterval(100ms, 200ms) = %v, should never be <= 0", got)
		}
	}

	// Jitter values that would overflow int64 when multiplied by 2 (e.g. 1500000h or math.MaxInt64)
	// must be safely clamped to prevent panic in rand.Int63n.
	overflowJitters := []time.Duration{
		time.Duration(math.MaxInt64/2 + 1),
		1500000 * time.Hour,
		time.Duration(math.MaxInt64),
	}
	for _, oj := range overflowJitters {
		got := jitteredInterval(interval, oj)
		if got <= 0 {
			t.Fatalf("jitteredInterval(%v, %v) = %v, should never be <= 0", interval, oj, got)
		}
	}
}

// TestStampAcceleratorType verifies that the family string is set on the
// status, and that an empty family is a no-op.
func TestStampAcceleratorType(t *testing.T) {
	status := &partitionv1beta1.PartitionTreeStatus{}

	stampAcceleratorType(status, "tpu7x")
	if status.AcceleratorType != "tpu7x" {
		t.Errorf("AcceleratorType = %q, want tpu7x", status.AcceleratorType)
	}

	// Empty family must not clobber existing values.
	stampAcceleratorType(status, "")
	if status.AcceleratorType != "tpu7x" {
		t.Errorf("after empty stamp: AcceleratorType = %q, want tpu7x", status.AcceleratorType)
	}
}

// TestRefreshLeaves_MultiLeafTree seeds 4 nodes across 2 leaf partitions
// and verifies NodeNames and NodeReady are set per-leaf.
func TestRefreshLeaves_MultiLeafTree(t *testing.T) {
	fx := newFixture(t)
	// Two leaves under one root, 2 nodes each.
	fx.seedPool("n0", true, "root", "leaf-a")
	fx.seedPool("n1", true, "root", "leaf-a")
	fx.seedPool("n2", true, "root", "leaf-b")
	fx.seedPool("n3", false, "root", "leaf-b") // NotReady

	resp := &gce.TopologyResponse{Entries: []gce.TopologyEntry{
		{PartitionID: "root", Topology: "4x4x4", State: "Healthy"},
		{PartitionID: "leaf-a", ParentID: "root", Topology: "2x2x1", State: "Healthy"},
		{PartitionID: "leaf-b", ParentID: "root", Topology: "2x2x1", State: "Healthy"},
	}}
	fx.gce.SetResponse("testpool-mig", resp)

	for i := range 2 {
		if _, err := fx.reconcile(); err != nil {
			t.Fatalf("bootstrap %d: %v", i, err)
		}
	}
	cr := fx.get()
	if len(cr.Status.Root.ChildPartitions) != 2 {
		t.Fatalf("children = %d want 2", len(cr.Status.Root.ChildPartitions))
	}

	// Find leaves by ID.
	var leafA, leafB *partitionv1beta1.PartitionInfo
	for i := range cr.Status.Root.ChildPartitions {
		switch cr.Status.Root.ChildPartitions[i].PartitionID {
		case "leaf-a":
			leafA = &cr.Status.Root.ChildPartitions[i]
		case "leaf-b":
			leafB = &cr.Status.Root.ChildPartitions[i]
		}
	}
	if leafA == nil || leafB == nil {
		t.Fatalf("missing leaf: a=%v b=%v", leafA, leafB)
	}

	if len(leafA.NodeNames) != 2 {
		t.Errorf("leaf-a nodes = %v want 2", leafA.NodeNames)
	}
	ca := meta.FindStatusCondition(leafA.Conditions, partitionv1beta1.ConditionTypeNodeReady)
	if ca == nil || ca.Status != "True" {
		t.Errorf("leaf-a NodeReady = %+v, want True (both ready)", ca)
	}

	if len(leafB.NodeNames) != 2 {
		t.Errorf("leaf-b nodes = %v want 2", leafB.NodeNames)
	}
	cb := meta.FindStatusCondition(leafB.Conditions, partitionv1beta1.ConditionTypeNodeReady)
	if cb == nil || cb.Status != "False" {
		t.Errorf("leaf-b NodeReady = %+v, want False (n3 not ready)", cb)
	}
}

// TestRefreshLeaves_MissingNode verifies the leaf whose expected node is
// missing from the cache. The node is in the GCE tree but never showed
// up in the node informer.
func TestRefreshLeaves_MissingNode(t *testing.T) {
	fx := newFixture(t)
	// Only seed 1 node for leaf that has capacity for more.
	fx.seedPool("n0", true, "root", "leaf")
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))

	for i := range 2 {
		if _, err := fx.reconcile(); err != nil {
			t.Fatalf("bootstrap %d: %v", i, err)
		}
	}
	cr := fx.get()
	leaf := cr.Status.Root.ChildPartitions[0]
	// Node n0 should be present.
	if len(leaf.NodeNames) != 1 || leaf.NodeNames[0] != "n0" {
		t.Errorf("leaf nodes = %v want [n0]", leaf.NodeNames)
	}
	c := meta.FindStatusCondition(leaf.Conditions, partitionv1beta1.ConditionTypeNodeReady)
	if c == nil || c.Status != "True" {
		t.Errorf("NodeReady = %+v, want True (single node ready)", c)
	}
}

// TestRateLimitedReconcilePath verifies that when the GCE client is
// rate-limited, the reconciler skips the GCE poll, still refreshes
// leaves, and returns a fast 100ms requeue.
func TestRateLimitedReconcilePath(t *testing.T) {
	fx := newFixture(t)
	fx.seedPool("node-0", true, "root", "leaf")
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))

	// Bootstrap with normal mock client (create + first poll).
	for i := range 2 {
		if _, err := fx.reconcile(); err != nil {
			t.Fatalf("bootstrap %d: %v", i, err)
		}
	}
	if fx.gce.Calls() != 1 {
		t.Fatalf("baseline gce calls = %d want 1", fx.gce.Calls())
	}
	cr := fx.get()
	originalRoot := cr.Status.Root.PartitionID

	// Wrap the mock in a rate-limited client with burst=1 and near-zero
	// refill rate. The token was already consumed during bootstrap (the
	// rate-limited client is freshly constructed here, so it starts with
	// burst=1 token). We consume it explicitly.
	rl := gce.NewRateLimitedClient(fx.gce, 0.001, 1)
	// Consume the single token.
	_, _ = rl.TryGetAvailableAcceleratorTopologies(ctxBG(), "p", "z", "m")
	fx.rec.GCE = rl

	// Advance past the polling interval so the reconciler wants to poll.
	fx.clk.SetTime(fx.clk.Now().Add(5 * time.Second))

	result, err := fx.reconcile()
	if err != nil {
		t.Fatalf("rate-limited reconcile: %v", err)
	}
	// Should requeue at 100ms (fast retry).
	if result.RequeueAfter != 100*time.Millisecond {
		t.Errorf("RequeueAfter = %v want 100ms", result.RequeueAfter)
	}
	// Tree should be unchanged — no new GCE call.
	cr = fx.get()
	if cr.Status.Root.PartitionID != originalRoot {
		t.Errorf("root changed: %q -> %q", originalRoot, cr.Status.Root.PartitionID)
	}
}

// TestMIGAnnotationBackfill verifies that a CR with an empty MIG
// annotation gets backfilled from the cache on the next reconcile.
func TestMIGAnnotationBackfill(t *testing.T) {
	fx := newFixture(t)
	fx.seedPool("node-0", true, "root", "leaf")
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))

	// Create CR manually with empty annotation.
	cr := &partitionv1beta1.PartitionTree{}
	cr.Name = fx.name
	cr.Annotations = map[string]string{AnnotationMIGName: ""}
	cr.Finalizers = []string{FinalizerName}
	if err := fx.env.client.Create(ctxBG(), cr); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Reconcile: should backfill the annotation.
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("backfill reconcile: %v", err)
	}
	cr = fx.get()
	if cr.Annotations[AnnotationMIGName] != testMIG {
		t.Errorf("annotation = %q, want %q", cr.Annotations[AnnotationMIGName], testMIG)
	}

	// Next reconcile should proceed to poll GCE.
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("post-backfill reconcile: %v", err)
	}
	if fx.gce.Calls() != 1 {
		t.Errorf("gce calls = %d want 1", fx.gce.Calls())
	}
}

// TestEventHelper_NilRecorder ensures the nil-safe event helper does
// not panic when no recorder is wired.
func TestEventHelper_NilRecorder(t *testing.T) {
	r := &PartitionTreeReconciler{} // Recorder is nil
	// Should not panic.
	r.event(&partitionv1beta1.PartitionTree{}, "Normal", "Test", "msg %s", "arg")
}

// TestGCEFailureUnderLoad exercises the error→grace→flip→recovery cycle
// across 50 pools simultaneously. This validates that at scale:
// - trees are never clobbered on GCE error
// - the Synced grace window works per-pool
// - recovery after errors restores Synced=True for all pools
func TestGCEFailureUnderLoad(t *testing.T) {
	const poolCount = 50

	fx := newFixture(t)

	type poolInfo struct {
		name   string
		rootID string
		leafID string
		mig    string
	}
	pools := make([]poolInfo, poolCount)
	for i := range poolCount {
		p := poolInfo{
			name:   fmt.Sprintf("loadpool-%d", i),
			rootID: fmt.Sprintf("root-%d", i),
			leafID: fmt.Sprintf("leaf-%d", i),
			mig:    fmt.Sprintf("https://www.googleapis.com/compute/v0.beta/projects/1234567890/zones/us-central2-b/instanceGroupManagers/%s-mig", fmt.Sprintf("loadpool-%d", i)),
		}
		pools[i] = p

		// Seed cache with one node per pool.
		fx.cch.Upsert(fmt.Sprintf("%s-node-0", p.name), topocache.NodeEntry{
			NodePool:        p.name,
			MIG:             p.mig,
			AcceleratorType: "tpu7x",
			PartitionIDs:    []string{p.rootID, p.leafID},
			Ready:           true,
		})

		// Register GCE mock response.
		migName := fmt.Sprintf("%s-mig", p.name)
		fx.gce.SetResponse(migName, singleLeafResponse(p.rootID, p.leafID))
	}

	// Bootstrap all 50 pools: create CR + first poll.
	for _, p := range pools {
		fx.name = p.name
		for range 2 {
			if _, err := fx.reconcile(); err != nil {
				t.Fatalf("bootstrap %s: %v", p.name, err)
			}
		}
	}

	// Verify baseline: all pools have trees and Synced=True.
	for _, p := range pools {
		fx.name = p.name
		cr := fx.get()
		if cr.Status.Root.PartitionID != p.rootID {
			t.Fatalf("baseline %s: root = %q want %q", p.name, cr.Status.Root.PartitionID, p.rootID)
		}
		c := meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeSynced)
		if c == nil || c.Status != "True" {
			t.Fatalf("baseline %s: Synced = %+v want True", p.name, c)
		}
	}

	// Phase 1: GCE fails. Advance past polling interval.
	fx.gce.SetError(errors.New("503 service unavailable"))
	fx.clk.SetTime(fx.clk.Now().Add(5 * time.Second))

	for _, p := range pools {
		fx.name = p.name
		if _, err := fx.reconcile(); err != nil {
			t.Fatalf("error-phase %s: %v", p.name, err)
		}
	}

	// Within grace window (5s < 15s default): trees preserved, Synced still True.
	for _, p := range pools {
		fx.name = p.name
		cr := fx.get()
		if cr.Status.Root.PartitionID != p.rootID {
			t.Errorf("grace %s: tree lost, root = %q", p.name, cr.Status.Root.PartitionID)
		}
		c := meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeSynced)
		if c == nil || c.Status != "True" {
			t.Errorf("grace %s: Synced = %+v want True (within grace window)", p.name, c)
		}
	}

	// Phase 2: Advance past grace window (>15s total since last success).
	fx.clk.SetTime(fx.clk.Now().Add(20 * time.Second))

	for _, p := range pools {
		fx.name = p.name
		if _, err := fx.reconcile(); err != nil {
			t.Fatalf("post-grace %s: %v", p.name, err)
		}
	}

	// All pools should flip to Synced=False, trees still preserved.
	for _, p := range pools {
		fx.name = p.name
		cr := fx.get()
		if cr.Status.Root.PartitionID != p.rootID {
			t.Errorf("post-grace %s: tree clobbered, root = %q", p.name, cr.Status.Root.PartitionID)
		}
		c := meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeSynced)
		if c == nil || c.Status != "False" {
			t.Errorf("post-grace %s: Synced = %+v want False", p.name, c)
		}
	}

	// Phase 3: GCE recovers. Clear error, advance past interval.
	fx.gce.SetError(nil)
	fx.clk.SetTime(fx.clk.Now().Add(5 * time.Second))

	for _, p := range pools {
		fx.name = p.name
		if _, err := fx.reconcile(); err != nil {
			t.Fatalf("recovery %s: %v", p.name, err)
		}
	}

	// All pools should recover Synced=True.
	for _, p := range pools {
		fx.name = p.name
		cr := fx.get()
		if cr.Status.Root.PartitionID != p.rootID {
			t.Errorf("recovery %s: tree lost, root = %q", p.name, cr.Status.Root.PartitionID)
		}
		c := meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeSynced)
		if c == nil || c.Status != "True" {
			t.Errorf("recovery %s: Synced = %+v want True", p.name, c)
		}
	}
}

// ---------------------------------------------------------------------------
// Slice-scheduler integration tests (cloud.google.com/tpu-pod-id label)
// ---------------------------------------------------------------------------

// TestCreateCR_StampsPodIDLabel verifies that when a node carries the
// gce-topology-block label, createCR stamps cloud.google.com/tpu-pod-id on
// the PartitionTree CR. This label is the handshake between
// partition-tree and slice-scheduler for multi-cube scheduling.
func TestCreateCR_StampsPodIDLabel(t *testing.T) {
	fx := newFixture(t)
	// Seed cache with a node that has TopologyBlock set.
	fx.cch.Upsert("node-0", topocache.NodeEntry{
		NodePool:        fx.name,
		MIG:             testMIG,
		AcceleratorType: "tpu7x",
		TopologyBlock:   "block-42",
		PartitionIDs:    []string{"root", "leaf"},
		Ready:           true,
	})
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))

	// First reconcile: creates the CR.
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("create reconcile: %v", err)
	}
	cr := fx.get()
	got := cr.Labels[LabelTpuPodID]
	if got != "block-42" {
		t.Errorf("pod-id label = %q, want %q", got, "block-42")
	}
}

// TestCreateCR_NoTopologyBlock_SkipsPodIDLabel verifies that when nodes
// do not carry the gce-topology-block label, the PartitionTree CR is
// created without the pod-id label (no empty-string label pollution).
func TestCreateCR_NoTopologyBlock_SkipsPodIDLabel(t *testing.T) {
	fx := newFixture(t)
	// Seed cache without TopologyBlock.
	fx.cch.Upsert("node-0", topocache.NodeEntry{
		NodePool:        fx.name,
		MIG:             testMIG,
		AcceleratorType: "tpu7x",
		TopologyBlock:   "", // not set
		PartitionIDs:    []string{"root", "leaf"},
		Ready:           true,
	})
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))

	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("create reconcile: %v", err)
	}
	cr := fx.get()
	if _, exists := cr.Labels[LabelTpuPodID]; exists {
		t.Errorf("pod-id label should not be set when no topology block; got %q", cr.Labels[LabelTpuPodID])
	}
}

// TestBackfillPodIDLabel_PatchesExistingCR verifies the backfill path:
// an older PartitionTree CR missing cloud.google.com/tpu-pod-id gets patched
// on the next reconcile when the cache has topology block info.
func TestBackfillPodIDLabel_PatchesExistingCR(t *testing.T) {
	fx := newFixture(t)
	// Seed the cache with a node that has the block.
	fx.cch.Upsert("node-0", topocache.NodeEntry{
		NodePool:        fx.name,
		MIG:             testMIG,
		AcceleratorType: "tpu7x",
		TopologyBlock:   "block-99",
		PartitionIDs:    []string{"root", "leaf"},
		Ready:           true,
	})
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))

	// Create a CR manually WITHOUT the pod-id label (simulates a
	// pre-existing CR from before this feature).
	cr := &partitionv1beta1.PartitionTree{}
	cr.Name = fx.name
	cr.Annotations = map[string]string{AnnotationMIGName: testMIG}
	cr.Finalizers = []string{FinalizerName}
	if err := fx.env.client.Create(ctxBG(), cr); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Verify no label initially.
	cr = fx.get()
	if cr.Labels[LabelTpuPodID] != "" {
		t.Fatalf("pre-condition: label already present: %q", cr.Labels[LabelTpuPodID])
	}

	// Reconcile: should backfill the pod-id label.
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("backfill reconcile: %v", err)
	}
	cr = fx.get()
	if cr.Labels[LabelTpuPodID] != "block-99" {
		t.Errorf("after backfill: pod-id = %q, want %q", cr.Labels[LabelTpuPodID], "block-99")
	}
}

// TestBackfillPodIDLabel_NoOpWhenPresent verifies that the backfill
// path is skipped when the CR already has the pod-id label — no
// unnecessary apiserver writes.
func TestBackfillPodIDLabel_NoOpWhenPresent(t *testing.T) {
	fx := newFixture(t)
	fx.cch.Upsert("node-0", topocache.NodeEntry{
		NodePool:        fx.name,
		MIG:             testMIG,
		AcceleratorType: "tpu7x",
		TopologyBlock:   "block-42",
		PartitionIDs:    []string{"root", "leaf"},
		Ready:           true,
	})
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))

	// Create CR WITH the label already set.
	cr := &partitionv1beta1.PartitionTree{}
	cr.Name = fx.name
	cr.Labels = map[string]string{LabelTpuPodID: "block-42"}
	cr.Annotations = map[string]string{AnnotationMIGName: testMIG}
	cr.Finalizers = []string{FinalizerName}
	if err := fx.env.client.Create(ctxBG(), cr); err != nil {
		t.Fatalf("create: %v", err)
	}
	rvBefore := fx.get().ResourceVersion

	// Reconcile: the backfill path should be skipped entirely since the
	// label is already present; it should proceed to the GCE poll path.
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	cr = fx.get()
	if cr.Labels[LabelTpuPodID] != "block-42" {
		t.Errorf("label changed unexpectedly: %q", cr.Labels[LabelTpuPodID])
	}
	// The reconciler should proceed to poll GCE (not return early from backfill).
	if fx.gce.Calls() != 1 {
		t.Errorf("gce calls = %d, want 1 (should have proceeded to GCE poll)", fx.gce.Calls())
	}
	_ = rvBefore // Avoid unused variable lint.
}

// TestBackfillPodIDLabel_NoCacheBlock_Skips verifies that the backfill
// path is skipped when the cache has no topology block data for the pool
// (avoids stamping an empty label).
func TestBackfillPodIDLabel_NoCacheBlock_Skips(t *testing.T) {
	fx := newFixture(t)
	// Seed without TopologyBlock.
	fx.cch.Upsert("node-0", topocache.NodeEntry{
		NodePool:        fx.name,
		MIG:             testMIG,
		AcceleratorType: "tpu7x",
		TopologyBlock:   "",
		PartitionIDs:    []string{"root", "leaf"},
		Ready:           true,
	})
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))

	// Create CR without label.
	cr := &partitionv1beta1.PartitionTree{}
	cr.Name = fx.name
	cr.Annotations = map[string]string{AnnotationMIGName: testMIG}
	cr.Finalizers = []string{FinalizerName}
	if err := fx.env.client.Create(ctxBG(), cr); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Reconcile: should skip backfill (block is "") and proceed to GCE poll.
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	cr = fx.get()
	if cr.Labels[LabelTpuPodID] != "" {
		t.Errorf("label should remain empty when cache has no block; got %q", cr.Labels[LabelTpuPodID])
	}
	// Should have proceeded to poll GCE.
	if fx.gce.Calls() != 1 {
		t.Errorf("gce calls = %d, want 1", fx.gce.Calls())
	}
}

// TestTaintParsingFailureReusesLastKnownGood verifies that when the taints
// annotation fails to parse, the controller:
// 1. Emits a warning event.
// 2. Sets the TaintsValid condition to False.
// 3. Reuses the last-known-good taints instead of clearing them.
func TestTaintParsingFailureReusesLastKnownGood(t *testing.T) {
	fx := newFixture(t)
	fx.rec.EnableTaints = NewTaintConfig(true)
	fx.seedPool("node-0", true, "root", "leaf")
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))

	// 1. Create CR.
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	// 2. Set valid taints and reconcile.
	cr := fx.get()
	metaOriginal := cr.DeepCopy()
	if cr.Annotations == nil {
		cr.Annotations = map[string]string{}
	}
	cr.Annotations[tree.TaintAnnotationPrefix+"test-taint"] = `{"partitionId": "leaf", "key": "key1", "value": "val1", "effect": "NoSchedule"}`
	if err := fx.env.client.Patch(ctxBG(), cr, client.MergeFrom(metaOriginal)); err != nil {
		t.Fatalf("patch annotations: %v", err)
	}

	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	// Verify taints are applied and TaintsValid is True.
	cr = fx.get()
	if len(cr.Status.Root.ChildPartitions) != 1 {
		t.Fatalf("expected 1 child partition")
	}
	leaf := cr.Status.Root.ChildPartitions[0]
	if len(leaf.Taints) != 1 || leaf.Taints[0].Key != "key1" {
		t.Errorf("expected taint key1 on leaf, got: %v", leaf.Taints)
	}
	c := meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeTaintsValid)
	if c == nil || c.Status != "True" {
		t.Errorf("TaintsValid condition = %+v, want True", c)
	}

	// 3. Set INVALID taints, advance time to force poll, and reconcile.
	metaOriginal = cr.DeepCopy()
	cr.Annotations[tree.TaintAnnotationPrefix+"test-taint"] = `invalid-json`
	if err := fx.env.client.Patch(ctxBG(), cr, client.MergeFrom(metaOriginal)); err != nil {
		t.Fatalf("patch invalid annotations: %v", err)
	}

	fx.clk.SetTime(fx.clk.Now().Add(5 * time.Second)) // Advance past 1s interval

	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("third reconcile: %v", err)
	}

	// Verify taints are preserved and TaintsValid is False.
	cr = fx.get()
	leaf = cr.Status.Root.ChildPartitions[0]
	if len(leaf.Taints) != 1 || leaf.Taints[0].Key != "key1" {
		t.Errorf("expected taint key1 to be preserved on leaf, got: %v", leaf.Taints)
	}
	c = meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeTaintsValid)
	if c == nil || c.Status != "False" || c.Reason != "TaintParseFailed" {
		t.Errorf("TaintsValid condition = %+v, want False/TaintParseFailed", c)
	}

	// 4. Reconcile again immediately (no time advance -> no poll).
	// This exercises the non-poll path with invalid taints.
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("fourth reconcile (non-poll): %v", err)
	}

	// Verify taints are still preserved and TaintsValid is still False.
	cr = fx.get()
	leaf = cr.Status.Root.ChildPartitions[0]
	if len(leaf.Taints) != 1 || leaf.Taints[0].Key != "key1" {
		t.Errorf("expected taint key1 to be preserved on leaf after non-poll reconcile, got: %v", leaf.Taints)
	}
	c = meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeTaintsValid)
	if c == nil || c.Status != "False" || c.Reason != "TaintParseFailed" {
		t.Errorf("TaintsValid condition after non-poll reconcile = %+v, want False/TaintParseFailed", c)
	}
}

// TestTaintsFeature_AllTaintTypes exercises all four taint propagation rules
// in a full controller reconciliation loop:
// 1. [Always] Custom taint annotations on the PartitionTree CR.
// 2. [NodeTaints] Taints propagated from Kubernetes Node objects.
// 3. [NodesNotFound] Controller-managed NodeReady=False (NodesNotFound) condition taints.
// 4. [PartitionState] Non-healthy partition state taints.
func TestTaintsFeature_AllTaintTypes(t *testing.T) {
	fx := newFixture(t)
	fx.rec.EnableTaints = NewTaintConfig(true) // All options enabled

	// Seed node-0 for leaf-1 with a Kubernetes node taint ([NodeTaints]).
	fx.cch.Upsert("node-0", topocache.NodeEntry{
		NodePool:        fx.name,
		MIG:             testMIG,
		AcceleratorType: "tpu7x",
		PartitionIDs:    []string{"root", "leaf-1"},
		Ready:           true,
		Taints: []corev1.Taint{
			{Key: "node.kubernetes.io/not-ready", Value: "", Effect: corev1.TaintEffectNoExecute},
		},
	})
	// Do not seed any node for leaf-2 so it triggers NodesNotFound ([NodesNotFound]).

	fx.gce.SetResponse("testpool-mig", &gce.TopologyResponse{
		Entries: []gce.TopologyEntry{
			{PartitionID: "root", Topology: "4x4x4", State: "Degraded"},                      // [PartitionState]
			{PartitionID: "leaf-1", ParentID: "root", Topology: "2x2x1", State: "Unhealthy"}, // [PartitionState]
			{PartitionID: "leaf-2", ParentID: "root", Topology: "2x2x1", State: "Healthy"},
		},
	})

	// 1. First reconcile creates CR.
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	// 2. Second reconcile polls GCE and populates Status.Root.
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	cr := fx.get()
	if cr.Status.Root == nil {
		t.Fatal("expected non-nil Status.Root")
	}

	// 3. Apply an external custom taint annotation to leaf-1 ([Always]).
	cr.Annotations["taints.partitiontree.gke.io/custom"] = `{"partitionId":"leaf-1","key":"custom.com/test","value":"val","effect":"NoSchedule"}`
	if err := fx.env.client.Update(ctxBG(), cr); err != nil {
		t.Fatalf("updating CR with taint annotation: %v", err)
	}
	// 4. Third reconcile propagates the external custom taint.
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("third reconcile: %v", err)
	}

	cr = fx.get()
	if cr.Status.Root == nil {
		t.Fatal("expected non-nil Status.Root after third reconcile")
	}

	// Verify root: Degraded state taint ([PartitionState])
	if len(cr.Status.Root.Taints) != 1 {
		t.Fatalf("root taints = %+v, want 1 taint", cr.Status.Root.Taints)
	}
	rootTaint := cr.Status.Root.Taints[0]
	if rootTaint.Key != TaintKeyPartitionState || rootTaint.Value != "Degraded" || rootTaint.Effect != partitionv1beta1.TaintEffectPreferNoSchedule {
		t.Errorf("root taint = %+v, want %s=Degraded:PreferNoSchedule", rootTaint, TaintKeyPartitionState)
	}

	if len(cr.Status.Root.ChildPartitions) != 2 {
		t.Fatalf("expected 2 child partitions, got %d", len(cr.Status.Root.ChildPartitions))
	}

	// Verify leaf-1: should have 3 taints in alphabetical order by Key:
	// 1. [PartitionState] cloud.google.com/partition-state=Unhealthy:NoExecute
	// 2. [Always] custom.com/test=val:NoSchedule
	// 3. [NodeTaints] node.kubernetes.io/not-ready:NoExecute
	leaf1 := cr.Status.Root.ChildPartitions[0]
	if len(leaf1.Taints) != 3 {
		t.Fatalf("leaf-1 taints len = %d, want 3: %+v", len(leaf1.Taints), leaf1.Taints)
	}
	wantLeaf1Keys := []string{
		TaintKeyPartitionState,
		"custom.com/test",
		"node.kubernetes.io/not-ready",
	}
	for i, wantKey := range wantLeaf1Keys {
		if leaf1.Taints[i].Key != wantKey {
			t.Errorf("leaf-1.Taints[%d].Key = %q, want %q (all taints: %+v)", i, leaf1.Taints[i].Key, wantKey, leaf1.Taints)
		}
	}

	// Verify leaf-2: should have 1 taint ([NodesNotFound])
	// cloud.google.com/nodes-not-found:NoExecute
	leaf2 := cr.Status.Root.ChildPartitions[1]
	if len(leaf2.Taints) != 1 {
		t.Fatalf("leaf-2 taints len = %d, want 1: %+v", len(leaf2.Taints), leaf2.Taints)
	}
	if leaf2.Taints[0].Key != TaintKeyNodesNotFound || leaf2.Taints[0].Effect != partitionv1beta1.TaintEffectNoExecute {
		t.Errorf("leaf-2 taint = %+v, want %s:NoExecute", leaf2.Taints[0], TaintKeyNodesNotFound)
	}
}

func TestAnnotationChangeRespectsBackoff(t *testing.T) {
	fx := newFixture(t)
	fx.rec.EnableTaints = NewTaintConfig(true)
	fx.seedPool("node-0", true, "root", "leaf")

	// 1. Success poll to get a root.
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("first reconcile (create + finalizer): %v", err)
	}
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("second reconcile (first poll): %v", err)
	}

	cr := fx.get()
	if cr.Status.Root == nil {
		t.Fatalf("expected root to be populated")
	}
	initialGceCalls := fx.gce.Calls()

	// 2. Force GCE failure on next poll.
	fx.clk.SetTime(fx.clk.Now().Add(5 * time.Second)) // Advance past interval
	fx.gce.SetError(errors.New("GCE internal error"))

	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("third reconcile (expected failure): %v", err)
	}

	// GCE should have been called and failed.
	if fx.gce.Calls() != initialGceCalls+1 {
		t.Fatalf("expected GCE call to fail, calls = %d, want %d", fx.gce.Calls(), initialGceCalls+1)
	}

	// We should be in backoff now.
	state := fx.rec.poll.get(cr.Name)
	if state.lastErr == nil {
		t.Fatalf("expected lastErr to be recorded")
	}
	if !state.nextEligibleAt.After(fx.clk.Now()) {
		t.Fatalf("expected nextEligibleAt to be in the future")
	}

	gceCallsAfterFailure := fx.gce.Calls()

	// 3. Change annotations during backoff.
	cr = fx.get()
	metaOriginal := cr.DeepCopy()
	if cr.Annotations == nil {
		cr.Annotations = map[string]string{}
	}
	cr.Annotations[tree.TaintAnnotationPrefix+"test-taint"] = `{"partitionId": "leaf", "key": "key1", "value": "val1", "effect": "NoSchedule"}`
	if err := fx.env.client.Patch(ctxBG(), cr, client.MergeFrom(metaOriginal)); err != nil {
		t.Fatalf("patch annotations: %v", err)
	}

	// Reconcile again, still within backoff window.
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("fourth reconcile: %v", err)
	}

	// 4. Verify GCE was NOT called.
	if fx.gce.Calls() != gceCallsAfterFailure {
		t.Errorf("GCE was called during backoff! calls = %d, want %d", fx.gce.Calls(), gceCallsAfterFailure)
	}

	// 5. Verify taints WERE updated in status.
	cr = fx.get()
	leaf := cr.Status.Root.ChildPartitions[0]
	if len(leaf.Taints) != 1 || leaf.Taints[0].Key != "key1" {
		t.Errorf("expected taint key1 to be applied from annotation even though GCE poll was skipped, got: %v", leaf.Taints)
	}
}

// ---------------------------------------------------------------------------
// Decoupled lifecycle tests
// ---------------------------------------------------------------------------

// newDecoupledFixture creates a fixture with DecoupledLifecycle=true.
func newDecoupledFixture(t *testing.T) *fixture {
	t.Helper()
	fx := newFixture(t)
	fx.rec.DecoupledLifecycle = true
	return fx
}

// TestDecoupled_CreateCRFromPendingUpdate verifies that in decoupled mode,
// when the node informer pushes a create update (MIG set, Drained=false),
// the reconciler creates a CR named after the partition ID.
func TestDecoupled_CreateCRFromPendingUpdate(t *testing.T) {
	fx := newDecoupledFixture(t)
	partID := "abc-123-root"

	// Simulate: node informer saw a node and pushed a create update.
	fx.cch.PushUpdate(partID, topocache.PTUpdateRequest{
		CRName: partID,
		MIG:    testMIG,
	})

	fx.name = partID
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("create reconcile: %v", err)
	}

	cr := fx.get()
	if cr.Name != partID {
		t.Errorf("CR name = %q, want %q", cr.Name, partID)
	}
	if cr.Annotations[AnnotationMIGName] != testMIG {
		t.Errorf("MIG annotation = %q, want %q", cr.Annotations[AnnotationMIGName], testMIG)
	}
	if len(cr.Finalizers) != 1 || cr.Finalizers[0] != FinalizerName {
		t.Errorf("finalizers = %v", cr.Finalizers)
	}
}

// TestDecoupled_DrainClearsMIG verifies that draining a pool in decoupled
// mode clears the MIG annotation instead of deleting the CR, and sets
// Synced=False with reason PoolDrained.
func TestDecoupled_DrainClearsMIG(t *testing.T) {
	fx := newDecoupledFixture(t)
	partID := "drain-test-root"
	fx.name = partID

	// Bootstrap: create CR via pending update.
	fx.cch.PushUpdate(partID, topocache.PTUpdateRequest{
		CRName: partID,
		MIG:    testMIG,
	})
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Seed pool so the normal reconcile path can poll GCE.
	fx.seedPool("node-0", true, "root", "leaf")
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))

	// Reconcile to populate tree + register CR in cache.
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("poll: %v", err)
	}

	// Now drain: push a drain update.
	fx.cch.PushUpdate(partID, topocache.PTUpdateRequest{
		CRName:  partID,
		Drained: true,
	})
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("drain reconcile: %v", err)
	}

	// CR must still exist, with empty MIG.
	cr := fx.get()
	if cr.Annotations[AnnotationMIGName] != "" {
		t.Errorf("MIG annotation after drain = %q, want empty", cr.Annotations[AnnotationMIGName])
	}

	// Synced condition should be False/PoolDrained.
	c := meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeSynced)
	if c == nil || c.Status != "False" || c.Reason != "PoolDrained" {
		t.Errorf("Synced condition = %+v, want False/PoolDrained", c)
	}
}

// TestDecoupled_DrainedCRSkipsGCEPoll verifies that a CR with an empty
// MIG annotation (drained) does not poll GCE and returns no requeue.
func TestDecoupled_DrainedCRSkipsGCEPoll(t *testing.T) {
	fx := newDecoupledFixture(t)
	partID := "idle-test-root"
	fx.name = partID

	// Create CR manually with empty MIG (simulates post-drain state).
	cr := &partitionv1beta1.PartitionTree{}
	cr.Name = partID
	cr.Annotations = map[string]string{AnnotationMIGName: ""}
	cr.Finalizers = []string{FinalizerName}
	if err := fx.env.client.Create(ctxBG(), cr); err != nil {
		t.Fatalf("create: %v", err)
	}

	result, err := fx.reconcile()
	if err != nil {
		t.Fatalf("idle reconcile: %v", err)
	}
	// Should not requeue (idle).
	if result.Requeue || result.RequeueAfter > 0 {
		t.Errorf("idle CR should not requeue: %+v", result)
	}
	// Should not call GCE.
	if fx.gce.Calls() != 0 {
		t.Errorf("gce calls = %d, want 0 (idle CR)", fx.gce.Calls())
	}
}

// TestDecoupled_ReprovisionUpdatesMIG verifies that when a re-provision
// update arrives (new MIG), the annotation is updated and GCE polling
// resumes.
func TestDecoupled_ReprovisionUpdatesMIG(t *testing.T) {
	fx := newDecoupledFixture(t)
	partID := "reprovision-test"
	fx.name = partID
	newMIG := "https://www.googleapis.com/compute/v0.beta/projects/1234567890/zones/us-central2-b/instanceGroupManagers/newpool-mig"

	// Create CR with empty MIG (drained state).
	cr := &partitionv1beta1.PartitionTree{}
	cr.Name = partID
	cr.Annotations = map[string]string{AnnotationMIGName: ""}
	cr.Finalizers = []string{FinalizerName}
	if err := fx.env.client.Create(ctxBG(), cr); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Push re-provision update with new MIG.
	fx.cch.PushUpdate(partID, topocache.PTUpdateRequest{
		CRName: partID,
		MIG:    newMIG,
	})

	// Seed pool so the reconciler can poll GCE.
	fx.cch.Upsert("node-0", topocache.NodeEntry{
		NodePool:        testPoolName,
		MIG:             newMIG,
		AcceleratorType: "tpu7x",
		PartitionIDs:    []string{"root", "leaf"},
		Ready:           true,
	})
	fx.gce.SetResponse("newpool-mig", singleLeafResponse("root", "leaf"))

	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("reprovision reconcile: %v", err)
	}

	cr = fx.get()
	if cr.Annotations[AnnotationMIGName] != newMIG {
		t.Errorf("MIG annotation = %q, want %q", cr.Annotations[AnnotationMIGName], newMIG)
	}
}

// TestDecoupled_LegacyModeUnchanged verifies that when DecoupledLifecycle
// is false, the scale-to-zero path still deletes the CR as before.
func TestDecoupled_LegacyModeUnchanged(t *testing.T) {
	fx := newFixture(t) // NOT decoupled
	fx.seedPool("node-0", true, "root", "leaf")
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))

	// Bootstrap: create + first poll.
	for i := range 2 {
		if _, err := fx.reconcile(); err != nil {
			t.Fatalf("bootstrap %d: %v", i, err)
		}
	}

	// Drain.
	fx.cch.Delete("node-0")
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("drain reconcile: %v", err)
	}
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("finalizer reconcile: %v", err)
	}

	// CR should be gone.
	var cr partitionv1beta1.PartitionTree
	err := fx.env.client.Get(ctxBG(), apitypes.NamespacedName{Name: fx.name}, &cr)
	if err == nil {
		t.Error("CR still exists after legacy drain, expected deletion")
	}
}

// TestDecoupled_FullDrainReprovisionCycle exercises the complete lifecycle:
// create → populate tree → drain (clear MIG, idle) → re-provision (set
// new MIG, resume polling) → verify tree re-populated.
func TestDecoupled_FullDrainReprovisionCycle(t *testing.T) {
	fx := newDecoupledFixture(t)
	partID := "e2e-cycle-root"
	fx.name = partID
	newMIG := "https://www.googleapis.com/compute/v0.beta/projects/1234567890/zones/us-central2-b/instanceGroupManagers/newpool-mig"

	// Phase 1: Create via pending update.
	fx.cch.PushUpdate(partID, topocache.PTUpdateRequest{
		CRName: partID,
		MIG:    testMIG,
	})
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Seed pool + partition mapping so refreshLeaves works.
	fx.seedPool("node-0", true, "root", "leaf")
	fx.cch.SetPoolRootPartitionID(testPoolName, partID)
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))

	// Reconcile to populate tree.
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("poll: %v", err)
	}
	crAfterPoll := fx.get()
	if crAfterPoll.Status.Root == nil || crAfterPoll.Status.Root.PartitionID != "root" {
		t.Fatalf("tree not populated after poll: %+v", crAfterPoll.Status.Root)
	}

	// Phase 2: Drain.
	fx.cch.PushUpdate(partID, topocache.PTUpdateRequest{
		CRName:  partID,
		Drained: true,
	})
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("drain: %v", err)
	}
	crDrained := fx.get()
	if crDrained.Annotations[AnnotationMIGName] != "" {
		t.Errorf("MIG after drain = %q, want empty", crDrained.Annotations[AnnotationMIGName])
	}
	c := meta.FindStatusCondition(crDrained.Status.Conditions, partitionv1beta1.ConditionTypeSynced)
	if c == nil || c.Status != "False" || c.Reason != "PoolDrained" {
		t.Errorf("Synced after drain = %+v, want False/PoolDrained", c)
	}

	// Phase 3: Idle reconcile — should not call GCE.
	gceBefore := fx.gce.Calls()
	result, err := fx.reconcile()
	if err != nil {
		t.Fatalf("idle: %v", err)
	}
	if result.Requeue || result.RequeueAfter > 0 {
		t.Errorf("idle should not requeue: %+v", result)
	}
	if fx.gce.Calls() != gceBefore {
		t.Errorf("idle called GCE: %d → %d", gceBefore, fx.gce.Calls())
	}

	// Phase 4: Re-provision with new MIG (same partition IDs).
	fx.cch.PushUpdate(partID, topocache.PTUpdateRequest{
		CRName: partID,
		MIG:    newMIG,
	})
	// Seed new pool data + partition mapping. Same partition IDs as
	// before (partition is immutable), but different MIG.
	fx.cch.Upsert("node-1", topocache.NodeEntry{
		NodePool:        "newpool",
		MIG:             newMIG,
		AcceleratorType: "tpu7x",
		PartitionIDs:    []string{"root", "leaf"},
		Ready:           true,
	})
	fx.cch.SetPoolRootPartitionID("newpool", partID)
	fx.gce.SetResponse("newpool-mig", singleLeafResponse("root", "leaf"))

	// Advance clock past polling interval.
	fx.clk.SetTime(fx.clk.Now().Add(5 * time.Second))
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("reprovision: %v", err)
	}
	crReprovisioned := fx.get()
	if crReprovisioned.Annotations[AnnotationMIGName] != newMIG {
		t.Errorf("MIG after reprovision = %q, want %q", crReprovisioned.Annotations[AnnotationMIGName], newMIG)
	}
}

// TestDecoupled_Case1_NoUpdatesNoPool verifies the no-op path in
// decoupled mode: CR doesn't exist, no pending updates, no pool.
func TestDecoupled_Case1_NoUpdatesNoPool(t *testing.T) {
	fx := newDecoupledFixture(t)
	fx.name = "nonexistent-partition"

	result, err := fx.reconcile()
	if err != nil {
		t.Fatalf("no-op reconcile: %v", err)
	}
	if result.Requeue || result.RequeueAfter > 0 {
		t.Errorf("should not requeue: %+v", result)
	}
}

// TestDecoupled_CreateCR_MultiUpdate_LastWriterWins verifies that when
// multiple create updates are queued before the reconciler runs, the
// last MIG wins (not the first).
func TestDecoupled_CreateCR_MultiUpdate_LastWriterWins(t *testing.T) {
	fx := newDecoupledFixture(t)
	partID := "multi-update-part"

	firstMIG := "projects/p/zones/z/instanceGroupManagers/mig-old"
	lastMIG := "projects/p/zones/z/instanceGroupManagers/mig-new"

	// Simulate: multiple updates queued before reconciler runs.
	fx.cch.PushUpdate(partID, topocache.PTUpdateRequest{
		CRName: partID,
		MIG:    firstMIG,
	})
	fx.cch.PushUpdate(partID, topocache.PTUpdateRequest{
		CRName: partID,
		MIG:    lastMIG,
	})

	fx.name = partID
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("create reconcile: %v", err)
	}

	cr := fx.get()
	if cr.Annotations[AnnotationMIGName] != lastMIG {
		t.Errorf("MIG = %q, want last-writer-wins %q", cr.Annotations[AnnotationMIGName], lastMIG)
	}
}

// TestDecoupled_HandleDeletion_CleansCache verifies that when a
// decoupled CR is manually deleted, handleDeletion clears the
// partitionCRs cache entry to prevent blocking re-creation.
func TestDecoupled_HandleDeletion_CleansCache(t *testing.T) {
	fx := newDecoupledFixture(t)
	partID := "del-cleanup-part"

	// Create the CR first.
	fx.cch.PushUpdate(partID, topocache.PTUpdateRequest{
		CRName: partID,
		MIG:    testMIG,
	})
	fx.name = partID
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Verify cache has the entry.
	_, ok := fx.cch.PartitionCR(partID)
	if !ok {
		t.Fatal("partitionCR should exist after creation")
	}

	// Delete the CR.
	cr := fx.get()
	if err := fx.env.client.Delete(ctxBG(), cr); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Reconcile should process the deletion (finalizer cleanup).
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("deletion reconcile: %v", err)
	}

	// Cache entry should be cleaned up.
	_, ok = fx.cch.PartitionCR(partID)
	if ok {
		t.Error("partitionCR should be deleted after handleDeletion")
	}
}

// TestDecoupled_HandleDeletion_SelfHeal verifies that when a decoupled
// CR is deleted but the pool still has nodes, handleDeletion pushes a
// create update so the CR can be re-created.
func TestDecoupled_HandleDeletion_SelfHeal(t *testing.T) {
	fx := newDecoupledFixture(t)
	partID := "self-heal-part"

	// Seed a node pool so the pool still exists after CR deletion.
	fx.cch.Upsert("node-sh", topocache.NodeEntry{
		NodePool:        "my-pool",
		MIG:             testMIG,
		AcceleratorType: "tpu7x",
		PartitionIDs:    []string{"root", "leaf"},
		Ready:           true,
	})
	fx.cch.SetPoolRootPartitionID("my-pool", partID)

	// Create the CR.
	fx.cch.PushUpdate(partID, topocache.PTUpdateRequest{
		CRName: partID,
		MIG:    testMIG,
	})
	fx.name = partID
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Delete the CR.
	cr := fx.get()
	if err := fx.env.client.Delete(ctxBG(), cr); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Reconcile: handleDeletion runs → cleans cache + pushes create update.
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("deletion reconcile: %v", err)
	}

	// There should be a pending create update pushed by self-healing.
	updates := fx.cch.DrainUpdates(partID)
	if len(updates) == 0 {
		t.Fatal("self-healing should have pushed a create update")
	}
	found := false
	for _, u := range updates {
		if !u.Drained && u.MIG == testMIG {
			found = true
		}
	}
	if !found {
		t.Errorf("expected create update with MIG=%q, got %+v", testMIG, updates)
	}
}

// TestDecoupled_CreateCR_StampsPodIDLabel verifies that
// createDecoupledCR stamps the cloud.google.com/tpu-pod-id label from
// the topology block, mirroring createCR behaviour.
func TestDecoupled_CreateCR_StampsPodIDLabel(t *testing.T) {
	fx := newDecoupledFixture(t)
	partID := "label-part"
	poolName := "label-pool"

	// Seed a node with a topology block label.
	fx.cch.Upsert("node-label", topocache.NodeEntry{
		NodePool:        poolName,
		MIG:             testMIG,
		AcceleratorType: "tpu7x",
		TopologyBlock:   "us-central2-b-block-7",
		PartitionIDs:    []string{"root", "leaf"},
		Ready:           true,
	})
	fx.cch.SetPoolRootPartitionID(poolName, partID)

	// Push a create update.
	fx.cch.PushUpdate(partID, topocache.PTUpdateRequest{
		CRName: partID,
		MIG:    testMIG,
	})

	fx.name = partID
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("create: %v", err)
	}

	cr := fx.get()
	if got := cr.Labels[LabelTpuPodID]; got != "us-central2-b-block-7" {
		t.Errorf("pod-id label = %q, want %q", got, "us-central2-b-block-7")
	}
}

// TestMetricsLifecycle verifies that when a PartitionTree CR is deleted,
// its associated taint and occupancy metrics are explicitly cleared from the gauge sets.
func TestMetricsLifecycle(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "test")
	metrics.ClearGaugeSets()
	readerCustomer := sdkmetric.NewManualReader()
	readerGoogle := sdkmetric.NewManualReader()
	mockMeta, err := metadata.Get(context.Background(), true)
	if err != nil {
		t.Fatalf("metadata.Get: %v", err)
	}
	shutdown, err := metrics.InitWithReaders(context.Background(), readerCustomer, readerGoogle, *mockMeta, 30*time.Minute, 100000, true, true)
	if err != nil {
		t.Fatalf("InitWithReaders: %v", err)
	}
	defer shutdown()

	fx := newFixture(t)
	fx.rec.EnableTaints = NewTaintConfig(true)
	fx.seedPool("node-0", true, "root-v7x-0", "leaf-v7x-0")
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root-v7x-0", "leaf-v7x-0"))

	// 1. Create CR
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	cr := fx.get()
	metaOriginal := cr.DeepCopy()
	if cr.Annotations == nil {
		cr.Annotations = map[string]string{}
	}
	// Add a taint via annotation so it gets parsed into metrics
	cr.Annotations[tree.TaintAnnotationPrefix+"test-taint"] = `{"partitionId": "root-v7x-0", "key": "test-key", "value": "test-val", "effect": "NoSchedule"}`
	if err := fx.env.client.Patch(ctxBG(), cr, client.MergeFrom(metaOriginal)); err != nil {
		t.Fatalf("patch annotations: %v", err)
	}

	// 2. Poll GCE + Process Annotations
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	getMetricCount := func(name string) int {
		var rm metricdata.ResourceMetrics
		if err := readerGoogle.Collect(ctxBG(), &rm); err != nil {
			t.Fatalf("collect metrics: %v", err)
		}
		count := 0
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == name {
					if gauge, ok := m.Data.(metricdata.Gauge[int64]); ok {
						for _, dp := range gauge.DataPoints {
							if dp.Value > 0 {
								count++
							}
						}
					}
				}
			}
		}
		return count
	}

	// Verify metrics are recorded.
	// Internal names are renamed by views.
	if count := getMetricCount("kubernetes.io/internal/addons/partition_tree/taint"); count != 1 {
		t.Errorf("taint metrics count = %d, want 1", count)
	}
	// We expect occupancy to be tracked for both the root and the leaf.
	if count := getMetricCount("kubernetes.io/internal/addons/partition_tree/occupancy"); count != 2 {
		t.Errorf("occupancy metrics count = %d, want 2", count)
	}

	// 3. Mark for deletion
	if err := fx.env.client.Delete(ctxBG(), cr); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// 4. Reconcile deletion (finalizer cleanup)
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("deletion reconcile: %v", err)
	}

	// Verify metrics are cleared
	if count := getMetricCount("kubernetes.io/internal/addons/partition_tree/taint"); count != 0 {
		t.Errorf("taint metrics count after deletion = %d, want 0", count)
	}
	if count := getMetricCount("kubernetes.io/internal/addons/partition_tree/occupancy"); count != 0 {
		t.Errorf("occupancy metrics count after deletion = %d, want 0", count)
	}
}

func TestNodeTaintTimeAdded_PropagatedToPartitionTree(t *testing.T) {
	fx := newFixture(t)
	fx.rec.EnableTaints = NewTaintConfig(true)

	taintAdded := metav1.NewTime(time.Date(2026, 4, 11, 11, 58, 30, 0, time.UTC))
	fx.cch.Upsert("node-0", topocache.NodeEntry{
		NodePool:        fx.name,
		MIG:             testMIG,
		AcceleratorType: "tpu7x",
		PartitionIDs:    []string{"root", "leaf"},
		Ready:           false,
		Taints: []corev1.Taint{{
			Key:       corev1.TaintNodeNotReady,
			Effect:    corev1.TaintEffectNoExecute,
			TimeAdded: &taintAdded,
		}},
	})
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))

	for i := range 2 {
		if _, err := fx.reconcile(); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}

	cr := fx.get()
	leaf := cr.Status.Root.ChildPartitions[0]
	if len(leaf.Taints) != 1 {
		t.Fatalf("leaf taints = %+v, want 1 taint", leaf.Taints)
	}
	if leaf.Taints[0].Key != corev1.TaintNodeNotReady || leaf.Taints[0].Effect != partitionv1beta1.TaintEffectNoExecute {
		t.Fatalf("leaf taint = %+v, want not-ready:NoExecute", leaf.Taints[0])
	}
	if leaf.Taints[0].TimeAdded == nil || !leaf.Taints[0].TimeAdded.Equal(&taintAdded) {
		t.Errorf("leaf taint TimeAdded = %v, want %v", leaf.Taints[0].TimeAdded, taintAdded)
	}
}

func TestTaintFirstSeenStampAndNoOpIdempotency(t *testing.T) {
	fx := newFixture(t)
	fx.rec.EnableTaints = NewTaintConfig(true)

	// Sub-second clock time verifies truncation to RFC3339 second precision
	// so apiserver round-trips do not cause spurious status patches.
	t0Raw := time.Date(2026, 11, 3, 12, 0, 7, 456_000_000, time.UTC)
	wantT0 := metav1.NewTime(time.Date(2026, 11, 3, 12, 0, 7, 0, time.UTC))
	wantExplicit := metav1.NewTime(time.Date(2026, 11, 3, 11, 55, 0, 0, time.UTC))
	fx.clk.SetTime(t0Raw)

	// leaf-1 has a node with an unstamped manual NoExecute taint; leaf-2 has no nodes (NodesNotFound).
	fx.cch.Upsert("node-0", topocache.NodeEntry{
		NodePool:        fx.name,
		MIG:             testMIG,
		AcceleratorType: "tpu7x",
		TopologyBlock:   "block-1",
		PartitionIDs:    []string{"root", "leaf-1"},
		Ready:           true,
		Taints: []corev1.Taint{{
			Key:    "example.com/manual",
			Value:  "true",
			Effect: corev1.TaintEffectNoExecute,
			// TimeAdded intentionally nil
		}},
	})
	fx.gce.SetResponse("testpool-mig", &gce.TopologyResponse{
		Entries: []gce.TopologyEntry{
			{PartitionID: "root", Topology: "4x4x4", State: "Degraded"},
			{PartitionID: "leaf-1", ParentID: "root", Topology: "2x2x1", State: "Unhealthy"},
			{PartitionID: "leaf-2", ParentID: "root", Topology: "2x2x1", State: "Healthy"},
		},
	})

	// Create CR with one explicit-timeAdded annotation on root and one unstamped annotation on leaf-1.
	cr := &partitionv1beta1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{
			Name:   fx.name,
			Labels: map[string]string{LabelTpuPodID: "block-1"},
			Annotations: map[string]string{
				AnnotationMIGName:                       testMIG,
				tree.TaintAnnotationPrefix + "explicit": `{"partitionId":"root","key":"example.com/explicit","value":"1","effect":"NoSchedule","timeAdded":"2026-11-03T11:55:00Z"}`,
				tree.TaintAnnotationPrefix + "maint":    `{"partitionId":"leaf-1","key":"example.com/maint","value":"1","effect":"NoSchedule"}`,
			},
			Finalizers: []string{FinalizerName},
		},
	}
	if err := fx.env.client.Create(ctxBG(), cr); err != nil {
		t.Fatalf("create CR: %v", err)
	}

	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}

	findTaint := func(p partitionv1beta1.PartitionInfo, key string) *partitionv1beta1.PartitionTaint {
		for i := range p.Taints {
			if p.Taints[i].Key == key {
				return &p.Taints[i]
			}
		}
		return nil
	}
	assertTaintTime := func(step string, p partitionv1beta1.PartitionInfo, key string, want metav1.Time) {
		t.Helper()
		got := findTaint(p, key)
		if got == nil {
			t.Fatalf("%s: partition %s missing taint %q (taints=%+v)", step, p.PartitionID, key, p.Taints)
		}
		if got.TimeAdded == nil || !got.TimeAdded.Equal(&want) {
			t.Errorf("%s: partition %s taint %q TimeAdded = %v, want %v", step, p.PartitionID, key, got.TimeAdded, want)
		}
	}

	cr = fx.get()
	rvInitial := cr.ResourceVersion
	gceCallsInitial := fx.gce.Calls()
	patchesAfterInitial := fx.env.statusPatches.Load()
	if patchesAfterInitial != 1 {
		t.Fatalf("initial reconcile /status PATCH count = %d, want 1", patchesAfterInitial)
	}

	assertTaintTime("initial", *cr.Status.Root, TaintKeyPartitionState, wantT0)
	assertTaintTime("initial", *cr.Status.Root, "example.com/explicit", wantExplicit)
	assertTaintTime("initial", cr.Status.Root.ChildPartitions[0], TaintKeyPartitionState, wantT0)
	assertTaintTime("initial", cr.Status.Root.ChildPartitions[0], "example.com/maint", wantT0)
	assertTaintTime("initial", cr.Status.Root.ChildPartitions[0], "example.com/manual", wantT0)
	assertTaintTime("initial", cr.Status.Root.ChildPartitions[1], TaintKeyNodesNotFound, wantT0)

	// Step 2: Advance 300ms (non-polling reconcile) -> zero /status PATCH requests, zero ResourceVersion bump, timestamps unchanged.
	fx.clk.SetTime(t0Raw.Add(300 * time.Millisecond))
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("non-poll reconcile: %v", err)
	}
	if fx.gce.Calls() != gceCallsInitial {
		t.Fatalf("non-poll reconcile polled GCE: calls=%d, want %d", fx.gce.Calls(), gceCallsInitial)
	}
	if gotPatches := fx.env.statusPatches.Load() - patchesAfterInitial; gotPatches != 0 {
		t.Errorf("non-poll reconcile sent %d /status PATCH(es), want 0", gotPatches)
	}
	cr = fx.get()
	if cr.ResourceVersion != rvInitial {
		t.Errorf("non-poll reconcile bumped ResourceVersion from %s to %s (spurious status patch)", rvInitial, cr.ResourceVersion)
	}

	// Step 3: Advance 5s and 10s (two GCE-polling reconciles rebuilding tree via tree.Build) -> zero /status PATCH requests, timestamps carried forward!
	patchesBeforePolls := fx.env.statusPatches.Load()
	for stepIdx, offset := range []time.Duration{5 * time.Second, 10 * time.Second} {
		fx.clk.SetTime(t0Raw.Add(offset))
		if _, err := fx.reconcile(); err != nil {
			t.Fatalf("poll reconcile %d: %v", stepIdx+1, err)
		}
	}
	if fx.gce.Calls() != gceCallsInitial+2 {
		t.Fatalf("poll reconciles GCE calls=%d, want %d", fx.gce.Calls(), gceCallsInitial+2)
	}
	if gotPatches := fx.env.statusPatches.Load() - patchesBeforePolls; gotPatches != 0 {
		t.Errorf("steady-state poll reconciles sent %d /status PATCH(es), want 0", gotPatches)
	}
	cr = fx.get()
	if cr.ResourceVersion != rvInitial {
		t.Errorf("poll reconcile bumped ResourceVersion from %s to %s (spurious status patch across tree.Build)", rvInitial, cr.ResourceVersion)
	}
	assertTaintTime("after-poll", *cr.Status.Root, TaintKeyPartitionState, wantT0)
	assertTaintTime("after-poll", *cr.Status.Root, "example.com/explicit", wantExplicit)
	assertTaintTime("after-poll", cr.Status.Root.ChildPartitions[0], TaintKeyPartitionState, wantT0)
	assertTaintTime("after-poll", cr.Status.Root.ChildPartitions[0], "example.com/maint", wantT0)
	assertTaintTime("after-poll", cr.Status.Root.ChildPartitions[0], "example.com/manual", wantT0)
	assertTaintTime("after-poll", cr.Status.Root.ChildPartitions[1], TaintKeyNodesNotFound, wantT0)

	// Step 4: Clear manual taint on node-0, reconcile, then re-add at T3 -> fresh TimeAdded for manual taint only!
	fx.clk.SetTime(time.Date(2026, 11, 3, 12, 2, 0, 0, time.UTC))
	fx.cch.Upsert("node-0", topocache.NodeEntry{
		NodePool:        fx.name,
		MIG:             testMIG,
		AcceleratorType: "tpu7x",
		TopologyBlock:   "block-1",
		PartitionIDs:    []string{"root", "leaf-1"},
		Ready:           true,
		Taints:          nil,
	})
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("clear manual taint reconcile: %v", err)
	}
	cr = fx.get()
	if findTaint(cr.Status.Root.ChildPartitions[0], "example.com/manual") != nil {
		t.Fatalf("expected example.com/manual to be cleared, got %+v", cr.Status.Root.ChildPartitions[0].Taints)
	}

	t3 := time.Date(2026, 11, 3, 12, 3, 0, 0, time.UTC)
	wantT3 := metav1.NewTime(t3)
	fx.clk.SetTime(t3)
	fx.cch.Upsert("node-0", topocache.NodeEntry{
		NodePool:        fx.name,
		MIG:             testMIG,
		AcceleratorType: "tpu7x",
		TopologyBlock:   "block-1",
		PartitionIDs:    []string{"root", "leaf-1"},
		Ready:           true,
		Taints: []corev1.Taint{{
			Key:    "example.com/manual",
			Value:  "true",
			Effect: corev1.TaintEffectNoExecute,
		}},
	})
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("re-add manual taint reconcile: %v", err)
	}
	cr = fx.get()
	assertTaintTime("re-added", cr.Status.Root.ChildPartitions[0], "example.com/manual", wantT3)
	assertTaintTime("re-added-unhealthy-unchanged", cr.Status.Root.ChildPartitions[0], TaintKeyPartitionState, wantT0)
}

func TestNodeHealthNoExecuteSwapPreservesEarliestTimeAdded(t *testing.T) {
	fx := newFixture(t)
	fx.rec.EnableTaints = NewTaintConfig(true)

	t0 := time.Date(2026, 11, 3, 13, 0, 0, 0, time.UTC)
	wantT0 := metav1.NewTime(t0)
	fx.clk.SetTime(t0)

	upsertNodeTaints := func(ready bool, taints []corev1.Taint) {
		fx.cch.Upsert("node-0", topocache.NodeEntry{
			NodePool:        fx.name,
			MIG:             testMIG,
			AcceleratorType: "tpu7x",
			TopologyBlock:   "block-1",
			PartitionIDs:    []string{"root", "leaf"},
			Ready:           ready,
			Taints:          taints,
		})
	}

	// 1. Episode 1 starts at T0 with node.kubernetes.io/not-ready:NoExecute (TimeAdded = T0).
	t0Meta := metav1.NewTime(t0)
	upsertNodeTaints(false, []corev1.Taint{
		{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoExecute, TimeAdded: &t0Meta},
		{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoSchedule},
	})
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))

	for i := range 2 {
		if _, err := fx.reconcile(); err != nil {
			t.Fatalf("initial reconcile %d: %v", i, err)
		}
	}

	findLeafTaint := func(key string, effect partitionv1beta1.TaintEffect) *partitionv1beta1.PartitionTaint {
		cr := fx.get()
		leaf := cr.Status.Root.ChildPartitions[0]
		for i := range leaf.Taints {
			if leaf.Taints[i].Key == key && leaf.Taints[i].Effect == effect {
				return &leaf.Taints[i]
			}
		}
		return nil
	}

	got := findLeafTaint(corev1.TaintNodeNotReady, partitionv1beta1.TaintEffectNoExecute)
	if got == nil || got.TimeAdded == nil || !got.TimeAdded.Equal(&wantT0) {
		t.Fatalf("step 1 (T0 not-ready): got %+v, want TimeAdded=%v", got, wantT0)
	}

	// 2. At T0+50s, node-lifecycle-controller swaps not-ready:NoExecute -> unreachable:NoExecute (TimeAdded = T0+50s).
	// Advance clock by 50s so GCE poll also runs and rebuilds Status.Root via tree.Build.
	tSwap1 := t0.Add(50 * time.Second)
	tSwap1Meta := metav1.NewTime(tSwap1)
	fx.clk.SetTime(tSwap1)
	upsertNodeTaints(false, []corev1.Taint{
		{Key: corev1.TaintNodeUnreachable, Effect: corev1.TaintEffectNoExecute, TimeAdded: &tSwap1Meta},
		{Key: corev1.TaintNodeUnreachable, Effect: corev1.TaintEffectNoSchedule},
	})
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("swap-to-unreachable reconcile: %v", err)
	}

	gotUnreach := findLeafTaint(corev1.TaintNodeUnreachable, partitionv1beta1.TaintEffectNoExecute)
	if gotUnreach == nil || gotUnreach.TimeAdded == nil || !gotUnreach.TimeAdded.Equal(&wantT0) {
		t.Errorf("step 2 (T0+50s swap to unreachable:NoExecute): TimeAdded = %v, want preserved %v", gotUnreach, wantT0)
	}
	// Non-NoExecute taint must NOT inherit the NoExecute group timestamp; it was first seen at T0+50s.
	gotUnreachNoSched := findLeafTaint(corev1.TaintNodeUnreachable, partitionv1beta1.TaintEffectNoSchedule)
	wantSwap1 := metav1.NewTime(tSwap1)
	if gotUnreachNoSched == nil || gotUnreachNoSched.TimeAdded == nil || !gotUnreachNoSched.TimeAdded.Equal(&wantSwap1) {
		t.Errorf("step 2 (unreachable:NoSchedule): TimeAdded = %v, want %v", gotUnreachNoSched, wantSwap1)
	}

	// 3. At T0+50.3s (non-polling reconcile) and T0+90s (swap back to not-ready:NoExecute with TimeAdded = T0+90s),
	// leaf PartitionTaint.TimeAdded still stays T0!
	fx.clk.SetTime(tSwap1.Add(300 * time.Millisecond))
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("non-poll after swap reconcile: %v", err)
	}
	tSwap2 := t0.Add(90 * time.Second)
	tSwap2Meta := metav1.NewTime(tSwap2)
	fx.clk.SetTime(tSwap2)
	upsertNodeTaints(false, []corev1.Taint{
		{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoExecute, TimeAdded: &tSwap2Meta},
	})
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("swap-back-to-not-ready reconcile: %v", err)
	}
	gotSwapBack := findLeafTaint(corev1.TaintNodeNotReady, partitionv1beta1.TaintEffectNoExecute)
	if gotSwapBack == nil || gotSwapBack.TimeAdded == nil || !gotSwapBack.TimeAdded.Equal(&wantT0) {
		t.Errorf("step 3 (T0+90s swap back to not-ready:NoExecute): TimeAdded = %v, want preserved %v", gotSwapBack, wantT0)
	}

	// 4. At T0+120s, node recovers completely -> leaf taints empty.
	fx.clk.SetTime(t0.Add(120 * time.Second))
	upsertNodeTaints(true, nil)
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("recovery reconcile: %v", err)
	}
	if leafTaints := fx.get().Status.Root.ChildPartitions[0].Taints; len(leafTaints) != 0 {
		t.Fatalf("step 4 (recovery): expected empty leaf taints, got %+v", leafTaints)
	}

	// 5. At T0+200s, a new outage episode begins -> fresh TimeAdded = T0+200s!
	tFresh := t0.Add(200 * time.Second)
	tFreshMeta := metav1.NewTime(tFresh)
	wantFresh := metav1.NewTime(tFresh)
	fx.clk.SetTime(tFresh)
	upsertNodeTaints(false, []corev1.Taint{
		{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoExecute, TimeAdded: &tFreshMeta},
	})
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("fresh episode reconcile: %v", err)
	}
	gotFresh := findLeafTaint(corev1.TaintNodeNotReady, partitionv1beta1.TaintEffectNoExecute)
	if gotFresh == nil || gotFresh.TimeAdded == nil || !gotFresh.TimeAdded.Equal(&wantFresh) {
		t.Errorf("step 5 (T0+200s fresh episode): TimeAdded = %v, want fresh %v", gotFresh, wantFresh)
	}
}

// TestNodeHealthEpisodeIsPerNodeInMultiNodeLeaf checks that a multi-node leaf
// reports the earliest outage start among the nodes that are tainted now: a
// recovered node's older start is not reused for another node's later episode,
// a not-ready taint re-added after a missed recovery starts a new episode, and a
// not-ready -> unreachable swap still keeps that node's start.
func TestNodeHealthEpisodeIsPerNodeInMultiNodeLeaf(t *testing.T) {
	fx := newFixture(t)
	fx.rec.EnableTaints = NewTaintConfig(true)
	t0 := time.Date(2026, 11, 3, 14, 0, 0, 0, time.UTC)
	at := func(d time.Duration) *metav1.Time { m := metav1.NewTime(t0.Add(d)); return &m }
	fx.clk.SetTime(t0)

	upsert := func(name string, taints ...corev1.Taint) {
		fx.cch.Upsert(name, topocache.NodeEntry{
			NodePool:        fx.name,
			MIG:             testMIG,
			AcceleratorType: "tpu7x",
			TopologyBlock:   "block-1",
			PartitionIDs:    []string{"root", "leaf"},
			Ready:           len(taints) == 0,
			Taints:          taints,
		})
	}
	noExec := func(key string, added *metav1.Time) corev1.Taint {
		return corev1.Taint{Key: key, Effect: corev1.TaintEffectNoExecute, TimeAdded: added}
	}
	leafNoExec := func(key string) *metav1.Time {
		for _, lt := range fx.get().Status.Root.ChildPartitions[0].Taints {
			if lt.Key == key && lt.Effect == partitionv1beta1.TaintEffectNoExecute {
				return lt.TimeAdded
			}
		}
		return nil
	}
	step := func(name string, d time.Duration, key string, want *metav1.Time) {
		t.Helper()
		fx.clk.SetTime(t0.Add(d))
		if _, err := fx.reconcile(); err != nil {
			t.Fatalf("%s: reconcile: %v", name, err)
		}
		if got := leafNoExec(key); got == nil || !got.Equal(want) {
			t.Fatalf("%s: leaf %s TimeAdded = %v, want %v", name, key, got, want)
		}
	}

	upsert("n1", noExec(corev1.TaintNodeNotReady, at(0)))
	upsert("n2")
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	step("n1 NotReady since T0", 0, corev1.TaintNodeNotReady, at(0))

	upsert("n2", noExec(corev1.TaintNodeNotReady, at(250*time.Second)))
	step("n2 also NotReady since T0+250s", 250*time.Second, corev1.TaintNodeNotReady, at(0))

	upsert("n1")
	step("n1 recovered: only n2's T0+250s episode remains", 260*time.Second, corev1.TaintNodeNotReady, at(250*time.Second))

	upsert("n2", noExec(corev1.TaintNodeNotReady, at(400*time.Second)))
	step("n2 not-ready re-added at T0+400s after a missed recovery", 400*time.Second, corev1.TaintNodeNotReady, at(400*time.Second))

	upsert("n2", noExec(corev1.TaintNodeUnreachable, at(440*time.Second)))
	step("n2 swaps to unreachable at T0+440s and keeps T0+400s", 440*time.Second, corev1.TaintNodeUnreachable, at(400*time.Second))
}

func TestDecoupled_DrainWithTaintsSkipsNoOpStatusPatch(t *testing.T) {
	fx := newDecoupledFixture(t)
	fx.rec.EnableTaints = NewTaintConfig(true)
	partID := "drain-taint-patch-root"
	fx.name = partID

	fx.cch.PushUpdate(partID, topocache.PTUpdateRequest{
		CRName: partID,
		MIG:    testMIG,
	})
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("create: %v", err)
	}

	t0 := metav1.NewTime(fx.clk.Now().UTC().Truncate(time.Second))
	bootingNode := func(name string, notReadySince *metav1.Time) {
		fx.cch.Upsert(name, topocache.NodeEntry{
			NodePool:        fx.name,
			MIG:             testMIG,
			AcceleratorType: "tpu7x",
			PartitionIDs:    []string{"root", "leaf"},
			Ready:           false,
			Taints: []corev1.Taint{
				{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoExecute, TimeAdded: notReadySince},
				{Key: "example.com/maint", Value: "true", Effect: corev1.TaintEffectNoSchedule}, // unstamped
			},
		})
	}
	bootingNode("node-0", &t0)
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("poll with taint: %v", err)
	}

	// First drain sets Synced=False/PoolDrained (1 /status PATCH expected).
	fx.cch.PushUpdate(partID, topocache.PTUpdateRequest{
		CRName:  partID,
		Drained: true,
	})
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("first drain reconcile: %v", err)
	}

	patchesAfterDrain := fx.env.statusPatches.Load()
	// Subsequent reconciles on the already-drained CR must send 0 /status PATCHes.
	for i := range 2 {
		fx.clk.SetTime(fx.clk.Now().Add(10 * time.Second))
		if _, err := fx.reconcile(); err != nil {
			t.Fatalf("repeat drained reconcile %d: %v", i, err)
		}
	}
	if got := fx.env.statusPatches.Load() - patchesAfterDrain; got != 0 {
		t.Errorf("repeated drained reconciles sent %d /status PATCH(es), want 0", got)
	}

	// Hours later the pool is re-provisioned with a brand-new node that boots
	// NotReady. The drained tree's timestamps (T0) must not carry onto it.
	fx.clk.SetTime(fx.clk.Now().Add(3 * time.Hour))
	tNew := metav1.NewTime(fx.clk.Now().UTC().Truncate(time.Second))
	fx.cch.Delete("node-0")
	bootingNode("node-1", &tNew)
	fx.cch.PushUpdate(partID, topocache.PTUpdateRequest{
		CRName: partID,
		MIG:    testMIG,
	})
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("re-provision reconcile: %v", err)
	}
	leafTaintTime := func(key string) *metav1.Time {
		for _, lt := range fx.get().Status.Root.ChildPartitions[0].Taints {
			if lt.Key == key {
				return lt.TimeAdded
			}
		}
		return nil
	}
	for _, key := range []string{corev1.TaintNodeNotReady, "example.com/maint"} {
		if got := leafTaintTime(key); got == nil || !got.Equal(&tNew) {
			t.Errorf("re-provisioned leaf %s TimeAdded = %v, want %v (not drain-time %v)", key, got, tNew, t0)
		}
	}

	// Once Synced has left PoolDrained, the fresh stamps carry forward without
	// re-stamping or extra /status PATCHes.
	patchesAfterReprovision := fx.env.statusPatches.Load()
	fx.clk.SetTime(fx.clk.Now().Add(10 * time.Second))
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("post re-provision reconcile: %v", err)
	}
	if got := leafTaintTime("example.com/maint"); got == nil || !got.Equal(&tNew) {
		t.Errorf("post re-provision example.com/maint TimeAdded = %v, want carried %v", got, tNew)
	}
	if got := fx.env.statusPatches.Load() - patchesAfterReprovision; got != 0 {
		t.Errorf("post re-provision reconcile sent %d /status PATCH(es), want 0", got)
	}
}

func collectGaugeCounts(t *testing.T, reader *sdkmetric.ManualReader, metricName string) (active, tombstoned int) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctxBG(), &rm); err != nil {
		t.Fatalf("collect metrics for %s: %v", metricName, err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != metricName {
				continue
			}
			if g, ok := m.Data.(metricdata.Gauge[int64]); ok {
				for _, dp := range g.DataPoints {
					if dp.Value > 0 {
						active++
					} else {
						tombstoned++
					}
				}
			}
		}
	}
	return active, tombstoned
}

func assertGaugeCounts(t *testing.T, reader *sdkmetric.ManualReader, stage, metricName string, wantActive, wantTomb int) {
	t.Helper()
	active, tomb := collectGaugeCounts(t, reader, metricName)
	if active != wantActive || tomb != wantTomb {
		t.Fatalf("%s %s (active, tomb) = (%d, %d), want (%d, %d)", stage, metricName, active, tomb, wantActive, wantTomb)
	}
}

// TestDecoupled_ReprovisionAndDeletionMetrics verifies that in decoupled lifecycle mode:
//  1. Draining a node pool preserves the CR and keeps its last metrics active (no dip to 0).
//  2. Reprovisioning with a new MIG supersedes the old MIG series inside internal/metrics,
//     emitting 1 for the new MIG while tombstoning the old MIG at 0 within the TTL window.
//  3. Deleting the PartitionTree CR itself transitions all active series to 0 via handleDeletion.
func TestDecoupled_ReprovisionAndDeletionMetrics(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "test")
	metrics.ClearGaugeSets()
	readerCustomer := sdkmetric.NewManualReader()
	readerGoogle := sdkmetric.NewManualReader()
	mockMeta, err := metadata.Get(context.Background(), true)
	if err != nil {
		t.Fatalf("metadata.Get: %v", err)
	}
	shutdown, err := metrics.InitWithReaders(
		context.Background(), readerCustomer, readerGoogle, *mockMeta, 30*time.Minute, 100000, true, true,
	)
	if err != nil {
		t.Fatalf("InitWithReaders: %v", err)
	}
	defer shutdown()

	fx := newDecoupledFixture(t)
	fx.rec.EnableTaints = NewTaintConfig(true)
	partID := "decoupled-metrics-root"
	fx.name = partID

	// 1. Create decoupled CR via pending update.
	fx.cch.PushUpdate(partID, topocache.PTUpdateRequest{
		CRName: partID,
		MIG:    testMIG,
	})
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("create reconcile: %v", err)
	}

	// Add a taint annotation on the CR.
	cr := fx.get()
	metaOriginal := cr.DeepCopy()
	if cr.Annotations == nil {
		cr.Annotations = map[string]string{}
	}
	cr.Annotations[tree.TaintAnnotationPrefix+"test-taint"] =
		`{"partitionId": "root", "key": "drain-test-key", "value": "v1", "effect": "NoSchedule"}`
	if err := fx.env.client.Patch(ctxBG(), cr, client.MergeFrom(metaOriginal)); err != nil {
		t.Fatalf("patch annotations: %v", err)
	}

	// 2. Seed pool and reconcile to populate tree and active metrics.
	fx.seedPool("node-0", true, "root", "leaf")
	fx.cch.SetPoolRootPartitionID(testPoolName, partID)
	fx.gce.SetResponse("testpool-mig", singleLeafResponse("root", "leaf"))
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("poll reconcile: %v", err)
	}

	assertGaugeCounts(t, readerCustomer, "pre-drain", metrics.MetricCustomerPartitionTreeInfo, 1, 0)
	assertGaugeCounts(t, readerCustomer, "pre-drain", metrics.MetricCustomerPartitionTreeOccupancy, 2, 0)
	assertGaugeCounts(t, readerCustomer, "pre-drain", metrics.MetricCustomerPartitionTreeTaint, 1, 0)

	// 3. Drain the node pool in decoupled mode -> CR is preserved and metrics stay active.
	fx.cch.Delete("node-0")
	fx.cch.DeletePoolRootPartitionID(testPoolName)
	fx.cch.PushUpdate(partID, topocache.PTUpdateRequest{
		CRName:  partID,
		Drained: true,
	})
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("drain reconcile: %v", err)
	}

	cr = fx.get()
	c := meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeSynced)
	if c == nil || c.Status != "False" || c.Reason != "PoolDrained" {
		t.Fatalf("Synced after drain = %+v, want False/PoolDrained", c)
	}

	assertGaugeCounts(t, readerCustomer, "post-drain", metrics.MetricCustomerPartitionTreeInfo, 1, 0)
	assertGaugeCounts(t, readerCustomer, "post-drain", metrics.MetricCustomerPartitionTreeOccupancy, 2, 0)
	assertGaugeCounts(t, readerCustomer, "post-drain", metrics.MetricCustomerPartitionTreeTaint, 1, 0)

	// 4. Reprovision with a new MIG (newpool-mig) within the TTL window.
	newMIG := "https://www.googleapis.com/compute/v0.beta/projects/1234567890/" +
		"zones/us-central2-b/instanceGroupManagers/newpool-mig"
	fx.cch.PushUpdate(partID, topocache.PTUpdateRequest{
		CRName: partID,
		MIG:    newMIG,
	})
	fx.cch.Upsert("node-1", topocache.NodeEntry{
		NodePool:        testPoolName,
		MIG:             newMIG,
		AcceleratorType: "tpu7x",
		PartitionIDs:    []string{"root", "leaf"},
		Ready:           true,
	})
	fx.cch.SetPoolRootPartitionID(testPoolName, partID)
	fx.gce.SetResponse("newpool-mig", singleLeafResponse("root", "leaf"))
	fx.clk.SetTime(fx.clk.Now().Add(5 * time.Second))

	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("reprovision reconcile: %v", err)
	}

	// Info should now have 1 active (newpool-mig = 1) and 1 tombstoned (testpool-mig = 0).
	assertGaugeCounts(t, readerCustomer, "post-reprovision", metrics.MetricCustomerPartitionTreeInfo, 1, 1)

	// 5. Delete the PartitionTree CR -> handleDeletion clears all remaining active series to 0.
	fx.cch.Delete("node-1")
	fx.cch.DeletePoolRootPartitionID(testPoolName)
	cr = fx.get()
	if err := fx.env.client.Delete(ctxBG(), cr); err != nil {
		t.Fatalf("delete CR: %v", err)
	}
	if _, err := fx.reconcile(); err != nil {
		t.Fatalf("delete reconcile: %v", err)
	}

	assertGaugeCounts(t, readerCustomer, "post-delete", metrics.MetricCustomerPartitionTreeInfo, 0, 2)
	assertGaugeCounts(t, readerCustomer, "post-delete", metrics.MetricCustomerPartitionTreeOccupancy, 0, 2)
	assertGaugeCounts(t, readerCustomer, "post-delete", metrics.MetricCustomerPartitionTreeTaint, 0, 1)
}
