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

package config

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apitypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
)

func TestLoadInitial_NotFound_ReturnsDefaults(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	cfg, err := LoadInitial(context.Background(), c, "ns", "missing-cm")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Polling.Interval.D() != 1*time.Second {
		t.Errorf("expected default polling interval, got %v", cfg.Polling.Interval.D())
	}
}

func TestLoadInitial_InvalidYAML_FallsBackToDefaults(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "bad-config", Namespace: "ns"},
		Data:       map[string]string{ConfigMapDataKey: "not: valid: yaml: [[["},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()

	cfg, err := LoadInitial(context.Background(), c, "ns", "bad-config")
	if err != nil {
		t.Fatalf("expected fallback to defaults, got error: %v", err)
	}
	if cfg.Polling.Interval.D() != 1*time.Second {
		t.Errorf("expected default polling interval, got %v", cfg.Polling.Interval.D())
	}
}

func TestLoadInitial_InvalidValidation_FallsBackToDefaults(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	// Valid YAML but fails validation (polling interval = 0).
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "bad-config", Namespace: "ns"},
		Data:       map[string]string{ConfigMapDataKey: `{"polling":{"interval":"0s"}}`},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()

	cfg, err := LoadInitial(context.Background(), c, "ns", "bad-config")
	if err != nil {
		t.Fatalf("expected fallback to defaults, got error: %v", err)
	}
	if cfg.Polling.Interval.D() != 1*time.Second {
		t.Errorf("expected default polling interval, got %v", cfg.Polling.Interval.D())
	}
}

func TestLoadInitial_ValidConfig(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "good-config", Namespace: "ns"},
		Data:       map[string]string{ConfigMapDataKey: `{"polling":{"interval":"5s","jitter":"200ms"},"syncTimeout":"10s","retry":{"initialBackoff":"2s","maxBackoff":"1m","multiplier":2},"rateLimit":{"qps":100,"burst":50}}`},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()

	cfg, err := LoadInitial(context.Background(), c, "ns", "good-config")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Polling.Interval.D() != 5*time.Second {
		t.Errorf("polling interval = %v want 5s", cfg.Polling.Interval.D())
	}
}

// loaderScheme builds a scheme with both core/v1 and the PartitionTree CRD.
func loaderScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = partitionv1beta1.AddToScheme(s)
	return s
}

func TestLoaderReconcile_ConfigDeleted(t *testing.T) {
	s := loaderScheme()
	c := fake.NewClientBuilder().WithScheme(s).Build()
	store := NewStore(Default())
	reloadCh := make(chan event.TypedGenericEvent[*partitionv1beta1.PartitionTree], 10)

	r := &LoaderReconciler{
		Client:    c,
		Namespace: "ns",
		Name:      "cfg",
		Store:     store,
		ReloadCh:  reloadCh,
	}

	result, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: apitypes.NamespacedName{Namespace: "ns", Name: "cfg"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("unexpected requeue: %+v", result)
	}
	// Store should still hold the default config.
	if store.Load().Polling.Interval.D() != 1*time.Second {
		t.Errorf("store changed after config deleted")
	}
}

func TestLoaderReconcile_ValidUpdate_FanOut(t *testing.T) {
	s := loaderScheme()

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cfg", Namespace: "ns"},
		Data: map[string]string{
			ConfigMapDataKey: `{"polling":{"interval":"7s","jitter":"100ms"},"syncTimeout":"20s","retry":{"initialBackoff":"1s","maxBackoff":"30s","multiplier":2},"rateLimit":{"qps":50,"burst":50}}`,
		},
	}
	pt1 := &partitionv1beta1.PartitionTree{ObjectMeta: metav1.ObjectMeta{Name: "pool-a"}}
	pt2 := &partitionv1beta1.PartitionTree{ObjectMeta: metav1.ObjectMeta{Name: "pool-b"}}

	c := fake.NewClientBuilder().WithScheme(s).WithObjects(cm, pt1, pt2).Build()
	store := NewStore(Default())
	reloadCh := make(chan event.TypedGenericEvent[*partitionv1beta1.PartitionTree], 10)

	r := &LoaderReconciler{
		Client:    c,
		Namespace: "ns",
		Name:      "cfg",
		Store:     store,
		ReloadCh:  reloadCh,
	}

	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: apitypes.NamespacedName{Namespace: "ns", Name: "cfg"},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// Store should be updated.
	if store.Load().Polling.Interval.D() != 7*time.Second {
		t.Errorf("store not updated: interval = %v", store.Load().Polling.Interval.D())
	}
	// Should have fanned out 2 reload events.
	if len(reloadCh) != 2 {
		t.Errorf("reload events = %d want 2", len(reloadCh))
	}
}

func TestLoaderReconcile_InvalidUpdate_KeepsPrevious(t *testing.T) {
	s := loaderScheme()

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cfg", Namespace: "ns"},
		Data:       map[string]string{ConfigMapDataKey: `not valid yaml [[[`},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(cm).Build()

	original := Default()
	store := NewStore(original)
	reloadCh := make(chan event.TypedGenericEvent[*partitionv1beta1.PartitionTree], 10)

	r := &LoaderReconciler{
		Client:    c,
		Namespace: "ns",
		Name:      "cfg",
		Store:     store,
		ReloadCh:  reloadCh,
	}

	result, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: apitypes.NamespacedName{Namespace: "ns", Name: "cfg"},
	})
	if err != nil {
		t.Fatalf("unexpected error (should swallow parse errors): %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Error("should not requeue on parse error")
	}
	// Store should still hold the original config.
	if store.Load().Polling.Interval.D() != original.Polling.Interval.D() {
		t.Errorf("store changed on invalid update")
	}
	// No reload events should have been sent.
	if len(reloadCh) != 0 {
		t.Errorf("reload events = %d want 0", len(reloadCh))
	}
}
