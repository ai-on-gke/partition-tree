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
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	sigsyaml "sigs.k8s.io/yaml"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
)

// ConfigMapDataKey is the ConfigMap data key holding the serialized
// Config YAML. Keeping this as a constant lets operators deploy the
// ConfigMap without guessing the field name.
const ConfigMapDataKey = "config.yaml"

// LoadInitial fetches the ConfigMap synchronously at startup and
// returns the parsed Config. A NotFound response is treated as
// "operator has not deployed a config yet" and falls back to the
// built-in defaults. Parse or validation errors also fall back to
// defaults with a warning — this prevents a broken ConfigMap from
// causing a crash loop on restart. The runtime LoaderReconciler
// applies the same policy (keep previous good config on bad update).
func LoadInitial(ctx context.Context, c client.Client, namespace, name string) (*Config, error) {
	lg := log.FromContext(ctx)
	// Guard against a slow or unresponsive API server at startup —
	// context.Background() callers would otherwise hang indefinitely.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var cm corev1.ConfigMap
	err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &cm)
	if apierrors.IsNotFound(err) {
		return Default(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading config map %s/%s: %w", namespace, name, err)
	}
	cfg, err := parseConfigData(cm.Data)
	if err != nil {
		lg.Error(err, "invalid config map at startup; falling back to defaults",
			"configmap", fmt.Sprintf("%s/%s", namespace, name))
		return Default(), nil
	}
	return cfg, nil
}

// parseConfigData parses, validates, and returns a Config. Missing
// keys fall through to Default() values via the YAML unmarshal of a
// pre-seeded struct.
func parseConfigData(data map[string]string) (*Config, error) {
	raw, ok := data[ConfigMapDataKey]
	if !ok {
		return nil, fmt.Errorf("config map missing %q key", ConfigMapDataKey)
	}
	c := Default()
	if err := sigsyaml.Unmarshal([]byte(raw), c); err != nil {
		return nil, fmt.Errorf("parsing config yaml: %w", err)
	}
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}
	return c, nil
}

// LoaderReconciler is a tiny reconciler that watches the single
// controller ConfigMap and swaps the atomic Store pointer on every
// valid update. Invalid updates are logged and rejected — the previous
// good config stays in effect.
//
// On a successful swap it lists every PartitionTree CR and emits one
// TypedGenericEvent per CR onto ReloadCh, so the main reconciler
// immediately re-reconciles each tree at the new cadence instead of
// waiting for the next polling tick.
type LoaderReconciler struct {
	Client    client.Client
	Namespace string
	Name      string
	Store     *Store
	ReloadCh  chan<- event.TypedGenericEvent[*partitionv1beta1.PartitionTree]
}

// Reconcile implements reconcile.Reconciler.
func (r *LoaderReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	lg := log.FromContext(ctx).WithValues("configmap", req.NamespacedName)

	var cm corev1.ConfigMap
	err := r.Client.Get(ctx, req.NamespacedName, &cm)
	if apierrors.IsNotFound(err) {
		lg.Info("config map deleted; keeping previous config")
		return reconcile.Result{}, nil
	}
	if err != nil {
		// Transient apiserver error: return the error so
		// controller-runtime backs off and retries. Returning nil here
		// would silently freeze the loader on the previous config until
		// the next watch event, which on a single-CM watch may never
		// arrive.
		lg.Error(err, "reading config map; will retry")
		return reconcile.Result{}, err
	}

	newCfg, err := parseConfigData(cm.Data)
	if err != nil {
		// Parse/validation errors are operator mistakes — retrying
		// won't help. Log and keep the previous good config.
		lg.Error(err, "invalid config map; keeping previous config")
		return reconcile.Result{}, nil
	}

	r.Store.Store(newCfg)
	lg.Info("config reloaded",
		"polling", newCfg.Polling.Interval.D().String(),
		"syncTimeout", newCfg.SyncTimeout.D().String())

	var list partitionv1beta1.PartitionTreeList
	if err := r.Client.List(ctx, &list); err != nil {
		lg.Error(err, "listing partition trees for fan-out; reload still took effect")
		return reconcile.Result{}, nil
	}
	// Fan out one reload event per CR. We block on send so a reconciler
	// that is momentarily slow does not cause silent drops. At target
	// scale (1440+ CRs) the channel is sized to absorb the whole
	// fan-out in one go, so blocking is a tail-latency guard, not a
	// steady cost. If ctx is cancelled we bail out — the manager is
	// shutting down and the new config will be picked up on the next
	// restart.
	for i := range list.Items {
		evt := event.TypedGenericEvent[*partitionv1beta1.PartitionTree]{Object: &list.Items[i]}
		select {
		case r.ReloadCh <- evt:
		case <-ctx.Done():
			lg.Info("context cancelled during reload fan-out",
				"delivered", i, "remaining", len(list.Items)-i)
			return reconcile.Result{}, nil
		}
	}
	return reconcile.Result{}, nil
}

// SetupWithManager registers the loader as a controller filtered to a
// single ConfigMap. Cache scoping is handled by the manager in cmd/main.go
// via cache.ByObject.
func (r *LoaderReconciler) SetupWithManager(mgr ctrl.Manager) error {
	onlyTarget := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return obj.GetNamespace() == r.Namespace && obj.GetName() == r.Name
	})
	return ctrl.NewControllerManagedBy(mgr).
		Named("partition-config-loader").
		For(&corev1.ConfigMap{}, builder.WithPredicates(onlyTarget)).
		Complete(r)
}
