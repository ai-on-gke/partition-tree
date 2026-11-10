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
	stderrors "errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/clock"

	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/source"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
	"github.com/ai-on-gke/partition-tree/internal/cache"
	"github.com/ai-on-gke/partition-tree/internal/config"
	"github.com/ai-on-gke/partition-tree/internal/gce"
	"github.com/ai-on-gke/partition-tree/internal/metrics"
	"github.com/ai-on-gke/partition-tree/internal/tree"
)

// PartitionTreeReconciler reconciles a PartitionTree object. It owns
// one work queue, fed by three sources: a GCE polling tick
// (RequeueAfter), node events (via nodeEventHandler), and config
// reload events (via ReloadCh).
type PartitionTreeReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	GCE         gce.Client
	Cache       *cache.Cache
	ConfigStore *config.Store
	Clock       clock.PassiveClock
	Recorder    events.EventRecorder
	// ReloadCh receives one event per existing PartitionTree CR each
	// time the config ConfigMap is hot-reloaded. Optional: if nil, the
	// reconciler simply won't be nudged on reload and will pick up the
	// new cadence on its next natural tick.
	ReloadCh <-chan event.TypedGenericEvent[*partitionv1beta1.PartitionTree]
	// PTUpdateCh receives events from the node informer when a
	// PartitionTree CR needs reconciliation due to a MIG change or
	// drain in decoupled lifecycle mode. Optional: only set when
	// DecoupledLifecycle is true.
	PTUpdateCh chan event.TypedGenericEvent[*partitionv1beta1.PartitionTree]
	// DecoupledLifecycle enables the decoupled PT CR lifecycle mode.
	// When true, CRs are named after the root partition ID instead of
	// the node pool, and draining clears the MIG annotation instead
	// of deleting the CR.
	DecoupledLifecycle bool

	EnableTaints TaintConfig

	// EnableTPUAllocatableReadiness makes a leaf's NodeReady condition
	// also require every node to have its full google.com/tpu chip count
	// allocatable (Reason TPUsNotAllocatable). It must match the
	// keepTPUAllocatable argument given to ManagerCacheOptions, otherwise
	// the cached Nodes carry no allocatable and every leaf reads False.
	EnableTPUAllocatableReadiness bool

	ControllerName string

	poll *pollStateStore
}

// +kubebuilder:rbac:groups=partition.tpu.google.com,resources=partitiontrees,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=partition.tpu.google.com,resources=partitiontrees/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=partition.tpu.google.com,resources=partitiontrees/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile is the single entrypoint for all updates to a PartitionTree.
// The decision tree, in short:
//
//  1. Handle absent-CR-with-live-cache (first-seen pool) → create.
//  2. Handle absent-pool-with-live-CR (scale-to-zero) → delete.
//  3. Handle deletion timestamp → finalizer cleanup.
//  4. Decide whether a GCE poll is due; if so, call GCE and rebuild
//     the tree. On error, preserve the previous tree and flip Synced
//     once syncTimeout elapses.
//  5. Refresh leaf NodeNames + per-leaf NodeReady from the cache.
//  6. Derive the top-level Synced condition.
//  7. Patch /status and requeue at pollingInterval.
//
//nolint:gocyclo // Reconcile is the main orchestration loop; splitting would reduce readability.
func (r *PartitionTreeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues("partitiontree", req.Name)
	if r.poll == nil {
		// Defensive: a unit test that constructs the reconciler
		// directly without going through SetupWithManager would
		// otherwise nil-panic on the first event.
		r.poll = newPollStateStore()
	}
	now := r.Clock.Now()
	cfg := r.ConfigStore.Load()
	requeue := ctrl.Result{RequeueAfter: jitteredInterval(cfg.Polling.Interval.D(), cfg.Polling.Jitter.D())}

	var cr partitionv1beta1.PartitionTree
	err := r.Get(ctx, req.NamespacedName, &cr)

	// Case 1: CR absent.
	if errors.IsNotFound(err) {
		if r.DecoupledLifecycle {
			// Decoupled mode: req.Name is a partition ID.
			// Check for pending create updates from the node informer.
			// Use last-writer-wins: if multiple updates queued before
			// this reconcile, the most recent MIG is the correct one.
			updates := r.Cache.DrainUpdates(req.Name)
			var createMIG string
			for _, u := range updates {
				if !u.Drained && u.MIG != "" {
					createMIG = u.MIG
				}
			}
			if createMIG != "" {
				return r.createDecoupledCR(ctx, req.Name, createMIG)
			}
			r.poll.forget(req.Name)
			return ctrl.Result{}, nil
		}
		pool, ok := r.Cache.NodePool(req.Name)
		if !ok {
			// Neither a CR nor a live pool — nothing to do.
			r.poll.forget(req.Name)
			return ctrl.Result{}, nil
		}
		return r.createCR(ctx, req.Name, pool.MIG)
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	// Case 2: deletion timestamp set → finalizer cleanup.
	if !cr.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, &cr)
	}

	// Ensure finalizer is present before anything else. Requeue
	// immediately (not at polling cadence) so the first GCE poll runs
	// without a polling-interval delay.
	if controllerutil.AddFinalizer(&cr, FinalizerName) {
		if err := r.Update(ctx, &cr); err != nil {
			return ctrl.Result{}, fmt.Errorf("adding finalizer: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Decoupled mode: process pending updates from the node informer.
	if r.DecoupledLifecycle {
		updates := r.Cache.DrainUpdates(req.Name)
		metaOrig := cr.DeepCopy()
		annotationChanged := false
		for _, u := range updates {
			if u.Drained {
				if cr.Annotations == nil {
					cr.Annotations = map[string]string{}
				}
				cr.Annotations[AnnotationMIGName] = ""
				annotationChanged = true
				log.Info("node pool drained (decoupled), clearing MIG")
				r.event(&cr, corev1.EventTypeNormal, EventReasonPoolDrained,
					"node pool drained, clearing MIG annotation on CR %s", req.Name)
			} else if u.MIG != "" && u.MIG != cr.Annotations[AnnotationMIGName] {
				if cr.Annotations == nil {
					cr.Annotations = map[string]string{}
				}
				cr.Annotations[AnnotationMIGName] = u.MIG
				annotationChanged = true
				log.Info("MIG updated (decoupled)", "newMIG", u.MIG)
				r.event(&cr, corev1.EventTypeNormal, EventReasonReprovision,
					"MIG updated to %s on CR %s", u.MIG, req.Name)
			}
		}
		if annotationChanged {
			if err := r.Patch(ctx, &cr, client.MergeFrom(metaOrig)); err != nil {
				return ctrl.Result{}, fmt.Errorf("patching MIG annotation: %w", err)
			}
		}

		// If MIG is empty (drained), suspend GCE polling and go idle.
		// The partition tree structure (cr.Status.Root) is intentionally
		// retained so re-provision can resume without rebuilding the
		// topology. Stale NodeNames/NodeReady are cleared by
		// refreshAndPropagate when the pool returns.
		if cr.Annotations[AnnotationMIGName] == "" {
			original := cr.DeepCopy()
			apimeta.SetStatusCondition(&cr.Status.Conditions, metav1.Condition{
				Type:               partitionv1beta1.ConditionTypeSynced,
				Status:             metav1.ConditionFalse,
				Reason:             ReasonPoolDrained,
				Message:            "node pool drained, GCE polling suspended",
				LastTransitionTime: metav1.NewTime(now),
			})
			if !apiequality.Semantic.DeepEqual(original.Status, cr.Status) {
				if err := r.Status().Patch(ctx, &cr, client.MergeFrom(original)); err != nil {
					return ctrl.Result{}, fmt.Errorf("patching drained status: %w", err)
				}
			}
			r.poll.forget(req.Name)
			return ctrl.Result{}, nil
		}

		// Register CR in cache for future node informer lookups.
		r.Cache.SetPartitionCR(req.Name, req.Name, cr.Annotations[AnnotationMIGName])
	}

	// Case 3: CR exists but the pool has drained.
	// In decoupled mode, req.Name is a partition ID, not a pool name.
	// Resolve the pool name for downstream lookups.
	poolKey := req.Name
	if r.DecoupledLifecycle {
		if pk, ok := r.Cache.RootPartitionPool(req.Name); ok {
			poolKey = pk
		}
	}
	pool, havePool := r.Cache.NodePool(poolKey)
	if !havePool && !r.DecoupledLifecycle {
		log.Info("node pool drained, deleting CR")
		r.event(&cr, corev1.EventTypeNormal, EventReasonPoolDrained,
			"node pool %s has no remaining nodes, deleting CR", req.Name)
		if err := r.Delete(ctx, &cr); err != nil && !errors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("deleting drained CR: %w", err)
		}
		return ctrl.Result{}, nil
	}
	// In decoupled mode, if the pool is not yet provisioned (or drained
	// without pending updates), skip the GCE poll and metrics to avoid
	// calling the API with an empty MIG and emitting metrics with
	// missing accelerator type.
	if !havePool && r.DecoupledLifecycle {
		log.V(1).Info("pool not provisioned yet, skipping GCE poll")
		return requeue, nil
	}

	// Case 4: normal reconcile.
	original := cr.DeepCopy()

	// Backfill MIG annotation on older CRs (defensive).
	if cr.Annotations[AnnotationMIGName] == "" {
		metaOriginal := cr.DeepCopy()
		if cr.Annotations == nil {
			cr.Annotations = map[string]string{}
		}
		cr.Annotations[AnnotationMIGName] = pool.MIG
		if err := r.Patch(ctx, &cr, client.MergeFrom(metaOriginal)); err != nil {
			return ctrl.Result{}, fmt.Errorf("stamping mig annotation: %w", err)
		}
		return requeue, nil
	}

	// Backfill cloud.google.com/tpu-pod-id label on older CRs (defensive).
	// This lets existing PartitionTree CRs pick up the label without
	// requiring a full re-creation.
	if cr.Labels[LabelTpuPodID] == "" {
		if block := r.Cache.PoolTopologyBlock(poolKey); block != "" {
			metaOriginal := cr.DeepCopy()
			if cr.Labels == nil {
				cr.Labels = map[string]string{}
			}
			cr.Labels[LabelTpuPodID] = block
			if err := r.Patch(ctx, &cr, client.MergeFrom(metaOriginal)); err != nil {
				return ctrl.Result{}, fmt.Errorf("stamping pod-id label: %w", err)
			}
			return requeue, nil
		}
	}

	migRef, err := parseMIGSelfLink(cr.Annotations[AnnotationMIGName])
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("invalid mig annotation: %w", err)
	}

	// Step 4a: decide on GCE poll. After a failure we honor an
	// exponential backoff window (initial → max, multiplier from
	// cfg.Retry) so node events / reload nudges don't hammer GCE.
	var taintsMap map[string][]partitionv1beta1.PartitionTaint
	var taintsParseErr error
	annotationsChanged := false
	if r.EnableTaints.Enabled {
		annotationsChanged = r.poll.UpdateAnnotations(req.Name, cr.Annotations)
		taintsMap, taintsParseErr = tree.ExtractTaintsFromAnnotations(cr.Annotations)
	}

	state := r.poll.get(req.Name)
	needPoll := false
	switch {
	case annotationsChanged && !now.Before(state.nextEligibleAt):
		needPoll = true
	case state.lastPolledAt.IsZero():
		needPoll = true
	case state.lastErr != nil:
		needPoll = !now.Before(state.nextEligibleAt)
	default:
		needPoll = now.Sub(state.lastPolledAt) >= cfg.Polling.Interval.D()
	}

	var pollErr error
	var rateLimited bool
	if needPoll {
		var resp *gce.TopologyResponse
		var err error

		// Prefer the non-blocking TryClient path so rate-limited GCE
		// polls do not block workers from refreshing node readiness.
		// The mock client does not implement TryClient, so unit tests
		// and e2e fall through to the blocking path (which never
		// blocks because mocks have no rate limiter).
		if tc, ok := r.GCE.(gce.TryClient); ok {
			resp, err = tc.TryGetAvailableAcceleratorTopologies(ctx, migRef.ProjectNumber, migRef.Zone, migRef.Name)
			if stderrors.Is(err, gce.ErrRateLimited) {
				rateLimited = true
				err = nil
				log.V(1).Info("GCE poll deferred: rate limited, proceeding with node refresh")
			}
		} else {
			resp, err = r.GCE.GetAvailableAcceleratorTopologies(ctx, migRef.ProjectNumber, migRef.Zone, migRef.Name)
		}

		if err != nil {
			pollErr = err
			r.poll.recordFailure(req.Name, now, err, cfg.Retry)
			log.Error(err, "GCE poll failed, preserving last-known tree")
			r.event(&cr, corev1.EventTypeWarning, EventReasonGCEPollError,
				"GCE poll failed: %v", err)
		} else if resp != nil {
			var activeTaintsMap map[string][]partitionv1beta1.PartitionTaint
			if r.EnableTaints.Enabled {
				if taintsParseErr != nil {
					log.Error(taintsParseErr, "Failed to parse external taints annotations, reusing last-known-good taints")
					r.event(&cr, corev1.EventTypeWarning, EventReasonTaintParseError, "Failed to parse taints annotations (reusing last-known-good): %v", taintsParseErr)
					activeTaintsMap = tree.ExtractTaints(cr.Status.Root)
					apimeta.SetStatusCondition(&cr.Status.Conditions, tree.DeriveTaintsValid(false, now, taintsParseErr.Error()))
				} else {
					activeTaintsMap = taintsMap
					apimeta.SetStatusCondition(&cr.Status.Conditions, tree.DeriveTaintsValid(true, now, "taints parsed successfully"))
					if len(activeTaintsMap) > 0 {
						r.validateTaintTargets(&cr, resp, activeTaintsMap)
					}
				}
			}
			root, err := tree.Build(resp, activeTaintsMap)
			if err != nil {
				pollErr = err
				r.poll.recordFailure(req.Name, now, err, cfg.Retry)
				log.Error(err, "tree build failed, preserving last-known tree")
			} else {
				tree.Bubble(&root)
				// GCE's response does not carry the TPU family
				// (v6e/v7x/…). Stamp it from the node-pool cache onto
				// the top-level status.
				stampAcceleratorType(&cr.Status, pool.AcceleratorType)
				// Carry forward per-partition conditions from the old
				// tree so LastTransitionTime is preserved when the
				// condition status hasn't actually changed.
				carryForwardConditions(cr.Status.Root, &root)
				cr.Status.Root = &root
				r.poll.recordSuccess(req.Name, now)

				// Re-read state so the freshness check below uses the update.
				state = r.poll.get(req.Name)
			}
		}
	}

	if cr.Status.Root != nil {
		snap := r.Cache.PoolLeaves(poolKey)
		if r.EnableTaints.Enabled {
			prevTaints := tree.ExtractTaints(original.Status.Root)
			if c := apimeta.FindStatusCondition(original.Status.Conditions, partitionv1beta1.ConditionTypeSynced); c != nil && c.Reason == ReasonPoolDrained {
				// The drained pool is back with new nodes: start fresh instead of
				// carrying the retained drain-time taint timestamps onto them. The
				// first reconcile that polls GCE moves Synced off PoolDrained
				// (applySynced), after which timestamps carry forward again.
				prevTaints = nil
			}
			if taintsParseErr != nil {
				log.V(1).Info("Failed to parse external taints annotations, freezing taints for this cycle", "error", taintsParseErr)
				r.refreshAndPropagate(cr.Status.Root, snap, nil, prevTaints, now, true /* skipTaintUpdates */, nil)
			} else {
				r.refreshAndPropagate(cr.Status.Root, snap, taintsMap, prevTaints, now, false /* skipTaintUpdates */, cfg.Taints.NodePropagation.ExcludedPrefixes)
			}
		} else {
			r.refreshAndPropagate(cr.Status.Root, snap, nil, nil, now, false /* skipTaintUpdates */, nil)
		}

		podName := cr.Labels[LabelTpuPodID]
		if podName == "" {
			podName = r.Cache.PoolTopologyBlock(poolKey)
		}
		metrics.RecordPartitionTreeMetrics(ctx, cr.Status.Root, cr.Name, string(cr.UID), poolKey, migRef.Name, pool.AcceleratorType, podName)
	}

	// Step 4d: top-level Synced condition (lives on cr.Status.Conditions,
	// not on any individual partition).
	r.applySynced(&cr, state, cfg, now, pollErr)

	// Step 4e: persist — skip the patch if nothing changed to avoid
	// unnecessary apiserver load (480 PATCH/sec at 1440 CRs). Semantic
	// equality compares metav1.Time by instant rather than Go location
	// pointer (apiserver round-trips decode RFC3339 into time.Local).
	if !apiequality.Semantic.DeepEqual(original.Status, cr.Status) {
		if err := r.Status().Patch(ctx, &cr, client.MergeFrom(original)); err != nil {
			return ctrl.Result{}, fmt.Errorf("patching status: %w", err)
		}
	}

	// Rate-limited CRs retry quickly so the GCE poll happens as
	// soon as a token is available, without blocking the worker.
	if rateLimited {
		return ctrl.Result{RequeueAfter: 100 * time.Millisecond}, nil
	}
	return requeue, nil
}

// createCR handles the first-seen-pool path: no CR exists yet but the
// node informer has already populated the cache. The freshly created
// object carries the MIG annotation and finalizer, with an initial
// Root.State=Unknown until the first successful poll.
func (r *PartitionTreeReconciler) createCR(ctx context.Context, name, migSelfLink string) (ctrl.Result, error) {
	labels := map[string]string{}
	// Propagate the GCE topology block from node labels to the
	// PartitionTree CR as cloud.google.com/tpu-pod-id. This lets the
	// slice-scheduler group trees belonging to the same physical
	// superpod for multi-cube scheduling (e.g. 4x4x8 = 2 cubes).
	if block := r.Cache.PoolTopologyBlock(name); block != "" {
		labels[LabelTpuPodID] = block
	}
	cr := &partitionv1beta1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Labels:      labels,
			Annotations: map[string]string{AnnotationMIGName: migSelfLink},
			Finalizers:  []string{FinalizerName},
		},
		Status: partitionv1beta1.PartitionTreeStatus{
			// Root is left as nil until the first successful GCE poll.
			// This allows saving status conditions even if the poll fails.
		},
	}
	if err := r.Create(ctx, cr); err != nil {
		if errors.IsAlreadyExists(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("creating partition tree: %w", err)
	}
	r.event(cr, corev1.EventTypeNormal, EventReasonCreated,
		"created PartitionTree for MIG %s", migSelfLink)
	return ctrl.Result{}, nil
}

// handleDeletion performs finalizer cleanup. In addition to removing
// the finalizer and forgetting poll state, it cleans up the in-memory
// partition CR cache to prevent stale entries from blocking
// re-creation. In decoupled mode, if the pool still exists, it pushes
// a create update so the node informer can re-create the CR.
func (r *PartitionTreeReconciler) handleDeletion(ctx context.Context, cr *partitionv1beta1.PartitionTree) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(cr, FinalizerName) {
		return ctrl.Result{}, nil
	}
	controllerutil.RemoveFinalizer(cr, FinalizerName)
	if err := r.Update(ctx, cr); err != nil {
		return ctrl.Result{}, fmt.Errorf("removing finalizer: %w", err)
	}
	r.poll.forget(cr.Name)
	metrics.ClearPartitionTreeMetrics(ctx, cr.Name)
	if r.DecoupledLifecycle {
		r.Cache.DeletePartitionCR(cr.Name)
		// Self-healing: if the pool still has nodes, push a create
		// update and wake the reconciler so it re-creates the CR
		// promptly instead of waiting for a random node event.
		if pool, ok := r.Cache.RootPartitionPool(cr.Name); ok {
			if pe, ok := r.Cache.NodePool(pool); ok {
				r.Cache.PushUpdate(cr.Name, cache.PTUpdateRequest{
					CRName: cr.Name,
					MIG:    pe.MIG,
				})
				sendPTUpdate(r.PTUpdateCh, cr.Name)
			}
		}
	}
	return ctrl.Result{}, nil
}

// ManagerCacheOptions returns the controller-runtime cache.Options used by the
// partition-tree manager:
//   - DefaultTransform strips managedFields across all cached objects.
//   - Nodes are server-side filtered to PROVISION_ONLY topology nodes and
//     trimmed in-memory via StripNodeForCache, or via
//     StripNodeForCacheKeepTPUAllocatable when keepTPUAllocatable is set
//     (--enable-tpu-allocatable-readiness). The transform is fixed for the
//     manager's lifetime.
//   - ConfigMaps are scoped to the single runtime configuration ConfigMap in
//     configNamespace with metadata.name=configName (when non-empty).
func ManagerCacheOptions(configNamespace, configName string, keepTPUAllocatable bool) ctrlcache.Options {
	nodeSel := labels.SelectorFromSet(labels.Set{
		LabelTopologyMode: LabelTopologyModeProvisionOnly,
	})
	nodeTransform := StripNodeForCache
	if keepTPUAllocatable {
		nodeTransform = StripNodeForCacheKeepTPUAllocatable
	}
	byObject := map[client.Object]ctrlcache.ByObject{
		&corev1.Node{}: {
			Label:     nodeSel,
			Transform: nodeTransform,
		},
	}
	if configNamespace != "" && configName != "" {
		byObject[&corev1.ConfigMap{}] = ctrlcache.ByObject{
			Namespaces: map[string]ctrlcache.Config{
				configNamespace: {
					FieldSelector: fields.OneTermEqualSelector("metadata.name", configName),
				},
			},
		}
	}
	return ctrlcache.Options{
		DefaultTransform: ctrlcache.TransformStripManagedFields(),
		ByObject:         byObject,
	}
}

// SetupWithManager registers the PartitionTree controller with the
// manager, wiring both the CR watch and the Node watch (with its
// predicate + custom event handler). The pollStateStore is
// initialized here so callers don't have to remember to do it.
func (r *PartitionTreeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Clock == nil {
		r.Clock = clock.RealClock{}
	}
	if r.poll == nil {
		r.poll = newPollStateStore()
	}
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("partition-controller")
	}

	cfg := r.ConfigStore.Load()
	b := ctrl.NewControllerManagedBy(mgr).
		For(&partitionv1beta1.PartitionTree{}).
		Named(r.controllerName()).
		WithOptions(controller.Options{MaxConcurrentReconciles: cfg.Concurrency.MaxReconciles}).
		WatchesRawSource(
			source.Kind(
				mgr.GetCache(),
				&corev1.Node{},
				nodeEventHandler(r.Cache, r.DecoupledLifecycle, r.PTUpdateCh),
				matchingNodesPredicate(),
			),
		)
	if r.ReloadCh != nil {
		b = b.WatchesRawSource(
			source.Channel(
				r.ReloadCh,
				&handler.TypedEnqueueRequestForObject[*partitionv1beta1.PartitionTree]{},
			),
		)
	}
	if r.PTUpdateCh != nil {
		b = b.WatchesRawSource(
			source.Channel(
				r.PTUpdateCh,
				&handler.TypedEnqueueRequestForObject[*partitionv1beta1.PartitionTree]{},
			),
		)
	}
	return b.Complete(r)
}

func (r *PartitionTreeReconciler) controllerName() string {
	if r.ControllerName != "" {
		return r.ControllerName
	}
	return "partitiontree"
}

// createDecoupledCR handles first-seen-partition in decoupled mode: no CR
// exists yet. The CR is named after the sanitized root partition ID.
func (r *PartitionTreeReconciler) createDecoupledCR(ctx context.Context, partitionID, migSelfLink string) (ctrl.Result, error) {
	labels := map[string]string{}
	// Look up the pool name for this partition to stamp the pod-id
	// label, mirroring createCR behaviour.
	if pool, ok := r.Cache.RootPartitionPool(partitionID); ok {
		if block := r.Cache.PoolTopologyBlock(pool); block != "" {
			labels[LabelTpuPodID] = block
		}
	}
	cr := &partitionv1beta1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{
			Name:        partitionID,
			Labels:      labels,
			Annotations: map[string]string{AnnotationMIGName: migSelfLink},
			Finalizers:  []string{FinalizerName},
		},
	}
	if err := r.Create(ctx, cr); err != nil {
		if errors.IsAlreadyExists(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("creating decoupled partition tree: %w", err)
	}
	// Register in cache for node informer — only on actual creation to
	// avoid overwriting the cache with a potentially stale MIG.
	r.Cache.SetPartitionCR(partitionID, partitionID, migSelfLink)
	r.event(cr, corev1.EventTypeNormal, EventReasonCreated,
		"created decoupled PartitionTree %s for MIG %s", partitionID, migSelfLink)
	return ctrl.Result{Requeue: true}, nil
}
