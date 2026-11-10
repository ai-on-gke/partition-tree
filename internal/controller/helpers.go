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
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
	"github.com/ai-on-gke/partition-tree/internal/cache"
	"github.com/ai-on-gke/partition-tree/internal/config"
	"github.com/ai-on-gke/partition-tree/internal/gce"
	"github.com/ai-on-gke/partition-tree/internal/tree"
)

// maxJitter is the upper bound for jitter durations to prevent integer overflow
// when calculating 2*jitter for rand.Int63n.
const maxJitter = time.Duration(math.MaxInt64 / 2)

var logMaxJitterOnce sync.Once

// jitteredInterval returns interval ± a uniform random offset in
// [-jitter, +jitter]. At target scale (1440+ CRs on a 1s cadence) this
// spreads the natural polling tick so the controller does not fan out
// all GCE RPCs in a synchronized burst every interval. A non-positive
// jitter degenerates to the raw interval.
func jitteredInterval(interval, jitter time.Duration) time.Duration {
	if jitter <= 0 {
		return interval
	}
	if jitter > maxJitter {
		logMaxJitterOnce.Do(func() {
			logf.Log.Info("Configured jitter exceeds maximum safe duration; clamping to prevent overflow",
				"configuredJitter", jitter, "clampedJitter", maxJitter)
		})
		jitter = maxJitter
	}
	delta := time.Duration(rand.Int63n(int64(2*jitter))) - jitter
	out := interval + delta
	if out <= 0 {
		return interval
	}
	return out
}

// event is a nil-safe wrapper around r.Recorder.Eventf. Tests that
// don't wire a recorder simply get no events.
func (r *PartitionTreeReconciler) event(obj runtime.Object, eventType, reason, msgFmt string, args ...any) {
	if r.Recorder != nil {
		r.Recorder.Eventf(obj, nil, eventType, reason, reason, msgFmt, args...)
	}
}

// stampAcceleratorType sets the top-level AcceleratorType on the CR
// status. A MIG is homogeneous, so the TPU family is uniform across
// the entire tree — no need to stamp individual partitions. An empty
// family is treated as "no information" and skipped so a missing label
// can never clobber a previously stamped value.
func stampAcceleratorType(status *partitionv1beta1.PartitionTreeStatus, family string) {
	if family == "" {
		return
	}
	status.AcceleratorType = family
}

// carryForwardConditions copies per-partition conditions from the old tree
// to the corresponding partitions in the new tree (matched by PartitionID).
// This preserves LastTransitionTime across tree rebuilds so that
// SetStatusCondition can correctly detect whether the condition status
// actually changed. Without this, rebuilding the tree from GCE wipes
// all conditions, causing LastTransitionTime to reset on every poll.
func carryForwardConditions(old, fresh *partitionv1beta1.PartitionInfo) {
	if old == nil || fresh == nil {
		return
	}
	// Build a lookup from the old tree's partitions by ID.
	oldMap := make(map[string]*partitionv1beta1.PartitionInfo)
	buildConditionMap(old, oldMap)

	// Walk the new tree and copy conditions from matching old partitions.
	applyConditionMap(fresh, oldMap)
}

func buildConditionMap(node *partitionv1beta1.PartitionInfo, m map[string]*partitionv1beta1.PartitionInfo) {
	if node == nil {
		return
	}
	if node.PartitionID != "" {
		m[node.PartitionID] = node
	}
	for i := range node.ChildPartitions {
		buildConditionMap(&node.ChildPartitions[i], m)
	}
}

func applyConditionMap(node *partitionv1beta1.PartitionInfo, oldMap map[string]*partitionv1beta1.PartitionInfo) {
	if node == nil {
		return
	}
	if node.PartitionID != "" {
		if prev, ok := oldMap[node.PartitionID]; ok && len(prev.Conditions) > 0 {
			node.Conditions = make([]metav1.Condition, len(prev.Conditions))
			// Safe: metav1.Condition is all value types, no pointer/slice fields.
			copy(node.Conditions, prev.Conditions)
		}
	}
	for i := range node.ChildPartitions {
		applyConditionMap(&node.ChildPartitions[i], oldMap)
	}
}

// stateToTaint maps a non-healthy PartitionStateType to its corresponding
// PartitionTaint with key cloud.google.com/partition-state.
func stateToTaint(state partitionv1beta1.PartitionStateType) *partitionv1beta1.PartitionTaint {
	switch state {
	case partitionv1beta1.PartitionStateUnhealthy:
		return &partitionv1beta1.PartitionTaint{
			Key:    TaintKeyPartitionState,
			Value:  string(state),
			Effect: partitionv1beta1.TaintEffectNoExecute,
		}
	case partitionv1beta1.PartitionStateDegraded:
		return &partitionv1beta1.PartitionTaint{
			Key:    TaintKeyPartitionState,
			Value:  string(state),
			Effect: partitionv1beta1.TaintEffectPreferNoSchedule,
		}
	case partitionv1beta1.PartitionStateIncomplete:
		return &partitionv1beta1.PartitionTaint{
			Key:    TaintKeyPartitionState,
			Value:  string(state),
			Effect: partitionv1beta1.TaintEffectNoSchedule,
		}
	case partitionv1beta1.PartitionStateUnknown:
		return &partitionv1beta1.PartitionTaint{
			Key:    TaintKeyPartitionState,
			Value:  string(state),
			Effect: partitionv1beta1.TaintEffectNoSchedule,
		}
	default:
		return nil
	}
}

// refreshAndPropagate walks the tree recursively and:
//  1. Resets partition taints to external taints (from annotations) if skipTaintUpdates is false.
//  2. Propagates state taints if PartitionState is enabled.
//  3. For leaf partitions, refreshes NodeNames and the NodeReady condition from the cache
//     snapshot (including the TPU chip check if EnableTPUAllocatableReadiness is set),
//     propagates node taints if NodeTaints is enabled, and propagates the NodesNotFound
//     taint if NodesNotFound is enabled and the condition reason is NodesNotFound.
//  4. Finalizes PartitionTaint.TimeAdded on each partition from prevTaints and now.
//
// This consolidates tree status updates into a single recursive pass. Callers must
// extract prevTaints from the prior Status.Root before any GCE poll rebuilds
// cr.Status.Root so first-seen timestamps carry forward across polls.
func (r *PartitionTreeReconciler) refreshAndPropagate(
	node *partitionv1beta1.PartitionInfo,
	snap cache.LeafSnapshot,
	externalTaints map[string][]partitionv1beta1.PartitionTaint,
	prevTaints map[string][]partitionv1beta1.PartitionTaint,
	now time.Time,
	skipTaintUpdates bool,
	excludedPrefixes []string,
) {
	if node == nil {
		return
	}

	// 1. Reset taints for this node and apply Always/PartitionState rules
	var times *taintTimeResolver
	if !skipTaintUpdates && r.EnableTaints.Enabled {
		times = newTaintTimeResolver(prevTaints[node.PartitionID], now)
	}
	if !skipTaintUpdates {
		if r.EnableTaints.Enabled {
			node.Taints = nil
			if taints, ok := externalTaints[node.PartitionID]; ok {
				node.Taints = make([]partitionv1beta1.PartitionTaint, len(taints))
				for i := range taints {
					node.Taints[i] = *taints[i].DeepCopy()
				}
				times.stamp(node.Taints)
			}
			if r.EnableTaints.PartitionState {
				if st := stateToTaint(node.State); st != nil {
					node.Taints = mergeTaints(node.Taints, times.stamp([]partitionv1beta1.PartitionTaint{*st}))
				}
			}
		} else {
			node.Taints = nil
		}
	}

	// 2. Leaf-specific logic
	if len(node.ChildPartitions) == 0 {
		nodes := snap.Partitions[node.PartitionID]
		node.NodeNames = nodes

		statuses := make([]tree.NodeStatus, 0, len(nodes))
		for _, name := range nodes {
			if ready, ok := snap.Ready[name]; ok {
				st := tree.NodeStatus{Name: name, Ready: ready, Found: true}
				if r.EnableTPUAllocatableReadiness {
					tpu := snap.TPU[name]
					st.TPUAllocatable = tpu.Allocatable
					st.TPUExpected = expectedTPUChipsPerHost(tpu.AcceleratorType)
				}
				statuses = append(statuses, st)
			} else {
				statuses = append(statuses, tree.NodeStatus{Name: name, Found: false})
			}
		}
		cond := tree.DeriveNodeReady(statuses, now)
		meta.SetStatusCondition(&node.Conditions, cond)

		if !skipTaintUpdates && r.EnableTaints.Enabled {
			if r.EnableTaints.NodeTaints && len(nodes) > 0 {
				nodeTaints := unionNodeTaints(nodes, snap.Taints, excludedPrefixes, times.episodeTimeFor)
				node.Taints = mergeTaints(node.Taints, nodeTaints)
			}
			if r.EnableTaints.NodesNotFound && cond.Status == metav1.ConditionFalse && cond.Reason == tree.ReasonNodesNotFound {
				notFoundTaint := []partitionv1beta1.PartitionTaint{{
					Key:    TaintKeyNodesNotFound,
					Effect: partitionv1beta1.TaintEffectNoExecute,
				}}
				node.Taints = mergeTaints(node.Taints, times.stamp(notFoundTaint))
			}
			finalizePartitionTaints(node, times)
		}
		return
	}

	if !skipTaintUpdates && r.EnableTaints.Enabled {
		finalizePartitionTaints(node, times)
	}

	// 3. Recurse on children
	for i := range node.ChildPartitions {
		r.refreshAndPropagate(&node.ChildPartitions[i], snap, externalTaints, prevTaints, now, skipTaintUpdates, excludedPrefixes)
	}
}

// taintTimeResolver supplies TimeAdded for taints whose source did not set one:
// the earliest TimeAdded the same (key, value, effect) had on this partition in
// the previous status, else now (both truncated to RFC3339 second precision).
// Each source is resolved before sources are merged, so a long-present unstamped
// taint keeps its start when another source adds the same taint with a later
// explicit TimeAdded, while an explicit TimeAdded from a single source (e.g. an
// edited annotation) still takes effect.
type taintTimeResolver struct {
	prev map[tree.TaintIdentity]metav1.Time
	now  *metav1.Time
}

func newTaintTimeResolver(prev []partitionv1beta1.PartitionTaint, now time.Time) *taintTimeResolver {
	r := &taintTimeResolver{prev: make(map[tree.TaintIdentity]metav1.Time, len(prev))}
	for i := range prev {
		pt := &prev[i]
		if pt.TimeAdded == nil {
			continue
		}
		ts := metav1.NewTime(pt.TimeAdded.UTC().Truncate(time.Second))
		id := tree.TaintIdentity{Key: pt.Key, Value: pt.Value, Effect: pt.Effect}
		if cur, ok := r.prev[id]; !ok || ts.Before(&cur) {
			r.prev[id] = ts
		}
	}
	if !now.IsZero() {
		ts := metav1.NewTime(now.UTC().Truncate(time.Second))
		r.now = &ts
	}
	return r
}

// episodeTimeFor returns the episode-start TimeAdded for an unstamped taint
// with identity id (carried from the previous status when continuously present,
// else now), or nil if there is neither a previous TimeAdded nor a current time.
func (r *taintTimeResolver) episodeTimeFor(id tree.TaintIdentity) *metav1.Time {
	if r == nil {
		return nil
	}
	if ts, ok := r.prev[id]; ok {
		return ts.DeepCopy()
	}
	if r.now == nil {
		return nil
	}
	return r.now.DeepCopy()
}

// stamp sets TimeAdded on every unstamped taint in ts, in place, and returns ts.
func (r *taintTimeResolver) stamp(ts []partitionv1beta1.PartitionTaint) []partitionv1beta1.PartitionTaint {
	for i := range ts {
		if ts[i].TimeAdded == nil {
			ts[i].TimeAdded = r.episodeTimeFor(tree.TaintIdentity{Key: ts[i].Key, Value: ts[i].Value, Effect: ts[i].Effect})
		}
	}
	return ts
}

// finalizePartitionTaints truncates every TimeAdded on node to RFC3339 second
// precision, so apiserver round-trips do not cause spurious status patches, and
// stamps any taint still lacking one via times. The outage clock of
// node.kubernetes.io/{not-ready,unreachable}:NoExecute is kept per node by the
// node cache (cache.Upsert), so it is never carried across nodes or episodes here.
func finalizePartitionTaints(node *partitionv1beta1.PartitionInfo, times *taintTimeResolver) {
	if node == nil {
		return
	}
	for i := range node.Taints {
		t := &node.Taints[i]
		if t.TimeAdded != nil {
			ts := metav1.NewTime(t.TimeAdded.UTC().Truncate(time.Second))
			t.TimeAdded = &ts
			continue
		}
		t.TimeAdded = times.episodeTimeFor(tree.TaintIdentity{Key: t.Key, Value: t.Value, Effect: t.Effect})
	}
}

// applySynced computes the top-level Synced condition and sets it on
// the CR status (not on individual partitions). The rules are: on
// success, InSync; on failure within the grace window,
// leave the condition untouched; once the grace window elapses,
// OutOfSync.
func (r *PartitionTreeReconciler) applySynced(
	cr *partitionv1beta1.PartitionTree,
	state pollState,
	cfg *config.Config,
	now time.Time,
	pollErr error,
) {
	prev := meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeSynced)
	wasSynced := prev != nil && prev.Status == metav1.ConditionTrue

	timeSinceLastSuccess := time.Duration(0)
	if !state.lastSuccessfulAt.IsZero() {
		timeSinceLastSuccess = now.Sub(state.lastSuccessfulAt)
	}

	switch {
	// Case 1: Never polled successfully and currently failing.
	case state.lastSuccessfulAt.IsZero() && (pollErr != nil || state.lastErr != nil):
		detail := "awaiting first successful poll"
		if pollErr != nil {
			detail += fmt.Sprintf(": %v", pollErr)
		} else if state.lastErr != nil {
			detail += fmt.Sprintf(": %v", state.lastErr)
		}
		meta.SetStatusCondition(&cr.Status.Conditions, tree.DeriveSynced(false, now, detail))

	// Case 2: Out of sync due to timeout.
	// This applies whether we just polled and failed, or skipped polling!
	case !state.lastSuccessfulAt.IsZero() && timeSinceLastSuccess > cfg.SyncTimeout.D():
		detail := fmt.Sprintf("no successful poll within %s", cfg.SyncTimeout.D())
		if pollErr != nil {
			detail += fmt.Sprintf(": %v", pollErr)
		}
		meta.SetStatusCondition(&cr.Status.Conditions, tree.DeriveSynced(false, now, detail))
		if wasSynced {
			r.event(cr, corev1.EventTypeWarning, EventReasonSyncLost, "GCE sync lost: %s", detail)
		}

	// Case 3: Successful poll in this reconcile cycle!
	case state.lastSuccessfulAt.Equal(now):
		meta.SetStatusCondition(&cr.Status.Conditions,
			tree.DeriveSynced(true, now, "in sync with GCE"))
		if !wasSynced {
			r.event(cr, corev1.EventTypeNormal, EventReasonSyncRestored, "GCE sync restored")
		}

	// Default Case: Within grace window or non-polling reconcile.
	default:
		// DO NOTHING. Leaves the condition exactly as it was previously.
	}
}

// validateTaintTargets checks if any taints in the map are targeted at partition IDs
// that do not exist in the GCE topology response. If so, it emits a Warning event.
func (r *PartitionTreeReconciler) validateTaintTargets(
	cr *partitionv1beta1.PartitionTree,
	resp *gce.TopologyResponse,
	taints map[string][]partitionv1beta1.PartitionTaint,
) {
	validIDs := make(map[string]bool)
	for _, e := range resp.Entries {
		validIDs[e.PartitionID] = true
	}
	for id := range taints {
		if !validIDs[id] {
			r.event(cr, corev1.EventTypeWarning, EventReasonUnusedTaint,
				"Taint targeted at non-existent partition %q", id)
		}
	}
}

// unionNodeTaints returns the union of taints across all specified nodes,
// keeping the earliest TimeAdded per (key, value, effect). A node taint without
// a TimeAdded takes episodeTimeFor(id) (when episodeTimeFor is non-nil) before
// the earliest is picked, so it is not overridden by a later explicit TimeAdded
// on another node.
//
// Note: If a node exists in the partition but has no entry in allTaints (e.g.,
// a newly joined node, or a node with no taints, as PoolLeaves only populates
// the map for nodes with len(taints) > 0), allTaints[node] will be nil.
// unionNodeTaints safely ignores these nodes (they contribute nothing to the
// union) rather than clearing the union.
func unionNodeTaints(nodes []string, allTaints map[string][]corev1.Taint, excludedPrefixes []string,
	episodeTimeFor func(tree.TaintIdentity) *metav1.Time) []partitionv1beta1.PartitionTaint {
	if len(nodes) == 0 {
		return nil
	}

	isExcluded := func(key string) bool {
		for _, prefix := range excludedPrefixes {
			if strings.HasPrefix(key, prefix) {
				return true
			}
		}
		return false
	}

	unionMap := make(map[tree.TaintIdentity]*metav1.Time)
	seen := make(map[tree.TaintIdentity]bool)
	for _, node := range nodes {
		for _, t := range allTaints[node] {
			if !isExcluded(t.Key) {
				id := tree.TaintIdentity{
					Key:    t.Key,
					Value:  t.Value,
					Effect: toPartitionTaintEffect(t.Effect, node, t.Key),
				}
				seen[id] = true
				var ts *metav1.Time
				if t.TimeAdded != nil {
					v := metav1.NewTime(t.TimeAdded.UTC().Truncate(time.Second))
					ts = &v
				} else if episodeTimeFor != nil {
					ts = episodeTimeFor(id)
				}
				if ts != nil {
					if prev := unionMap[id]; prev == nil || ts.Before(prev) {
						unionMap[id] = ts
					}
				}
			}
		}
	}

	if len(seen) == 0 {
		return nil
	}

	out := make([]partitionv1beta1.PartitionTaint, 0, len(seen))
	for id := range seen {
		out = append(out, partitionv1beta1.PartitionTaint{
			Key:       id.Key,
			Value:     id.Value,
			Effect:    id.Effect,
			TimeAdded: unionMap[id],
		})
	}

	slices.SortFunc(out, tree.ComparePartitionTaints)

	return out
}

func toPartitionTaintEffect(effect corev1.TaintEffect, nodeName, taintKey string) partitionv1beta1.TaintEffect {
	switch effect {
	case corev1.TaintEffectNoSchedule:
		return partitionv1beta1.TaintEffectNoSchedule
	case corev1.TaintEffectPreferNoSchedule:
		return partitionv1beta1.TaintEffectPreferNoSchedule
	case corev1.TaintEffectNoExecute:
		return partitionv1beta1.TaintEffectNoExecute
	default:
		logf.Log.Info("Unrecognized taint effect encountered, mapping to NoSchedule", "effect", effect, "node", nodeName, "taintKey", taintKey)
		return partitionv1beta1.TaintEffectNoSchedule
	}
}

func mergeTaints(a, b []partitionv1beta1.PartitionTaint) []partitionv1beta1.PartitionTaint {
	if len(a) == 0 && len(b) == 0 {
		return nil
	}

	seenIdx := make(map[tree.TaintIdentity]int)
	out := make([]partitionv1beta1.PartitionTaint, 0, len(a)+len(b))

	addUnique := func(slice []partitionv1beta1.PartitionTaint) {
		for i := range slice {
			t := &slice[i]
			id := tree.TaintIdentity{Key: t.Key, Value: t.Value, Effect: t.Effect}
			idx, exists := seenIdx[id]
			if !exists {
				seenIdx[id] = len(out)
				cp := *t.DeepCopy()
				if cp.TimeAdded != nil {
					ts := metav1.NewTime(cp.TimeAdded.UTC().Truncate(time.Second))
					cp.TimeAdded = &ts
				}
				out = append(out, cp)
				continue
			}
			if t.TimeAdded != nil {
				ts := metav1.NewTime(t.TimeAdded.UTC().Truncate(time.Second))
				if out[idx].TimeAdded == nil || ts.Before(out[idx].TimeAdded) {
					out[idx].TimeAdded = &ts
				}
			}
		}
	}

	addUnique(a)
	addUnique(b)

	slices.SortFunc(out, tree.ComparePartitionTaints)

	return out
}

// rootPartitionIDForNode extracts the root partition ID from a node's
// labels based on its accelerator type. Returns an error if the
// accelerator type is unsupported or the root label is missing.
func rootPartitionIDForNode(acceleratorType string, labels map[string]string) (string, error) {
	labelKey, ok := topologyRootLabel[acceleratorType]
	if !ok {
		return "", fmt.Errorf("unsupported accelerator type %q for decoupled lifecycle", acceleratorType)
	}
	id := labels[labelKey]
	if id == "" {
		return "", fmt.Errorf("root partition label %q is missing or empty", labelKey)
	}
	return id, nil
}

// sanitizePartitionID normalises a raw partition ID into a valid DNS
// label (RFC 1123): lowercase, alphanumeric or hyphens, no leading/
// trailing hyphens, max 63 characters.
func sanitizePartitionID(raw string) (string, error) {
	if raw == "" {
		return "", fmt.Errorf("partition ID must not be empty")
	}

	// Lowercase.
	result := make([]byte, 0, len(raw))
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		switch {
		case c >= 'A' && c <= 'Z':
			result = append(result, c+32) // toLower
		case (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'):
			result = append(result, c)
		default:
			result = append(result, '-')
		}
	}

	// Trim leading/trailing hyphens.
	s := string(result)
	for len(s) > 0 && s[0] == '-' {
		s = s[1:]
	}
	for len(s) > 0 && s[len(s)-1] == '-' {
		s = s[:len(s)-1]
	}

	if len(s) == 0 {
		return "", fmt.Errorf("partition ID %q sanitizes to empty string", raw)
	}
	if len(s) > 63 {
		return "", fmt.Errorf("partition ID %q exceeds 63 characters after sanitization (%d)", raw, len(s))
	}
	return s, nil
}
