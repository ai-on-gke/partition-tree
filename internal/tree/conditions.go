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
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
)

// Condition reason strings used by this package. ReasonTPUsNotAllocatable
// is only produced with --enable-tpu-allocatable-readiness: every node is
// found and kubelet-Ready, but at least one reports fewer google.com/tpu
// chips allocatable than its accelerator provides.
const (
	ReasonNodesReady         = "NodesReady"
	ReasonNodesNotReady      = "NodesNotReady"
	ReasonNodesNotFound      = "NodesNotFound"
	ReasonTPUsNotAllocatable = "TPUsNotAllocatable"
	ReasonInSync             = "InSync"
	ReasonOutOfSync          = "OutOfSync"
	ReasonTaintsValid        = "TaintsValid"
	ReasonTaintParseFailed   = "TaintParseFailed"
)

// NodeStatus is the minimal per-node view the conditions derivation
// needs. Found=false means "expected by this leaf but absent from the
// node informer cache." TPUExpected is the google.com/tpu allocatable a
// fully healthy node reports; 0 disables the chip check for that node.
type NodeStatus struct {
	Name           string
	Ready          bool
	Found          bool
	TPUAllocatable int64
	TPUExpected    int64
}

// DeriveNodeReady computes the NodeReady condition for a leaf partition
// from the cached status of every node that leaf is expected to own.
// Precedence: missing > not-ready > TPUs-not-allocatable > all-ready.
// The chip check only applies to found, Ready nodes with TPUExpected > 0.
func DeriveNodeReady(nodes []NodeStatus, now time.Time) metav1.Condition {
	cond := metav1.Condition{
		Type:               partitionv1beta1.ConditionTypeNodeReady,
		LastTransitionTime: metav1.NewTime(now),
	}
	if len(nodes) == 0 {
		cond.Status = metav1.ConditionFalse
		cond.Reason = ReasonNodesNotFound
		cond.Message = "no nodes assigned to this partition"
		return cond
	}

	var missing, notReady, notAllocatable []string
	for _, n := range nodes {
		if !n.Found {
			missing = append(missing, n.Name)
			continue
		}
		if !n.Ready {
			notReady = append(notReady, n.Name)
			continue
		}
		if n.TPUExpected > 0 && n.TPUAllocatable < n.TPUExpected {
			notAllocatable = append(notAllocatable,
				fmt.Sprintf("%s (%d/%d)", n.Name, n.TPUAllocatable, n.TPUExpected))
		}
	}
	switch {
	case len(missing) > 0:
		cond.Status = metav1.ConditionFalse
		cond.Reason = ReasonNodesNotFound
		cond.Message = fmt.Sprintf("nodes not found in cache: %v", missing)
	case len(notReady) > 0:
		cond.Status = metav1.ConditionFalse
		cond.Reason = ReasonNodesNotReady
		cond.Message = fmt.Sprintf("nodes not ready: %v", notReady)
	case len(notAllocatable) > 0:
		cond.Status = metav1.ConditionFalse
		cond.Reason = ReasonTPUsNotAllocatable
		cond.Message = fmt.Sprintf("nodes with TPU chips not allocatable: %v", notAllocatable)
	default:
		cond.Status = metav1.ConditionTrue
		cond.Reason = ReasonNodesReady
		cond.Message = "all expected nodes are Ready"
	}
	return cond
}

// DeriveSynced produces the top-level Synced condition. It is the only
// place where GCE-reachability degradation is expressed — State stays
// at its last-known value on transient errors.
func DeriveSynced(inSync bool, now time.Time, detail string) metav1.Condition {
	cond := metav1.Condition{
		Type:               partitionv1beta1.ConditionTypeSynced,
		LastTransitionTime: metav1.NewTime(now),
		Message:            detail,
	}
	if inSync {
		cond.Status = metav1.ConditionTrue
		cond.Reason = ReasonInSync
	} else {
		cond.Status = metav1.ConditionFalse
		cond.Reason = ReasonOutOfSync
	}
	return cond
}

// DeriveTaintsValid produces the TaintsValid condition.
func DeriveTaintsValid(valid bool, now time.Time, detail string) metav1.Condition {
	cond := metav1.Condition{
		Type:               partitionv1beta1.ConditionTypeTaintsValid,
		LastTransitionTime: metav1.NewTime(now),
		Message:            detail,
	}
	if valid {
		cond.Status = metav1.ConditionTrue
		cond.Reason = ReasonTaintsValid
	} else {
		cond.Status = metav1.ConditionFalse
		cond.Reason = ReasonTaintParseFailed
	}
	return cond
}
