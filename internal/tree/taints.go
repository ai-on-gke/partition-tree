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
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

// TaintIdentity represents a unique taint by its key, value, and effect.
// It is used to de-duplicate taints.
type TaintIdentity struct {
	Key    string
	Value  string
	Effect partitionv1beta1.TaintEffect
}

// TargetedTaint represents a taint targeted at a specific partition.
// It is used to parse the external taints annotation.
type TargetedTaint struct {
	PartitionID                     string `json:"partitionId"`
	partitionv1beta1.PartitionTaint `json:",inline"`
}

// TaintAnnotationPrefix is the prefix for annotations that define taints.
const TaintAnnotationPrefix = "taints.partitiontree.gke.io/"

// ExtractTaintsFromAnnotations scans the annotations map for keys starting with
// TaintAnnotationPrefix, parses their values as single TargetedTaint objects,
// and returns a map of PartitionID -> []partitionv1beta1.PartitionTaint.
// If any matching annotation is malformed or invalid, it returns an error.
func ExtractTaintsFromAnnotations(annotations map[string]string) (map[string][]partitionv1beta1.PartitionTaint, error) {
	taintsMap := make(map[string][]partitionv1beta1.PartitionTaint)
	seenIdx := make(map[string]map[TaintIdentity]int)

	for key, val := range annotations {
		if !strings.HasPrefix(key, TaintAnnotationPrefix) {
			continue
		}

		var pt TargetedTaint
		if err := json.Unmarshal([]byte(val), &pt); err != nil {
			return nil, fmt.Errorf("failed to parse taint annotation %q: %w", key, err)
		}

		if pt.PartitionID == "" {
			return nil, fmt.Errorf("missing partitionId in taint annotation %q", key)
		}
		if !isValidTaintEffect(pt.Effect) {
			return nil, fmt.Errorf("invalid taint effect %q for partition %q in annotation %q", pt.Effect, pt.PartitionID, key)
		}
		if err := validateTaintKey(pt.Key); err != nil {
			return nil, fmt.Errorf("invalid taint key %q for partition %q in annotation %q: %w", pt.Key, pt.PartitionID, key, err)
		}
		if err := validateTaintValue(pt.Value); err != nil {
			return nil, fmt.Errorf("invalid taint value %q for partition %q in annotation %q: %w", pt.Value, pt.PartitionID, key, err)
		}
		if pt.TimeAdded != nil {
			ts := metav1.NewTime(pt.TimeAdded.UTC().Truncate(time.Second))
			pt.TimeAdded = &ts
		}

		if seenIdx[pt.PartitionID] == nil {
			seenIdx[pt.PartitionID] = make(map[TaintIdentity]int)
		}
		identity := TaintIdentity{Key: pt.Key, Value: pt.Value, Effect: pt.Effect}
		idx, exists := seenIdx[pt.PartitionID][identity]
		if !exists {
			seenIdx[pt.PartitionID][identity] = len(taintsMap[pt.PartitionID])
			// TargetedTaint embeds PartitionTaint and has no DeepCopy of its own, so this is
			// the promoted (*PartitionTaint).DeepCopy (copies TimeAdded too).
			taintsMap[pt.PartitionID] = append(taintsMap[pt.PartitionID], *pt.DeepCopy())
		} else if pt.TimeAdded != nil {
			cur := taintsMap[pt.PartitionID][idx].TimeAdded
			if cur == nil || pt.TimeAdded.Before(cur) {
				taintsMap[pt.PartitionID][idx].TimeAdded = pt.TimeAdded.DeepCopy()
			}
		}
	}

	// Sort the taints for each partition deterministically to avoid
	// non-deterministic status updates.
	for partID := range taintsMap {
		slices.SortFunc(taintsMap[partID], ComparePartitionTaints)
	}

	return taintsMap, nil
}

// ComparePartitionTaints compares two PartitionTaints by Key, Value, and Effect.
// It returns:
//   - a negative number if a < b
//   - 0 if a == b
//   - a positive number if a > b
func ComparePartitionTaints(a, b partitionv1beta1.PartitionTaint) int {
	if c := cmp.Compare(a.Key, b.Key); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Value, b.Value); c != 0 {
		return c
	}
	return cmp.Compare(string(a.Effect), string(b.Effect))
}

func isValidTaintEffect(effect partitionv1beta1.TaintEffect) bool {
	switch effect {
	case partitionv1beta1.TaintEffectNoSchedule,
		partitionv1beta1.TaintEffectPreferNoSchedule,
		partitionv1beta1.TaintEffectNoExecute:
		return true
	default:
		return false
	}
}

func validateTaintKey(key string) error {
	if errs := validation.IsQualifiedName(key); len(errs) > 0 {
		return fmt.Errorf("invalid key: %s", strings.Join(errs, "; "))
	}
	return nil
}

func validateTaintValue(value string) error {
	if value == "" {
		return nil
	}
	if errs := validation.IsValidLabelValue(value); len(errs) > 0 {
		return fmt.Errorf("invalid value: %s", strings.Join(errs, "; "))
	}
	return nil
}

// ExtractTaints walks the tree and collects all taints, returning a map of PartitionID -> []PartitionTaint.
func ExtractTaints(root *partitionv1beta1.PartitionInfo) map[string][]partitionv1beta1.PartitionTaint {
	if root == nil {
		return nil
	}
	taintsMap := make(map[string][]partitionv1beta1.PartitionTaint)
	var walk func(p *partitionv1beta1.PartitionInfo)
	walk = func(p *partitionv1beta1.PartitionInfo) {
		if len(p.Taints) > 0 {
			// Deep-copy taints (including TimeAdded pointers) to avoid sharing mutated state.
			taints := make([]partitionv1beta1.PartitionTaint, len(p.Taints))
			for i := range p.Taints {
				taints[i] = *p.Taints[i].DeepCopy()
			}
			taintsMap[p.PartitionID] = taints
		}
		for i := range p.ChildPartitions {
			walk(&p.ChildPartitions[i])
		}
	}
	walk(root)
	if len(taintsMap) == 0 {
		return nil
	}
	return taintsMap
}
