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
	"reflect"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
)

func TestExtractTaintsFromAnnotations(t *testing.T) {
	tests := []struct {
		name           string
		annotations    map[string]string
		want           map[string][]partitionv1beta1.PartitionTaint
		wantErr        bool
		wantErrSubstrs []string
	}{
		{
			name:        "nil annotations",
			annotations: nil,
			want:        map[string][]partitionv1beta1.PartitionTaint{},
			wantErr:     false,
		},
		{
			name:        "empty annotations",
			annotations: map[string]string{},
			want:        map[string][]partitionv1beta1.PartitionTaint{},
			wantErr:     false,
		},
		{
			name: "no matching annotations",
			annotations: map[string]string{
				"foo": "bar",
			},
			want:    map[string][]partitionv1beta1.PartitionTaint{},
			wantErr: false,
		},
		{
			name: "malformed json",
			annotations: map[string]string{
				"taints.partitiontree.gke.io/t1": `{"partitionId":"p1", "key": "k"`,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "valid taints",
			annotations: map[string]string{
				"taints.partitiontree.gke.io/t1": `{"partitionId":"p1", "key":"maintenance", "effect":"NoSchedule"}`,
				"taints.partitiontree.gke.io/t2": `{"partitionId":"p2", "key":"repair", "effect":"NoExecute"}`,
			},
			want: map[string][]partitionv1beta1.PartitionTaint{
				"p1": {
					{Key: "maintenance", Effect: partitionv1beta1.TaintEffectNoSchedule},
				},
				"p2": {
					{Key: "repair", Effect: partitionv1beta1.TaintEffectNoExecute},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid effect",
			annotations: map[string]string{
				"taints.partitiontree.gke.io/t1": `{"partitionId":"p1", "key":"maintenance", "effect":"UnknownEffect"}`,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "mixed valid and invalid taints",
			annotations: map[string]string{
				"taints.partitiontree.gke.io/t1": `{"partitionId":"p1", "key":"maintenance", "effect":"NoSchedule"}`,
				"taints.partitiontree.gke.io/t2": `{"partitionId":"p2", "key":"repair", "effect":"UnknownEffect"}`,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "duplicate taints on same partition (same key/effect)",
			annotations: map[string]string{
				"taints.partitiontree.gke.io/t1": `{"partitionId":"p1", "key":"maintenance", "effect":"NoSchedule"}`,
				"taints.partitiontree.gke.io/t2": `{"partitionId":"p1", "key":"maintenance", "effect":"NoSchedule"}`,
			},
			want: map[string][]partitionv1beta1.PartitionTaint{
				"p1": {
					{Key: "maintenance", Effect: partitionv1beta1.TaintEffectNoSchedule},
				},
			},
			wantErr: false,
		},
		{
			name: "duplicate taints with different values on same partition",
			annotations: map[string]string{
				"taints.partitiontree.gke.io/t1": `{"partitionId":"p1", "key":"maintenance", "value":"v1", "effect":"NoSchedule"}`,
				"taints.partitiontree.gke.io/t2": `{"partitionId":"p1", "key":"maintenance", "value":"v2", "effect":"NoSchedule"}`,
			},
			want: map[string][]partitionv1beta1.PartitionTaint{
				"p1": {
					{Key: "maintenance", Value: "v1", Effect: partitionv1beta1.TaintEffectNoSchedule},
					{Key: "maintenance", Value: "v2", Effect: partitionv1beta1.TaintEffectNoSchedule},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid taint key (malformed)",
			annotations: map[string]string{
				"taints.partitiontree.gke.io/t1": `{"partitionId":"p1", "key":"invalid_key/", "effect":"NoSchedule"}`,
			},
			want:    nil,
			wantErr: true,
			wantErrSubstrs: []string{
				"must consist of alphanumeric characters",
			},
		},
		{
			name: "invalid taint key (too long)",
			annotations: map[string]string{
				"taints.partitiontree.gke.io/t1": `{"partitionId":"p1", "key":"` + strings.Repeat("a", 254) + `", "effect":"NoSchedule"}`,
			},
			want:    nil,
			wantErr: true,
			wantErrSubstrs: []string{
				"must be no more than 63 bytes",
			},
		},
		{
			name: "invalid taint value",
			annotations: map[string]string{
				"taints.partitiontree.gke.io/t1": `{"partitionId":"p1", "key":"maintenance", "value":"toolong` + strings.Repeat("a", 60) + `", "effect":"NoSchedule"}`,
			},
			want:    nil,
			wantErr: true,
			wantErrSubstrs: []string{
				"must be no more than 63 bytes",
			},
		},
		{
			name: "missing partitionId",
			annotations: map[string]string{
				"taints.partitiontree.gke.io/t1": `{"key":"maintenance", "effect":"NoSchedule"}`,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "taints are sorted deterministically",
			annotations: map[string]string{
				"taints.partitiontree.gke.io/t1": `{"partitionId":"p1", "key":"z", "effect":"NoSchedule"}`,
				"taints.partitiontree.gke.io/t2": `{"partitionId":"p1", "key":"a", "effect":"NoSchedule"}`,
				"taints.partitiontree.gke.io/t3": `{"partitionId":"p1", "key":"m", "effect":"NoSchedule"}`,
			},
			want: map[string][]partitionv1beta1.PartitionTaint{
				"p1": {
					{Key: "a", Effect: partitionv1beta1.TaintEffectNoSchedule},
					{Key: "m", Effect: partitionv1beta1.TaintEffectNoSchedule},
					{Key: "z", Effect: partitionv1beta1.TaintEffectNoSchedule},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExtractTaintsFromAnnotations(tt.annotations)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractTaintsFromAnnotations() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && len(tt.wantErrSubstrs) > 0 {
				for _, substr := range tt.wantErrSubstrs {
					if !strings.Contains(err.Error(), substr) {
						t.Errorf("ExtractTaintsFromAnnotations() error %q missing expected substring %q", err.Error(), substr)
					}
				}
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExtractTaintsFromAnnotations() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractTaintsFromAnnotations_DuplicateKeepsEarliestTimeAddedDeterministically(t *testing.T) {
	want := metav1.NewTime(time.Date(2026, 11, 3, 12, 1, 0, 0, time.UTC))
	annotations := map[string]string{
		"taints.partitiontree.gke.io/a-later":   `{"partitionId":"p1","key":"node.kubernetes.io/not-ready","effect":"NoExecute","timeAdded":"2026-11-03T12:05:00Z"}`,
		"taints.partitiontree.gke.io/b-earlier": `{"partitionId":"p1","key":"node.kubernetes.io/not-ready","effect":"NoExecute","timeAdded":"2026-11-03T12:01:00Z"}`,
		"taints.partitiontree.gke.io/c-nil":     `{"partitionId":"p1","key":"node.kubernetes.io/not-ready","effect":"NoExecute"}`,
	}
	// Run multiple iterations across randomized Go map orderings to prove determinism.
	for i := range 50 {
		got, err := ExtractTaintsFromAnnotations(annotations)
		if err != nil {
			t.Fatalf("iter %d: unexpected error: %v", i, err)
		}
		if len(got["p1"]) != 1 || got["p1"][0].TimeAdded == nil || !got["p1"][0].TimeAdded.Equal(&want) {
			t.Fatalf("iter %d: ExtractTaintsFromAnnotations() = %+v, want earliest TimeAdded %v", i, got["p1"], want)
		}
	}
}
