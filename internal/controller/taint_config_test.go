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
	"testing"
)

func TestTaintConfig_Set(t *testing.T) {
	tests := []struct {
		name    string
		val     string
		want    TaintConfig
		wantErr bool
	}{
		{
			name: "empty string defaults to All",
			val:  "",
			want: TaintConfig{Enabled: true, NodeTaints: true, NodesNotFound: true, PartitionState: true},
		},
		{
			name: "true defaults to All",
			val:  "true",
			want: TaintConfig{Enabled: true, NodeTaints: true, NodesNotFound: true, PartitionState: true},
		},
		{
			name: "all (case insensitive)",
			val:  "ALL",
			want: TaintConfig{Enabled: true, NodeTaints: true, NodesNotFound: true, PartitionState: true},
		},
		{
			name: "false disables taints",
			val:  "false",
			want: TaintConfig{},
		},
		{
			name: "0 disables taints",
			val:  "0",
			want: TaintConfig{},
		},
		{
			name: "single option NodeTaints",
			val:  "NodeTaints",
			want: TaintConfig{Enabled: true, NodeTaints: true},
		},
		{
			name: "comma-separated NodeTaints,NodesNotFound",
			val:  "NodeTaints,NodesNotFound",
			want: TaintConfig{Enabled: true, NodeTaints: true, NodesNotFound: true},
		},
		{
			name: "comma-separated with whitespace",
			val:  " NodeTaints , PartitionState ",
			want: TaintConfig{Enabled: true, NodeTaints: true, PartitionState: true},
		},
		{
			name: "single option PartitionState",
			val:  "PartitionState",
			want: TaintConfig{Enabled: true, PartitionState: true},
		},
		{
			name:    "invalid option",
			val:     "InvalidOption",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg TaintConfig
			err := cfg.Set(tt.val)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Set(%q) error = %v, wantErr %v", tt.val, err, tt.wantErr)
			}
			if !tt.wantErr && cfg != tt.want {
				t.Errorf("Set(%q) = %+v, want %+v", tt.val, cfg, tt.want)
			}
		})
	}
}

func TestTaintConfig_String(t *testing.T) {
	tests := []struct {
		name string
		cfg  TaintConfig
		want string
	}{
		{
			name: "disabled",
			cfg:  TaintConfig{},
			want: "false",
		},
		{
			name: "all enabled",
			cfg:  TaintConfig{Enabled: true, NodeTaints: true, NodesNotFound: true, PartitionState: true},
			want: "All",
		},
		{
			name: "subset enabled",
			cfg:  TaintConfig{Enabled: true, NodeTaints: true, NodesNotFound: true},
			want: "NodeTaints,NodesNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.String(); got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}
