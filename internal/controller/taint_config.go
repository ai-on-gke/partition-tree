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
	"strings"
)

// TaintConfig represents the configuration for the --enable-taints flag.
// It supports either a boolean (true = All, false = disabled) or a
// comma-separated list of feature options (NodeTaints, NodesNotFound, PartitionState).
type TaintConfig struct {
	Enabled        bool
	NodeTaints     bool
	NodesNotFound  bool
	PartitionState bool
}

// NewTaintConfig returns a TaintConfig. If enabled is true and no options
// are specified, all taint features default to enabled ("Unset values: All").
func NewTaintConfig(enabled bool, options ...string) TaintConfig {
	if !enabled {
		return TaintConfig{}
	}
	if len(options) == 0 {
		return TaintConfig{
			Enabled:        true,
			NodeTaints:     true,
			NodesNotFound:  true,
			PartitionState: true,
		}
	}
	cfg := TaintConfig{Enabled: true}
	for _, opt := range options {
		_ = cfg.Set(opt)
	}
	return cfg
}

// Set parses the flag value into the TaintConfig.
func (t *TaintConfig) Set(val string) error {
	val = strings.TrimSpace(val)
	if val == "" || strings.EqualFold(val, "true") || strings.EqualFold(val, TaintOptionAll) {
		*t = NewTaintConfig(true)
		return nil
	}
	if strings.EqualFold(val, "false") || val == "0" || strings.EqualFold(val, "no") {
		*t = TaintConfig{}
		return nil
	}

	cfg := TaintConfig{Enabled: true}
	parts := strings.Split(val, ",")
	for _, part := range parts {
		p := strings.TrimSpace(part)
		switch strings.ToLower(p) {
		case strings.ToLower(TaintOptionNodeTaints):
			cfg.NodeTaints = true
		case strings.ToLower(TaintOptionNodesNotFound):
			cfg.NodesNotFound = true
		case strings.ToLower(TaintOptionPartitionState):
			cfg.PartitionState = true
		case strings.ToLower(TaintOptionAll):
			cfg.NodeTaints = true
			cfg.NodesNotFound = true
			cfg.PartitionState = true
		case "":
			// ignore empty items from leading/trailing commas or spaces
		default:
			return fmt.Errorf("invalid value %q for --enable-taints: valid options are All, NodeTaints, NodesNotFound, PartitionState (or true/false)", p)
		}
	}
	*t = cfg
	return nil
}

// String returns a string representation of the enabled taint options.
func (t *TaintConfig) String() string {
	if !t.Enabled {
		return "false"
	}
	if t.NodeTaints && t.NodesNotFound && t.PartitionState {
		return TaintOptionAll
	}
	var opts []string
	if t.NodeTaints {
		opts = append(opts, TaintOptionNodeTaints)
	}
	if t.NodesNotFound {
		opts = append(opts, TaintOptionNodesNotFound)
	}
	if t.PartitionState {
		opts = append(opts, TaintOptionPartitionState)
	}
	if len(opts) == 0 {
		return "true"
	}
	return strings.Join(opts, ",")
}

// IsBoolFlag allows --enable-taints to be passed without an argument, defaulting to "true" (All).
func (t *TaintConfig) IsBoolFlag() bool {
	return true
}

// Type returns the flag type name.
func (t *TaintConfig) Type() string {
	return "string"
}
