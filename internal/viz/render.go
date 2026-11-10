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

// Package viz renders PartitionTree CRs as human-readable ASCII trees
// and fleet summary tables. All functions are pure — they take API types
// and return strings, with no Kubernetes client dependency.
package viz

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
)

// ANSI color codes for terminal output.
const (
	colorReset  = "\033[0m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorRed    = "\033[31m"
	colorCyan   = "\033[36m"
	colorGray   = "\033[90m"
	colorBold   = "\033[1m"
)

// RenderTree produces a multi-line ASCII tree for a single PartitionTree CR.
// When color is true, ANSI escape codes are used to highlight state.
//
// Example output:
//
//	pool-0  (v7x)  Synced=True
//	└── root [4x4x4] Healthy ■ Occupied
//	    ├── p-0 [2x2x4] Healthy □ Empty
//	    │   ├── leaf-0 [2x2x1] Healthy □ Empty  nodes=[n1,n2] Ready
//	    │   └── leaf-1 [2x2x1] Degraded □ Empty  nodes=[n3,n4] NotReady
//	    └── p-1 [2x2x4] Healthy ■ Occupied
//	        ├── leaf-2 [2x2x1] Healthy ■ Occupied  nodes=[n5,n6] Ready
//	        └── leaf-3 [2x2x1] Healthy ■ Occupied  nodes=[n7,n8] Ready
func RenderTree(cr *partitionv1beta1.PartitionTree, color bool) string {
	var b strings.Builder

	// Header line: pool name + accelerator type + Synced status.
	header := cr.Name
	if cr.Status.AcceleratorType != "" {
		header += "  (" + cr.Status.AcceleratorType + ")"
	}
	synced := syncedStatus(cr)
	header += "  " + synced

	if color {
		b.WriteString(colorBold + header + colorReset + "\n")
	} else {
		b.WriteString(header + "\n")
	}

	if cr.Status.Root != nil {
		renderNode(&b, cr.Status.Root, "", true, color)
	} else {
		b.WriteString("└── [No tree data available]\n")
	}
	return b.String()
}

func renderNode(b *strings.Builder, p *partitionv1beta1.PartitionInfo, prefix string, isLast bool, color bool) {
	// Connector characters.
	connector := "├── "
	childPrefix := "│   "
	if isLast {
		connector = "└── "
		childPrefix = "    "
	}

	// Build the line: partitionID [topology] State <occupancy> [nodes=... NodeReady]
	var line strings.Builder
	line.WriteString(prefix + connector)

	// Partition ID
	line.WriteString(p.PartitionID)

	// Topology
	if p.Topology != "" {
		line.WriteString(" [" + p.Topology + "]")
	}

	// State with color
	stateStr := string(p.State)
	if color {
		line.WriteString(" " + stateColor(p.State) + stateStr + colorReset)
	} else {
		line.WriteString(" " + stateStr)
	}

	// Occupancy indicator
	occStr := occupancyIndicator(p.Occupancy)
	if color {
		line.WriteString(" " + occupancyColor(p.Occupancy) + occStr + colorReset)
	} else {
		line.WriteString(" " + occStr)
	}

	// Taints
	if len(p.Taints) > 0 {
		var taintStrs []string
		for _, t := range p.Taints {
			if t.Value != "" {
				taintStrs = append(taintStrs, fmt.Sprintf("%s=%s:%s", t.Key, t.Value, t.Effect))
			} else {
				taintStrs = append(taintStrs, fmt.Sprintf("%s:%s", t.Key, t.Effect))
			}
		}
		taintLine := " [Taints: " + strings.Join(taintStrs, ", ") + "]"
		if color {
			line.WriteString(" " + colorYellow + taintLine + colorReset)
		} else {
			line.WriteString(" " + taintLine)
		}
	}

	// Leaf: show node names and NodeReady condition
	if len(p.ChildPartitions) == 0 {
		if len(p.NodeNames) > 0 {
			nodeList := strings.Join(p.NodeNames, ",")
			if len(nodeList) > 60 {
				nodeList = nodeList[:57] + "..."
			}
			line.WriteString("  nodes=[" + nodeList + "]")
		}
		ready := nodeReadyStatus(p)
		if ready != "" {
			if color {
				line.WriteString(" " + nodeReadyColor(ready) + ready + colorReset)
			} else {
				line.WriteString(" " + ready)
			}
		}
	}

	b.WriteString(line.String() + "\n")

	// Recurse into children.
	newPrefix := prefix + childPrefix
	for i := range p.ChildPartitions {
		renderNode(b, &p.ChildPartitions[i], newPrefix, i == len(p.ChildPartitions)-1, color)
	}
}

// occupancyIndicator returns a compact visual for occupancy.
func occupancyIndicator(o partitionv1beta1.OccupancyState) string {
	switch o {
	case partitionv1beta1.OccupancyOccupied:
		return "[Occupied]"
	case partitionv1beta1.OccupancyEmpty:
		return "[Empty]"
	default:
		return "[?]"
	}
}

// syncedStatus extracts the Synced condition status string from the
// top-level CR conditions (not from any individual partition).
func syncedStatus(cr *partitionv1beta1.PartitionTree) string {
	c := meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeSynced)
	if c == nil {
		return "Synced=Unknown"
	}
	return fmt.Sprintf("Synced=%s", c.Status)
}

// nodeReadyStatus extracts a short NodeReady label from a leaf.
func nodeReadyStatus(p *partitionv1beta1.PartitionInfo) string {
	c := meta.FindStatusCondition(p.Conditions, partitionv1beta1.ConditionTypeNodeReady)
	if c == nil {
		return ""
	}
	return c.Reason
}

// Color helpers.

func stateColor(s partitionv1beta1.PartitionStateType) string {
	switch s {
	case partitionv1beta1.PartitionStateHealthy:
		return colorGreen
	case partitionv1beta1.PartitionStateDegraded:
		return colorYellow
	case partitionv1beta1.PartitionStateUnhealthy:
		return colorRed
	case partitionv1beta1.PartitionStateIncomplete:
		return colorYellow
	default:
		return colorGray
	}
}

func occupancyColor(o partitionv1beta1.OccupancyState) string {
	switch o {
	case partitionv1beta1.OccupancyOccupied:
		return colorCyan
	case partitionv1beta1.OccupancyEmpty:
		return colorGray
	default:
		return colorGray
	}
}

func nodeReadyColor(reason string) string {
	switch reason {
	case "NodesReady":
		return colorGreen
	case "NodesNotReady", "TPUsNotAllocatable":
		return colorYellow
	case "NodesNotFound":
		return colorRed
	default:
		return colorGray
	}
}

// FleetSummary holds the computed stats for one PartitionTree CR,
// used by RenderFleetTable.
type FleetSummary struct {
	Name            string
	AcceleratorType string
	Synced          string
	RootState       string
	RootOccupancy   string
	TotalPartitions int
	LeafPartitions  int
	TotalNodes      int
	HealthyLeaves   int
	DegradedLeaves  int
	UnhealthyLeaves int
}

// SummarizeFleet computes per-CR summaries for the fleet view.
func SummarizeFleet(items []partitionv1beta1.PartitionTree) []FleetSummary {
	out := make([]FleetSummary, 0, len(items))
	for i := range items {
		cr := &items[i]
		s := FleetSummary{
			Name:            cr.Name,
			AcceleratorType: cr.Status.AcceleratorType,
			Synced:          syncedStatus(cr),
		}
		if cr.Status.Root != nil {
			s.RootState = string(cr.Status.Root.State)
			s.RootOccupancy = string(cr.Status.Root.Occupancy)
			walkPartitions(cr.Status.Root, &s)
		} else {
			s.RootState = "Unknown"
			s.RootOccupancy = "Unspecified"
		}
		out = append(out, s)
	}
	return out
}

func walkPartitions(p *partitionv1beta1.PartitionInfo, s *FleetSummary) {
	s.TotalPartitions++
	if len(p.ChildPartitions) == 0 {
		s.LeafPartitions++
		s.TotalNodes += len(p.NodeNames)
		switch p.State {
		case partitionv1beta1.PartitionStateHealthy:
			s.HealthyLeaves++
		case partitionv1beta1.PartitionStateDegraded:
			s.DegradedLeaves++
		case partitionv1beta1.PartitionStateUnhealthy:
			s.UnhealthyLeaves++
		}
	}
	for i := range p.ChildPartitions {
		walkPartitions(&p.ChildPartitions[i], s)
	}
}

// RenderFleetTable produces a tabular summary of many PartitionTree CRs.
// Suitable for fleets of 500+ CRs. Sorted by name by default.
// When color is true, unhealthy rows are highlighted.
func RenderFleetTable(items []partitionv1beta1.PartitionTree, color bool) string {
	summaries := SummarizeFleet(items)
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].Name < summaries[j].Name
	})

	var b strings.Builder

	// Aggregate stats header.
	totalNodes := 0
	totalLeaves := 0
	totalDegraded := 0
	totalUnhealthy := 0
	syncedCount := 0
	for _, s := range summaries {
		totalNodes += s.TotalNodes
		totalLeaves += s.LeafPartitions
		totalDegraded += s.DegradedLeaves
		totalUnhealthy += s.UnhealthyLeaves
		if s.Synced == "Synced=True" {
			syncedCount++
		}
	}

	fmt.Fprintf(&b, "Fleet: %d PartitionTrees, %d nodes, %d leaves\n",
		len(summaries), totalNodes, totalLeaves)
	fmt.Fprintf(&b, "Synced: %d/%d", syncedCount, len(summaries))
	if totalDegraded > 0 || totalUnhealthy > 0 {
		fmt.Fprintf(&b, "  Degraded leaves: %d  Unhealthy leaves: %d", totalDegraded, totalUnhealthy)
	}
	b.WriteString("\n")
	b.WriteString(strings.Repeat("─", 125) + "\n")

	// Column headers.
	header := fmt.Sprintf("%-40s %-15s %-12s %-12s %-10s %6s %6s %5s %5s %5s",
		"NAME", "TYPE", "SYNCED", "STATE", "OCCUPANCY", "PARTS", "LEAVES", "NODES", "DEG", "UNHL")
	if color {
		b.WriteString(colorBold + header + colorReset + "\n")
	} else {
		b.WriteString(header + "\n")
	}

	// Rows.
	for _, s := range summaries {
		row := fmt.Sprintf("%-40s %-15s %-12s %-12s %-10s %6d %6d %5d %5d %5d",
			truncate(s.Name, 40),
			truncate(s.AcceleratorType, 15),
			truncate(s.Synced, 12),
			truncate(s.RootState, 12),
			truncate(s.RootOccupancy, 10),
			s.TotalPartitions,
			s.LeafPartitions,
			s.TotalNodes,
			s.DegradedLeaves,
			s.UnhealthyLeaves,
		)
		if color && (s.UnhealthyLeaves > 0 || s.DegradedLeaves > 0) {
			rowColor := colorYellow
			if s.UnhealthyLeaves > 0 {
				rowColor = colorRed
			}
			b.WriteString(rowColor + row + colorReset + "\n")
		} else {
			b.WriteString(row + "\n")
		}
	}

	return b.String()
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	if max <= 3 {
		return s[:max]
	}
	return s[:max-3] + "..."
}
