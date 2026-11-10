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

// Command ptree-viz renders PartitionTree custom resources as ASCII
// trees or fleet summary tables. It connects to the current kubeconfig
// cluster and reads PartitionTree CRs.
//
// Usage:
//
//	ptree-viz                      # fleet table of all CRs
//	ptree-viz <pool-name>          # single tree view
//	ptree-viz --no-color           # disable ANSI colors
//	ptree-viz --sort=nodes         # sort fleet by node count
//	ptree-viz --filter=unhealthy   # show only unhealthy CRs
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
	"github.com/ai-on-gke/partition-tree/internal/viz"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(partitionv1beta1.AddToScheme(scheme))
}

func main() {
	var noColor bool
	var sortBy string
	var filter string

	flag.BoolVar(&noColor, "no-color", false, "Disable ANSI color output")
	flag.StringVar(&sortBy, "sort", "name", "Sort fleet by: name, nodes, type, state")
	flag.StringVar(&filter, "filter", "", "Filter: unhealthy, degraded, out-of-sync, occupied, empty")
	flag.Parse()

	color := !noColor

	cfg, err := ctrl.GetConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: cannot load kubeconfig: %v\n", err)
		os.Exit(1)
	}

	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: cannot create client: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()

	// Single-tree mode: ptree-viz <pool-name>
	if flag.NArg() > 0 {
		name := flag.Arg(0)
		var cr partitionv1beta1.PartitionTree
		if err := c.Get(ctx, client.ObjectKey{Name: name}, &cr); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Print(viz.RenderTree(&cr, color))
		return
	}

	// Fleet mode: list all CRs.
	var list partitionv1beta1.PartitionTreeList
	if err := c.List(ctx, &list); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if len(list.Items) == 0 {
		fmt.Println("No PartitionTree resources found.")
		return
	}

	items := list.Items

	// Apply filter.
	if filter != "" {
		items = applyFilter(items, filter)
		if len(items) == 0 {
			fmt.Printf("No PartitionTree resources match filter %q.\n", filter)
			return
		}
	}

	// Apply sort.
	summaries := viz.SummarizeFleet(items)
	sortFleet(summaries, items, sortBy)

	fmt.Print(viz.RenderFleetTable(items, color))
}

func applyFilter(items []partitionv1beta1.PartitionTree, filter string) []partitionv1beta1.PartitionTree {
	summaries := viz.SummarizeFleet(items)
	var out []partitionv1beta1.PartitionTree
	for i, s := range summaries {
		match := false
		switch strings.ToLower(filter) {
		case "unhealthy":
			match = s.UnhealthyLeaves > 0
		case "degraded":
			match = s.DegradedLeaves > 0
		case "out-of-sync":
			match = s.Synced != "Synced=True"
		case "occupied":
			match = s.RootOccupancy == string(partitionv1beta1.OccupancyOccupied)
		case "empty":
			match = s.RootOccupancy == string(partitionv1beta1.OccupancyEmpty)
		default:
			// Name substring match.
			match = strings.Contains(s.Name, filter)
		}
		if match {
			out = append(out, items[i])
		}
	}
	return out
}

func sortFleet(summaries []viz.FleetSummary, items []partitionv1beta1.PartitionTree, sortBy string) {
	indices := make([]int, len(summaries))
	for i := range indices {
		indices[i] = i
	}

	sort.SliceStable(indices, func(a, b int) bool {
		sa, sb := summaries[indices[a]], summaries[indices[b]]
		switch strings.ToLower(sortBy) {
		case "nodes":
			return sa.TotalNodes > sb.TotalNodes
		case "type":
			return sa.AcceleratorType < sb.AcceleratorType
		case "state":
			return sa.RootState < sb.RootState
		default:
			return sa.Name < sb.Name
		}
	})

	// Reorder items in-place based on sorted indices.
	sorted := make([]partitionv1beta1.PartitionTree, len(items))
	for i, idx := range indices {
		sorted[i] = items[idx]
	}
	copy(items, sorted)
}
