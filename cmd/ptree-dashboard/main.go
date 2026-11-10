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

// Command ptree-dashboard serves a web dashboard for monitoring
// PartitionTree custom resources across a GKE TPU fleet.
//
// Usage:
//
//	ptree-dashboard                  # connect to current kubeconfig cluster
//	ptree-dashboard --port=9090      # custom port
//	ptree-dashboard --demo           # serve realistic mock data without K8s
package main

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
)

//go:embed dashboard.html
var dashboardHTML embed.FS

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(partitionv1beta1.AddToScheme(scheme))
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

// DashboardTreeSummary is the per-tree summary sent in the /api/trees response.
type DashboardTreeSummary struct {
	Name            string      `json:"name"`
	AcceleratorType string      `json:"acceleratorType"`
	PodID           string      `json:"podID,omitempty"`
	Synced          bool        `json:"synced"`
	WorstState      string      `json:"worstState"`
	NodeReadyFailed bool        `json:"nodeReadyFailed"`
	OccupancyRatio  float64     `json:"occupancyRatio"`
	LeafCount       int         `json:"leafCount"`
	OccupiedLeaves  int         `json:"occupiedLeaves"`
	TotalNodes      int         `json:"totalNodes"`
	RootTopology    string      `json:"rootTopology"`
	LeafSlices      []LeafSlice `json:"leafSlices,omitempty"`
}

// LeafSlice carries per-leaf data for the pod-tab host grid.
type LeafSlice struct {
	SliceID   string `json:"sliceId,omitempty"`
	State     string `json:"state"`
	Occupancy string `json:"occupancy"`
}

// FleetAggregate is the fleet-level summary.
type FleetAggregate struct {
	TotalTrees       int     `json:"totalTrees"`
	TotalLeaves      int     `json:"totalLeaves"`
	TotalNodes       int     `json:"totalNodes"`
	OccupiedLeaves   int     `json:"occupiedLeaves"`
	HealthyLeaves    int     `json:"healthyLeaves"`
	DegradedLeaves   int     `json:"degradedLeaves"`
	UnhealthyLeaves  int     `json:"unhealthyLeaves"`
	IncompleteLeaves int     `json:"incompleteLeaves"`
	NodeReadyFailed  int     `json:"nodeReadyFailed"`
	SyncedTrees      int     `json:"syncedTrees"`
	OccupancyPercent float64 `json:"occupancyPercent"`
}

// TreesResponse is the top-level /api/trees JSON response.
type TreesResponse struct {
	Timestamp string                 `json:"timestamp"`
	Fleet     FleetAggregate         `json:"fleet"`
	Trees     []DashboardTreeSummary `json:"trees"`
}

// TreeDetailPartition is used in the /api/trees/{name} response.
type TreeDetailPartition struct {
	PartitionID     string                            `json:"partitionId"`
	Topology        string                            `json:"topology"`
	State           string                            `json:"state"`
	Occupancy       string                            `json:"occupancy"`
	SliceID         string                            `json:"sliceId,omitempty"`
	NodeNames       []string                          `json:"nodeNames,omitempty"`
	NodeReady       *bool                             `json:"nodeReady,omitempty"`
	NodeReadyReason string                            `json:"nodeReadyReason,omitempty"`
	Taints          []partitionv1beta1.PartitionTaint `json:"taints,omitempty"`
	Children        []TreeDetailPartition             `json:"children"`
}

// TreeEvent is a K8s Event associated with a PartitionTree CR.
type TreeEvent struct {
	Timestamp string `json:"timestamp"`
	Type      string `json:"type"`
	Reason    string `json:"reason"`
	Message   string `json:"message"`
}

// TreeDetailResponse is the /api/trees/{name} JSON response.
type TreeDetailResponse struct {
	Name            string               `json:"name"`
	AcceleratorType string               `json:"acceleratorType"`
	Synced          bool                 `json:"synced"`
	SyncedReason    string               `json:"syncedReason"`
	Root            *TreeDetailPartition `json:"root"`
	Events          []TreeEvent          `json:"events"`
}

// summaryAccumulator collects data during a recursive tree walk.
type summaryAccumulator struct {
	worstState       partitionv1beta1.PartitionStateType
	leafCount        int
	occupiedLeaves   int
	totalNodes       int
	nodeReadyFailed  bool
	healthyLeaves    int
	degradedLeaves   int
	unhealthyLeaves  int
	incompleteLeaves int
	leafSlices       []LeafSlice
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func main() {
	var port int
	var demo bool
	var authEnabled bool
	var authAudience string
	var authCacheTTL time.Duration
	flag.IntVar(&port, "port", 8080, "HTTP server port")
	flag.BoolVar(&demo, "demo", false, "Serve realistic mock data without connecting to K8s")
	flag.BoolVar(&authEnabled, "auth-enabled", false, "Enable Kubernetes TokenReview and SubjectAccessReview authentication and authorization for API endpoints")
	flag.StringVar(&authAudience, "auth-audience", "", "Optional audience to validate in TokenReview requests")
	flag.DurationVar(&authCacheTTL, "auth-cache-ttl", 1*time.Minute, "Cache TTL for successful authentication and authorization checks")
	flag.Parse()

	mux := http.NewServeMux()

	// Serve embedded dashboard.html at root.
	mux.HandleFunc("GET /", handleDashboard)

	if demo {
		log.Println("Running in --demo mode with mock data")
		dg := newDemoGenerator()
		mux.HandleFunc("GET /api/trees/{name}", handleTreeDetailDemo(dg))
		mux.HandleFunc("GET /api/trees", handleTreesDemo(dg))
	} else {
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

		var treesHandler http.Handler = handleTrees(c)
		var treeDetailHandler http.Handler = handleTreeDetail(c)

		if authEnabled {
			kubeClient, err := kubernetes.NewForConfig(cfg)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: cannot create kubernetes client: %v\n", err)
				os.Exit(1)
			}
			log.Println("Authentication and authorization enabled (TokenReview + SubjectAccessReview)")
			cache := newAuthCache(1000)
			opts := authOptions{
				audience: authAudience,
				cacheTTL: authCacheTTL,
				negTTL:   5 * time.Second,
			}
			treesHandler = withAuth(kubeClient, "list", cache, opts, treesHandler)
			treeDetailHandler = withAuth(kubeClient, "get", cache, opts, treeDetailHandler)
		}

		// Register {name} route BEFORE the /api/trees route so it takes precedence.
		mux.Handle("GET /api/trees/{name}", treeDetailHandler)
		mux.Handle("GET /api/trees", treesHandler)
	}

	addr := fmt.Sprintf(":%d", port)
	log.Printf("Starting ptree-dashboard on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Authentication and Authorization (TokenReview + SubjectAccessReview)
// ---------------------------------------------------------------------------

type authDecision struct {
	allowed    bool
	statusCode int
	statusMsg  string
	expiresAt  time.Time
}

type authCache struct {
	mu         sync.RWMutex
	entries    map[string]authDecision
	maxEntries int
}

func newAuthCache(maxEntries int) *authCache {
	if maxEntries <= 0 {
		maxEntries = 1000
	}
	return &authCache{
		entries:    make(map[string]authDecision),
		maxEntries: maxEntries,
	}
}

func (c *authCache) get(key string) (authDecision, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[key]
	if !ok || time.Now().After(entry.expiresAt) {
		return authDecision{}, false
	}
	return entry, true
}

func (c *authCache) set(key string, d authDecision, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= c.maxEntries {
		now := time.Now()
		for k, v := range c.entries {
			if now.After(v.expiresAt) {
				delete(c.entries, k)
			}
		}
		if len(c.entries) >= c.maxEntries {
			count := 0
			for k := range c.entries {
				delete(c.entries, k)
				count++
				if count > c.maxEntries/2 {
					break
				}
			}
		}
	}
	d.expiresAt = time.Now().Add(ttl)
	c.entries[key] = d
}

func tokenHash(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

type authOptions struct {
	audience string
	cacheTTL time.Duration
	negTTL   time.Duration
}

// withAuth wraps an http.Handler with Kubernetes authentication (TokenReview)
// and authorization (SubjectAccessReview) for PartitionTree resources.
func withAuth(kubeClient kubernetes.Interface, verb string, cache *authCache, opts authOptions, next http.Handler) http.Handler {
	if cache == nil {
		cache = newAuthCache(1000)
	}
	if opts.cacheTTL <= 0 {
		opts.cacheTTL = 1 * time.Minute
	}
	if opts.negTTL <= 0 {
		opts.negTTL = 5 * time.Second
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		trimmed := strings.TrimSpace(authHeader)
		parts := strings.SplitN(trimmed, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
			w.Header().Set("WWW-Authenticate", `Bearer realm="ptree-dashboard"`)
			http.Error(w, "Unauthorized: missing or invalid Bearer token", http.StatusUnauthorized)
			return
		}
		token := strings.TrimSpace(parts[1])
		if token == "" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="ptree-dashboard"`)
			http.Error(w, "Unauthorized: empty Bearer token", http.StatusUnauthorized)
			return
		}

		name := r.PathValue("name")
		cacheKey := fmt.Sprintf("%s:%s:%s", tokenHash(token), verb, name)

		if d, ok := cache.get(cacheKey); ok {
			if !d.allowed {
				if d.statusCode == http.StatusUnauthorized {
					w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
				}
				http.Error(w, d.statusMsg, d.statusCode)
				return
			}
			next.ServeHTTP(w, r)
			return
		}

		ctx := r.Context()
		tr := &authenticationv1.TokenReview{
			Spec: authenticationv1.TokenReviewSpec{
				Token: token,
			},
		}
		if opts.audience != "" {
			tr.Spec.Audiences = []string{opts.audience}
		}

		trResult, err := kubeClient.AuthenticationV1().TokenReviews().Create(ctx, tr, metav1.CreateOptions{})
		if err != nil {
			log.Printf("TokenReview error: %v", err)
			http.Error(w, "Internal Server Error: authentication check failed", http.StatusInternalServerError)
			return
		}
		if !trResult.Status.Authenticated {
			w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
			cache.set(cacheKey, authDecision{
				allowed:    false,
				statusCode: http.StatusUnauthorized,
				statusMsg:  "Unauthorized: token is not valid",
			}, opts.negTTL)
			http.Error(w, "Unauthorized: token is not valid", http.StatusUnauthorized)
			return
		}

		sar := &authorizationv1.SubjectAccessReview{
			Spec: authorizationv1.SubjectAccessReviewSpec{
				User:   trResult.Status.User.Username,
				Groups: trResult.Status.User.Groups,
				UID:    trResult.Status.User.UID,
				Extra:  convertExtra(trResult.Status.User.Extra),
				ResourceAttributes: &authorizationv1.ResourceAttributes{
					Group:    "partition.tpu.google.com",
					Resource: "partitiontrees",
					Verb:     verb,
					Name:     name,
				},
			},
		}
		sarResult, err := kubeClient.AuthorizationV1().SubjectAccessReviews().Create(ctx, sar, metav1.CreateOptions{})
		if err != nil {
			log.Printf("SubjectAccessReview error: %v", err)
			http.Error(w, "Internal Server Error: authorization check failed", http.StatusInternalServerError)
			return
		}
		if !sarResult.Status.Allowed {
			cache.set(cacheKey, authDecision{
				allowed:    false,
				statusCode: http.StatusForbidden,
				statusMsg:  "Forbidden: user is not authorized to access partitiontrees",
			}, opts.negTTL)
			http.Error(w, "Forbidden: user is not authorized to access partitiontrees", http.StatusForbidden)
			return
		}

		cache.set(cacheKey, authDecision{
			allowed:    true,
			statusCode: http.StatusOK,
		}, opts.cacheTTL)

		next.ServeHTTP(w, r)
	})
}

// convertExtra converts authentication extra values to authorization extra values.
func convertExtra(in map[string]authenticationv1.ExtraValue) map[string]authorizationv1.ExtraValue {
	if in == nil {
		return nil
	}
	out := make(map[string]authorizationv1.ExtraValue, len(in))
	for k, v := range in {
		out[k] = authorizationv1.ExtraValue(v)
	}
	return out
}

// ---------------------------------------------------------------------------
// HTTP Handlers — Real K8s
// ---------------------------------------------------------------------------

// handleDashboard serves the embedded dashboard.html.
func handleDashboard(w http.ResponseWriter, r *http.Request) {
	data, err := dashboardHTML.ReadFile("dashboard.html")
	if err != nil {
		http.Error(w, "dashboard.html not found", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

// handleTrees lists all PartitionTree CRs, computes summaries, returns JSON.
func handleTrees(c client.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var list partitionv1beta1.PartitionTreeList
		if err := c.List(ctx, &list); err != nil {
			http.Error(w, fmt.Sprintf("failed to list PartitionTrees: %v", err), http.StatusInternalServerError)
			return
		}

		fleet := FleetAggregate{TotalTrees: len(list.Items)}
		trees := make([]DashboardTreeSummary, 0, len(list.Items))

		for i := range list.Items {
			s := summarizeTree(&list.Items[i])
			trees = append(trees, s)

			fleet.TotalLeaves += s.LeafCount
			fleet.TotalNodes += s.TotalNodes
			fleet.OccupiedLeaves += s.OccupiedLeaves
			if s.Synced {
				fleet.SyncedTrees++
			}
			if s.NodeReadyFailed {
				fleet.NodeReadyFailed++
			}
		}

		// Second pass for per-leaf health counts from the raw CRs.
		for i := range list.Items {
			cr := &list.Items[i]
			if cr.Status.Root != nil {
				var acc summaryAccumulator
				acc.worstState = partitionv1beta1.PartitionStateHealthy
				walkForSummary(cr.Status.Root, &acc, "")
				fleet.HealthyLeaves += acc.healthyLeaves
				fleet.DegradedLeaves += acc.degradedLeaves
				fleet.UnhealthyLeaves += acc.unhealthyLeaves
				fleet.IncompleteLeaves += acc.incompleteLeaves
			}
		}

		if fleet.TotalLeaves > 0 {
			fleet.OccupancyPercent = float64(fleet.OccupiedLeaves) / float64(fleet.TotalLeaves) * 100
		}

		resp := TreesResponse{
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Fleet:     fleet,
			Trees:     trees,
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}

// handleTreeDetail fetches a single PartitionTree CR by name and its K8s Events.
func handleTreeDetail(c client.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if name == "" {
			http.Error(w, "missing tree name", http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		var cr partitionv1beta1.PartitionTree
		if err := c.Get(ctx, client.ObjectKey{Name: name}, &cr); err != nil {
			http.Error(w, fmt.Sprintf("tree %q not found: %v", name, err), http.StatusNotFound)
			return
		}

		events, err := listTreeEvents(ctx, c, name)
		if err != nil {
			// Non-fatal: return tree without events.
			log.Printf("Warning: failed to list events for %q: %v", name, err)
		}

		var root *TreeDetailPartition
		if cr.Status.Root != nil {
			built := buildDetailTree(cr.Status.Root)
			root = &built
		}

		resp := TreeDetailResponse{
			Name:            cr.Name,
			AcceleratorType: cr.Status.AcceleratorType,
			Synced:          isSynced(&cr),
			SyncedReason:    syncedReason(&cr),
			Root:            root,
			Events:          events,
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}

// listTreeEvents fetches K8s Events for a specific PartitionTree CR.
func listTreeEvents(ctx context.Context, c client.Client, treeName string) ([]TreeEvent, error) {
	var eventList corev1.EventList
	err := c.List(ctx, &eventList,
		client.MatchingFields{
			"involvedObject.name": treeName,
			"involvedObject.kind": "PartitionTree",
		},
	)
	if err != nil {
		return nil, err
	}

	// Sort by LastTimestamp descending.
	items := eventList.Items
	sort.Slice(items, func(i, j int) bool {
		return items[i].LastTimestamp.After(items[j].LastTimestamp.Time)
	})

	// Return up to 50 events.
	limit := min(50, len(items))

	result := make([]TreeEvent, 0, limit)
	for _, e := range items[:limit] {
		ts := e.LastTimestamp.Time
		if ts.IsZero() {
			ts = e.CreationTimestamp.Time
		}
		result = append(result, TreeEvent{
			Timestamp: ts.UTC().Format(time.RFC3339),
			Type:      e.Type,
			Reason:    e.Reason,
			Message:   e.Message,
		})
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// Tree Summarization
// ---------------------------------------------------------------------------

// summarizeTree walks a single PartitionTree CR and returns a DashboardTreeSummary.
func summarizeTree(cr *partitionv1beta1.PartitionTree) DashboardTreeSummary {
	podID := cr.Labels["cloud.google.com/tpu-pod-id"]
	s := DashboardTreeSummary{
		Name:            cr.Name,
		AcceleratorType: cr.Status.AcceleratorType,
		PodID:           podID,
		Synced:          isSynced(cr),
	}

	if cr.Status.Root == nil {
		s.WorstState = string(partitionv1beta1.PartitionStateUnknown)
		return s
	}

	s.RootTopology = cr.Status.Root.Topology

	var acc summaryAccumulator
	acc.worstState = partitionv1beta1.PartitionStateHealthy
	walkForSummary(cr.Status.Root, &acc, "")

	s.WorstState = string(acc.worstState)
	s.NodeReadyFailed = acc.nodeReadyFailed
	s.LeafCount = acc.leafCount
	s.OccupiedLeaves = acc.occupiedLeaves
	s.TotalNodes = acc.totalNodes
	s.LeafSlices = acc.leafSlices

	if acc.leafCount > 0 {
		s.OccupancyRatio = float64(acc.occupiedLeaves) / float64(acc.leafCount)
	}

	return s
}

// walkForSummary is the recursive tree walker.
// inheritedSliceId carries the nearest ancestor's sliceId down to leaves.
func walkForSummary(p *partitionv1beta1.PartitionInfo, acc *summaryAccumulator, inheritedSliceId string) {
	acc.totalNodes++
	acc.worstState = worstState(acc.worstState, p.State)

	// If this partition has its own sliceId, use it; otherwise inherit from parent.
	effectiveSliceId := inheritedSliceId
	if p.SliceId != "" {
		effectiveSliceId = p.SliceId
	}

	isLeaf := len(p.ChildPartitions) == 0

	if isLeaf {
		acc.leafCount++
		if p.Occupancy == partitionv1beta1.OccupancyOccupied {
			acc.occupiedLeaves++
		}

		// Collect per-leaf slice data for the pod-tab host grid.
		acc.leafSlices = append(acc.leafSlices, LeafSlice{
			SliceID:   effectiveSliceId,
			State:     string(p.State),
			Occupancy: string(p.Occupancy),
		})

		// Count per-state leaves.
		switch p.State {
		case partitionv1beta1.PartitionStateHealthy:
			acc.healthyLeaves++
		case partitionv1beta1.PartitionStateDegraded:
			acc.degradedLeaves++
		case partitionv1beta1.PartitionStateUnhealthy:
			acc.unhealthyLeaves++
		case partitionv1beta1.PartitionStateIncomplete:
			acc.incompleteLeaves++
		}

		// Check NodeReady condition on leaves.
		cond := meta.FindStatusCondition(p.Conditions, partitionv1beta1.ConditionTypeNodeReady)
		if cond != nil && cond.Status == metav1.ConditionFalse {
			acc.nodeReadyFailed = true
		}
	}

	for i := range p.ChildPartitions {
		walkForSummary(&p.ChildPartitions[i], acc, effectiveSliceId)
	}
}

// worstState returns the higher-priority state between two states.
// Priority: Unhealthy > Degraded > Incomplete > Unknown > Healthy
func worstState(a, b partitionv1beta1.PartitionStateType) partitionv1beta1.PartitionStateType {
	priority := map[partitionv1beta1.PartitionStateType]int{
		partitionv1beta1.PartitionStateUnhealthy:  1,
		partitionv1beta1.PartitionStateDegraded:   2,
		partitionv1beta1.PartitionStateIncomplete: 3,
		partitionv1beta1.PartitionStateUnknown:    4,
		partitionv1beta1.PartitionStateHealthy:    5,
	}
	pa, ok := priority[a]
	if !ok {
		pa = 4 // Unknown
	}
	pb, ok := priority[b]
	if !ok {
		pb = 4
	}
	if pa <= pb {
		return a
	}
	return b
}

// isSynced checks the top-level Synced condition.
func isSynced(cr *partitionv1beta1.PartitionTree) bool {
	cond := meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeSynced)
	if cond == nil {
		return false
	}
	return cond.Status == metav1.ConditionTrue
}

// syncedReason returns the reason string from the Synced condition.
func syncedReason(cr *partitionv1beta1.PartitionTree) string {
	cond := meta.FindStatusCondition(cr.Status.Conditions, partitionv1beta1.ConditionTypeSynced)
	if cond == nil {
		return ""
	}
	return cond.Reason
}

// buildDetailTree recursively converts PartitionInfo to TreeDetailPartition.
func buildDetailTree(p *partitionv1beta1.PartitionInfo) TreeDetailPartition {
	d := TreeDetailPartition{
		PartitionID: p.PartitionID,
		Topology:    p.Topology,
		State:       string(p.State),
		Occupancy:   string(p.Occupancy),
		SliceID:     p.SliceId,
		NodeNames:   p.NodeNames,
		Taints:      p.Taints,
		Children:    make([]TreeDetailPartition, 0, len(p.ChildPartitions)),
	}

	// NodeReady condition only on leaves.
	if len(p.ChildPartitions) == 0 {
		cond := meta.FindStatusCondition(p.Conditions, partitionv1beta1.ConditionTypeNodeReady)
		if cond != nil {
			ready := cond.Status == metav1.ConditionTrue
			d.NodeReady = &ready
			d.NodeReadyReason = cond.Reason
		}
	}

	for i := range p.ChildPartitions {
		d.Children = append(d.Children, buildDetailTree(&p.ChildPartitions[i]))
	}

	return d
}

// ---------------------------------------------------------------------------
// Demo Mode — Mock Data Generator
// ---------------------------------------------------------------------------

// demoGenerator holds the mock fleet state and mutates it slightly on each poll.
type demoGenerator struct {
	mu    sync.Mutex
	trees []partitionv1beta1.PartitionTree
	rng   *rand.Rand
}

func newDemoGenerator() *demoGenerator {
	dg := &demoGenerator{
		rng: rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0)),
	}
	dg.trees = dg.generateFleet()
	return dg
}

func (dg *demoGenerator) generateFleet() []partitionv1beta1.PartitionTree {
	const totalTreesFleet = 1440
	v7xCount := int(float64(totalTreesFleet) * 0.60) // 864 v7x

	trees := make([]partitionv1beta1.PartitionTree, 0, totalTreesFleet)

	// Pre-assign special tree indices.
	degradedIndices := dg.pickIndices(totalTreesFleet, 12)
	unhealthyIndices := dg.pickIndices(totalTreesFleet, 3)
	nodeReadyFailIndices := dg.pickIndices(totalTreesFleet, 5)
	outOfSyncIndices := dg.pickIndices(totalTreesFleet, 2)

	// Generate pod names to group trees into pods.
	podNames := []string{
		"ml-training-prod-v7x-alpha",
		"ml-training-prod-v7x-beta",
		"ml-training-staging-v7x",
		"llm-serving-prod-us-east",
		"llm-serving-prod-us-west",
		"research-team-experiment-42",
		"research-team-experiment-99",
		"diffusion-model-finetune",
		"inference-batch-nightly",
		"ml-training-prod-v6e-gamma",
		"ml-training-prod-v6e-delta",
		"eval-pipeline-prod",
	}

	for i := range totalTreesFleet {
		accelType := "v7x"
		if i >= v7xCount {
			accelType = "v6e"
		}

		name := fmt.Sprintf("gke-tpu-%s-pool-%04d", accelType, i)

		// Assign a pod name: distribute trees across pods.
		podName := podNames[i%len(podNames)]

		treeState := partitionv1beta1.PartitionStateHealthy
		if unhealthyIndices[i] {
			treeState = partitionv1beta1.PartitionStateUnhealthy
		} else if degradedIndices[i] {
			treeState = partitionv1beta1.PartitionStateDegraded
		}

		nodeReadyFail := nodeReadyFailIndices[i]
		outOfSync := outOfSyncIndices[i]

		// Build a realistic tree structure.
		topology := dg.pickTopology(accelType)
		root := dg.buildMockTree(topology, treeState, nodeReadyFail, accelType, name)

		// Synced condition.
		syncedStatus := metav1.ConditionTrue
		syncedReason := "GCEPollSucceeded"
		if outOfSync {
			syncedStatus = metav1.ConditionFalse
			syncedReason = "GCEPollFailed"
		}

		cr := partitionv1beta1.PartitionTree{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				CreationTimestamp: metav1.Now(),
				Labels: map[string]string{
					"cloud.google.com/tpu-pod-id": podName,
				},
			},
			Status: partitionv1beta1.PartitionTreeStatus{
				AcceleratorType: accelType,
				Root:            root,
				Conditions: []metav1.Condition{
					{
						Type:               partitionv1beta1.ConditionTypeSynced,
						Status:             syncedStatus,
						LastTransitionTime: metav1.Now(),
						Reason:             syncedReason,
						Message:            fmt.Sprintf("Sync status for %s", name),
					},
				},
			},
		}
		trees = append(trees, cr)
	}

	return trees
}

//nolint:unparam // total is always totalTreesFleet but kept as param for testability.
func (dg *demoGenerator) pickIndices(total, count int) map[int]bool {
	m := make(map[int]bool, count)
	for len(m) < count {
		m[dg.rng.IntN(total)] = true
	}
	return m
}

func (dg *demoGenerator) pickTopology(accelType string) string {
	if accelType == "v7x" {
		topos := []string{"4x4x4", "2x2x4", "4x4x2"}
		return topos[dg.rng.IntN(len(topos))]
	}
	// v6e: realistic superslice topologies from the slice controller.
	topos := []string{"16x16", "8x16", "8x8", "4x8", "4x4"}
	return topos[dg.rng.IntN(len(topos))]
}

func (dg *demoGenerator) buildMockTree(
	topology string,
	treeState partitionv1beta1.PartitionStateType,
	nodeReadyFail bool,
	accelType string,
	treeName string,
) *partitionv1beta1.PartitionInfo {
	// Parse topology dimensions: "4x4" → [4,4], "4x4x4" → [4,4,4].
	parts := strings.Split(topology, "x")
	dims := make([]int, len(parts))
	for i, p := range parts {
		if _, err := fmt.Sscanf(p, "%d", &dims[i]); err != nil {
			dims[i] = 1
		}
		if dims[i] < 1 {
			dims[i] = 1
		}
	}

	root := dg.buildMockPartition(treeName, dims, treeState, nodeReadyFail, accelType, 0, "root")
	return root
}

// isLeafDims returns true when the partition has reached leaf size.
//   - v6e (2D): leaf = 2x2 (1 node). All dims must be ≤ 2.
//   - v7x (3D): leaf = 2x2x1 (1 node). Any dim reduced to 1.
//
//nolint:unparam // accelType is threaded through for future per-type leaf logic.
func isLeafDims(dims []int, _ string) bool {
	if len(dims) == 3 {
		// v7x: leaf when any dimension reaches 1.
		for _, d := range dims {
			if d <= 1 {
				return true
			}
		}
		return false
	}
	// v6e: leaf when all dimensions are ≤ 2 (i.e. 2x2).
	for _, d := range dims {
		if d > 2 {
			return false
		}
	}
	return true
}

//nolint:unparam // accelType is passed through for recursive calls; may be used in future.
func (dg *demoGenerator) buildMockPartition(
	treeName string,
	dims []int, // current topology dimensions
	treeState partitionv1beta1.PartitionStateType,
	nodeReadyFail bool,
	accelType string,
	currentDepth int,
	partitionID string,
) *partitionv1beta1.PartitionInfo {
	// Build topology string from dims.
	dimStrs := make([]string, len(dims))
	for i, d := range dims {
		dimStrs[i] = fmt.Sprintf("%d", d)
	}
	topo := strings.Join(dimStrs, "x")

	p := &partitionv1beta1.PartitionInfo{
		PartitionID: partitionID,
		Topology:    topo,
		State:       partitionv1beta1.PartitionStateHealthy,
	}

	leaf := isLeafDims(dims, accelType)

	if leaf {
		// Each leaf = 1 host with 1 node name.
		p.NodeNames = []string{fmt.Sprintf("%s-node-%s", treeName, partitionID)}

		// Assign occupancy: ~70% occupied.
		if dg.rng.Float64() < 0.70 {
			p.Occupancy = partitionv1beta1.OccupancyOccupied
		} else {
			p.Occupancy = partitionv1beta1.OccupancyEmpty
		}

		// Simulate a single-host slice: ~10% of leaves get their own sliceId.
		if dg.rng.Float64() < 0.10 {
			p.SliceId = fmt.Sprintf("single-host-%s-%s", treeName, partitionID)
		}

		// Apply tree-level state to some leaves.
		if treeState != partitionv1beta1.PartitionStateHealthy && dg.rng.Float64() < 0.3 {
			p.State = treeState
		}

		// NodeReady condition.
		if nodeReadyFail && dg.rng.Float64() < 0.25 {
			p.Conditions = []metav1.Condition{
				{
					Type:               partitionv1beta1.ConditionTypeNodeReady,
					Status:             metav1.ConditionFalse,
					LastTransitionTime: metav1.Now(),
					Reason:             "NodeNotReady",
					Message:            fmt.Sprintf("Node %s is not ready", p.NodeNames[0]),
				},
			}
		} else {
			p.Conditions = []metav1.Condition{
				{
					Type:               partitionv1beta1.ConditionTypeNodeReady,
					Status:             metav1.ConditionTrue,
					LastTransitionTime: metav1.Now(),
					Reason:             "NodeReady",
					Message:            "All nodes ready",
				},
			}
		}
	} else {
		// Binary split: halve the first largest dimension → 2 children.
		// Real controller: 4x4x4 → 2x4x4, 8x8 → 4x8.
		splitIdx := 0
		for i := 1; i < len(dims); i++ {
			if dims[i] > dims[splitIdx] {
				splitIdx = i
			}
		}

		childDims := make([]int, len(dims))
		copy(childDims, dims)
		childDims[splitIdx] = dims[splitIdx] / 2

		p.ChildPartitions = make([]partitionv1beta1.PartitionInfo, 2)
		for i := range 2 {
			childID := fmt.Sprintf("%s-b%d", partitionID, i)
			child := dg.buildMockPartition(
				treeName, childDims, treeState, nodeReadyFail,
				accelType, currentDepth+1, childID,
			)
			p.ChildPartitions[i] = *child
		}

		// Branch state: usually healthy; inherit tree state on one branch.
		if treeState != partitionv1beta1.PartitionStateHealthy && dg.rng.Float64() < 0.2 {
			p.State = treeState
		}

		// Simulate subslicing: assign a sliceId to some branches one level
		// above leaves so their leaf children inherit it as a group.
		if isLeafDims(childDims, accelType) && dg.rng.Float64() < 0.6 {
			p.SliceId = fmt.Sprintf("default-jobset-%s-slice%d", treeName, dg.rng.IntN(4))
		}
	}

	return p
}

// mutateFleet randomly flips a few occupancy states to simulate live updates.
func (dg *demoGenerator) mutateFleet() {
	// Flip 5-15 random leaves' occupancy.
	flips := 5 + dg.rng.IntN(11)
	for range flips {
		treeIdx := dg.rng.IntN(len(dg.trees))
		root := dg.trees[treeIdx].Status.Root
		if root != nil {
			dg.mutateRandomLeaf(root)
		}
	}
}

func (dg *demoGenerator) mutateRandomLeaf(p *partitionv1beta1.PartitionInfo) {
	if len(p.ChildPartitions) == 0 {
		// Toggle occupancy.
		if p.Occupancy == partitionv1beta1.OccupancyOccupied {
			p.Occupancy = partitionv1beta1.OccupancyEmpty
			p.SliceId = ""
		} else {
			p.Occupancy = partitionv1beta1.OccupancyOccupied
			p.SliceId = fmt.Sprintf("slice-mutated-%d", dg.rng.IntN(10000))
		}
		return
	}
	// Recurse into a random child.
	idx := dg.rng.IntN(len(p.ChildPartitions))
	dg.mutateRandomLeaf(&p.ChildPartitions[idx])
}

// generateDemoEvents creates fake K8s Events for a given tree.
func (dg *demoGenerator) generateDemoEvents(treeName string, cr *partitionv1beta1.PartitionTree) []TreeEvent {
	events := []TreeEvent{
		{
			Timestamp: time.Now().Add(-24 * time.Hour).UTC().Format(time.RFC3339),
			Type:      "Normal",
			Reason:    "PartitionTreeCreated",
			Message:   fmt.Sprintf("PartitionTree %s created", treeName),
		},
	}

	// Add warning events for unhealthy/degraded trees.
	if cr.Status.Root != nil {
		var acc summaryAccumulator
		acc.worstState = partitionv1beta1.PartitionStateHealthy
		walkForSummary(cr.Status.Root, &acc, "")

		if acc.worstState == partitionv1beta1.PartitionStateUnhealthy {
			events = append(events, TreeEvent{
				Timestamp: time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339),
				Type:      "Warning",
				Reason:    "NodePoolDrained",
				Message:   fmt.Sprintf("Unhealthy partition detected in %s, draining node pool", treeName),
			})
		}
		if acc.worstState == partitionv1beta1.PartitionStateDegraded {
			events = append(events, TreeEvent{
				Timestamp: time.Now().Add(-30 * time.Minute).UTC().Format(time.RFC3339),
				Type:      "Warning",
				Reason:    "GCEPollFailed",
				Message:   fmt.Sprintf("GCE poll returned degraded state for %s", treeName),
			})
		}
	}

	// Add sync events.
	if !isSynced(cr) {
		events = append(events, TreeEvent{
			Timestamp: time.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339),
			Type:      "Warning",
			Reason:    "SyncLost",
			Message:   fmt.Sprintf("Sync lost for %s: GCE API unreachable", treeName),
		})
	} else {
		events = append(events, TreeEvent{
			Timestamp: time.Now().Add(-5 * time.Minute).UTC().Format(time.RFC3339),
			Type:      "Normal",
			Reason:    "SyncRestored",
			Message:   fmt.Sprintf("Sync restored for %s", treeName),
		})
	}

	// Sort by timestamp descending.
	sort.Slice(events, func(i, j int) bool {
		return events[i].Timestamp > events[j].Timestamp
	})

	return events
}

// ---------------------------------------------------------------------------
// HTTP Handlers — Demo Mode
// ---------------------------------------------------------------------------

// handleTreesDemo serves the /api/trees endpoint from demo data.
func handleTreesDemo(dg *demoGenerator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		dg.mu.Lock()
		dg.mutateFleet()

		fleet := FleetAggregate{TotalTrees: len(dg.trees)}
		trees := make([]DashboardTreeSummary, 0, len(dg.trees))

		for i := range dg.trees {
			s := summarizeTree(&dg.trees[i])
			trees = append(trees, s)

			fleet.TotalLeaves += s.LeafCount
			fleet.TotalNodes += s.TotalNodes
			fleet.OccupiedLeaves += s.OccupiedLeaves
			if s.Synced {
				fleet.SyncedTrees++
			}
			if s.NodeReadyFailed {
				fleet.NodeReadyFailed++
			}
		}

		for i := range dg.trees {
			cr := &dg.trees[i]
			if cr.Status.Root != nil {
				var acc summaryAccumulator
				acc.worstState = partitionv1beta1.PartitionStateHealthy
				walkForSummary(cr.Status.Root, &acc, "")
				fleet.HealthyLeaves += acc.healthyLeaves
				fleet.DegradedLeaves += acc.degradedLeaves
				fleet.UnhealthyLeaves += acc.unhealthyLeaves
				fleet.IncompleteLeaves += acc.incompleteLeaves
			}
		}

		dg.mu.Unlock()

		if fleet.TotalLeaves > 0 {
			fleet.OccupancyPercent = float64(fleet.OccupiedLeaves) / float64(fleet.TotalLeaves) * 100
		}

		resp := TreesResponse{
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Fleet:     fleet,
			Trees:     trees,
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}

// handleTreeDetailDemo serves /api/trees/{name} from demo data.
func handleTreeDetailDemo(dg *demoGenerator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if name == "" {
			http.Error(w, "missing tree name", http.StatusBadRequest)
			return
		}

		dg.mu.Lock()
		var found *partitionv1beta1.PartitionTree
		for i := range dg.trees {
			if dg.trees[i].Name == name {
				found = &dg.trees[i]
				break
			}
		}

		if found == nil {
			dg.mu.Unlock()
			http.Error(w, fmt.Sprintf("tree %q not found", name), http.StatusNotFound)
			return
		}

		// Build response while holding lock.
		var root *TreeDetailPartition
		if found.Status.Root != nil {
			built := buildDetailTree(found.Status.Root)
			root = &built
		}

		events := dg.generateDemoEvents(name, found)

		resp := TreeDetailResponse{
			Name:            found.Name,
			AcceleratorType: found.Status.AcceleratorType,
			Synced:          isSynced(found),
			SyncedReason:    syncedReason(found),
			Root:            root,
			Events:          events,
		}
		dg.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}
