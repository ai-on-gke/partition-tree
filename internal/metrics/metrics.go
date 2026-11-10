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

// Package metrics provides OpenTelemetry instrumentation, metric registration,
// and Cloud Monitoring export for PartitionTree controllers.
package metrics

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	gcmetric "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
	"github.com/ai-on-gke/partition-tree/internal/metadata"
	"github.com/ai-on-gke/partition-tree/internal/version"
)

// OpenTelemetry instrument names.
const (
	InstrumentOccupancy                 = "partition_tree_occupancy"
	InstrumentTaint                     = "partition_tree_taint"
	InstrumentInfo                      = "partition_tree_info"
	InstrumentBuildInfo                 = "partition_tree_build_info"
	InstrumentMetricsProductionDuration = "partition_tree_metrics_production_duration_seconds"
)

// Google internal metric stream names (Cloud Monitoring).
const (
	MetricGooglePartitionTreeOccupancy = "kubernetes.io/internal/addons/partition_tree/occupancy"
	MetricGooglePartitionTreeTaint     = "kubernetes.io/internal/addons/partition_tree/taint"
	MetricGooglePartitionTreeInfo      = "kubernetes.io/internal/addons/partition_tree/info"
	MetricGooglePartitionTreeBuildInfo = "kubernetes.io/internal/addons/partition_tree/build_info"
)

// Customer metric stream names (Google Cloud Managed Service for Prometheus).
const (
	MetricCustomerPartitionTreeOccupancy                 = "prometheus.googleapis.com/partition_tree/occupancy"
	MetricCustomerPartitionTreeTaint                     = "prometheus.googleapis.com/partition_tree/taint"
	MetricCustomerPartitionTreeInfo                      = "prometheus.googleapis.com/partition_tree/info"
	MetricCustomerPartitionTreeBuildInfo                 = "prometheus.googleapis.com/partition_tree/build_info"
	MetricCustomerPartitionTreeMetricsProductionDuration = "prometheus.googleapis.com/partition_tree/metrics_production_duration_seconds"
)

// Monitored resource types and target identities.
const (
	ServiceName = "partition-controller"

	// ResourceTypeGoogle ("k8s_entity") identifies Kubernetes objects. Domain metrics identify
	// PartitionTree CRs without an instance label, making them invariant to controller pod restarts.
	ResourceTypeGoogle      = "k8s_entity"
	EntityTypePartitionTree = "partitiontree"

	// ResourceTypeGoogleContainer ("k8s_container") identifies the running controller daemon container.
	// Used for build_info to correlate controller version and instance lifecycle with container logs and system metrics.
	ResourceTypeGoogleContainer = "k8s_container"

	// ResourceTypeCustomer ("prometheus_target") requires [project_id, location, cluster, namespace, job, instance].
	// StaticInstance is used for "instance" to prevent time-series churn; dynamic pod identity is published on build_info instead.
	ResourceTypeCustomer = "prometheus_target"
	StaticInstance       = "partition-controller"
)

// Attribute keys for metric dimensions and resource labels.
const (
	// Resource attribute keys.
	Attr_gcp_resource_type = "gcp.resource_type"
	Attr_project_id        = "project_id"
	Attr_location          = "location"
	Attr_cluster_name      = "cluster_name"
	Attr_cluster           = "cluster"
	Attr_namespace         = "namespace"
	Attr_namespace_name    = "namespace_name"
	Attr_container_name    = "container_name"
	Attr_job               = "job"
	Attr_instance          = "instance"
	Attr_entity_type       = "entity_type"
	Attr_entity_namespace  = "entity_namespace"
	Attr_entity_name       = "entity_name"
	Attr_entity_uid        = "entity_uid"

	// Domain metric dimension keys.
	Attr_partition_tree_name = "partition_tree_name"
	Attr_partition_tree_uid  = "partition_tree_uid"
	Attr_node_pool_name      = "node_pool_name"
	Attr_mig_name            = "mig_name"
	Attr_accelerator_type    = "accelerator_type"
	Attr_tpu_pod_id          = "tpu_pod_id"
	Attr_topology            = "topology"
	Attr_partition_id        = "partition_id"
	Attr_root_partition_id   = "root_partition_id"
	Attr_state               = "state"
	Attr_taint_effect        = "taint_effect"
	Attr_taint_key           = "taint_key"
	Attr_taint_value         = "taint_value"

	// Controller instance and build metadata keys.
	Attr_build_date  = "build_date"
	Attr_compiler    = "compiler"
	Attr_git_commit  = "git_commit"
	Attr_git_version = "git_version"
	Attr_go_version  = "go_version"
	Attr_platform    = "platform"
	Attr_pod_name    = "pod_name"

	// Metric production duration dimension keys.
	Attr_stream = "stream"
	Attr_phase  = "phase"
	Attr_status = "status"
)

// Operational defaults.
const (
	NamespacePath       = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	ExportInterval      = 60 * time.Second
	DefaultTombstoneTTL = 30 * time.Minute
)

// Init initializes OpenTelemetry metrics with dynamic prefixes and periodic readers.
func Init(ctx context.Context, endpoint string, directPush bool, meta metadata.GoogleMetadata, ttl time.Duration, cardinalityLimit int, enableCustomer, enableGoogle bool) (func(), error) {
	readerGoogle, err := newGoogleReader(meta, enableGoogle)
	if err != nil {
		return nil, err
	}

	readerCustomer, err := newCustomerReader(ctx, endpoint, directPush, meta, enableCustomer)
	if err != nil {
		return nil, err
	}

	promExporter, err := prometheus.New(
		prometheus.WithRegisterer(crmetrics.Registry),
		prometheus.WithoutUnits(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create prometheus exporter: %w", err)
	}

	return initWithReaders(ctx, readerCustomer, readerGoogle, promExporter, meta, ttl, cardinalityLimit, enableCustomer, enableGoogle)
}

// InitWithReaders initializes the OTEL providers with custom readers.
func InitWithReaders(ctx context.Context, readerCustomer, readerGoogle sdkmetric.Reader, meta metadata.GoogleMetadata, ttl time.Duration, cardinalityLimit int, enableCustomer, enableGoogle bool) (func(), error) {
	return initWithReaders(ctx, readerCustomer, readerGoogle, nil, meta, ttl, cardinalityLimit, enableCustomer, enableGoogle)
}

func initWithReaders(ctx context.Context, readerCustomer, readerGoogle, promExporter sdkmetric.Reader, meta metadata.GoogleMetadata, ttl time.Duration, cardinalityLimit int, enableCustomer, enableGoogle bool) (func(), error) {
	globalTaintGaugeSet.SetTTL(ttl)
	globalOccupancyGaugeSet.SetTTL(ttl)
	globalInfoGaugeSet.SetTTL(ttl)
	globalGoogleTracker.reset()
	globalCustomerTracker.reset()
	globalProductionRecorder.setHistogram(nil)

	type providerEntry struct {
		provider   *sdkmetric.MeterProvider
		streamName string
		isCustomer bool
	}
	var entries []providerEntry
	var globalProvider *sdkmetric.MeterProvider

	// Initialize customer provider first so globalProductionRecorder's histogram
	// is registered before any collection callbacks run.
	if enableCustomer {
		resCustomer, err := buildCustomerResource(ctx, meta)
		if err != nil {
			return nil, err
		}
		// When customer metrics are enabled, promExporter attaches here so :8443/metrics
		// exposes customer-visible metric streams.
		p := newMeterProvider(resCustomer, readerCustomer, promExporter, streamNames{
			occupancy:          MetricCustomerPartitionTreeOccupancy,
			info:               MetricCustomerPartitionTreeInfo,
			taint:              MetricCustomerPartitionTreeTaint,
			buildInfo:          MetricCustomerPartitionTreeBuildInfo,
			productionDuration: MetricCustomerPartitionTreeMetricsProductionDuration,
		}, cardinalityLimit)
		entries = append(entries, providerEntry{provider: p, streamName: StreamCustomer, isCustomer: true})
		globalProvider = p
	}

	if enableGoogle {
		resGoogle, err := buildGoogleResource(ctx, meta)
		if err != nil {
			return nil, err
		}
		// If customer metrics are disabled, attach the Prometheus exporter to the
		// Google provider so :8443/metrics still exposes internal metrics.
		var promReader sdkmetric.Reader
		if promExporter != nil && !enableCustomer {
			promReader = promExporter
		}
		p := newMeterProvider(resGoogle, readerGoogle, promReader, streamNames{
			occupancy: MetricGooglePartitionTreeOccupancy,
			info:      MetricGooglePartitionTreeInfo,
			taint:     MetricGooglePartitionTreeTaint,
			buildInfo: MetricGooglePartitionTreeBuildInfo,
		}, cardinalityLimit)
		entries = append(entries, providerEntry{provider: p, streamName: StreamGoogle, isCustomer: false})
		if globalProvider == nil {
			globalProvider = p
		}
	}

	// Register global MeterProvider (preferring customer-facing provider if enabled).
	// Note: both Google and Customer providers remain active independently in entries,
	// registering their own instruments and background periodic export readers.
	if globalProvider != nil {
		otel.SetMeterProvider(globalProvider)
	}

	for _, entry := range entries {
		meter := entry.provider.Meter(ServiceName)
		if err := registerInstruments(meter, entry.streamName, entry.isCustomer); err != nil {
			return nil, err
		}
	}

	ctxSweeper, cancelSweeper := context.WithCancel(context.Background())
	go startTombstoneSweeper(ctxSweeper, 1*time.Minute, 15*time.Minute)

	shutdown := func() {
		cancelSweeper()
		for _, entry := range entries {
			if err := entry.provider.Shutdown(context.Background()); err != nil {
				otel.Handle(err)
			}
		}
	}

	return shutdown, nil
}

// startTombstoneSweeper runs a background ticker that prunes expired tombstones
// if no metric read callback has occurred across any reader in over staleThreshold (15 minutes).
func startTombstoneSweeper(ctx context.Context, checkInterval, staleThreshold time.Duration) {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			globalTaintGaugeSet.pruneIfStale(staleThreshold, now)
			globalOccupancyGaugeSet.pruneIfStale(staleThreshold, now)
			globalInfoGaugeSet.pruneIfStale(staleThreshold, now)
		}
	}
}

func newGoogleReader(meta metadata.GoogleMetadata, enable bool) (sdkmetric.Reader, error) {
	if !enable || meta.ProjectID == "" || meta.ProjectID == "mock" {
		return sdkmetric.NewManualReader(), nil
	}

	k8sEntityLabels := []string{
		Attr_project_id,
		Attr_location,
		Attr_cluster_name,
		Attr_entity_type,
		Attr_entity_namespace,
		Attr_entity_name,
		Attr_entity_uid,
	}
	entityExporter, err := gcmetric.New(
		gcmetric.WithProjectID(meta.ProjectID),
		gcmetric.WithDisableCreateMetricDescriptors(),
		gcmetric.WithMetricDescriptorTypeFormatter(func(m metricdata.Metrics) string { return m.Name }),
		gcmetric.WithFilteredResourceAttributes(gcmetric.NoAttributes),
		gcmetric.WithMonitoredResourceDescription(ResourceTypeGoogle, k8sEntityLabels),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCP exporter for %s: %w", ResourceTypeGoogle, err)
	}

	k8sContainerLabels := []string{
		Attr_project_id,
		Attr_location,
		Attr_cluster_name,
		Attr_namespace_name,
		Attr_pod_name,
		Attr_container_name,
	}
	containerExporter, err := gcmetric.New(
		gcmetric.WithProjectID(meta.ProjectID),
		gcmetric.WithDisableCreateMetricDescriptors(),
		gcmetric.WithMetricDescriptorTypeFormatter(func(m metricdata.Metrics) string { return m.Name }),
		gcmetric.WithFilteredResourceAttributes(gcmetric.NoAttributes),
		gcmetric.WithMonitoredResourceDescription(ResourceTypeGoogleContainer, k8sContainerLabels),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCP exporter for %s: %w", ResourceTypeGoogleContainer, err)
	}

	splittingExporter := NewGoogleExporter(entityExporter, containerExporter, meta)
	return sdkmetric.NewPeriodicReader(WrapExporter(splittingExporter, StreamGoogle), sdkmetric.WithInterval(ExportInterval)), nil
}

func newCustomerReader(ctx context.Context, endpoint string, directPush bool, meta metadata.GoogleMetadata, enable bool) (sdkmetric.Reader, error) {
	if !enable {
		return sdkmetric.NewManualReader(), nil
	}

	if directPush && endpoint != "" {
		return nil, fmt.Errorf("metrics direct push and OTLP endpoint cannot both be configured")
	}

	if directPush {
		if meta.ProjectID == "" || meta.ProjectID == "mock" {
			return sdkmetric.NewManualReader(), nil
		}

		customerLabels := []string{
			Attr_project_id,
			Attr_location,
			Attr_cluster,
			Attr_namespace,
			Attr_job,
			Attr_instance,
		}
		customerExporter, err := gcmetric.New(
			gcmetric.WithProjectID(meta.ProjectID),
			gcmetric.WithDisableCreateMetricDescriptors(),
			gcmetric.WithMetricDescriptorTypeFormatter(formatCustomerMetricName),
			gcmetric.WithFilteredResourceAttributes(gcmetric.NoAttributes),
			gcmetric.WithMonitoredResourceDescription(ResourceTypeCustomer, customerLabels),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create GCP exporter for %s: %w", ResourceTypeCustomer, err)
		}
		batchExporter := NewConcurrentBatchExporter(customerExporter)
		return sdkmetric.NewPeriodicReader(WrapExporter(batchExporter, StreamCustomer), sdkmetric.WithInterval(ExportInterval)), nil
	}

	if endpoint == "" {
		return sdkmetric.NewManualReader(), nil
	}

	exporterCustomer, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(endpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter for customer metrics: %w", err)
	}
	return sdkmetric.NewPeriodicReader(WrapExporter(exporterCustomer, StreamCustomer), sdkmetric.WithInterval(ExportInterval)), nil
}

// formatCustomerMetricName formats the Cloud Monitoring metric descriptor type for customer metrics,
// appending "/histogram" for histogram instruments and "/gauge" for gauges to ensure parity with
// Google Managed Prometheus and the OpenTelemetry Telemetry API.
func formatCustomerMetricName(m metricdata.Metrics) string {
	switch m.Data.(type) {
	case metricdata.Histogram[float64], metricdata.Histogram[int64], metricdata.ExponentialHistogram[float64], metricdata.ExponentialHistogram[int64]:
		if strings.HasSuffix(m.Name, "/histogram") {
			return m.Name
		}
		return m.Name + "/histogram"
	default:
		if strings.HasSuffix(m.Name, "/gauge") {
			return m.Name
		}
		return m.Name + "/gauge"
	}
}

type streamNames struct {
	occupancy          string
	info               string
	taint              string
	buildInfo          string
	productionDuration string
}

func newMeterProvider(res *resource.Resource, reader sdkmetric.Reader, promReader sdkmetric.Reader, streams streamNames, cardinalityLimit int) *sdkmetric.MeterProvider {
	viewOccupancy := sdkmetric.NewView(sdkmetric.Instrument{Name: InstrumentOccupancy}, sdkmetric.Stream{Name: streams.occupancy})
	viewInfo := sdkmetric.NewView(sdkmetric.Instrument{Name: InstrumentInfo}, sdkmetric.Stream{Name: streams.info})
	viewTaint := sdkmetric.NewView(sdkmetric.Instrument{Name: InstrumentTaint}, sdkmetric.Stream{Name: streams.taint})
	viewBuildInfo := sdkmetric.NewView(sdkmetric.Instrument{Name: InstrumentBuildInfo}, sdkmetric.Stream{Name: streams.buildInfo})

	views := []sdkmetric.View{viewOccupancy, viewTaint, viewInfo, viewBuildInfo}
	if streams.productionDuration != "" {
		viewProdDuration := sdkmetric.NewView(
			sdkmetric.Instrument{Name: InstrumentMetricsProductionDuration},
			sdkmetric.Stream{
				Name: streams.productionDuration,
				Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
					Boundaries: DefaultProductionDurationBuckets,
				},
			},
		)
		views = append(views, viewProdDuration)
	}
	dropAll := sdkmetric.NewView(
		sdkmetric.Instrument{Name: "*"},
		sdkmetric.Stream{Aggregation: sdkmetric.AggregationDrop{}},
	)
	views = append(views, dropAll)

	opts := []sdkmetric.Option{
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(reader),
		sdkmetric.WithView(views...),
		sdkmetric.WithCardinalityLimit(cardinalityLimit),
	}
	if promReader != nil {
		opts = append(opts, sdkmetric.WithReader(promReader))
	}
	return sdkmetric.NewMeterProvider(opts...)
}

func registerInstruments(meter metric.Meter, streamName string, isCustomer bool) error {
	occ, err := meter.Int64ObservableGauge(InstrumentOccupancy, metric.WithDescription("Occupancy state"), metric.WithUnit("1"))
	if err != nil {
		return err
	}
	taint, err := meter.Int64ObservableGauge(InstrumentTaint, metric.WithDescription("Taint present"), metric.WithUnit("1"))
	if err != nil {
		return err
	}
	info, err := meter.Int64ObservableGauge(InstrumentInfo, metric.WithDescription("Info"), metric.WithUnit("1"))
	if err != nil {
		return err
	}
	// build_info serves as the controller instance descriptor (constant value 1).
	// Exactly one time-series exists per running controller replica. By attaching dynamic
	// instance metadata (pod_name) and static build metadata (git_version, git_commit, etc.)
	// to this single metric, domain metrics remain completely static across pod restarts while
	// operators can still perform 1-to-1 PromQL label joins on pod_name when needed.
	buildInfo, err := meter.Int64ObservableGauge(
		InstrumentBuildInfo,
		metric.WithDescription("Partition Tree build and instance information. 1 labeled by git version, git commit, build date, go version, compiler, platform, pod name"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return err
	}

	if isCustomer {
		prodDurationHist, err := meter.Float64Histogram(
			InstrumentMetricsProductionDuration,
			metric.WithDescription("Duration in seconds to produce PartitionTree metrics for the entire cluster, labeled by phase: 'observe' (in-memory tree walk and tombstone pruning), 'export' (batching and network dispatch), and 'total' (end-to-end collection cycle)"),
			metric.WithUnit("s"),
		)
		if err != nil {
			return err
		}
		globalProductionRecorder.setHistogram(prodDurationHist)
	}

	tracker := trackerForStream(streamName)
	observeCluster := func(ctx context.Context, obs metric.Observer) error {
		start := time.Now()
		tracker.setStart(start)

		globalTaintGaugeSet.observe(ctx, obs, taint, start)
		globalOccupancyGaugeSet.observe(ctx, obs, occ, start)
		globalInfoGaugeSet.observe(ctx, obs, info, start)

		globalProductionRecorder.record(ctx, streamName, PhaseObserve, StatusSuccess, time.Since(start))
		return nil
	}
	if _, err := meter.RegisterCallback(observeCluster, taint, occ, info); err != nil {
		return err
	}

	vInfo := version.Get()
	buildInfoAttrs := []attribute.KeyValue{
		attribute.String(Attr_build_date, vInfo.BuildDate),
		attribute.String(Attr_compiler, vInfo.Compiler),
		attribute.String(Attr_git_commit, vInfo.GitCommit),
		attribute.String(Attr_git_version, vInfo.GitVersion),
		attribute.String(Attr_go_version, vInfo.GoVersion),
		attribute.String(Attr_platform, vInfo.Platform),
		attribute.String(Attr_pod_name, getPodName()),
	}
	if _, err := meter.RegisterCallback(func(_ context.Context, obs metric.Observer) error {
		obs.ObserveInt64(buildInfo, 1, metric.WithAttributes(buildInfoAttrs...))
		return nil
	}, buildInfo); err != nil {
		return err
	}
	return nil
}

// buildGoogleResource constructs the base k8s_entity resource for Google internal metrics.
// Entity-specific attributes (entity_name, entity_uid) are dynamically populated per PartitionTree CR
// by GoogleExporter during export, while build_info is retargeted to k8s_container.
func buildGoogleResource(ctx context.Context, meta metadata.GoogleMetadata) (*resource.Resource, error) {
	return resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(ServiceName),
			attribute.String(Attr_gcp_resource_type, ResourceTypeGoogle),
			attribute.String(Attr_project_id, meta.ProjectID),
			attribute.String(Attr_location, meta.ClusterLocation),
			attribute.String(Attr_cluster_name, meta.ClusterName),
		),
	)
}

// buildCustomerResource constructs the prometheus_target resource for customer metrics.
// "job" is set to ServiceName and "instance" is set to StaticInstance ("partition-controller")
// to satisfy GMP's required resource schema while ensuring time series do not churn across pod restarts.
func buildCustomerResource(ctx context.Context, meta metadata.GoogleMetadata) (*resource.Resource, error) {
	namespace, _ := readNamespace()

	return resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(ServiceName),
			attribute.String(Attr_gcp_resource_type, ResourceTypeCustomer),
			attribute.String(Attr_project_id, meta.ProjectID),
			attribute.String(Attr_location, meta.ClusterLocation),
			attribute.String(Attr_cluster, meta.ClusterName),
			attribute.String(Attr_namespace, namespace),
			attribute.String(Attr_job, ServiceName),
			attribute.String(Attr_instance, StaticInstance),
		),
	)
}

// readNamespace reads the pod namespace from the service account mount.
func readNamespace() (string, error) {
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns, nil
	}
	data, err := os.ReadFile(NamespacePath)
	if err != nil {
		return "", fmt.Errorf("failed to read namespace: %w", err)
	}
	return strings.TrimSpace(string(data)), nil
}

// getPodName retrieves the controller pod name from the POD_NAME environment variable
// (injected via Kubernetes Downward API) or falls back to os.Hostname().
func getPodName() string {
	if pod := os.Getenv("POD_NAME"); pod != "" {
		return pod
	}
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "unknown"
}

var (
	globalTaintGaugeSet     = newTTLTombstoneGaugeSet(DefaultTombstoneTTL)
	globalOccupancyGaugeSet = newTTLTombstoneGaugeSet(DefaultTombstoneTTL)
	globalInfoGaugeSet      = newTTLTombstoneGaugeSet(DefaultTombstoneTTL)
)

// RecordPartitionTreeMetrics walks a PartitionInfo tree and atomically updates
// both occupancy and taint metrics for the entire tree.
func RecordPartitionTreeMetrics(ctx context.Context, root *partitionv1beta1.PartitionInfo, treeName, treeUID, poolName, migName, accType, podName string) {
	recordPartitionTreeMetricsAt(ctx, root, treeName, treeUID, poolName, migName, accType, podName, time.Now())
}

// recordPartitionTreeMetricsAt supports dependency injection of time for unit testing.
func recordPartitionTreeMetricsAt(ctx context.Context, root *partitionv1beta1.PartitionInfo, treeName, treeUID, poolName, migName, accType, podName string, now time.Time) {
	if root == nil {
		return
	}

	infoAttrs := []attribute.KeyValue{
		attribute.String(Attr_partition_tree_name, treeName),
		attribute.String(Attr_partition_tree_uid, treeUID),
		attribute.String(Attr_root_partition_id, root.PartitionID),
		attribute.String(Attr_node_pool_name, poolName),
		attribute.String(Attr_mig_name, migName),
		attribute.String(Attr_accelerator_type, accType),
		attribute.String(Attr_tpu_pod_id, podName),
		attribute.String(Attr_topology, root.Topology),
	}
	infoItems := []tombstoneItem{{
		attrs: infoAttrs,
	}}

	var occItems []tombstoneItem
	var taintItems []tombstoneItem

	var walk func(node *partitionv1beta1.PartitionInfo)
	walk = func(node *partitionv1beta1.PartitionInfo) {
		occAttrs := []attribute.KeyValue{
			attribute.String(Attr_partition_tree_name, treeName),
			attribute.String(Attr_partition_tree_uid, treeUID),
			attribute.String(Attr_topology, node.Topology),
			attribute.String(Attr_partition_id, node.PartitionID),
			attribute.String(Attr_state, string(node.Occupancy)),
		}
		occItems = append(occItems, tombstoneItem{
			attrs: occAttrs,
		})

		for _, t := range node.Taints {
			taintAttrs := []attribute.KeyValue{
				attribute.String(Attr_partition_tree_name, treeName),
				attribute.String(Attr_partition_tree_uid, treeUID),
				attribute.String(Attr_partition_id, node.PartitionID),
				attribute.String(Attr_topology, node.Topology),
				attribute.String(Attr_taint_effect, string(t.Effect)),
				attribute.String(Attr_taint_key, t.Key),
				attribute.String(Attr_taint_value, t.Value),
			}
			taintItems = append(taintItems, tombstoneItem{
				attrs: taintAttrs,
			})
		}

		for i := range node.ChildPartitions {
			walk(&node.ChildPartitions[i])
		}
	}
	walk(root)

	globalInfoGaugeSet.update(treeName, infoItems, now)
	globalOccupancyGaugeSet.update(treeName, occItems, now)
	globalTaintGaugeSet.update(treeName, taintItems, now)
}

// ClearPartitionTreeMetrics zeroes out all previously recorded metrics for the given PartitionTree CR name
// and marks them as tombstones.
func ClearPartitionTreeMetrics(ctx context.Context, treeName string) {
	clearPartitionTreeMetricsAt(ctx, treeName, time.Now())
}

func clearPartitionTreeMetricsAt(ctx context.Context, treeName string, now time.Time) {
	globalInfoGaugeSet.update(treeName, nil, now)
	globalOccupancyGaugeSet.update(treeName, nil, now)
	globalTaintGaugeSet.update(treeName, nil, now)
}

// ClearGaugeSets is exposed ONLY for unit testing.
func ClearGaugeSets() {
	globalTaintGaugeSet.clear()
	globalInfoGaugeSet.clear()
	globalOccupancyGaugeSet.clear()
	globalGoogleTracker.reset()
	globalCustomerTracker.reset()
}
