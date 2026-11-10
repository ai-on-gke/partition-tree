# Partition Tree Metrics

This document defines the OpenTelemetry and Cloud Monitoring metrics emitted by the Partition Tree controller, detailing the attribute mapping between the **Customer Stream** (Google Cloud Managed Service for Prometheus / GMP via `prometheus_target`) and the **Google Internal Stream** (Cloud Monitoring via `k8s_entity` and `k8s_container`).

---

## Metric Streams Overview

The controller emits metrics via two parallel pipelines:
1. **Customer Stream (GMP / Prometheus)**:
   - Targets the `prometheus_target` Monitored Resource (`[project_id, location, cluster, namespace, job, instance]`).
   - Uses `StaticInstance = "partition-controller"` for the `instance` label to prevent time-series churn across controller pod restarts.
   - Stream prefix: `prometheus.googleapis.com/partition_tree/...`
2. **Google Internal Stream (Cloud Monitoring)**:
   - Targets the `k8s_entity` Monitored Resource (`[project_id, location, cluster_name, entity_type, entity_namespace, entity_name, entity_uid]`) for domain metrics (`occupancy`, `taint`, `info`). The exporter dynamically promotes `partition_tree_name` and `partition_tree_uid` to `entity_name` and `entity_uid`, stripping them from the metric payload to avoid Cloud Monitoring descriptor schema conflicts.
   - Targets the `k8s_container` Monitored Resource (`[project_id, location, cluster_name, namespace_name, pod_name, container_name]`) for `build_info`, mapping controller pod and container identity directly into standard GKE container monitored resource labels and stripping `pod_name` from metric dimensions to avoid descriptor collision.
   - Stream prefix: `kubernetes.io/internal/addons/partition_tree/...`

---

## Metrics & Attribute Specifications

### 1. `partition_tree_occupancy`
* **Customer Metric**: `prometheus.googleapis.com/partition_tree/occupancy/gauge` (Resource: `prometheus_target`, PromQL: `partition_tree_occupancy`)
* **Google Metric**: `kubernetes.io/internal/addons/partition_tree/occupancy` (Resource: `k8s_entity`)
* **Type**: Observable Gauge (`int64`, Unit: `1`)
* **Value**: `1` if the partition is in this `state`, `0` otherwise (one-hot encoding with TTL tombstoning).

| Customer Attr | Google Attr | Description |
| :--- | :--- | :--- |
| `project_id` | *(same)* | GCP Project ID from GCE metadata (resource label on both streams). |
| `location` | *(same)* | Cluster region or zone from GCE metadata (resource label on both streams). |
| `cluster` | `cluster_name` | Cluster name (GMP `prometheus_target` expects `cluster`; Cloud Monitoring `k8s_entity` expects `cluster_name`). |
| `namespace` | `entity_namespace` | Controller pod namespace on customer stream; empty string `""` on Google stream (cluster-scoped CR). |
| `job` | `entity_type` | Service name (`"partition-controller"`) on customer stream; resource kind (`"partitiontree"`) on Google stream. |
| `instance` | *(omitted)* | Static service identity (`"partition-controller"`) on customer stream; omitted on Google (`k8s_entity` has no `instance` label). |
| `partition_tree_name` | `entity_name` | Name of the `PartitionTree` CR. Emitted as a metric attribute on customer stream; promoted to `k8s_entity.entity_name` resource label on Google stream (omitted from Google metric attributes). |
| `partition_tree_uid` | `entity_uid` | Kubernetes UID of the `PartitionTree` CR. Emitted as a metric attribute on customer stream; promoted to `k8s_entity.entity_uid` resource label on Google stream (omitted from Google metric attributes). |
| `partition_id` | *(same)* | Unique identifier of the partition node within the tree topology. |
| `topology` | *(same)* | Physical TPU topology shape of this partition node (e.g. `2x2x1`, `4x4x4`). |
| `state` | *(same)* | Occupancy state enum of the partition (e.g. `Occupied`, `Empty`, `Allocated`). |

---

### 2. `partition_tree_taint`
* **Customer Metric**: `prometheus.googleapis.com/partition_tree/taint/gauge` (Resource: `prometheus_target`, PromQL: `partition_tree_taint`)
* **Google Metric**: `kubernetes.io/internal/addons/partition_tree/taint` (Resource: `k8s_entity`)
* **Type**: Observable Gauge (`int64`, Unit: `1`)

| Customer Attr | Google Attr | Description |
| :--- | :--- | :--- |
| `project_id` | *(same)* | GCP Project ID from GCE metadata (resource label on both streams). |
| `location` | *(same)* | Cluster region or zone from GCE metadata (resource label on both streams). |
| `cluster` | `cluster_name` | Cluster name (GMP `prometheus_target` expects `cluster`; Cloud Monitoring `k8s_entity` expects `cluster_name`). |
| `namespace` | `entity_namespace` | Controller pod namespace on customer stream; empty string `""` on Google stream (cluster-scoped CR). |
| `job` | `entity_type` | Service name (`"partition-controller"`) on customer stream; resource kind (`"partitiontree"`) on Google stream. |
| `instance` | *(omitted)* | Static service identity (`"partition-controller"`) on customer stream; omitted on Google (`k8s_entity` has no `instance` label). |
| `partition_tree_name` | `entity_name` | Name of the `PartitionTree` CR. Emitted as a metric attribute on customer stream; promoted to `k8s_entity.entity_name` resource label on Google stream (omitted from Google metric attributes). |
| `partition_tree_uid` | `entity_uid` | Kubernetes UID of the `PartitionTree` CR. Emitted as a metric attribute on customer stream; promoted to `k8s_entity.entity_uid` resource label on Google stream (omitted from Google metric attributes). |
| `partition_id` | *(same)* | Identifier of the partition node carrying the taint. |
| `topology` | *(same)* | Physical TPU topology shape of the tainted partition. |
| `taint_key` | *(same)* | Key of the partition taint (e.g. node health condition, drain trigger). |
| `taint_value` | *(same)* | Value associated with the partition taint key. |
| `taint_effect` | *(same)* | Kubernetes taint effect enum (e.g. `NoSchedule`, `PreferNoSchedule`, `NoExecute`). |

---

### 3. `partition_tree_info`
* **Customer Metric**: `prometheus.googleapis.com/partition_tree/info/gauge` (Resource: `prometheus_target`, PromQL: `partition_tree_info`)
* **Google Metric**: `kubernetes.io/internal/addons/partition_tree/info` (Resource: `k8s_entity`)
* **Type**: Observable Gauge (`int64`, Unit: `1`, Constant Value: `1`)

| Customer Attr | Google Attr | Description |
| :--- | :--- | :--- |
| `project_id` | *(same)* | GCP Project ID from GCE metadata (resource label on both streams). |
| `location` | *(same)* | Cluster region or zone from GCE metadata (resource label on both streams). |
| `cluster` | `cluster_name` | Cluster name (GMP `prometheus_target` expects `cluster`; Cloud Monitoring `k8s_entity` expects `cluster_name`). |
| `namespace` | `entity_namespace` | Controller pod namespace on customer stream; empty string `""` on Google stream (cluster-scoped CR). |
| `job` | `entity_type` | Service name (`"partition-controller"`) on customer stream; resource kind (`"partitiontree"`) on Google stream. |
| `instance` | *(omitted)* | Static service identity (`"partition-controller"`) on customer stream; omitted on Google (`k8s_entity` has no `instance` label). |
| `partition_tree_name` | `entity_name` | Name of the `PartitionTree` CR. Emitted as a metric attribute on customer stream; promoted to `k8s_entity.entity_name` resource label on Google stream (omitted from Google metric attributes). |
| `partition_tree_uid` | `entity_uid` | Kubernetes UID of the `PartitionTree` CR. Emitted as a metric attribute on customer stream; promoted to `k8s_entity.entity_uid` resource label on Google stream (omitted from Google metric attributes). |
| `root_partition_id` | *(same)* | Identifier of the root partition node for the partition tree. |
| `node_pool_name` | *(same)* | GKE node pool name backing this partition tree. |
| `mig_name` | *(same)* | Compute Engine Managed Instance Group (MIG) URL/name backing the node pool. |
| `accelerator_type` | *(same)* | TPU hardware generation and accelerator type (e.g. `tpu-v4`, `tpu-v5p`). |
| `tpu_pod_id` | *(same)* | Physical TPU Pod slice identifier from GCE topology metadata. |
| `topology` | *(same)* | Root TPU topology shape for the partition tree. |

---

### 4. `partition_tree_build_info`
* **Customer Metric**: `prometheus.googleapis.com/partition_tree/build_info/gauge` (Resource: `prometheus_target`, PromQL: `partition_tree_build_info`)
* **Google Metric**: `kubernetes.io/internal/addons/partition_tree/build_info` (Resource: `k8s_container`)
* **Type**: Observable Gauge (`int64`, Unit: `1`, Constant Value: `1`)

| Customer Attr | Google Attr | Description |
| :--- | :--- | :--- |
| `project_id` | *(same)* | GCP Project ID from GCE metadata (resource label on both streams). |
| `location` | *(same)* | Cluster region or zone from GCE metadata (resource label on both streams). |
| `cluster` | `cluster_name` | Cluster name (GMP `prometheus_target` expects `cluster`; Cloud Monitoring `k8s_container` expects `cluster_name`). |
| `namespace` | `namespace_name` | Controller pod namespace (resource label on both streams: `namespace` on `prometheus_target`, `namespace_name` on `k8s_container`). |
| `job` | `container_name` | Controller service identity (`"partition-controller"`). Mapped to `job` on `prometheus_target` and `container_name` on `k8s_container`. |
| `instance` | *(omitted)* | Static service identity (`"partition-controller"`) on customer stream; omitted on Google (`k8s_container` has no `instance` label). |
| `pod_name` | *(same)* | Controller replica pod name from Kubernetes downward API (`POD_NAME`). Emitted as a metric attribute on customer stream; promoted to `k8s_container.pod_name` resource label on Google stream (omitted from Google metric attributes to avoid descriptor collision). |
| `git_version` | *(same)* | Semantic release version of the partition controller binary. |
| `git_commit` | *(same)* | Git SHA-1 commit hash of the compiled controller binary. |
| `build_date` | *(same)* | ISO 8601 build timestamp of the binary. |
| `go_version` | *(same)* | Go toolchain runtime version (e.g. `go1.24.0`). |
| `compiler` | *(same)* | Go compiler implementation (`gc`). |
| `platform` | *(same)* | Operating system and CPU architecture target (e.g. `linux/amd64`). |

---

### 5. `partition_tree_metrics_production_duration_seconds`
* **Customer Metric**: `prometheus.googleapis.com/partition_tree/metrics_production_duration_seconds/histogram` (Resource: `prometheus_target`, PromQL: `partition_tree_metrics_production_duration_seconds`)
* **Google Metric**: *(None — emitted strictly as a customer-facing metric)*
* **Type**: Histogram (`float64`, Unit: `s`, Buckets: `{0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 60.0, 90.0, 120.0}`)
* **Description**: Duration in seconds to produce PartitionTree metrics for the entire cluster, labeled by phase: 'observe' (in-memory tree walk and tombstone pruning), 'export' (batching and network dispatch), and 'total' (end-to-end collection cycle).

| Customer Attr | Description |
| :--- | :--- |
| `project_id` | GCP Project ID from GCE metadata (resource label on `prometheus_target`). |
| `location` | Cluster region or zone from GCE metadata (resource label on `prometheus_target`). |
| `cluster` | Cluster name (resource label on `prometheus_target`). |
| `namespace` | Controller pod namespace (resource label on `prometheus_target`). |
| `job` | Controller service identity (`"partition-controller"`). |
| `instance` | Static service identity (`"partition-controller"`). |
| `stream` | Pipeline stream being measured (`"customer"` or `"google"`). |
| `phase` | Stage of cluster metric production: `"observe"` (in-memory OTel callback & tombstone pruning pass across all cluster trees), `"export"` (network export & batching pass), or `"total"` (true end-to-end wall-clock time from observation start to export completion). |
| `status` | Execution status of the phase (`"success"` or `"error"`). |

---

## Operational Characteristics

### Time-Series Churn Prevention
Dynamic controller pod names (`pod_name`) are strictly isolated to `partition_tree_build_info` (as a metric attribute on customer `prometheus_target` and as a resource label on Google `k8s_container`). Domain metrics (`occupancy`, `taint`, `info`) identify only the Kubernetes `PartitionTree` custom resource under `k8s_entity`. When a controller restarts, upgrades, or reschedules to another node:
- Domain metric time series remain completely invariant (no churn).
- Only `partition_tree_build_info` transitions to the new pod name.
- PromQL joins (`partition_tree_info * on() group_left(pod_name) partition_tree_build_info`) allow operators to correlate domain series with specific controller pods when debugging.

### Metric Lifecycle & Tombstoning
When a `PartitionTree` custom resource is deleted from Kubernetes:
- The controller emits a value of `0` to signal deallocation.
- `TTLTombstoneGaugeSet` retains the zero-value data point for the duration configured by `--metrics-tombstone-ttl` (default: 30 minutes).
- After the TTL expires, the entry is automatically purged from memory, allowing the metric series to age out cleanly in both Prometheus and Cloud Monitoring without creating permanent zombie series.

### Cardinality Limits
- The MeterProvider enforces a global cardinality limit per instrument configured via `--metric-cardinality-limit` (default: 100,000).
- This guards against high-cardinality label explosions across large multi-slice fleets.

---

## Export Modes & Flag Configuration

The controller supports three transport options for customer metrics, alongside an independent Google-internal direct stream.

> **IAM & API Prerequisites for Direct Cloud Monitoring Export:**
> Whenever `--enable-google-internal-metrics=true` (the default) or `--metrics-direct-push=true`, the controller calls `google.monitoring.v3.MetricService.CreateTimeSeries` directly from the controller pod using Application Default Credentials (Workload Identity or the GKE node service account):
> - **Required API**: `monitoring.googleapis.com` enabled on the target project.
> - **Required IAM Role**: **`roles/monitoring.metricWriter`** (`monitoring.timeSeries.create`) granted on the project to the controller's GCP service account (in addition to `roles/compute.viewer` for GCE topology polling).
> - **Failure Mode**: If `roles/monitoring.metricWriter` is missing, `PartitionTree` reconciliation remains `Synced=True`, while background metric exports fail every 60s with HTTP `403` / gRPC `PermissionDenied` and no metrics land in Cloud Monitoring.

### 1. Direct Push to Cloud Monitoring (`--metrics-direct-push`)
* **Flag**: `--metrics-direct-push=true` (default: `false`)
* **Required IAM**: `roles/monitoring.metricWriter` (`monitoring.timeSeries.create`) on the controller's GCP service account.
* **How it works**: The controller pushes customer metrics directly in-process to Google Cloud Monitoring (`monitoring.googleapis.com`) using `gcmetric` under the `prometheus_target` Monitored Resource, formatting metric descriptor types with the `/gauge` suffix for complete parity with Google Managed Prometheus and the OpenTelemetry Telemetry API.
* **When to use**: When running on GKE and you want GMP/Cloud Monitoring metrics without deploying or maintaining an OpenTelemetry Collector daemonset or setting up Prometheus scrapers.
* **Mutual Exclusivity**: `--metrics-direct-push` and `--otlp-endpoint` cannot both be specified.

### 2. OpenTelemetry Collector Push (`--otlp-endpoint`)
* **Flag**: `--otlp-endpoint=<host:port>` (default: `""`, e.g. `opentelemetry-collector.gke-managed-otel.svc.cluster.local:4317`)
* **How it works**: Customer metrics are pushed via standard OTLP/gRPC to an external or in-cluster OpenTelemetry collector.
* **When to use**: When the cluster already has a centralized telemetry pipeline (e.g. GKE Managed OTel, Datadog, Dynatrace) routing metrics.

### 3. Prometheus Pull Scraping (`--metrics-bind-address`)
* **Flag**: `--metrics-bind-address=:<port>` (e.g. `:8443` or `:8080`, default `"0"` disabled)
* **How it works**: Serves a Prometheus `/metrics` HTTP(S) endpoint.
* **When to use**: When Prometheus, VictoriaMetrics, or GMP `PodMonitoring` custom resources scrape the controller directly. Can be used alongside `--metrics-direct-push` for local debugging.

### 4. Google Internal Metrics (`--enable-google-internal-metrics`)
* **Flag**: `--enable-google-internal-metrics=true` (default: `true`)
* **Required IAM**: `roles/monitoring.metricWriter` (`monitoring.timeSeries.create`) on the controller's GCP service account.
* **How it works**: Directly pushes `kubernetes.io/internal/addons/...` metrics in-process to Cloud Monitoring using `k8s_entity` and `k8s_container` resource types.

