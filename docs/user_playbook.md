# User Playbook: PartitionTree

A practical, debugging-focused guide for GKE customers using the
**PartitionTree** feature. This playbook focuses on what you can see
and act on with `kubectl` and standard monitoring tools.

> Every behavioral claim in this playbook is verified against the
> controller's source. If you want to dig deeper, see the
> [Source Map](#appendix-a-source-map) appendix.

---

## Table of Contents

1. [What is a PartitionTree?](#1-what-is-a-partitiontree)
2. [Quick Reference](#2-quick-reference)
3. [Lifecycle: how trees are created, updated, and deleted](#3-lifecycle-how-trees-are-created-updated-and-deleted)
4. [Inspecting your trees](#4-inspecting-your-trees)
5. [Triage decision tree](#5-triage-decision-tree)
6. [Troubleshooting scenarios](#6-troubleshooting-scenarios)
7. [Events & conditions cheatsheet](#7-events--conditions-cheatsheet)
8. [Metrics & monitoring](#8-metrics--monitoring)
9. [Visualization (`ptree-viz`)](#9-visualization-ptree-viz)
10. [FAQ](#10-faq)

---

## 1. What is a PartitionTree?

A `PartitionTree` is a **cluster-scoped** Custom Resource
(`partition.tpu.google.com/v1beta1`) that gives you a live, in-cluster
view of the TPU partition layout of one GKE node pool. For details on
how to define and provision a TPU node pool for dynamic slicing, see
[GKE Dynamic Slicing](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/dynamic-slicing).
The controller queries GCE every few seconds and keeps `.status` fresh,
so you don't have to `gcloud` your way around to understand what's
going on.

> **Decoupled lifecycle mode:** When the controller is started with
> `--decoupled-lifecycle`, CRs are named after the root partition ID
> (instead of the node pool name) and persist across node pool
> deletion/recreation. See [Decoupled Lifecycle](#decoupled-lifecycle)
> below.

You get:

- A **tree of partitions** under `.status.root` reflecting the physical
  TPU topology (e.g. a `4x4x4` cube broken into smaller sub-slices).
- Per-partition **health** (`Healthy` / `Degraded` / `Unhealthy` /
  `Incomplete` / `Unknown`) and **occupancy** (`Occupied` / `Empty` /
  `Unspecified`).
- Per-leaf **`NodeReady`** condition that ties partitions back to
  Kubernetes Node readiness.
- A top-level **`Synced`** condition that tells you whether the view is
  current.
- **Metrics** (`occupancy`, `taint`, `info`, `build_info`) for dashboards and alerts (see [Metrics Documentation](metrics.md)).

**Supported TPUs:** v6e and v7x only. Pools with other TPU generations
are ignored.

### What a healthy CR looks like

```yaml
apiVersion: partition.tpu.google.com/v1beta1
kind: PartitionTree
metadata:
  name: my-tpu-pool                 # = your GKE node pool name (or partition ID in decoupled mode)
  annotations:
    tpu.gke.io/mig-name: projects/1234567890/zones/us-central2-b/instanceGroupManagers/my-tpu-pool-mig
status:
  acceleratorType: v7x
  root:
    partitionId: my-tpu-pool-root
    topology: 4x4x4
    state: Healthy                  # see §7 for the full enum
    occupancy: Empty
    childPartitions:
      - partitionId: my-tpu-pool-root-0
        topology: 2x2x1
        state: Healthy
        occupancy: Empty
        nodeNames: [gke-my-tpu-pool-0-xxxx]   # only on leaves
        conditions:
          - type: NodeReady
            status: "True"
            reason: NodesReady
            message: all expected nodes are Ready
  conditions:                       # only Synced lives at the top
    - type: Synced
      status: "True"
      reason: InSync
      message: last successful poll at 2026-04-11T12:00:00Z
```

### Things worth knowing up front

- The tree is **fully controller-managed** — don't `kubectl apply` or
  edit it by hand.
- `.status.root` is `null` until the first successful poll completes
  (usually a few seconds).
- `nodeNames` appears **only on leaves**.
- The `Synced` condition is the single source of truth for "is what I'm
  looking at fresh?"
- The `NodeReady` condition appears **only on leaves** and is **not**
  bubbled up. To check a non-leaf's overall node health, look at its
  children.

### Which nodes does the controller include?

A node is included in a `PartitionTree` only if **all** of the following
hold:

| Label / annotation                                          | Required value                          |
|-------------------------------------------------------------|-----------------------------------------|
| `cloud.google.com/gke-accelerator-topology-mode` (label)    | `PROVISION_ONLY`                        |
| `cloud.google.com/gke-nodepool` (label)                     | non-empty (becomes the CR name)         |
| `cloud.google.com/gke-tpu-accelerator` (label)              | `tpu7x` **or** `tpu-v6e-slice`          |
| `node.gke.io/created-by-mig` (annotation)                   | non-empty MIG self-link                 |
| `cloud.google.com/gke-tpu-partition-<topology>-id` (label)  | at least one, with a non-empty value    |

If any one of these is missing, the node is silently excluded. Manually
adding labels with `kubectl label` is **not** a fix — GKE will revert
ad-hoc labels. Configure these correctly at node-pool creation time.

---

## 2. Quick Reference

```bash
# List every PartitionTree (cluster-scoped)
kubectl get partitiontrees

# Human-readable summary for one
kubectl describe partitiontree <pool>

# Is the view fresh?
kubectl get partitiontree <pool> \
  -o jsonpath='{.status.conditions[?(@.type=="Synced")]}{"\n"}'

# Dump the tree
kubectl get partitiontree <pool> -o jsonpath='{.status.root}' | jq .

# See controller events for your pool (uses events.k8s.io/v1)
kubectl get events.v1.events.k8s.io --all-namespaces \
  --field-selector regarding.kind=PartitionTree,regarding.name=<pool>

# Confirm what nodes the controller sees
kubectl get nodes \
  -l cloud.google.com/gke-nodepool=<pool>,cloud.google.com/gke-accelerator-topology-mode=PROVISION_ONLY
```

**Read-only access for your team:** the project ships a
`partition-viewer-role` ClusterRole that grants `get/list/watch` on
`partitiontrees` and `get` on `partitiontrees/status`. Bind it to your
users or groups for monitoring access.

---

## 3. Lifecycle: how trees are created, updated, and deleted

### Creation
The controller creates a `PartitionTree` the first time it sees ≥1
matching node in a pool.

- **Default mode**: The CR is named after the
  `cloud.google.com/gke-nodepool` label value.
- **Decoupled mode** (`--decoupled-lifecycle`): The CR is named after
  the sanitized root partition ID from the GCE topology labels.

You'll see a `PartitionTreeCreated` event in both modes.

### Updates
The tree refreshes on:

- **Node add / delete** in a matching pool.
- **Node update** when something meaningful changes (readiness flipped,
  MIG annotation changed, partition-id labels changed, pool label
  changed). Pure heartbeat / status updates are ignored — by design, to
  keep apiserver load low.
- A **periodic poll** of the GCE topology API every few seconds
  (default 1 s ± 100 ms).
- Live **config reloads** of the `partition-config` ConfigMap.

### Deletion

**Default mode**: When the last matching node leaves the pool, the
controller deletes the CR and emits `NodePoolDrained`. If you delete a
CR manually while nodes still exist, the controller recreates it on
the next reconcile.

**Decoupled mode**: When the last matching node leaves the pool, the
controller **preserves** the CR and:
- Clears the MIG annotation
- Sets `Synced=False`, `Reason=PoolDrained`
- Retains the partition tree structure in `.status.root` (stale `NodeNames` and `NodeReady` conditions are cleared on re-provision)
- Suspends GCE polling for the CR
- Emits a `NodePoolDrained` event (with a message indicating the CR is preserved)

When a new node pool is created with nodes in the same physical
partition, the same CR recovers automatically:
- The MIG annotation is updated to the new MIG
- GCE polling resumes
- The tree is rebuilt
- `Synced` transitions back to `True`

---

## 4. Inspecting your trees

### List & summary

```bash
kubectl get partitiontrees
kubectl describe partitiontree <pool>
```

### Is it fresh?

```bash
kubectl get partitiontree <pool> \
  -o jsonpath='{.status.conditions[?(@.type=="Synced")]}{"\n"}'
```

`status: "True"` with `reason: InSync` means the latest GCE poll
succeeded. Anything else: see §7.2.

### Dump the tree (a few useful recipes)

```bash
# Full tree as JSON
kubectl get partitiontree <pool> -o jsonpath='{.status.root}' | jq .

# One line per partition: id, topology, state, occupancy
kubectl get partitiontree <pool> -o json | jq -r '
  .. | objects | select(.partitionId) |
  "\(.partitionId)\t\(.topology)\t\(.state)\t\(.occupancy)"'

# Just the leaves (partitions that own nodes)
kubectl get partitiontree <pool> -o json | jq '
  .. | objects | select(.nodeNames? and (.nodeNames|length) > 0)'
```

### Fleet-wide queries

```bash
# Every CR not currently in sync
kubectl get partitiontrees -o json | jq -r '
  .items[] | select(
    .status.conditions[]? |
    select(.type=="Synced" and .status!="True")
  ) | .metadata.name'

# Every leaf with NodeReady != True, grouped by CR
kubectl get partitiontrees -o json | jq -r '
  .items[] as $pt
  | ($pt.status.root // empty) | .. | objects
  | select(.conditions?[]? | .type=="NodeReady" and .status!="True")
  | "\($pt | .metadata.name)\t\(.partitionId)"' | sort -u
```

### Why isn't my node in the tree?

When a node you expect is missing, check the five filter rules from §1:

```bash
kubectl get node <node> -o json | jq '{
  topo_mode: .metadata.labels["cloud.google.com/gke-accelerator-topology-mode"],
  pool:      .metadata.labels["cloud.google.com/gke-nodepool"],
  accel:     .metadata.labels["cloud.google.com/gke-tpu-accelerator"],
  mig:       .metadata.annotations["node.gke.io/created-by-mig"],
  part_ids:  (.metadata.labels | with_entries(
                select(.key|test("^cloud\\.google\\.com/gke-tpu-partition-.+-id$"))
              )),
  ready:     (.status.conditions[]? | select(.type=="Ready") | .status)
}'
```

You want:
- `topo_mode == "PROVISION_ONLY"`
- `pool` non-empty
- `accel ∈ {"tpu7x", "tpu-v6e-slice"}`
- `mig` non-empty
- at least one entry in `part_ids`

### Watch events for one CR

```bash
kubectl get events.v1.events.k8s.io --all-namespaces \
  --field-selector regarding.kind=PartitionTree,regarding.name=<pool> \
  --sort-by=.metadata.creationTimestamp -w
```

---

## 5. Triage decision tree

```
Symptom you observed
│
├── CR doesn't exist for my pool
│       → §6.1
│
├── CR exists, .status.root is null, no Synced condition yet
│       → §6.2 (first poll hasn't completed)
│
├── Synced=False, "awaiting first successful poll: ..."
│       → §6.3 (first poll is failing)
│
├── Synced=False, "no successful poll within ...: ..."
│       → §6.4 (was healthy, now sync is lost)
│
├── Synced=True but a node I expect is missing from the tree
│       → §6.5 (node doesn't match the filter)
│
├── A leaf shows NodeReady=False
│       → §6.6
│
├── A partition is Degraded / Unhealthy / Incomplete
│       → §6.7
│
├── Occupancy looks wrong
│       → §6.8
│
└── Synced is flapping (frequent SyncLost ↔ SyncRestored)
        → §6.4 (intermittent GCE errors / rate limiting)
```

---

## 6. Troubleshooting scenarios

Each scenario gives **symptoms → what it means → what to do**.

### 6.1 The CR was never created

**Symptoms**
- `kubectl get partitiontree <pool>` returns `NotFound`.
- Nodes exist in the pool.

**What it means**
No node in the pool passed the filter (the five rules in §1). The
controller doesn't emit an event in this case — the nodes are just
quietly ignored.

**What to do**
Run the inspection in §4 ("Why isn't my node in the tree?") on a few
nodes. The most common causes, roughly in order:

1. `cloud.google.com/gke-accelerator-topology-mode` is missing or not
   exactly `PROVISION_ONLY`.
2. `node.gke.io/created-by-mig` is missing (node wasn't created via a
   MIG).
3. `cloud.google.com/gke-tpu-accelerator` is something other than
   `tpu7x` or `tpu-v6e-slice` (e.g. v5e/v5p — not supported).
4. The node has no `gke-tpu-partition-*-id` labels.

Fix at the **node-pool** level by recreating the pool with the correct
TPU topology configuration. Don't try to patch with `kubectl label` —
GKE will revert your changes.

### 6.2 CR exists, `.status.root` is null, no `Synced` condition yet

**Symptoms**
- The CR exists.
- `.status.root` is `null`.
- `.status.conditions` is empty (no `Synced` at all).

**What it means**
The CR was just created and the first poll hasn't completed yet *and*
hasn't failed yet. The controller only writes the `Synced` condition
once a poll succeeds, fails, or the sync-timeout window elapses — so
right after creation, you'll briefly see no condition at all.

**What to do**
Wait one or two poll intervals (typically a few seconds). If after
about 15 s there's still no `Synced` condition, look at controller
events for `GCEPollFailed` and follow §6.3.

### 6.3 First poll keeps failing

**Symptoms**
- `Synced=False`, `reason=OutOfSync`, message starts with
  `awaiting first successful poll: ...`.
- `GCEPollFailed` Warning events.
- `.status.root` is `null`.

**What it means**
The controller has never received a valid GCE response. Read the error
tail in the condition message and the events.

**Errors the controller itself produces**

| Message contains                                                            | What it means                                                                       |
|-----------------------------------------------------------------------------|--------------------------------------------------------------------------------------|
| `invalid MIG self-link "..."`                                               | The `node.gke.io/created-by-mig` annotation is malformed. Recreate the node pool.   |
| `rate limited: no token available`                                          | The controller's local rate limiter is throttling. Transient; recovers automatically. |
| `gce: get available accelerator topologies ...: <wrapped error>`            | The GCE API call failed. See the table below for common wrapped errors.             |
| `empty topology response`                                                   | GCE returned an empty map. Usually a brief race during pool resize.                 |
| `duplicate partition id "..."`                                              | GCE returned the same partition ID twice. Should not happen — file a bug.           |
| `multiple roots: "..." and "..."`                                           | GCE returned ≥2 parent-less partitions. Should not happen — file a bug.             |
| `orphan partition "...": parent "..." not in response`                      | GCE returned a child without its parent in the same response. Usually transient.    |
| `no root partition in response`                                             | GCE returned no parent-less partition. Should not happen — file a bug.              |
| `cycle detected at "..."`                                                   | GCE returned a cyclic parent graph. Should not happen — file a bug.                 |

> All of the "tree build" errors above keep the **last-known-good
> tree** in `.status.root`. They're treated like any other failed
> poll — `Synced` flips false after the timeout, the tree stays put.

**Common GCE / network errors you may see wrapped inside the message**
*(these come from Google's API client and Go's networking libraries,
not from the controller itself)*

| You see                                       | Most likely cause                                                                 |
|-----------------------------------------------|-----------------------------------------------------------------------------------|
| `googleapi: Error 403: Request had insufficient authentication scopes` (`ACCESS_TOKEN_SCOPE_INSUFFICIENT`) | The service account's OAuth token is missing the `compute.readonly` scope, or Workload Identity isn't bound correctly. |
| `googleapi: Error 403: This feature is not supported., forbidden` | The `GetAvailableAcceleratorTopologies` beta API has not been allowlisted for this project. Contact the GCE team to request allowlisting. |
| `googleapi: Error 403` (`PERMISSION_DENIED`)  | The service account is missing `roles/compute.viewer` on the project that owns the MIG. |
| `googleapi: Error 404`                        | The MIG no longer exists at the self-link the CR is using.                       |
| `googleapi: Error 503: Internal error`        | Transient GCE backend error. The controller retries automatically; if persistent, contact GCE support. |
| `context deadline exceeded`, `i/o timeout`   | Network egress to the GCE compute API is blocked or slow (common in private clusters with VPC-SC). |

**Checklist**

1. Verify IAM: the controller's service account needs
   `roles/compute.viewer` on the project that owns the MIG, granted
   either via Workload Identity (recommended) or directly to the GKE
   node service account.
2. For private clusters: confirm Private Google Access / VPC-SC
   permits egress to the GCE compute endpoint.
3. If the error is `invalid MIG self-link`, the node annotation itself
   is broken — recreate the pool.

### 6.4 Sync was healthy, now it's lost

**Symptoms**
- `Synced` was `True`, now `False` with `reason=OutOfSync` and a
  message like `no successful poll within 15s: ...` (the duration
  matches your configured sync-timeout; the default is 15 s).
- A `SyncLost` Warning event is emitted on the flip; a
  `SyncRestored` Normal event is emitted on recovery.
- `.status.root` is **preserved** at its last-known-good value — the
  controller never wipes a previously good tree on a poll failure.

**What it means**
The controller was getting good data from GCE, then started failing
long enough to exceed the sync-timeout window. Same root causes as
§6.3 — read the error tail, use the same checklist.

**If Synced is flapping rapidly:**
- Look for `rate limited: no token available` — that's transient and
  self-healing.
- Otherwise it's most likely intermittent network errors or GCE
  flakiness; check the controller pod logs in `partition-system`
  for the underlying error.

### 6.5 A node is missing from the tree

**Symptoms**
- `kubectl get node <node>` works.
- `<node>` doesn't appear in any partition's `nodeNames`.
- `Synced` is `True` (so freshness is fine).

**What it means**
The node fails at least one of the filter rules. The controller
silently excludes it — no event, no log.

**What to do**
Run the inspection in §4 ("Why isn't my node in the tree?") on the
missing node. Fix at the pool level; manual labels won't stick.

### 6.6 A leaf shows `NodeReady=False`

The reason field on the leaf's condition tells you exactly what's
wrong:

| Reason          | Message                                                | What it means                                                              |
|-----------------|--------------------------------------------------------|----------------------------------------------------------------------------|
| `NodesReady`    | `all expected nodes are Ready`                         | Everything is fine.                                                        |
| `NodesNotReady` | `nodes not ready: [n1 n2]`                             | The listed Kubernetes Nodes have `Ready != True`.                          |
| `NodesNotFound` | `nodes not found in cache: [n1 n2]`                    | GCE expects these instances, but the Kubernetes Node objects don't exist (or haven't been observed yet). Usually a transient race during scale-up. |
| `NodesNotFound` | `no nodes assigned to this partition`                  | A structural leaf with zero nodes — typically transient during pool fan-out. |

When both conditions exist (some missing **and** some not-ready),
`NodesNotFound` wins.

**What to do**
- `NodesNotReady`: standard Node debugging — `kubectl describe node
  <n>`, look at kubelet logs, hardware events, networking.
- `NodesNotFound`: wait a minute. If it persists, the Node never
  registered with the cluster — check the underlying GCE instance and
  node-pool autoscaler / repair events.

### 6.7 A partition is Degraded / Unhealthy / Incomplete

| State        | What GCE is telling us                                                                                                |
|--------------|------------------------------------------------------------------------------------------------------------------------|
| `Healthy`    | All VMs are running and inter-chip connectivity is fine.                                                              |
| `Degraded`   | Inter-chip connectivity is impaired but still usable in a degraded mode.                                              |
| `Unhealthy`  | Either the inter-chip connectivity or one of the VMs is unhealthy. The partition isn't usable as a sub-slice.        |
| `Incomplete` | Not all the VMs in this partition have been provisioned yet.                                                          |
| `Unknown`    | GCE didn't return a recognized health value.                                                                          |

**What to do**
- `Incomplete` often pairs with `NodesNotFound` on the same leaf — fix
  the underlying node-registration issue (§6.6).
- `Unhealthy` / `Degraded`: inspect the underlying VMs:
  ```bash
  gcloud compute instance-groups managed describe <mig> --zone <zone>
  ```
  Persistent unhealthy state should go through GKE/TPU support.

### 6.8 Occupancy looks wrong

A few things to know about how occupancy is computed:

1. A partition is **seeded** as `Occupied` if it has a non-empty slice
   ID (i.e. GCE says a slice is reserved on it), and `Unspecified`
   otherwise.
2. Occupancy then **bubbles up**: any `Occupied` descendant makes all
   ancestors `Occupied`.
3. It then **bubbles down**: an `Occupied` ancestor forces all its
   descendants `Occupied` (because a reserved super-slice consumes the
   whole sub-tree).
4. Anything left over becomes `Empty`.

So:

- "An entire branch shows `Occupied` even though only one leaf is in
  use" → **expected**, by step 3.
- "A workload is running on a node but the partition shows `Empty`" →
  Occupancy reflects **GCE-side slice reservations**, not pod
  placement. Pods that don't go through the slice-reservation API
  won't flip the partition.
- "Everything shows `Unspecified`" → typically means the first poll
  hasn't completed yet (§6.2), or GCE returned no slice information
  for this MIG.

---

## 7. Events & conditions cheatsheet

### 7.1 Events

The controller uses `events.k8s.io/v1` (not the legacy core
`v1.Event`). Query them with:

```bash
kubectl get events.v1.events.k8s.io --all-namespaces \
  --field-selector regarding.kind=PartitionTree
```

| Reason                         | Type      | When                                                                |
|--------------------------------|-----------|----------------------------------------------------------------------|
| `PartitionTreeCreated`         | `Normal`  | First time the controller sees a matching pool; CR created.         |
| `NodePoolDrained`              | `Normal`  | All matching nodes left the pool; CR deleted (default) or preserved (decoupled). |
| `GCEPollFailed`                | `Warning` | A single GCE poll returned an error.                                |
| `SyncLost`                     | `Warning` | `Synced` just flipped `True → False` after the timeout window.      |
| `SyncRestored`                 | `Normal`  | `Synced` just transitioned to `True` — either from `False`, or from absent on the first successful poll. |

### 7.2 The `Synced` condition (`.status.conditions[]`)

| Status   | Reason        | Message you'll see                                              | Means                                                                                     |
|----------|---------------|------------------------------------------------------------------|--------------------------------------------------------------------------------------------|
| `True`   | `InSync`      | `last successful poll at <timestamp>`                            | Latest poll succeeded; the tree is current.                                                |
| `False`  | `OutOfSync`   | `awaiting first successful poll: <error>`                        | No poll has ever succeeded; the most recent attempt failed.                                |
| `False`  | `OutOfSync`   | `no successful poll within <syncTimeout>: <error>`               | Was healthy; sync has been lost for at least the timeout (default 15 s).                   |
| `False`  | `PoolDrained` | `node pool drained, GCE polling suspended`                       | Decoupled mode only: node pool was deleted; CR is preserved but idle.                     |
| *absent* | —             | —                                                                | Brand-new CR — first poll hasn't run **and** hasn't failed yet (see §6.2).                |

### 7.3 The per-leaf `NodeReady` condition

See §6.6 — that table is the complete set of (reason, message)
combinations.

### 7.4 The full state and occupancy enums

| Enum         | Values                                                                |
|--------------|-----------------------------------------------------------------------|
| `state`      | `Healthy`, `Degraded`, `Unhealthy`, `Incomplete`, `Unknown`           |
| `occupancy`  | `Occupied`, `Empty`, `Unspecified`                                    |

---

## 8. Metrics & monitoring

The controller exports OpenTelemetry metrics dual-written to two parallel streams:
- **Customer Stream**: Google Cloud Managed Service for Prometheus (GMP) via `prometheus_target` (stream prefix `prometheus.googleapis.com/partition_tree/...`).
- **Google Internal Stream**: Cloud Monitoring via `k8s_entity` and `k8s_container` (stream prefix `kubernetes.io/internal/addons/partition_tree/...`).

> **IAM requirement:** Whenever `--enable-google-internal-metrics=true` (the default) or `--metrics-direct-push=true`, the controller's GCP service account must have **`roles/monitoring.metricWriter`** (`monitoring.timeSeries.create`) on the project (in addition to `roles/compute.viewer`). Without `roles/monitoring.metricWriter`, `PartitionTree` CRs continue reconciling normally (`Synced=True`), while background `CreateTimeSeries` calls fail with `PermissionDenied` (`403`) and metrics will not appear in Cloud Monitoring.

For the full catalog of metric definitions, attribute mappings, resource types, and lifecycle semantics, see the [Partition Tree Metrics Documentation](metrics.md).

### 8.1 Metrics Overview

| Metric | Type | Purpose | Details |
| :--- | :--- | :--- | :--- |
| `partition_tree_occupancy` | Gauge (`int64`) | One-hot partition occupancy state (`Occupied`, `Empty`, `Unspecified`). | [metrics.md §1](metrics.md#1-partition_tree_occupancy) |
| `partition_tree_taint` | Gauge (`int64`) | Active taints on partition nodes. | [metrics.md §2](metrics.md#2-partition_tree_taint) |
| `partition_tree_info` | Gauge (`int64`) | Static CR metadata (`root_partition_id`, `node_pool_name`, `mig_name`, `accelerator_type`, `tpu_pod_id`, `topology`). | [metrics.md §3](metrics.md#3-partition_tree_info) |
| `partition_tree_build_info` | Gauge (`int64`) | Controller build metadata and daemon pod replica identity. | [metrics.md §4](metrics.md#4-partition_tree_build_info) |

### 8.2 Useful queries (PromQL)

In Prometheus / GMP, metric names correspond to the OpenTelemetry instrument names. Since node pool and hardware metadata are attached to `partition_tree_info`, join on `partition_tree_name` when aggregating across node pools.

**Occupied partitions per pool**

```promql
sum by (node_pool_name) (
  partition_tree_occupancy{state="Occupied"}
  * on(partition_tree_name) group_left(node_pool_name)
  partition_tree_info
)
```

**Occupied partitions per partition tree**

```promql
sum by (partition_tree_name) (
  partition_tree_occupancy{state="Occupied"}
)
```

**Per-pool occupancy ratio (Occupied / total)**

```promql
sum by (node_pool_name) (
  partition_tree_occupancy{state="Occupied"}
  * on(partition_tree_name) group_left(node_pool_name)
  partition_tree_info
)
/
sum by (node_pool_name) (
  partition_tree_occupancy
  * on(partition_tree_name) group_left(node_pool_name)
  partition_tree_info
)
```

**Total cluster occupancy overview**

```promql
sum(partition_tree_occupancy{state="Occupied"})
```

**Active taints per partition tree**

```promql
sum by (partition_tree_name, taint_key) (
  partition_tree_taint
)
```

### 8.3 Suggested alerts

| Alert                            | Description                                                                                        |
|----------------------------------|----------------------------------------------------------------------------------------------------|
| Sync lost (per pool)             | Use `SyncLost` Kubernetes events or poll `Synced=False` via the API.                               |
| Unhealthy partitions present     | Any partition with `state ∈ {Unhealthy, Incomplete}` for > 10 min.                                |
| Pool fully booked                | Occupancy ratio == 1 sustained for > N min (see ratio query above).                               |
| Active partition taints          | Partitions reporting non-zero `partition_tree_taint` unexpectedly.                                 |

The controller also exposes standard controller-runtime metrics on the
metrics Service, which you can scrape into Prometheus / Cloud Managed
Service for Prometheus.

### 8.4 Web Dashboard

For a real-time visual overview, the project ships a **web dashboard**
(`ptree-dashboard`) that connects to your cluster and displays all
PartitionTree CRs with auto-refreshing health, occupancy, and node
readiness.

```bash
# Build and run
go build -o bin/ptree-dashboard ./cmd/ptree-dashboard/
./bin/ptree-dashboard              # uses current kubeconfig
./bin/ptree-dashboard --port=9090  # custom port
```

Open `http://localhost:8080` in a browser. The dashboard provides:
- Fleet-wide summary (total trees, nodes, leaves, health breakdown)
- Per-tree drill-down with partition hierarchy
- Color-coded health and occupancy states
- Filtering and sorting

---

## 9. Visualization (`ptree-viz`)

The project ships a small CLI that renders any `PartitionTree` (or all
of them) in your terminal. Build it once:

```bash
go build -o bin/ptree-viz ./cmd/ptree-viz/
```

| Command                                | What you get                                            |
|----------------------------------------|---------------------------------------------------------|
| `./bin/ptree-viz <pool-name>`          | ASCII tree for one pool                                 |
| `./bin/ptree-viz`                      | Fleet-wide summary table across all CRs                 |

Useful flags:

- `--no-color` — disable ANSI colors (for piping / log capture).
- `--sort=<name|nodes|type|state>` — sort the fleet table
  (default: `name`).
- `--filter=<unhealthy|degraded|out-of-sync|occupied|empty|<substring>>`
  — filter rows. Any value not in the keyword list is treated as a
  name substring match.

For a richer visual experience, see the **web dashboard** in §8.5,
which provides interactive, auto-refreshing fleet monitoring in a
browser.

Color legend:

| Color  | Used for                                              |
|--------|-------------------------------------------------------|
| Green  | `Healthy`, `NodesReady`                               |
| Yellow | `Degraded`, `Incomplete`, `NodesNotReady`             |
| Red    | `Unhealthy`, `NodesNotFound`                          |
| Cyan   | `Occupied`                                            |
| Gray   | `Empty`, `Unknown`, anything unmatched                |

Sample single-tree output:

```
my-tpu-pool  (v7x)  Synced=True
└── root [4x4x4] Healthy [Occupied]
    ├── p-0 [2x2x4] Healthy [Empty]
    │   ├── leaf-0 [2x2x1] Healthy [Empty]  nodes=[n1,n2] NodesReady
    │   └── leaf-1 [2x2x1] Degraded [Empty]  nodes=[n3,n4] NodesNotReady
    └── p-1 [2x2x4] Healthy [Occupied]
        ├── leaf-2 [2x2x1] Healthy [Occupied]  nodes=[n5,n6] NodesReady
        └── leaf-3 [2x2x1] Healthy [Occupied]  nodes=[n7,n8] NodesReady
```

---

## 10. FAQ

**Q. Can I edit `.spec` to influence the tree?**
There is no `.spec` — the type only has `metadata` and `status`. Manual
status edits get reverted on the next reconcile.

**Q. Why is `.status.root` `null` right after pool creation?**
The controller leaves `.status.root` unset until the first **successful**
poll completes. Typical settling time is a few seconds.

**Q. Why do I see `Occupied` on a partition where I'm not running any
pods?**
Occupancy reflects GCE-side slice reservations, not pod placement. A
reserved-but-idle slice will show `Occupied`. See §6.8.

**Q. I deleted the CR by accident. Did I break anything?**
No. The controller recreates it on the next reconcile. There's a brief
window where `.status.root` is `null` and partition metrics stop
emitting for that CR until the first poll completes.

**Q. My cluster has v5e and v7x pools. Why only v7x CRs?**
Only TPU v6e (`tpu-v6e-slice`) and v7x (`tpu7x`) are supported.

**Q. Can I rename a `PartitionTree`?**
No — the CR name equals the node-pool name (default mode) or the root
partition ID (decoupled mode). To "rename" in default mode, recreate
the node pool with the new name.

**Q. What is decoupled lifecycle mode?**
When the controller is started with `--decoupled-lifecycle`, CRs are
named after the root partition ID from GCE topology labels (instead of
the node pool name). This means the CR persists when the node pool is
deleted and automatically recovers when a new node pool is created in
the same physical partition.

**Q. My CR shows `Synced=False, Reason=PoolDrained`. Is it broken?**
No — this is expected in decoupled mode after a node pool deletion.
The CR is preserved but idle. GCE polling is suspended to avoid wasting
API calls. When you recreate a node pool in the same partition, the CR
will recover automatically.

**Q. I see `NodePoolDrained` events but my CR still exists. Is that normal?**
Yes — in decoupled mode, `NodePoolDrained` preserves the CR with
`Synced=False, Reason=PoolDrained` instead of deleting it. The event
message will indicate that the MIG annotation was cleared. When you
recreate the node pool, the CR recovers automatically.

**Q. What IAM does the controller need?**
Grant both roles on the project that owns the MIG (via Workload Identity,
recommended, or directly on the GKE node service account):
- `roles/compute.viewer` — reads GCE MIG accelerator topology (`GetAvailableAcceleratorTopologies`).
- `roles/monitoring.metricWriter` (`monitoring.timeSeries.create`) — exports `PartitionTree` metrics to Cloud Monitoring (required by default when `--enable-google-internal-metrics=true`, and also required when `--metrics-direct-push=true`; only optional if both flags are set to `false`).

See the project README for step-by-step setup commands.

**Q. Where can I see what the controller is doing?**
Start with the in-cluster signals: the CR conditions, the events in §7.1,
and the metrics in §8. For deeper debugging, tail the controller
pod logs directly:

```bash
kubectl -n partition-system logs -l control-plane=controller-manager -f
```
