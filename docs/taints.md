# PartitionTree Taints and Health Signals

## 1. Flag Configuration (`--enable-taints`)

Taint propagation in the `partition-tree` controller is controlled by the `--enable-taints` command-line flag (`internal/controller/taint_config.go`):

* **Default Behavior (Disabled)**: If `--enable-taints` is omitted or set to `false` (or `0` / `no`), all taint propagation is disabled (`Enabled: false`), and `.status.root...taints` is cleared (`nil`) across all partitions in the tree.
* **Enable All Taints**: Passing `--enable-taints` without an argument (boolean flag), `--enable-taints=true`, or `--enable-taints=All` (case-insensitive) activates all taint features.
* **Selective Feature Activation**: `--enable-taints` accepts a comma-separated list of specific feature options (`PartitionState`, `NodesNotFound`, `NodeTaints`, `All`):
  1. **`PartitionState`**: Propagates GCE-level hardware and topology health states (`Incomplete`, `Unhealthy`, `Degraded`, `Unknown`) into status taints (`cloud.google.com/partition-state`) on any affected partition node (Root, Intermediate, or Leaf).
  2. **`NodesNotFound`**: Propagates missing or unassigned node conditions (`NodeReady=False` with `Reason=NodesNotFound`) into leaf partition taints (`cloud.google.com/nodes-not-found`).
  3. **`NodeTaints`**: Propagates Kubernetes `Node` object spec taints (`node.Spec.Taints`) to leaf partitions as a union across assigned nodes (with configurable opt-out prefix exclusions).
  4. **External Annotation Taints**: Propagates custom taints set via `taints.partitiontree.gke.io/*` annotations on the `PartitionTree` CR to targeted `partitionId`s.
     * *Note*: External annotation taints do not have a separate flag option name; they are **always active whenever `--enable-taints` is enabled** with any non-false value (e.g., even if only `--enable-taints=PartitionState` is specified).

---

## 2. Hierarchy, Merging, and Ordering Semantics

* **No Upward or Downward Bubbling**: Unlike `Occupancy` (which bubbles up to ancestors and down to descendants via `tree.Bubble`), **taints never bubble up or down the partition tree**. Each taint lives strictly on the partition node where it originates or is targeted:
  * `PartitionState` taints appear on whichever node (Root, Intermediate, or Leaf) has a non-`Healthy` state.
  * `NodesNotFound` and `NodeTaints` appear **only on Leaf partitions** (`len(childPartitions) == 0`).
  * External annotation taints appear **only on the specific `partitionId`** targeted by the annotation.
* **Merging & De-duplication**: A partition can simultaneously carry taints from multiple active sources:
  * *Root / Intermediate partitions*: External Annotations + `PartitionState`.
  * *Leaf partitions*: External Annotations + `PartitionState` + `NodeTaints` + `NodesNotFound`.
  * All taints on a partition are merged (`mergeTaints`) and de-duplicated by their unique identity tuple `(Key, Value, Effect)` (`tree.TaintIdentity`). Two taints with the same `Key` and `Effect` but different `Value`s are treated as distinct taints and both are retained. When duplicate taints with non-nil `timeAdded` are merged (across nodes in a multi-host leaf or across multiple sources), the **earliest** `timeAdded` is retained.
* **`timeAdded` Timestamp Semantics**:
  * **Propagated Node Taints**: `node.Spec.Taints[i].TimeAdded` (stamped by Kubernetes `node-lifecycle-controller` on `NoExecute` taints) is copied into `PartitionTaint.timeAdded`.
  * **First-Seen Stamping & Carry-Over**: Taints without an upstream `timeAdded` (`cloud.google.com/partition-state`, `cloud.google.com/nodes-not-found`, manual node taints without `timeAdded`, or `taints.partitiontree.gke.io/*` annotations omitting `timeAdded`) are stamped with the controller's reconciliation timestamp (truncated to RFC3339 second precision) when first observed and carried forward across subsequent polls without causing spurious `/status` patches. Each source is stamped before sources are merged, so an unstamped taint present since T0 keeps T0 when another node or an annotation later adds the same taint with an explicit, later `timeAdded`; an explicit `timeAdded` from a single source (e.g. an edited annotation) still takes effect.
  * **Node-Health `NoExecute` Episode Continuity (per node)**: When `node-lifecycle-controller` swaps a node's `node.kubernetes.io/not-ready:NoExecute` for `node.kubernetes.io/unreachable:NoExecute` (or back), the new taint gets a fresh `timeAdded`; the controller's node cache keeps that node's original outage start instead. The same key re-appearing with a newer `timeAdded` means the taint was removed and re-added (a recovery), so it starts a new episode. Across the nodes of a multi-host leaf, the partition reports the earliest start among the nodes that are tainted now, so a recovered node's older start is never applied to another node's later outage. The swap history is kept in memory only: if the controller restarts mid-episode, the clock restarts at the swap time (eviction can only be later, never earlier).
  * **Drained Pools (decoupled lifecycle)**: A drained CR keeps its last tree, taints included. When the pool is re-provisioned, the new nodes start fresh: timestamps from the drained tree are not carried forward.
  * **Upgrade Order (CRD First)**: Apply the updated `PartitionTree` CRD (`config/crd`) before rolling out a controller that writes `timeAdded`; the Helm chart does not install CRDs (kustomize `config/default` does). Under an older CRD the API server prunes `timeAdded` from root-partition taints (`childPartitions` is schemaless, so non-root partitions keep it), and every reconcile or poll of a tree whose root carries a taint re-sends a no-op `/status` patch.
* **Deterministic Sorting**: Every partition's `.taints` list is sorted deterministically by `Key`, then `Value`, then `Effect` (`tree.ComparePartitionTaints`) to prevent non-deterministic `/status` patches and unnecessary apiserver load.

---

## 3. Health Signals Surfaced via Taints

### Partition State
* **Taint Key**: `cloud.google.com/partition-state`
* **Target Level**: Root, Intermediate, or Leaf
* **Signal Origin / Source**: GCE Compute Engine API ([`GetAvailableAcceleratorTopologies`](https://cloud.google.com/compute/docs/reference/rest/beta/instanceGroupManagers/getAvailableAcceleratorTopologies)) or Initial Controller State. *(Note: State enums are defined in the [Compute API Discovery JSON](https://raw.githubusercontent.com/googleapis/google-api-go-client/main/compute/v0.beta/compute-api.json)).*
* **Precedence Order**: `Incomplete` > `Unhealthy` > `Degraded` > `Healthy` > `Unknown` (`internal/gce/client_impl.go`).
* **Values / Effects / Meaning**:
  * `Incomplete` (`NoSchedule`): Maps to `AcceleratorTopologyState.CurrentState` = `INCOMPLETE`. Not all VMs in the topology have been provisioned yet. Has highest precedence on non-root partitions, overriding `Unhealthy` or `Degraded`. Do not schedule new workloads until complete.
  * `Unhealthy` (`NoExecute`): Maps to `AcceleratorTopologyHealth` = `UNHEALTHY` or `InstancesHealth` = `UNHEALTHY_OR_MISSING`. Some VMs are not in a `RUNNING` state or are missing, OR there is an issue with inter-chip connectivity making the infrastructure unsuitable for forming a working group. Running workloads must be evicted.
  * `Degraded` (`PreferNoSchedule`): Maps to `AcceleratorTopologyHealth` = `DEGRADED` (when instances are not unhealthy). All VMs are `RUNNING`, but there is an issue with inter-chip connectivity causing performance degradation. Avoid scheduling new jobs here if alternatives exist.
  * `Unknown` (`NoSchedule`): GCE returned an unrecognized/empty health status, or the initial state before the first successful poll.
  * `Healthy`: Produces **no taint** (`stateToTaint` returns `nil`).

### Nodes Not Found
* **Taint Key**: `cloud.google.com/nodes-not-found`
* **Value**: *(none / empty)*
* **Effect**: `NoExecute`
* **Target Level**: Leaf only
* **Signal Origin / Source**: Controller Node Informer Cache (`internal/cache/topology_cache.go`) and Leaf Condition Derivation (`tree.DeriveNodeReady`)
* **What It Tells You**: The leaf partition's `NodeReady` condition is `False` with `Reason: NodesNotFound`. This occurs when:
  1. **No nodes assigned (`len(nodes) == 0`)**: The GCE topology includes this leaf partition, but no matching Kubernetes `Node` in the cluster cache carries the partition label (`cloud.google.com/gke-tpu-partition-<topology>-id` or `cloud.google.com/gke-tpu-slice-<topology>-id`) for this `partitionId` (e.g., node pool scaled down, node deleted, or failed registration). Condition message: `"no nodes assigned to this partition"`.
  2. **Missing from readiness map (`!Found`)**: Expected node names are associated with the leaf, but their readiness entry is absent from the cache (`snap.Ready[nodeName]` missing). Condition message: `"nodes not found in cache: [<nodes>]"`. (`NodesNotFound` takes precedence over `NodesNotReady`).

### Node Not Ready
* **Taint Key**: `node.kubernetes.io/not-ready`
* **Value**: *(varies / empty)*
* **Effect**: `NoExecute` or `NoSchedule`
* **Target Level**: Leaf only
* **Signal Origin / Source**: Kubernetes Node Spec (`node.Spec.Taints` via `NodeTaints`)
* **What It Tells You**: Kubelet on the backing node is unhealthy or failing health checks / heartbeat.

### Node Unreachable
* **Taint Key**: `node.kubernetes.io/unreachable`
* **Value**: *(varies / empty)*
* **Effect**: `NoExecute` or `NoSchedule`
* **Target Level**: Leaf only
* **Signal Origin / Source**: Kubernetes Node Spec (`node.Spec.Taints` via `NodeTaints`)
* **What It Tells You**: Node controller has lost communication with the underlying node.

### Node Pressure *(Disk, Memory, PID, Network)*
* **Taint Key**: `node.kubernetes.io/disk-pressure`, `node.kubernetes.io/memory-pressure`, `node.kubernetes.io/pid-pressure`, `node.kubernetes.io/network-unavailable`
* **Value**: *(none / empty)*
* **Effect**: `NoSchedule`
* **Target Level**: Leaf only
* **Signal Origin / Source**: Kubernetes Node Spec (`node.Spec.Taints` via `NodeTaints`)
* **What It Tells You**: Kubelet / OS resource exhaustion on the underlying VM host (out of disk space, RAM, PIDs, or broken CNI).

### Node Cordoned / Draining
* **Taint Key**: `node.kubernetes.io/unschedulable`, `cloud.google.com/gke-drain`
* **Value**: *(varies / empty)*
* **Effect**: `NoSchedule` or `NoExecute`
* **Target Level**: Leaf only
* **Signal Origin / Source**: Kubernetes Node Spec (`node.Spec.Taints` via `NodeTaints`)
* **What It Tells You**: Node has been cordoned by an admin, autoscaler, or GKE auto-repair/upgrade process.

### Custom External Taints
* **Taint Key**: Custom (e.g., `example.com/maintenance`)
* **Value**: Custom (e.g., `ongoing`)
* **Effect**: `NoSchedule`, `PreferNoSchedule`, or `NoExecute`
* **Target Level**: Targeted `PartitionID` (Root, Intermediate, or Leaf)
* **Signal Origin / Source**: CR Annotations (`taints.partitiontree.gke.io/*`)
* **What It Tells You**: Operator, maintenance system, or custom scheduler has placed a manual/programmatic lock or health quarantine on a specific partition.

---

## 4. Detailed Breakdown & Taint Examples

### 1. Partition State Taints (`PartitionState`)
* **Origin**: `.state` is populated during GCE polling (`tree.Build`), and `refreshAndPropagate` derives the `cloud.google.com/partition-state` taint from `node.State` on **every** reconciliation pass (`stateToTaint`).
* **Behavior**: Applied directly to any partition node (Root, Intermediate, or Leaf) whose state is `Unhealthy`, `Degraded`, `Incomplete`, or `Unknown`. `Healthy` partitions receive no partition-state taint.

```yaml
# Example: An Unhealthy leaf partition in status
partitionId: leaf-0
topology: 2x2x1
state: Unhealthy
taints:
  - key: cloud.google.com/partition-state
    value: Unhealthy
    effect: NoExecute
```

```yaml
# Example: A Degraded intermediate partition in status
partitionId: part-2x2x2
topology: 2x2x2
state: Degraded
taints:
  - key: cloud.google.com/partition-state
    value: Degraded
    effect: PreferNoSchedule
```

---

### 2. Nodes Not Found Taint (`NodesNotFound`)
* **Origin**: Derived during leaf status refresh (`refreshAndPropagate`) whenever `DeriveNodeReady` sets `NodeReady` to `False` with `Reason: NodesNotFound`.
* **Behavior**: Emitted with `NoExecute` so scheduling engines avoid assigning multi-host jobs to leaves with missing or unregistered Kubernetes nodes.
* **Runtime Representation**: When all Kubernetes nodes for a leaf are absent from the cluster cache (e.g., deleted or not yet registered), `snap.Partitions[partitionId]` is empty (`len(nodes) == 0`), so `nodeNames` is omitted and `message` is `"no nodes assigned to this partition"`. If `nodeNames` is non-empty but `snap.Ready[nodeName]` is missing (`Found: false`), `message` reports `"nodes not found in cache: [<nodeNames>]"`.

```yaml
# Example: Leaf partition where no matching Kubernetes node exists in the cluster cache
partitionId: leaf-1
topology: 2x2x1
state: Healthy
conditions:
  - type: NodeReady
    status: "False"
    reason: NodesNotFound
    message: "no nodes assigned to this partition"
taints:
  - key: cloud.google.com/nodes-not-found
    effect: NoExecute
```

---

### 3. Propagated Node Taints (`NodeTaints`)
* **Origin**: Read from `node.Spec.Taints` by the node event handler and cached per node (`snap.Taints`).
* **Union Semantics**: Computed via `unionNodeTaints` as the set union across all nodes assigned to the leaf partition (`snap.Partitions[node.PartitionID]`). Nodes without taints contribute nothing to the union (they do not clear taints contributed by other nodes in the same leaf).
* **Effect Mapping**: Standard Kubernetes effects (`NoSchedule`, `PreferNoSchedule`, `NoExecute`) map 1:1. Any unrecognized `corev1.TaintEffect` is logged and fallback-mapped to `NoSchedule` (`toPartitionTaintEffect`).
* **Prefix Filtering (Opt-Out)**: Configured via the hot-reloadable `partition-config` ConfigMap under `taints.nodePropagation.excludedPrefixes` (`internal/config/config.go`). Any node taint whose key starts with a configured prefix (`strings.HasPrefix`) is excluded. Defaults to excluding `["google.com/tpu"]`.

```yaml
# ConfigMap configuration (partition-config)
apiVersion: v1
kind: ConfigMap
metadata:
  name: partition-config
  namespace: partition-system
data:
  config.yaml: |
    taints:
      nodePropagation:
        excludedPrefixes:
          - "google.com/tpu"
```

```yaml
# Example: Leaf partition reflecting node unreadiness and disk pressure from its backing node(s)
partitionId: leaf-0
topology: 2x2x1
state: Healthy
nodeNames:
  - gke-tpu-pool-node-0
taints:
  - key: node.kubernetes.io/disk-pressure
    effect: NoSchedule
  - key: node.kubernetes.io/not-ready
    effect: NoExecute
```

---

### 4. External Annotation Taints (`taints.partitiontree.gke.io/*`)
* **Origin**: Read from `PartitionTree` CR annotations matching the prefix `taints.partitiontree.gke.io/` (`tree.ExtractTaintsFromAnnotations`).
* **Schema & Validation Rules**:
  * Each matching annotation value must be a JSON object deserializing into `TargetedTaint`: `{"partitionId": "<id>", "key": "<key>", "value": "<val>", "effect": "<effect>"}`.
  * `partitionId` must be non-empty.
  * `effect` must be one of `NoSchedule`, `PreferNoSchedule`, or `NoExecute`.
  * `key` must be a valid Kubernetes qualified name (`validation.IsQualifiedName`).
  * `value` (optional) must be empty or a valid Kubernetes label value (`validation.IsValidLabelValue`).
* **Top-Level Condition (`TaintsValid`)**:
  * Stored in `.status.conditions` on the `PartitionTree` CR (`ConditionTypeTaintsValid = "TaintsValid"`).
  * When all taint annotations parse and validate cleanly: `status: "True"`, `reason: "TaintsValid"`, `message: "taints parsed successfully"`.
  * When any taint annotation fails parsing or validation: `status: "False"`, `reason: "TaintParseFailed"`, `message: "<error details>"`.
* **Error Handling & Last-Known-Good Freeze**:
  * If any `taints.partitiontree.gke.io/*` annotation is malformed or invalid (`taintsParseErr != nil`), the controller **preserves safety by freezing taints at their last-known-good state**:
    * Emits a `Warning` event on the CR with reason `TaintParseError` (`Failed to parse taints annotations (reusing last-known-good): ...`).
    * During GCE topology rebuilds (`tree.Build`), reuses the last-known-good taints extracted from the existing `.status.root` (`tree.ExtractTaints`).
    * During `refreshAndPropagate`, sets `skipTaintUpdates = true`, freezing all partition taints across the tree until the invalid annotation is corrected or removed.
* **Target Partition Validation (`UnusedTaintTarget`)**:
  * When annotations parse cleanly, `validateTaintTargets` checks if every targeted `partitionId` exists in the GCE topology response. If an annotation targets a `partitionId` that does not exist in the topology tree, the controller emits a `Warning` event with reason `UnusedTaintTarget` (`Taint targeted at non-existent partition "<id>"`).

```bash
# How it is applied by an operator/controller:
kubectl annotate partitiontree my-tpu-pool \
  taints.partitiontree.gke.io/maintenance='{"partitionId":"leaf-0","key":"example.com/maintenance","value":"ongoing","effect":"NoSchedule"}'
```

```yaml
# How it surfaces in PartitionTree status:
status:
  conditions:
    - type: TaintsValid
      status: "True"
      reason: TaintsValid
      message: "taints parsed successfully"
  root:
    partitionId: leaf-0
    topology: 2x2x1
    state: Healthy
    taints:
      - key: example.com/maintenance
        value: ongoing
        effect: NoSchedule
```

---

## 5. Taint Metrics & Observability

All active taints on every partition node in `.status.root` are exported as OpenTelemetry observable gauge metrics (`internal/metrics/metrics.go`):

* **Customer Stream (GMP / Prometheus)**: `prometheus.googleapis.com/partition_tree/taint/gauge` (PromQL: `partition_tree_taint`, monitored resource: `prometheus_target`).
* **Google Internal Stream (Cloud Monitoring)**: `kubernetes.io/internal/addons/partition_tree/taint` (monitored resource: `k8s_entity`).
* **Metric Dimensions**:
  * `partition_tree_name` (promoted to `entity_name` resource label on Google stream)
  * `partition_tree_uid` (promoted to `entity_uid` resource label on Google stream)
  * `partition_id`: Identifier of the tainted partition node.
  * `topology`: Physical TPU topology shape of the tainted partition (e.g., `2x2x1`, `4x4x4`).
  * `taint_key`: Key of the taint (e.g., `cloud.google.com/partition-state`, `cloud.google.com/nodes-not-found`, `node.kubernetes.io/not-ready`).
  * `taint_value`: Value of the taint (empty string `""` if unset).
  * `taint_effect`: Effect of the taint (`NoSchedule`, `PreferNoSchedule`, `NoExecute`).
* **Lifecycle & Tombstoning**:
  * Emits a gauge value of `1` while a taint is present on a partition.
  * When a taint is removed (or when a `PartitionTree` CR is deleted), `TTLTombstoneGaugeSet` emits a value of `0` (tombstone) for the duration configured by `--metrics-tombstone-ttl` (default: `30m`) before pruning the time series from memory.

For full details on metric streams, monitored resource labels, and PromQL queries, see [Partition Tree Metrics (`docs/metrics.md`)](metrics.md).
