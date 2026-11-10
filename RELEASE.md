# Release Notes

## v0.7.0

- PartitionTree controller that reflects GCE accelerator topology into
  cluster-scoped `PartitionTree` custom resources.
- Polls GCE and reports partition hierarchy, health state, and per-leaf
  `NodeReady` conditions.
- Supports TPU `v6e` and `v7x`.
- Optional node taints, and decoupled lifecycle mode.
- Optional OpenTelemetry metrics export.
- `ptree-viz` CLI and `ptree-dashboard` web dashboard for inspecting trees.
- Helm chart and kustomize deployment.
