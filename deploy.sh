#!/bin/bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


set -e

# Determine the directory of this script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Default values
CLUSTER_NAME="partition-test-e2e"
DEBUG_ENABLED=false
DEBUG_LEVEL=1
ENABLE_TAINTS=false
DECOUPLED_LIFECYCLE=false
NAMESPACE="partition-system"
IMAGE_TAG="v1"

# Parse command-line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --cluster) CLUSTER_NAME="$2"; shift ;;
        --debug-level) DEBUG_ENABLED=true; DEBUG_LEVEL="$2"; shift ;;
        --enable-taints) ENABLE_TAINTS="$2"; shift ;;
        --decoupled-lifecycle) DECOUPLED_LIFECYCLE="$2"; shift ;;
        --namespace) NAMESPACE="$2"; shift ;;
        --image-tag) IMAGE_TAG="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

echo ""
echo "### Build and Deploy the partition-tree controller ###"
echo "--- Building and loading the controller Docker image ---"
make -C "$SCRIPT_DIR" docker-build IMG=partition-tree-controller:${IMAGE_TAG}
if ! kind load docker-image partition-tree-controller:${IMAGE_TAG} --name "$CLUSTER_NAME"; then
    echo "--- kind load failed, trying manual loading ---"
    docker save partition-tree-controller:${IMAGE_TAG} | docker exec -i ${CLUSTER_NAME}-control-plane ctr --namespace k8s.io images import -
fi

echo "--- Installing CRDs ---"
kubectl apply -f "$SCRIPT_DIR/config/crd/bases"

echo "--- Deploying the Helm chart ---"

# Detect Kind cluster: disable OTel (no GCE metadata) and use IfNotPresent pull policy.
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
    KIND_OVERRIDES="--set image.pullPolicy=IfNotPresent --set otlp.endpoint="
else
    KIND_OVERRIDES=""
fi

HELM_ARGS="--install partition-tree $SCRIPT_DIR/charts/partition-tree \
    --namespace $NAMESPACE \
    --create-namespace \
    -f $SCRIPT_DIR/charts/partition-tree/values.yaml \
    --set image.tag=${IMAGE_TAG} \
    --set metrics.enabled=true \
    --set enableTaints=${ENABLE_TAINTS} \
    --set decoupledLifecycle=${DECOUPLED_LIFECYCLE} \
    $KIND_OVERRIDES"

if [ "$DEBUG_ENABLED" = true ]; then
    HELM_ARGS="$HELM_ARGS --set debug.enabled=true --set debug.level=$DEBUG_LEVEL"
fi

helm upgrade $HELM_ARGS

echo "--- Waiting for partition-tree controller to be ready ---"
kubectl wait --for=condition=Available=True deployment -l app.kubernetes.io/name=partition-tree \
    --namespace "$NAMESPACE" --timeout=120s

echo "--- Partition-tree controller deployed successfully ---"
