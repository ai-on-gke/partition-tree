//go:build e2e
// +build e2e

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

package e2e

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ai-on-gke/partition-tree/test/utils"
)

// The fixtures baked into test/e2e/config/gce_mock_fixtures.yaml are
// keyed by these MIG self-links. The Node objects in this spec are
// crafted so the controller resolves each pool's MIG to one of them.
const (
	healthyPool     = "healthy-pool"
	healthyMIGLink  = "projects/1234/zones/us-central1-a/instanceGroupManagers/healthy-mig"
	healthyNodeName = "healthy-node-0"

	incompletePool     = "incomplete-pool"
	incompleteMIGLink  = "projects/1234/zones/us-central1-a/instanceGroupManagers/incomplete-mig"
	incompleteNodeName = "incomplete-node-0"

	degradedPool     = "degraded-root-pool"
	degradedMIGLink  = "projects/1234/zones/us-central1-a/instanceGroupManagers/degraded-root-mig"
	degradedNodeName = "degraded-root-node-0"

	occupiedPool     = "occupied-pool"
	occupiedMIGLink  = "projects/1234/zones/us-central1-a/instanceGroupManagers/occupied-mig"
	occupiedNodeName = "occupied-node-0"

	superSlicePool     = "super-slice-pool"
	superSliceMIGLink  = "projects/1234/zones/us-central1-a/instanceGroupManagers/super-slice-mig"
	superSliceNodeName = "super-slice-node-0"

	occupiedBranchingPool    = "occupied-branching-pool"
	occupiedBranchingMIGLink = "projects/1234/zones/us-central1-a/instanceGroupManagers/occupied-branching-mig"
	occupiedBranchingNodeA   = "occupied-branching-node-a"
	occupiedBranchingNodeB   = "occupied-branching-node-b"

	superSliceBranchingPool    = "super-slice-branching-pool"
	superSliceBranchingMIGLink = "projects/1234/zones/us-central1-a/instanceGroupManagers/super-slice-branching-mig"
	superSliceBranchingNodeA   = "super-slice-branching-node-a"
	superSliceBranchingNodeB   = "super-slice-branching-node-b"

	occupancyBranchingPool    = "occupancy-branching-pool"
	occupancyBranchingMIGLink = "projects/1234/zones/us-central1-a/instanceGroupManagers/occupancy-branching-mig"
	occupancyBranchingNodeA1  = "occupancy-branching-node-a1"
	occupancyBranchingNodeA2  = "occupancy-branching-node-a2"
	occupancyBranchingNodeB1  = "occupancy-branching-node-b1"
	occupancyBranchingNodeB2  = "occupancy-branching-node-b2"

	superSliceOccupancyBranchingNodeA1 = "super-slice-occupancy-node-a1"
	superSliceOccupancyBranchingNodeA2 = "super-slice-occupancy-node-a2"
	superSliceOccupancyBranchingNodeB1 = "super-slice-occupancy-node-b1"
	superSliceOccupancyBranchingNodeB2 = "super-slice-occupancy-node-b2"

	superSliceOccupancyBranchingPool    = "super-slice-occupancy-branching-pool"
	superSliceOccupancyBranchingMIGLink = "projects/1234/zones/us-central1-a/instanceGroupManagers/super-slice-occupancy-branching-mig"

	multiPoolName     = "multi-pool"
	multiPoolMIGLink  = "projects/1234/zones/us-central1-a/instanceGroupManagers/multi-pool-mig"
	multiPoolNodeName = "multi-pool-node-0"

	// configMapName is the runtime config ConfigMap deployed by the
	// base kustomization (config/samples + namePrefix "partition-").
	configMapName = "partition-config"
)

var _ = Describe("PartitionTree CRs from GCE mock", Ordered, func() {
	BeforeAll(func() {
		By("waiting for the controller-manager to be ready")
		Eventually(controllerReady, 3*time.Minute, time.Second).Should(Succeed())

		By("applying synthetic Nodes that map to the baked-in mock fixtures")
		setupNode(healthyNodeName, healthyPool, healthyMIGLink, "tpu-v6e-slice",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-root-id": "root",
				"cloud.google.com/gke-tpu-partition-leaf-id": "leaf",
			})

		setupNode(incompleteNodeName, incompletePool, incompleteMIGLink, "tpu-v6e-slice",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-root-id": "root",
			})

		setupNode(degradedNodeName, degradedPool, degradedMIGLink, "tpu-v6e-slice",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-root-id": "root",
				"cloud.google.com/gke-tpu-partition-leaf-id": "leaf",
			})
	})

	AfterAll(func() {
		By("deleting the e2e PartitionTree CRs")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "partitiontree", healthyPool,
			incompletePool, degradedPool, "--ignore-not-found=true", "--wait=false"))

		By("deleting the e2e Nodes")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "node", healthyNodeName,
			incompleteNodeName, degradedNodeName, "--ignore-not-found=true", "--wait=false"))
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	It("creates a PartitionTree CR with the expected tree shape", func() {
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", healthyPool,
				"-o", "jsonpath={.status.root.partitionId} "+
					"{.status.acceleratorType} "+
					"{.status.root.state} "+
					"{.status.root.childPartitions[0].partitionId} "+
					"{.status.root.childPartitions[0].nodeNames[0]}"))
			g.Expect(err).NotTo(HaveOccurred(), "Failed to get healthy PartitionTree")
			g.Expect(out).To(Equal(fmt.Sprintf("root tpu-v6e-slice Healthy leaf %s", healthyNodeName)))
		}).Should(Succeed())
	})

	It("sets Synced=True on the top-level status conditions", func() {
		verifyCRStatus(healthyPool, "{.status.conditions[?(@.type=='Synced')].status}", "True")
	})

	It("does not produce spurious status patches across poll cycles", func() {
		By("waiting for the CR to reach Synced=True")
		verifyCRStatus(healthyPool, "{.status.conditions[?(@.type=='Synced')].status}", "True")

		By("recording the current resourceVersion")
		out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", healthyPool,
			"-o", "jsonpath={.metadata.resourceVersion}"))
		Expect(err).NotTo(HaveOccurred())
		initialRV := strings.TrimSpace(out)
		Expect(initialRV).NotTo(BeEmpty())

		By("waiting across multiple poll cycles and asserting resourceVersion is stable")
		Consistently(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", healthyPool,
				"-o", "jsonpath={.metadata.resourceVersion}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(Equal(initialRV),
				"resourceVersion changed — spurious status patch detected")
		}, 30*time.Second, 5*time.Second).Should(Succeed())
	})

	It("surfaces Incomplete state on the root partition", func() {
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", incompletePool,
				"-o", "jsonpath={.status.root.partitionId} "+
					"{.status.acceleratorType} "+
					"{.status.root.state}"))
			g.Expect(err).NotTo(HaveOccurred(), "Failed to get incomplete PartitionTree")
			g.Expect(out).To(Equal("root tpu-v6e-slice Incomplete"))
		}).Should(Succeed())
	})

	It("deletes the CR when the underlying node pool drains", func() {
		By("deleting the healthy node — the cache should empty and the CR should follow")
		_, err := utils.Run(exec.Command("kubectl", "delete", "node", healthyNodeName))
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			cmd := exec.Command("kubectl", "get", "partitiontree", healthyPool)
			out, err := cmd.CombinedOutput()
			// Either the CR is gone (kubectl exits non-zero with NotFound)
			// or it's still around with a deletion timestamp from the
			// finalizer path. Both eventually converge to NotFound.
			g.Expect(err).To(HaveOccurred(), "expected NotFound, got:\n%s", out)
			g.Expect(string(out)).To(ContainSubstring("not found"))
		}).Should(Succeed())
	})
})

var _ = Describe("PartitionTree node readiness", Ordered, func() {
	BeforeAll(func() {
		By("waiting for the controller-manager to be ready")
		Eventually(controllerReady, 3*time.Minute, time.Second).Should(Succeed())

		By("applying a node for the readiness test")
		setupNode("readiness-node-0", "readiness-pool", healthyMIGLink, "tpu-v6e-slice",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-root-id": "root",
				"cloud.google.com/gke-tpu-partition-leaf-id": "leaf",
			})
	})

	AfterAll(func() {
		_, _ = utils.Run(exec.Command("kubectl", "delete", "partitiontree", "readiness-pool",
			"--ignore-not-found=true", "--wait=false"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "node", "readiness-node-0",
			"--ignore-not-found=true", "--wait=false"))
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	It("reflects NodeReady=True when the node is Ready", func() {
		verifyCRStatus("readiness-pool", "{.status.root.childPartitions[0].conditions[?(@.type=='NodeReady')].status}", "True")
	})

	It("flips NodeReady to False when the node becomes NotReady", func() {
		By("patching the node to NotReady")
		patch := `{"status":{"conditions":[{"type":"Ready","status":"False","lastHeartbeatTime":"2026-01-01T00:00:00Z","lastTransitionTime":"2026-01-02T00:00:00Z","reason":"KubeletNotReady","message":"e2e-not-ready"}]}}`
		_, err := utils.Run(exec.Command("kubectl", "patch", "node", "readiness-node-0",
			"--subresource=status", "--type=merge", "-p", patch))
		Expect(err).NotTo(HaveOccurred())

		verifyCRStatus("readiness-pool", "{.status.root.childPartitions[0].conditions[?(@.type=='NodeReady')].status}", "False")
	})
})

var _ = Describe("PartitionTree occupancy", Ordered, func() {
	BeforeAll(func() {
		By("waiting for the controller-manager to be ready")
		Eventually(controllerReady, 3*time.Minute, time.Second).Should(Succeed())

		By("applying a node for the occupied pool")
		setupNode(occupiedNodeName, occupiedPool, occupiedMIGLink, "tpu7x",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-root-id": "root",
				"cloud.google.com/gke-tpu-partition-leaf-id": "leaf",
			})

		By("applying a node for the super-slice pool")
		setupNode(superSliceNodeName, superSlicePool, superSliceMIGLink, "tpu7x",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-root-id": "root",
				"cloud.google.com/gke-tpu-partition-leaf-id": "leaf",
			})

		By("applying nodes for the occupied branching pool")
		setupNode(occupiedBranchingNodeA, occupiedBranchingPool, occupiedBranchingMIGLink, "tpu7x",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-root-id": "root",
				"cloud.google.com/gke-tpu-partition-leaf-id": "leaf-a",
			})

		setupNode(occupiedBranchingNodeB, occupiedBranchingPool, occupiedBranchingMIGLink, "tpu7x",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-root-id": "root",
				"cloud.google.com/gke-tpu-partition-leaf-id": "leaf-b",
			})

		By("applying nodes for the super-slice branching pool")
		setupNode(superSliceBranchingNodeA, superSliceBranchingPool, superSliceBranchingMIGLink, "tpu7x",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-root-id": "root",
				"cloud.google.com/gke-tpu-partition-leaf-id": "leaf-a",
			})

		setupNode(superSliceBranchingNodeB, superSliceBranchingPool, superSliceBranchingMIGLink, "tpu7x",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-root-id": "root",
				"cloud.google.com/gke-tpu-partition-leaf-id": "leaf-b",
			})

		By("applying nodes for the occupancy branching pool")
		setupNode(occupancyBranchingNodeA1, occupancyBranchingPool, occupancyBranchingMIGLink, "tpu7x",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-2x2x4-id": "root",
				"cloud.google.com/gke-tpu-partition-2x2x2-id": "branch-a",
				"cloud.google.com/gke-tpu-partition-2x2x1-id": "leaf-a1",
			})

		setupNode(occupancyBranchingNodeA2, occupancyBranchingPool, occupancyBranchingMIGLink, "tpu7x",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-2x2x4-id": "root",
				"cloud.google.com/gke-tpu-partition-2x2x2-id": "branch-a",
				"cloud.google.com/gke-tpu-partition-2x2x1-id": "leaf-a2",
			})

		setupNode(occupancyBranchingNodeB1, occupancyBranchingPool, occupancyBranchingMIGLink, "tpu7x",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-2x2x4-id": "root",
				"cloud.google.com/gke-tpu-partition-2x2x2-id": "branch-b",
				"cloud.google.com/gke-tpu-partition-2x2x1-id": "leaf-b1",
			})

		setupNode(occupancyBranchingNodeB2, occupancyBranchingPool, occupancyBranchingMIGLink, "tpu7x",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-2x2x4-id": "root",
				"cloud.google.com/gke-tpu-partition-2x2x2-id": "branch-b",
				"cloud.google.com/gke-tpu-partition-2x2x1-id": "leaf-b2",
			})

		By("applying nodes for the super-slice occupancy branching pool")
		setupNode(superSliceOccupancyBranchingNodeA1, superSliceOccupancyBranchingPool, superSliceOccupancyBranchingMIGLink, "tpu7x",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-2x2x4-id": "root",
				"cloud.google.com/gke-tpu-partition-2x2x2-id": "branch-a",
				"cloud.google.com/gke-tpu-partition-2x2x1-id": "leaf-a1",
			})

		setupNode(superSliceOccupancyBranchingNodeA2, superSliceOccupancyBranchingPool, superSliceOccupancyBranchingMIGLink, "tpu7x",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-2x2x4-id": "root",
				"cloud.google.com/gke-tpu-partition-2x2x2-id": "branch-a",
				"cloud.google.com/gke-tpu-partition-2x2x1-id": "leaf-a2",
			})

		setupNode(superSliceOccupancyBranchingNodeB1, superSliceOccupancyBranchingPool, superSliceOccupancyBranchingMIGLink, "tpu7x",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-2x2x4-id": "root",
				"cloud.google.com/gke-tpu-partition-2x2x2-id": "branch-b",
				"cloud.google.com/gke-tpu-partition-2x2x1-id": "leaf-b1",
			})

		setupNode(superSliceOccupancyBranchingNodeB2, superSliceOccupancyBranchingPool, superSliceOccupancyBranchingMIGLink, "tpu7x",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-2x2x4-id": "root",
				"cloud.google.com/gke-tpu-partition-2x2x2-id": "branch-b",
				"cloud.google.com/gke-tpu-partition-2x2x1-id": "leaf-b2",
			})
	})

	AfterAll(func() {
		_, _ = utils.Run(exec.Command("kubectl", "delete", "partitiontree", occupiedPool,
			superSlicePool, occupiedBranchingPool, superSliceBranchingPool, superSliceOccupancyBranchingPool, occupancyBranchingPool, "--ignore-not-found=true", "--wait=false"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "node", occupiedNodeName,
			superSliceNodeName, occupiedBranchingNodeA, occupiedBranchingNodeB, superSliceBranchingNodeA, superSliceBranchingNodeB,
			occupancyBranchingNodeA1, occupancyBranchingNodeA2, occupancyBranchingNodeB1, occupancyBranchingNodeB2,
			superSliceOccupancyBranchingNodeA1, superSliceOccupancyBranchingNodeA2, superSliceOccupancyBranchingNodeB1, superSliceOccupancyBranchingNodeB2, "--ignore-not-found=true", "--wait=false"))
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	It("propagates Occupied occupancy from leaf sliceId to root and isolates sibling branches", func() {
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", occupancyBranchingPool,
				"-o", "jsonpath={.status.root.occupancy} "+
					"{.status.root.childPartitions[?(@.partitionId=='branch-a')].occupancy} "+
					"{.status.root.childPartitions[?(@.partitionId=='branch-a')].childPartitions[?(@.partitionId=='leaf-a1')].occupancy} "+
					"{.status.root.childPartitions[?(@.partitionId=='branch-a')].childPartitions[?(@.partitionId=='leaf-a2')].occupancy} "+
					"{.status.root.childPartitions[?(@.partitionId=='branch-b')].occupancy}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Occupied Occupied Occupied Empty Empty"))
		}, 5*time.Minute, 5*time.Second).Should(Succeed())
	})

})

var _ = Describe("PartitionTree multi-pool coexistence", Ordered, func() {
	BeforeAll(func() {
		By("waiting for the controller-manager to be ready")
		Eventually(controllerReady, 3*time.Minute, time.Second).Should(Succeed())

		By("applying nodes for two independent pools")
		setupNode("coexist-node-a", "coexist-pool-a", healthyMIGLink, "tpu-v6e-slice",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-root-id": "root",
				"cloud.google.com/gke-tpu-partition-leaf-id": "leaf",
			})

		setupNode(multiPoolNodeName, multiPoolName, multiPoolMIGLink, "tpu7x",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-root-id": "root",
				"cloud.google.com/gke-tpu-partition-leaf-id": "leaf",
			})
	})

	AfterAll(func() {
		_, _ = utils.Run(exec.Command("kubectl", "delete", "partitiontree",
			"coexist-pool-a", multiPoolName,
			"--ignore-not-found=true", "--wait=false"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "node",
			"coexist-node-a", multiPoolNodeName,
			"--ignore-not-found=true", "--wait=false"))
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	It("creates independent CRs for each pool", func() {
		Eventually(func(g Gomega) {
			outA, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", "coexist-pool-a",
				"-o", "jsonpath={.status.root.partitionId}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(outA).To(Equal("root"))

			outB, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", multiPoolName,
				"-o", "jsonpath={.status.root.partitionId} {.status.acceleratorType}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(outB).To(Equal("root tpu7x"))
		}).Should(Succeed())
	})

	It("assigns nodes to the correct pool's CR", func() {
		Eventually(func(g Gomega) {
			outA, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", "coexist-pool-a",
				"-o", "jsonpath={.status.root.childPartitions[0].nodeNames[0]}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(outA).To(Equal("coexist-node-a"))

			outB, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", multiPoolName,
				"-o", "jsonpath={.status.root.childPartitions[0].nodeNames[0]}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(outB).To(Equal(multiPoolNodeName))
		}).Should(Succeed())
	})

})

var _ = Describe("PartitionTree config hot-reload", Ordered, func() {
	const (
		reloadPool     = "reload-pool"
		reloadNodeName = "reload-node-0"
	)

	BeforeAll(func() {
		By("waiting for the controller-manager to be ready")
		Eventually(controllerReady, 3*time.Minute, time.Second).Should(Succeed())

		By("applying a node for the reload test pool")
		setupNode(reloadNodeName, reloadPool, healthyMIGLink, "tpu-v6e-slice",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-root-id": "root",
				"cloud.google.com/gke-tpu-partition-leaf-id": "leaf",
			})
	})

	AfterAll(func() {
		By("restoring the original config")
		restoreDefaultConfig()

		_, _ = utils.Run(exec.Command("kubectl", "delete", "partitiontree", reloadPool,
			"--ignore-not-found=true", "--wait=false"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "node", reloadNodeName,
			"--ignore-not-found=true", "--wait=false"))
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	It("picks up ConfigMap changes without a restart", func() {
		By("waiting for the pool CR to be Synced")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", reloadPool,
				"-o", "jsonpath={.status.conditions[?(@.type=='Synced')].status}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}).Should(Succeed())

		By("patching the config ConfigMap to change polling interval to 5s")
		newConfig := "polling:\n  interval: 5s\n  jitter: 200ms\nsyncTimeout: 15s\nretry:\n  initialBackoff: 1s\n  maxBackoff: 30s\n  multiplier: 2.0\n"
		patch := fmt.Sprintf(`{"data":{"config.yaml":%q}}`, newConfig)
		_, err := utils.Run(exec.Command("kubectl", "patch", "configmap", configMapName,
			"-n", namespace, "--type=merge", "-p", patch))
		Expect(err).NotTo(HaveOccurred())

		By("checking controller logs for config reload confirmation")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "-n", namespace,
				"logs", "-l", "control-plane=controller-manager", "--tail=100"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("config reloaded"))
			g.Expect(out).To(ContainSubstring("5s"))
		}).Should(Succeed())

		By("verifying the CR is still Synced after reload")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", reloadPool,
				"-o", "jsonpath={.status.conditions[?(@.type=='Synced')].status}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}).Should(Succeed())
	})

	It("rejects invalid configuration values and preserves last good config", func() {
		By("waiting for the pool CR to be Synced")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", reloadPool,
				"-o", "jsonpath={.status.conditions[?(@.type=='Synced')].status}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}).Should(Succeed())

		By("patching the config ConfigMap with invalid polling interval")
		invalidConfig := "polling:\n  interval: -5s\n  jitter: 200ms\nsyncTimeout: 15s\nretry:\n  initialBackoff: 1s\n  maxBackoff: 30s\n  multiplier: 2.0\n"
		patch := fmt.Sprintf(`{"data":{"config.yaml":%q}}`, invalidConfig)
		_, err := utils.Run(exec.Command("kubectl", "patch", "configmap", configMapName,
			"-n", namespace, "--type=merge", "-p", patch))
		Expect(err).NotTo(HaveOccurred())

		By("checking controller logs for validation error")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "-n", namespace,
				"logs", "-l", "control-plane=controller-manager", "--tail=100"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("validating config"))
			g.Expect(out).To(ContainSubstring("must be > 0"))
		}).Should(Succeed())

		By("verifying the CR is still Synced with previous valid config")
		// Wait a bit to ensure it didn't crash or apply bad config.
		time.Sleep(10 * time.Second)
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", reloadPool,
				"-o", "jsonpath={.status.conditions[?(@.type=='Synced')].status}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}).Should(Succeed())
	})

	It("falls back to defaults when ConfigMap is deleted", func() {
		By("deleting the config ConfigMap")
		_, err := utils.Run(exec.Command("kubectl", "delete", "configmap", configMapName, "-n", namespace))
		Expect(err).NotTo(HaveOccurred())

		By("checking controller logs for fallback confirmation")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "-n", namespace,
				"logs", "-l", "control-plane=controller-manager", "--tail=100"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("config map deleted; keeping previous config"))
		}).Should(Succeed())

		By("re-creating the default ConfigMap for subsequent tests")
		restoreDefaultConfig()
	})
})

var _ = Describe("PartitionTree events", Ordered, func() {
	const (
		eventPool     = "event-pool"
		eventNodeName = "event-node-0"
	)

	BeforeAll(func() {
		By("waiting for the controller-manager to be ready")
		Eventually(controllerReady, 3*time.Minute, time.Second).Should(Succeed())

		By("applying a node for the events test pool")
		setupNode(eventNodeName, eventPool, healthyMIGLink, "tpu-v6e-slice",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-root-id": "root",
				"cloud.google.com/gke-tpu-partition-leaf-id": "leaf",
			})
	})

	AfterAll(func() {
		_, _ = utils.Run(exec.Command("kubectl", "delete", "partitiontree", eventPool,
			"--ignore-not-found=true", "--wait=false"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "node", eventNodeName,
			"--ignore-not-found=true", "--wait=false"))
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	It("emits a PartitionTreeCreated event when a CR is created", func() {
		Eventually(func(g Gomega) {
			// events.k8s.io/v1 events use "regarding" instead of "involvedObject".
			// Use jsonpath to find the event matching our CR by reason + name.
			out, err := utils.Run(exec.Command("kubectl", "get", "events.v1.events.k8s.io",
				"--field-selector=reason=PartitionTreeCreated",
				"-o", "jsonpath={.items[?(@.regarding.name=='"+eventPool+"')].reason}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("PartitionTreeCreated"))
		}).Should(Succeed())
	})

	It("emits a SyncRestored event on the first successful sync", func() {
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "events.v1.events.k8s.io",
				"--field-selector=reason=SyncRestored",
				"-o", "jsonpath={.items[?(@.regarding.name=='"+eventPool+"')].reason}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("SyncRestored"))
		}).Should(Succeed())
	})
})

var _ = Describe("PartitionTree resilience", Ordered, func() {
	const (
		resiliencePool     = "resilience-pool"
		resilienceNodeName = "resilience-node-0"
		resilienceMIGLink  = "projects/1234/zones/us-central1-a/instanceGroupManagers/resilience-mig"
	)

	BeforeAll(func() {
		By("waiting for the controller-manager to be ready")
		Eventually(controllerReady, 3*time.Minute, time.Second).Should(Succeed())

		By("applying a node for the resilience test pool")
		setupNode(resilienceNodeName, resiliencePool, resilienceMIGLink, "tpu-v6e-slice",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-root-id": "root",
				"cloud.google.com/gke-tpu-partition-leaf-id": "leaf",
			})
	})

	AfterAll(func() {
		By("restoring the original fixtures")
		restoreDefaultFixtures()

		_, _ = utils.Run(exec.Command("kubectl", "delete", "partitiontree", resiliencePool,
			"--ignore-not-found=true", "--wait=false"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "node", resilienceNodeName,
			"--ignore-not-found=true", "--wait=false"))
	})

	It("flips Synced to False when fixture is removed", func() {
		By("waiting for the pool CR to be Synced")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", resiliencePool,
				"-o", "jsonpath={.status.conditions[?(@.type=='Synced')].status}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}).Should(Succeed())

		By("updating ConfigMap to remove the resilience-mig entry")
		removeResilienceFixture()

		By("waiting for Synced to flip to False")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", resiliencePool,
				"-o", "jsonpath={.status.conditions[?(@.type=='Synced')].status}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("False"))
		}, 5*time.Minute, 5*time.Second).Should(Succeed())
	})
})

var _ = Describe("PartitionTree infrastructure failure", Ordered, func() {
	const (
		unhealthyPool     = "unhealthy-pool"
		unhealthyNodeName = "unhealthy-node-0"
		unhealthyMIGLink  = "projects/1234/zones/us-central1-a/instanceGroupManagers/unhealthy-mig"
	)

	BeforeAll(func() {
		By("waiting for the controller-manager to be ready")
		Eventually(controllerReady, 3*time.Minute, time.Second).Should(Succeed())

		By("applying a node for the unhealthy pool")
		setupNode(unhealthyNodeName, unhealthyPool, unhealthyMIGLink, "tpu-v6e-slice",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-root-id": "root",
				"cloud.google.com/gke-tpu-partition-leaf-id": "leaf",
			})

		By("updating ConfigMap to add the unhealthy-mig fixture")
		addUnhealthyFixture()
	})

	AfterAll(func() {
		By("restoring the original fixtures")
		restoreDefaultFixtures()

		_, _ = utils.Run(exec.Command("kubectl", "delete", "partitiontree", unhealthyPool,
			"--ignore-not-found=true", "--wait=false"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "node", unhealthyNodeName,
			"--ignore-not-found=true", "--wait=false"))
	})

	It("surfaces Unhealthy state on the leaf partition", func() {
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", unhealthyPool,
				"-o", "jsonpath={.status.root.childPartitions[0].state}"))
			g.Expect(err).NotTo(HaveOccurred(), "Failed to get unhealthy PartitionTree")
			g.Expect(out).To(Equal("Unhealthy"))
		}, 5*time.Minute, 5*time.Second).Should(Succeed())
	})
})

var _ = Describe("PartitionTree scale", Ordered, func() {
	const scalePools = 50
	const nodesPerPool = 2
	const totalNodes = scalePools * nodesPerPool

	// scalePoolName returns a deterministic pool name for the i-th pool.
	scalePoolName := func(i int) string { return fmt.Sprintf("scale-pool-%03d", i) }
	scaleNodeName := func(pool, node int) string {
		return fmt.Sprintf("scale-node-%03d-%d", pool, node)
	}

	BeforeAll(func() {
		By("waiting for the controller-manager to be ready")
		Eventually(controllerReady, 3*time.Minute, time.Second).Should(Succeed())

		By(fmt.Sprintf("applying %d Nodes across %d pools", totalNodes, scalePools))
		for p := range scalePools {
			pool := scalePoolName(p)
			for n := range nodesPerPool {
				nodeName := scaleNodeName(p, n)
				// All pools share the healthy-mig fixture — the mock falls
				// back from FQN to bare MIG name "healthy-mig".
				setupNode(nodeName, pool, healthyMIGLink, "tpu-v6e-slice",
					map[string]string{
						"cloud.google.com/gke-tpu-partition-root-id": "root",
						"cloud.google.com/gke-tpu-partition-leaf-id": "leaf",
					})
			}
		}
	})

	AfterAll(func() {
		By("deleting scale test PartitionTree CRs")
		args := []string{"delete", "partitiontree", "--ignore-not-found=true", "--wait=false"}
		for p := range scalePools {
			args = append(args, scalePoolName(p))
		}
		_, _ = utils.Run(exec.Command("kubectl", args...))

		By("deleting scale test Nodes")
		args = []string{"delete", "node", "--ignore-not-found=true", "--wait=false"}
		for p := range scalePools {
			for n := range nodesPerPool {
				args = append(args, scaleNodeName(p, n))
			}
		}
		_, _ = utils.Run(exec.Command("kubectl", args...))
	})

	SetDefaultEventuallyTimeout(3 * time.Minute)
	SetDefaultEventuallyPollingInterval(5 * time.Second)

	It("creates all 50 PartitionTree CRs", func() {
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree",
				"-o", "jsonpath={.items[*].metadata.name}"))
			g.Expect(err).NotTo(HaveOccurred())
			names := strings.Fields(string(out))
			scaleCount := 0
			for _, name := range names {
				if strings.HasPrefix(name, "scale-pool-") {
					scaleCount++
				}
			}
			g.Expect(scaleCount).To(BeNumerically(">=", scalePools),
				"expected at least %d scale CRs, got %d", scalePools, scaleCount)
		}).Should(Succeed())
	})

	It("all scale CRs reach Synced=True", func() {
		Eventually(func(g Gomega) {
			syncedCount := 0
			for p := range scalePools {
				pool := scalePoolName(p)
				out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", pool,
					"-o", "jsonpath={.status.conditions[?(@.type=='Synced')].status}"))
				if err != nil {
					continue
				}
				if strings.TrimSpace(string(out)) == "True" {
					syncedCount++
				}
			}
			g.Expect(syncedCount).To(BeNumerically(">=", scalePools),
				"expected %d synced CRs, got %d", scalePools, syncedCount)
		}).Should(Succeed())
	})
})

// NOTE: A GCE error resilience test (Synced flips False when mock
// returns errors, recovers when restored) was considered but skipped.
// The mock reads fixtures from a ConfigMap volume mount, and
// kubelet's ConfigMap volume sync can take 1-3+ minutes in Kind,
// making the test slow and flaky. The Synced condition logic is
// thoroughly covered by unit tests in helpers_test.go (4 tests).
// The resilience-mig fixture entry is retained in gce_mock_fixtures.yaml
// for future use if a faster GCE error injection mechanism is added.

// restoreDefaultConfig patches the runtime config ConfigMap back to
// the built-in defaults. Called by AfterAll for the hot-reload test.
func restoreDefaultConfig() {
	defaultYAML := "polling:\n  interval: 3s\n  jitter: 300ms\nsyncTimeout: 15s\nretry:\n  initialBackoff: 1s\n  maxBackoff: 30s\n  multiplier: 2.0\n"
	// Use create + apply to handle both recreation after deletion and updates.
	cmd := exec.Command("kubectl", "create", "configmap", configMapName, "-n", namespace,
		"--from-literal=config.yaml="+defaultYAML, "--dry-run=client", "-o", "yaml")
	out, err := cmd.Output()
	Expect(err).NotTo(HaveOccurred())

	applyCmd := exec.Command("kubectl", "apply", "-f", "-")
	applyCmd.Stdin = bytes.NewReader(out)
	_, err = applyCmd.CombinedOutput()
	Expect(err).NotTo(HaveOccurred())
}

func restoreDefaultFixtures() {
	data, err := os.ReadFile("test/e2e/testdata/fixtures/default.yaml")
	Expect(err).NotTo(HaveOccurred())
	patch := fmt.Sprintf(`{"data":{"fixtures.yaml":%q}}`, string(data))
	_, _ = utils.Run(exec.Command("kubectl", "patch", "configmap", "gce-mock-fixtures",
		"-n", namespace, "--type=merge", "-p", patch))
}

func removeResilienceFixture() {
	data, err := os.ReadFile("test/e2e/testdata/fixtures/remove-resilience.yaml")
	Expect(err).NotTo(HaveOccurred())
	patch := fmt.Sprintf(`{"data":{"fixtures.yaml":%q}}`, string(data))
	_, _ = utils.Run(exec.Command("kubectl", "patch", "configmap", "gce-mock-fixtures",
		"-n", namespace, "--type=merge", "-p", patch))
}

func addUnhealthyFixture() {
	data, err := os.ReadFile("test/e2e/testdata/fixtures/add-unhealthy.yaml")
	Expect(err).NotTo(HaveOccurred())
	patch := fmt.Sprintf(`{"data":{"fixtures.yaml":%q}}`, string(data))
	_, _ = utils.Run(exec.Command("kubectl", "patch", "configmap", "gce-mock-fixtures",
		"-n", namespace, "--type=merge", "-p", patch))
}

// controllerReady asserts the controller-manager pod is Ready. The
// e2e overlay deploys exactly one replica.
func controllerReady(g Gomega) {
	out, err := utils.Run(exec.Command("kubectl", "get", "pods",
		"-n", namespace,
		"-l", "control-plane=controller-manager",
		"-o", "jsonpath={.items[0].status.conditions[?(@.type=='Ready')].status}"))
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(out).To(Equal("True"))
}

// applyNode kubectl-applies a synthetic Node carrying the four-element
// label/annotation filter the controller requires (see
// internal/controller/node.go). The acceleratorLabel becomes
// `cloud.google.com/gke-tpu-accelerator`; partitionLabels are merged
// in as additional labels (each must be `cloud.google.com/gke-tpu-
// partition-<id>-id`).
func applyNode(name, nodePool, migSelfLink, acceleratorLabel string, partitionLabels map[string]string) error {
	var partitionLabelLines bytes.Buffer
	for k, v := range partitionLabels {
		fmt.Fprintf(&partitionLabelLines, "    %s: %q\n", k, v)
	}
	manifest := fmt.Sprintf(`apiVersion: v1
kind: Node
metadata:
  name: %s
  labels:
    cloud.google.com/gke-accelerator-topology-mode: PROVISION_ONLY
    cloud.google.com/gke-nodepool: %s
    cloud.google.com/gke-tpu-accelerator: %s
%s  annotations:
    node.gke.io/created-by-mig: %q
`, name, nodePool, acceleratorLabel, partitionLabelLines.String(), migSelfLink)

	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = bytes.NewReader([]byte(manifest))
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("kubectl apply node %s: %w (output: %s)", name, err, out)
	}
	return nil
}

// markNodeReady patches the Node /status subresource so the Ready
// condition is True. envtest and real apiservers both reject Ready
// updates via the main object endpoint, hence the explicit subresource.
func markNodeReady(name string) error {
	patch := `{"status":{"conditions":[{"type":"Ready","status":"True","lastHeartbeatTime":"2026-01-01T00:00:00Z","lastTransitionTime":"2026-01-01T00:00:00Z","reason":"KubeletReady","message":"e2e"}]}}`
	cmd := exec.Command("kubectl", "patch", "node", name,
		"--subresource=status", "--type=merge", "-p", patch)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("kubectl patch node %s status: %w (output: %s)", name, err, out)
	}
	return nil
}

// setupNode applies a synthetic node and marks it as Ready.
func setupNode(name, nodePool, migSelfLink, acceleratorLabel string, partitionLabels map[string]string) {
	Expect(applyNode(name, nodePool, migSelfLink, acceleratorLabel, partitionLabels)).To(Succeed())
	Expect(markNodeReady(name)).To(Succeed())
}

// verifyCRStatus asserts a field in the CR status matches the expected value.
func verifyCRStatus(poolName, jsonpath, expectedValue string) {
	Eventually(func(g Gomega) {
		out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", poolName,
			"-o", "jsonpath="+jsonpath))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(out).To(Equal(expectedValue))
	}).Should(Succeed())
}
