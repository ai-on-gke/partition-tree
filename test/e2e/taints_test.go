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
	"encoding/json"
	"fmt"
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ai-on-gke/partition-tree/test/utils"
)

var _ = Describe("PartitionTree External Taints", Ordered, func() {
	const (
		taintsPoolName = "taints-pool"
		taintsMIGLink  = "projects/1234/zones/us-central1-a/instanceGroupManagers/healthy-mig"
		taintsNodeName = "taints-node-0"
	)

	BeforeAll(func() {
		By("waiting for the controller-manager to be ready")
		Eventually(controllerReady, 3*time.Minute, time.Second).Should(Succeed())

		By("applying a synthetic Node for the taints pool")
		setupNode(taintsNodeName, taintsPoolName, taintsMIGLink, "tpu-v6e-slice",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-root-id": "root",
				"cloud.google.com/gke-tpu-partition-leaf-id": "leaf",
				"cloud.google.com/gke-tpu-topology":          "2x2",
			})
	})

	AfterAll(func() {
		By("deleting the e2e PartitionTree CR")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "partitiontree", taintsPoolName, "--ignore-not-found=true", "--wait=false"))

		By("deleting the e2e Node")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "node", taintsNodeName, "--ignore-not-found=true", "--wait=false"))
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	It("creates a healthy PartitionTree CR without taints initially", func() {
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", taintsPoolName,
				"-o", "jsonpath={.status.root.partitionId} {.status.root.taints}"))
			g.Expect(err).NotTo(HaveOccurred())
			// taints field doesn't exist so it will just be "root "
			g.Expect(out).To(Equal("root "))
		}).Should(Succeed())
	})

	It("propagates a valid external taint annotation to the tree", func() {
		By("patching the CR with a valid taint annotation")
		patch := `{"metadata":{"annotations":{"taints.partitiontree.gke.io/test-taint":"{\"partitionId\":\"root\", \"key\":\"test-key\", \"effect\":\"NoSchedule\"}"}}}`
		_, err := utils.Run(exec.Command("kubectl", "patch", "partitiontree", taintsPoolName, "--type=merge", "-p", patch))
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the status.tree to reflect the taint")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", taintsPoolName,
				"-o", "jsonpath={.status.root.taints[0].key} {.status.root.taints[0].effect}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("test-key NoSchedule"))
		}).Should(Succeed())

		By("verifying the TaintsValid condition is True")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", taintsPoolName,
				"-o", "jsonpath={.status.conditions[?(@.type==\"TaintsValid\")].status}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}).Should(Succeed())
	})

	It("emits a Warning event for malformed JSON and preserves taints", func() {
		By("patching the CR with a malformed JSON string")
		patch := `{"metadata":{"annotations":{"taints.partitiontree.gke.io/test-taint":"{\"partitionId\":\"root\", \"key\":\"test-key\", \"effect\":\"NoSchedule\""}}}`
		_, err := utils.Run(exec.Command("kubectl", "patch", "partitiontree", taintsPoolName, "--type=merge", "-p", patch))
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the Warning event TaintParseError")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "events", "--field-selector", "involvedObject.name="+taintsPoolName+",reason=TaintParseError", "-o", "jsonpath={.items[*].message}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("Failed to parse taints annotations"))
		}).Should(Succeed())

		By("verifying the taints are preserved in the status")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", taintsPoolName,
				"-o", "jsonpath={.status.root.taints[0].key}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("test-key"))
		}).Should(Succeed())

		By("verifying the TaintsValid condition is False")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", taintsPoolName,
				"-o", "jsonpath={.status.conditions[?(@.type==\"TaintsValid\")].status}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("False"))
		}).Should(Succeed())
	})

	It("emits a Warning event for invalid TaintEffect", func() {
		By("patching the CR with valid JSON but an invalid effect")
		patch := `{"metadata":{"annotations":{"taints.partitiontree.gke.io/test-taint":"{\"partitionId\":\"root\", \"key\":\"test-key\", \"effect\":\"InvalidEffect\"}"}}}`
		_, err := utils.Run(exec.Command("kubectl", "patch", "partitiontree", taintsPoolName, "--type=merge", "-p", patch))
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the Warning event TaintParseError")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "events", "--field-selector", "involvedObject.name="+taintsPoolName+",reason=TaintParseError", "-o", "jsonpath={.items[*].message}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("invalid taint effect \"InvalidEffect\""))
		}).Should(Succeed())
	})

	It("clears taints completely when annotation is removed", func() {
		By("removing the annotation via JSON patch")
		patch := `[{"op": "remove", "path": "/metadata/annotations/taints.partitiontree.gke.io~1test-taint"}]`
		_, err := utils.Run(exec.Command("kubectl", "patch", "partitiontree", taintsPoolName, "--type=json", "-p", patch))
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the status.tree to reflect no taints")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", taintsPoolName,
				"-o", "jsonpath={.status.root.taints}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(BeEmpty())
		}).Should(Succeed())
	})

	It("clears taints completely when EnableTaints is disabled globally", func() {
		By("patching the CR with a valid taint annotation")
		patch := `{"metadata":{"annotations":{"taints.partitiontree.gke.io/test-taint":"{\"partitionId\":\"root\", \"key\":\"test-key\", \"effect\":\"NoSchedule\"}"}}}`
		_, err := utils.Run(exec.Command("kubectl", "patch", "partitiontree", taintsPoolName, "--type=merge", "-p", patch))
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the status.tree to reflect the taint")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", taintsPoolName,
				"-o", "jsonpath={.status.root.taints[0].key}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("test-key"))
		}).Should(Succeed())

		By("disabling taints in the controller deployment")
		argsPatch := `[{"op": "replace", "path": "/spec/template/spec/containers/0/args", "value": ["--leader-elect", "--health-probe-bind-address=:8081", "--metrics-bind-address=:8443", "--otlp-endpoint=", "--gce-mock", "--gce-mock-fixtures=/etc/gce-mock/fixtures.yaml"]}]`
		_, err = utils.Run(exec.Command("kubectl", "patch", "deployment", "partition-controller-manager", "-n", namespace, "--type=json", "-p", argsPatch))
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the controller-manager to be redeployed and ready")
		_, err = utils.Run(exec.Command("kubectl", "rollout", "status", "deployment/partition-controller-manager", "-n", namespace, "--timeout=3m"))
		Expect(err).NotTo(HaveOccurred())
		Eventually(controllerReady, 3*time.Minute, time.Second).Should(Succeed())

		By("waiting for the status.tree to reflect no taints")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", taintsPoolName,
				"-o", "jsonpath={.status.root.taints}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(BeEmpty())
		}).Should(Succeed())

		// Clean up: restore the flag
		By("re-enabling taints in the controller deployment")
		restorePatch := `[{"op": "replace", "path": "/spec/template/spec/containers/0/args", "value": ["--leader-elect", "--health-probe-bind-address=:8081", "--metrics-bind-address=:8443", "--otlp-endpoint=", "--gce-mock", "--gce-mock-fixtures=/etc/gce-mock/fixtures.yaml", "--enable-taints"]}]`
		_, err = utils.Run(exec.Command("kubectl", "patch", "deployment", "partition-controller-manager", "-n", namespace, "--type=json", "-p", restorePatch))
		Expect(err).NotTo(HaveOccurred())

		_, err = utils.Run(exec.Command("kubectl", "rollout", "status", "deployment/partition-controller-manager", "-n", namespace, "--timeout=3m"))
		Expect(err).NotTo(HaveOccurred())
		Eventually(controllerReady, 3*time.Minute, time.Second).Should(Succeed())

		// Also clean up the annotation we added
		By("removing the annotation")
		removePatch := `[{"op": "remove", "path": "/metadata/annotations/taints.partitiontree.gke.io~1test-taint"}]`
		_, err = utils.Run(exec.Command("kubectl", "patch", "partitiontree", taintsPoolName, "--type=json", "-p", removePatch))
		Expect(err).NotTo(HaveOccurred())
	})

	It("does not propagate taints up the tree", func() {
		By("patching the CR with a taint on a leaf partition")
		patch := `{"metadata":{"annotations":{"taints.partitiontree.gke.io/leaf-taint":"{\"partitionId\":\"leaf\", \"key\":\"leaf-key\", \"effect\":\"NoSchedule\"}"}}}`
		_, err := utils.Run(exec.Command("kubectl", "patch", "partitiontree", taintsPoolName, "--type=merge", "-p", patch))
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the status.tree to reflect the taint on the leaf")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", taintsPoolName,
				"-o", "jsonpath={.status.root.childPartitions[0].taints[0].key}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("leaf-key"))
		}).Should(Succeed())

		By("verifying the root partition does NOT inherit the taint")
		Consistently(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", taintsPoolName,
				"-o", "jsonpath={.status.root.taints}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(BeEmpty())
		}, 5*time.Second, time.Second).Should(Succeed())

		By("removing the leaf taint annotation")
		removePatch := `[{"op": "remove", "path": "/metadata/annotations/taints.partitiontree.gke.io~1leaf-taint"}]`
		_, err = utils.Run(exec.Command("kubectl", "patch", "partitiontree", taintsPoolName, "--type=json", "-p", removePatch))
		Expect(err).NotTo(HaveOccurred())
	})

	It("surfaces cloud.google.com/nodes-not-found taint on a leaf when no node is assigned", func() {
		const missingPoolName = "missing-nodes-pool"
		const missingMIGLink = "projects/1234/zones/us-central1-a/instanceGroupManagers/healthy-mig"
		const missingNodeName = "missing-node-0"

		By("applying a node for missing-nodes-pool")
		setupNode(missingNodeName, missingPoolName, missingMIGLink, "tpu-v6e-slice",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-root-id": "root",
				"cloud.google.com/gke-tpu-partition-leaf-id": "leaf",
				"cloud.google.com/gke-tpu-topology":          "2x2",
			})

		By("waiting for the PartitionTree CR to be created and synced")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", missingPoolName,
				"-o", "jsonpath={.status.conditions[?(@.type==\"Synced\")].status}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}).Should(Succeed())

		By("deleting the node to trigger NodesNotFound on leaf")
		_, err := utils.Run(exec.Command("kubectl", "delete", "node", missingNodeName, "--ignore-not-found=true"))
		Expect(err).NotTo(HaveOccurred())

		By("waiting for cloud.google.com/nodes-not-found taint to appear on leaf")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", missingPoolName,
				"-o", "jsonpath={.status.root.childPartitions[0].taints[?(@.key==\"cloud.google.com/nodes-not-found\")].effect}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("NoExecute"))
		}).Should(Succeed())

		By("cleaning up missing-nodes-pool")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "partitiontree", missingPoolName, "--ignore-not-found=true", "--wait=false"))
	})

	It("surfaces cloud.google.com/partition-state taint when a partition is Unhealthy", func() {
		const unhPoolName = "unhealthy-taint-pool"
		const unhMIGLink = "projects/1234/zones/us-central1-a/instanceGroupManagers/unhealthy-mig"
		const unhNodeName = "unhealthy-taint-node-0"

		By("updating ConfigMap to add the unhealthy-mig fixture")
		addUnhealthyFixture()

		By("applying a node for the unhealthy pool")
		setupNode(unhNodeName, unhPoolName, unhMIGLink, "tpu-v6e-slice",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-root-id": "root",
				"cloud.google.com/gke-tpu-partition-leaf-id": "leaf",
			})

		By("waiting for cloud.google.com/partition-state taint to appear on the leaf partition")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", unhPoolName,
				"-o", "jsonpath={.status.root.childPartitions[0].taints[?(@.key==\"cloud.google.com/partition-state\")].value} {.status.root.childPartitions[0].taints[?(@.key==\"cloud.google.com/partition-state\")].effect}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Unhealthy NoExecute"))
		}).Should(Succeed())

		By("restoring original fixtures and cleaning up")
		restoreDefaultFixtures()
		_, _ = utils.Run(exec.Command("kubectl", "delete", "partitiontree", unhPoolName, "--ignore-not-found=true", "--wait=false"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "node", unhNodeName, "--ignore-not-found=true", "--wait=false"))
	})

	Context("Node Taint Propagation", func() {
		const (
			extraNodeName = "taints-node-1"
			configMapName = "partition-config"
		)

		BeforeEach(func() {
			By("enabling taint propagation in config")
			patch := `{"data":{"config.yaml":"polling:\n  interval: 1s\n  jitter: 100ms\ntaints:\n  nodePropagation:\n    excludedPrefixes:\n    - other.com/"}}`
			_, err := utils.Run(exec.Command("kubectl", "patch", "configmap", configMapName,
				"-n", namespace, "--type=merge", "-p", patch))
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			By("restoring original config")
			defaultConfig := `{"data":{"config.yaml":"polling:\n  interval: 1s\n  jitter: 100ms\n"}}`
			_, err := utils.Run(exec.Command("kubectl", "patch", "configmap", configMapName,
				"-n", namespace, "--type=merge", "-p", defaultConfig))
			Expect(err).NotTo(HaveOccurred())

			By("cleaning up extra node")
			_, _ = utils.Run(exec.Command("kubectl", "delete", "node", extraNodeName, "--ignore-not-found=true", "--wait=false"))

			By("cleaning up taints on main node")
			_, _ = utils.Run(exec.Command("kubectl", "patch", "node", taintsNodeName, "--type=json", "-p", `[{"op":"remove","path":"/spec/taints"}]`))
			_ = markNodeReady(taintsNodeName)
		})

		It("propagates matching taints from a single node to the leaf partition", func() {
			By("adding taints to the node")
			_, err := utils.Run(exec.Command("kubectl", "taint", "node", taintsNodeName,
				"example.com/a=v1:NoSchedule", "other.com/b=v2:NoExecute", "--overwrite"))
			Expect(err).NotTo(HaveOccurred())

			By("waiting for the leaf partition to reflect only the matching taint")
			Eventually(func(g Gomega) {
				out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", taintsPoolName,
					"-o", "jsonpath={.status.root.childPartitions[0].taints}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring("example.com/a"))
				g.Expect(out).NotTo(ContainSubstring("other.com/b"))
			}).Should(Succeed())
		})

		It("propagates the union of taints when multiple nodes exist", func() {
			By("creating a second node in the same partition")
			setupNode(extraNodeName, taintsPoolName, taintsMIGLink, "tpu-v6e-slice",
				map[string]string{
					"cloud.google.com/gke-tpu-partition-root-id": "root",
					"cloud.google.com/gke-tpu-partition-leaf-id": "leaf",
					"cloud.google.com/gke-tpu-topology":          "2x2",
				})

			By("adding taints to both nodes")
			_, err := utils.Run(exec.Command("kubectl", "taint", "node", taintsNodeName,
				"example.com/a=v1:NoSchedule", "example.com/b=v2:NoExecute", "--overwrite"))
			Expect(err).NotTo(HaveOccurred())

			_, err = utils.Run(exec.Command("kubectl", "taint", "node", extraNodeName,
				"example.com/a=v1:NoSchedule", "example.com/c=v3:NoSchedule", "--overwrite"))
			Expect(err).NotTo(HaveOccurred())

			By("waiting for the leaf partition to reflect the union of taints")
			Eventually(func(g Gomega) {
				out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", taintsPoolName,
					"-o", "jsonpath={.status.root.childPartitions[0].taints}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring("example.com/a"))
				g.Expect(out).To(ContainSubstring("example.com/b"))
				g.Expect(out).To(ContainSubstring("example.com/c"))
			}).Should(Succeed())
		})

		It("propagates PartitionTaint.timeAdded and preserves it across not-ready to unreachable swaps", func() {
			t0 := time.Now().UTC().Add(-30 * time.Second).Truncate(time.Second).Format(time.RFC3339)
			tSwap := time.Now().UTC().Add(-10 * time.Second).Truncate(time.Second).Format(time.RFC3339)

			patchNodeReadyCond := func(status, reason, transition string) {
				hb := time.Now().UTC().Format(time.RFC3339)
				condPatch := fmt.Sprintf(
					`{"status":{"conditions":[{"type":"Ready","status":%q,"lastHeartbeatTime":%q,`+
						`"lastTransitionTime":%q,"reason":%q,"message":"e2e"}]}}`,
					status, hb, transition, reason)
				_, err := utils.Run(exec.Command("kubectl", "patch", "node", taintsNodeName,
					"--subresource=status", "--type=merge", "-p", condPatch))
				ExpectWithOffset(1, err).NotTo(HaveOccurred())
			}
			patchNodeNoExecTaint := func(key, added string) {
				taintPatch := fmt.Sprintf(
					`{"spec":{"taints":[{"key":%q,"effect":"NoExecute","timeAdded":%q}]}}`,
					key, added)
				_, err := utils.Run(exec.Command("kubectl", "patch", "node", taintsNodeName,
					"--type=merge", "-p", taintPatch))
				ExpectWithOffset(1, err).NotTo(HaveOccurred())
			}
			leafNoExecTimeAdded := func(key string) (string, error) {
				out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", taintsPoolName,
					"-o", "json"))
				if err != nil {
					return "", err
				}
				var pt struct {
					Status struct {
						Root struct {
							ChildPartitions []struct {
								Taints []struct {
									Key       string `json:"key"`
									Effect    string `json:"effect"`
									TimeAdded string `json:"timeAdded"`
								} `json:"taints"`
							} `json:"childPartitions"`
						} `json:"root"`
					} `json:"status"`
				}
				if err := json.Unmarshal([]byte(out), &pt); err != nil {
					return "", err
				}
				if len(pt.Status.Root.ChildPartitions) == 0 {
					return "", nil
				}
				for _, t := range pt.Status.Root.ChildPartitions[0].Taints {
					if t.Key == key && t.Effect == "NoExecute" {
						return t.TimeAdded, nil
					}
				}
				return "", nil
			}

			By("marking the synthetic node Ready=False and adding not-ready:NoExecute with timeAdded=t0")
			patchNodeReadyCond("False", "KubeletNotReady", t0)
			patchNodeNoExecTaint("node.kubernetes.io/not-ready", t0)

			Eventually(func(g Gomega) {
				patchNodeReadyCond("False", "KubeletNotReady", t0)
				got, err := leafNoExecTimeAdded("node.kubernetes.io/not-ready")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(got).To(Equal(t0))
			}).Should(Succeed())

			By("swapping node taint to unreachable:NoExecute with newer timeAdded and verifying t0 is preserved (R4)")
			patchNodeReadyCond("Unknown", "NodeStatusUnknown", tSwap)
			patchNodeNoExecTaint("node.kubernetes.io/unreachable", tSwap)

			Eventually(func(g Gomega) {
				patchNodeReadyCond("Unknown", "NodeStatusUnknown", tSwap)
				got, err := leafNoExecTimeAdded("node.kubernetes.io/unreachable")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(got).To(Equal(t0))
			}).Should(Succeed())

			By("recovering node health and verifying a subsequent failure episode gets fresh timeAdded=tFresh")
			Expect(markNodeReady(taintsNodeName)).To(Succeed())
			_, err := utils.Run(exec.Command("kubectl", "patch", "node", taintsNodeName,
				"--type=merge", "-p", `{"spec":{"taints":null}}`))
			Expect(err).NotTo(HaveOccurred())

			Eventually(func(g Gomega) {
				gotNR, err := leafNoExecTimeAdded("node.kubernetes.io/not-ready")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(gotNR).To(BeEmpty())
				gotUR, err := leafNoExecTimeAdded("node.kubernetes.io/unreachable")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(gotUR).To(BeEmpty())
			}).Should(Succeed())

			tFresh := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)
			patchNodeReadyCond("False", "KubeletNotReady", tFresh)
			patchNodeNoExecTaint("node.kubernetes.io/not-ready", tFresh)

			Eventually(func(g Gomega) {
				patchNodeReadyCond("False", "KubeletNotReady", tFresh)
				got, err := leafNoExecTimeAdded("node.kubernetes.io/not-ready")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(got).To(Equal(tFresh))
			}).Should(Succeed())
		})
	})
})
