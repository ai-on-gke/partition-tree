//go:build e2e
// +build e2e

/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package e2e

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ai-on-gke/partition-tree/test/utils"
)

// This spec restarts the shared controller with
// --enable-tpu-allocatable-readiness for its own duration and restores
// the original Deployment args afterwards, so every other Describe keeps
// running flag-off.
var _ = Describe("PartitionTree TPU allocatable readiness", Ordered, func() {
	const (
		tpuAllocFlag       = "--enable-tpu-allocatable-readiness"
		tpuAllocDeployment = "partition-controller-manager"
		// tpuAllocLeaseName is the LeaderElectionID set in cmd/main.go.
		tpuAllocLeaseName = "446eed18.tpu.google.com"
		tpuAllocPool      = "tpu-alloc-pool"
		tpuAllocNodeName  = "tpu-alloc-node-0"
		nodesNotFoundKey  = "cloud.google.com/nodes-not-found"
	)

	// leafNodeReady returns the leaf's NodeReady status, reason and
	// message. The healthy-mig fixture has a single leaf named "leaf".
	leafNodeReady := func(g Gomega) (string, string, string) {
		out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", tpuAllocPool,
			"-o", "jsonpath="+
				"{.status.root.childPartitions[?(@.partitionId=='leaf')].conditions[?(@.type=='NodeReady')].status}|"+
				"{.status.root.childPartitions[?(@.partitionId=='leaf')].conditions[?(@.type=='NodeReady')].reason}|"+
				"{.status.root.childPartitions[?(@.partitionId=='leaf')].conditions[?(@.type=='NodeReady')].message}"))
		g.Expect(err).NotTo(HaveOccurred())
		parts := strings.SplitN(out, "|", 3)
		g.Expect(parts).To(HaveLen(3), "unexpected jsonpath output %q", out)
		return parts[0], parts[1], parts[2]
	}

	// leafTaintKeys returns the space-separated taint keys on the leaf.
	leafTaintKeys := func(g Gomega) string {
		out, err := utils.Run(exec.Command("kubectl", "get", "partitiontree", tpuAllocPool,
			"-o", "jsonpath={.status.root.childPartitions[?(@.partitionId=='leaf')].taints[*].key}"))
		g.Expect(err).NotTo(HaveOccurred())
		return out
	}

	// nodesNotFoundTaintsOn reports whether args enable the NodesNotFound
	// taint, following TaintConfig.Set: the last --enable-taints wins; bare,
	// "true" or "All" enable everything; otherwise a comma-separated list.
	nodesNotFoundTaintsOn := func(args []string) bool {
		on := false
		for _, a := range args {
			name, val, hasVal := strings.Cut(strings.TrimLeft(a, "-"), "=")
			if name != "enable-taints" {
				continue
			}
			val = strings.TrimSpace(val)
			if !hasVal || val == "" || strings.EqualFold(val, "true") {
				on = true
				continue
			}
			on = false
			for _, opt := range strings.Split(val, ",") {
				opt = strings.TrimSpace(opt)
				if strings.EqualFold(opt, "All") || strings.EqualFold(opt, "NodesNotFound") {
					on = true
				}
			}
		}
		return on
	}

	// managerArgs returns the index of the "manager" container in the
	// controller Deployment and its current args.
	managerArgs := func() (int, []string) {
		out, err := utils.Run(exec.Command("kubectl", "get", "deployment", tpuAllocDeployment,
			"-n", namespace, "-o", "jsonpath={.spec.template.spec.containers[*].name}"))
		Expect(err).NotTo(HaveOccurred())
		idx := slices.Index(strings.Fields(out), "manager")
		Expect(idx).To(BeNumerically(">=", 0), "no manager container in %q", out)

		out, err = utils.Run(exec.Command("kubectl", "get", "deployment", tpuAllocDeployment,
			"-n", namespace, "-o", fmt.Sprintf("jsonpath={.spec.template.spec.containers[%d].args}", idx)))
		Expect(err).NotTo(HaveOccurred())
		var args []string
		if strings.TrimSpace(out) != "" {
			Expect(json.Unmarshal([]byte(out), &args)).To(Succeed(), "parsing manager args %q", out)
		}
		return idx, args
	}

	// setManagerArgs replaces the manager container args and waits for
	// the rollout. JSON-patch "add" replaces an existing member.
	setManagerArgs := func(idx int, args []string) {
		value, err := json.Marshal(args)
		Expect(err).NotTo(HaveOccurred())
		patch := fmt.Sprintf(`[{"op": "add", "path": "/spec/template/spec/containers/%d/args", "value": %s}]`, idx, value)
		_, err = utils.Run(exec.Command("kubectl", "patch", "deployment", tpuAllocDeployment,
			"-n", namespace, "--type=json", "-p", patch))
		Expect(err).NotTo(HaveOccurred())

		_, err = utils.Run(exec.Command("kubectl", "rollout", "status", "deployment/"+tpuAllocDeployment,
			"-n", namespace, "--timeout=3m"))
		Expect(err).NotTo(HaveOccurred())
	}

	// controllerLeaderWithFlag asserts there is exactly one live
	// controller pod, it is Ready, its args carry (or omit) the flag, and
	// it holds the leader lease. controllerReady alone can look at a pod
	// that is still terminating right after a rollout.
	controllerLeaderWithFlag := func(wantFlag bool) func(g Gomega) {
		return func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "pods",
				"-n", namespace, "-l", "control-plane=controller-manager",
				"-o", "go-template={{ range .items }}"+
					"{{ if not .metadata.deletionTimestamp }}"+
					"{{ .metadata.name }}"+
					"{{ \"\\n\" }}{{ end }}{{ end }}"))
			g.Expect(err).NotTo(HaveOccurred())
			pods := utils.GetNonEmptyLines(out)
			g.Expect(pods).To(HaveLen(1), "expected 1 live controller pod")
			pod := pods[0]

			out, err = utils.Run(exec.Command("kubectl", "get", "pod", pod, "-n", namespace,
				"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"), "controller pod %s not Ready", pod)

			out, err = utils.Run(exec.Command("kubectl", "get", "pod", pod, "-n", namespace,
				"-o", "jsonpath={.spec.containers[?(@.name=='manager')].args}"))
			g.Expect(err).NotTo(HaveOccurred())
			var args []string
			g.Expect(json.Unmarshal([]byte(out), &args)).To(Succeed())
			g.Expect(slices.Contains(args, tpuAllocFlag)).To(Equal(wantFlag),
				"controller pod %s args %v", pod, args)

			// holderIdentity is "<hostname>_<uuid>"; the hostname is the pod name.
			out, err = utils.Run(exec.Command("kubectl", "get", "lease", tpuAllocLeaseName,
				"-n", namespace, "-o", "jsonpath={.spec.holderIdentity}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(HavePrefix(pod+"_"), "controller pod %s does not hold the leader lease", pod)
		}
	}

	// patchTPUNodeStatus sets the node's Ready condition and its
	// google.com/tpu capacity and allocatable to tpu ("" removes both
	// keys, mirroring a node whose device plugin has not registered).
	// Capacity follows allocatable because after a reboot the kubelet
	// publishes both as 0; the controller must not depend on capacity.
	// Every patch carries a fresh lastHeartbeatTime so the node lifecycle
	// controller does not turn this kubelet-less Node Unknown. The patch
	// is idempotent, so each Eventually re-sends it on every poll to keep
	// the heartbeat fresh however long the step waits.
	patchTPUNodeStatus := func(g Gomega, ready bool, tpu string) {
		now := time.Now().UTC().Format(time.RFC3339)
		readyCond := map[string]any{
			"type":               "Ready",
			"status":             "True",
			"lastHeartbeatTime":  now,
			"lastTransitionTime": now,
			"reason":             "KubeletReady",
			"message":            "e2e",
		}
		if !ready {
			readyCond["status"] = "False"
			readyCond["reason"] = "KubeletNotReady"
			readyCond["message"] = "e2e-not-ready"
		}
		var quantity any // nil marshals to null, which removes the key
		if tpu != "" {
			quantity = tpu
		}
		patch, err := json.Marshal(map[string]any{
			"status": map[string]any{
				"conditions":  []any{readyCond},
				"capacity":    map[string]any{"google.com/tpu": quantity},
				"allocatable": map[string]any{"google.com/tpu": quantity},
			},
		})
		g.Expect(err).NotTo(HaveOccurred())
		_, err = utils.Run(exec.Command("kubectl", "patch", "node", tpuAllocNodeName,
			"--subresource=status", "--type=merge", "-p", string(patch)))
		g.Expect(err).NotTo(HaveOccurred())
	}

	BeforeAll(func() {
		By("waiting for the controller-manager to be ready")
		Eventually(controllerReady, 3*time.Minute, time.Second).Should(Succeed())

		By("reading the current controller-manager args")
		idx, originalArgs := managerArgs()
		_, _ = fmt.Fprintf(GinkgoWriter, "original manager args: %v\n", originalArgs)

		// The no-nodes-not-found-taint check below is only meaningful
		// with NodesNotFound taints on. Another spec (taints_test.go) can
		// leave the Deployment without --enable-taints if it fails midway,
		// so turn it on here rather than rely on the starting args.
		flagOnArgs := slices.Clone(originalArgs)
		if !slices.Contains(flagOnArgs, tpuAllocFlag) {
			flagOnArgs = append(flagOnArgs, tpuAllocFlag)
		}
		if !nodesNotFoundTaintsOn(flagOnArgs) {
			// The last --enable-taints wins, so this overrides any earlier one.
			flagOnArgs = append(flagOnArgs, "--enable-taints=NodesNotFound")
		}

		if slices.Equal(flagOnArgs, originalArgs) {
			_, _ = fmt.Fprintf(GinkgoWriter, "%s and NodesNotFound taints already set; leaving the Deployment untouched\n", tpuAllocFlag)
		} else {
			// Registered before patching so the original args come back
			// even if the patch, the rollout or any later spec fails.
			DeferCleanup(func() {
				By("restoring the original controller-manager args")
				setManagerArgs(idx, originalArgs)
				Eventually(controllerLeaderWithFlag(slices.Contains(originalArgs, tpuAllocFlag)),
					3*time.Minute, time.Second).Should(Succeed())
			})

			By("setting controller-manager args to " + strings.Join(flagOnArgs, " "))
			setManagerArgs(idx, flagOnArgs)
		}

		By("waiting for a flag-on controller-manager pod to be Ready and leader")
		Eventually(controllerLeaderWithFlag(true), 3*time.Minute, time.Second).Should(Succeed())

		// Runs before the args restore above (LIFO), while the controller
		// is still up to process the CR finalizer.
		DeferCleanup(func() {
			By("deleting the e2e PartitionTree CR and Node")
			_, _ = utils.Run(exec.Command("kubectl", "delete", "partitiontree", tpuAllocPool,
				"--ignore-not-found=true", "--wait=false"))
			_, _ = utils.Run(exec.Command("kubectl", "delete", "node", tpuAllocNodeName,
				"--ignore-not-found=true", "--wait=false"))
		})

		By("applying a Ready tpu7x Node with google.com/tpu=4 allocatable")
		Expect(applyNode(tpuAllocNodeName, tpuAllocPool, healthyMIGLink, "tpu7x",
			map[string]string{
				"cloud.google.com/gke-tpu-partition-root-id": "root",
				"cloud.google.com/gke-tpu-partition-leaf-id": "leaf",
			})).To(Succeed())
		patchTPUNodeStatus(Default, true, "4")
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	It("reports NodeReady=True/NodesReady when every chip is allocatable (4/4)", func() {
		Eventually(func(g Gomega) {
			patchTPUNodeStatus(g, true, "4")
			status, reason, _ := leafNodeReady(g)
			g.Expect(status).To(Equal("True"))
			g.Expect(reason).To(Equal("NodesReady"))
		}).Should(Succeed())
	})

	It("flips NodeReady to False/TPUsNotAllocatable at 0/4 without the nodes-not-found taint", func() {
		By("patching google.com/tpu to 0 on a Ready node")
		Eventually(func(g Gomega) {
			patchTPUNodeStatus(g, true, "0")
			status, reason, message := leafNodeReady(g)
			g.Expect(status).To(Equal("False"))
			g.Expect(reason).To(Equal("TPUsNotAllocatable"))
			g.Expect(message).To(ContainSubstring(tpuAllocNodeName + " (0/4)"))
		}).Should(Succeed())

		By("verifying the leaf does not carry the " + nodesNotFoundKey + " taint")
		Consistently(func(g Gomega) {
			patchTPUNodeStatus(g, true, "0")
			g.Expect(leafTaintKeys(g)).NotTo(ContainSubstring(nodesNotFoundKey))
		}, 5*time.Second, time.Second).Should(Succeed())
	})

	It("returns NodeReady to True/NodesReady when allocatable is back to 4", func() {
		Eventually(func(g Gomega) {
			patchTPUNodeStatus(g, true, "4")
			status, reason, _ := leafNodeReady(g)
			g.Expect(status).To(Equal("True"))
			g.Expect(reason).To(Equal("NodesReady"))
		}).Should(Succeed())
	})

	It("treats an absent google.com/tpu as 0/4", func() {
		Eventually(func(g Gomega) {
			patchTPUNodeStatus(g, true, "")
			status, reason, message := leafNodeReady(g)
			g.Expect(status).To(Equal("False"))
			g.Expect(reason).To(Equal("TPUsNotAllocatable"))
			g.Expect(message).To(ContainSubstring(tpuAllocNodeName + " (0/4)"))
		}).Should(Succeed())
	})

	It("reports NodesNotReady over TPUsNotAllocatable when the node is also NotReady", func() {
		Eventually(func(g Gomega) {
			patchTPUNodeStatus(g, false, "0")
			status, reason, _ := leafNodeReady(g)
			g.Expect(status).To(Equal("False"))
			g.Expect(reason).To(Equal("NodesNotReady"))
		}).Should(Succeed())
	})
})
