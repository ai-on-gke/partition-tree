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
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
	"github.com/ai-on-gke/partition-tree/test/utils"
)

var _ = Describe("PartitionTree Node Churn (24 Pods / 3,456 MIGs)", Ordered, func() {
	const (
		numPods        = 24
		migsPerPod     = 144
		totalMIGs      = numPods * migsPerPod // 3,456 MIGs (24 TPU7x Pods)
		churnWaveMIGs  = 144                  // 1 full TPU7x Pod (144 MIGs) per rapid NotReady->Ready wave
		numCPUNodes    = 50
		workerCount    = 64
		churnPoolPref  = "churn-pool-"
		churnNodePref  = "churn-tpu7x-node-"
		cpuPoolName    = "churn-cpu-pool"
		cpuNodePrefix  = "churn-cpu-node-"
		churnMIGLink   = "projects/1234/zones/us-central1-a/instanceGroupManagers/tpu7x-churn-mig"
		repairTaintKey = "cloud.google.com/tpu-repair"
	)

	var (
		clientset *kubernetes.Clientset
		k8sClient client.Client
		largeBlob = strings.Repeat("label-bloat-payload-", 100) // 2 KB annotation bloat
	)

	poolName := func(idx int) string {
		return fmt.Sprintf("%s%04d", churnPoolPref, idx)
	}
	nodeName := func(idx int) string {
		return fmt.Sprintf("%s%04d", churnNodePref, idx)
	}

	buildFatE2ENode := func(name, pool, podBlock, topologyMode string) *corev1.Node {
		labels := map[string]string{
			"cloud.google.com/gke-nodepool":               pool,
			"cloud.google.com/gke-tpu-accelerator":        "tpu7x",
			"cloud.google.com/gce-topology-block":         podBlock,
			"cloud.google.com/gke-tpu-partition-4x4x4-id": "p-4x4x4",
			"cloud.google.com/gke-tpu-partition-2x4x4-id": "p-2x4x4",
			"cloud.google.com/gke-tpu-partition-2x2x4-id": "p-2x2x4",
			"cloud.google.com/gke-tpu-partition-2x2x2-id": "p-2x2x2",
			"cloud.google.com/gke-tpu-partition-2x2x1-id": "p-2x2x1",
			"kubernetes.io/os":                            "linux",
			"kubernetes.io/arch":                          "amd64",
			"node.kubernetes.io/instance-type":            "tpu7x-standard-4t",
		}
		if topologyMode != "" {
			labels["cloud.google.com/gke-accelerator-topology-mode"] = topologyMode
		}
		for i := range 20 {
			labels[fmt.Sprintf("cloud.google.com/extra-gke-label-%02d", i)] = "extra-metadata-value"
		}
		return &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   name,
				Labels: labels,
				Annotations: map[string]string{
					"node.gke.io/created-by-mig":           churnMIGLink,
					"node.gke.io/last-applied-node-labels": largeBlob,
					"csi.volume.kubernetes.io/nodeid":      fmt.Sprintf(`{"pd.csi.storage.gke.io":"projects/1234/zones/us-central1-a/instances/%s"}`, name),
				},
			},
			Spec: corev1.NodeSpec{
				PodCIDR:    "10.244.0.0/24",
				ProviderID: fmt.Sprintf("gce://project-1234/us-central1-a/%s", name),
			},
		}
	}

	buildFatNodeStatus := func(readyStatus corev1.ConditionStatus, wave int) corev1.NodeStatus {
		now := metav1.Now()
		return corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("180"),
				corev1.ResourceMemory: resource.MustParse("1400Gi"),
			},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("178"),
				corev1.ResourceMemory: resource.MustParse("1350Gi"),
			},
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse, LastHeartbeatTime: now, Reason: "KubeletHasSufficientMemory", Message: fmt.Sprintf("wave-%d memory ok", wave)},
				{Type: corev1.NodeDiskPressure, Status: corev1.ConditionFalse, LastHeartbeatTime: now, Reason: "KubeletHasNoDiskPressure", Message: fmt.Sprintf("wave-%d disk ok", wave)},
				{Type: corev1.NodePIDPressure, Status: corev1.ConditionFalse, LastHeartbeatTime: now, Reason: "KubeletHasSufficientPID", Message: fmt.Sprintf("wave-%d pid ok", wave)},
				{Type: "KernelDeadlock", Status: corev1.ConditionFalse, LastHeartbeatTime: now, Reason: "KernelHasNoDeadlock", Message: fmt.Sprintf("wave-%d kernel ok", wave)},
				{Type: corev1.NodeReady, Status: readyStatus, LastHeartbeatTime: now, LastTransitionTime: now, Reason: "KubeletReady", Message: fmt.Sprintf("wave-%d ready=%s", wave, readyStatus)},
			},
			Images: []corev1.ContainerImage{
				{Names: []string{"gke.gcr.io/pause:3.9", fmt.Sprintf("gke.gcr.io/tpu-plugin:wave-%d", wave)}, SizeBytes: 45000000},
			},
		}
	}

	parallelDo := func(count int, fn func(idx int) error) error {
		jobs := make(chan int, count)
		for i := range count {
			jobs <- i
		}
		close(jobs)

		var firstErr error
		var errOnce sync.Once
		var wg sync.WaitGroup
		workers := min(workerCount, count)
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for idx := range jobs {
					if err := fn(idx); err != nil {
						errOnce.Do(func() { firstErr = err })
						return
					}
				}
			}()
		}
		wg.Wait()
		return firstErr
	}

	createOrUpdateFatNode := func(ctx context.Context, name, pool, podBlock, topologyMode string, readyStatus corev1.ConditionStatus, wave int) error {
		n := buildFatE2ENode(name, pool, podBlock, topologyMode)
		created, err := clientset.CoreV1().Nodes().Create(ctx, n, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			existing, getErr := clientset.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
			if getErr != nil {
				return getErr
			}
			n.ResourceVersion = existing.ResourceVersion
			created, err = clientset.CoreV1().Nodes().Update(ctx, n, metav1.UpdateOptions{})
		}
		if err != nil {
			return fmt.Errorf("create/update node %s: %w", name, err)
		}
		created.Status = buildFatNodeStatus(readyStatus, wave)
		if _, err := clientset.CoreV1().Nodes().UpdateStatus(ctx, created, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("update node status %s: %w", name, err)
		}
		return nil
	}

	churnNodeWave := func(ctx context.Context, idx int, readyStatus corev1.ConditionStatus, withRepairTaint bool, wave int) error {
		name := nodeName(idx)
		for attempt := range 5 {
			cur, err := clientset.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if withRepairTaint {
				cur.Spec.Taints = []corev1.Taint{{
					Key:    repairTaintKey,
					Value:  fmt.Sprintf("wave-%d", wave),
					Effect: corev1.TaintEffectNoSchedule,
				}}
			} else {
				cur.Spec.Taints = nil
			}
			updated, err := clientset.CoreV1().Nodes().Update(ctx, cur, metav1.UpdateOptions{})
			if apierrors.IsConflict(err) && attempt < 4 {
				continue
			}
			if err != nil {
				return fmt.Errorf("update node taints %s: %w", name, err)
			}
			updated.Status = buildFatNodeStatus(readyStatus, wave)
			if _, err := clientset.CoreV1().Nodes().UpdateStatus(ctx, updated, metav1.UpdateOptions{}); err != nil {
				if apierrors.IsConflict(err) && attempt < 4 {
					continue
				}
				return fmt.Errorf("update node status %s: %w", name, err)
			}
			return nil
		}
		return fmt.Errorf("conflict retries exhausted for %s", name)
	}

	BeforeAll(func() {
		By("waiting for the controller-manager to be ready")
		Eventually(controllerReady, 3*time.Minute, time.Second).Should(Succeed())

		restCfg := ctrl.GetConfigOrDie()
		restCfg.QPS = 2000
		restCfg.Burst = 4000

		var err error
		clientset, err = kubernetes.NewForConfig(restCfg)
		Expect(err).NotTo(HaveOccurred())

		_ = partitionv1beta1.AddToScheme(scheme.Scheme)
		k8sClient, err = client.New(restCfg, client.Options{Scheme: scheme.Scheme})
		Expect(err).NotTo(HaveOccurred())

		ctx := context.Background()

		By(fmt.Sprintf("applying %d fat non-PROVISION_ONLY nodes (DISABLED + absent topology-mode) that must be filtered out by ByObject[Node].Label", numCPUNodes))
		Expect(parallelDo(numCPUNodes, func(i int) error {
			cpuNode := fmt.Sprintf("%s%02d", cpuNodePrefix, i)
			mode := "DISABLED"
			if i%2 == 0 {
				mode = ""
			}
			return createOrUpdateFatNode(ctx, cpuNode, cpuPoolName, "cpu-block", mode, corev1.ConditionTrue, 0)
		})).To(Succeed())

		By(fmt.Sprintf("creating 24 TPU7x Pods (%d MIGs = 24 x 144 MIGs) of fat PROVISION_ONLY TPU7x Nodes concurrently", totalMIGs))
		start := time.Now()
		Expect(parallelDo(totalMIGs, func(i int) error {
			podBlock := fmt.Sprintf("block-%02d", i/migsPerPod)
			return createOrUpdateFatNode(ctx, nodeName(i), poolName(i), podBlock, "PROVISION_ONLY", corev1.ConditionTrue, 0)
		})).To(Succeed())
		_, _ = fmt.Fprintf(GinkgoWriter, "Created %d PROVISION_ONLY TPU7x Nodes (24 Pods) in %v\n", totalMIGs, time.Since(start))
	})

	AfterAll(func() {
		ctx := context.Background()
		By(fmt.Sprintf("cleaning up %d TPU7x Nodes, %d CPU Nodes, and %d PartitionTree CRs", totalMIGs, numCPUNodes, totalMIGs))
		_ = parallelDo(totalMIGs, func(i int) error {
			_ = clientset.CoreV1().Nodes().Delete(ctx, nodeName(i), metav1.DeleteOptions{})
			return nil
		})
		_ = parallelDo(numCPUNodes, func(i int) error {
			_ = clientset.CoreV1().Nodes().Delete(ctx, fmt.Sprintf("%s%02d", cpuNodePrefix, i), metav1.DeleteOptions{})
			return nil
		})
		// Strip finalizers and delete all 3,456 PartitionTree CRs concurrently so
		// AfterSuite CRD teardown does not block after the controller deployment is removed.
		_ = parallelDo(totalMIGs, func(i int) error {
			cr := &partitionv1beta1.PartitionTree{ObjectMeta: metav1.ObjectMeta{Name: poolName(i)}}
			_ = k8sClient.Patch(ctx, cr, client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`)))
			_ = k8sClient.Delete(ctx, cr)
			return nil
		})
	})

	SetDefaultEventuallyTimeout(3 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	It("materializes all 3,456 PartitionTree CRs (24 TPU7x Pods) with Ready=True while ignoring non-PROVISION_ONLY nodes", func() {
		ctx := context.Background()
		Eventually(func(g Gomega) {
			var crList partitionv1beta1.PartitionTreeList
			g.Expect(k8sClient.List(ctx, &crList)).To(Succeed())

			readyCRs := 0
			for i := range crList.Items {
				cr := &crList.Items[i]
				if !strings.HasPrefix(cr.Name, churnPoolPref) {
					continue
				}
				if cr.Status.Root != nil && cr.Status.Root.PartitionID == "p-4x4x4" && cr.Status.AcceleratorType == "tpu7x" {
					readyCRs++
				}
			}
			_, _ = fmt.Fprintf(GinkgoWriter, "24-Pod Materialization Progress: %d / %d PartitionTree CRs populated\n", readyCRs, totalMIGs)
			g.Expect(readyCRs).To(Equal(totalMIGs))
		}, 3*time.Minute, 2*time.Second).Should(Succeed())

		// Verify non-PROVISION_ONLY pool CR was never created.
		var cpuCR partitionv1beta1.PartitionTree
		err := k8sClient.Get(ctx, client.ObjectKey{Name: cpuPoolName}, &cpuCR)
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "non-PROVISION_ONLY pool should never materialize a PartitionTree CR")
	})

	It("survives large multi-MIG waves of NotReady -> Ready, NPD condition, taint, and delete/recreate churn at 24-Pod scale with 0 restarts", func() {
		ctx := context.Background()

		findLeaf := func(p *partitionv1beta1.PartitionInfo, targetID string) *partitionv1beta1.PartitionInfo {
			var walk func(n *partitionv1beta1.PartitionInfo) *partitionv1beta1.PartitionInfo
			walk = func(n *partitionv1beta1.PartitionInfo) *partitionv1beta1.PartitionInfo {
				if n == nil {
					return nil
				}
				if n.PartitionID == targetID {
					return n
				}
				for i := range n.ChildPartitions {
					if found := walk(&n.ChildPartitions[i]); found != nil {
						return found
					}
				}
				return nil
			}
			return walk(p)
		}

		leafConditionStatus := func(leaf *partitionv1beta1.PartitionInfo, condType string) metav1.ConditionStatus {
			if leaf == nil {
				return ""
			}
			for _, c := range leaf.Conditions {
				if c.Type == condType {
					return c.Status
				}
			}
			return ""
		}

		leafHasTaintKey := func(leaf *partitionv1beta1.PartitionInfo, key string) bool {
			if leaf == nil {
				return false
			}
			for _, t := range leaf.Taints {
				if t.Key == key {
					return true
				}
			}
			return false
		}

		for wave := 1; wave <= 2; wave++ {
			By(fmt.Sprintf("wave %d: flipping %d MIGs (1 full TPU7x Pod out of 24 live Pods) to Ready=False (NotReady) + repair taint + NPD condition bloat", wave, churnWaveMIGs))
			var updatedCount atomic.Int32
			Expect(parallelDo(churnWaveMIGs, func(i int) error {
				err := churnNodeWave(ctx, i, corev1.ConditionFalse, true, wave)
				if err == nil {
					updatedCount.Add(1)
				}
				return err
			})).To(Succeed())
			Expect(int(updatedCount.Load())).To(Equal(churnWaveMIGs))

			Eventually(func(g Gomega) {
				notReadyConverged := 0
				for i := range churnWaveMIGs {
					var cr partitionv1beta1.PartitionTree
					if err := k8sClient.Get(ctx, client.ObjectKey{Name: poolName(i)}, &cr); err != nil {
						continue
					}
					leaf := findLeaf(cr.Status.Root, "p-2x2x1")
					if leafConditionStatus(leaf, "NodeReady") == metav1.ConditionFalse && leafHasTaintKey(leaf, repairTaintKey) {
						notReadyConverged++
					}
				}
				_, _ = fmt.Fprintf(GinkgoWriter, "Wave %d NotReady+Taint convergence: %d / %d MIG CRs\n", wave, notReadyConverged, churnWaveMIGs)
				g.Expect(notReadyConverged).To(Equal(churnWaveMIGs))
			}, 2*time.Minute, 2*time.Second).Should(Succeed())

			By(fmt.Sprintf("wave %d: flipping %d MIGs back to Ready=True and clearing repair taint", wave, churnWaveMIGs))
			Expect(parallelDo(churnWaveMIGs, func(i int) error {
				return churnNodeWave(ctx, i, corev1.ConditionTrue, false, wave+10)
			})).To(Succeed())

			Eventually(func(g Gomega) {
				readyConverged := 0
				for i := range churnWaveMIGs {
					var cr partitionv1beta1.PartitionTree
					if err := k8sClient.Get(ctx, client.ObjectKey{Name: poolName(i)}, &cr); err != nil {
						continue
					}
					leaf := findLeaf(cr.Status.Root, "p-2x2x1")
					if leafConditionStatus(leaf, "NodeReady") == metav1.ConditionTrue && !leafHasTaintKey(leaf, repairTaintKey) {
						readyConverged++
					}
				}
				_, _ = fmt.Fprintf(GinkgoWriter, "Wave %d Ready recovery convergence: %d / %d MIG CRs\n", wave, readyConverged, churnWaveMIGs)
				g.Expect(readyConverged).To(Equal(churnWaveMIGs))
			}, 2*time.Minute, 2*time.Second).Should(Succeed())
		}

		By("deleting and recreating churn-tpu7x-node-0000 while all 3,456 MIGs (24 Pods) remain active")
		Expect(clientset.CoreV1().Nodes().Delete(ctx, nodeName(0), metav1.DeleteOptions{})).To(Succeed())
		Expect(createOrUpdateFatNode(ctx, nodeName(0), poolName(0), "block-00", "PROVISION_ONLY", corev1.ConditionTrue, 99)).To(Succeed())

		Eventually(func(g Gomega) {
			var cr partitionv1beta1.PartitionTree
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: poolName(0)}, &cr)).To(Succeed())
			leaf := findLeaf(cr.Status.Root, "p-2x2x1")
			g.Expect(leafConditionStatus(leaf, "NodeReady")).To(Equal(metav1.ConditionTrue))
		}, time.Minute, time.Second).Should(Succeed())

		By("verifying controller-manager pod survived 24-Pod (3,456 MIGs) + large churn waves with 0 restarts")
		out, err := utils.Run(exec.Command("kubectl", "get", "pods",
			"-n", namespace,
			"-l", "control-plane=controller-manager",
			"-o", "jsonpath={.items[0].status.containerStatuses[?(@.name=='manager')].restartCount}"))
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(Equal("0"))
	})
})
