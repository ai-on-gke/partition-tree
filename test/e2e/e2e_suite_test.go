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
	"fmt"
	"os"
	"os/exec"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ai-on-gke/partition-tree/test/utils"
)

var (
	// managerImage is the manager image to be built and loaded for testing.
	managerImage = "example.com/partitiontree:v0.0.1"
	// shouldCleanupCertManager tracks whether CertManager was installed by this suite.
	shouldCleanupCertManager = false
)

// TestE2E runs the e2e test suite to validate the solution in an isolated environment.
// The default setup requires Kind and CertManager.
//
// To skip CertManager installation, set: CERT_MANAGER_INSTALL_SKIP=true
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting partitiontree e2e test suite\n")
	RunSpecs(t, "e2e suite")
}

var _ = BeforeSuite(func() {
	By("building the manager image")
	cmd := exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", managerImage))
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the manager image")

	// TODO(user): If you want to change the e2e test vendor from Kind,
	// ensure the image is built and available, then remove the following block.
	By("loading the manager image on Kind")
	err = utils.LoadImageToKindClusterWithName(managerImage)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the manager image into Kind")

	setupCertManager()

	// Deploy the e2e overlay (controller + CRD + GCE mock fixtures)
	// once for the whole suite. Multiple Describe blocks share this
	// deployment — Ginkgo's Ordered only orders within a Describe, not
	// across siblings, so per-Describe BeforeAll/AfterAll would tear
	// down the controller before later Describes ran.
	By("deploying the e2e overlay (controller + CRD + GCE mock fixtures)")
	Expect(deployE2EOverlay(managerImage)).To(Succeed(),
		"Failed to deploy the e2e overlay")

	By("labeling the namespace to enforce the restricted security policy")
	cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
		"pod-security.kubernetes.io/enforce=restricted")
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")
})

var _ = AfterSuite(func() {
	// Best-effort diagnostic dump: if anything failed, pull the
	// controller-manager pod logs so the CI log shows exactly what the
	// controller did. Cheap no-op on green runs.
	if CurrentSpecReport().Failed() {
		By("dumping controller-manager logs (some spec failed)")
		dumpControllerLogs()
	}

	By("deleting the e2e overlay")
	_ = teardownE2EOverlay()

	By("removing manager namespace")
	cmd := exec.Command("kubectl", "delete", "ns", namespace, "--ignore-not-found=true")
	_, _ = utils.Run(cmd)

	teardownCertManager()
})

// dumpControllerLogs streams the current controller-manager pod logs to
// GinkgoWriter so a CI failure includes them without having to re-run
// kubectl logs by hand.
func dumpControllerLogs() {
	out, err := utils.Run(exec.Command("kubectl", "-n", namespace,
		"get", "pods", "-l", "control-plane=controller-manager",
		"-o", "jsonpath={.items[0].metadata.name}"))
	if err != nil || out == "" {
		_, _ = fmt.Fprintf(GinkgoWriter, "dumpControllerLogs: no controller pod: %v\n", err)
		return
	}
	logs, err := utils.Run(exec.Command("kubectl", "-n", namespace,
		"logs", out, "--tail=500"))
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "dumpControllerLogs: kubectl logs: %v\n", err)
		return
	}
	_, _ = fmt.Fprintf(GinkgoWriter, "=== controller-manager logs (%s) ===\n%s\n=== end logs ===\n",
		out, logs)
}

// setupCertManager installs CertManager if needed for webhook tests.
// Skips installation if CERT_MANAGER_INSTALL_SKIP=true or if already present.
func setupCertManager() {
	if os.Getenv("CERT_MANAGER_INSTALL_SKIP") == "true" {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping CertManager installation (CERT_MANAGER_INSTALL_SKIP=true)\n")
		return
	}

	By("checking if CertManager is already installed")
	if utils.IsCertManagerCRDsInstalled() {
		_, _ = fmt.Fprintf(GinkgoWriter, "CertManager is already installed. Skipping installation.\n")
		return
	}

	// Mark for cleanup before installation to handle interruptions and partial installs.
	shouldCleanupCertManager = true

	By("installing CertManager")
	Expect(utils.InstallCertManager()).To(Succeed(), "Failed to install CertManager")
}

// teardownCertManager uninstalls CertManager if it was installed by setupCertManager.
// This ensures we only remove what we installed.
func teardownCertManager() {
	if !shouldCleanupCertManager {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping CertManager cleanup (not installed by this suite)\n")
		return
	}

	By("uninstalling CertManager")
	utils.UninstallCertManager()
}
