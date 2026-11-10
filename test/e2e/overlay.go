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
	"path/filepath"
)

// e2eOverlayPath is the kustomize directory the e2e suite deploys.
// Resolved relative to the repo root, which is two levels above this
// file at run time.
const e2eOverlayPath = "test/e2e/config"

// originalKustomization stores the content of config/manager/kustomization.yaml
// so it can be restored after the test to prevent dirtying the git tree.
var originalKustomization []byte

// repoRoot returns the absolute path to the repo root by walking up
// from the current working directory until it finds a go.mod file.
// Walking (rather than a fixed "../..") is required because
// test/utils.Run does an os.Chdir to the project root before every
// shell-out, so by the time the overlay helpers run the cwd is
// already the repo root rather than test/e2e.
func repoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("repoRoot: no go.mod found walking up from %s", wd)
		}
		dir = parent
	}
}

// kustomizeBin locates the bin/kustomize built by the Makefile. The
// e2e specs deliberately use the pinned version checked in by the
// project rather than whatever is on $PATH.
func kustomizeBin() (string, error) {
	root, err := repoRoot()
	if err != nil {
		return "", err
	}
	bin := filepath.Join(root, "bin", "kustomize")
	if _, err := os.Stat(bin); err != nil {
		return "", fmt.Errorf("bin/kustomize not found at %s — run `make kustomize` first: %w", bin, err)
	}
	return bin, nil
}

// deployE2EOverlay runs `kustomize build test/e2e/config | kubectl apply -f -`
// after stamping the manager image into config/manager via `kustomize edit
// set image`. The same image-stamping pattern as the upstream `make deploy`.
func deployE2EOverlay(image string) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	kustomize, err := kustomizeBin()
	if err != nil {
		return err
	}

	// Backup the original kustomization.yaml so we don't dirty the git tree
	kustPath := filepath.Join(root, "config", "manager", "kustomization.yaml")
	originalKustomization, _ = os.ReadFile(kustPath)

	// Stamp the image into config/manager — `make deploy` does this
	// inline; we have to mirror it because we're invoking kustomize
	// directly.
	editCmd := exec.Command(kustomize, "edit", "set", "image",
		fmt.Sprintf("controller=%s", image))
	editCmd.Dir = filepath.Join(root, "config", "manager")
	if out, err := editCmd.CombinedOutput(); err != nil {
		return fmt.Errorf("kustomize edit set image: %w (output: %s)", err, out)
	}

	manifest, err := buildOverlay()
	if err != nil {
		return err
	}
	apply := exec.Command("kubectl", "apply", "-f", "-")
	apply.Stdin = bytes.NewReader(manifest)
	if out, err := apply.CombinedOutput(); err != nil {
		return fmt.Errorf("kubectl apply overlay: %w (output: %s)", err, out)
	}
	return nil
}

// teardownE2EOverlay runs `kustomize build test/e2e/config | kubectl
// delete --ignore-not-found -f -`. Errors are returned but the caller
// usually ignores them — best-effort cleanup.
func teardownE2EOverlay() error {
	manifest, err := buildOverlay()
	if err != nil {
		return err
	}
	del := exec.Command("kubectl", "delete", "--ignore-not-found=true", "-f", "-")
	del.Stdin = bytes.NewReader(manifest)
	if out, err := del.CombinedOutput(); err != nil {
		return fmt.Errorf("kubectl delete overlay: %w (output: %s)", err, out)
	}

	// Restore the original kustomization.yaml
	if len(originalKustomization) > 0 {
		root, _ := repoRoot()
		kustPath := filepath.Join(root, "config", "manager", "kustomization.yaml")
		_ = os.WriteFile(kustPath, originalKustomization, 0644)
	}

	return nil
}

// buildOverlay shells out to `kustomize build` and returns the rendered
// YAML manifest as bytes.
func buildOverlay() ([]byte, error) {
	root, err := repoRoot()
	if err != nil {
		return nil, err
	}
	kustomize, err := kustomizeBin()
	if err != nil {
		return nil, err
	}
	build := exec.Command(kustomize, "build", filepath.Join(root, e2eOverlayPath))
	out, err := build.Output()
	if err != nil {
		stderr := ""
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = string(exitErr.Stderr)
		}
		return nil, fmt.Errorf("kustomize build %s: %w (stderr: %s)", e2eOverlayPath, err, stderr)
	}
	return out, nil
}
