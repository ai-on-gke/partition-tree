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

package controller

import (
	"context"
	"net/http"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
)

// testEnv holds the shared envtest environment used across all reconciler
// tests in this package. It is initialized lazily on first use and torn
// down via t.Cleanup on the bootstrapping test.
type testEnv struct {
	env           *envtest.Environment
	cfg           *rest.Config
	client        client.Client
	statusPatches atomic.Int64
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// newTestEnv starts a fresh envtest apiserver with the project CRDs
// installed. Each test that needs a live apiserver calls this and gets
// its own isolated instance — cheap because envtest boots a throwaway
// etcd + apiserver per test, and tests are short.
func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	logf.SetLogger(zap.New(zap.UseDevMode(true), zap.WriteTo(testingWriter{t})))

	_, thisFile, _, _ := runtime.Caller(0)
	crdPath := filepath.Join(filepath.Dir(thisFile), "..", "..", "config", "crd", "bases")

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{crdPath},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("envtest start: %v", err)
	}
	t.Cleanup(func() {
		_ = env.Stop()
	})

	// Register our CRD types on the test scheme so the client can
	// encode/decode PartitionTree objects.
	if err := partitionv1beta1.AddToScheme(scheme.Scheme); err != nil {
		t.Fatalf("adding to scheme: %v", err)
	}

	te := &testEnv{env: env, cfg: cfg}
	wrappedCfg := rest.CopyConfig(cfg)
	wrappedCfg.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPatch && strings.HasSuffix(req.URL.Path, "/status") {
				te.statusPatches.Add(1)
			}
			return rt.RoundTrip(req)
		})
	})

	c, err := client.New(wrappedCfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		t.Fatalf("building client: %v", err)
	}
	te.client = c
	return te
}

// testingWriter adapts a *testing.T into an io.Writer so the zap logger
// output appears next to the failing test instead of polluting stderr.
type testingWriter struct{ t *testing.T }

func (w testingWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}

// waitFor polls cond until it returns true or the timeout elapses.
// Envtest tests are inherently asynchronous — the reconciler reads status
// back via the apiserver — so a short wait loop is the path of least
// resistance compared to plumbing synchronous result channels through
// every scenario.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("waitFor: condition not met within %s", timeout)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// Ensure the CRD group-version is registered on package init so any
// scheme lookup against it (from scheme.Scheme) succeeds.
func init() {
	_ = schema.GroupVersion{}
}

// ctxBG is a convenience for tests that don't care about cancellation.
func ctxBG() context.Context { return context.Background() }
