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

package gce

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"sigs.k8s.io/yaml"
)

// MockClient is the shared in-memory fake used by unit, envtest, and
// e2e suites. It is safe for concurrent use and records the number of
// calls so tests can assert fast-path reconciles do not hit GCE.
//
// For e2e, the mock can be backed by a YAML fixture file: when
// fixturePath is set, every call re-reads the file before resolving the
// response, which lets the test pod swap fixtures by editing a mounted
// ConfigMap without restarting the controller.
type MockClient struct {
	mu          sync.Mutex
	responses   map[string]*TopologyResponse
	err         error
	calls       int
	fixturePath string
	fixtureMod  time.Time
}

// NewMockClient returns an empty MockClient.
func NewMockClient() *MockClient {
	return &MockClient{responses: map[string]*TopologyResponse{}}
}

// SetResponse registers a response for a given MIG. Keys match on either
// "<project>/<zone>/<mig>" or just "<mig>" — the lookup tries the fully
// qualified form first and falls back to bare MIG name.
func (m *MockClient) SetResponse(key string, resp *TopologyResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.responses[key] = resp
}

// SetError makes every subsequent call return the given error. Pass nil
// to clear.
func (m *MockClient) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

// Calls returns the number of calls observed so far.
func (m *MockClient) Calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

// LoadFromFile configures the mock to re-read the given YAML fixture
// file on every call. The file is parsed once eagerly so a malformed
// fixture surfaces at startup rather than on the first reconcile.
func (m *MockClient) LoadFromFile(path string) error {
	if _, err := loadFixtureFile(path); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fixturePath = path
	return nil
}

// GetAvailableAcceleratorTopologies implements Client.
func (m *MockClient) GetAvailableAcceleratorTopologies(
	_ context.Context,
	projectNumber, zone, mig string,
) (*TopologyResponse, error) {
	m.mu.Lock()
	m.calls++
	if m.err != nil {
		err := m.err
		m.mu.Unlock()
		return nil, err
	}
	fqn := fmt.Sprintf("%s/%s/%s", projectNumber, zone, mig)
	if m.fixturePath == "" {
		if r, ok := m.responses[fqn]; ok {
			m.mu.Unlock()
			return r, nil
		}
		if r, ok := m.responses[mig]; ok {
			m.mu.Unlock()
			return r, nil
		}
		m.mu.Unlock()
		return nil, fmt.Errorf("mock: no response registered for %s", fqn)
	}

	// Snapshot the path under the lock and release it before doing
	// disk I/O so concurrent reconciles don't serialize on a syscall.
	path := m.fixturePath
	lastMod := m.fixtureMod
	m.mu.Unlock()

	stat, err := os.Stat(path)
	// If it's a directory, stat.ModTime() changes when files are added/removed.
	// But it might not change when a file inside is modified. For mock fixtures
	// via configmaps, the symlinks change so the dir mtime usually changes.
	if err == nil && stat.ModTime().Equal(lastMod) {
		m.mu.Lock()
		defer m.mu.Unlock()
		if r, ok := m.responses[fqn]; ok {
			return r, nil
		}
		if r, ok := m.responses[mig]; ok {
			return r, nil
		}
		return nil, fmt.Errorf("mock: no fixture entry for %s in %s", fqn, path)
	}

	fixtures, err := loadFixtureFile(path)
	if err != nil {
		return nil, fmt.Errorf("mock: reload fixtures: %w", err)
	}

	m.mu.Lock()
	m.responses = fixtures
	if stat != nil {
		m.fixtureMod = stat.ModTime()
	}
	m.mu.Unlock()

	if r, ok := fixtures[fqn]; ok {
		return r, nil
	}
	if r, ok := fixtures[mig]; ok {
		return r, nil
	}
	return nil, fmt.Errorf("mock: no fixture entry for %s in %s", fqn, path)
}

// fixtureFile is the on-disk YAML schema for -gce-mock-fixtures. The
// outer list is keyed by either bare MIG name or "<project>/<zone>/<mig>";
// the lookup tries the FQN first.
type fixtureFile struct {
	Entries []fixtureEntry `json:"entries"`
}

type fixtureEntry struct {
	Key      string           `json:"key"`
	Response TopologyResponse `json:"response"`
}

func loadFixtureFile(path string) (map[string]*TopologyResponse, error) {
	out := make(map[string]*TopologyResponse)

	stat, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	var files []string
	if stat.IsDir() {
		if err := filepath.WalkDir(path, func(p string, d os.DirEntry, err error) error {
			if err != nil {
				return fmt.Errorf("walk fixture dir %s: %w", p, err)
			}
			if !d.IsDir() && (strings.HasSuffix(d.Name(), ".yaml") || strings.HasSuffix(d.Name(), ".yml")) {
				files = append(files, p)
			}
			return nil
		}); err != nil {
			return nil, err
		}
	} else {
		files = append(files, path)
	}

	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			return nil, fmt.Errorf("read fixture %s: %w", f, err)
		}
		var doc fixtureFile
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			return nil, fmt.Errorf("parse fixture %s: %w", f, err)
		}
		for i := range doc.Entries {
			e := doc.Entries[i]
			if e.Key == "" {
				return nil, fmt.Errorf("fixture %s: entry %d missing key", f, i)
			}
			resp := e.Response
			out[e.Key] = &resp
		}
	}
	return out, nil
}

// Ensure MockClient satisfies Client at compile time.
var _ Client = (*MockClient)(nil)
