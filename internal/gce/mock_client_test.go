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
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestMockClient_SetError(t *testing.T) {
	m := NewMockClient()
	m.SetResponse("mig-a", &TopologyResponse{
		Entries: []TopologyEntry{{PartitionID: "root", State: "Healthy"}},
	})

	// With error set, calls should fail.
	m.SetError(errors.New("boom"))
	_, err := m.GetAvailableAcceleratorTopologies(context.Background(), "p", "z", "mig-a")
	if err == nil {
		t.Fatal("expected error")
	}

	// Clear error: calls should succeed again.
	m.SetError(nil)
	resp, err := m.GetAvailableAcceleratorTopologies(context.Background(), "p", "z", "mig-a")
	if err != nil {
		t.Fatalf("after clear: %v", err)
	}
	if len(resp.Entries) != 1 {
		t.Errorf("entries = %d want 1", len(resp.Entries))
	}
}

func TestMockClient_CallsCounter(t *testing.T) {
	m := NewMockClient()
	m.SetError(errors.New("fail"))

	for range 3 {
		_, _ = m.GetAvailableAcceleratorTopologies(context.Background(), "p", "z", "m")
	}
	if m.Calls() != 3 {
		t.Errorf("after 3 failures: Calls() = %d want 3", m.Calls())
	}

	m.SetError(nil)
	m.SetResponse("m", &TopologyResponse{})
	_, _ = m.GetAvailableAcceleratorTopologies(context.Background(), "p", "z", "m")
	if m.Calls() != 4 {
		t.Errorf("after 1 success: Calls() = %d want 4", m.Calls())
	}
}

func TestMockClient_LoadFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fixtures.yaml")

	content := `entries:
- key: proj/zone/mig-x
  response:
    entries:
    - partitionId: root
      state: Healthy
      topology: "4x4x4"
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	m := NewMockClient()
	if err := m.LoadFromFile(path); err != nil {
		t.Fatalf("LoadFromFile: %v", err)
	}

	resp, err := m.GetAvailableAcceleratorTopologies(context.Background(), "proj", "zone", "mig-x")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(resp.Entries) != 1 || resp.Entries[0].PartitionID != "root" {
		t.Errorf("unexpected response: %+v", resp)
	}
}

func TestMockClient_LoadFromFile_Errors(t *testing.T) {
	m := NewMockClient()

	// Nonexistent file.
	if err := m.LoadFromFile("/nonexistent/path"); err == nil {
		t.Error("expected error for missing file")
	}

	dir := t.TempDir()

	// Invalid YAML.
	badYAML := filepath.Join(dir, "bad.yaml")
	_ = os.WriteFile(badYAML, []byte("not: valid: yaml: [[["), 0o644)
	if err := m.LoadFromFile(badYAML); err == nil {
		t.Error("expected error for invalid YAML")
	}

	// Missing key field.
	noKey := filepath.Join(dir, "nokey.yaml")
	_ = os.WriteFile(noKey, []byte("entries:\n- response:\n    entries: []\n"), 0o644)
	if err := m.LoadFromFile(noKey); err == nil {
		t.Error("expected error for missing key")
	}
}

func TestMockClient_FQNFallback(t *testing.T) {
	m := NewMockClient()
	m.SetResponse("my-mig", &TopologyResponse{
		Entries: []TopologyEntry{{PartitionID: "root", State: "Healthy"}},
	})

	// Call with full project/zone/mig — should fall back to bare "my-mig".
	resp, err := m.GetAvailableAcceleratorTopologies(context.Background(), "proj", "zone", "my-mig")
	if err != nil {
		t.Fatalf("fqn fallback: %v", err)
	}
	if len(resp.Entries) != 1 {
		t.Errorf("entries = %d want 1", len(resp.Entries))
	}
}
