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

package metadata

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGet_MockDefaults(t *testing.T) {
	ctx := context.Background()
	meta, err := Get(ctx, true)
	if err != nil {
		t.Fatalf("Get(ctx, true) error = %v", err)
	}

	if meta.ProjectID != "mock" {
		t.Errorf("ProjectID = %q, want %q", meta.ProjectID, "mock")
	}
	if meta.ClusterLocation != "mock" {
		t.Errorf("ClusterLocation = %q, want %q", meta.ClusterLocation, "mock")
	}
	if meta.ClusterName != "mock" {
		t.Errorf("ClusterName = %q, want %q", meta.ClusterName, "mock")
	}
}

func TestGet_MockWithEnvVars(t *testing.T) {
	ctx := context.Background()

	t.Setenv("MOCK_PROJECT_ID", "my-real-project-123")
	t.Setenv("MOCK_CLUSTER_LOCATION", "us-west1-b")
	t.Setenv("MOCK_CLUSTER_NAME", "prod-cluster")

	meta, err := Get(ctx, true)
	if err != nil {
		t.Fatalf("Get(ctx, true) error = %v", err)
	}

	if meta.ProjectID != "my-real-project-123" {
		t.Errorf("ProjectID = %q, want %q", meta.ProjectID, "my-real-project-123")
	}
	if meta.ClusterLocation != "us-west1-b" {
		t.Errorf("ClusterLocation = %q, want %q", meta.ClusterLocation, "us-west1-b")
	}
	if meta.ClusterName != "prod-cluster" {
		t.Errorf("ClusterName = %q, want %q", meta.ClusterName, "prod-cluster")
	}
}

func TestGet_RealServer(t *testing.T) {
	ctx := context.Background()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(metadataFlavorKey) != metadataFlavorValue {
			http.Error(w, "missing metadata flavor header", http.StatusForbidden)
			return
		}
		switch r.URL.Path {
		case "/" + pathProjectID:
			_, _ = w.Write([]byte("server-project-id\n"))
		case "/" + pathClusterLocation:
			_, _ = w.Write([]byte("us-central1-a\n"))
		case "/" + pathClusterName:
			_, _ = w.Write([]byte("server-cluster-name\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	originalBaseURL := baseURL
	baseURL = server.URL + "/"
	defer func() { baseURL = originalBaseURL }()

	meta, err := Get(ctx, false)
	if err != nil {
		t.Fatalf("Get(ctx, false) error = %v", err)
	}

	if meta.ProjectID != "server-project-id" {
		t.Errorf("ProjectID = %q, want %q", meta.ProjectID, "server-project-id")
	}
	if meta.ClusterLocation != "us-central1-a" {
		t.Errorf("ClusterLocation = %q, want %q", meta.ClusterLocation, "us-central1-a")
	}
	if meta.ClusterName != "server-cluster-name" {
		t.Errorf("ClusterName = %q, want %q", meta.ClusterName, "server-cluster-name")
	}
}
