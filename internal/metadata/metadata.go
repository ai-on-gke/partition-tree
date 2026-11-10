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

// Package metadata queries the Google Compute Engine (GCE) metadata server for cluster context.
package metadata

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	defaultBaseURL = "http://metadata.google.internal/computeMetadata/v1/"

	// GCE metadata paths.
	pathProjectID       = "project/project-id"
	pathClusterLocation = "instance/attributes/cluster-location"
	pathClusterName     = "instance/attributes/cluster-name"

	// HTTP headers for GCE metadata.
	metadataFlavorKey   = "Metadata-Flavor"
	metadataFlavorValue = "Google"

	// metadataTimeout is the maximum time to wait for a GCE metadata response.
	metadataTimeout = 5 * time.Second

	// metadataMaxBodyBytes limits the metadata response body to prevent OOM
	// from a misbehaving metadata server.
	metadataMaxBodyBytes = 1024
)

var (
	// baseURL can be overridden in tests.
	baseURL = defaultBaseURL

	// metadataClient is a dedicated HTTP client for GCE metadata requests.
	metadataClient = &http.Client{Timeout: metadataTimeout}
)

// GoogleMetadata contains GCE project and cluster information.
type GoogleMetadata struct {
	ProjectID       string
	ClusterLocation string
	ClusterName     string
}

// Metadata is an alias for GoogleMetadata.
type Metadata = GoogleMetadata

// Get fetches GCE metadata. If mock is true, it returns mock metadata values
// which can be customized via environment variables:
//   - MOCK_PROJECT_ID (or MOCK_PROJECT_PROJECT_ID)
//   - MOCK_CLUSTER_LOCATION (or MOCK_INSTANCE_ATTRIBUTES_CLUSTER_LOCATION)
//   - MOCK_CLUSTER_NAME (or MOCK_INSTANCE_ATTRIBUTES_CLUSTER_NAME)
//
// If mock is false, it queries the GCE metadata server directly.
func Get(ctx context.Context, mock bool) (*GoogleMetadata, error) {
	if mock {
		return getMockMetadata(), nil
	}

	projectID, err := queryMetadata(ctx, pathProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to detect project ID: %w", err)
	}

	clusterLocation, err := queryMetadata(ctx, pathClusterLocation)
	if err != nil {
		return nil, fmt.Errorf("failed to detect cluster location: %w", err)
	}

	clusterName, err := queryMetadata(ctx, pathClusterName)
	if err != nil {
		return nil, fmt.Errorf("failed to detect cluster name: %w", err)
	}

	return &GoogleMetadata{
		ProjectID:       projectID,
		ClusterLocation: clusterLocation,
		ClusterName:     clusterName,
	}, nil
}

func getMockMetadata() *GoogleMetadata {
	projectID := getEnvFirst("MOCK_PROJECT_ID", "MOCK_PROJECT_PROJECT_ID")
	if projectID == "" {
		projectID = "mock"
	}
	location := getEnvFirst("MOCK_CLUSTER_LOCATION", "MOCK_INSTANCE_ATTRIBUTES_CLUSTER_LOCATION")
	if location == "" {
		location = "mock"
	}
	cluster := getEnvFirst("MOCK_CLUSTER_NAME", "MOCK_INSTANCE_ATTRIBUTES_CLUSTER_NAME")
	if cluster == "" {
		cluster = "mock"
	}
	return &GoogleMetadata{
		ProjectID:       projectID,
		ClusterLocation: location,
		ClusterName:     cluster,
	}
}

func getEnvFirst(keys ...string) string {
	for _, key := range keys {
		if val := os.Getenv(key); val != "" {
			return val
		}
	}
	return ""
}

func queryMetadata(ctx context.Context, path string) (string, error) {
	targetBase := baseURL
	if targetBase == defaultBaseURL {
		if host := strings.TrimSpace(os.Getenv("GCE_METADATA_HOST")); host != "" {
			if !strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://") {
				host = "http://" + host
			}
			targetBase = strings.TrimRight(host, "/") + "/computeMetadata/v1/"
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetBase+path, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set(metadataFlavorKey, metadataFlavorValue)

	resp, err := metadataClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("metadata request failed for %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metadata %s returned HTTP %d", path, resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, metadataMaxBodyBytes))
	if err != nil {
		return "", fmt.Errorf("metadata read failed for %s: %w", path, err)
	}
	return strings.TrimSpace(string(body)), nil
}
