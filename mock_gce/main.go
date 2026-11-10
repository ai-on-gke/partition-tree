// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// This matches the GCE beta API structure needed by partition-tree controller.
type AcceleratorTopologyInfo struct {
	AcceleratorTopology       string                    `json:"acceleratorTopology,omitempty"`
	AcceleratorTopologyHealth string                    `json:"acceleratorTopologyHealth,omitempty"`
	InstancesHealth           string                    `json:"instancesHealth,omitempty"`
	Parent                    string                    `json:"parent,omitempty"`
	AcceleratorTopologyState  *AcceleratorTopologyState `json:"acceleratorTopologyState,omitempty"`
}

type ErrorItem struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type ErrorObj struct {
	Errors []ErrorItem `json:"errors,omitempty"`
}

type AcceleratorTopologyState struct {
	CurrentState string    `json:"currentState,omitempty"`
	ExternalId   string    `json:"externalId,omitempty"`
	Error        *ErrorObj `json:"error,omitempty"`
}

type GetAvailableAcceleratorTopologiesResponse struct {
	AcceleratorTopologiesInfo map[string]AcceleratorTopologyInfo `json:"acceleratorTopologiesInfo,omitempty"`
	MultiMig                  string                             `json:"multiMig,omitempty"`
}

var (
	// Map of MIG name -> Response
	topoData = make(map[string]*GetAvailableAcceleratorTopologiesResponse)
	mu       sync.RWMutex

	// Error and delay injection state
	activeError   int
	activeMessage string
	activeDelay   time.Duration
	errMu         sync.RWMutex
)

type FixtureEntry struct {
	MigName  string                                     `json:"migName"`
	Response *GetAvailableAcceleratorTopologiesResponse `json:"response"`
}

func main() {
	http.HandleFunc("/inject/topo-health", handleInjectTopo)
	http.HandleFunc("/inject/error", handleInjectError)
	http.HandleFunc("/", handleGCE)

	log.Println("Starting mock GCE server on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}

// handleInjectTopo allows tests to inject topology data for a specific MIG.
// Example:
//
//	curl -X POST -H "Content-Type: application/json" -d '{
//	  "migName": "np1",
//	  "response": {
//	    "acceleratorTopologiesInfo": {
//	      "1": {
//	        "acceleratorTopology": "8x8",
//	        "acceleratorTopologyHealth": "HEALTHY",
//	        "instancesHealth": "ALL_HEALTHY"
//	      }
//	    }
//	  }
//	}' http://localhost:8080/inject/topo-health
func handleInjectTopo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		MigName  string                                     `json:"migName"`
		Response *GetAvailableAcceleratorTopologiesResponse `json:"response"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Failed to decode body", http.StatusBadRequest)
		return
	}

	if req.MigName == "" || req.Response == nil {
		http.Error(w, "migName and response are required", http.StatusBadRequest)
		return
	}

	mu.Lock()
	topoData[req.MigName] = req.Response
	mu.Unlock()

	log.Printf("Injected topo for MIG %s", req.MigName)
	w.WriteHeader(http.StatusOK)
}

func handleGCE(w http.ResponseWriter, r *http.Request) {
	log.Printf("Received request: %s %s", r.Method, r.URL.Path)

	errMu.RLock()
	delay := activeDelay
	errCode := activeError
	errMsg := activeMessage
	errMu.RUnlock()

	if delay > 0 {
		log.Printf("Simulating delay of %s", delay)
		time.Sleep(delay)
	}

	if errCode > 0 {
		log.Printf("Returning injected error %d: %s", errCode, errMsg)
		http.Error(w, errMsg, errCode)
		return
	}

	// Expected path: /projects/{project}/zones/{zone}/instanceGroupManagers/{igmName}/getAvailableAcceleratorTopologies
	// Or /compute/v0.beta/projects/... depending on how the client constructs it.

	if !strings.HasSuffix(r.URL.Path, "/getAvailableAcceleratorTopologies") {
		http.NotFound(w, r)
		return
	}

	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 2 {
		http.Error(w, "Malformed URL", http.StatusBadRequest)
		return
	}
	igmName := parts[len(parts)-2]

	mu.RLock()
	resp, ok := topoData[igmName]
	mu.RUnlock()

	if !ok {
		log.Printf("No topo data for MIG %s, returning empty response", igmName)
		resp = &GetAvailableAcceleratorTopologiesResponse{
			AcceleratorTopologiesInfo: make(map[string]AcceleratorTopologyInfo),
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("Error encoding response: %v", err)
	}
}

type ErrorInjection struct {
	StatusCode int    `json:"statusCode,omitempty"`
	Message    string `json:"message,omitempty"`
	Delay      string `json:"delay,omitempty"`
}

func handleInjectError(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ErrorInjection
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Failed to decode body", http.StatusBadRequest)
		return
	}

	errMu.Lock()
	activeError = req.StatusCode
	activeMessage = req.Message
	if req.Delay != "" {
		d, err := time.ParseDuration(req.Delay)
		if err != nil {
			errMu.Unlock()
			http.Error(w, "Invalid delay duration", http.StatusBadRequest)
			return
		}
		activeDelay = d
	} else {
		activeDelay = 0
	}
	errMu.Unlock()

	log.Printf("Injected error: code=%d, delay=%s", activeError, activeDelay)
	w.WriteHeader(http.StatusOK)
}
