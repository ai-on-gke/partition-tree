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
	"fmt"

	"golang.org/x/time/rate"
)

// ErrRateLimited is returned by TryGetAvailableAcceleratorTopologies
// when no rate-limit token is available. Callers should skip the GCE
// poll, complete any non-GCE work (e.g. node refresh), and retry on
// the next reconcile.
var ErrRateLimited = errors.New("rate limited: no token available")

// TryClient extends Client with a non-blocking rate-limit check. The
// controller prefers this method when available so GCE rate limiting
// does not block workers from refreshing node readiness.
type TryClient interface {
	Client
	TryGetAvailableAcceleratorTopologies(
		ctx context.Context,
		projectNumber, zone, mig string,
	) (*TopologyResponse, error)
}

// RateLimitedClient wraps a Client with a token-bucket rate limiter.
// At target scale (1440+ MIGs on a 1s cadence), the controller can
// burst up to ~480 RPC/s. GCE quotas vary by project but a
// conservative default (e.g. 50 RPC/s with a burst of 10) protects
// against quota exhaustion and thundering-herd spikes.
type RateLimitedClient struct {
	inner   Client
	limiter *rate.Limiter
}

// NewRateLimitedClient wraps inner with a token-bucket limiter that
// allows rps sustained requests per second with the given burst size.
func NewRateLimitedClient(inner Client, rps float64, burst int) *RateLimitedClient {
	return &RateLimitedClient{
		inner:   inner,
		limiter: rate.NewLimiter(rate.Limit(rps), burst),
	}
}

// GetAvailableAcceleratorTopologies waits for a rate-limit token and
// then delegates to the inner client.
func (r *RateLimitedClient) GetAvailableAcceleratorTopologies(
	ctx context.Context,
	projectNumber, zone, mig string,
) (*TopologyResponse, error) {
	if err := r.limiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("rate limiter: %w", err)
	}
	return r.inner.GetAvailableAcceleratorTopologies(ctx, projectNumber, zone, mig)
}

// TryGetAvailableAcceleratorTopologies performs a non-blocking
// rate-limit check. If a token is available it calls the inner client;
// otherwise it returns ErrRateLimited immediately without blocking the
// worker goroutine.
func (r *RateLimitedClient) TryGetAvailableAcceleratorTopologies(
	ctx context.Context,
	projectNumber, zone, mig string,
) (*TopologyResponse, error) {
	if !r.limiter.Allow() {
		return nil, ErrRateLimited
	}
	return r.inner.GetAvailableAcceleratorTopologies(ctx, projectNumber, zone, mig)
}

// Compile-time interface conformance.
var _ Client = (*RateLimitedClient)(nil)
var _ TryClient = (*RateLimitedClient)(nil)
