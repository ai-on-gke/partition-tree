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

// Package config defines the hot-reloadable runtime configuration held
// in an atomic.Pointer. The loader (wired by the manager entrypoint)
// watches the ConfigMap and swaps the pointer on valid updates.
package config

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"
)

// Duration is a time.Duration that JSON-unmarshals from strings like
// "3s", "15s", "1m". Yaml passes through sigs.k8s.io/yaml as JSON, so a
// single unmarshal path covers both formats.
type Duration time.Duration

// UnmarshalJSON parses a Duration from a JSON string.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string: %w", err)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// MarshalJSON emits the Duration as a string.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// D returns the underlying time.Duration for use in timers / requeues.
func (d Duration) D() time.Duration { return time.Duration(d) }

// Polling controls the GCE polling cadence.
type Polling struct {
	Interval Duration `json:"interval"`
	Jitter   Duration `json:"jitter"`
}

// Retry controls backoff on GCE errors.
type Retry struct {
	InitialBackoff Duration `json:"initialBackoff"`
	MaxBackoff     Duration `json:"maxBackoff"`
	Multiplier     float64  `json:"multiplier"`
}

// RateLimit controls GCE API rate limiting via a token-bucket.
//
// NOTE: QPS and Burst are read once at startup to construct the
// rate.Limiter and are NOT hot-reloaded. Changing them in the
// ConfigMap requires a controller restart to take effect.
type RateLimit struct {
	// QPS is the sustained requests per second allowed to GCE.
	QPS float64 `json:"qps"`
	// Burst is the maximum burst size above QPS.
	Burst int `json:"burst"`
}

// Concurrency controls the reconciler work queue.
//
// NOTE: MaxReconciles is read once at SetupWithManager and is NOT
// hot-reloaded. controller-runtime does not support changing the
// worker count after startup. A controller restart is required.
type Concurrency struct {
	// MaxReconciles caps parallel reconciles. At 1440 MIGs and a 1s
	// polling interval we need ~1440 reconciles/sec, so the default (1)
	// is wildly insufficient. A single-digit worker count is enough
	// because each reconcile is O(leaves) CPU + one GCE RPC.
	MaxReconciles int `json:"maxReconciles"`
}

// NodePropagation controls how taints are propagated from nodes to the CR.
type NodePropagation struct {
	ExcludedPrefixes []string `json:"excludedPrefixes,omitempty"`
}

// Taints controls taint-related behaviors.
type Taints struct {
	NodePropagation NodePropagation `json:"nodePropagation"`
}

// Config is the full runtime config blob. It maps 1:1 to the ConfigMap
// data.config.yaml schema.
type Config struct {
	Polling     Polling     `json:"polling"`
	SyncTimeout Duration    `json:"syncTimeout"`
	Retry       Retry       `json:"retry"`
	RateLimit   RateLimit   `json:"rateLimit"`
	Concurrency Concurrency `json:"concurrency"`
	Taints      Taints      `json:"taints"`
}

// Default returns the built-in defaults used when the ConfigMap is absent
// or a field is omitted: 1s polling, 15s sync
// timeout, 1s→30s exponential backoff, 8 concurrent reconciles.
func Default() *Config {
	return &Config{
		Polling: Polling{
			Interval: Duration(1 * time.Second),
			// 10% jitter spreads the natural polling tick for large
			// fleets. At 1440+ CRs that means the first fan-out cycle
			// staggers across ±100 ms instead of hammering GCE in a
			// synchronized burst every 1 s.
			Jitter: Duration(100 * time.Millisecond),
		},
		SyncTimeout: Duration(15 * time.Second),
		Retry: Retry{
			InitialBackoff: Duration(1 * time.Second),
			MaxBackoff:     Duration(30 * time.Second),
			Multiplier:     2.0,
		},
		RateLimit: RateLimit{
			// 50 QPS with burst 50 handles 1440+ MIGs on a 1s cadence.
			// Burst matches QPS so the token bucket can absorb a full
			// second of traffic after idle periods (e.g. post-outage
			// catch-up or startup fan-out).
			QPS:   50,
			Burst: 50,
		},
		Concurrency: Concurrency{
			MaxReconciles: 8,
		},
		Taints: Taints{
			NodePropagation: NodePropagation{
				ExcludedPrefixes: []string{"google.com/tpu"},
			},
		},
	}
}

// Validate rejects obviously bad configs so a broken ConfigMap cannot
// replace a good in-memory one.
func (c *Config) Validate() error {
	if c.Polling.Interval.D() <= 0 {
		return fmt.Errorf("polling.interval must be > 0")
	}
	if c.Polling.Jitter.D() < 0 {
		return fmt.Errorf("polling.jitter must be >= 0")
	}
	if c.SyncTimeout.D() <= 0 {
		return fmt.Errorf("syncTimeout must be > 0")
	}
	if c.Retry.Multiplier <= 1 {
		return fmt.Errorf("retry.multiplier must be > 1 for exponential backoff growth")
	}
	if c.Retry.InitialBackoff.D() < 0 || c.Retry.MaxBackoff.D() < 0 {
		return fmt.Errorf("retry backoffs must be >= 0")
	}
	if c.RateLimit.QPS <= 0 {
		return fmt.Errorf("rateLimit.qps must be > 0")
	}
	if c.RateLimit.Burst <= 0 {
		return fmt.Errorf("rateLimit.burst must be > 0")
	}
	if c.Concurrency.MaxReconciles < 0 {
		return fmt.Errorf("concurrency.maxReconciles must be >= 0 (0 = controller-runtime default)")
	}
	for _, prefix := range c.Taints.NodePropagation.ExcludedPrefixes {
		if prefix == "" {
			return fmt.Errorf("taints.nodePropagation.excludedPrefixes cannot contain empty strings")
		}
	}
	return nil
}

// Store is a lock-free container for the current Config. The loader
// swaps the pointer on hot-reload; the reconciler reads it on every
// Reconcile with no lock contention.
type Store struct {
	ptr atomic.Pointer[Config]
}

// NewStore creates a Store seeded with an initial config.
func NewStore(initial *Config) *Store {
	s := &Store{}
	s.ptr.Store(initial)
	return s
}

// Load returns the current config. The returned pointer must be treated
// as read-only; callers that need to mutate should build a new Config
// and Store it.
func (s *Store) Load() *Config { return s.ptr.Load() }

// Store atomically replaces the current config.
func (s *Store) Store(c *Config) { s.ptr.Store(c) }
