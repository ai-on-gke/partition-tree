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

package config

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestDuration_JSONRoundTrip(t *testing.T) {
	in := []byte(`"3s"`)
	var d Duration
	if err := d.UnmarshalJSON(in); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if d.D() != 3*time.Second {
		t.Errorf("got %v want 3s", d.D())
	}
	out, err := d.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != `"3s"` {
		t.Errorf("got %s want \"3s\"", string(out))
	}
}

func TestDuration_InvalidJSON(t *testing.T) {
	var d Duration
	if err := d.UnmarshalJSON([]byte(`"not a duration"`)); err == nil {
		t.Errorf("expected error for bogus duration")
	}
	if err := d.UnmarshalJSON([]byte(`42`)); err == nil {
		t.Errorf("expected error for non-string duration")
	}
}

func TestConfig_UnmarshalFullYAMLShape(t *testing.T) {
	// The yaml ConfigMap is converted to JSON on the way in; test the
	// JSON unmarshal path directly.
	raw := []byte(`{
		"polling": {"interval": "1s", "jitter": "100ms"},
		"syncTimeout": "30s",
		"retry": {
			"initialBackoff": "500ms",
			"maxBackoff": "1m",
			"multiplier": 1.5
		},
		"rateLimit": {
			"qps": 100,
			"burst": 20
		}
	}`)
	var c Config
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if c.Polling.Interval.D() != time.Second {
		t.Errorf("polling.interval: %v", c.Polling.Interval.D())
	}
	if c.SyncTimeout.D() != 30*time.Second {
		t.Errorf("syncTimeout: %v", c.SyncTimeout.D())
	}
	if c.Retry.Multiplier != 1.5 {
		t.Errorf("retry: %+v", c.Retry)
	}
	if err := c.Validate(); err != nil {
		t.Errorf("validate: %v", err)
	}
}

func TestConfig_Defaults(t *testing.T) {
	c := Default()
	if err := c.Validate(); err != nil {
		t.Errorf("defaults invalid: %v", err)
	}
	if c.Polling.Interval.D() != 1*time.Second {
		t.Errorf("default polling: %v", c.Polling.Interval.D())
	}
	if c.RateLimit.Burst != 50 {
		t.Errorf("default burst = %d want 50", c.RateLimit.Burst)
	}
}

func TestConfig_ValidateRejectsBadValues(t *testing.T) {
	bad := []Config{
		{}, // zero polling interval
		{Polling: Polling{Interval: Duration(time.Second)}, SyncTimeout: 0},
		{
			Polling:     Polling{Interval: Duration(time.Second)},
			SyncTimeout: Duration(time.Second),
			Retry:       Retry{Multiplier: 0.5},
		},
	}
	for i, c := range bad {
		if err := c.Validate(); err == nil {
			t.Errorf("case %d: expected validation error", i)
		}
	}
}

func TestStore_AtomicLoadStore(t *testing.T) {
	s := NewStore(Default())
	if s.Load().Polling.Interval.D() != 1*time.Second {
		t.Errorf("initial load")
	}
	updated := Default()
	updated.Polling.Interval = Duration(500 * time.Millisecond)
	s.Store(updated)
	if s.Load().Polling.Interval.D() != 500*time.Millisecond {
		t.Errorf("after store")
	}
}

func TestParseConfigData_MissingKey(t *testing.T) {
	_, err := parseConfigData(map[string]string{"wrong-key": "value"})
	if err == nil {
		t.Fatal("expected error for missing key")
	}
	if !strings.Contains(err.Error(), ConfigMapDataKey) {
		t.Errorf("error should mention %q, got: %v", ConfigMapDataKey, err)
	}
}

func TestParseConfigData_PartialFields(t *testing.T) {
	cfg, err := parseConfigData(map[string]string{
		ConfigMapDataKey: `{"polling":{"interval":"5s"},"syncTimeout":"10s","retry":{"initialBackoff":"1s","maxBackoff":"30s","multiplier":2}}`,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Polling.Interval.D() != 5*time.Second {
		t.Errorf("interval = %v want 5s", cfg.Polling.Interval.D())
	}
	// Jitter should fall back to Default() value.
	d := Default()
	if cfg.Polling.Jitter.D() != d.Polling.Jitter.D() {
		t.Errorf("jitter = %v want default %v", cfg.Polling.Jitter.D(), d.Polling.Jitter.D())
	}
}

func TestValidate_EdgeCases(t *testing.T) {
	base := func() Config {
		c := *Default()
		return c
	}
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"negative QPS", func(c *Config) { c.RateLimit.QPS = -1 }},
		{"zero Burst", func(c *Config) { c.RateLimit.Burst = 0 }},
		{"negative InitialBackoff", func(c *Config) { c.Retry.InitialBackoff = Duration(-time.Second) }},
		{"negative MaxBackoff", func(c *Config) { c.Retry.MaxBackoff = Duration(-time.Second) }},
		{"empty ExcludedPrefix", func(c *Config) { c.Taints.NodePropagation.ExcludedPrefixes = []string{"a", "", "b"} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := base()
			tc.mutate(&c)
			if err := c.Validate(); err == nil {
				t.Errorf("expected validation error for %s", tc.name)
			}
		})
	}
}
